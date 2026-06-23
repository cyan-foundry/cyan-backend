// src/actors/discovery_actor.rs
//
// DiscoveryActor - handles peer discovery and mesh formation
//
// Responsibilities:
// - Broadcast groups_exchange on startup and NeighborUp
// - Handle peer_introduction to add peers to group topics
// - Track which peers are in which groups
// - Communicate with NetworkActor to spawn/join topics

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::PublicKey;
use iroh_gossip::api::{Event as GossipEvent, GossipReceiver, GossipSender};
use iroh_gossip::net::Gossip;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{
    actors::{make_topic_id, ActorHandle, ActorMessage, SystemCommand},
    models::events::SwiftEvent,
    storage,
};

// ═══════════════════════════════════════════════════════════════════════════
// DISCOVERY WIRE MESSAGES
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "msg_type")]
pub enum DiscoveryMessage {
    /// Peer announces which groups they belong to
    #[serde(rename = "groups_exchange")]
    GroupsExchange {
        node_id: String,
        #[serde(rename = "local_groups")]
        groups: Vec<String>,
    },
    /// Introduce peers for a specific group (sent by bootstrap or existing peers)
    #[serde(rename = "peer_introduction")]
    PeerIntroduction {
        group_id: String,
        peers: Vec<String>,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
// DISCOVERY COMMANDS (from NetworkActor or external)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
pub enum DiscoveryCommand {
    /// Announce we joined a new group (triggers rebroadcast)
    AnnounceGroup(String),
    /// Remove a group from our list (we left it)
    LeaveGroup(String),
    /// Force rebroadcast of groups_exchange
    Rebroadcast,
    /// Broadcast peer introduction for a group (we're helping mesh form)
    BroadcastPeerIntro { group_id: String, peers: Vec<String> },
}

// ═══════════════════════════════════════════════════════════════════════════
// INTERNAL NETWORK COMMANDS (sent TO NetworkActor)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum DiscoveryNetworkCmd {
    /// Add a single peer to a topic
    JoinPeerToTopic { group_id: String, peer: PublicKey },
    /// Add multiple peers to a topic
    JoinPeersToTopic { group_id: String, peers: Vec<PublicKey> },
    /// Spawn a topic actor if it doesn't exist
    EnsureTopicExists { group_id: String },
    /// A new peer was discovered
    PeerDiscovered { peer_id: String, groups: Vec<String> },
}

// ═══════════════════════════════════════════════════════════════════════════
// DISCOVERY ACTOR
// ═══════════════════════════════════════════════════════════════════════════

pub struct DiscoveryActor {
    node_id: String,
    /// Discovery namespace this actor was constructed with. Retained for parity
    /// with the seed/config path; not read on the gossip hot path.
    #[allow(dead_code)]
    discovery_key: String,
    sender: GossipSender,

    /// Groups I'm a member of
    my_groups: HashSet<String>,

    /// Peer tracking: peer_id → set of their groups
    peer_groups: HashMap<String, HashSet<String>>,

    /// All known peers (for mesh health)
    known_peers: HashSet<PublicKey>,

    /// Channel to send commands to NetworkActor
    network_tx: UnboundedSender<DiscoveryNetworkCmd>,

    /// Channel to send events to Swift
    event_tx: UnboundedSender<SwiftEvent>,
}

impl DiscoveryActor {
    /// Spawn the discovery actor
    pub async fn spawn(
        node_id: String,
        discovery_key: String,
        bootstrap_peers: Vec<PublicKey>,
        gossip: Arc<Gossip>,
        network_tx: UnboundedSender<DiscoveryNetworkCmd>,
        event_tx: UnboundedSender<SwiftEvent>,
    ) -> Result<ActorHandle<DiscoveryCommand>> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        // Create discovery topic ID
        let topic_str = format!("cyan/discovery/{}", discovery_key);
        let topic_id = make_topic_id(&topic_str)?;

        // Bootstrap peers for the discovery topic are chosen by the caller from the
        // node's DiscoveryPolicy (Bootstrap(id) → [id]; MdnsOnly → []). Production
        // passes the default bootstrap node id, so behavior is unchanged.

        tracing::info!(
            "🔍 [DISCOVERY] Subscribing to topic: {} with {} bootstrap peers",
            &topic_str[..32.min(topic_str.len())],
            bootstrap_peers.len()
        );

        // Subscribe to the discovery topic WITHOUT awaiting `joined()`. `subscribe_and_join`
        // parks until ≥1 neighbour connects; offline (MdnsOnly → empty bootstrap, or an
        // unreachable bootstrap) that never happens, so awaiting it here would block
        // `NetworkActor::start` before it reaches its command loop. The join proceeds in the
        // background — `run` broadcasts our groups on startup and again on every `NeighborUp`
        // (`handle_gossip_event`) — so a cold offline start is non-blocking and discovery
        // recovers the moment a neighbour appears.
        let topic = gossip.subscribe(topic_id, bootstrap_peers.clone()).await?;
        let (sender, receiver) = topic.split();

        // Load my groups from DB
        let my_groups = storage::group_list_ids();
        tracing::info!("🔍 [DISCOVERY] Loaded {} groups from DB", my_groups.len());

        let actor = Self {
            node_id: node_id.clone(),
            discovery_key,
            sender,
            my_groups,
            peer_groups: HashMap::new(),
            known_peers: bootstrap_peers.into_iter().collect(),
            network_tx,
            event_tx,
        };

        let join_handle = tokio::spawn(actor.run(cmd_rx, receiver));

        tracing::info!("🔍 [DISCOVERY] Actor spawned for node {}", &node_id[..16]);

        Ok(ActorHandle {
            cmd_tx,
            join_handle,
        })
    }

    async fn run(
        mut self,
        mut cmd_rx: UnboundedReceiver<ActorMessage<DiscoveryCommand>>,
        mut receiver: GossipReceiver,
    ) {
        // Initial broadcast of our groups
        if !self.my_groups.is_empty()
            && let Err(e) = self.broadcast_groups_exchange().await {
                tracing::error!("🔴 [DISCOVERY] Initial broadcast failed: {}", e);
            }

        loop {
            tokio::select! {
                // Handle commands
                msg = cmd_rx.recv() => {
                    match msg {
                        Some(ActorMessage::System(sys)) => {
                            if self.handle_system_command(sys) {
                                break;
                            }
                        }
                        Some(ActorMessage::Domain(cmd)) => {
                            self.handle_command(cmd).await;
                        }
                        None => {
                            tracing::info!("🛑 [DISCOVERY] Command channel closed");
                            break;
                        }
                    }
                }

                // Handle gossip events
                event = receiver.next() => {
                    match event {
                        Some(Ok(gossip_event)) => {
                            self.handle_gossip_event(gossip_event).await;
                        }
                        Some(Err(e)) => {
                            tracing::error!("🔴 [DISCOVERY] Gossip error: {}", e);
                        }
                        None => {
                            tracing::warn!("⚠️ [DISCOVERY] Gossip receiver closed");
                            break;
                        }
                    }
                }
            }
        }

        tracing::info!("🛑 [DISCOVERY] Actor stopped");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // COMMAND HANDLING
    // ═══════════════════════════════════════════════════════════════════════

    fn handle_system_command(&self, cmd: SystemCommand) -> bool {
        match cmd {
            SystemCommand::PoisonPill => {
                tracing::info!("💀 [DISCOVERY] Received PoisonPill");
                true
            }
            SystemCommand::DumpDiagnostics => {
                tracing::info!(
                    "🩺 [DISCOVERY] my_groups={}, known_peers={}, peer_groups={}",
                    self.my_groups.len(),
                    self.known_peers.len(),
                    self.peer_groups.len()
                );
                for (peer, groups) in &self.peer_groups {
                    tracing::info!("   peer {}: {} groups", &peer[..16], groups.len());
                }
                false
            }
            SystemCommand::PullInFlight => false,
        }
    }

    async fn handle_command(&mut self, cmd: DiscoveryCommand) {
        match cmd {
            DiscoveryCommand::AnnounceGroup(group_id) => {
                tracing::info!("🔍 [DISCOVERY] Adding group: {}", &group_id[..16.min(group_id.len())]);
                self.my_groups.insert(group_id);

                // Rebroadcast with new group
                if let Err(e) = self.broadcast_groups_exchange().await {
                    tracing::error!("🔴 [DISCOVERY] Rebroadcast failed: {}", e);
                }
            }

            DiscoveryCommand::LeaveGroup(group_id) => {
                tracing::info!("🔍 [DISCOVERY] Removing group: {}", &group_id[..16.min(group_id.len())]);
                self.my_groups.remove(&group_id);

                // Rebroadcast without the group
                if let Err(e) = self.broadcast_groups_exchange().await {
                    tracing::error!("🔴 [DISCOVERY] Rebroadcast failed: {}", e);
                }
            }

            DiscoveryCommand::Rebroadcast => {
                if let Err(e) = self.broadcast_groups_exchange().await {
                    tracing::error!("🔴 [DISCOVERY] Rebroadcast failed: {}", e);
                }
            }

            DiscoveryCommand::BroadcastPeerIntro { group_id, peers } => {
                if let Err(e) = self.broadcast_peer_introduction(&group_id, peers).await {
                    tracing::error!("🔴 [DISCOVERY] Peer intro broadcast failed: {}", e);
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GOSSIP EVENT HANDLING
    // ═══════════════════════════════════════════════════════════════════════

    async fn handle_gossip_event(&mut self, event: GossipEvent) {
        match event {
            GossipEvent::Received(msg) => {
                let from = msg.delivered_from.to_string();

                // Ignore our own messages
                if from == self.node_id {
                    return;
                }

                // Parse discovery message
                match serde_json::from_slice::<DiscoveryMessage>(&msg.content) {
                    Ok(disc_msg) => {
                        self.handle_discovery_message(disc_msg, msg.delivered_from).await;
                    }
                    Err(e) => {
                        tracing::debug!(
                            "🔍 [DISCOVERY] Non-discovery message from {}: {}",
                            &from[..16],
                            e
                        );
                    }
                }
            }

            GossipEvent::NeighborUp(peer) => {
                let peer_str = peer.to_string();
                tracing::info!("🟢 [DISCOVERY] Neighbor UP: {}", &peer_str[..16]);

                self.known_peers.insert(peer);

                // Broadcast our groups to new neighbor
                if let Err(e) = self.broadcast_groups_exchange().await {
                    tracing::error!("🔴 [DISCOVERY] Broadcast on NeighborUp failed: {}", e);
                }

                // Emit event
                let _ = self.event_tx.send(SwiftEvent::StatusUpdate {
                    message: format!("Peer connected: {}...", &peer_str[..8]),
                });
            }

            GossipEvent::NeighborDown(peer) => {
                let peer_str = peer.to_string();
                tracing::info!("🔴 [DISCOVERY] Neighbor DOWN: {}", &peer_str[..16]);

                self.known_peers.remove(&peer);

                // Get groups this peer was in before removing
                let peer_was_in: Vec<String> = self.peer_groups
                    .get(&peer_str)
                    .map(|g| g.iter().cloned().collect())
                    .unwrap_or_default();

                self.peer_groups.remove(&peer_str);

                // For groups we share, rebroadcast peer intro with remaining peers
                for group_id in peer_was_in {
                    if self.my_groups.contains(&group_id) {
                        let remaining_peers = self.get_peers_for_group(&group_id);
                        if remaining_peers.len() > 1 {
                            let _ = self.broadcast_peer_introduction(&group_id, remaining_peers).await;
                        }
                    }
                }
            }

            GossipEvent::Lagged => {
                tracing::warn!("⚠️ [DISCOVERY] Lagged - missed messages");
            }
        }
    }

    async fn handle_discovery_message(&mut self, msg: DiscoveryMessage, from_peer: PublicKey) {
        match msg {
            DiscoveryMessage::GroupsExchange { node_id, groups } => {
                self.handle_groups_exchange(node_id, groups, from_peer).await;
            }
            DiscoveryMessage::PeerIntroduction { group_id, peers } => {
                self.handle_peer_introduction(group_id, peers).await;
            }
        }
    }

    async fn handle_groups_exchange(
        &mut self,
        peer_node_id: String,
        their_groups: Vec<String>,
        peer_pk: PublicKey,
    ) {
        tracing::info!(
            "🔍 [DISCOVERY] GroupsExchange from {}: {} groups",
            &peer_node_id[..16],
            their_groups.len()
        );

        // Find shared groups
        let shared_groups: Vec<String> = their_groups
            .iter()
            .filter(|g| self.my_groups.contains(*g))
            .cloned()
            .collect();

        if !shared_groups.is_empty() {
            tracing::info!(
                "🔗 [DISCOVERY] {} shared groups with {}",
                shared_groups.len(),
                &peer_node_id[..16]
            );

            // Tell NetworkActor to add this peer to shared topics
            for group_id in &shared_groups {
                let _ = self.network_tx.send(DiscoveryNetworkCmd::JoinPeerToTopic {
                    group_id: group_id.clone(),
                    peer: peer_pk,
                });
            }

            // Broadcast peer introduction for shared groups
            // This helps the mesh form faster
            for group_id in &shared_groups {
                let all_peers = self.get_peers_for_group(group_id);
                if all_peers.len() > 1 {
                    let _ = self.broadcast_peer_introduction(group_id, all_peers).await;
                }
            }
        }

        // Track this peer's groups
        self.peer_groups.insert(
            peer_node_id.clone(),
            their_groups.into_iter().collect(),
        );

        // Notify NetworkActor about peer discovery
        let _ = self.network_tx.send(DiscoveryNetworkCmd::PeerDiscovered {
            peer_id: peer_node_id,
            groups: shared_groups,
        });
    }

    async fn handle_peer_introduction(&mut self, group_id: String, peers: Vec<String>) {
        // Only process if we're in this group
        if !self.my_groups.contains(&group_id) {
            tracing::debug!(
                "🔍 [DISCOVERY] Ignoring peer_intro for group {} (not a member)",
                &group_id[..16.min(group_id.len())]
            );
            return;
        }

        tracing::info!(
            "🔍 [DISCOVERY] PeerIntroduction for {}: {} peers",
            &group_id[..16.min(group_id.len())],
            peers.len()
        );

        // Parse and collect valid peers (excluding ourselves)
        let new_peers: Vec<PublicKey> = peers
            .iter()
            .filter(|p| *p != &self.node_id)
            .filter_map(|p| PublicKey::from_str(p).ok())
            .collect();

        if new_peers.is_empty() {
            return;
        }

        tracing::info!(
            "🔗 [DISCOVERY] Adding {} peers to group {}",
            new_peers.len(),
            &group_id[..16.min(group_id.len())]
        );

        // Tell NetworkActor to add these peers to the topic
        let _ = self.network_tx.send(DiscoveryNetworkCmd::JoinPeersToTopic {
            group_id,
            peers: new_peers,
        });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // BROADCAST HELPERS
    // ═══════════════════════════════════════════════════════════════════════

    async fn broadcast_groups_exchange(&self) -> Result<()> {
        let msg = DiscoveryMessage::GroupsExchange {
            node_id: self.node_id.clone(),
            groups: self.my_groups.iter().cloned().collect(),
        };

        let json = serde_json::to_vec(&msg)?;
        self.sender.broadcast(Bytes::from(json)).await?;

        tracing::debug!(
            "📤 [DISCOVERY] Broadcast groups_exchange: {} groups",
            self.my_groups.len()
        );

        Ok(())
    }

    async fn broadcast_peer_introduction(&self, group_id: &str, peers: Vec<String>) -> Result<()> {
        let msg = DiscoveryMessage::PeerIntroduction {
            group_id: group_id.to_string(),
            peers,
        };

        let json = serde_json::to_vec(&msg)?;
        self.sender.broadcast(Bytes::from(json)).await?;

        tracing::debug!(
            "📤 [DISCOVERY] Broadcast peer_introduction for {}",
            &group_id[..16.min(group_id.len())]
        );

        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════════════════════════════

    /// Get all known peers for a specific group
    fn get_peers_for_group(&self, group_id: &str) -> Vec<String> {
        let mut peers = vec![self.node_id.clone()];

        for (peer_id, groups) in &self.peer_groups {
            if groups.contains(group_id) {
                peers.push(peer_id.clone());
            }
        }

        peers
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// RE-EXPORT for mod.rs
// ═══════════════════════════════════════════════════════════════════════════

pub use DiscoveryCommand as DiscoveryActorCommand;
pub use DiscoveryNetworkCmd as DiscoveryNetworkActorHandle;
