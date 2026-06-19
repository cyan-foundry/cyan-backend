// src/actors/topic_actor.rs
//
// TopicActor - Handles a single group's gossip topic
//
// Responsibilities:
// - Subscribe to group topic
// - Handle incoming NetworkEvents from gossip
// - Persist events to DB via storage module
// - Emit SwiftEvents to app layer
// - Handle file transfers (request/send)
// - Handle snapshot requests AND downloads

use std::{
    collections::HashSet,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::endpoint::Connection;
use iroh::{Endpoint, PublicKey};
use iroh_gossip::api::{Event as GossipEvent, GossipReceiver, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{
    actors::{make_group_topic_id, ActorHandle, ActorMessage, SystemCommand, TopicNetworkCmd, FILE_TRANSFER_ALPN},
    bootstrap_node_id,
    models::{
        commands::NetworkCommand,
        events::{NetworkEvent, SwiftEvent},
        protocol::{FileTransferMsg, SnapshotFrame},
    },
    storage,
};

/// ALPN for snapshot transfer protocol
pub const SNAPSHOT_ALPN: &[u8] = b"cyan-snapshot-v1";

// ═══════════════════════════════════════════════════════════════════════════
// TOPIC COMMANDS
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
pub enum TopicCommand {
    /// Broadcast a NetworkEvent to this group
    Broadcast(NetworkEvent),
    /// Add peers to this topic (discovered via groups_exchange)
    JoinPeers(Vec<PublicKey>),
    /// Download a file from a peer
    DownloadFile {
        file_id: String,
        hash: String,
        source_peer: String,
        resume_offset: u64,
    },
    /// Request snapshot from peers
    RequestSnapshot,
    /// Download snapshot from a specific peer (triggered by SnapshotAvailable)
    DownloadSnapshot {
        source_peer: String,
    },
    /// Mark snapshot as needed or received
    SetNeedSnapshot(bool),
}

// ═══════════════════════════════════════════════════════════════════════════
// TOPIC ACTOR
// ═══════════════════════════════════════════════════════════════════════════

pub struct TopicActor {
    node_id: String,
    group_id: String,
    endpoint: Endpoint,
    sender: GossipSender,

    /// Known peers in this topic
    known_peers: HashSet<PublicKey>,

    /// Local flag: do we need a snapshot for this group?
    need_snapshot: bool,

    /// Channel to NetworkActor for snapshot coordination
    network_tx: UnboundedSender<TopicNetworkCmd>,

    /// Event channel to Swift
    event_tx: UnboundedSender<SwiftEvent>,
}

impl TopicActor {
    /// Spawn a new TopicActor for a group
    pub async fn spawn(
        node_id: String,
        group_id: String,
        endpoint: Endpoint,
        gossip: Arc<Gossip>,
        initial_peers: Vec<PublicKey>,
        network_tx: UnboundedSender<TopicNetworkCmd>,
        event_tx: UnboundedSender<SwiftEvent>,
    ) -> Result<ActorHandle<TopicCommand>> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        // Create topic ID
        let topic_id = make_group_topic_id(&group_id)?;

        // Get bootstrap peer
        let mut peers = initial_peers;
        if let Ok(bootstrap_pk) = PublicKey::from_str(bootstrap_node_id()) {
            if !peers.contains(&bootstrap_pk) {
                peers.push(bootstrap_pk);
            }
        }

        eprintln!("🎯 [TOPIC] ════════════════════════════════════════════════════════");
        eprintln!("🎯 [TOPIC] Spawning TopicActor for group");
        eprintln!("   group_id: {}...", &group_id[..16.min(group_id.len())]);
        eprintln!("   initial_peers: {}", peers.len());
        eprintln!("🎯 [TOPIC] ════════════════════════════════════════════════════════");

        tracing::info!(
            "🎯 [TOPIC] Subscribing to group {} with {} peers",
            &group_id[..16.min(group_id.len())],
            peers.len()
        );

        // Subscribe to the topic WITHOUT awaiting `joined()`. `subscribe_and_join` parks
        // until ≥1 neighbour connects; with only an unreachable bootstrap (the relay-only
        // default, offline) that never happens, so awaiting it here would block the caller
        // (`NetworkActor::start`/`handle JoinGroup`) and wedge the command loop. The join
        // proceeds in the background — `run`'s gossip stream surfaces the first `NeighborUp`
        // via `handle_gossip_event` exactly like every subsequent one — so startup is
        // non-blocking and the topic recovers the moment a neighbour appears.
        let topic = gossip.subscribe(topic_id, peers.clone()).await?;
        let (sender, receiver) = topic.split();

        // Signal to NetworkActor that we need a snapshot
        let _ = network_tx.send(TopicNetworkCmd::NeedSnapshot {
            group_id: group_id.clone(),
        });
        eprintln!("📤 [TOPIC] Sent NeedSnapshot to NetworkActor");

        let actor = Self {
            node_id: node_id.clone(),
            group_id: group_id.clone(),
            endpoint,
            sender,
            known_peers: peers.into_iter().collect(),
            need_snapshot: true,  // New groups always need snapshot
            network_tx,
            event_tx,
        };

        let join_handle = tokio::spawn(actor.run(cmd_rx, receiver));

        tracing::info!(
            "✅ [TOPIC] Actor spawned for group {}",
            &group_id[..16.min(group_id.len())]
        );

        Ok(ActorHandle {
            cmd_tx,
            join_handle,
        })
    }

    async fn run(
        mut self,
        mut cmd_rx: UnboundedReceiver<ActorMessage<TopicCommand>>,
        mut receiver: GossipReceiver,
    ) {
        tracing::info!(
            "🎧 [TOPIC] Listener started for group {}",
            &self.group_id[..16.min(self.group_id.len())]
        );

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
                            tracing::info!("🛑 [TOPIC] Command channel closed for {}", &self.group_id[..16.min(self.group_id.len())]);
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
                            tracing::error!("🔴 [TOPIC] Gossip error: {}", e);
                        }
                        None => {
                            tracing::warn!("⚠️ [TOPIC] Gossip receiver closed for {}", &self.group_id[..16.min(self.group_id.len())]);
                            break;
                        }
                    }
                }
            }
        }

        // CRITICAL: Explicitly drop the sender to leave the gossip topic
        // The gossip network needs time to propagate the leave before we can rejoin
        eprintln!("🛑 [TOPIC] Leaving gossip topic for {}...", &self.group_id[..16.min(self.group_id.len())]);
        drop(self.sender);

        // Small delay to let the leave propagate through the network
        tokio::time::sleep(Duration::from_millis(100)).await;
        eprintln!("✅ [TOPIC] Gossip topic left for {}...", &self.group_id[..16.min(self.group_id.len())]);

        tracing::info!("🛑 [TOPIC] Actor stopped for group {}", &self.group_id[..16.min(self.group_id.len())]);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // COMMAND HANDLING
    // ═══════════════════════════════════════════════════════════════════════

    fn handle_system_command(&self, cmd: SystemCommand) -> bool {
        match cmd {
            SystemCommand::PoisonPill => {
                tracing::info!("💀 [TOPIC] Received PoisonPill for {}", &self.group_id[..16.min(self.group_id.len())]);
                true
            }
            SystemCommand::DumpDiagnostics => {
                tracing::info!(
                    "🩺 [TOPIC] group={}, known_peers={}",
                    &self.group_id[..16.min(self.group_id.len())],
                    self.known_peers.len()
                );
                false
            }
            SystemCommand::PullInFlight => false,
        }
    }

    async fn handle_command(&mut self, cmd: TopicCommand) {
        match cmd {
            TopicCommand::Broadcast(event) => {
                eprintln!("📤 [TOPIC] Broadcasting event: {:?}", std::mem::discriminant(&event));
                eprintln!("   group_id: {}...", &self.group_id[..16.min(self.group_id.len())]);
                eprintln!("   known_peers: {}", self.known_peers.len());
                for peer in &self.known_peers {
                    let peer_str = peer.to_string();
                    eprintln!("     → {}...", &peer_str[..16]);
                }
                match self.broadcast_event(&event).await {
                    Ok(_) => {
                        eprintln!("📤 [TOPIC] ✓ Broadcast sent successfully");
                    }
                    Err(e) => {
                        eprintln!("📤 [TOPIC] 🔴 Broadcast FAILED: {}", e);
                        tracing::error!("🔴 [TOPIC] Broadcast failed: {}", e);
                    }
                }
            }

            TopicCommand::JoinPeers(peers) => {
                let new_peers: Vec<PublicKey> = peers
                    .into_iter()
                    .filter(|p| self.known_peers.insert(*p))
                    .collect();

                if !new_peers.is_empty() {
                    tracing::info!(
                        "🔗 [TOPIC] Adding {} new peers to {}",
                        new_peers.len(),
                        &self.group_id[..16.min(self.group_id.len())]
                    );

                    // CRITICAL: Use join_peers, NOT subscribe_and_join
                    if let Err(e) = self.sender.join_peers(new_peers).await {
                        tracing::error!("🔴 [TOPIC] join_peers failed: {}", e);
                    }
                }
            }

            TopicCommand::DownloadFile { file_id, hash, source_peer, resume_offset } => {
                let endpoint = self.endpoint.clone();
                let event_tx = self.event_tx.clone();
                let group_id = self.group_id.clone();

                tokio::spawn(async move {
                    if let Err(e) = download_file(
                        endpoint,
                        &file_id,
                        &hash,
                        &source_peer,
                        resume_offset,
                        &group_id,
                        event_tx,
                    ).await {
                        tracing::error!("🔴 [TOPIC] File download failed: {}", e);
                    }
                });
            }

            TopicCommand::RequestSnapshot => {
                eprintln!("═══════════════════════════════════════════════════════════════════");
                eprintln!("🗂️ [TOPIC-SNAP-1] TopicActor sending SNAPSHOT REQUEST");
                eprintln!("   group_id: {}...", &self.group_id[..16.min(self.group_id.len())]);
                eprintln!("   from_peer (me): {}...", &self.node_id[..16]);
                eprintln!("   known_peers: {}", self.known_peers.len());
                for peer in &self.known_peers {
                    eprintln!("     peer: {}...", &peer.to_string()[..16]);
                }
                eprintln!("═══════════════════════════════════════════════════════════════════");

                let request = NetworkCommand::RequestSnapshot {
                    from_peer: self.node_id.clone(),
                };

                eprintln!("🗂️ [TOPIC-SNAP-2] Serializing and broadcasting request...");
                if let Ok(data) = serde_json::to_vec(&request) {
                    match self.sender.broadcast(Bytes::from(data)).await {
                        Ok(_) => {
                            eprintln!("🗂️ [TOPIC-SNAP-3] ✓ Snapshot request broadcast SUCCESS");
                            tracing::info!(
                                "🗂️ [TOPIC-SNAP-3] Snapshot request sent for group {}",
                                &self.group_id[..16.min(self.group_id.len())]
                            );
                        }
                        Err(e) => {
                            eprintln!("🗂️ [TOPIC-SNAP-3] 🔴 Snapshot request broadcast FAILED: {}", e);
                            tracing::error!("🔴 [TOPIC-SNAP-3] Snapshot request broadcast failed: {}", e);
                        }
                    }
                } else {
                    eprintln!("🗂️ [TOPIC-SNAP-2a] 🔴 Failed to serialize request");
                }
            }

            TopicCommand::DownloadSnapshot { source_peer } => {
                eprintln!("═══════════════════════════════════════════════════════════════════");
                eprintln!("📥 [SNAP-DL-1] TopicActor downloading snapshot");
                eprintln!("   group_id: {}...", &self.group_id[..16.min(self.group_id.len())]);
                eprintln!("   source_peer: {}...", &source_peer[..16.min(source_peer.len())]);
                eprintln!("═══════════════════════════════════════════════════════════════════");

                let endpoint = self.endpoint.clone();
                let group_id = self.group_id.clone();
                let event_tx = self.event_tx.clone();
                let network_tx = self.network_tx.clone();

                tokio::spawn(async move {
                    match download_snapshot(
                        endpoint,
                        &source_peer,
                        &group_id,
                        event_tx,
                    ).await {
                        Ok(_) => {
                            eprintln!("✅ [SNAP-DL] Snapshot download SUCCESS");
                            let _ = network_tx.send(TopicNetworkCmd::SnapshotComplete {
                                group_id,
                            });
                        }
                        Err(e) => {
                            eprintln!("🔴 [SNAP-DL] Snapshot download FAILED: {}", e);
                            tracing::error!("🔴 [SNAP-DL] Snapshot download failed: {}", e);
                            let _ = network_tx.send(TopicNetworkCmd::SnapshotFailed {
                                group_id,
                                reason: e.to_string(),
                            });
                        }
                    }
                });
            }

            TopicCommand::SetNeedSnapshot(needs) => {
                eprintln!("🗂️ [TOPIC] SetNeedSnapshot({}) for {}...", needs, &self.group_id[..16.min(self.group_id.len())]);
                self.need_snapshot = needs;
                if needs {
                    let _ = self.network_tx.send(TopicNetworkCmd::NeedSnapshot {
                        group_id: self.group_id.clone(),
                    });
                } else {
                    let _ = self.network_tx.send(TopicNetworkCmd::SnapshotComplete {
                        group_id: self.group_id.clone(),
                    });
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

                // Try parsing as NetworkEvent
                if let Ok(evt) = serde_json::from_slice::<NetworkEvent>(&msg.content) {
                    eprintln!("📩 [TOPIC] NetworkEvent from {}... on group {}...: {:?}",
                        &from[..16],
                        &self.group_id[..16.min(self.group_id.len())],
                        std::mem::discriminant(&evt)
                    );
                    self.handle_network_event(evt).await;
                }
                // Try parsing as NetworkCommand (for snapshot requests)
                else if let Ok(cmd) = serde_json::from_slice::<NetworkCommand>(&msg.content) {
                    eprintln!("📩 [TOPIC] NetworkCommand from {}... on group {}...",
                        &from[..16],
                        &self.group_id[..16.min(self.group_id.len())]
                    );
                    self.handle_network_command(cmd, msg.delivered_from).await;
                }
            }

            GossipEvent::NeighborUp(peer) => {
                let peer_str = peer.to_string();
                eprintln!("🟢 [TOPIC] Peer JOINED group {}...: {}...",
                    &self.group_id[..16.min(self.group_id.len())],
                    &peer_str[..16]
                );
                tracing::info!(
                    "🟢 [TOPIC] Peer {} joined group {}",
                    &peer_str[..16],
                    &self.group_id[..16.min(self.group_id.len())]
                );

                self.known_peers.insert(peer);

                // CRITICAL: Re-send snapshot request when peer joins
                // The initial request may have been sent before mesh was ready
                if self.need_snapshot {
                    eprintln!("🗂️ [TOPIC] Peer joined while need_snapshot=true, re-sending request");
                    let request = NetworkCommand::RequestSnapshot {
                        from_peer: self.node_id.clone(),
                    };
                    if let Ok(data) = serde_json::to_vec(&request) {
                        if let Err(e) = self.sender.broadcast(Bytes::from(data)).await {
                            eprintln!("🔴 [TOPIC] Snapshot request broadcast failed: {}", e);
                        } else {
                            eprintln!("🗂️ [TOPIC] ✓ Snapshot request re-sent after peer join");
                        }
                    }
                } else {
                    // We have data - proactively offer snapshot to new peer
                    // This handles asymmetric mesh where peer can't reach us
                    eprintln!("🗂️ [TOPIC] Peer joined and we have data - offering snapshot");
                    let event = NetworkEvent::GroupSnapshotAvailable {
                        source: self.node_id.clone(),
                        group_id: self.group_id.clone(),
                    };
                    if let Err(e) = self.broadcast_event(&event).await {
                        eprintln!("⚠️ [TOPIC] Failed to offer snapshot: {}", e);
                    } else {
                        eprintln!("🗂️ [TOPIC] ✓ Snapshot offer sent to new peer");
                    }
                }

                let _ = self.event_tx.send(SwiftEvent::PeerJoined {
                    group_id: self.group_id.clone(),
                    peer_id: peer_str,
                });
            }

            GossipEvent::NeighborDown(peer) => {
                let peer_str = peer.to_string();
                eprintln!("🔴 [TOPIC] Peer LEFT group {}...: {}...",
                    &self.group_id[..16.min(self.group_id.len())],
                    &peer_str[..16]
                );
                tracing::info!(
                    "🔴 [TOPIC] Peer {} left group {}",
                    &peer_str[..16],
                    &self.group_id[..16.min(self.group_id.len())]
                );

                self.known_peers.remove(&peer);

                let _ = self.event_tx.send(SwiftEvent::PeerLeft {
                    group_id: self.group_id.clone(),
                    peer_id: peer_str,
                });
            }

            GossipEvent::Lagged => {
                eprintln!("⚠️ [TOPIC] LAGGED on group {}... - missed messages!",
                    &self.group_id[..16.min(self.group_id.len())]
                );
                tracing::warn!(
                    "⚠️ [TOPIC] Lagged on group {}",
                    &self.group_id[..16.min(self.group_id.len())]
                );
            }
        }
    }

    async fn handle_network_event(&mut self, evt: NetworkEvent) {
        // Check for GroupSnapshotAvailable - this triggers snapshot download
        if let NetworkEvent::GroupSnapshotAvailable { ref source, ref group_id } = evt {
            eprintln!("═══════════════════════════════════════════════════════════════════");
            eprintln!("📬 [SNAP-AVAIL-1] Received GroupSnapshotAvailable");
            eprintln!("   source: {}...", &source[..16.min(source.len())]);
            eprintln!("   group_id: {}...", &group_id[..16.min(group_id.len())]);
            eprintln!("   my_node_id: {}...", &self.node_id[..16]);
            eprintln!("═══════════════════════════════════════════════════════════════════");

            // Don't download from ourselves
            if source == &self.node_id {
                eprintln!("📬 [SNAP-AVAIL-2] Ignoring - this is our own SnapshotAvailable");
                return;
            }

            // Check if we need a snapshot
            eprintln!("📬 [SNAP-AVAIL-3] need_snapshot flag: {}", self.need_snapshot);

            if self.need_snapshot {
                eprintln!("📬 [SNAP-AVAIL-4] ✓ We need snapshot - triggering download");

                // Mark as no longer needing (prevent duplicate downloads)
                self.need_snapshot = false;

                // Spawn download task
                let endpoint = self.endpoint.clone();
                let group_id = self.group_id.clone();
                let source_peer = source.clone();
                let event_tx = self.event_tx.clone();
                let network_tx = self.network_tx.clone();

                tokio::spawn(async move {
                    eprintln!("📥 [SNAP-DL-SPAWN] Download task started for {}...", &source_peer[..16]);
                    match download_snapshot(
                        endpoint,
                        &source_peer,
                        &group_id,
                        event_tx,
                    ).await {
                        Ok(_) => {
                            eprintln!("✅ [SNAP-DL-SPAWN] Download SUCCESS");
                            let _ = network_tx.send(TopicNetworkCmd::SnapshotComplete { group_id });
                        }
                        Err(e) => {
                            eprintln!("🔴 [SNAP-DL-SPAWN] Download FAILED: {}", e);
                            tracing::error!("🔴 [SNAP-DL] Snapshot download failed: {}", e);
                            let _ = network_tx.send(TopicNetworkCmd::SnapshotFailed {
                                group_id,
                                reason: e.to_string(),
                            });
                        }
                    }
                });
            } else {
                eprintln!("📬 [SNAP-AVAIL-4] ✗ We don't need snapshot - ignoring");
            }

            return; // Don't persist/forward SnapshotAvailable events
        }

        tracing::debug!(
            "📩 [TOPIC] Received event on group {}: {:?}",
            &self.group_id[..16.min(self.group_id.len())],
            std::mem::discriminant(&evt)
        );

        // Persist to DB
        Self::persist_event(&evt);

        // Forward to Swift
        eprintln!("📤 [TOPIC→SWIFT] Forwarding event: {:?}", std::mem::discriminant(&evt));
        let _ = self.event_tx.send(SwiftEvent::Network(evt));
    }

    async fn handle_network_command(&self, cmd: NetworkCommand, from_peer: PublicKey) {
        match cmd {
            NetworkCommand::RequestSnapshot { from_peer: requester } => {
                // Don't respond to our own requests
                if requester == self.node_id {
                    return;
                }

                eprintln!("═══════════════════════════════════════════════════════════════════");
                eprintln!("🟣 [TOPIC] SNAPSHOT REQUEST RECEIVED");
                eprintln!("   from_peer: {}...", &requester[..16]);
                eprintln!("   for_group: {}...", &self.group_id[..16.min(self.group_id.len())]);
                eprintln!("═══════════════════════════════════════════════════════════════════");

                tracing::info!(
                    "🟣 [TOPIC] Snapshot request from {} for group {}",
                    &requester[..16],
                    &self.group_id[..16.min(self.group_id.len())]
                );

                // Note: Actual snapshot sending is handled by NetworkActor's
                // protocol acceptor when peer connects with SNAPSHOT_ALPN
                // We just acknowledge via a SnapshotAvailable event
                let event = NetworkEvent::GroupSnapshotAvailable {
                    source: self.node_id.clone(),
                    group_id: self.group_id.clone(),
                };

                eprintln!("   → Broadcasting SnapshotAvailable response");

                if let Err(e) = self.broadcast_event(&event).await {
                    eprintln!("🔴 [TOPIC] SnapshotAvailable broadcast FAILED: {}", e);
                    tracing::error!("🔴 [TOPIC] SnapshotAvailable broadcast failed: {}", e);
                } else {
                    eprintln!("   ✓ SnapshotAvailable broadcast sent");
                }
            }
            _ => {}
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // BROADCAST HELPER
    // ═══════════════════════════════════════════════════════════════════════

    async fn broadcast_event(&self, event: &NetworkEvent) -> Result<()> {
        let data = serde_json::to_vec(event)?;
        self.sender.broadcast(Bytes::from(data)).await?;

        tracing::debug!(
            "📤 [TOPIC] Broadcast event to group {}",
            &self.group_id[..16.min(self.group_id.len())]
        );

        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DB PERSISTENCE (static - uses storage module)
    // ═══════════════════════════════════════════════════════════════════════

    fn persist_event(evt: &NetworkEvent) {
        use crate::models::dto::{NotebookCellDTO, WhiteboardElementDTO};

        match evt {
            NetworkEvent::GroupCreated(g) => {
                let _ = storage::group_insert(g);
            }
            NetworkEvent::GroupRenamed { id, name } => {
                let _ = storage::group_rename(id, name);
            }
            NetworkEvent::GroupDeleted { id } => {
                let _ = storage::group_delete(id);
            }
            NetworkEvent::GroupDissolved { id } => {
                // Owner dissolved the group - delete locally
                let _ = storage::group_delete(id);
            }
            NetworkEvent::WorkspaceCreated(ws) => {
                eprintln!("💾 [PERSIST] WorkspaceCreated:");
                eprintln!("   workspace_id: {}...", &ws.id[..16.min(ws.id.len())]);
                eprintln!("   group_id: {}...", &ws.group_id[..16.min(ws.group_id.len())]);
                eprintln!("   name: {}", ws.name);
                match storage::workspace_insert(ws) {
                    Ok(_) => eprintln!("💾 [PERSIST] ✓ Workspace inserted to DB"),
                    Err(e) => eprintln!("💾 [PERSIST] 🔴 Workspace insert FAILED: {}", e),
                }
            }
            NetworkEvent::WorkspaceRenamed { id, name } => {
                let _ = storage::workspace_rename(id, name);
            }
            NetworkEvent::WorkspaceDeleted { id } => {
                let _ = storage::workspace_delete(id);
            }
            NetworkEvent::WorkspaceDissolved { id } => {
                // Owner dissolved the workspace - delete locally
                let _ = storage::workspace_delete(id);
            }
            NetworkEvent::BoardCreated { id, workspace_id, name, created_at } => {
                eprintln!("💾 [PERSIST] BoardCreated:");
                eprintln!("   board_id: {}...", &id[..16.min(id.len())]);
                eprintln!("   workspace_id: {}...", &workspace_id[..16.min(workspace_id.len())]);
                eprintln!("   name: {}", name);
                match storage::board_insert(id, workspace_id, name, *created_at) {
                    Ok(_) => eprintln!("💾 [PERSIST] ✓ Board inserted to DB"),
                    Err(e) => eprintln!("💾 [PERSIST] 🔴 Board insert FAILED: {}", e),
                }
            }
            NetworkEvent::BoardRenamed { id, name } => {
                let _ = storage::board_rename(id, name);
            }
            NetworkEvent::BoardDeleted { id } => {
                let _ = storage::board_delete(id);
            }
            NetworkEvent::BoardDissolved { id } => {
                // Owner dissolved the board - delete locally
                let _ = storage::board_delete(id);
            }
            NetworkEvent::FileAvailable { id, group_id, workspace_id, board_id, name, hash, size, source_peer, created_at } => {
                eprintln!("💾 [PERSIST] FileAvailable:");
                eprintln!("   file_id: {}...", &id[..16.min(id.len())]);
                eprintln!("   name: {}", name);
                eprintln!("   group_id: {:?}", group_id.as_ref().map(|g| &g[..16.min(g.len())]));
                eprintln!("   workspace_id: {:?}", workspace_id.as_ref().map(|w| &w[..16.min(w.len())]));
                match storage::file_insert(
                    id,
                    group_id.as_deref(),
                    workspace_id.as_deref(),
                    board_id.as_deref(),
                    name,
                    hash,
                    *size,
                    source_peer,
                    *created_at,
                ) {
                    Ok(_) => eprintln!("💾 [PERSIST] ✓ File inserted to DB"),
                    Err(e) => eprintln!("💾 [PERSIST] 🔴 File insert FAILED: {}", e),
                }
            }
            NetworkEvent::ChatSent { id, workspace_id, message, author, parent_id, timestamp } => {
                eprintln!("💾 [PERSIST] ChatSent:");
                eprintln!("   chat_id: {}...", &id[..16.min(id.len())]);
                eprintln!("   workspace_id: {}...", &workspace_id[..16.min(workspace_id.len())]);
                eprintln!("   author: {}...", &author[..16.min(author.len())]);
                match storage::chat_insert(id, workspace_id, message, author, parent_id.as_deref(), *timestamp) {
                    Ok(_) => eprintln!("💾 [PERSIST] ✓ Chat inserted to DB"),
                    Err(e) => eprintln!("💾 [PERSIST] 🔴 Chat insert FAILED: {}", e),
                }
            }
            NetworkEvent::ChatDeleted { id } => {
                let _ = storage::chat_delete(id);
            }
            NetworkEvent::WhiteboardElementAdded {
                id, board_id, element_type, x, y, width, height, z_index,
                style_json, content_json, created_at, updated_at,
            } => {
                let dto = WhiteboardElementDTO {
                    id: id.clone(),
                    board_id: board_id.clone(),
                    element_type: element_type.clone(),
                    x: *x,
                    y: *y,
                    width: *width,
                    height: *height,
                    z_index: *z_index,
                    style_json: style_json.clone(),
                    content_json: content_json.clone(),
                    created_at: *created_at,
                    updated_at: *updated_at,
                };
                let _ = storage::element_insert(&dto);
            }
            NetworkEvent::WhiteboardElementUpdated {
                id, board_id, element_type, x, y, width, height, z_index,
                style_json, content_json, updated_at,
            } => {
                let dto = WhiteboardElementDTO {
                    id: id.clone(),
                    board_id: board_id.clone(),
                    element_type: element_type.clone(),
                    x: *x,
                    y: *y,
                    width: *width,
                    height: *height,
                    z_index: *z_index,
                    style_json: style_json.clone(),
                    content_json: content_json.clone(),
                    created_at: 0, // not used in update
                    updated_at: *updated_at,
                };
                let _ = storage::element_update(&dto);
            }
            NetworkEvent::WhiteboardElementDeleted { id, .. } => {
                let _ = storage::element_delete(id);
            }
            NetworkEvent::WhiteboardCleared { board_id } => {
                let _ = storage::element_clear_board(board_id);
            }
            NetworkEvent::NotebookCellAdded { id, board_id, cell_type, cell_order, content } => {
                let _ = storage::cell_insert(id, board_id, cell_type, *cell_order, content.as_deref());
            }
            NetworkEvent::NotebookCellUpdated {
                id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json,
            } => {
                let dto = NotebookCellDTO {
                    id: id.clone(),
                    board_id: board_id.clone(),
                    cell_type: cell_type.clone(),
                    cell_order: *cell_order,
                    content: content.clone(),
                    output: output.clone(),
                    collapsed: *collapsed,
                    height: *height,
                    metadata_json: metadata_json.clone(),
                    created_at: 0, // not used in update
                    updated_at: 0, // set by storage layer
                };
                let _ = storage::cell_update(&dto);
            }
            NetworkEvent::NotebookCellDeleted { id, .. } => {
                let _ = storage::cell_delete(id);
            }
            NetworkEvent::NotebookCellsReordered { board_id, cell_ids } => {
                let _ = storage::cell_reorder(board_id, cell_ids);
            }
            NetworkEvent::ProfileUpdated { node_id, display_name, avatar_hash } => {
                let _ = storage::profile_upsert(node_id, display_name, avatar_hash.as_deref());
            }
            _ => {}
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FILE DOWNLOAD CLIENT
// ═══════════════════════════════════════════════════════════════════════════

async fn download_file(
    endpoint: Endpoint,
    file_id: &str,
    hash: &str,
    source_peer: &str,
    resume_offset: u64,
    group_id: &str,
    event_tx: UnboundedSender<SwiftEvent>,
) -> Result<()> {
    let pk = PublicKey::from_str(source_peer)?;

    tracing::info!(
        "📥 [FILE] Downloading {} from {} (offset: {})",
        &file_id[..16.min(file_id.len())],
        &source_peer[..16],
        resume_offset
    );

    // Emit progress start
    let _ = event_tx.send(SwiftEvent::FileDownloadProgress {
        file_id: file_id.to_string(),
        progress: 0.0,
    });

    // Connect to peer
    let conn: Connection = tokio::time::timeout(
        Duration::from_secs(30),
        endpoint.connect(pk, FILE_TRANSFER_ALPN)
    ).await
        .map_err(|_| anyhow!("File transfer connection timeout"))?
        .map_err(|e| anyhow!("File transfer connect failed: {}", e))?;

    let (mut send, mut recv) = conn.open_bi().await?;

    // Send request
    let request = FileTransferMsg::Request {
        file_id: file_id.to_string(),
        hash: hash.to_string(),
        offset: resume_offset,
    };

    let req_data = serde_json::to_vec(&request)?;
    let len = (req_data.len() as u32).to_be_bytes();
    send.write_chunk(Bytes::copy_from_slice(&len)).await?;
    send.write_chunk(Bytes::from(req_data)).await?;

    // Read header
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut header_data = vec![0u8; len];
    recv.read_exact(&mut header_data).await?;

    let response: FileTransferMsg = serde_json::from_slice(&header_data)?;

    match response {
        FileTransferMsg::Header { file_id, file_name, total_size, hash, byte_offset, byte_length } => {
            tracing::info!(
                "📥 [FILE] Receiving {} ({} bytes from offset {})",
                file_name,
                byte_length,
                byte_offset
            );

            // Create temp file for download
            let data_dir = crate::DATA_DIR.get().cloned().unwrap_or_else(|| std::path::PathBuf::from("."));
            let downloads_dir = data_dir.join("downloads");
            std::fs::create_dir_all(&downloads_dir)?;

            let temp_path = downloads_dir.join(format!("{}.tmp", file_id));
            let final_path = downloads_dir.join(&file_name);

            // Open file for writing (append if resuming)
            let mut file = if resume_offset > 0 && temp_path.exists() {
                tokio::fs::OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&temp_path)
                    .await?
            } else {
                tokio::fs::File::create(&temp_path).await?
            };

            // Update transfer tracking
            let _ = storage::transfer_upsert(
                &file_id,
                &file_name,
                total_size,
                &hash,
                byte_offset,
                temp_path.to_string_lossy().as_ref(),
                source_peer,
                "in_progress",
            );

            // Receive file data using read_chunk
            use tokio::io::AsyncWriteExt;
            let mut received = byte_offset;
            let target = byte_offset + byte_length;

            while received < target {
                let remaining = (target - received) as usize;
                let chunk_size = remaining.min(64 * 1024);

                match recv.read_chunk(chunk_size, true).await? {
                    Some(chunk) => {
                        file.write_all(&chunk.bytes).await?;
                        received += chunk.bytes.len() as u64;

                        // Update progress every 256KB
                        if received % (256 * 1024) < chunk.bytes.len() as u64 {
                            let _ = storage::transfer_update_progress(&file_id, received);
                            let progress = received as f64 / total_size as f64;
                            let _ = event_tx.send(SwiftEvent::FileDownloadProgress {
                                file_id: file_id.clone(),
                                progress,
                            });
                        }
                    }
                    None => {
                        return Err(anyhow!("Stream ended before complete file"));
                    }
                }
            }

            file.flush().await?;
            drop(file);

            // Verify hash
            let file_data = tokio::fs::read(&temp_path).await?;
            let actual_hash = blake3::hash(&file_data).to_hex().to_string();

            if actual_hash != hash {
                let _ = storage::transfer_set_status(&file_id, "hash_mismatch");
                return Err(anyhow!("Hash mismatch: expected {}, got {}", hash, actual_hash));
            }

            // Move to final location
            tokio::fs::rename(&temp_path, &final_path).await?;

            // Update DB
            let _ = storage::file_set_local_path(&file_id, final_path.to_string_lossy().as_ref());
            let _ = storage::transfer_set_status(&file_id, "complete");

            tracing::info!("✅ [FILE] Download complete: {}", file_name);

            let _ = event_tx.send(SwiftEvent::FileDownloaded {
                file_id,
                local_path: final_path.to_string_lossy().to_string(),
            });
        }

        FileTransferMsg::NotFound { file_id } => {
            tracing::warn!("⚠️ [FILE] File not found on peer: {}", &file_id[..16.min(file_id.len())]);
            let _ = event_tx.send(SwiftEvent::FileDownloadFailed {
                file_id,
                error: "File not found on peer".to_string(),
            });
        }

        _ => {
            return Err(anyhow!("Unexpected response from file server"));
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// SNAPSHOT DOWNLOAD
// ═══════════════════════════════════════════════════════════════════════════

/// Download a snapshot from a peer
async fn download_snapshot(
    endpoint: Endpoint,
    source_peer: &str,
    group_id: &str,
    event_tx: UnboundedSender<SwiftEvent>,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    eprintln!("═══════════════════════════════════════════════════════════════════");
    eprintln!("📥 [SNAP-DL-1] Starting snapshot download");
    eprintln!("   source_peer: {}...", &source_peer[..16.min(source_peer.len())]);
    eprintln!("   group_id: {}...", &group_id[..16.min(group_id.len())]);
    eprintln!("═══════════════════════════════════════════════════════════════════");

    // Emit status update
    let _ = event_tx.send(SwiftEvent::StatusUpdate {
        message: "Connecting to peer for sync...".to_string(),
    });

    // Parse peer public key
    let pk = PublicKey::from_str(source_peer)
        .map_err(|e| anyhow!("Invalid peer ID: {}", e))?;

    eprintln!("📥 [SNAP-DL-2] Connecting to peer with SNAPSHOT_ALPN...");

    // Connect to peer with snapshot protocol
    let conn: Connection = tokio::time::timeout(
        Duration::from_secs(30),
        endpoint.connect(pk, SNAPSHOT_ALPN)
    ).await
        .map_err(|_| anyhow!("Snapshot connection timeout"))?
        .map_err(|e| anyhow!("Snapshot connect failed: {}", e))?;

    eprintln!("📥 [SNAP-DL-3] ✓ Connected! Opening bidirectional stream...");

    let (mut send, mut recv) = conn.open_bi().await?;

    eprintln!("📥 [SNAP-DL-4] Sending group_id request...");

    // Send group_id request
    let group_id_bytes = group_id.as_bytes();
    let len = (group_id_bytes.len() as u32).to_be_bytes();
    send.write_all(&len).await?;
    send.write_all(group_id_bytes).await?;
    send.flush().await?;

    eprintln!("📥 [SNAP-DL-5] ✓ Request sent. Waiting for snapshot frames...");

    let _ = event_tx.send(SwiftEvent::StatusUpdate {
        message: "Receiving snapshot data...".to_string(),
    });

    // Receive frames until Complete
    let mut frame_count = 0;
    let mut structure_received = false;

    loop {
        frame_count += 1;
        eprintln!("📥 [SNAP-DL-6.{}] Reading frame...", frame_count);

        // Read frame length
        let mut len_buf = [0u8; 4];
        if let Err(e) = recv.read_exact(&mut len_buf).await {
            if frame_count == 1 {
                return Err(anyhow!("Failed to read first frame: {}", e));
            }
            eprintln!("📥 [SNAP-DL-6.{}] Stream ended ({})", frame_count, e);
            break;
        }
        let frame_len = u32::from_be_bytes(len_buf) as usize;

        eprintln!("📥 [SNAP-DL-6.{}] Frame length: {} bytes", frame_count, frame_len);

        if frame_len > 10 * 1024 * 1024 {
            return Err(anyhow!("Frame too large: {} bytes", frame_len));
        }

        // Read frame data
        let mut frame_data = vec![0u8; frame_len];
        recv.read_exact(&mut frame_data).await?;

        // Parse frame
        let frame: SnapshotFrame = serde_json::from_slice(&frame_data)
            .map_err(|e| anyhow!("Failed to parse snapshot frame: {}", e))?;

        match frame {
            SnapshotFrame::Structure { group, workspaces, boards } => {
                eprintln!("📥 [SNAP-DL-7] STRUCTURE frame received:");
                eprintln!("   group: {} ({})", group.name, &group.id[..16.min(group.id.len())]);
                eprintln!("   workspaces: {}", workspaces.len());
                eprintln!("   boards: {}", boards.len());

                // Emit sync started
                let _ = event_tx.send(SwiftEvent::SyncStarted {
                    group_id: group.id.clone(),
                    group_name: group.name.clone(),
                });

                // Insert structure into DB
                eprintln!("📥 [SNAP-DL-7a] Inserting structure into DB...");

                // Insert/update group
                if let Err(e) = storage::group_insert_simple(&group.id, &group.name, &group.icon, &group.color) {
                    eprintln!("   ⚠️ Group insert: {}", e);
                }

                // Insert workspaces
                for w in &workspaces {
                    if let Err(e) = storage::workspace_insert_simple(&w.id, &w.group_id, &w.name) {
                        eprintln!("   ⚠️ Workspace insert: {}", e);
                    }
                }

                // Insert boards
                for b in &boards {
                    if let Err(e) = storage::board_insert_simple(&b.id, &b.workspace_id, &b.name, b.created_at) {
                        eprintln!("   ⚠️ Board insert: {}", e);
                    }
                }

                eprintln!("📥 [SNAP-DL-7b] ✓ Structure inserted");
                structure_received = true;

                // Emit structure received
                let _ = event_tx.send(SwiftEvent::SyncStructureReceived {
                    group_id: group_id.to_string(),
                    workspace_count: workspaces.len() as u32,
                    board_count: boards.len() as u32,
                });
            }

            SnapshotFrame::Content { elements, cells } => {
                eprintln!("📥 [SNAP-DL-8] CONTENT frame received:");
                eprintln!("   elements: {}", elements.len());
                eprintln!("   cells: {}", cells.len());

                // Insert elements
                for elem in &elements {
                    if let Err(e) = storage::element_insert_simple(
                        &elem.id, &elem.board_id, &elem.element_type,
                        elem.x, elem.y, elem.width, elem.height, elem.z_index,
                        elem.style_json.as_deref(), elem.content_json.as_deref(),
                        elem.created_at, elem.updated_at,
                    ) {
                        eprintln!("   ⚠️ Element insert: {}", e);
                    }
                }

                // Insert cells
                for cell in &cells {
                    if let Err(e) = storage::cell_insert_simple(
                        &cell.id, &cell.board_id, &cell.cell_type, cell.cell_order,
                        cell.content.as_deref(), cell.output.as_deref(), cell.collapsed,
                        cell.height, cell.metadata_json.as_deref(),
                        cell.created_at, cell.updated_at,
                    ) {
                        eprintln!("   ⚠️ Cell insert: {}", e);
                    }
                }

                eprintln!("📥 [SNAP-DL-8a] ✓ Content inserted");

                // Emit board ready
                let _ = event_tx.send(SwiftEvent::SyncBoardReady {
                    board_id: "all".to_string(),
                    element_count: elements.len() as u32,
                    cell_count: cells.len() as u32,
                });
            }

            SnapshotFrame::Metadata { chats, files, integrations, board_metadata } => {
                eprintln!("📥 [SNAP-DL-9] METADATA frame received:");
                eprintln!("   chats: {}", chats.len());
                eprintln!("   files: {}", files.len());
                eprintln!("   integrations: {}", integrations.len());
                eprintln!("   board_metadata: {}", board_metadata.len());

                // Insert chats
                for chat in &chats {
                    if let Err(e) = storage::chat_insert_simple(
                        &chat.id, &chat.workspace_id, &chat.message,
                        &chat.author, chat.parent_id.as_deref(), chat.timestamp,
                    ) {
                        eprintln!("   ⚠️ Chat insert: {}", e);
                    }
                }

                // Insert files (metadata only)
                for file in &files {
                    if let Err(e) = storage::file_insert_simple(
                        &file.id, file.group_id.as_deref(), file.workspace_id.as_deref(),
                        file.board_id.as_deref(), &file.name, &file.hash, file.size,
                        file.source_peer.as_deref(), file.created_at,
                    ) {
                        eprintln!("   ⚠️ File insert: {}", e);
                    }
                }

                // Insert integrations
                for integ in &integrations {
                    if let Err(e) = storage::integration_insert(
                        &integ.id, &integ.scope_type, &integ.scope_id, &integ.integration_type,
                        &integ.config, integ.created_at,
                    ) {
                        eprintln!("   ⚠️ Integration insert: {}", e);
                    }
                }

                // Insert board metadata
                for meta in &board_metadata {
                    if let Err(e) = storage::board_metadata_upsert(
                        &meta.board_id, &meta.labels, meta.rating, meta.view_count,
                        meta.contains_model.as_deref(), &meta.contains_skills,
                        Some(&meta.board_type), meta.last_accessed, meta.is_pinned,
                    ) {
                        eprintln!("   ⚠️ Board metadata insert: {}", e);
                    }
                }

                eprintln!("📥 [SNAP-DL-9a] ✓ Metadata inserted");

                // Emit files received
                if !files.is_empty() {
                    let _ = event_tx.send(SwiftEvent::SyncFilesReceived {
                        group_id: group_id.to_string(),
                        file_count: files.len() as u32,
                    });
                }
            }

            SnapshotFrame::Complete => {
                eprintln!("═══════════════════════════════════════════════════════════════════");
                eprintln!("✅ [SNAP-DL-10] COMPLETE frame received - snapshot download SUCCESS");
                eprintln!("═══════════════════════════════════════════════════════════════════");

                // Emit sync complete (caller will notify NetworkActor)
                let _ = event_tx.send(SwiftEvent::SyncComplete {
                    group_id: group_id.to_string(),
                });

                let _ = event_tx.send(SwiftEvent::StatusUpdate {
                    message: "Sync complete!".to_string(),
                });

                break;
            }
        }
    }

    if !structure_received {
        return Err(anyhow!("No structure frame received"));
    }

    eprintln!("📥 [SNAP-DL-11] Snapshot download finished, {} frames processed", frame_count);

    Ok(())
}