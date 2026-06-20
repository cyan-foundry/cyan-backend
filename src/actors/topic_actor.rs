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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
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
    anti_entropy::{self, AntiEntropyMsg},
    bootstrap_node_id,
    identity::{MeshAuthorizer, WriteDecision},
    models::{
        commands::NetworkCommand,
        events::{NetworkEvent, SwiftEvent},
        protocol::{FileTransferMsg, SnapshotFrame},
    },
    storage,
    swarm::{BlobSwarm, Hash, SwarmMessage},
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
    /// Announce (over gossip) that this node holds the blob with this Blake3 hash (`IHave`).
    AnnounceBlob { hash: String },
    /// Ask the group (over gossip) who holds the blob with this Blake3 hash (`WhoHas`).
    QueryBlob { hash: String },
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

    /// Shared blob swarm (G10): incoming i-have/who-has gossip is fed to `swarm.on_message`, and
    /// `AnnounceBlob`/`QueryBlob` commands broadcast the negotiation messages onto this group topic.
    swarm: Arc<BlobSwarm>,

    /// Per-node mesh-write authority (identity/RBAC mesh half), shared with the NetworkActor.
    /// Inbound writes from a peer are gated by `authorize_write(group, from_peer)`. Fail-open
    /// unless this group has been enforced — so it does not change behavior for un-enforced groups.
    authorizer: Arc<std::sync::Mutex<MeshAuthorizer>>,

    /// The signed capability-grant QR payload this node scanned to join the group (if any).
    /// Presented to the snapshot holder so it can authorize the per-group snapshot read when the
    /// group is enforced. `None` for groups created locally or joined without a grant (fail-open).
    grant: Option<String>,

    /// Anti-entropy repair debounce: at most one repair pull in flight per group, so a divergent
    /// digest seen from several peers in one sweep triggers a single merge, not a thundering pull.
    /// Shared with the spawned pull task, which clears it on completion. Bounds repair traffic.
    repairing: Arc<AtomicBool>,

    /// Multi-source snapshot pick: snapshot offers (`GroupSnapshotAvailable` sources) collected for
    /// a short window before a holder is chosen at random, so concurrent cold-joiners spread across
    /// holders instead of all hammering the host (the "snapshot under load" fix).
    snapshot_offers: HashSet<String>,

    /// Deadline at which the collected `snapshot_offers` are resolved into one pick. `None` when no
    /// pick is pending.
    snapshot_pick_at: Option<Instant>,

    /// Drain offload: inbound, authorized `NetworkEvent`s are handed to a single FIFO worker that
    /// does the SQLite write + Swift forward, so the gossip select loop is never blocked on disk I/O
    /// and drains the receiver promptly (makes `Lagged` rarer). FIFO preserves per-id ordering.
    persist_tx: UnboundedSender<NetworkEvent>,
}

impl TopicActor {
    /// Spawn a new TopicActor for a group
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        node_id: String,
        group_id: String,
        endpoint: Endpoint,
        gossip: Arc<Gossip>,
        initial_peers: Vec<PublicKey>,
        network_tx: UnboundedSender<TopicNetworkCmd>,
        event_tx: UnboundedSender<SwiftEvent>,
        swarm: Arc<BlobSwarm>,
        authorizer: Arc<std::sync::Mutex<MeshAuthorizer>>,
        grant: Option<String>,
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

        // Drain offload: a single FIFO worker persists authorized NetworkEvents off the select loop.
        // One consumer ⇒ per-id ordering preserved (an add then an update for the same id stay
        // ordered); same SwiftEvents emitted in the same order, so the FFI event stream is unchanged.
        let (persist_tx, mut persist_rx) = mpsc::unbounded_channel::<NetworkEvent>();
        let persist_event_tx = event_tx.clone();
        tokio::spawn(async move {
            while let Some(evt) = persist_rx.recv().await {
                Self::persist_event(&evt);
                let _ = persist_event_tx.send(SwiftEvent::Network(evt));
            }
        });

        let actor = Self {
            node_id: node_id.clone(),
            group_id: group_id.clone(),
            endpoint,
            sender,
            known_peers: peers.into_iter().collect(),
            need_snapshot: true,  // New groups always need snapshot
            network_tx,
            event_tx,
            swarm,
            authorizer,
            grant,
            repairing: Arc::new(AtomicBool::new(false)),
            snapshot_offers: HashSet::new(),
            snapshot_pick_at: None,
            persist_tx,
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

        // Anti-entropy sweep clock: gossip our per-group digest on a bounded, jittered cadence so
        // peers detect + repair anything live gossip dropped (the convergence guarantee).
        let mut next_sweep = Instant::now() + anti_entropy::jittered_sweep();

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

                // Anti-entropy sweep: broadcast this group's state digest, then re-arm the jittered
                // clock. The branch never blocks the others — it just fires on its absolute deadline.
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_sweep)) => {
                    self.do_sweep().await;
                    next_sweep = Instant::now() + anti_entropy::jittered_sweep();
                }

                // Multi-source snapshot pick: when the offer-collection window elapses, choose one
                // holder at random from the offers gathered and start the join-time snapshot pull.
                _ = async {
                    match self.snapshot_pick_at {
                        Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if self.snapshot_pick_at.is_some() => {
                    self.commit_snapshot_pick().await;
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

            TopicCommand::AnnounceBlob { hash } => {
                // Broadcast an `IHave` for a blob this node holds onto the group topic (G10).
                match crate::swarm::Hash::from_str(&hash) {
                    Ok(h) => self.broadcast_swarm_message(&self.swarm.announce(&h)).await,
                    Err(e) => tracing::warn!("⚠️ [TOPIC] AnnounceBlob bad hash {}: {}", hash, e),
                }
            }

            TopicCommand::QueryBlob { hash } => {
                // Broadcast a `WhoHas` query onto the group topic; holders reply with `IHave` (G10).
                match crate::swarm::Hash::from_str(&hash) {
                    Ok(h) => self.broadcast_swarm_message(&self.swarm.query(&h)).await,
                    Err(e) => tracing::warn!("⚠️ [TOPIC] QueryBlob bad hash {}: {}", hash, e),
                }
            }

            TopicCommand::DownloadFile { file_id, hash, source_peer, resume_offset } => {
                let endpoint = self.endpoint.clone();
                let event_tx = self.event_tx.clone();
                let group_id = self.group_id.clone();

                // Content-addressed swarm path (G10): if this node already knows holders for the
                // blob (learned via `IHave` gossip — e.g. a `.cyanplugin` seeded by the uploader),
                // fetch it multi-source with churn/resume + Blake3 verify. Falls back to the existing
                // single-source file transfer when no holders are known, so non-swarm files (and
                // plugins before any IHave is seen) behave exactly as before.
                let swarm = self.swarm.clone();
                tokio::spawn(async move {
                    let holders = match Hash::from_str(&hash) {
                        Ok(h) => swarm.holders(&h).await,
                        Err(_) => Vec::new(),
                    };
                    if !holders.is_empty() {
                        match swarm_download_file(&swarm, &file_id, &hash, &holders, event_tx.clone())
                            .await
                        {
                            Ok(()) => return,
                            Err(e) => {
                                tracing::warn!(
                                    "⚠️ [TOPIC] swarm fetch for {} failed ({}); falling back to direct transfer",
                                    &file_id[..16.min(file_id.len())],
                                    e
                                );
                            }
                        }
                    }
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
                let grant = self.grant.clone();

                tokio::spawn(async move {
                    match download_snapshot(
                        endpoint,
                        &source_peer,
                        &group_id,
                        grant.as_deref(),
                        event_tx,
                        false,
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

                // Observability only (stress fabric "no message storm" oracle): count every
                // inbound, non-self gossip message this node actually processes. Behavior-neutral.
                crate::metrics::record_gossip_recv();

                // Try parsing as a blob-swarm negotiation message FIRST (G10): its `{"type":"IHave"
                // |"WhoHas"}` shape is disjoint from NetworkEvent/NetworkCommand. Record the holder /
                // answer a WhoHas with our own IHave, broadcast over this same group topic.
                if let Ok(reply) = self.swarm.on_message(&msg.content).await {
                    if let Some(reply) = reply {
                        self.broadcast_swarm_message(&reply).await;
                    }
                    return;
                }

                // Anti-entropy digest (`{"type":"Digest"}`) — disjoint from every other gossip
                // shape. A divergent digest from a peer not behind us triggers a quiet repair pull.
                if let Ok(ae) = serde_json::from_slice::<AntiEntropyMsg>(&msg.content) {
                    self.handle_anti_entropy(ae).await;
                    return;
                }

                // Try parsing as NetworkEvent
                if let Ok(evt) = serde_json::from_slice::<NetworkEvent>(&msg.content) {
                    eprintln!("📩 [TOPIC] NetworkEvent from {}... on group {}...: {:?}",
                        &from[..16],
                        &self.group_id[..16.min(self.group_id.len())],
                        std::mem::discriminant(&evt)
                    );
                    self.handle_network_event(evt, &from).await;
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
                crate::metrics::record_neighbor_up(); // observability only (gossip-degree gauge)

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
                self.emit_presence();
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
                crate::metrics::record_neighbor_down(); // observability only (gossip-degree gauge)

                let _ = self.event_tx.send(SwiftEvent::PeerLeft {
                    group_id: self.group_id.clone(),
                    peer_id: peer_str,
                });
                self.emit_presence();
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

    async fn handle_network_event(&mut self, evt: NetworkEvent, from_peer: &str) {
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
                // Multi-source snapshot serving (the "snapshot under load" fix): instead of pulling
                // from whichever holder happens to answer first (always the host → thundering herd),
                // collect offers over a short jittered window and then pick one holder at random
                // (`commit_snapshot_pick`). Many concurrent cold-joiners thus spread across all
                // holders rather than overloading a single host.
                eprintln!("📬 [SNAP-AVAIL-4] ✓ We need snapshot - recording holder offer");
                self.snapshot_offers.insert(source.clone());
                if self.snapshot_pick_at.is_none() {
                    self.snapshot_pick_at = Some(Instant::now() + anti_entropy::jittered_pick_window());
                }
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

        // MESH-WRITE ENFORCEMENT (identity/RBAC mesh half). An inbound NetworkEvent is a write to
        // group state, so it must come from a peer this node has authorized via a valid capability
        // grant. Fail-open until the group is enforced (`MeshAuthorizer::enforce_group`) — so this
        // is inert for groups that have not opted into grant enforcement. Decided on the RECEIVER's
        // own authorizer state; refused writes are NOT persisted or forwarded to Swift.
        let decision = self
            .authorizer
            .lock()
            .map(|a| a.authorize_write(&self.group_id, from_peer))
            .unwrap_or(WriteDecision::Allow);
        if let WriteDecision::Deny(reason) = decision {
            // Flat obs (tenant = group_id) at the refusal point — never a substitute for the
            // assertion oracle (tests assert on the authorizer state), just operator visibility.
            tracing::warn!(
                target: "obs",
                tenant = %self.group_id,
                peer = %from_peer,
                action = "mesh_write",
                decision = "deny",
                reason = ?reason,
                "refused mesh write from peer without a valid capability grant"
            );
            eprintln!(
                "⛔ [TOPIC] Refused mesh write on group {}... from {}... ({:?})",
                &self.group_id[..16.min(self.group_id.len())],
                &from_peer[..16.min(from_peer.len())],
                reason
            );
            return;
        }

        // Persist + forward off the select loop (single FIFO worker) so the gossip receiver keeps
        // draining and `Lagged` stays rare. The worker does the SQLite write then forwards the same
        // SwiftEvent in order; if the worker is gone (shutdown) we drop, same as a closed channel.
        eprintln!("📤 [TOPIC→SWIFT] Queuing event for persist+forward: {:?}", std::mem::discriminant(&evt));
        let _ = self.persist_tx.send(evt);
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
    // ANTI-ENTROPY (delta repair) + MULTI-SOURCE SNAPSHOT PICK
    // ═══════════════════════════════════════════════════════════════════════

    /// One anti-entropy sweep: gossip this group's compact state digest so peers can detect and
    /// repair anything live deltas dropped. Skipped when we have no neighbours (nobody to tell) or
    /// no state yet (nothing to advertise). `O(1)` gossip; the digest is `O(state)` to compute.
    async fn do_sweep(&self) {
        if self.known_peers.is_empty() {
            return;
        }
        let (count, hash) = anti_entropy::group_digest(&self.group_id);
        if count == 0 {
            return;
        }
        let msg = AntiEntropyMsg::Digest {
            group_id: self.group_id.clone(),
            node_id: self.node_id.clone(),
            count,
            hash,
        };
        match serde_json::to_vec(&msg) {
            Ok(data) => {
                if self.sender.broadcast(Bytes::from(data)).await.is_ok() {
                    crate::metrics::record_ae_digest_sent();
                }
            }
            Err(e) => tracing::error!("🔴 [TOPIC] anti-entropy digest serialize failed: {}", e),
        }
    }

    /// Handle a peer's state digest. If it matches ours we are in sync; if the sender is strictly
    /// behind us they will pull from us instead; otherwise the sender holds something we lack, so we
    /// pull a **quiet** merge snapshot from them (idempotent upsert-by-id ⇒ a union merge). Debounced
    /// to one repair in flight per group so a digest seen from many peers triggers a single pull.
    async fn handle_anti_entropy(&mut self, ae: AntiEntropyMsg) {
        let AntiEntropyMsg::Digest { group_id, node_id, count, hash } = ae;
        if group_id != self.group_id || node_id == self.node_id {
            return;
        }
        let (my_count, my_hash) = anti_entropy::group_digest(&self.group_id);
        if hash == my_hash {
            return; // in sync — nothing to repair
        }
        if count < my_count {
            return; // sender is behind us; they will pull from our digest
        }
        // Debounce: claim the single in-flight repair slot for this group.
        if self.repairing.swap(true, Ordering::AcqRel) {
            return;
        }
        crate::metrics::record_ae_repair();

        let endpoint = self.endpoint.clone();
        let gid = self.group_id.clone();
        let grant = self.grant.clone();
        let event_tx = self.event_tx.clone();
        let repairing = self.repairing.clone();
        tokio::spawn(async move {
            // Quiet pull: merge the sender's state into ours WITHOUT re-emitting join-time Sync*
            // events. Reuses the snapshot serve/apply path — no new transfer protocol.
            if let Err(e) =
                download_snapshot(endpoint, &node_id, &gid, grant.as_deref(), event_tx, true).await
            {
                tracing::debug!(
                    "🩹 [TOPIC] anti-entropy repair from {}... failed: {}",
                    &node_id[..16.min(node_id.len())],
                    e
                );
            }
            repairing.store(false, Ordering::Release);
        });
    }

    /// Resolve the collected snapshot offers into a single holder pick and start the join-time
    /// snapshot pull. Picks at random so concurrent cold-joiners spread across holders (no single
    /// host overload). No-op if we no longer need a snapshot or gathered no offers.
    async fn commit_snapshot_pick(&mut self) {
        self.snapshot_pick_at = None;
        if !self.need_snapshot {
            self.snapshot_offers.clear();
            return;
        }
        let sources: Vec<String> = self.snapshot_offers.drain().collect();
        if sources.is_empty() {
            return;
        }
        // Random holder among device peers that offered (Lens is an HTTP enrichment leg, not a mesh
        // peer, so it never appears here; the mesh repairs itself entirely from device holders).
        let idx = {
            use rand::Rng;
            rand::thread_rng().gen_range(0..sources.len())
        };
        let source_peer = sources[idx].clone();
        self.need_snapshot = false;

        eprintln!(
            "📥 [SNAP-PICK] Picked holder {}... of {} offer(s) for {}...",
            &source_peer[..16.min(source_peer.len())],
            sources.len(),
            &self.group_id[..16.min(self.group_id.len())]
        );

        let endpoint = self.endpoint.clone();
        let group_id = self.group_id.clone();
        let event_tx = self.event_tx.clone();
        let network_tx = self.network_tx.clone();
        let grant = self.grant.clone();
        tokio::spawn(async move {
            match download_snapshot(endpoint, &source_peer, &group_id, grant.as_deref(), event_tx, false).await {
                Ok(_) => {
                    let _ = network_tx.send(TopicNetworkCmd::SnapshotComplete { group_id });
                }
                Err(e) => {
                    tracing::error!("🔴 [SNAP-DL] Snapshot download failed: {}", e);
                    let _ = network_tx.send(TopicNetworkCmd::SnapshotFailed {
                        group_id,
                        reason: e.to_string(),
                    });
                }
            }
        });
    }

    /// Emit the live presence/reachability status for this group off the topic's own peer set
    /// (the honest oracle). Called after every NeighborUp/NeighborDown so the app's status bar
    /// reflects the real connected-peer count and whether the group is reachable on the mesh
    /// (`online`) or working against just this device's copy (`local_only`). Additive, receive-only.
    fn emit_presence(&self) {
        let count = self.known_peers.len() as u32;
        let _ = self.event_tx.send(SwiftEvent::PeerCountChanged {
            group_id: self.group_id.clone(),
            count,
        });
        let state = if count == 0 { "local_only" } else { "online" };
        let _ = self.event_tx.send(SwiftEvent::MeshReachability {
            group_id: self.group_id.clone(),
            state: state.to_string(),
        });
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

    /// Broadcast a blob-swarm negotiation message (`IHave`/`WhoHas`) onto this group topic (G10).
    /// Rides the existing gossip channel exactly like `NetworkEvent`/`NetworkCommand` do.
    async fn broadcast_swarm_message(&self, msg: &SwarmMessage) {
        match serde_json::to_vec(msg) {
            Ok(data) => {
                if let Err(e) = self.sender.broadcast(Bytes::from(data)).await {
                    tracing::error!("🔴 [TOPIC] swarm message broadcast failed: {}", e);
                }
            }
            Err(e) => tracing::error!("🔴 [TOPIC] swarm message serialize failed: {}", e),
        }
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
            // ROUND8 §W2 notes — apply via idempotent LWW upsert-by-id; Added/Updated
            // are handled identically (the split is informational for the UI).
            NetworkEvent::NoteAdded {
                id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at,
            }
            | NetworkEvent::NoteUpdated {
                id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at,
            } => {
                let note = crate::models::dto::NoteDTO {
                    id: id.clone(),
                    board_id: board_id.clone(),
                    tenant_id: tenant_id.clone(),
                    author_id: author_id.clone(),
                    author_name: author_name.clone(),
                    text: text.clone(),
                    created_at: *created_at,
                    updated_at: *updated_at,
                };
                let _ = storage::note_upsert(&note);
            }
            NetworkEvent::NoteDeleted { id } => {
                let _ = storage::note_delete(id);
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

/// Content-addressed swarm download (G10): fetch the blob multi-source from `holders` (churn/resume +
/// Blake3 verify inside `BlobSwarm::fetch`), land it on disk under the downloads dir, record its
/// `local_path`, and emit `FileDownloaded` — reusing the same storage rows and event as the direct
/// path so the iOS app sees a plugin land exactly like any other file (no new client FFI).
async fn swarm_download_file(
    swarm: &Arc<BlobSwarm>,
    file_id: &str,
    hash: &str,
    holders: &[String],
    event_tx: UnboundedSender<SwiftEvent>,
) -> Result<()> {
    let parsed = Hash::from_str(hash).map_err(|e| anyhow!("bad blob hash {}: {}", hash, e))?;

    let bytes = swarm.fetch(&parsed, holders).await?; // Blake3-verified on completion

    // File name from the existing file row (registered via FileAvailable); fall back to the id.
    let file_name = storage::file_get_for_transfer(file_id, hash)
        .map(|(name, _, _)| name)
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| file_id.to_string());

    let data_dir = crate::DATA_DIR
        .get()
        .cloned()
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let downloads_dir = data_dir.join("downloads");
    tokio::fs::create_dir_all(&downloads_dir).await?;
    let final_path = downloads_dir.join(&file_name);
    tokio::fs::write(&final_path, &bytes).await?;

    let _ = storage::file_set_local_path(file_id, &final_path.to_string_lossy());
    let _ = storage::transfer_set_status(file_id, "complete");

    tracing::info!(
        "✅ [FILE] Swarm download complete: {} ({} bytes from {} holders)",
        file_name,
        bytes.len(),
        holders.len()
    );
    let _ = event_tx.send(SwiftEvent::FileDownloaded {
        file_id: file_id.to_string(),
        local_path: final_path.to_string_lossy().to_string(),
    });
    Ok(())
}

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

/// Download a snapshot from a peer.
///
/// `quiet` distinguishes the two callers that share this one transfer path:
/// - `false` — join-time snapshot: emit the user-facing `Sync*`/`StatusUpdate` events the iOS app
///   drives its onboarding UI from (unchanged behavior).
/// - `true`  — anti-entropy repair: silently merge the holder's state into ours (idempotent
///   upsert-by-id) WITHOUT re-emitting those events, since this is a background reconciliation, not
///   a join. The storage writes — the actual repair — happen in both modes.
async fn download_snapshot(
    endpoint: Endpoint,
    source_peer: &str,
    group_id: &str,
    grant: Option<&str>,
    event_tx: UnboundedSender<SwiftEvent>,
    quiet: bool,
) -> Result<()> {
    use crate::models::protocol::SnapshotRequest;
    use tokio::io::AsyncWriteExt;

    eprintln!("═══════════════════════════════════════════════════════════════════");
    eprintln!("📥 [SNAP-DL-1] Starting snapshot download");
    eprintln!("   source_peer: {}...", &source_peer[..16.min(source_peer.len())]);
    eprintln!("   group_id: {}...", &group_id[..16.min(group_id.len())]);
    eprintln!("═══════════════════════════════════════════════════════════════════");

    // Emit status update
    if !quiet {
        let _ = event_tx.send(SwiftEvent::StatusUpdate {
            message: "Connecting to peer for sync...".to_string(),
        });
    }

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

    // Send the request frame: JSON {group_id, grant?}. The holder verifies the grant before
    // serving an enforced group; un-enforced groups ignore it (the holder also accepts a bare
    // legacy group_id payload, so this stays interoperable).
    let request = SnapshotRequest {
        group_id: group_id.to_string(),
        grant: grant.map(|g| g.to_string()),
    };
    let req_bytes = serde_json::to_vec(&request)?;
    let len = (req_bytes.len() as u32).to_be_bytes();
    send.write_all(&len).await?;
    send.write_all(&req_bytes).await?;
    send.flush().await?;

    eprintln!("📥 [SNAP-DL-5] ✓ Request sent. Waiting for snapshot frames...");

    if !quiet {
        let _ = event_tx.send(SwiftEvent::StatusUpdate {
            message: "Receiving snapshot data...".to_string(),
        });
    }

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
                if !quiet {
                    let _ = event_tx.send(SwiftEvent::SyncStarted {
                        group_id: group.id.clone(),
                        group_name: group.name.clone(),
                    });
                }

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
                if !quiet {
                    let _ = event_tx.send(SwiftEvent::SyncStructureReceived {
                        group_id: group_id.to_string(),
                        workspace_count: workspaces.len() as u32,
                        board_count: boards.len() as u32,
                    });
                }
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
                if !quiet {
                    let _ = event_tx.send(SwiftEvent::SyncBoardReady {
                        board_id: "all".to_string(),
                        element_count: elements.len() as u32,
                        cell_count: cells.len() as u32,
                    });
                }
            }

            SnapshotFrame::Metadata { chats, files, integrations, board_metadata, notes } => {
                eprintln!("📥 [SNAP-DL-9] METADATA frame received:");
                eprintln!("   chats: {}", chats.len());
                eprintln!("   files: {}", files.len());
                eprintln!("   integrations: {}", integrations.len());
                eprintln!("   board_metadata: {}", board_metadata.len());
                eprintln!("   notes: {}", notes.len());

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

                // Insert notes — idempotent LWW upsert-by-id, so a merge snapshot
                // (anti-entropy repair) converges to the latest value without churn.
                for nt in &notes {
                    if let Err(e) = storage::note_upsert(nt) {
                        eprintln!("   ⚠️ Note insert: {}", e);
                    }
                }

                eprintln!("📥 [SNAP-DL-9a] ✓ Metadata inserted");

                // Emit files received
                if !files.is_empty() && !quiet {
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
                if !quiet {
                    let _ = event_tx.send(SwiftEvent::SyncComplete {
                        group_id: group_id.to_string(),
                    });

                    let _ = event_tx.send(SwiftEvent::StatusUpdate {
                        message: "Sync complete!".to_string(),
                    });
                }

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