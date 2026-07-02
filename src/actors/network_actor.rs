// src/actors/network_actor.rs
//
// NetworkActor - Central network coordinator
//
// Responsibilities:
// - Owns endpoint, gossip
// - Spawns and manages DiscoveryActor
// - Spawns and manages TopicActors (one per group)
// - Handles DM streams (peer-to-peer QUIC)
// - Routes NetworkCommand from FFI layer
// - Receives TopicNetworkCmd from TopicActors (snapshot coordination)
// - Broadcasts profile on startup

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::discovery::mdns::{DiscoveryEvent, MdnsDiscovery};
use iroh::discovery::static_provider::StaticProvider;
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, EndpointAddr, PublicKey, SecretKey,
};
use iroh_gossip::net::Gossip;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::identity::{Grant, MeshAuthorizer, Role};
use crate::util::MutexExt;
use crate::swarm::{BlobSwarm, BLOB_ALPN};
use crate::{
    actors::{
        discovery_actor::{DiscoveryActor, DiscoveryCommand, DiscoveryNetworkCmd},
        topic_actor::{TopicActor, TopicCommand},
        ActorHandle, ActorMessage, SystemCommand, TopicNetworkCmd,
    },
    bootstrap_node_id,
    models::{
        commands::NetworkCommand,
        events::{NetworkEvent, SwiftEvent},
        node_config::{relay_mode_for, DiscoveryPolicy, NodeConfig},
    },
    storage,
};
// ═══════════════════════════════════════════════════════════════════════════
// ALPN PROTOCOLS
// ═══════════════════════════════════════════════════════════════════════════

pub const FILE_TRANSFER_ALPN: &[u8] = b"cyan-file-v2";
pub const SNAPSHOT_ALPN: &[u8] = b"cyan-snapshot-v1";
pub const DM_ALPN: &[u8] = b"cyan-dm-v1";

// ═══════════════════════════════════════════════════════════════════════════
// PROTOCOL HANDLERS (for Router integration)
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot protocol handler - accepts incoming snapshot requests
#[derive(Debug, Clone)]
pub struct SnapshotHandler {
    node_id: String,
    event_tx: UnboundedSender<SwiftEvent>,
    /// Shared per-node authority — gates the join-time snapshot read for enforced groups.
    authorizer: Arc<std::sync::Mutex<MeshAuthorizer>>,
}

impl ProtocolHandler for SnapshotHandler {
    async fn accept(&self, conn: Connection) -> std::result::Result<(), AcceptError> {
        let peer_id = conn.remote_id().to_string();
        eprintln!("═══════════════════════════════════════════════════════════════════");
        eprintln!("📥 [SNAPSHOT] Incoming request from {}...", &peer_id[..16]);
        eprintln!("═══════════════════════════════════════════════════════════════════");
        tracing::info!("📥 [SNAPSHOT] Incoming snapshot request from {}", &peer_id[..16]);

        if let Err(e) = handle_snapshot_server(conn, self.node_id.clone(), self.event_tx.clone(), self.authorizer.clone()).await {
            eprintln!("🔴 [SNAPSHOT] Transfer error: {}", e);
            tracing::error!("🔴 Snapshot transfer error: {}", e);
        }
        Ok(())
    }
}

/// File transfer protocol handler - accepts incoming file requests
#[derive(Debug, Clone)]
pub struct FileTransferHandler {
    event_tx: UnboundedSender<SwiftEvent>,
}

impl ProtocolHandler for FileTransferHandler {
    async fn accept(&self, conn: Connection) -> std::result::Result<(), AcceptError> {
        let peer_id = conn.remote_id().to_string();
        eprintln!("📥 [FILE] Transfer request from {}...", &peer_id[..16]);
        tracing::info!("📥 [FILE] Incoming file transfer from {}", &peer_id[..16]);

        if let Err(e) = handle_file_transfer_server(conn, self.event_tx.clone()).await {
            eprintln!("🔴 [FILE] Transfer error: {}", e);
            tracing::error!("🔴 File transfer error: {}", e);
        }
        Ok(())
    }
}

/// DM protocol handler - accepts incoming DM connections
#[derive(Debug, Clone)]
pub struct DmHandler {
    dm_senders: Arc<std::sync::Mutex<HashMap<String, UnboundedSender<DirectMessage>>>>,
    event_tx: UnboundedSender<SwiftEvent>,
    /// Lets the inbound DM handler fetch an attachment into scope (see `self_cmd_tx`).
    self_cmd_tx: UnboundedSender<NetworkCommand>,
}

impl ProtocolHandler for DmHandler {
    async fn accept(&self, conn: Connection) -> std::result::Result<(), AcceptError> {
        let peer_id = conn.remote_id().to_string();
        eprintln!("💬 [DM] Incoming connection from {}...", &peer_id[..16]);
        tracing::info!("💬 [DM] Incoming connection from {}", &peer_id[..16]);

        if let Err(e) = handle_dm_stream(conn, peer_id.clone(), self.dm_senders.clone(), self.event_tx.clone(), self.self_cmd_tx.clone()).await {
            eprintln!("🔴 [DM] Connection error with {}: {}", &peer_id[..16], e);
            tracing::error!("🔴 DM connection error: {}", e);
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// DM WIRE PROTOCOL
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectMessage {
    pub id: String,
    pub workspace_id: Option<String>,
    pub message: String,
    pub parent_id: Option<String>,
    pub timestamp: i64,
    pub attachment: Option<DmAttachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DmAttachment {
    pub file_id: String,
    pub name: String,
    pub hash: String,
    pub size: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// NETWORK ACTOR
// ═══════════════════════════════════════════════════════════════════════════

pub struct NetworkActor {
    node_id: String,
    endpoint: Endpoint,
    gossip: Arc<Gossip>,
    #[allow(dead_code)]
    router: Router,

    /// Discovery actor handle
    discovery_handle: Option<ActorHandle<DiscoveryCommand>>,

    /// Topic actors: group_id → handle
    topics: HashMap<String, ActorHandle<TopicCommand>>,

    /// DM stream senders: peer_id → sender (Arc<Mutex> for sharing with acceptor)
    dm_senders: Arc<std::sync::Mutex<HashMap<String, UnboundedSender<DirectMessage>>>>,

    /// Peers per group (shared with FFI for queries)
    peers_per_group: Arc<std::sync::Mutex<HashMap<String, HashSet<PublicKey>>>>,

    /// Event channel to Swift
    event_tx: UnboundedSender<SwiftEvent>,

    /// Self-command channel: lets internal handlers (e.g. the DM receive path, when a
    /// message carries an attachment) enqueue a `NetworkCommand` back into this actor's own
    /// loop so it runs through the normal command handling (here: `RequestFileDownload`).
    /// The receiver half is drained in `run_loop` alongside the FFI command channel.
    self_cmd_tx: UnboundedSender<NetworkCommand>,
    self_cmd_rx: UnboundedReceiver<NetworkCommand>,

    /// Channel to receive commands from DiscoveryActor
    discovery_rx: UnboundedReceiver<DiscoveryNetworkCmd>,

    /// Sender half (clone given to DiscoveryActor)
    #[allow(dead_code)]
    discovery_tx: UnboundedSender<DiscoveryNetworkCmd>,

    /// Channel to receive snapshot coordination from TopicActors
    topic_network_rx: UnboundedReceiver<TopicNetworkCmd>,

    /// Sender half (clone given to TopicActors)
    topic_network_tx: UnboundedSender<TopicNetworkCmd>,

    /// Groups currently needing snapshot sync
    groups_needing_snapshot: HashSet<String>,

    /// This node's network config (relay/discovery/discovery_key), read in `start()`
    /// to pick the discovery key and the discovery-topic bootstrap peers.
    cfg: NodeConfig,

    /// Out-of-band static address provider (see `new`). Retained so it can be cloned
    /// out via `static_discovery()`; inert unless a caller adds entries.
    static_discovery: StaticProvider,

    /// mDNS discovery handle (MESH_HARDENING §2.1). Built explicitly (instead of via the builder)
    /// so `start()` can `subscribe()` to its discovery stream and route LAN-discovered peers into
    /// the group topics — the fix that makes single-laptop / same-WiFi mesh with no infra. `None`
    /// only if mDNS failed to start (e.g. no usable multicast interface); the engine still runs.
    mdns: Option<MdnsDiscovery>,

    /// Content-addressed blob swarm (G10). Shared with each TopicActor so i-have/who-has
    /// negotiation rides the group gossip, and exposed via `swarm()` for the swarm-fetch path.
    swarm: Arc<BlobSwarm>,

    /// Per-node mesh-write authority (identity/RBAC mesh half). Shared with each TopicActor so
    /// inbound writes are gated by `authorize_write(group, from_peer)`. **Fail-open by default**:
    /// a group is only enforced after `MeshAuthorizer::enforce_group`, so shipping behavior is
    /// unchanged for groups that have not opted into grant enforcement. Exposed via `authorizer()`.
    authorizer: Arc<std::sync::Mutex<MeshAuthorizer>>,
}

impl NetworkActor {
    pub async fn new(
        secret_key: SecretKey,
        event_tx: UnboundedSender<SwiftEvent>,
        peers_per_group: Arc<std::sync::Mutex<HashMap<String, HashSet<PublicKey>>>>,
        cfg: NodeConfig,
    ) -> Result<Self> {
        let node_id = secret_key.public().to_string();
        tracing::info!("🌐 [NET] Creating NetworkActor for node {}", &node_id[..16]);

        // Configure relay mode from this node's policy (pure mapping; behavior for
        // the production `RELAY_URL`-derived config is identical to before).
        let relay_mode = relay_mode_for(&cfg.relay);
        tracing::info!("🌐 [NET] Relay policy: {:?}", cfg.relay);

        // DM senders map (shared between struct and DM handler)
        let dm_senders = Arc::new(std::sync::Mutex::new(HashMap::new()));

        // A StaticProvider lets callers feed in known peer addresses out of band. It is
        // INERT in production (no entries ⇒ resolves nothing; mDNS still does the work),
        // and is the supported way for the in-process test harness to inject loopback
        // addresses so nodes can dial each other without relying on mDNS multicast.
        let static_discovery = StaticProvider::new();

        // mDNS discovery (MESH_HARDENING §2.1): build it explicitly with this node's id so we keep a
        // handle and can `subscribe()` to its discovery stream in `start()`. Behavior for resolving
        // peer addresses is identical to the previous `MdnsDiscovery::builder()` form; the only
        // addition is that we now also LISTEN for discovered peers and seed them into group topics.
        // Best-effort: if it can't start (no multicast interface), fall back to the builder form so
        // the endpoint still binds with mDNS exactly as before.
        let public = secret_key.public();
        // Dailies-grade transfer windows (G8 hardening). quinn's defaults are tuned for
        // 100 Mbps @ 100 ms RTT; same-WiFi/loopback media moves are choked by them. Keep
        // N chunks pipelined per stream: stream window = CYAN_XFER_WINDOW (default 32)
        // × 256 KB chunks = 8 MB; connection budget covers the parallel streams (8×).
        // iroh's own default (keep_alive 1s) is preserved.
        let window_chunks: u64 = std::env::var("CYAN_XFER_WINDOW")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|n| (1..=1024).contains(n))
            .unwrap_or(32);
        let stream_window = window_chunks * 256 * 1024;
        let mut transport = iroh::endpoint::TransportConfig::default();
        transport.keep_alive_interval(Some(Duration::from_secs(1)));
        if let Ok(w) = iroh::endpoint::VarInt::from_u64(stream_window) {
            transport.stream_receive_window(w);
        }
        if let Ok(w) = iroh::endpoint::VarInt::from_u64(stream_window * 8) {
            transport.receive_window(w);
        }
        transport.send_window(stream_window * 8);
        let mut builder = Endpoint::builder()
            .secret_key(secret_key)
            .transport_config(transport)
            .alpns(vec![
                iroh_gossip::ALPN.to_vec(),
                FILE_TRANSFER_ALPN.to_vec(),
                SNAPSHOT_ALPN.to_vec(),
                DM_ALPN.to_vec(),
                BLOB_ALPN.to_vec(),
            ])
            .relay_mode(relay_mode);
        let mdns = match MdnsDiscovery::builder().build(public) {
            Ok(m) => {
                builder = builder.discovery(m.clone());
                Some(m)
            }
            Err(e) => {
                tracing::warn!("⚠️ [NET] mDNS discovery unavailable ({e}); LAN seeding disabled");
                builder = builder.discovery(MdnsDiscovery::builder());
                None
            }
        };
        let endpoint = builder
            .discovery(static_discovery.clone())
            .bind()
            .await?;

        tracing::info!("✅ [NET] Endpoint bound: {}", &node_id[..16]);

        // Create gossip
        let gossip = Arc::new(Gossip::builder().spawn(endpoint.clone()));

        // Content-addressed blob swarm (G10): a per-node store served on the blobs ALPN over THIS
        // same endpoint/router. Additive and behavior-preserving — holders are addressed by the
        // node's normal node id, and the gossip/file/dm/snapshot paths are untouched. See `swarm.rs`.
        // The store is FS-BACKED (RAM-flat, resumable across restarts) under a per-node root so
        // in-process multi-node tests keep honest per-node stores.
        let blobs_root = crate::DATA_DIR
            .get()
            .cloned()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join("blobs")
            .join(&node_id[..16.min(node_id.len())]);
        let swarm = Arc::new(BlobSwarm::new(endpoint.clone(), node_id.clone(), &blobs_root).await?);

        // Per-node mesh-write authority (identity/RBAC mesh half). Created here so the snapshot
        // server handler can share it: the join-time snapshot read is gated by the same authorizer
        // that gates inbound writes (fail-open until a group is enforced).
        let authorizer = Arc::new(std::sync::Mutex::new(MeshAuthorizer::new()));

        // Create protocol handlers
        let snapshot_handler = SnapshotHandler {
            node_id: node_id.clone(),
            event_tx: event_tx.clone(),
            authorizer: authorizer.clone(),
        };

        let file_handler = FileTransferHandler {
            event_tx: event_tx.clone(),
        };

        // Self-command channel (see struct field): the DM acceptor handler gets the sender so
        // an inbound attachment can enqueue a `RequestFileDownload` back into this actor.
        let (self_cmd_tx, self_cmd_rx) = mpsc::unbounded_channel();

        let dm_handler = DmHandler {
            dm_senders: dm_senders.clone(),
            event_tx: event_tx.clone(),
            self_cmd_tx: self_cmd_tx.clone(),
        };

        // Setup router with ALL protocols
        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(SNAPSHOT_ALPN, snapshot_handler)
            .accept(FILE_TRANSFER_ALPN, file_handler)
            .accept(DM_ALPN, dm_handler)
            .accept(BLOB_ALPN, swarm.blobs_protocol())
            .spawn();

        tracing::info!("✅ [NET] Router spawned with gossip + snapshot + file + dm + blob-swarm");

        // Create channel for DiscoveryActor → NetworkActor communication
        let (discovery_tx, discovery_rx) = mpsc::unbounded_channel();

        // Create channel for TopicActor → NetworkActor communication (snapshot coordination)
        let (topic_network_tx, topic_network_rx) = mpsc::unbounded_channel();

        Ok(Self {
            node_id,
            endpoint,
            gossip,
            router,
            discovery_handle: None,
            topics: HashMap::new(),
            dm_senders,
            peers_per_group,
            event_tx,
            self_cmd_tx,
            self_cmd_rx,
            discovery_rx,
            discovery_tx,
            topic_network_rx,
            topic_network_tx,
            groups_needing_snapshot: HashSet::new(),
            cfg,
            static_discovery,
            mdns,
            swarm,
            authorizer,
        })
    }

    /// A clone of this node's mesh-write authorizer (identity/RBAC mesh half). The honest
    /// per-node oracle for grant enforcement: callers `enforce_group`, seed admins, record a
    /// presented grant, and read back `authorize_write`/`role_of_peer`. Cheap (Arc clone).
    pub fn authorizer(&self) -> Arc<std::sync::Mutex<MeshAuthorizer>> {
        self.authorizer.clone()
    }

    /// A clone of this node's blob swarm handle (G10). Test-support seam and the engine's
    /// content-addressed multi-source fetch entry point. Cheap (internally reference-counted).
    pub fn swarm(&self) -> Arc<BlobSwarm> {
        self.swarm.clone()
    }

    /// A clone of this actor's endpoint. Test-support seam: the in-process harness uses
    /// it to read each node's `EndpointAddr` (loopback direct addresses) so peers can be
    /// wired to each other without mDNS. Cheap (the endpoint is internally reference-counted).
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    /// A clone of this actor's static address provider. Test-support seam: the harness
    /// calls `add_endpoint_info(addr)` on it to make a peer dialable out of band.
    pub fn static_discovery(&self) -> StaticProvider {
        self.static_discovery.clone()
    }

    /// Start the network actor - spawns discovery and runs command loop
    pub async fn start(mut self, cmd_rx: UnboundedReceiver<NetworkCommand>) {
        eprintln!("🚀 [NET] ════════════════════════════════════════════════════════════");
        eprintln!("🚀 [NET] NetworkActor STARTING - node: {}", &self.node_id[..16]);
        eprintln!("🚀 [NET] ════════════════════════════════════════════════════════════");
        tracing::info!("🚀 [NET] Starting NetworkActor");

        // Spawn DiscoveryActor (key + bootstrap peers come from this node's config).
        // Production config is DiscoveryPolicy::Bootstrap(bootstrap_node_id()), so the
        // bootstrap set is the same default peer as before — behavior unchanged. A
        // per-node bootstrap (or MdnsOnly → no bootstrap) is what lets the in-process
        // harness seed one node off another instead of an unreachable global default.
        let discovery_key = self.cfg.discovery_key.clone();
        let disc_bootstrap: Vec<PublicKey> = match &self.cfg.discovery {
            DiscoveryPolicy::Bootstrap(hex) => match PublicKey::from_str(hex) {
                Ok(pk) => vec![pk],
                Err(e) => {
                    tracing::warn!("⚠️ [NET] Invalid discovery bootstrap id '{}': {}", hex, e);
                    vec![]
                }
            },
            DiscoveryPolicy::MdnsOnly => vec![],
        };

        eprintln!(
            "🔍 [NET] Spawning DiscoveryActor with key: {} ({} bootstrap peers)",
            discovery_key,
            disc_bootstrap.len()
        );

        match DiscoveryActor::spawn(
            self.node_id.clone(),
            discovery_key,
            disc_bootstrap,
            self.gossip.clone(),
            self.discovery_tx.clone(),
            self.event_tx.clone(),
        ).await {
            Ok(handle) => {
                eprintln!("✅ [NET] DiscoveryActor spawned successfully");
                tracing::info!("✅ [NET] DiscoveryActor spawned");
                self.discovery_handle = Some(handle);
            }
            Err(e) => {
                eprintln!("🔴 [NET] FAILED to spawn DiscoveryActor: {}", e);
                tracing::error!("🔴 [NET] Failed to spawn DiscoveryActor: {}", e);
            }
        }

        // Load existing groups and spawn topic actors
        let existing_groups = storage::group_list_ids();
        eprintln!("📂 [NET] Loading {} existing groups from DB", existing_groups.len());
        for group_id in existing_groups {
            eprintln!("   → Spawning TopicActor for group: {}...", &group_id[..16.min(group_id.len())]);
            if let Err(e) = self.spawn_topic_actor(&group_id, vec![], None).await {
                eprintln!("🔴 [NET] FAILED to spawn TopicActor for {}: {}", &group_id[..16.min(group_id.len())], e);
                tracing::error!(
                    "🔴 [NET] Failed to spawn TopicActor for {}: {}",
                    &group_id[..16.min(group_id.len())],
                    e
                );
            }
        }

        // NOTE: Router handles DM, file, and snapshot acceptance now
        // No need for spawn_dm_acceptor or spawn_protocol_acceptor

        // mDNS LAN seeding (MESH_HARDENING §2.1): listen for peers discovered on the local network
        // and route each into our group topics. This is the offline/single-laptop fix — gossip needs
        // a present, resolvable peer in a topic's bootstrap set to ever fire `NeighborUp`, and mDNS
        // is the only source of that with no internet/relay/bootstrap. Each discovered peer is fed
        // back through this actor's own command loop as `SeedDiscoveredPeer` (which seeds it into
        // every joined group); gossip only forms a neighbor for genuinely shared topics.
        if let Some(mdns) = self.mdns.clone() {
            let seed_tx = self.self_cmd_tx.clone();
            tokio::spawn(async move {
                let mut events = mdns.subscribe().await;
                while let Some(ev) = events.next().await {
                    if let DiscoveryEvent::Discovered { endpoint_info, .. } = ev {
                        let addr = endpoint_info.to_endpoint_addr();
                        match serde_json::to_string(&addr) {
                            Ok(addr_json) => {
                                let _ = seed_tx.send(NetworkCommand::SeedDiscoveredPeer { addr_json });
                            }
                            Err(e) => tracing::warn!("⚠️ [NET] mDNS addr serialize failed: {e}"),
                        }
                    }
                }
                tracing::info!("🛑 [NET] mDNS discovery stream ended");
            });
            eprintln!("🔍 [NET] mDNS LAN seeding task started");
        }

        // Publish our resolvable address for the QR inviter-addr seam (§2.2): poll until the endpoint
        // has a direct address, then store it so `cyan_issue_grant_qr` can stamp the full NodeAddr
        // into invites. Best-effort and non-blocking; gives up quietly if no address ever appears.
        {
            let endpoint = self.endpoint.clone();
            tokio::spawn(async move {
                for _ in 0..50 {
                    let addr = endpoint.addr();
                    if addr.addrs.iter().next().is_some() {
                        if let Ok(json) = serde_json::to_string(&addr) {
                            crate::publish_local_endpoint_addr(json);
                        }
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        }

        // Broadcast our profile on startup
        self.broadcast_profile_to_all_groups();

        eprintln!("🟢 [NET] NetworkActor READY - entering main loop");

        // Main run loop
        self.run_loop(cmd_rx).await;
    }

    async fn run_loop(&mut self, mut cmd_rx: UnboundedReceiver<NetworkCommand>) {
        tracing::info!("🔄 [NET] Entering main run loop");

        loop {
            tokio::select! {
                // Commands from FFI/app layer
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(network_cmd) => {
                            self.handle_network_command(network_cmd).await;
                        }
                        None => {
                            tracing::info!("🛑 [NET] Command channel closed");
                            break;
                        }
                    }
                }

                // Self-issued commands (e.g. fetch an attachment received over a DM)
                self_cmd = self.self_cmd_rx.recv() => {
                    match self_cmd {
                        Some(network_cmd) => {
                            self.handle_network_command(network_cmd).await;
                        }
                        None => {
                            // tx is held by self; this arm only resolves to None at shutdown.
                            tracing::debug!("🛑 [NET] Self-command channel closed");
                        }
                    }
                }

                // Commands from DiscoveryActor
                disc_cmd = self.discovery_rx.recv() => {
                    match disc_cmd {
                        Some(cmd) => {
                            self.handle_discovery_cmd(cmd).await;
                        }
                        None => {
                            tracing::warn!("⚠️ [NET] Discovery channel closed");
                        }
                    }
                }

                // Commands from TopicActors (snapshot coordination)
                topic_cmd = self.topic_network_rx.recv() => {
                    match topic_cmd {
                        Some(cmd) => {
                            self.handle_topic_network_cmd(cmd);
                        }
                        None => {
                            tracing::warn!("⚠️ [NET] Topic network channel closed");
                        }
                    }
                }
            }
        }

        tracing::info!("🛑 [NET] NetworkActor stopped");
    }

    fn handle_topic_network_cmd(&mut self, cmd: TopicNetworkCmd) {
        match cmd {
            TopicNetworkCmd::NeedSnapshot { group_id } => {
                eprintln!("📥 [NET] TopicActor needs snapshot for {}...", &group_id[..16.min(group_id.len())]);
                self.groups_needing_snapshot.insert(group_id);
            }
            TopicNetworkCmd::SnapshotComplete { group_id } => {
                eprintln!("✅ [NET] Snapshot complete for {}...", &group_id[..16.min(group_id.len())]);
                self.groups_needing_snapshot.remove(&group_id);
            }
            TopicNetworkCmd::SnapshotFailed { group_id, reason } => {
                eprintln!("⚠️ [NET] Snapshot failed for {}...: {}", &group_id[..16.min(group_id.len())], reason);
                // Keep in set for retry
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // NETWORK COMMAND HANDLING (from FFI)
    // ═══════════════════════════════════════════════════════════════════════

    async fn handle_network_command(&mut self, cmd: NetworkCommand) {
        // Log every command received
        eprintln!("📥 [NET] handle_network_command: {:?}", std::mem::discriminant(&cmd));

        match cmd {
            NetworkCommand::JoinGroup { group_id, bootstrap_peer, grant } => {
                // SIGNPOST: JoinGroup received from FFI
                eprintln!("═══════════════════════════════════════════════════════════════════");
                eprintln!("🔗 [NET-JOIN-1] NetworkActor received JoinGroup command");
                eprintln!("   group_id: {}...", &group_id[..16.min(group_id.len())]);
                eprintln!("   bootstrap_peer: {:?}", bootstrap_peer.as_ref().map(|p| format!("{}...", &p[..16.min(p.len())])));
                eprintln!("═══════════════════════════════════════════════════════════════════");

                tracing::info!(
                    "🔗 [NET-JOIN-1] JoinGroup: {} (bootstrap: {:?})",
                    &group_id[..16.min(group_id.len())],
                    bootstrap_peer.as_ref().map(|p| &p[..16.min(p.len())])
                );

                // §6 ENTITLEMENT-GATED JOIN: a peer may join/subscribe ONLY groups in its grant.
                // Fail-open for groups this node has not enforced (seam — un-enforced joins behave
                // exactly as before). For an enforced group, the join MUST carry a valid grant FOR
                // THIS group; otherwise we refuse to spawn the topic actor, so the node never
                // subscribes to — or enumerates — a group it isn't entitled to. The single-use
                // nonce is NOT consumed here (the holder spends it at snapshot time); this is a
                // local, non-mutating entitlement check.
                {
                    let parsed = grant
                        .as_deref()
                        .and_then(|qr| Grant::from_qr_payload(qr).ok());
                    let decision = match self.authorizer.lock() {
                        Ok(mut auth) => {
                            let d = auth.authorize_join(&group_id, parsed.as_ref());
                            // SINGLE-USE, MESH-WIDE: we are about to spend this grant to pull
                            // `group_id`'s snapshot from a holder. Mark its nonce consumed in OUR
                            // own authority now, so that if WE later serve this group, the snapshot
                            // serve gate refuses a replay of the same QR. Without this, a peer that
                            // received the snapshot becomes a fail-open re-distribution point that
                            // serves a replayed (already-consumed) grant — the leak the replay test
                            // catches. Marking here (not at completion) is deterministic: it lands
                            // before this node could ever serve a peer.
                            if let Some(g) = parsed
                                .as_ref()
                                .filter(|g| d.is_ok() && g.group_id == group_id)
                            {
                                auth.note_grant_used(g);
                            }
                            d
                        }
                        // A poisoned authorizer must never deadlock or silently widen access; treat
                        // it as fail-open ONLY for un-enforced semantics by allowing — the snapshot
                        // serve gate still applies on the holder side.
                        Err(_) => Ok(Role::Member),
                    };
                    if let Err(reason) = decision {
                        eprintln!(
                            "⛔ [NET-JOIN] entitlement-gated join REFUSED for {}...: {:?}",
                            &group_id[..16.min(group_id.len())],
                            reason
                        );
                        tracing::warn!(
                            "⛔ [NET-JOIN] join refused (not entitled) for {}: {:?}",
                            &group_id[..16.min(group_id.len())],
                            reason
                        );
                        let _ = self.event_tx.send(SwiftEvent::Error {
                            message: format!("join refused: not entitled to group ({reason:?})"),
                        });
                        return;
                    }
                }

                // Parse bootstrap peer if provided
                let mut initial_peers = vec![];
                if let Some(ref peer_str) = bootstrap_peer {
                    match PublicKey::from_str(peer_str) {
                        Ok(pk) => {
                            eprintln!("🔗 [NET-JOIN-2] ✓ Parsed bootstrap peer: {}...", &peer_str[..16]);
                            initial_peers.push(pk);
                        }
                        Err(e) => {
                            eprintln!("🔗 [NET-JOIN-2] ✗ Failed to parse bootstrap peer: {}", e);
                        }
                    }
                }

                // Always add bootstrap node
                if let Ok(bootstrap_pk) = PublicKey::from_str(bootstrap_node_id()) {
                    if !initial_peers.contains(&bootstrap_pk) {
                        eprintln!("🔗 [NET-JOIN-3] Adding bootstrap node: {}...", &bootstrap_node_id()[..16]);
                        initial_peers.push(bootstrap_pk);
                    }
                } else {
                    eprintln!("🔗 [NET-JOIN-3] ⚠️ Failed to parse BOOTSTRAP_NODE_ID");
                }

                eprintln!("🔗 [NET-JOIN-4] Spawning TopicActor with {} initial peers", initial_peers.len());

                // Spawn topic actor (carrying the scanned grant, if any, so the snapshot
                // download presents it to the holder)
                match self.spawn_topic_actor(&group_id, initial_peers, grant).await {
                    Ok(_) => {
                        eprintln!("🔗 [NET-JOIN-5] ✓ TopicActor spawned successfully");
                        tracing::info!("🔗 [NET-JOIN-5] TopicActor spawned for {}", &group_id[..16.min(group_id.len())]);

                        // Trigger snapshot request - this also helps establish the gossip mesh
                        eprintln!("🔗 [NET-JOIN-6] Triggering snapshot request...");
                        if let Some(handle) = self.topics.get(&group_id) {
                            eprintln!("🔗 [NET-JOIN-6a] Sending RequestSnapshot to TopicActor");
                            let _ = handle.cmd_tx.send(ActorMessage::Domain(
                                TopicCommand::RequestSnapshot
                            ));
                            eprintln!("🔗 [NET-JOIN-6b] ✓ RequestSnapshot command sent");
                        } else {
                            eprintln!("🔗 [NET-JOIN-6a] ⚠️ TopicActor handle not found!");
                        }
                    }
                    Err(e) => {
                        eprintln!("🔗 [NET-JOIN-5] 🔴 FAILED to spawn TopicActor: {}", e);
                        tracing::error!("🔴 [NET-JOIN-5] Failed to spawn TopicActor: {}", e);
                    }
                }

                // Tell discovery actor about new group
                if let Some(ref handle) = self.discovery_handle {
                    eprintln!("🔗 [NET-JOIN-7] Announcing group to DiscoveryActor");
                    let _ = handle.cmd_tx.send(ActorMessage::Domain(
                        DiscoveryCommand::AnnounceGroup(group_id.clone())
                    ));
                } else {
                    eprintln!("🔗 [NET-JOIN-7] ⚠️ No DiscoveryActor handle");
                }

                eprintln!("═══════════════════════════════════════════════════════════════════");
                eprintln!("✅ [NET-JOIN-8] JoinGroup complete for {}...", &group_id[..16.min(group_id.len())]);
                eprintln!("═══════════════════════════════════════════════════════════════════");
            }

            NetworkCommand::Broadcast { group_id, event } => {
                eprintln!("📤 [NET] Broadcast command received:");
                eprintln!("   group_id: {}...", &group_id[..16.min(group_id.len())]);
                eprintln!("   event: {:?}", std::mem::discriminant(&event));
                if let Some(handle) = self.topics.get(&group_id) {
                    eprintln!("📤 [NET] ✓ TopicActor found, forwarding...");
                    let _ = handle.cmd_tx.send(ActorMessage::Domain(
                        TopicCommand::Broadcast(event)
                    ));
                } else {
                    eprintln!("📤 [NET] ⚠️ No TopicActor for group {}...", &group_id[..16.min(group_id.len())]);
                    tracing::warn!(
                        "⚠️ [NET] No TopicActor for group {}",
                        &group_id[..16.min(group_id.len())]
                    );
                }
            }

            NetworkCommand::SeedGroupPeer { group_id, addr_json } => {
                // ONE seeding pipeline (§2): a single resolvable peer → one group topic. Used by the
                // QR-inviter / persisted / bootstrap / Lens sources (and the test harness).
                match serde_json::from_str::<EndpointAddr>(&addr_json) {
                    Ok(addr) => self.seed_peer_into_group(&group_id, addr).await,
                    Err(e) => tracing::warn!("⚠️ [NET] SeedGroupPeer bad addr_json: {e}"),
                }
            }

            NetworkCommand::SeedDiscoveredPeer { addr_json } => {
                // mDNS source (§2.1): a LAN-discovered peer, group membership unknown → offer it to
                // every joined group. Gossip only forms a neighbor where the topic is genuinely shared.
                match serde_json::from_str::<EndpointAddr>(&addr_json) {
                    Ok(addr) => {
                        if addr.id.to_string() != self.node_id {
                            let groups: Vec<String> = self.topics.keys().cloned().collect();
                            for group_id in groups {
                                self.seed_peer_into_group(&group_id, addr.clone()).await;
                            }
                        }
                    }
                    Err(e) => tracing::warn!("⚠️ [NET] SeedDiscoveredPeer bad addr_json: {e}"),
                }
            }

            NetworkCommand::RequestSnapshot { from_peer } => {
                tracing::info!("🗂️ [NET] RequestSnapshot from {}", &from_peer[..16]);
                // This is handled by TopicActor via gossip - the request is broadcast
                // and peers respond with SnapshotAvailable
            }

            NetworkCommand::CatchUp { group_id, source_peer, since } => {
                // MESH_HARDENING §5: pull only the missing range from `source_peer`. When `since`
                // is unset, fall back to the group's persisted "synced as of T" watermark (set by a
                // §11 bundle import), else the local high-water mark — so a returning peer asks for
                // exactly what it lacks instead of a full re-snapshot.
                let since = since
                    .or_else(|| storage::group_sync_state_get(&group_id))
                    .or_else(|| {
                        let hw = crate::snapshot::group_high_water(&group_id);
                        (hw > 0).then_some(hw)
                    });
                if let Some(handle) = self.topics.get(&group_id) {
                    let _ = handle.cmd_tx.send(ActorMessage::Domain(
                        TopicCommand::CatchUp { source_peer, since },
                    ));
                } else {
                    tracing::warn!(
                        "⚠️ [NET] CatchUp: no TopicActor for group {}",
                        &group_id[..16.min(group_id.len())]
                    );
                }
            }

            NetworkCommand::RequestFileDownload { file_id, hash, source_peer, resume_offset } => {
                tracing::info!(
                    "📥 [NET] RequestFileDownload: {} from {}",
                    &file_id[..16.min(file_id.len())],
                    &source_peer[..16]
                );

                // Look up which group this file belongs to
                if let Some(group_id) = storage::file_get_group_id(&file_id) {
                    if let Some(handle) = self.topics.get(&group_id) {
                        let _ = handle.cmd_tx.send(ActorMessage::Domain(
                            TopicCommand::DownloadFile {
                                file_id,
                                hash,
                                source_peer,
                                resume_offset,
                            }
                        ));
                    } else {
                        tracing::warn!(
                            "⚠️ [NET] No TopicActor for file's group {}",
                            &group_id[..16.min(group_id.len())]
                        );
                    }
                } else {
                    tracing::warn!(
                        "⚠️ [NET] File {} not found in DB, cannot route download",
                        &file_id[..16.min(file_id.len())]
                    );
                }
            }

            NetworkCommand::SwarmAnnounce { group_id, hash } => {
                if let Some(handle) = self.topics.get(&group_id) {
                    let _ = handle
                        .cmd_tx
                        .send(ActorMessage::Domain(TopicCommand::AnnounceBlob { hash }));
                } else {
                    tracing::warn!(
                        "⚠️ [NET] SwarmAnnounce: no TopicActor for group {}",
                        &group_id[..16.min(group_id.len())]
                    );
                }
            }

            NetworkCommand::SwarmWhoHas { group_id, hash } => {
                if let Some(handle) = self.topics.get(&group_id) {
                    let _ = handle
                        .cmd_tx
                        .send(ActorMessage::Domain(TopicCommand::QueryBlob { hash }));
                } else {
                    tracing::warn!(
                        "⚠️ [NET] SwarmWhoHas: no TopicActor for group {}",
                        &group_id[..16.min(group_id.len())]
                    );
                }
            }

            NetworkCommand::SeedAndAnnounceBlob { group_id, hash, path } => {
                // Plugin distribution (G10): add the file's bytes to this node's content-addressed
                // swarm store, then announce `IHave` to the group so members can swarm-fetch it.
                match tokio::fs::read(&path).await {
                    Ok(bytes) => match self.swarm.add(bytes).await {
                        Ok(added) => {
                            if added.to_string() != hash {
                                tracing::warn!(
                                    "⚠️ [NET] SeedAndAnnounceBlob: content hash {} != declared {}",
                                    added,
                                    hash
                                );
                            }
                            if let Some(handle) = self.topics.get(&group_id) {
                                let _ = handle.cmd_tx.send(ActorMessage::Domain(
                                    TopicCommand::AnnounceBlob { hash: added.to_string() },
                                ));
                            } else {
                                tracing::warn!(
                                    "⚠️ [NET] SeedAndAnnounceBlob: no TopicActor for group {}",
                                    &group_id[..16.min(group_id.len())]
                                );
                            }
                        }
                        Err(e) => tracing::error!("🔴 [NET] swarm add for plugin seed failed: {}", e),
                    },
                    Err(e) => tracing::error!("🔴 [NET] reading plugin {} to seed failed: {}", path, e),
                }
            }

            NetworkCommand::StartChatStream { peer_id, workspace_id } => {
                tracing::info!("💬 [NET] StartChatStream with {}", &peer_id[..16]);

                match self.ensure_dm_stream(&peer_id).await {
                    Ok(_) => {
                        let _ = self.event_tx.send(SwiftEvent::ChatStreamReady {
                            peer_id,
                            workspace_id,
                        });
                    }
                    Err(e) => {
                        tracing::error!("🔴 [NET] Failed to start DM stream: {}", e);
                    }
                }
            }

            NetworkCommand::SendDirectChat { peer_id, workspace_id, message, parent_id, attachment } => {
                eprintln!("💬 [NET] SendDirectChat:");
                eprintln!("   peer_id: {}...", &peer_id[..16.min(peer_id.len())]);
                eprintln!("   message: {}...", &message[..50.min(message.len())]);
                if let Some(ref att) = attachment {
                    eprintln!("   📎 attachment: {} ({} bytes)", att.name, att.size);
                }
                tracing::info!("💬 [NET] SendDirectChat to {}", &peer_id[..16]);

                let dm = DirectMessage {
                    id: blake3::hash(
                        format!("dm:{}-{}-{}", peer_id, message, chrono::Utc::now()).as_bytes()
                    ).to_hex().to_string(),
                    workspace_id: Some(workspace_id.clone()),
                    message: message.clone(),
                    parent_id,
                    timestamp: chrono::Utc::now().timestamp(),
                    attachment,
                };

                eprintln!("💬 [NET] Created DM id: {}...", &dm.id[..16]);

                match self.ensure_dm_stream(&peer_id).await {
                    Ok(sender) => {
                        eprintln!("💬 [NET] ✓ DM stream established, sending...");
                        if let Err(e) = sender.send(dm.clone()) {
                            eprintln!("💬 [NET] 🔴 Failed to send DM to channel: {}", e);
                            tracing::error!("🔴 [NET] Failed to send DM: {}", e);
                        } else {
                            eprintln!("💬 [NET] ✓ DM sent to channel");
                            // Store locally and emit event
                            let _ = storage::dm_insert(
                                &dm.id,
                                &peer_id,
                                &dm.message,
                                dm.timestamp,
                                false, // outgoing
                            );

                            let _ = self.event_tx.send(SwiftEvent::DirectMessageReceived {
                                id: dm.id,
                                peer_id,
                                message,
                                timestamp: dm.timestamp,
                                is_incoming: false,
                            });
                            eprintln!("💬 [NET] ✓ DM stored and event emitted");
                        }
                    }
                    Err(e) => {
                        eprintln!("💬 [NET] 🔴 Failed to establish DM stream: {}", e);
                        tracing::error!("🔴 [NET] Failed to establish DM stream: {}", e);
                    }
                }
            }

            NetworkCommand::DissolveGroup { id } => {
                // Owner dissolved group - topic actor already broadcast dissolution
                // Now clean up local state

                // Remove topic actor
                if let Some(handle) = self.topics.remove(&id) {
                    let _ = handle.cmd_tx.send(ActorMessage::System(SystemCommand::PoisonPill));
                }

                // Remove from snapshot tracking
                self.groups_needing_snapshot.remove(&id);

                // Tell discovery
                if let Some(ref handle) = self.discovery_handle {
                    let _ = handle.cmd_tx.send(ActorMessage::Domain(
                        DiscoveryCommand::LeaveGroup(id)
                    ));
                }
            }

            NetworkCommand::LeaveGroup { id } => {
                // Non-owner left group - local cleanup only, no broadcast needed

                // Remove topic actor
                if let Some(handle) = self.topics.remove(&id) {
                    let _ = handle.cmd_tx.send(ActorMessage::System(SystemCommand::PoisonPill));
                }

                // Remove from snapshot tracking
                self.groups_needing_snapshot.remove(&id);

                // Tell discovery
                if let Some(ref handle) = self.discovery_handle {
                    let _ = handle.cmd_tx.send(ActorMessage::Domain(
                        DiscoveryCommand::LeaveGroup(id)
                    ));
                }
            }

            NetworkCommand::DissolveWorkspace { id: _, group_id: _ } => {
                // Owner dissolved workspace - already broadcast via topic actor
                // Nothing to do at network level
            }

            NetworkCommand::LeaveWorkspace { id: _ } => {
                // Non-owner left workspace - local cleanup only
                // Nothing to do at network level
            }

            NetworkCommand::DissolveBoard { id: _, group_id: _ } => {
                // Owner dissolved board - already broadcast via topic actor
                // Nothing to do at network level
            }

            NetworkCommand::LeaveBoard { id: _ } => {
                // Non-owner left board - local cleanup only
                // Nothing to do at network level
            }

            NetworkCommand::ResumePendingTransfers => {
                tracing::info!("📥 [NET] ResumePendingTransfers");

                // Get all pending transfers from storage
                if let Ok(transfers) = storage::transfer_list_pending() {
                    for (file_id, hash, source_peer, bytes_received) in transfers {
                        // Route to appropriate topic actor
                        if let Some(group_id) = storage::file_get_group_id(&file_id)
                            && let Some(handle) = self.topics.get(&group_id)
                        {
                            let _ = handle.cmd_tx.send(ActorMessage::Domain(
                                TopicCommand::DownloadFile {
                                    file_id,
                                    hash,
                                    source_peer,
                                    resume_offset: bytes_received,
                                },
                            ));
                        }
                    }
                }
            }

            // Pass-through commands handled elsewhere
            NetworkCommand::UploadToGroup { .. } |
            NetworkCommand::UploadToWorkspace { .. } |
            NetworkCommand::DeleteChat { .. } => {
                // These are handled by CommandActor or TopicActor
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DISCOVERY COMMAND HANDLING (from DiscoveryActor)
    // ═══════════════════════════════════════════════════════════════════════

    async fn handle_discovery_cmd(&mut self, cmd: DiscoveryNetworkCmd) {
        match cmd {
            DiscoveryNetworkCmd::JoinPeerToTopic { group_id, peer } => {
                tracing::info!(
                    "🔗 [NET] JoinPeerToTopic: {} → {}",
                    &peer.to_string()[..16],
                    &group_id[..16.min(group_id.len())]
                );

                if let Some(handle) = self.topics.get(&group_id) {
                    let _ = handle.cmd_tx.send(ActorMessage::Domain(
                        TopicCommand::JoinPeers(vec![peer])
                    ));

                    // Update peers_per_group
                    let mut peers = self.peers_per_group.lock_safe();
                    peers.entry(group_id).or_default().insert(peer);
                }
            }

            DiscoveryNetworkCmd::JoinPeersToTopic { group_id, peers } => {
                tracing::info!(
                    "🔗 [NET] JoinPeersToTopic: {} peers → {}",
                    peers.len(),
                    &group_id[..16.min(group_id.len())]
                );

                if let Some(handle) = self.topics.get(&group_id) {
                    let _ = handle.cmd_tx.send(ActorMessage::Domain(
                        TopicCommand::JoinPeers(peers.clone())
                    ));

                    // Update peers_per_group
                    let mut peers_map = self.peers_per_group.lock_safe();
                    let group_peers = peers_map.entry(group_id).or_default();
                    for p in peers {
                        group_peers.insert(p);
                    }
                }
            }

            DiscoveryNetworkCmd::EnsureTopicExists { group_id } => {
                if !self.topics.contains_key(&group_id) {
                    let _ = self.spawn_topic_actor(&group_id, vec![], None).await;
                }
            }

            DiscoveryNetworkCmd::PeerDiscovered { peer_id, groups } => {
                tracing::info!(
                    "👋 [NET] PeerDiscovered: {} in {} shared groups",
                    &peer_id[..16],
                    groups.len()
                );
                // Could emit event to Swift here if needed
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // MESH SEEDING (MESH_HARDENING §2)
    // ═══════════════════════════════════════════════════════════════════════

    /// The ONE seeding pipeline (§2): make a peer's full `EndpointAddr` resolvable, persist it for
    /// rejoin, and route it into a group's gossip topic so `NeighborUp` can fire. Every seed source
    /// (mDNS / QR-inviter / persisted known-peers / bootstrap / Lens) funnels through here, so there
    /// is exactly one place that turns "an address" into "a present peer in a topic's bootstrap set".
    async fn seed_peer_into_group(&mut self, group_id: &str, addr: EndpointAddr) {
        let peer = addr.id;
        // 1. Make the peer dialable out of band — the mechanism `cyan_node.rs` already proves.
        self.static_discovery.add_endpoint_info(addr.clone());
        // 2. Persist for rejoin re-seeding (§2.3). Serialize is infallible for EndpointAddr in practice.
        if let Ok(addr_json) = serde_json::to_string(&addr) {
            let _ = storage::group_known_peer_upsert(group_id, &peer.to_string(), &addr_json);
        }
        // 3. Route the peer into the group topic — spawning it if we have not joined the group yet.
        if self.topics.contains_key(group_id) {
            if let Some(handle) = self.topics.get(group_id) {
                let _ = handle
                    .cmd_tx
                    .send(ActorMessage::Domain(TopicCommand::JoinPeers(vec![peer])));
            }
        } else {
            let _ = self.spawn_topic_actor(group_id, vec![peer], None).await;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // TOPIC ACTOR MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    async fn spawn_topic_actor(
        &mut self,
        group_id: &str,
        initial_peers: Vec<PublicKey>,
        grant: Option<String>,
    ) -> Result<()> {
        // Don't spawn duplicate
        if self.topics.contains_key(group_id) {
            eprintln!("🔍 [TOPIC-SPAWN] TopicActor already exists for {}...", &group_id[..16.min(group_id.len())]);
            tracing::debug!("🔍 [TOPIC-SPAWN] TopicActor already exists for {}", &group_id[..16.min(group_id.len())]);
            return Ok(());
        }

        eprintln!("🚀 [TOPIC-SPAWN-1] Spawning TopicActor for {}...", &group_id[..16.min(group_id.len())]);
        eprintln!("   Initial peers: {}", initial_peers.len());
        for (i, peer) in initial_peers.iter().enumerate() {
            eprintln!("   Peer {}: {}...", i, &peer.to_string()[..16]);
        }

        tracing::info!(
            "🚀 [TOPIC-SPAWN-1] Spawning TopicActor for {} with {} initial peers",
            &group_id[..16.min(group_id.len())],
            initial_peers.len()
        );

        let handle = TopicActor::spawn(
            self.node_id.clone(),
            group_id.to_string(),
            self.endpoint.clone(),
            self.gossip.clone(),
            initial_peers,
            self.topic_network_tx.clone(),
            self.event_tx.clone(),
            self.swarm.clone(),
            self.authorizer.clone(),
            grant,
            self.peers_per_group.clone(),
        ).await?;

        eprintln!("🚀 [TOPIC-SPAWN-2] ✓ TopicActor spawned, inserting into topics map");
        self.topics.insert(group_id.to_string(), handle);

        // Initialize peers_per_group entry
        self.peers_per_group.lock_safe()
            .entry(group_id.to_string())
            .or_default();

        // Persisted-peer re-seed (§2.3): re-feed every saved NodeAddr for this group so the topic
        // re-forms on rejoin/restart without needing bootstrap/relay reachable. add_endpoint_info
        // makes each resolvable; JoinPeers routes them into the freshly-subscribed topic. No-op for
        // a group with no saved peers (a brand-new group), so first-join behavior is unchanged.
        for (peer_id, addr_json) in storage::group_known_peers_list(group_id) {
            if let Ok(addr) = serde_json::from_str::<EndpointAddr>(&addr_json) {
                self.static_discovery.add_endpoint_info(addr);
            }
            if let Ok(pk) = PublicKey::from_str(&peer_id)
                && let Some(handle) = self.topics.get(group_id)
            {
                let _ = handle
                    .cmd_tx
                    .send(ActorMessage::Domain(TopicCommand::JoinPeers(vec![pk])));
            }
        }

        eprintln!("🚀 [TOPIC-SPAWN-3] ✓ State initialized for group");
        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DM STREAM MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    /// Ensure we have a DM stream with a peer, creating one if needed
    async fn ensure_dm_stream(&mut self, peer_id: &str) -> Result<UnboundedSender<DirectMessage>> {
        eprintln!("💬 [DM] ensure_dm_stream for peer {}...", &peer_id[..16.min(peer_id.len())]);

        // Check existing
        {
            let senders = self.dm_senders.lock_safe();
            if let Some(sender) = senders.get(peer_id) {
                eprintln!("💬 [DM] ✓ Reusing existing stream");
                return Ok(sender.clone());
            }
        }

        eprintln!("💬 [DM] No existing stream, opening new connection...");

        // Open new connection
        let pk = PublicKey::from_str(peer_id)?;

        tracing::info!("💬 [NET] Opening DM stream to {}", &peer_id[..16]);

        let conn: Connection = tokio::time::timeout(
            Duration::from_secs(10),
            self.endpoint.connect(pk, DM_ALPN)
        ).await
            .map_err(|_| anyhow!("DM connection timeout"))?
            .map_err(|e| anyhow!("DM connect failed: {}", e))?;

        eprintln!("💬 [DM] ✓ QUIC connection established, opening bi-stream...");

        let (send_stream, recv_stream) = conn.open_bi().await?;

        eprintln!("💬 [DM] ✓ Bi-stream opened");

        // Create channel for outbound messages
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn bidirectional handler for OUTBOUND connection
        let peer_id_clone = peer_id.to_string();
        let event_tx = self.event_tx.clone();
        let self_cmd_tx = self.self_cmd_tx.clone();

        tokio::spawn(async move {
            handle_dm_stream_with_streams(
                peer_id_clone,
                send_stream,
                recv_stream,
                rx,
                event_tx,
                self_cmd_tx,
            ).await;
        });

        self.dm_senders.lock_safe().insert(peer_id.to_string(), tx.clone());

        eprintln!("💬 [DM] ✓ DM stream handler spawned and registered");

        Ok(tx)
    }

    // NOTE: spawn_dm_acceptor and spawn_protocol_acceptor removed
    // Router handles all protocol acceptance now via ProtocolHandler trait

    // ═══════════════════════════════════════════════════════════════════════
    // PROFILE BROADCAST
    // ═══════════════════════════════════════════════════════════════════════

    fn broadcast_profile_to_all_groups(&self) {
        // Get profile from storage
        if let Some(profile) = storage::profile_get(&self.node_id) {
            let event = NetworkEvent::ProfileUpdated {
                node_id: self.node_id.clone(),
                display_name: profile.0,
                avatar_hash: profile.1,
            };

            // Broadcast to all topics
            for (group_id, handle) in &self.topics {
                let _ = handle.cmd_tx.send(ActorMessage::Domain(
                    TopicCommand::Broadcast(event.clone())
                ));
                tracing::debug!(
                    "📤 [NET] Broadcast profile to group {}",
                    &group_id[..16.min(group_id.len())]
                );
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// DM STREAM HANDLER (called by DmHandler ProtocolHandler)
// ═══════════════════════════════════════════════════════════════════════════

/// On receiving a DM that carries an attachment, share that file into the message's scope and
/// fetch it from the sender. Registers the file locally (best-effort, so it appears in the
/// workspace/group the chat was posted to even if we've never seen it) and enqueues a
/// `RequestFileDownload` from the sending peer. Both DM receive paths funnel through here.
/// No-op when the message has no attachment.
fn fetch_attachment_into_scope(
    dm: &DirectMessage,
    peer_id: &str,
    self_cmd_tx: &UnboundedSender<NetworkCommand>,
) {
    let Some(attachment) = dm.attachment.clone() else {
        return;
    };
    tracing::info!(
        "📎 [DM] Message has attachment: {} ({} bytes) — sharing into scope",
        attachment.name,
        attachment.size
    );

    // Make the file locally known in the message's scope if we don't already have it, so it
    // belongs to the workspace/group the chat was posted to (and `RequestFileDownload` can
    // route it via the file's group). No-op when the file is already in our DB.
    if storage::file_get_group_id(&attachment.file_id).is_none() {
        let group_id = dm
            .workspace_id
            .as_deref()
            .and_then(storage::workspace_get_group_id);
        let _ = storage::file_insert_simple(
            &attachment.file_id,
            group_id.as_deref(),
            dm.workspace_id.as_deref(),
            None,
            &attachment.name,
            &attachment.hash,
            attachment.size,
            Some(peer_id),
            dm.timestamp,
        );
    }

    // Fetch the bytes from the sender into that scope.
    if let Err(e) = self_cmd_tx.send(NetworkCommand::RequestFileDownload {
        file_id: attachment.file_id,
        hash: attachment.hash,
        source_peer: peer_id.to_string(),
        resume_offset: 0,
    }) {
        tracing::warn!("⚠️ [DM] Failed to enqueue attachment download: {}", e);
    }
}

/// Handle incoming DM connection from ProtocolHandler
async fn handle_dm_stream(
    conn: Connection,
    peer_id: String,
    dm_senders: Arc<std::sync::Mutex<HashMap<String, UnboundedSender<DirectMessage>>>>,
    event_tx: UnboundedSender<SwiftEvent>,
    self_cmd_tx: UnboundedSender<NetworkCommand>,
) -> Result<()> {
    tracing::info!("💬 [DM] Accepting bi-stream from {}", &peer_id[..16]);

    let (mut send, mut recv) = conn.accept_bi().await?;

    // Create outbound channel for this peer
    let (tx, mut outbound_rx) = mpsc::unbounded_channel();

    // Register sender for bidirectional communication
    dm_senders.lock_safe().insert(peer_id.clone(), tx);

    // Emit event that stream is ready
    let _ = event_tx.send(SwiftEvent::ChatStreamReady {
        peer_id: peer_id.clone(),
        workspace_id: String::new(),
    });

    tracing::info!("💬 [DM] Stream handler started for {}", &peer_id[..16]);

    loop {
        tokio::select! {
            // Incoming message
            result = read_dm_frame(&mut recv) => {
                match result {
                    Ok(dm) => {
                        tracing::info!(
                            "💬 [DM] Received from {}: {}",
                            &peer_id[..16],
                            &dm.message[..50.min(dm.message.len())]
                        );

                        // Share any attachment into the message's scope and fetch it.
                        fetch_attachment_into_scope(&dm, &peer_id, &self_cmd_tx);

                        // Store in DB
                        let _ = storage::dm_insert(
                            &dm.id,
                            &peer_id,
                            &dm.message,
                            dm.timestamp,
                            true, // incoming
                        );

                        // Emit event to Swift
                        let _ = event_tx.send(SwiftEvent::DirectMessageReceived {
                            id: dm.id,
                            peer_id: peer_id.clone(),
                            message: dm.message,
                            timestamp: dm.timestamp,
                            is_incoming: true,
                        });
                    }
                    Err(e) => {
                        tracing::info!("💬 [DM] Stream closed with {}: {}", &peer_id[..16], e);
                        break;
                    }
                }
            }

            // Outgoing message
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(dm) => {
                        if let Err(e) = write_dm_frame(&mut send, &dm).await {
                            tracing::error!("🔴 [DM] Failed to send to {}: {}", &peer_id[..16], e);
                            break;
                        }
                    }
                    None => {
                        tracing::info!("💬 [DM] Outbound channel closed for {}", &peer_id[..16]);
                        break;
                    }
                }
            }
        }
    }

    // Cleanup
    dm_senders.lock_safe().remove(&peer_id);
    let _ = event_tx.send(SwiftEvent::ChatStreamClosed { peer_id: peer_id.clone() });
    tracing::info!("💬 [DM] Stream handler stopped for {}", &peer_id[..16]);

    Ok(())
}

/// Handle OUTBOUND DM connection (we already have streams from open_bi)
async fn handle_dm_stream_with_streams(
    peer_id: String,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    mut outbound_rx: UnboundedReceiver<DirectMessage>,
    event_tx: UnboundedSender<SwiftEvent>,
    self_cmd_tx: UnboundedSender<NetworkCommand>,
) {
    tracing::info!("💬 [DM] Outbound stream handler started for {}", &peer_id[..16]);

    loop {
        tokio::select! {
            // Incoming message
            result = read_dm_frame(&mut recv) => {
                match result {
                    Ok(dm) => {
                        tracing::info!(
                            "💬 [DM] Received from {}: {}",
                            &peer_id[..16],
                            &dm.message[..50.min(dm.message.len())]
                        );

                        // Share any attachment into the message's scope and fetch it.
                        fetch_attachment_into_scope(&dm, &peer_id, &self_cmd_tx);

                        // Store in DB
                        let _ = storage::dm_insert(
                            &dm.id,
                            &peer_id,
                            &dm.message,
                            dm.timestamp,
                            true, // incoming
                        );

                        // Emit event to Swift
                        let _ = event_tx.send(SwiftEvent::DirectMessageReceived {
                            id: dm.id,
                            peer_id: peer_id.clone(),
                            message: dm.message,
                            timestamp: dm.timestamp,
                            is_incoming: true,
                        });
                    }
                    Err(e) => {
                        tracing::info!("💬 [DM] Stream closed with {}: {}", &peer_id[..16], e);
                        break;
                    }
                }
            }

            // Outgoing message
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(dm) => {
                        if let Err(e) = write_dm_frame(&mut send, &dm).await {
                            tracing::error!("🔴 [DM] Failed to send to {}: {}", &peer_id[..16], e);
                            break;
                        }
                    }
                    None => {
                        tracing::info!("💬 [DM] Outbound channel closed for {}", &peer_id[..16]);
                        break;
                    }
                }
            }
        }
    }

    let _ = event_tx.send(SwiftEvent::ChatStreamClosed { peer_id: peer_id.clone() });
    tracing::info!("💬 [DM] Outbound stream handler stopped for {}", &peer_id[..16]);
}

/// Read a length-prefixed JSON frame using read_chunk for efficiency
async fn read_dm_frame(recv: &mut iroh::endpoint::RecvStream) -> Result<DirectMessage> {
    // Read length prefix (4 bytes)
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 10 * 1024 * 1024 {
        return Err(anyhow!("DM frame too large: {} bytes", len));
    }

    // Read data using read_chunk for efficiency
    let mut data = Vec::with_capacity(len);
    while data.len() < len {
        let remaining = len - data.len();
        match recv.read_chunk(remaining, true).await? {
            Some(chunk) => {
                data.extend_from_slice(&chunk.bytes);
            }
            None => {
                return Err(anyhow!("Stream ended before complete frame"));
            }
        }
    }

    Ok(serde_json::from_slice(&data)?)
}

/// Write a length-prefixed JSON frame using write_chunk for efficiency
async fn write_dm_frame(send: &mut iroh::endpoint::SendStream, dm: &DirectMessage) -> Result<()> {
    let data = serde_json::to_vec(dm)?;
    let len = (data.len() as u32).to_be_bytes();

    // Write length prefix
    send.write_chunk(Bytes::copy_from_slice(&len)).await?;

    // Write data
    send.write_chunk(Bytes::from(data)).await?;

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// FILE TRANSFER SERVER (handles incoming file requests)
// ═══════════════════════════════════════════════════════════════════════════

/// Accept every bi-directional stream a downloader opens on this connection and serve
/// each independently. A legacy client opens ONE stream (`Request`) — served exactly as
/// before; the pipelined parallel client opens M streams (`RequestStriped`) that this
/// loop serves concurrently. The loop ends when the client closes the connection.
async fn handle_file_transfer_server(
    conn: Connection,
    _event_tx: UnboundedSender<SwiftEvent>,
) -> Result<()> {
    let mut served_any = false;
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                served_any = true;
                tokio::spawn(async move {
                    if let Err(e) = serve_file_stream(send, recv).await {
                        tracing::warn!("🔴 [FILE] transfer stream error: {}", e);
                    }
                });
            }
            // Client closed the connection — the normal end of a transfer.
            Err(e) => {
                if !served_any {
                    return Err(anyhow!("file transfer connection closed early: {e}"));
                }
                return Ok(());
            }
        }
    }
}

/// Serve one file-transfer stream: a legacy whole-remainder `Request` or one strided
/// slice (`RequestStriped`) of the pipelined parallel transfer.
async fn serve_file_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    use crate::models::protocol::FileTransferMsg;

    // Read request
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut req_data = vec![0u8; len];
    recv.read_exact(&mut req_data).await?;

    let request: FileTransferMsg = serde_json::from_slice(&req_data)?;

    match request {
        FileTransferMsg::Request { file_id, hash, offset: resume_offset } => {
            tracing::info!(
                "📤 [FILE] Serving file {} from offset {}",
                &file_id[..16.min(file_id.len())],
                resume_offset
            );

            // Look up file in storage
            match storage::file_get_for_transfer(&file_id, &hash) {
                Some((name, local_path, total_size)) => {
                    if local_path.is_empty() {
                        // File exists but not downloaded locally
                        let not_found = FileTransferMsg::NotFound { file_id };
                        let data = serde_json::to_vec(&not_found)?;
                        let len = (data.len() as u32).to_be_bytes();
                        send.write_chunk(Bytes::copy_from_slice(&len)).await?;
                        send.write_chunk(Bytes::from(data)).await?;
                        return Ok(());
                    }

                    // Send header
                    let header = FileTransferMsg::Header {
                        file_id: file_id.clone(),
                        file_name: name,
                        total_size,
                        hash: hash.clone(),
                        byte_offset: resume_offset,
                        byte_length: total_size - resume_offset,
                    };

                    let header_data = serde_json::to_vec(&header)?;
                    let len = (header_data.len() as u32).to_be_bytes();
                    send.write_chunk(Bytes::copy_from_slice(&len)).await?;
                    send.write_chunk(Bytes::from(header_data)).await?;

                    // Stream file content using write_chunk
                    let mut file = tokio::fs::File::open(&local_path).await?;

                    if resume_offset > 0 {
                        use tokio::io::AsyncSeekExt;
                        file.seek(std::io::SeekFrom::Start(resume_offset)).await?;
                    }

                    use tokio::io::AsyncReadExt;
                    let mut buf = vec![0u8; 64 * 1024]; // 64KB chunks
                    let mut sent = 0u64;

                    loop {
                        let n = file.read(&mut buf).await?;
                        if n == 0 {
                            break;
                        }

                        send.write_chunk(Bytes::copy_from_slice(&buf[..n])).await?;
                        sent += n as u64;
                    }

                    tracing::info!("📤 [FILE] Sent {} bytes for {}", sent, &file_id[..16.min(file_id.len())]);

                    // Send complete acknowledgment
                    let complete = FileTransferMsg::Complete { file_id, hash };
                    let data = serde_json::to_vec(&complete)?;
                    let len = (data.len() as u32).to_be_bytes();
                    send.write_chunk(Bytes::copy_from_slice(&len)).await?;
                    send.write_chunk(Bytes::from(data)).await?;
                }
                None => {
                    let not_found = FileTransferMsg::NotFound { file_id };
                    let data = serde_json::to_vec(&not_found)?;
                    let len = (data.len() as u32).to_be_bytes();
                    send.write_chunk(Bytes::copy_from_slice(&len)).await?;
                    send.write_chunk(Bytes::from(data)).await?;
                }
            }
        }
        FileTransferMsg::RequestStriped { file_id, hash, chunk_size, stride, index } => {
            serve_striped(&mut send, &file_id, &hash, chunk_size, stride, index).await?;
        }
        _ => {
            tracing::warn!("⚠️ [FILE] Unexpected message type");
        }
    }

    // CRITICAL: Signal end of stream and wait for peer to receive
    send.finish()?;
    let _ = send.stopped().await;

    Ok(())
}

/// Serve one strided slice of a file: chunks `index, index+stride, …` of `chunk_size`
/// bytes. Seeky 256 KB-granularity reads from the staged file (page-cache friendly);
/// the QUIC stream window keeps N chunks pipelined in flight — no per-chunk lockstep.
async fn serve_striped(
    send: &mut iroh::endpoint::SendStream,
    file_id: &str,
    hash: &str,
    chunk_size: u64,
    stride: u32,
    index: u32,
) -> Result<()> {
    use crate::models::protocol::FileTransferMsg;

    let send_msg = async |send: &mut iroh::endpoint::SendStream, msg: &FileTransferMsg| -> Result<()> {
        let data = serde_json::to_vec(msg)?;
        let len = (data.len() as u32).to_be_bytes();
        send.write_chunk(Bytes::copy_from_slice(&len)).await?;
        send.write_chunk(Bytes::copy_from_slice(&data)).await?;
        Ok(())
    };

    if chunk_size == 0 || stride == 0 || index >= stride {
        let msg = FileTransferMsg::Error {
            file_id: file_id.to_string(),
            message: format!("bad stripe geometry: chunk_size={chunk_size} stride={stride} index={index}"),
        };
        send_msg(send, &msg).await?;
        return Ok(());
    }

    let Some((name, local_path, total_size)) = storage::file_get_for_transfer(file_id, hash) else {
        send_msg(send, &FileTransferMsg::NotFound { file_id: file_id.to_string() }).await?;
        return Ok(());
    };
    if local_path.is_empty() {
        send_msg(send, &FileTransferMsg::NotFound { file_id: file_id.to_string() }).await?;
        return Ok(());
    }

    // This stream's byte total: every `stride`-th chunk starting at `index`.
    let n_chunks = total_size.div_ceil(chunk_size);
    let mut stream_len = 0u64;
    let mut k = index as u64;
    while k < n_chunks {
        stream_len += chunk_size.min(total_size - k * chunk_size);
        k += stride as u64;
    }

    send_msg(
        send,
        &FileTransferMsg::Header {
            file_id: file_id.to_string(),
            file_name: name,
            total_size,
            hash: hash.to_string(),
            byte_offset: (index as u64) * chunk_size,
            byte_length: stream_len,
        },
    )
    .await?;

    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = tokio::fs::File::open(&local_path).await?;
    let mut buf = vec![0u8; chunk_size as usize];
    let mut k = index as u64;
    let mut sent = 0u64;
    while k * chunk_size < total_size {
        let pos = k * chunk_size;
        let n = chunk_size.min(total_size - pos) as usize;
        file.seek(std::io::SeekFrom::Start(pos)).await?;
        file.read_exact(&mut buf[..n]).await?;
        send.write_chunk(Bytes::copy_from_slice(&buf[..n])).await?;
        sent += n as u64;
        k += stride as u64;
    }

    tracing::info!(
        "📤 [FILE] Sent stripe {}/{} ({} bytes) for {}",
        index,
        stride,
        sent,
        &file_id[..16.min(file_id.len())]
    );
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// SNAPSHOT SERVER (handles incoming snapshot requests)
// ═══════════════════════════════════════════════════════════════════════════

async fn handle_snapshot_server(
    conn: Connection,
    _node_id: String,
    _event_tx: UnboundedSender<SwiftEvent>,
    authorizer: Arc<std::sync::Mutex<MeshAuthorizer>>,
) -> Result<()> {
    use crate::identity::SnapshotDenial;
    use crate::models::protocol::SnapshotRequest;

    let peer_id = conn.remote_id().to_string();
    eprintln!("📤 [SNAP] ════════════════════════════════════════════════════════");
    eprintln!("📤 [SNAP] SNAPSHOT SERVER - handling request from {}...", &peer_id[..16]);
    eprintln!("📤 [SNAP] ════════════════════════════════════════════════════════");

    let (mut send, mut recv) = conn.accept_bi().await?;
    eprintln!("   ✓ Bidirectional stream opened");

    // Read the request frame (length-prefixed). New peers send a JSON `SnapshotRequest`;
    // legacy peers send the raw group_id bytes — fall back to that if JSON parse fails.
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut req_bytes = vec![0u8; len];
    recv.read_exact(&mut req_bytes).await?;

    let (group_id, grant_qr, since): (String, Option<String>, Option<i64>) =
        match serde_json::from_slice::<SnapshotRequest>(&req_bytes) {
            Ok(req) => (req.group_id, req.grant, req.since),
            Err(_) => (String::from_utf8(req_bytes)?, None, None),
        };

    eprintln!("   → Requested group: {}...", &group_id[..16.min(group_id.len())]);
    tracing::info!("📤 [SNAP] Sending snapshot for group {}", &group_id[..16.min(group_id.len())]);

    // ── Join-time read gate ──────────────────────────────────────────────────────────────
    // Fail-open unless the group is enforced. For an enforced group the joiner must present a
    // valid grant FOR THIS group; otherwise we serve NOTHING (finish the stream with no frames),
    // which the client reads as a refused snapshot. This — together with the per-group snapshot
    // build below — is the "zero leakage of the holder's other groups" property.
    let decision = {
        let grant = grant_qr.as_deref().and_then(|qr| Grant::from_qr_payload(qr).ok());
        let mut auth = authorizer
            .lock()
            .map_err(|_| anyhow!("authorizer mutex poisoned"))?;
        auth.authorize_snapshot(&peer_id, &group_id, grant.as_ref())
    };
    if let Err(reason) = decision {
        // obs only — assertions are on the receiver's storage, never on log lines.
        eprintln!(
            "⛔ [SNAP] target=obs tenant={} peer={}... action=snapshot decision=deny reason={:?}",
            &group_id[..16.min(group_id.len())],
            &peer_id[..16.min(peer_id.len())],
            match &reason {
                SnapshotDenial::NoGrant => "no_grant".to_string(),
                SnapshotDenial::WrongGroup => "wrong_group".to_string(),
                SnapshotDenial::Verify(e) => format!("verify:{:?}", e),
            }
        );
        tracing::warn!("⛔ [SNAP] Refused snapshot for enforced group (denied)");
        send.finish()?;
        let _ = send.stopped().await;
        return Ok(());
    }

    // Build the snapshot via the shared builder (MESH_HARDENING §5). `since=None` ⇒ the full
    // snapshot (unchanged cold-start behavior); `since=Some(t)` ⇒ only the rows newer than the
    // requester's high-water mark — the incremental catch-up. Same `SnapshotFrame` wire shape
    // and ORDER either way, so the existing apply path is untouched.
    let frames = crate::snapshot::build_snapshot_frames(&group_id, since)?;
    if frames.is_empty() {
        eprintln!("⚠️ [SNAP] Group not found in DB: {}...", &group_id[..16.min(group_id.len())]);
        return Ok(());
    }

    let rows = crate::snapshot::frames_row_count(&frames);
    eprintln!(
        "   📊 Serving {} snapshot for {}... ({} data rows, since={:?})",
        if since.is_some() { "INCREMENTAL" } else { "FULL" },
        &group_id[..16.min(group_id.len())],
        rows,
        since,
    );

    for frame in &frames {
        let data = serde_json::to_vec(frame)?;
        let len = (data.len() as u32).to_be_bytes();
        send.write_chunk(Bytes::copy_from_slice(&len)).await?;
        send.write_chunk(Bytes::from(data)).await?;
    }

    // CRITICAL: Signal end of stream and wait for peer to receive.
    send.finish()?;
    let _ = send.stopped().await;

    // Observability only (behavior-neutral). `record_snapshot_served` keeps the existing
    // "snapshot under load" oracle; the incremental/full split is the §5 "pulled only the
    // delta, not a full re-snapshot" oracle.
    crate::metrics::record_snapshot_served();
    if since.is_some() {
        crate::metrics::record_incremental_served(rows);
    } else {
        crate::metrics::record_full_served(rows);
    }

    eprintln!("✅ [SNAP] Snapshot SENT for group {}...", &group_id[..16.min(group_id.len())]);
    tracing::info!("📤 [SNAP] Snapshot sent for group {}", &group_id[..16.min(group_id.len())]);

    Ok(())
}