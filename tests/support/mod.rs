//! MeshHarness — the keystone for the substrate test suite (SUBSTRATE_TEST_SPEC §1).
//!
//! Spins N cyan-backend nodes **in one test process** and lets a test drive them
//! (command channel) and observe them (events + `storage::*`). Every substrate
//! test file (`tests/substrate_*.rs`) imports this via `mod support;`.
//!
//! ## Two engine realities this harness has to live with
//!
//! 1. **Storage is a process-global singleton.** `cyan_backend::storage` keys every
//!    operation off a single `static DB: OnceLock<Mutex<Connection>>`, and `init_db`
//!    errors on a second call. So all in-process nodes share ONE SQLite database —
//!    there is no per-node storage. This harness therefore initialises the DB exactly
//!    once and treats it as shared. Discovery tests (G1) assert on **per-node**
//!    state — `peers_per_group` (a fresh `Arc` per node) and `PeerJoined` events —
//!    so the shared DB does not affect them. Tests that must assert on the *receiver's*
//!    storage (snapshot/delta/chat/file content, G3–G9) cannot be honestly isolated
//!    in-process with a shared DB and are `#[ignore]`d with that reason; they belong
//!    to the multi-process rig.
//!
//! 2. **`gossip.subscribe_and_join` blocks until ≥1 neighbour connects.** A node whose
//!    only bootstrap is unreachable hangs at startup. The engine now takes the
//!    discovery-topic bootstrap from each node's `DiscoveryPolicy` (PHASE 1/2 seam), so
//!    the harness can seed one node off another instead of an unreachable global
//!    default. With relay disabled, the dial's address resolution goes over **mDNS** on
//!    the loopback LAN — which is exactly the offline path (G2-LAN/G9) we want to prove.
//!
//! In-process scope: discovery (mDNS/bootstrap), and — for the storage-oracle files —
//! whatever the shared-DB constraint allows. The relay/WebSocket rungs (G2 ladder,
//! G8-R, G11) are NOT in-process (they need real network isolation / the docker rig).

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use iroh::discovery::static_provider::StaticProvider;
use iroh::{Endpoint, EndpointAddr, PublicKey, SecretKey};
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex as AsyncMutex;

use cyan_backend::models::commands::NetworkCommand;
use cyan_backend::models::events::{NetworkEvent, SwiftEvent};
use cyan_backend::models::node_config::{
    DiscoveryPolicy as EngineDiscoveryPolicy, NodeConfig as EngineNodeConfig,
    RelayPolicy as EngineRelayPolicy,
};
use cyan_backend::actors::NetworkActor;
use cyan_backend::identity::MeshAuthorizer;
use cyan_backend::storage;
use cyan_backend::swarm::BlobSwarm;

/// Per-node network policy — the in-process subset (see module docs).
#[derive(Clone, Debug)]
pub enum RelayPolicy {
    /// `RelayMode::Disabled` — LAN/offline; no relay at all (G9).
    Disabled,
    /// `RelayMode::Custom(url)` — a real (e.g. local) relay.
    Url(String),
    /// `RelayMode::Default` — n0 public relays (rarely wanted in tests).
    Default,
}

/// How a node discovers peers.
#[derive(Clone, Debug)]
pub enum DiscoveryPolicy {
    /// mDNS only — same-LAN/loopback discovery, no bootstrap (offline-friendly).
    MdnsOnly,
    /// Dial a known bootstrap node id for gossip discovery.
    Bootstrap(String), // hex node id; harness converts to PublicKey
}

/// One node's full config, handed to the (refactored) `NetworkActor::new`.
#[derive(Clone, Debug)]
pub struct NodeCfg {
    pub relay: RelayPolicy,
    pub discovery: DiscoveryPolicy,
    pub discovery_key: String, // e.g. "cyan-dev"
}

impl Default for NodeCfg {
    /// The default every test starts from: offline-friendly, mDNS, dev key.
    fn default() -> Self {
        NodeCfg {
            relay: RelayPolicy::Disabled,
            discovery: DiscoveryPolicy::MdnsOnly,
            discovery_key: "cyan-test".to_string(),
        }
    }
}

impl NodeCfg {
    fn to_engine(&self) -> EngineNodeConfig {
        EngineNodeConfig {
            relay: match &self.relay {
                RelayPolicy::Disabled => EngineRelayPolicy::Disabled,
                RelayPolicy::Url(u) => EngineRelayPolicy::Url(u.clone()),
                RelayPolicy::Default => EngineRelayPolicy::Default,
            },
            discovery: match &self.discovery {
                DiscoveryPolicy::MdnsOnly => EngineDiscoveryPolicy::MdnsOnly,
                DiscoveryPolicy::Bootstrap(id) => EngineDiscoveryPolicy::Bootstrap(id.clone()),
            },
            discovery_key: self.discovery_key.clone(),
        }
    }
}

/// A live node: its actor task, the command sink, the event stream, and its DB.
pub struct Node {
    pub name: String,
    pub node_id: String,
    cmd_tx: UnboundedSender<NetworkCommand>,
    events: Arc<AsyncMutex<UnboundedReceiver<SwiftEvent>>>,
    peers_per_group: Arc<Mutex<HashMap<String, HashSet<PublicKey>>>>,
    db_path: PathBuf,
    // Test-support seams (see module docs): used to wire loopback addresses between
    // nodes so they dial each other without depending on flaky in-process mDNS.
    endpoint: Endpoint,
    static_discovery: StaticProvider,
    // This node's content-addressed blob swarm (G10), mounted by the NetworkActor on the same
    // endpoint. Per-node store == an honest per-node oracle even under the shared SQLite DB.
    swarm: Arc<BlobSwarm>,
    // This node's mesh-write authorizer (identity/RBAC mesh half). A fresh `Arc` per node, like
    // `peers_per_group` — the honest per-node oracle for grant enforcement under the shared DB.
    authorizer: Arc<Mutex<MeshAuthorizer>>,
    // The spawned actor task. Held so a resilience test can "pull the plug" on a peer
    // via `shutdown()` — aborting this drops the actor (and the gossip/topic/router it
    // owns), which is the in-process equivalent of a peer going away.
    actor_handle: tokio::task::JoinHandle<()>,
}

impl Node {
    /// Send a command into the node (fire-and-forget).
    pub fn cmd(&self, c: NetworkCommand) {
        if let Err(e) = self.cmd_tx.send(c) {
            // The actor task is gone; nothing actionable in a test besides surfacing it.
            eprintln!("⚠️ [harness] cmd send to {} failed: {}", self.name, e);
        }
    }

    /// Convenience: drive a `JoinGroup` for `group_id`, optionally seeded with a
    /// bootstrap peer (the node whose group topic we dial into).
    pub fn join_group(&self, group_id: &str, bootstrap_peer: Option<String>) {
        self.cmd(NetworkCommand::JoinGroup {
            group_id: group_id.to_string(),
            bootstrap_peer,
            grant: None,
        });
    }

    /// Join `group_id` presenting a signed capability-grant QR payload (the invite). Used to
    /// drive the grant-gated snapshot path: the holder verifies this before serving an enforced
    /// group's snapshot.
    pub fn join_group_with_grant(
        &self,
        group_id: &str,
        bootstrap_peer: Option<String>,
        grant: Option<String>,
    ) {
        self.cmd(NetworkCommand::JoinGroup {
            group_id: group_id.to_string(),
            bootstrap_peer,
            grant,
        });
    }

    /// Ask this node to download `file_id` (blake3 `hash`) from `source_peer` over the
    /// file-transfer protocol. The node must already share the file's group (have a TopicActor).
    pub fn request_download(&self, file_id: &str, hash: &str, source_peer: &str) {
        self.cmd(NetworkCommand::RequestFileDownload {
            file_id: file_id.to_string(),
            hash: hash.to_string(),
            source_peer: source_peer.to_string(),
            resume_offset: 0,
        });
    }

    /// Await `SwiftEvent::FileDownloaded { file_id, local_path }` for `file_id`, returning
    /// the local path the bytes landed at. (The engine blake3-verifies before emitting this,
    /// so the event already implies an intact transfer; tests re-verify the bytes too.)
    pub async fn wait_file_downloaded(&self, file_id: &str, timeout: Duration) -> Result<String> {
        let want = file_id.to_string();
        let ev = self
            .wait_for(
                move |e| matches!(e, SwiftEvent::FileDownloaded { file_id, .. } if *file_id == want),
                timeout,
            )
            .await?;
        match ev {
            SwiftEvent::FileDownloaded { local_path, .. } => Ok(local_path),
            _ => unreachable!("predicate guarantees FileDownloaded"),
        }
    }

    /// Broadcast a delta/chat `NetworkEvent` into `group_id` (the live-collaboration path).
    pub fn broadcast(&self, group_id: &str, event: NetworkEvent) {
        self.cmd(NetworkCommand::Broadcast {
            group_id: group_id.to_string(),
            event,
        });
    }

    /// Await the first received `SwiftEvent::Network(..)` whose inner event matches `pred`.
    /// This is the per-node oracle for live deltas/chat: the receiver surfaces every event
    /// it gets over the mesh on its own event channel (it also persists it, but the engine's
    /// storage is process-global so the channel — not storage — is the per-node signal).
    pub async fn wait_network<F>(&self, pred: F, timeout: Duration) -> Result<NetworkEvent>
    where
        F: Fn(&NetworkEvent) -> bool,
    {
        let ev = self
            .wait_for(
                |e| matches!(e, SwiftEvent::Network(ne) if pred(ne)),
                timeout,
            )
            .await?;
        match ev {
            SwiftEvent::Network(ne) => Ok(ne),
            _ => unreachable!("predicate guarantees Network"),
        }
    }

    /// Await the first event matching `pred`, or fail after `timeout`. Non-matching
    /// events seen before the match are consumed (fine for the convergence asserts here).
    pub async fn wait_for<F>(&self, pred: F, timeout: Duration) -> Result<SwiftEvent>
    where
        F: Fn(&SwiftEvent) -> bool,
    {
        tokio::time::timeout(timeout, async {
            let mut rx = self.events.lock().await;
            loop {
                match rx.recv().await {
                    Some(ev) if pred(&ev) => return Ok(ev),
                    Some(_) => continue,
                    None => return Err(anyhow!("event channel closed for {}", self.name)),
                }
            }
        })
        .await
        .map_err(|_| anyhow!("timeout after {:?} waiting for event on {}", timeout, self.name))?
    }

    /// Convenience: await `SwiftEvent::PeerJoined { group_id, .. }` for this group.
    pub async fn wait_peer_joined(&self, group_id: &str, timeout: Duration) -> Result<String> {
        let gid = group_id.to_string();
        let ev = self
            .wait_for(
                move |e| matches!(e, SwiftEvent::PeerJoined { group_id, .. } if *group_id == gid),
                timeout,
            )
            .await?;
        match ev {
            SwiftEvent::PeerJoined { peer_id, .. } => Ok(peer_id),
            _ => unreachable!("predicate guarantees PeerJoined"),
        }
    }

    /// Convenience: await `SwiftEvent::SyncComplete { group_id }`.
    pub async fn wait_sync(&self, group_id: &str, timeout: Duration) -> Result<()> {
        let gid = group_id.to_string();
        self.wait_for(
            move |e| matches!(e, SwiftEvent::SyncComplete { group_id } if *group_id == gid),
            timeout,
        )
        .await
        .map(|_| ())
    }

    /// Whether this node has a live topic for `group_id` yet (the engine inserts the
    /// `peers_per_group` key when it spawns the group's TopicActor). Used as a deterministic
    /// "this node has joined and is past start-up group loading" signal.
    pub fn has_group(&self, group_id: &str) -> bool {
        self.peers_per_group
            .lock()
            .map(|m| m.contains_key(group_id))
            .unwrap_or(false)
    }

    /// Number of peers this node currently tracks in `group_id`. Mirrors the FFI
    /// `cyan_get_group_peer_count` (both read this same per-node `peers_per_group`). Now
    /// populated off the **live gossip neighbor set** (TopicActor NeighborUp/NeighborDown) —
    /// the same channel that carries the group's data — so it is honest presence truth.
    pub fn peers_in_group(&self, group_id: &str) -> usize {
        self.peers_per_group
            .lock()
            .map(|m| m.get(group_id).map(|s| s.len()).unwrap_or(0))
            .unwrap_or(0)
    }

    /// The peer ids this node currently tracks in `group_id`, as hex strings. Mirrors the FFI
    /// `cyan_get_group_peers` (same `peers_per_group` source) — the roster oracle.
    pub fn group_peers(&self, group_id: &str) -> Vec<String> {
        self.peers_per_group
            .lock()
            .map(|m| {
                m.get(group_id)
                    .map(|s| s.iter().map(|pk| pk.to_string()).collect())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

    /// Total peers across every group this node tracks. Mirrors the FFI
    /// `cyan_get_total_peer_count` — a node in N groups with a neighbor counts it per group.
    pub fn total_peers(&self) -> usize {
        self.peers_per_group
            .lock()
            .map(|m| m.values().map(|s| s.len()).sum())
            .unwrap_or(0)
    }

    /// Path to the (shared) SQLite db. NOTE: with the engine's process-global storage
    /// this is the SAME db for every node — see module docs.
    pub fn db(&self) -> &Path {
        &self.db_path
    }

    /// This node's resolvable address as a serialized `EndpointAddr` JSON (MESH_HARDENING §2). The
    /// seed-pipeline tests hand this to another node's `seed_group_peer` to simulate a peer learned
    /// via mDNS / QR / persisted store. Bounded: waits for a direct loopback address.
    pub async fn endpoint_addr_json(&self, timeout: Duration) -> Result<String> {
        let addr = await_direct_addr(&self.endpoint, timeout).await?;
        serde_json::to_string(&addr).map_err(|e| anyhow!("serialize EndpointAddr: {e}"))
    }

    /// Drive the engine's ONE seeding pipeline (§2): feed a resolvable peer `EndpointAddr` (JSON)
    /// into `group_id`'s topic. The engine makes it resolvable, persists it, and routes it into the
    /// topic so `NeighborUp` fires — the source-agnostic core every seed source funnels through.
    pub fn seed_group_peer(&self, group_id: &str, addr_json: &str) {
        self.cmd(NetworkCommand::SeedGroupPeer {
            group_id: group_id.to_string(),
            addr_json: addr_json.to_string(),
        });
    }

    /// The persistent roster for `group_id` as `(peer_id, name, avatar, online, last_seen)` — the
    /// exact shape `cyan_get_group_members` returns (MESH_HARDENING §3). Members come from the shared
    /// `storage::group_members` table; `online` is overlaid from THIS node's live `peers_per_group`.
    pub fn members(&self, group_id: &str) -> Vec<(String, Option<String>, Option<String>, bool, i64)> {
        let online: HashSet<String> = self
            .peers_per_group
            .lock()
            .map(|m| {
                m.get(group_id)
                    .map(|s| s.iter().map(|pk| pk.to_string()).collect())
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        storage::group_members_list(group_id)
            .into_iter()
            .map(|(peer_id, name, avatar, last_seen)| {
                let online = online.contains(&peer_id);
                (peer_id, name, avatar, online, last_seen)
            })
            .collect()
    }

    /// This node's blob swarm handle (G10). The per-node `iroh-blobs` store is the honest
    /// per-node oracle for swarm tests (`has`/`holders`) — unaffected by the shared SQLite DB.
    pub fn swarm(&self) -> Arc<BlobSwarm> {
        self.swarm.clone()
    }

    /// This node's mesh-write authorizer (identity/RBAC mesh half). The honest per-node oracle
    /// for grant enforcement: `enforce_group`, `set_admin`, `present_grant`, `authorize_write`.
    pub fn authorizer(&self) -> Arc<Mutex<MeshAuthorizer>> {
        self.authorizer.clone()
    }

    /// Announce over `group_id`'s gossip that this node holds the blob `hash` (G10 i-have).
    pub fn swarm_announce(&self, group_id: &str, hash: &str) {
        self.cmd(NetworkCommand::SwarmAnnounce {
            group_id: group_id.to_string(),
            hash: hash.to_string(),
        });
    }

    /// Ask `group_id`'s gossip which peers hold the blob `hash` (G10 who-has).
    pub fn swarm_who_has(&self, group_id: &str, hash: &str) {
        self.cmd(NetworkCommand::SwarmWhoHas {
            group_id: group_id.to_string(),
            hash: hash.to_string(),
        });
    }

    /// Seed a plugin file (`path`, Blake3 `hash`) into this node's swarm and announce it to
    /// `group_id` — the engine's `.cyanplugin` distribution hook (G10).
    pub fn seed_plugin(&self, group_id: &str, hash: &str, path: &str) {
        self.cmd(NetworkCommand::SeedAndAnnounceBlob {
            group_id: group_id.to_string(),
            hash: hash.to_string(),
            path: path.to_string(),
        });
    }

    /// "Pull the plug" on this peer: abort its actor task (dropping the gossip/topic/router
    /// it owns) and close its iroh endpoint, then consume the node. After this returns the
    /// peer is gone from the mesh — its command sink and event stream are dropped with it.
    /// Bounded: `Endpoint::close` resolves promptly; the abort is immediate. This is the
    /// in-process model of peer churn used by `tests/substrate_resilience.rs`.
    pub async fn shutdown(self) {
        self.actor_handle.abort();
        // The actor held its own clone of the endpoint; this closes the shared endpoint so
        // open connections to peers tear down rather than lingering as half-open state.
        self.endpoint.close().await;
    }
}

/// The shared, process-global database path. The engine's `storage` is a global
/// singleton, so we initialise it exactly once and hand every node the same path.
/// The backing tempdir is intentionally leaked so it outlives every node.
static SHARED_DB: OnceLock<PathBuf> = OnceLock::new();

/// Ensure the process-global substrate DB is initialised (base schema + `storage::init_db`)
/// and return its path. For storage-oracle UNIT tests that exercise `storage`/`snapshot`/
/// `group_bundle` directly without spinning nodes. Idempotent (the `OnceLock` inits once).
pub fn ensure_db() -> PathBuf {
    shared_db()
}

fn shared_db() -> PathBuf {
    SHARED_DB
        .get_or_init(|| {
            let dir = tempfile::tempdir().expect("create tempdir for shared substrate db");
            let path = dir.path().join("substrate.db");
            init_base_schema(&path).expect("init base schema");
            storage::init_db(path.to_str().expect("utf8 db path")).expect("storage::init_db");
            // The engine writes downloads under DATA_DIR (a process-global OnceCell); point
            // it at this leaked tempdir so file transfers have a real place to land.
            let data_dir = dir.path().join("data");
            std::fs::create_dir_all(&data_dir).expect("create data dir");
            let _ = cyan_backend::DATA_DIR.set(data_dir);
            std::mem::forget(dir); // keep the dir alive for the whole test process
            path
        })
        .clone()
}

/// Stage a file on a "host" node so it can be served over the file-transfer protocol:
/// write `content` to disk, compute its blake3, and register it in the (shared) DB with
/// that local path so `file_get_for_transfer` finds it. Returns the blake3 hex hash.
/// `source_peer` is the host's node id (the address a downloader dials).
pub fn stage_file(
    file_id: &str,
    group_id: &str,
    workspace_id: Option<&str>,
    board_id: Option<&str>,
    content: &[u8],
    source_peer: &str,
) -> String {
    let hash = blake3::hash(content).to_hex().to_string();
    let data_dir = cyan_backend::DATA_DIR
        .get()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("."));
    let staged = data_dir.join("staged");
    std::fs::create_dir_all(&staged).expect("create staged dir");
    let path = staged.join(file_id);
    std::fs::write(&path, content).expect("write staged file");
    let name = format!("{file_id}.bin");
    let _ = storage::file_insert_simple(
        file_id,
        Some(group_id),
        workspace_id,
        board_id,
        &name,
        &hash,
        content.len() as u64,
        Some(source_peer),
        1,
    );
    let _ = storage::file_set_local_path(file_id, path.to_str().expect("utf8 staged path"));
    hash
}

/// Create the base tables the migrations assume exist (mirrors the multi-process bins'
/// `init_test_schema`). Runs once against the shared DB before `storage::init_db`.
fn init_base_schema(db_path: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, icon TEXT, color TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY, group_id TEXT NOT NULL, name TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY, workspace_id TEXT, group_id TEXT, board_id TEXT,
            type TEXT NOT NULL, name TEXT NOT NULL, hash TEXT, data TEXT, size INTEGER,
            source_peer TEXT, local_path TEXT, created_at INTEGER NOT NULL,
            board_mode TEXT DEFAULT 'canvas'
        );
        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, element_type TEXT NOT NULL,
            x REAL NOT NULL, y REAL NOT NULL, width REAL NOT NULL, height REAL NOT NULL,
            z_index INTEGER NOT NULL, style_json TEXT, content_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, cell_id TEXT
        );
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL, content TEXT, output TEXT,
            collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

/// Unique-id source so concurrently-running tests never collide on a group id
/// (group-scoped `peers_per_group`/`PeerJoined` keep their assertions isolated even
/// though the engine's storage and discovery topic are process-wide).
static UNIQUE: AtomicU64 = AtomicU64::new(1);

/// A fresh group id, unique within this process run.
pub fn unique_group_id() -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    format!("substrate-group-{n:016x}-1111-2222-3333-444444444444")
}

/// A fresh discovery key, unique within this process run. Tests use one per scenario
/// so concurrently-running tests do not share a discovery gossip topic (the engine's
/// storage is process-global, but the discovery topic is keyed by this string).
pub fn unique_discovery_key() -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    format!("cyan-test-{n:016x}")
}

/// Spawn one node with the given config. Mirrors the existing test bins
/// (`tests/network_actor_test.rs`): shared DB, ephemeral key, channels, a fresh
/// per-node `peers_per_group`, `NetworkActor::new(.., cfg)`, `spawn(actor.start(..))`.
pub async fn spawn_node(name: &str, cfg: NodeCfg) -> Result<Node> {
    let db_path = shared_db();

    let mut rng = ChaCha8Rng::from_os_rng();
    let secret_key = SecretKey::generate(&mut rng);
    let node_id = secret_key.public().to_string();

    let (event_tx, event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<NetworkCommand>();
    let peers_per_group = Arc::new(Mutex::new(HashMap::<String, HashSet<PublicKey>>::new()));

    let actor = NetworkActor::new(secret_key, event_tx, peers_per_group.clone(), cfg.to_engine())
        .await
        .map_err(|e| anyhow!("NetworkActor::new for {} failed: {}", name, e))?;

    // Grab the test-support seams before the actor is moved into its task.
    let endpoint = actor.endpoint();
    let static_discovery = actor.static_discovery();
    let swarm = actor.swarm();
    let authorizer = actor.authorizer();

    let actor_handle = tokio::spawn(async move {
        actor.start(cmd_rx).await;
    });

    Ok(Node {
        name: name.to_string(),
        node_id,
        cmd_tx,
        events: Arc::new(AsyncMutex::new(event_rx)),
        peers_per_group,
        db_path,
        endpoint,
        static_discovery,
        swarm,
        authorizer,
        actor_handle,
    })
}

/// Poll an endpoint until it has at least one direct (loopback/LAN) address, bounded.
async fn await_direct_addr(endpoint: &Endpoint, timeout: Duration) -> Result<EndpointAddr> {
    tokio::time::timeout(timeout, async {
        loop {
            let a = endpoint.addr();
            if a.ip_addrs().next().is_some() {
                return a;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("no direct address within {:?}", timeout))
}

/// Belt-and-suspenders re-wire of a specific node set (spawn-time registry already wires
/// every node pair; this just re-asserts it for an explicitly-passed set). Bounded.
pub async fn wire_addrs(nodes: &[Node], timeout: Duration) -> Result<()> {
    let mut addrs = Vec::with_capacity(nodes.len());
    for node in nodes {
        addrs.push(
            await_direct_addr(&node.endpoint, timeout)
                .await
                .map_err(|e| anyhow!("{}: {}", node.name, e))?,
        );
    }
    for (i, node) in nodes.iter().enumerate() {
        for (j, addr) in addrs.iter().enumerate() {
            if i != j {
                node.static_discovery.add_endpoint_info(addr.clone());
            }
        }
    }
    Ok(())
}

/// Seed a host fixture into the (shared) DB: a group with one workspace, one board,
/// `elements` board elements, `chats` chats, and one file-meta record. Returns
/// `(workspace_id, board_id)`. Call this AFTER nodes are spawned (so no node loads the
/// group at start and blocks on the snapshot join) and BEFORE a joiner requests a snapshot.
pub fn seed_group_fixture(group_id: &str, elements: usize, chats: usize) -> (String, String) {
    let ws = format!("{group_id}-ws");
    let board = format!("{group_id}-board");
    let _ = storage::group_insert_simple(group_id, "Fixture Group", "folder.fill", "#00AEEF");
    let _ = storage::workspace_insert_simple(&ws, group_id, "Main Workspace");
    let _ = storage::board_insert_simple(&board, &ws, "Canvas", 1);
    for i in 0..elements {
        let _ = storage::element_insert_simple(
            &format!("{group_id}-elem-{i}"),
            &board,
            "rectangle",
            i as f64,
            i as f64,
            100.0,
            50.0,
            i as i32,
            Some("{\"fill\":\"#00AEEF\"}"),
            Some("{\"text\":\"x\"}"),
            1,
            1,
        );
    }
    for i in 0..chats {
        let _ = storage::chat_insert_simple(
            &format!("{group_id}-chat-{i}"),
            &ws,
            &format!("message {i}"),
            "author",
            None,
            1,
        );
    }
    let _ = storage::file_insert_simple(
        &format!("{group_id}-file-0"),
        Some(group_id),
        Some(&ws),
        Some(&board),
        "doc.pdf",
        "deadbeefdeadbeef",
        1024,
        None,
        1,
    );
    (ws, board)
}

/// Spawn `n` nodes that share one discovery key and form a mesh. `node[0]` is the
/// in-process LAN rendezvous seed; every later node is given `node[0]`'s id as its
/// discovery bootstrap so the gossip discovery topic actually forms (gossip needs a
/// bootstrap peer; there is no pure-mDNS topic auto-join in iroh 0.95). The relay
/// policy from `cfg` is preserved — with `Disabled` the bootstrap dial resolves the
/// seed's address over mDNS on loopback, which is the offline-LAN path we want.
pub async fn spawn_mesh(n: usize, cfg: NodeCfg) -> Result<Vec<Node>> {
    assert!(n >= 1, "spawn_mesh needs at least one node");
    let mut nodes = Vec::with_capacity(n);

    let seed = spawn_node("node-0", cfg.clone()).await?;
    let seed_id = seed.node_id.clone();
    nodes.push(seed);

    for i in 1..n {
        let mut node_cfg = cfg.clone();
        node_cfg.discovery = DiscoveryPolicy::Bootstrap(seed_id.clone());
        nodes.push(spawn_node(&format!("node-{i}"), node_cfg).await?);
    }

    Ok(nodes)
}

/// Bounded poll of a condition (a real convergence oracle), not a sleep-as-sync: it
/// returns as soon as `cond` holds, and fails with a clear error at `timeout`.
pub async fn wait_until<F>(mut cond: F, timeout: Duration, what: &str) -> Result<()>
where
    F: FnMut() -> bool,
{
    tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout after {:?} waiting for: {}", timeout, what))
}

/// Drive a group meeting across an already-spawned mesh and wait for mutual discovery.
///
/// `node[0]` joins the group as the host; every other node joins seeded with
/// `node[0]`'s id (so its group-topic gossip dials the host). Returns once **every**
/// node has the host (and the host has at least one peer) in its **per-node**
/// `peers_per_group` for `group_id` — i.e. the discovery `groups_exchange` actually
/// introduced the peers, end to end.
///
/// We assert on `peers_per_group` rather than `PeerJoined` because the engine's
/// `gossip.subscribe_and_join(..).joined()` consumes the first `NeighborUp` of a topic,
/// so in a 2-node mesh the sole peer's `PeerJoined` is never surfaced by the TopicActor.
/// `peers_per_group` is populated from the discovery `groups_exchange` *message*, which
/// is unaffected by that — see the note in this module.
pub async fn meet(nodes: &[Node], group_id: &str, timeout: Duration) -> Result<()> {
    let seed_id = nodes
        .first()
        .ok_or_else(|| anyhow!("meet needs at least one node"))?
        .node_id
        .clone();

    // Make every node dialable by every other node over loopback, so the group topic
    // forms deterministically instead of via flaky mDNS.
    wire_addrs(nodes, timeout).await?;

    // Form the group topic: the seed hosts it, every other node dials the seed. These
    // dials happen AFTER wiring (unlike the discovery topic, which dials at start-up
    // before addresses are wired), so the group topic forms reliably.
    nodes[0].join_group(group_id, None);
    for node in &nodes[1..] {
        node.join_group(group_id, Some(seed_id.clone()));
    }

    // Confirm end-to-end group-topic delivery with a re-broadcast probe. A freshly-formed
    // 2-node gossip topic can drop its first message(s) before the mesh stabilises, so we
    // re-broadcast a sentinel until EVERY non-seed node has actually received it. This is
    // the robust "the mesh is up and delivering" signal — and it is exactly the capability
    // (group-topic broadcast → peers) the substrate tests then exercise.
    let probe_id = format!("__probe__{group_id}");
    let probe = NetworkEvent::WhiteboardElementAdded {
        id: probe_id.clone(),
        board_id: "__probe__".to_string(),
        element_type: "probe".to_string(),
        x: 0.0,
        y: 0.0,
        width: 0.0,
        height: 0.0,
        z_index: 0,
        style_json: None,
        content_json: None,
        created_at: 0,
        updated_at: 0,
    };
    let mut confirmed = vec![false; nodes.len()];
    confirmed[0] = true; // the seed is the source of the probe

    let result = tokio::time::timeout(timeout, async {
        loop {
            nodes[0].broadcast(group_id, probe.clone());
            for (i, node) in nodes.iter().enumerate() {
                if confirmed[i] {
                    continue;
                }
                let pid = probe_id.clone();
                let got = node
                    .wait_network(
                        move |e| {
                            matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if *id == pid)
                        },
                        Duration::from_millis(150),
                    )
                    .await
                    .is_ok();
                if got {
                    confirmed[i] = true;
                }
            }
            if confirmed.iter().all(|c| *c) {
                return;
            }
        }
    })
    .await;

    if result.is_err() {
        let pending: Vec<&str> = nodes
            .iter()
            .zip(&confirmed)
            .filter(|(_, c)| !**c)
            .map(|(n, _)| n.name.as_str())
            .collect();
        return Err(anyhow!(
            "group topic did not deliver to all nodes for {} within {:?}; not reached: {:?}",
            group_id,
            timeout,
            pending
        ));
    }
    Ok(())
}

/// Serialization for node-spinning tests, **across processes**.
///
/// Many concurrent iroh endpoints make in-process discovery timing fragile, and `cargo
/// test` runs each test BINARY in its own process in parallel — so an in-process mutex
/// isn't enough; a discovery/files binary would still overlap a chat binary. This guard
/// is a cross-process advisory file lock (plus an in-process async mutex to avoid lock
/// thrash within a binary), so at most ONE substrate scenario spins nodes machine-wide at
/// a time. It is panic-safe (released on unwind via Drop) and tolerates a stale lock left
/// by a killed process. Bounded: gives up after a generous deadline with a clear error.
static SERIAL: OnceLock<AsyncMutex<()>> = OnceLock::new();

fn serial_lock_path() -> PathBuf {
    std::env::temp_dir().join("cyan-substrate-serial.lock")
}

/// Guard returned by [`serial`]; releases both the in-process mutex and the file lock.
pub struct SerialGuard {
    _inner: tokio::sync::MutexGuard<'static, ()>,
    path: PathBuf,
}

impl Drop for SerialGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Acquire the cross-process serialization guard; hold it for the whole test.
pub async fn serial() -> SerialGuard {
    let inner = SERIAL.get_or_init(|| AsyncMutex::new(())).lock().await;
    let path = serial_lock_path();
    let deadline = Duration::from_secs(240);
    let acquired = tokio::time::timeout(deadline, async {
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(_) => return,
                Err(_) => {
                    // Reclaim a stale lock from a killed process (no Drop ran).
                    if let Ok(meta) = std::fs::metadata(&path)
                        && let Ok(modified) = meta.modified()
                        && modified.elapsed().map(|e| e.as_secs() > 180).unwrap_or(false)
                    {
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    })
    .await;
    if acquired.is_err() {
        // Don't hang the suite; proceed best-effort (the in-process mutex still holds).
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path);
    }
    SerialGuard { _inner: inner, path }
}

/// Default generous-but-bounded wait for convergence assertions. Generous (30s) so it
/// survives the CPU starvation of the whole substrate suite's binaries running at once
/// under `cargo test` — still a hard, clearly-reported deadline, never an unbounded wait.
pub const SYNC_TIMEOUT: Duration = Duration::from_secs(30);
