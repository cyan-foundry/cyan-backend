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
use iroh::{Endpoint, PublicKey, SecretKey};
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
use cyan_backend::storage;

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
        });
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

    /// Number of peers this node currently tracks in `group_id` (secondary oracle for
    /// discovery — populated by the discovery `groups_exchange` → `JoinPeerToTopic` path).
    pub fn peers_in_group(&self, group_id: &str) -> usize {
        self.peers_per_group
            .lock()
            .map(|m| m.get(group_id).map(|s| s.len()).unwrap_or(0))
            .unwrap_or(0)
    }

    /// Path to the (shared) SQLite db. NOTE: with the engine's process-global storage
    /// this is the SAME db for every node — see module docs.
    pub fn db(&self) -> &Path {
        &self.db_path
    }
}

/// The shared, process-global database path. The engine's `storage` is a global
/// singleton, so we initialise it exactly once and hand every node the same path.
/// The backing tempdir is intentionally leaked so it outlives every node.
static SHARED_DB: OnceLock<PathBuf> = OnceLock::new();

fn shared_db() -> PathBuf {
    SHARED_DB
        .get_or_init(|| {
            let dir = tempfile::tempdir().expect("create tempdir for shared substrate db");
            let path = dir.path().join("substrate.db");
            init_base_schema(&path).expect("init base schema");
            storage::init_db(path.to_str().expect("utf8 db path")).expect("storage::init_db");
            std::mem::forget(dir); // keep the dir alive for the whole test process
            path
        })
        .clone()
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

    tokio::spawn(async move {
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
    })
}

/// Wire every node's loopback `EndpointAddr` into every other node's static address
/// provider, so they can dial each other by id **without mDNS** (which is unreliable
/// for many in-process endpoints). Bounded: fails if a node has no direct address yet.
pub async fn wire_addrs(nodes: &[Node], timeout: Duration) -> Result<()> {
    let mut addrs = Vec::with_capacity(nodes.len());
    for node in nodes {
        let ep = node.endpoint.clone();
        let addr = tokio::time::timeout(timeout, async {
            loop {
                let a = ep.addr();
                if a.ip_addrs().next().is_some() {
                    return a;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("{} had no direct address within {:?}", node.name, timeout))?;
        addrs.push(addr);
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

    // Make every node dialable by every other node over loopback, so the gossip
    // discovery/group topics form deterministically instead of via flaky mDNS.
    wire_addrs(nodes, timeout).await?;

    // Round 1: everyone joins. This spawns each node's group TopicActor, forms the
    // gossip discovery mesh (joiners dial the seed), and sets group_id in every node's
    // `my_groups`. The host reliably learns a peer here; joiners may miss the host's
    // first `groups_exchange` if it arrived before their own join (a join-order race).
    nodes[0].join_group(group_id, None);
    for node in &nodes[1..] {
        node.join_group(group_id, Some(seed_id.clone()));
    }
    wait_until(
        || nodes[0].peers_in_group(group_id) >= 1,
        timeout,
        &format!("{} (host) to see a peer in {}", nodes[0].name, group_id),
    )
    .await?;

    // Round 2: re-announce now that every node has the group in `my_groups`, so the
    // `groups_exchange` is evaluated symmetrically and each joiner records the host.
    for node in nodes {
        node.join_group(group_id, None);
    }
    for node in nodes {
        wait_until(
            || node.peers_in_group(group_id) >= 1,
            timeout,
            &format!("{} to discover a peer in {}", node.name, group_id),
        )
        .await?;
    }
    Ok(())
}

/// Process-wide serialization lock for node-spinning tests.
///
/// In-process iroh `MdnsDiscovery` is the only way nodes resolve each other's address
/// with relay disabled, and it does not reliably support many concurrent endpoints in
/// one process (4+ live nodes makes discovery flaky; ≤2 is reliable). Cargo runs a
/// binary's `#[tokio::test]`s concurrently, so each such test acquires this guard for
/// its duration — keeping at most one scenario's nodes alive at a time. Bounded waits
/// inside the test still apply; this only stops unrelated scenarios from overlapping.
static SERIAL: OnceLock<AsyncMutex<()>> = OnceLock::new();

/// Acquire the serialization guard; hold it for the whole test.
pub async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.get_or_init(|| AsyncMutex::new(())).lock().await
}

/// Default generous-but-bounded wait for convergence assertions.
pub const SYNC_TIMEOUT: Duration = Duration::from_secs(15);
