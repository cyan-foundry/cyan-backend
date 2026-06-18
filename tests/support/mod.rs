//! MeshHarness — the keystone for the substrate test suite (SUBSTRATE_TEST_SPEC §1).
//!
//! Spins N cyan-backend nodes **in one test process** and lets a test drive them
//! (command channel) and observe them (events + `storage::*`). Every substrate
//! test file (`tests/substrate_*.rs`) imports this via `mod support;`.
//!
//! REVIEWED SHAPE — bodies are `todo!()` for the harness agent to implement to
//! green against the real `NetworkActor`. Do NOT change the public signatures
//! without a note in SUBSTRATE_TEST_SPEC; the test files depend on them.
//!
//! PREREQUISITE: this assumes the engine refactor that threads `NodeConfig` into
//! `NetworkActor::new(..)` and adds a `RelayMode::Disabled` path (see the engine
//! task). Until that lands, `spawn_node` cannot give each in-process node its own
//! relay/discovery config — that's the whole reason the harness needs it.
//!
//! In-process scope: discovery (mDNS/bootstrap), snapshot+delta sync, chat, and
//! file transfer over loopback (G1, G3–G8, and G9 via `RelayPolicy::Disabled`).
//! The relay/WebSocket rungs (G2 ladder, G8-R, G11) are NOT in-process — they
//! need the docker rig in `cyan-local-harness/` because forcing relay-only
//! requires real network isolation.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

// NOTE(agent): confirm these import paths against the crate's public API.
use cyan_backend::models::commands::NetworkCommand;
use cyan_backend::models::events::SwiftEvent;
// use cyan_backend::actors::network_actor::NetworkActor;
// use cyan_backend::models::NodeConfig;          // introduced by the engine refactor
// use cyan_backend::storage;
// use iroh::{PublicKey, SecretKey};

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

/// A live node: its actor task, the command sink, the event stream, and its DB.
pub struct Node {
    pub name: String,
    pub node_id: String,
    cmd_tx: UnboundedSender<NetworkCommand>,
    events: Arc<Mutex<UnboundedReceiver<SwiftEvent>>>,
    db_path: PathBuf,
    // keep the tempdir alive for the node's lifetime
    _tmp: tempfile::TempDir,
}

impl Node {
    /// Send a command into the node (fire-and-forget).
    pub fn cmd(&self, _c: NetworkCommand) {
        todo!("cmd_tx.send(c) — log/ignore SendError")
    }

    /// Await the first event matching `pred`, or fail after `timeout`.
    pub async fn wait_for<F>(&self, _pred: F, _timeout: Duration) -> anyhow::Result<SwiftEvent>
    where
        F: Fn(&SwiftEvent) -> bool,
    {
        todo!("tokio::time::timeout over events.recv(); return the matching event or Err")
    }

    /// Convenience: await `SwiftEvent::SyncComplete { group_id }`.
    pub async fn wait_sync(&self, group_id: &str, timeout: Duration) -> anyhow::Result<()> {
        let gid = group_id.to_string();
        self.wait_for(
            move |e| matches!(e, SwiftEvent::SyncComplete { group_id } if *group_id == gid),
            timeout,
        )
        .await
        .map(|_| ())
    }

    /// Path to this node's SQLite db — assertion oracle (`storage::*` queries).
    pub fn db(&self) -> &Path {
        &self.db_path
    }
}

/// Spawn one node with the given config. The setup mirrors the existing test
/// bins (`tests/network_actor_test.rs`): fresh temp DB, ephemeral key, channels,
/// `peers_per_group`, `NetworkActor::new(.., cfg)`, `spawn(actor.start(cmd_rx))`.
pub async fn spawn_node(_name: &str, _cfg: NodeCfg) -> anyhow::Result<Node> {
    // 1. let tmp = tempfile::tempdir()?; let db_path = tmp.path().join("node.db");
    // 2. storage::init_db(db_path.to_str().unwrap())?;
    // 3. let mut rng = ChaCha8Rng::from_os_rng(); let sk = SecretKey::generate(&mut rng);
    // 4. let (event_tx, event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    //    let (cmd_tx,   cmd_rx)   = mpsc::unbounded_channel::<NetworkCommand>();
    //    let peers = Arc::new(Mutex::new(HashMap::<String, HashSet<PublicKey>>::new()));
    // 5. let node_cfg = NodeConfig::from(cfg);   // map RelayPolicy/Discovery → engine types
    //    let actor = NetworkActor::new(sk, event_tx, peers, node_cfg).await?;
    // 6. tokio::spawn(async move { actor.start(cmd_rx).await; });
    // 7. wrap in Node { .. } and return.
    todo!("implement per the steps above; keep it ~30 lines like the test bins")
}

/// Spawn `n` nodes sharing one `discovery_key` so they form a mesh.
/// The first node's id can be used as `Bootstrap(..)` for the rest if desired.
pub async fn spawn_mesh(_n: usize, _cfg: NodeCfg) -> anyhow::Result<Vec<Node>> {
    todo!("loop spawn_node; return the vec")
}

/// Default generous-but-bounded wait for convergence assertions.
pub const SYNC_TIMEOUT: Duration = Duration::from_secs(15);
