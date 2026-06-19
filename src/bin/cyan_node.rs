//! cyan_node — a TEST-ONLY peer binary for the multi-process substrate rig.
//!
//! Each `cyan_node` process boots a real `NetworkActor` against its OWN SQLite
//! database (`NODE_DB`), giving the substrate suite *genuine per-node storage* —
//! which the in-process harness cannot have (the engine's `storage` is a
//! process-global singleton). This is the honest path for snapshot/storage-truth
//! assertions like `late_joiner_gets_full_snapshot`: the joiner runs in its own
//! process, so counting rows in its DB really proves it received the data.
//!
//! It uses ONLY the crate's PUBLIC API (`cyan_backend::{actors, storage, models,
//! DATA_DIR}`) — no FFI, no engine edits. It is driven over a tiny line protocol
//! on stdin/stdout (one request per line; one response line per request, tagged
//! with the `@@CYAN@@` sentinel so engine stderr/stdout noise can never be mistaken
//! for a response).
//!
//! ## Environment (read once at boot)
//! - `NODE_DB`            — path to this process's SQLite DB (required).
//! - `DISCOVERY_KEY`      — gossip discovery key (default `cyan-test`).
//! - `RELAY`              — `disabled` (default) or a relay URL.
//! - `BOOTSTRAP_NODE_ID`  — optional hex node id ⇒ `DiscoveryPolicy::Bootstrap`
//!   (absent ⇒ `DiscoveryPolicy::MdnsOnly`).
//! - `DATA_DIR`           — optional download dir; defaults to `<NODE_DB dir>/data`.
//!
//! ## Control protocol (stdin → stdout, line oriented)
//! Request (one per line)                 Response (`@@CYAN@@ ` prefixed)
//! - `node_id`                            `@@CYAN@@ node_id <hex>`
//! - `addr`                               `@@CYAN@@ addr <endpoint-addr-json>`
//! - `add_peer <endpoint-addr-json>`      `@@CYAN@@ ok add_peer`
//! - `seed_empty_group <gid>`             `@@CYAN@@ ok seed_empty_group`
//! - `seed_fixture <gid>`                 `@@CYAN@@ ok seed_fixture`
//! - `join_group <gid> [bootstrap_hex]`   `@@CYAN@@ ok join_group`
//! - `wait_sync <gid> <timeout_ms>`       `@@CYAN@@ ok wait_sync` | `@@CYAN@@ err wait_sync timeout`
//! - `count <kind> <gid>`                 `@@CYAN@@ count <kind> <n>`
//!   kinds: groups|workspaces|boards|elements|cells|chats|files
//! - `quit`                               (process exits 0)
//!
//! Every wait is bounded; the binary never blocks unboundedly.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use iroh::{EndpointAddr, PublicKey, SecretKey};
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Notify;

use cyan_backend::actors::NetworkActor;
use cyan_backend::models::commands::NetworkCommand;
use cyan_backend::models::events::SwiftEvent;
use cyan_backend::models::node_config::{DiscoveryPolicy, NodeConfig, RelayPolicy};
use cyan_backend::storage;

/// Response sentinel — the harness scans stdout for lines starting with this, so
/// engine logging (which goes to stderr anyway) can never be parsed as a response.
const SENTINEL: &str = "@@CYAN@@";

/// The fixed fixture shape (mirrors `network_actor_test::create_test_data`): the
/// host seeds exactly these counts; a synced joiner must end with the same.
const FIXTURE_ELEMENTS: usize = 5;
const FIXTURE_CELLS: usize = 3;
const FIXTURE_CHATS: usize = 3;
const FIXTURE_FILES: usize = 1;

#[tokio::main]
async fn main() -> Result<()> {
    // The engine logs to stderr (eprintln!); leave stdout pristine for the protocol.
    // Opt-in iroh/gossip tracing to STDERR when RUST_LOG is set (debugging the rig only;
    // stdout — the control channel — is never touched).
    if std::env::var("RUST_LOG").is_ok() {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }

    let db_path = std::env::var("NODE_DB").context("NODE_DB env required")?;
    let discovery_key = std::env::var("DISCOVERY_KEY").unwrap_or_else(|_| "cyan-test".to_string());
    let relay = match std::env::var("RELAY").as_deref() {
        Ok("disabled") | Err(_) => RelayPolicy::Disabled,
        Ok(url) => RelayPolicy::Url(url.to_string()),
    };
    let discovery = match std::env::var("BOOTSTRAP_NODE_ID") {
        Ok(id) if !id.is_empty() => DiscoveryPolicy::Bootstrap(id),
        _ => DiscoveryPolicy::MdnsOnly,
    };

    // Per-process storage: create base tables, then init the global DB at NODE_DB.
    init_base_schema(&db_path)?;
    storage::init_db(&db_path).map_err(|e| anyhow!("storage::init_db({db_path}): {e}"))?;

    // Optionally seed the host fixture BEFORE the actor starts. This is deliberate:
    // the engine's startup group-load auto-spawns a group TopicActor (which blocks on
    // `gossip.subscribe_and_join(..).await` until a neighbor connects). For the HOST we
    // WANT that — it makes the host host the group topic deterministically and wait for
    // the joiner. A late joiner, by contrast, must start with an EMPTY db so its command
    // loop is reachable to process `JoinGroup` (with the reachable host as a peer).
    if let Ok(gid) = std::env::var("SEED_FIXTURE")
        && !gid.is_empty()
    {
        seed_fixture(&gid)?;
    }

    // Downloads land under DATA_DIR (process-global OnceCell in the engine).
    let data_dir = std::env::var("DATA_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        PathBuf::from(&db_path)
            .parent()
            .map(|p| p.join("data"))
            .unwrap_or_else(|| PathBuf::from("data"))
    });
    std::fs::create_dir_all(&data_dir).ok();
    let _ = cyan_backend::DATA_DIR.set(data_dir);

    // Identity + channels.
    let mut rng = ChaCha8Rng::from_os_rng();
    let secret_key = SecretKey::generate(&mut rng);
    let node_id = secret_key.public().to_string();

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<NetworkCommand>();
    let peers_per_group = Arc::new(Mutex::new(HashMap::<String, HashSet<PublicKey>>::new()));

    let cfg = NodeConfig {
        relay,
        discovery,
        discovery_key,
    };
    let actor = NetworkActor::new(secret_key, event_tx, peers_per_group, cfg)
        .await
        .map_err(|e| anyhow!("NetworkActor::new: {e}"))?;

    // Test-support seams grabbed before the actor moves into its task.
    let endpoint = actor.endpoint();
    let static_discovery = actor.static_discovery();

    tokio::spawn(async move {
        actor.start(cmd_rx).await;
    });

    // Drain events into shared state so `wait_sync` can observe SyncComplete even
    // if it arrives before the verb is issued.
    let synced: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let notify = Arc::new(Notify::new());
    {
        let synced = synced.clone();
        let notify = notify.clone();
        tokio::spawn(async move {
            while let Some(ev) = event_rx.recv().await {
                if let SwiftEvent::SyncComplete { group_id } = ev {
                    if let Ok(mut s) = synced.lock() {
                        s.insert(group_id);
                    }
                    notify.notify_waiters();
                }
            }
        });
    }

    // Control loop: read requests on stdin, write tagged responses on stdout.
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let verb = parts.next().unwrap_or("");
        let rest: Vec<&str> = parts.collect();

        if verb == "quit" {
            break;
        }

        let resp = handle_verb(
            verb,
            &rest,
            &node_id,
            &endpoint,
            &static_discovery,
            &cmd_tx,
            &synced,
            &notify,
        )
        .await
        .unwrap_or_else(|e| format!("err {verb} {e}"));

        stdout
            .write_all(format!("{SENTINEL} {resp}\n").as_bytes())
            .await?;
        stdout.flush().await?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_verb(
    verb: &str,
    rest: &[&str],
    node_id: &str,
    endpoint: &iroh::Endpoint,
    static_discovery: &iroh::discovery::static_provider::StaticProvider,
    cmd_tx: &UnboundedSender<NetworkCommand>,
    synced: &Arc<Mutex<HashSet<String>>>,
    notify: &Arc<Notify>,
) -> Result<String> {
    match verb {
        "node_id" => Ok(format!("node_id {node_id}")),

        "addr" => {
            let addr = await_direct_addr(endpoint, Duration::from_secs(10)).await?;
            let json = serde_json::to_string(&addr).context("serialize EndpointAddr")?;
            Ok(format!("addr {json}"))
        }

        "add_peer" => {
            // The JSON may contain spaces? No — serde_json output has none for this
            // type, but rejoin defensively in case of any whitespace.
            let json = rest.join(" ");
            let addr: EndpointAddr =
                serde_json::from_str(&json).context("deserialize EndpointAddr")?;
            static_discovery.add_endpoint_info(addr);
            Ok("ok add_peer".to_string())
        }

        "seed_empty_group" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            storage::group_insert_simple(gid, "Invited Group", "folder.fill", "#FF6B6B")
                .map_err(|e| anyhow!("group_insert_simple: {e}"))?;
            Ok("ok seed_empty_group".to_string())
        }

        "seed_fixture" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            seed_fixture(gid)?;
            Ok("ok seed_fixture".to_string())
        }

        "join_group" => {
            let gid = rest
                .first()
                .ok_or_else(|| anyhow!("group_id required"))?
                .to_string();
            let bootstrap_peer = rest.get(1).map(|s| s.to_string());
            cmd_tx
                .send(NetworkCommand::JoinGroup {
                    group_id: gid,
                    bootstrap_peer,
                })
                .map_err(|e| anyhow!("send JoinGroup: {e}"))?;
            Ok("ok join_group".to_string())
        }

        "wait_sync" => {
            let gid = rest
                .first()
                .ok_or_else(|| anyhow!("group_id required"))?
                .to_string();
            let timeout_ms: u64 = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(60_000);
            let ok = wait_sync(synced, notify, &gid, Duration::from_millis(timeout_ms)).await;
            if ok {
                Ok("ok wait_sync".to_string())
            } else {
                Ok("err wait_sync timeout".to_string())
            }
        }

        "count" => {
            let kind = rest.first().ok_or_else(|| anyhow!("kind required"))?;
            let gid = rest.get(1).ok_or_else(|| anyhow!("group_id required"))?;
            let n = count_kind(kind, gid)?;
            Ok(format!("count {kind} {n}"))
        }

        other => Err(anyhow!("unknown verb '{other}'")),
    }
}

/// Poll the endpoint until it has at least one direct (loopback) address. Bounded.
async fn await_direct_addr(endpoint: &iroh::Endpoint, timeout: Duration) -> Result<EndpointAddr> {
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

/// Bounded wait for `SyncComplete` of `group_id`. The 50ms re-poll tick guards against
/// a lost wakeup between the set check and `notified()`; the real signal is the set,
/// and the whole wait is bounded by `timeout` — never an unbounded block.
async fn wait_sync(
    synced: &Arc<Mutex<HashSet<String>>>,
    notify: &Arc<Notify>,
    group_id: &str,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if synced
                .lock()
                .map(|s| s.contains(group_id))
                .unwrap_or(false)
            {
                return;
            }
            tokio::select! {
                _ = notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    })
    .await
    .is_ok()
}

/// Count rows of `kind` scoped to `group_id`, reading THIS process's storage.
fn count_kind(kind: &str, group_id: &str) -> Result<usize> {
    let ws_ids = storage::workspace_list_ids_by_group(group_id);
    let n = match kind {
        "groups" => storage::group_list()
            .map_err(|e| anyhow!("group_list: {e}"))?
            .iter()
            .filter(|g| g.id == group_id)
            .count(),
        "workspaces" => storage::workspace_list_by_group(group_id)
            .map_err(|e| anyhow!("workspace_list_by_group: {e}"))?
            .len(),
        "boards" => storage::board_list_by_workspaces(&ws_ids)
            .map_err(|e| anyhow!("board_list_by_workspaces: {e}"))?
            .len(),
        "elements" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::element_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("element_list_by_boards: {e}"))?
                .len()
        }
        "cells" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::cell_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("cell_list_by_boards: {e}"))?
                .len()
        }
        "chats" => storage::chat_list_by_workspaces(&ws_ids)
            .map_err(|e| anyhow!("chat_list_by_workspaces: {e}"))?
            .len(),
        "files" => storage::file_list_by_group(group_id)
            .map_err(|e| anyhow!("file_list_by_group: {e}"))?
            .len(),
        other => return Err(anyhow!("unknown count kind '{other}'")),
    };
    Ok(n)
}

fn board_ids(ws_ids: &[String]) -> Result<Vec<String>> {
    Ok(storage::board_list_by_workspaces(ws_ids)
        .map_err(|e| anyhow!("board_list_by_workspaces: {e}"))?
        .iter()
        .map(|b| b.id.clone())
        .collect())
}

/// Seed the full host fixture into this process's DB: a group with one workspace,
/// one board, 5 elements, 3 cells, 3 chats, and one file-meta record. Mirrors the
/// proven `network_actor_test::create_test_data` shape.
fn seed_fixture(group_id: &str) -> Result<()> {
    let ws = format!("{group_id}-ws");
    let board = format!("{group_id}-board");
    let now = chrono::Utc::now().timestamp();

    storage::group_insert_simple(group_id, "Fixture Group", "folder.fill", "#00AEEF")
        .map_err(|e| anyhow!("group_insert_simple: {e}"))?;
    storage::workspace_insert_simple(&ws, group_id, "Main Workspace")
        .map_err(|e| anyhow!("workspace_insert_simple: {e}"))?;
    storage::board_insert_simple(&board, &ws, "Test Canvas", now)
        .map_err(|e| anyhow!("board_insert_simple: {e}"))?;

    for i in 0..FIXTURE_ELEMENTS {
        storage::element_insert_simple(
            &format!("{group_id}-elem-{i:03}"),
            &board,
            "rectangle",
            (i * 100) as f64,
            (i * 50) as f64,
            200.0,
            100.0,
            i as i32,
            Some("{\"fill\":\"#00AEEF\"}"),
            Some(&format!("{{\"text\":\"Element {i}\"}}")),
            now,
            now,
        )
        .map_err(|e| anyhow!("element_insert_simple: {e}"))?;
    }

    for i in 0..FIXTURE_CELLS {
        storage::cell_insert_simple(
            &format!("{group_id}-cell-{i:03}"),
            &board,
            "code",
            i as i32,
            Some(&format!("# Cell {i}\nprint('hello')")),
            Some("hello"),
            false,
            Some(100.0),
            None,
            now,
            now,
        )
        .map_err(|e| anyhow!("cell_insert_simple: {e}"))?;
    }

    for i in 0..FIXTURE_CHATS {
        storage::chat_insert_simple(
            &format!("{group_id}-chat-{i:03}"),
            &ws,
            &format!("Test message {i}"),
            "test-author",
            None,
            now,
        )
        .map_err(|e| anyhow!("chat_insert_simple: {e}"))?;
    }

    for i in 0..FIXTURE_FILES {
        storage::file_insert_simple(
            &format!("{group_id}-file-{i:03}"),
            Some(group_id),
            Some(&ws),
            Some(&board),
            "test-document.pdf",
            "abc123def456",
            1_024_000,
            None,
            now,
        )
        .map_err(|e| anyhow!("file_insert_simple: {e}"))?;
    }

    Ok(())
}

/// Create the base tables the migrations assume exist (mirrors the in-process
/// harness's `init_base_schema` and the multi-process bins' `init_test_schema`).
fn init_base_schema(db_path: &str) -> Result<()> {
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
