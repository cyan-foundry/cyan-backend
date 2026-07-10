// src/lib.rs
#![allow(clippy::too_many_arguments)]

extern crate core;

// Re-export xaeroid FFI functions for Swift
mod xaero_ffi {
    pub use xaeroid::xaero_create_pass_json;
    pub use xaeroid::xaero_create_pass_with_profile;
    pub use xaeroid::xaero_derive_identity;
    pub use xaeroid::xaero_free_string;
    pub use xaeroid::xaero_generate_json;
    pub use xaeroid::xaero_sign_with_key;
    pub use xaeroid::anonymous::xaero_create_anonymous_session;
    pub use xaeroid::anonymous::xaero_reveal_anonymous_identity;
    pub use xaeroid::anonymous::xaero_verify_anonymous_join;
    pub use xaeroid::anonymous::xaero_verify_reveal;
}
pub use xaero_ffi::*;

mod ai_bridge;
pub mod util;
pub mod cyan_lens_client;
pub mod models;
mod ffi;
pub mod actors;
pub mod storage;
pub mod swarm;
pub mod metrics;
pub mod anti_entropy;
pub mod snapshot;
pub mod group_bundle;
pub mod lens_commands;
pub mod mcp_host;
pub mod mesh_invoke;
pub mod identity;
pub mod licensing;
pub mod sso_grant;
pub mod group_rekey;
pub mod device_vault;
pub mod rendezvous;

use crate::models::commands::{CommandMsg, NetworkCommand};
use crate::util::MutexExt;
use crate::models::core::{Group, Workspace};
use crate::models::dto::{
    BoardMetadataDTO, ChatDTO, FileDTO, IntegrationBindingDTO, TreeSnapshotDTO, WhiteboardDTO
};
use crate::models::events::{NetworkEvent, SwiftEvent};
use crate::storage::run_migrations;
pub use ai_bridge::AIBridge;

use anyhow::Result;
use iroh::{PublicKey, SecretKey};
use once_cell::sync::OnceCell;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rusqlite::{params, Connection};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{
    runtime::Runtime,
    sync::mpsc,
};

// ═══════════════════════════════════════════════════════════════════════════
// CONSTANTS - exported for actors module
// ═══════════════════════════════════════════════════════════════════════════

// §5: the bootstrap node id is no longer a load-bearing hardcode here. It is resolved at startup
// from a signed, discoverable rendezvous config (`rendezvous::fetch_and_apply_if_configured`); the
// only remaining hardcoded value is the *bundled cold-start fallback*
// (`rendezvous::BUNDLED_BOOTSTRAP_NODE_ID`), used when no signed config is configured/reachable —
// which keeps behavior identical to before when no rendezvous URL is set.

// ═══════════════════════════════════════════════════════════════════════════
// GLOBALS
// ═══════════════════════════════════════════════════════════════════════════

pub static RUNTIME: OnceCell<Runtime> = OnceCell::new();
static SYSTEM: OnceCell<Arc<CyanSystem>> = OnceCell::new();
pub static DISCOVERY_KEY: OnceCell<String> = OnceCell::new();
pub static DATA_DIR: OnceCell<PathBuf> = OnceCell::new();
pub static RELAY_URL: OnceCell<String> = OnceCell::new();
pub static BOOTSTRAP_NODE_ID: OnceCell<String> = OnceCell::new();
static NODE_ID: OnceCell<String> = OnceCell::new();
static AI_RESPONSE_QUEUE: OnceCell<Mutex<VecDeque<String>>> = OnceCell::new();

/// Process-wide device-key vault (W17 §B). The macOS Keychain in production, the
/// in-memory fake headless/in tests — see [`device_vault::default_device_vault`].
/// Lazily built so the FFI "delete identity" / migration paths share one backing.
pub static DEVICE_VAULT: OnceCell<Arc<dyn device_vault::Vault>> = OnceCell::new();

/// The shared device-key vault, initialized on first use.
pub fn device_vault() -> Arc<dyn device_vault::Vault> {
    DEVICE_VAULT
        .get_or_init(device_vault::default_device_vault)
        .clone()
}

/// This node's live, resolvable address as a serialized `iroh::EndpointAddr` (MESH_HARDENING §2.2).
/// The `NetworkActor` publishes it once its endpoint has a direct address; `cyan_issue_grant_qr`
/// reads it to stamp the inviter's full NodeAddr into the QR so a joiner can dial directly (no
/// relay/bootstrap). `None` until published. Additive seam — nothing else depends on it.
pub static LOCAL_ENDPOINT_ADDR: OnceCell<Mutex<Option<String>>> = OnceCell::new();

/// Publish this node's serialized `EndpointAddr` for the QR inviter-addr seam (§2.2). Idempotent.
pub fn publish_local_endpoint_addr(addr_json: String) {
    let cell = LOCAL_ENDPOINT_ADDR.get_or_init(|| Mutex::new(None));
    if let Ok(mut g) = cell.lock() {
        *g = Some(addr_json);
    }
}

/// The last-published local `EndpointAddr` JSON, if any (§2.2).
pub fn local_endpoint_addr() -> Option<String> {
    LOCAL_ENDPOINT_ADDR.get().and_then(|m| m.lock().ok().and_then(|g| g.clone()))
}

/// Get the bootstrap node id in effect: the value resolved from the signed rendezvous config (set
/// into `BOOTSTRAP_NODE_ID` by `rendezvous::apply`), else the bundled cold-start fallback. No
/// standalone hardcode — the fallback lives in one place (`rendezvous::BUNDLED_BOOTSTRAP_NODE_ID`).
pub fn bootstrap_node_id() -> &'static str {
    BOOTSTRAP_NODE_ID
        .get()
        .map(|s| s.as_str())
        .unwrap_or(rendezvous::BUNDLED_BOOTSTRAP_NODE_ID)
}

/// Queue an engine-internal `CommandMsg` onto the running system's command loop —
/// the ledger-sync broadcast bridge (`changelist::dispatch` → group-topic gossip,
/// CYAN_FORMAT_SPEC §6.2). A no-op when the full system isn't up (unit tests, bare
/// storage use, the substrate harness driving `NetworkActor` directly) — the sync
/// stays engine-internal and never blocks or fails a local store operation.
pub(crate) fn queue_command(cmd: CommandMsg) {
    if let Some(sys) = SYSTEM.get() {
        let _ = sys.command_tx.send(cmd);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CYAN SYSTEM
// ═══════════════════════════════════════════════════════════════════════════

pub struct CyanSystem {
    pub node_id: String,
    pub secret_key: SecretKey,
    pub command_tx: mpsc::UnboundedSender<CommandMsg>,
    pub event_tx: mpsc::UnboundedSender<SwiftEvent>,
    pub network_tx: mpsc::UnboundedSender<NetworkCommand>,

    // ═══════════════════════════════════════════════════════════════════════
    // PER-COMPONENT EVENT BUFFERS - prevents event loss from wrong component polling
    // ═══════════════════════════════════════════════════════════════════════
    /// FileTree events (structure: groups, workspaces, boards, files, sync progress)
    pub file_tree_events: Arc<Mutex<VecDeque<String>>>,
    /// Chat panel events (messages, DMs, peer updates)
    pub chat_panel_events: Arc<Mutex<VecDeque<String>>>,
    /// Whiteboard events (elements, notebook cells)
    pub whiteboard_events: Arc<Mutex<VecDeque<String>>>,
    /// Board grid events (board list, metadata)
    pub board_grid_events: Arc<Mutex<VecDeque<String>>>,
    /// Network/status events (general network status)
    pub network_status_events: Arc<Mutex<VecDeque<String>>>,

    pub db: Arc<Mutex<Connection>>,
    /// Peers per group, shared with NetworkActor for FFI queries
    pub peers_per_group: Arc<Mutex<HashMap<String, HashSet<PublicKey>>>>,
    /// AI bridge for XaeroAI integration
    pub ai_bridge: Arc<AIBridge>,
}

/// The engine's base DDL — the FK clauses on `workspaces.group_id` / `objects.*` are
/// load-bearing (the bundled SQLite is compiled with `SQLITE_DEFAULT_FOREIGN_KEYS=1`,
/// so they are ENFORCED in prod). Public so integration tests can run the exact
/// schema the shipping app runs instead of a drifted FK-less copy.
pub fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            icon TEXT NOT NULL,
            color TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id)
        );
        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY,
            group_id TEXT,
            workspace_id TEXT,
            board_id TEXT,
            type TEXT NOT NULL,
            name TEXT,
            hash TEXT,
            size INTEGER,
            source_peer TEXT,
            local_path TEXT,
            data BLOB,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id),
            FOREIGN KEY(workspace_id) REFERENCES workspaces(id),
            FOREIGN KEY(board_id) REFERENCES objects(id)
        );
        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY,
            board_id TEXT NOT NULL,
            element_type TEXT NOT NULL,
            x REAL NOT NULL,
            y REAL NOT NULL,
            width REAL NOT NULL,
            height REAL NOT NULL,
            z_index INTEGER DEFAULT 0,
            style_json TEXT,
            content_json TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY(board_id) REFERENCES objects(id)
        );
        CREATE INDEX IF NOT EXISTS idx_whiteboard_elements_board ON whiteboard_elements(board_id);
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY,
            board_id TEXT NOT NULL,
            cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL,
            content TEXT,
            output TEXT,
            collapsed INTEGER DEFAULT 0,
            height REAL,
            metadata_json TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY(board_id) REFERENCES objects(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_notebook_cells_order ON notebook_cells(board_id, cell_order);
        CREATE TABLE IF NOT EXISTS board_metadata (
            board_id TEXT PRIMARY KEY,
            labels TEXT DEFAULT '[]',
            rating INTEGER DEFAULT 0,
            view_count INTEGER DEFAULT 0,
            contains_model TEXT,
            contains_skills TEXT DEFAULT '[]',
            board_type TEXT DEFAULT 'canvas',
            last_accessed INTEGER DEFAULT 0,
            is_pinned INTEGER DEFAULT 0,
            -- R11 §9/§9b: per-field LWW clocks (descriptive lane + pin lane).
            meta_updated_at INTEGER DEFAULT 0,
            pin_updated_at INTEGER DEFAULT 0,
            FOREIGN KEY (board_id) REFERENCES objects(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_board_rating ON board_metadata(rating DESC);
        CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY,
            board_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            author_id TEXT NOT NULL,
            author_name TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_notes_board ON notes(board_id);
        "#,
    )?;

    Ok(())
}

impl CyanSystem {
    /// Create system with optional provided secret_key.
    /// If None, generates ephemeral key (for testing - different each launch).
    /// If Some, uses provided key from Swift Keychain (persistent identity).
    async fn new(db_path: String, provided_secret_key: Option<[u8; 32]>) -> Result<Self> {
        let secret_key = match provided_secret_key {
            Some(bytes) => {
                // Use provided key from Swift Keychain - persistent identity
                SecretKey::from_bytes(&bytes)
            }
            None => {
                // Ephemeral key for testing - DIFFERENT EVERY LAUNCH
                let mut rng = ChaCha8Rng::from_os_rng();
                SecretKey::generate(&mut rng)
            }
        };
        let node_id = secret_key.public().to_string();
        eprintln!("🔑 Step 1: Node ID: {} (persistent={})", &node_id[..16], provided_secret_key.is_some());

        // Resolve once so the primary connection and storage::init_db open the
        // SAME file, and create the parent dir / surface a typed error instead of
        // panicking when the data dir does not exist yet.
        let resolved_db_path = storage::resolve_db_path(&db_path);
        eprintln!("🔵 Step 2: resolved DB path: {}", resolved_db_path.display());
        let db_path_clone = resolved_db_path.to_string_lossy().to_string();
        let db = storage::open_db(&resolved_db_path)?;
        ensure_schema(&db)?;
        run_migrations(&db)?;
        eprintln!("🔵 Step 2: DB opened, schema ready");

        // Initialize storage module with DB connection
        storage::init_db(&db_path_clone)?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<CommandMsg>();
        let (net_tx, net_rx) = mpsc::unbounded_channel::<NetworkCommand>();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();

        // Per-component event buffers - prevents event loss from wrong component polling
        let file_tree_events: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
        let chat_panel_events: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
        let whiteboard_events: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
        let board_grid_events: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
        let network_status_events: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
        let peers_per_group: Arc<Mutex<HashMap<String, HashSet<PublicKey>>>> = Arc::new(Mutex::new(HashMap::new()));

        // Clones for event router task
        let file_tree_events_clone = file_tree_events.clone();
        let chat_panel_events_clone = chat_panel_events.clone();
        let whiteboard_events_clone = whiteboard_events.clone();
        let board_grid_events_clone = board_grid_events.clone();
        let network_status_events_clone = network_status_events.clone();
        let secret_key_clone = secret_key.clone();
        let peers_per_group_clone = peers_per_group.clone();

        let db_arc = Arc::new(Mutex::new(db));

        // Create AI bridge
        let ai_bridge = Arc::new(AIBridge::new(
            db_arc.clone(),
            event_tx.clone(),
        ));
        ai_bridge.set_cyan_db_path(PathBuf::from(db_path_clone)).await;
        ai_bridge.start_insight_generator();
        eprintln!("🔵 Step 3: AI bridge started");

        let system = Self {
            node_id: node_id.clone(),
            secret_key: secret_key.clone(),
            event_tx: event_tx.clone(),
            command_tx: cmd_tx,
            network_tx: net_tx.clone(),
            file_tree_events,
            chat_panel_events,
            whiteboard_events,
            board_grid_events,
            network_status_events,
            db: db_arc.clone(),
            peers_per_group,
            ai_bridge,
        };
        eprintln!("🔵 Step 4: System struct created (per-component event routing)");

        // Spawn CommandActor
        let db_clone = system.db.clone();
        let event_tx_clone = event_tx.clone();
        let command_actor_node_id = node_id.clone();
        RUNTIME.get().ok_or_else(|| anyhow::anyhow!("async runtime not initialized"))?.spawn(async move {
            CommandActor {
                db: db_clone,
                rx: cmd_rx,
                network_tx: net_tx,
                event_tx: event_tx_clone,
                node_id: command_actor_node_id,
            }.run().await;
        });
        eprintln!("🔵 Step 5: CommandActor spawned");

        // Spawn NEW NetworkActor from actors module.
        // Build its NodeConfig from the existing globals so behavior is unchanged
        // (this is a seam, not a change): RELAY_URL → relay policy, DISCOVERY_KEY →
        // key, BOOTSTRAP_NODE_ID → bootstrap discovery.
        let node_cfg = crate::models::node_config::NodeConfig {
            relay: match RELAY_URL.get() {
                Some(url) => crate::models::node_config::RelayPolicy::Url(url.clone()),
                None => crate::models::node_config::RelayPolicy::Default,
            },
            discovery: crate::models::node_config::DiscoveryPolicy::Bootstrap(
                bootstrap_node_id().to_string(),
            ),
            discovery_key: DISCOVERY_KEY
                .get()
                .cloned()
                .unwrap_or_else(|| "cyan-dev".to_string()),
        };
        let event_tx_for_network = event_tx.clone();
        eprintln!("🚀 Spawning NetworkActor (new architecture)...");
        RUNTIME.get().ok_or_else(|| anyhow::anyhow!("async runtime not initialized"))?.spawn(async move {
            match actors::NetworkActor::new(
                secret_key_clone,
                event_tx_for_network,
                peers_per_group_clone,
                node_cfg,
            ).await {
                Ok(actor) => {
                    println!("✅ NetworkActor created, starting...");
                    actor.start(net_rx).await;
                },
                Err(e) => eprintln!("❌ NetworkActor failed: {e}"),
            }
        });
        eprintln!("🔵 Step 6: NetworkActor spawned");

        // Event router: routes events to appropriate component buffer(s)
        RUNTIME.get().ok_or_else(|| anyhow::anyhow!("async runtime not initialized"))?.spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match serde_json::to_string(&event) {
                    Ok(event_json) => {
                        route_event_to_buffers(
                            &event,
                            &event_json,
                            &file_tree_events_clone,
                            &chat_panel_events_clone,
                            &whiteboard_events_clone,
                            &board_grid_events_clone,
                            &network_status_events_clone,
                        );
                    }
                    Err(e) => {
                        eprintln!("Failed to serialize event: {e:?}");
                    }
                }
            }
        });

        Ok(system)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// COMMAND ACTOR
// ═══════════════════════════════════════════════════════════════════════════

struct CommandActor {
    db: Arc<Mutex<Connection>>,
    rx: mpsc::UnboundedReceiver<CommandMsg>,
    network_tx: mpsc::UnboundedSender<NetworkCommand>,
    event_tx: mpsc::UnboundedSender<SwiftEvent>,
    node_id: String,
}

impl CommandActor {
    async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                CommandMsg::CreateGroup { name, icon, color } => {
                    let id = blake3::hash(format!("{}-{}", name, chrono::Utc::now()).as_bytes()).to_hex().to_string();
                    let now = chrono::Utc::now().timestamp();
                    let g = Group {
                        id: id.clone(),
                        name: name.clone(),
                        icon: icon.clone(),
                        color: color.clone(),
                        created_at: now,
                    };

                    {
                        let db = self.db.lock_safe();
                        // A failed group INSERT cascades (workspaces/objects FK-reference
                        // groups, and the bundled SQLite ENFORCES FKs) — log it loudly
                        // instead of letting downstream provisioning fail mysteriously.
                        if let Err(e) = db.execute(
                            "INSERT INTO groups (id, name, icon, color, created_at, owner_node_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![g.id, g.name, g.icon, g.color, g.created_at, self.node_id],
                        ) {
                            tracing::error!(tenant_id = %id, "CreateGroup: groups INSERT failed: {e}");
                        }
                    }

                    // ROUND8 §W3: a group is never born empty — auto-seed the default
                    // landing workspace and the per-group system "Plugins" workspace.
                    // Both ride the existing snapshot/digest replication; broadcasting
                    // their WorkspaceCreated events also delivers them to already-live
                    // peers (the same path a normal CreateWorkspace uses).
                    let seeded = storage::provision_group_workspaces(&id, Some(&self.node_id));

                    let _ = self.network_tx.send(NetworkCommand::JoinGroup {
                        group_id: id.clone(),
                        bootstrap_peer: None,
                        grant: None,
                    });
                    let _ = self.network_tx.send(NetworkCommand::Broadcast {
                        group_id: id.clone(),
                        event: NetworkEvent::GroupCreated(g.clone()),
                    });

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::GroupCreated(g)));

                    match seeded {
                        Ok((default, plugins)) => {
                            let default_ws_id = default.id.clone();
                            for ws in [default, plugins] {
                                tracing::info!(
                                    tenant_id = %id,
                                    "obs group_provision_ws group={} ws={} system={}",
                                    id, ws.id, ws.system
                                );
                                let _ = self.network_tx.send(NetworkCommand::Broadcast {
                                    group_id: id.clone(),
                                    event: NetworkEvent::WorkspaceCreated(ws.clone()),
                                });
                                let _ = self.event_tx.send(SwiftEvent::Network(
                                    NetworkEvent::WorkspaceCreated(ws),
                                ));
                            }

                            // TIER 3.5 (AUTHORING_FIXES_ROUND2): a new group is never
                            // born board-less either — auto-create a DEFAULT BOARD in
                            // the landing workspace so the first click lands on an
                            // authorable Workflow, not an empty grid. Deterministic id
                            // (INSERT OR IGNORE) ⇒ idempotent on re-delivery; creator-
                            // only (this handler never runs on sync receivers — the
                            // board reaches them via the same broadcast/snapshot path
                            // any board does). Seeding lives in storage so the real-
                            // schema regression test drives the identical code path.
                            match storage::provision_default_board(&default_ws_id, &self.node_id, now) {
                                Ok((board_id, board_name)) => {
                                    let board_event = NetworkEvent::BoardCreated {
                                        id: board_id,
                                        workspace_id: default_ws_id,
                                        name: board_name,
                                        created_at: now,
                                    };
                                    let _ = self.network_tx.send(NetworkCommand::Broadcast {
                                        group_id: id.clone(),
                                        event: board_event.clone(),
                                    });
                                    let _ = self.event_tx.send(SwiftEvent::Network(board_event));
                                }
                                Err(e) => tracing::error!(
                                    tenant_id = %id,
                                    "CreateGroup: default-board seed failed: {e}"
                                ),
                            }
                        }
                        Err(e) => tracing::error!(tenant_id = %id, "group provisioning failed: {e}"),
                    }
                }

                CommandMsg::RenameGroup { id, name } => {
                    let ok = {
                        let db = self.db.lock_safe();
                        db.execute("UPDATE groups SET name=?1 WHERE id=?2", params![name, id]).unwrap_or(0) > 0
                    };

                    if ok {
                        let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::GroupRenamed {
                            id: id.clone(),
                            name: name.clone(),
                        }));

                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: id.clone(),
                            event: NetworkEvent::GroupRenamed { id, name },
                        });
                    }
                }

                CommandMsg::DeleteGroup { id } => {
                    // Check ownership
                    let is_owner = storage::group_is_owner(&id, &self.node_id);

                    if is_owner {
                        eprintln!("🗑️ [DELETE-GROUP] Owner deleting group: {}...", &id[..16.min(id.len())]);

                        // Owner: broadcast dissolution to all peers FIRST
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: id.clone(),
                            event: NetworkEvent::GroupDissolved { id: id.clone() },
                        });

                        // Then delete locally using storage function
                        match storage::group_delete(&id) {
                            Ok(_) => eprintln!("🗑️ [DELETE-GROUP] ✓ Cascade delete complete"),
                            Err(e) => eprintln!("🗑️ [DELETE-GROUP] ⚠️ Cascade delete failed: {}", e),
                        }

                        let _ = self.event_tx.send(SwiftEvent::GroupDeleted { id: id.clone() });
                        let _ = self.network_tx.send(NetworkCommand::DissolveGroup { id });
                    } else {
                        // Not owner - send error
                        let _ = self.event_tx.send(SwiftEvent::Error {
                            message: "Only the group owner can delete it. Use Leave instead.".into()
                        });
                    }
                }

                CommandMsg::LeaveGroup { id } => {
                    // Non-owner leaving: local delete only, no broadcast
                    eprintln!("🚪 [LEAVE-GROUP] Starting cascade delete for group: {}...", &id[..16.min(id.len())]);

                    match storage::group_delete(&id) {
                        Ok(deleted) => {
                            if deleted {
                                eprintln!("🚪 [LEAVE-GROUP] ✓ Cascade delete complete");
                            } else {
                                eprintln!("🚪 [LEAVE-GROUP] ⚠️ Group not found in DB");
                            }
                        }
                        Err(e) => {
                            eprintln!("🚪 [LEAVE-GROUP] ⚠️ Cascade delete failed: {}", e);
                        }
                    }

                    let _ = self.network_tx.send(NetworkCommand::LeaveGroup { id: id.clone() });
                    let _ = self.event_tx.send(SwiftEvent::GroupLeft { id });
                }

                CommandMsg::CreateWorkspace { group_id, name } => {
                    let id = blake3::hash(format!("ws:{}-{}", group_id, name).as_bytes()).to_hex().to_string();
                    let now = chrono::Utc::now().timestamp();
                    let ws = Workspace {
                        id: id.clone(),
                        group_id: group_id.clone(),
                        name: name.clone(),
                        created_at: now,
                        system: false,
                    };

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "INSERT OR IGNORE INTO workspaces (id, group_id, name, created_at, owner_node_id) VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![ws.id, ws.group_id, ws.name, ws.created_at, self.node_id],
                        );
                    }

                    eprintln!("📤 [CMD] Broadcasting WorkspaceCreated:");
                    eprintln!("   workspace_id: {}...", &ws.id[..16.min(ws.id.len())]);
                    eprintln!("   group_id: {}...", &group_id[..16.min(group_id.len())]);

                    match self.network_tx.send(NetworkCommand::Broadcast {
                        group_id: group_id.clone(),
                        event: NetworkEvent::WorkspaceCreated(ws.clone()),
                    }) {
                        Ok(_) => eprintln!("📤 [CMD] ✓ Broadcast sent to NetworkActor"),
                        Err(e) => eprintln!("📤 [CMD] 🔴 Broadcast FAILED: {}", e),
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::WorkspaceCreated(ws)));
                }

                CommandMsg::RenameWorkspace { id, name } => {
                    let group_id = {
                        let db = self.db.lock_safe();
                        db.query_row("SELECT group_id FROM workspaces WHERE id=?1", params![id], |r| r.get::<_, String>(0)).ok()
                    };

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute("UPDATE workspaces SET name=?1 WHERE id=?2", params![name, id]);
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::WorkspaceRenamed { id: id.clone(), name: name.clone() },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::WorkspaceRenamed { id, name }));
                }

                CommandMsg::DeleteWorkspace { id } => {
                    let group_id = storage::workspace_get_group_id(&id);
                    let is_owner = storage::workspace_is_owner(&id, &self.node_id);

                    if is_owner {
                        // Owner: broadcast dissolution
                        if let Some(ref gid) = group_id {
                            let _ = self.network_tx.send(NetworkCommand::Broadcast {
                                group_id: gid.clone(),
                                event: NetworkEvent::WorkspaceDissolved { id: id.clone() },
                            });
                        }

                        // Delete locally using storage function
                        let _ = storage::workspace_delete(&id);

                        if let Some(gid) = group_id {
                            let _ = self.network_tx.send(NetworkCommand::DissolveWorkspace { id: id.clone(), group_id: gid });
                        }
                        let _ = self.event_tx.send(SwiftEvent::WorkspaceDeleted { id });
                    } else {
                        let _ = self.event_tx.send(SwiftEvent::Error {
                            message: "Only the workspace owner can delete it. Use Leave instead.".into()
                        });
                    }
                }

                CommandMsg::LeaveWorkspace { id } => {
                    // Non-owner leaving: local delete only
                    let _ = storage::workspace_delete(&id);

                    let _ = self.network_tx.send(NetworkCommand::LeaveWorkspace { id: id.clone() });
                    let _ = self.event_tx.send(SwiftEvent::WorkspaceLeft { id });
                }

                CommandMsg::CreateBoard { workspace_id, name } => {
                    let id = blake3::hash(format!("board:{}-{}", workspace_id, name).as_bytes()).to_hex().to_string();
                    let now = chrono::Utc::now().timestamp();

                    let group_id = {
                        let db = self.db.lock_safe();
                        db.query_row("SELECT group_id FROM workspaces WHERE id=?1", params![workspace_id], |r| r.get::<_, String>(0)).ok()
                    };

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, created_at, owner_node_id) VALUES (?1, ?2, 'whiteboard', ?3, ?4, ?5)",
                            params![id, workspace_id, name, now, self.node_id],
                        );
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::BoardCreated {
                                id: id.clone(),
                                workspace_id: workspace_id.clone(),
                                name: name.clone(),
                                created_at: now,
                            },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::BoardCreated {
                        id,
                        workspace_id,
                        name,
                        created_at: now,
                    }));
                }

                CommandMsg::RenameBoard { id, name } => {
                    let group_id = self.get_group_id_for_board(&id);
                    self.note_board_activity(&id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute("UPDATE objects SET name=?1 WHERE id=?2 AND type='whiteboard'", params![name, id]);
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::BoardRenamed { id: id.clone(), name: name.clone() },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::BoardRenamed { id, name }));
                }

                CommandMsg::DeleteBoard { id } => {
                    let group_id = storage::board_get_group_id(&id);
                    let is_owner = storage::board_is_owner(&id, &self.node_id);

                    if is_owner {
                        // Owner: broadcast dissolution
                        if let Some(ref gid) = group_id {
                            let _ = self.network_tx.send(NetworkCommand::Broadcast {
                                group_id: gid.clone(),
                                event: NetworkEvent::BoardDissolved { id: id.clone() },
                            });
                        }

                        // Delete locally using storage function
                        let _ = storage::board_delete(&id);

                        if let Some(gid) = group_id {
                            let _ = self.network_tx.send(NetworkCommand::DissolveBoard { id: id.clone(), group_id: gid });
                        }
                        let _ = self.event_tx.send(SwiftEvent::BoardDeleted { id });
                    } else {
                        let _ = self.event_tx.send(SwiftEvent::Error {
                            message: "Only the board owner can delete it. Use Leave instead.".into()
                        });
                    }
                }

                CommandMsg::LeaveBoard { id } => {
                    // Non-owner leaving: local delete only
                    let _ = storage::board_delete(&id);

                    let _ = self.network_tx.send(NetworkCommand::LeaveBoard { id: id.clone() });
                    let _ = self.event_tx.send(SwiftEvent::BoardLeft { id });
                }

                CommandMsg::SendChat { board_id, message, parent_id } => {
                    let id = blake3::hash(format!("chat:{}-{}-{}", board_id, message, chrono::Utc::now()).as_bytes()).to_hex().to_string();
                    let now = chrono::Utc::now().timestamp();
                    let author = self.node_id.clone();

                    // R11 §1: chat is board-scoped. Derive the board's workspace (for storage
                    // scoping) and group (for gossip) — a board belongs to exactly one of each.
                    let workspace_id = storage::board_get_workspace_id(&board_id).unwrap_or_default();
                    let group_id = storage::board_get_group_id(&board_id);

                    eprintln!("💬 [CHAT] SendChat board={}... author={}...",
                        &board_id[..16.min(board_id.len())], &author[..16.min(author.len())]);

                    match storage::chat_insert(&id, &board_id, &workspace_id, &message, &author, parent_id.as_deref(), now) {
                        Ok(_) => eprintln!("💬 [CHAT] ✓ Chat inserted to DB via storage module"),
                        Err(e) => eprintln!("💬 [CHAT] 🔴 DB INSERT FAILED: {}", e),
                    }

                    if let Some(gid) = group_id {
                        eprintln!("💬 [CHAT] Broadcasting ChatSent to group {}...", &gid[..16.min(gid.len())]);
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::ChatSent {
                                id: id.clone(),
                                board_id: board_id.clone(),
                                workspace_id: workspace_id.clone(),
                                message: message.clone(),
                                author: author.clone(),
                                parent_id: parent_id.clone(),
                                timestamp: now,
                            },
                        });
                    } else {
                        eprintln!("💬 [CHAT] ⚠️ No group_id found for board, skipping broadcast");
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::ChatSent {
                        id,
                        board_id,
                        workspace_id,
                        message,
                        author,
                        parent_id,
                        timestamp: now,
                    }));
                }

                CommandMsg::DeleteChat { id } => {
                    // Use storage module for consistent DB access
                    let ws_id = storage::chat_get_workspace_id(&id);
                    let group_id = ws_id.as_ref().and_then(|ws| storage::workspace_get_group_id(ws));

                    // Delete using storage module
                    let _ = storage::chat_delete(&id);

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::ChatDeleted { id: id.clone() },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::ChatDeleted { id }));
                }

                // ── Note commands (ROUND8 §W2) — board-level authored LWW ledger ──
                CommandMsg::PutNote { board_id, note_id, tenant_id, text, scope, kind } => {
                    // feat/notes-constitution: scope/kind are additive; absent ⇒ the exact
                    // pre-scope behavior (a board editor-note). Invalid values REJECT the
                    // command (never silently misfile a constitution/preference note).
                    let scope = scope.unwrap_or_else(crate::models::dto::default_note_scope);
                    let kind = kind.unwrap_or_else(crate::models::dto::default_note_kind);
                    if !crate::models::dto::note_scope_valid(&scope)
                        || !crate::models::dto::note_kind_valid(&kind)
                    {
                        tracing::error!(
                            "PutNote rejected: invalid scope={scope:?} kind={kind:?} board={board_id}"
                        );
                        continue;
                    }

                    let now = chrono::Utc::now().timestamp();
                    let author_id = self.node_id.clone();
                    // author_name resolves from the author's XaeroID profile (same path
                    // presence/chat use); fall back to the raw id if no profile yet.
                    let author_name = storage::profile_get(&author_id)
                        .map(|(name, _)| name)
                        .filter(|n| !n.is_empty())
                        .unwrap_or_else(|| author_id.clone());

                    // Tenant: explicit, else the board's group (group == tenant). For
                    // group/tenant scope, `board_id` IS the anchor (the group/tenant id),
                    // so it is also the broadcast group.
                    let group_id = if scope == "board" {
                        self.get_group_id_for_board(&board_id)
                    } else {
                        Some(board_id.clone())
                    };
                    let tenant = tenant_id
                        .or_else(|| group_id.clone())
                        .unwrap_or_else(|| author_id.clone());

                    // Editing an existing note preserves its original created_at; a new
                    // note gets a generated id + created_at = now. An id that resolves to
                    // an existing row is an edit (NoteUpdated); otherwise it's an add.
                    let id = note_id.unwrap_or_else(|| {
                        blake3::hash(format!("note:{board_id}-{text}-{now}").as_bytes())
                            .to_hex()
                            .to_string()
                    });
                    let existing = storage::note_get(&id).ok().flatten();
                    let is_new = existing.is_none();
                    let created_at = existing.map(|n| n.created_at).unwrap_or(now);

                    let note = crate::models::dto::NoteDTO {
                        id: id.clone(),
                        board_id: board_id.clone(),
                        tenant_id: tenant.clone(),
                        author_id: author_id.clone(),
                        author_name: author_name.clone(),
                        text: text.clone(),
                        created_at,
                        updated_at: now,
                        scope: scope.clone(),
                        kind: kind.clone(),
                    };
                    match storage::note_upsert(&note) {
                        Ok(_) => tracing::info!(tenant_id = %tenant, "obs note_put board={board_id} id={id} scope={scope} kind={kind}"),
                        Err(e) => eprintln!("📝 [NOTE] 🔴 note_upsert failed: {e}"),
                    }

                    let event = if is_new {
                        NetworkEvent::NoteAdded {
                            id, board_id, tenant_id: tenant, author_id, author_name,
                            text, created_at, updated_at: now, scope, kind,
                        }
                    } else {
                        NetworkEvent::NoteUpdated {
                            id, board_id, tenant_id: tenant, author_id, author_name,
                            text, created_at, updated_at: now, scope, kind,
                        }
                    };

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: event.clone(),
                        });
                    }
                    let _ = self.event_tx.send(SwiftEvent::Network(event));
                }

                CommandMsg::DeleteNote { id } => {
                    let group_id = storage::note_get(&id)
                        .ok()
                        .flatten()
                        .and_then(|n| self.get_group_id_for_board(&n.board_id));
                    let _ = storage::note_delete(&id);
                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::NoteDeleted { id: id.clone() },
                        });
                    }
                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::NoteDeleted { id }));
                }

                // ── Template + pin commands (ROUND8 §W4) ──
                CommandMsg::WorkflowFromTemplate { template_id, board_id, tenant_id } => {
                    let group_id = self.get_group_id_for_board(&board_id);
                    // Tenant: explicit, else the board's group (group == tenant).
                    let tenant = tenant_id
                        .or_else(|| group_id.clone())
                        .unwrap_or_else(|| board_id.clone());

                    match crate::templates::clone_to_board(&template_id, &board_id, &tenant) {
                        Ok(cells) => {
                            tracing::info!(
                                tenant_id = %tenant,
                                "obs workflow_from_template template={template_id} board={board_id} steps={}",
                                cells.len()
                            );
                            // Broadcast each cloned step so already-live peers converge
                            // immediately (cold joiners get them via the snapshot too).
                            for c in cells {
                                let event = NetworkEvent::NotebookCellAdded {
                                    id: c.id,
                                    board_id: c.board_id,
                                    cell_type: c.cell_type,
                                    cell_order: c.cell_order,
                                    content: c.content,
                                };
                                if let Some(gid) = group_id.clone() {
                                    let _ = self.network_tx.send(NetworkCommand::Broadcast {
                                        group_id: gid,
                                        event: event.clone(),
                                    });
                                }
                                let _ = self.event_tx.send(SwiftEvent::Network(event));
                            }
                        }
                        Err(e) => eprintln!("📋 [TEMPLATE] 🔴 clone_to_board failed: {e}"),
                    }
                }

                CommandMsg::SetPin { board_id, pinned } => {
                    let now = chrono::Utc::now().timestamp();
                    let group_id = self.get_group_id_for_board(&board_id);
                    let tenant = group_id.clone().unwrap_or_else(|| board_id.clone());

                    let pin = crate::models::dto::PinDTO {
                        board_id: board_id.clone(),
                        tenant_id: tenant.clone(),
                        pinned,
                        updated_at: now,
                    };
                    match storage::pin_upsert(&pin) {
                        Ok(_) => tracing::info!(tenant_id = %tenant, "obs pin_set board={board_id} pinned={pinned}"),
                        Err(e) => eprintln!("📌 [PIN] 🔴 pin_upsert failed: {e}"),
                    }

                    let event = NetworkEvent::PinSet {
                        board_id, tenant_id: tenant, pinned, updated_at: now,
                    };
                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: event.clone(),
                        });
                    }
                    let _ = self.event_tx.send(SwiftEvent::Network(event));
                }

                // Whiteboard element commands
                CommandMsg::CreateWhiteboardElement { board_id, element_type, x, y, width, height, z_index, style_json, content_json } => {
                    let id = blake3::hash(format!("elem:{}-{}", board_id, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)).as_bytes()).to_hex().to_string();
                    let now = chrono::Utc::now().timestamp();
                    let group_id = self.get_group_id_for_board(&board_id);
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "INSERT INTO whiteboard_elements (id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                            params![id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, now, now],
                        );
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::WhiteboardElementAdded {
                                id: id.clone(),
                                board_id: board_id.clone(),
                                element_type: element_type.clone(),
                                x, y, width, height, z_index,
                                style_json: style_json.clone(),
                                content_json: content_json.clone(),
                                created_at: now,
                                updated_at: now,
                            },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::WhiteboardElementAdded {
                        id, board_id, element_type, x, y, width, height, z_index,
                        style_json, content_json, created_at: now, updated_at: now,
                    }));
                }

                CommandMsg::UpdateWhiteboardElement { id, board_id, element_type, x, y, width, height, z_index, style_json, content_json } => {
                    let now = chrono::Utc::now().timestamp();
                    let group_id = self.get_group_id_for_board(&board_id);
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "UPDATE whiteboard_elements SET board_id=?2, element_type=?3, x=?4, y=?5, width=?6, height=?7, z_index=?8, style_json=?9, content_json=?10, updated_at=?11 WHERE id=?1",
                            params![id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, now],
                        );
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::WhiteboardElementUpdated {
                                id: id.clone(),
                                board_id: board_id.clone(),
                                element_type: element_type.clone(),
                                x, y, width, height, z_index,
                                style_json: style_json.clone(),
                                content_json: content_json.clone(),
                                updated_at: now,
                            },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::WhiteboardElementUpdated {
                        id, board_id, element_type, x, y, width, height, z_index,
                        style_json, content_json, updated_at: now,
                    }));
                }

                CommandMsg::DeleteWhiteboardElement { id, board_id } => {
                    let group_id = self.get_group_id_for_board(&board_id);
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute("DELETE FROM whiteboard_elements WHERE id=?1", params![id]);
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::WhiteboardElementDeleted { id: id.clone(), board_id: board_id.clone() },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::WhiteboardElementDeleted { id, board_id }));
                }

                CommandMsg::ClearWhiteboard { board_id } => {
                    let group_id = self.get_group_id_for_board(&board_id);
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute("DELETE FROM whiteboard_elements WHERE board_id=?1", params![board_id]);
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::WhiteboardCleared { board_id: board_id.clone() },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::WhiteboardCleared { board_id }));
                }

                // Notebook cell commands
                CommandMsg::AddNotebookCell { board_id, cell_type, cell_order, content } => {
                    // §W1: the step is the only authorable kind — collapse legacy kinds.
                    let cell_type = crate::workflow::coerce_authoring_cell_type(&cell_type);
                    let id = blake3::hash(format!("cell:{}-{}", board_id, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)).as_bytes()).to_hex().to_string();
                    let now = chrono::Utc::now().timestamp();
                    let group_id = self.get_group_id_for_board(&board_id);
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "INSERT INTO notebook_cells (id, board_id, cell_type, cell_order, content, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                            params![id, board_id, cell_type, cell_order, content, now, now],
                        );
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::NotebookCellAdded {
                                id: id.clone(),
                                board_id: board_id.clone(),
                                cell_type: cell_type.clone(),
                                cell_order,
                                content: content.clone(),
                            },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::NotebookCellAdded {
                        id, board_id, cell_type, cell_order, content,
                    }));
                }

                CommandMsg::UpdateNotebookCell { id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json } => {
                    // §W1: keep authoring writes on the single step primitive.
                    let cell_type = crate::workflow::coerce_authoring_cell_type(&cell_type);
                    let now = chrono::Utc::now().timestamp();
                    let group_id = self.get_group_id_for_board(&board_id);
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "UPDATE notebook_cells SET cell_type=?2, cell_order=?3, content=?4, output=?5, collapsed=?6, height=?7, metadata_json=?8, updated_at=?9 WHERE id=?1",
                            params![id, cell_type, cell_order, content, output, collapsed as i32, height, metadata_json, now],
                        );
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::NotebookCellUpdated {
                                id: id.clone(),
                                board_id: board_id.clone(),
                                cell_type: cell_type.clone(),
                                cell_order,
                                content: content.clone(),
                                output: output.clone(),
                                collapsed,
                                height,
                                metadata_json: metadata_json.clone(),
                            },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::NotebookCellUpdated {
                        id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json,
                    }));
                }

                CommandMsg::DeleteNotebookCell { id, board_id } => {
                    let group_id = self.get_group_id_for_board(&board_id);
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute("DELETE FROM notebook_cells WHERE id=?1", params![id]);
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::NotebookCellDeleted { id: id.clone(), board_id: board_id.clone() },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::NotebookCellDeleted { id, board_id }));
                }

                CommandMsg::ReorderNotebookCells { board_id, cell_ids } => {
                    let group_id = self.get_group_id_for_board(&board_id);
                    let now = chrono::Utc::now().timestamp();
                    self.note_board_activity(&board_id, group_id.as_deref());

                    {
                        let db = self.db.lock_safe();
                        for (order, cell_id) in cell_ids.iter().enumerate() {
                            let _ = db.execute(
                                "UPDATE notebook_cells SET cell_order=?1, updated_at=?2 WHERE id=?3",
                                params![order as i64, now, cell_id],
                            );
                        }
                    }

                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::NotebookCellsReordered {
                                board_id: board_id.clone(),
                                cell_ids: cell_ids.clone(),
                            },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::NotebookCellsReordered { board_id, cell_ids }));
                }

                // Board metadata commands
                CommandMsg::UpdateBoardMetadata { board_id, labels, rating, view_count, contains_model, contains_skills, board_type, last_accessed, is_pinned } => {
                    // R11 §9/PATTERN: per-field convergent LWW upsert — never a whole-record
                    // replace. The descriptive + pin lanes are both stamped `now` (a local edit
                    // is the newest writer); the snapshot merge then resolves cross-peer races
                    // per-field instead of clobbering.
                    let now = chrono::Utc::now().timestamp();
                    let _ = storage::board_metadata_upsert(
                        &board_id,
                        &labels,
                        rating,
                        view_count,
                        contains_model.as_deref(),
                        &contains_skills,
                        board_type.as_deref(),
                        last_accessed.unwrap_or(0),
                        is_pinned,
                        now,
                        now,
                    );

                    let _ = self.event_tx.send(SwiftEvent::BoardMetadataUpdated { board_id });
                }

                CommandMsg::IncrementBoardViewCount { board_id } => {
                    let now = chrono::Utc::now().timestamp();
                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "UPDATE board_metadata SET view_count = view_count + 1, last_accessed = ?1 WHERE board_id = ?2",
                            params![now, board_id],
                        );
                    }
                }

                CommandMsg::SetBoardPinned { board_id, is_pinned } => {
                    // R10FB §B3: pinning is a SYNCED board property. Upsert the flag locally,
                    // then gossip `BoardPinned` so the pin appears on peers (the previous
                    // local-only UPDATE was the "pin didn't show on peer 2" bug).
                    let now = chrono::Utc::now().timestamp();
                    let _ = storage::board_meta_set_pinned(&board_id, is_pinned, now);
                    let group_id = self.get_group_id_for_board(&board_id);
                    let event = NetworkEvent::BoardPinned {
                        board_id: board_id.clone(),
                        is_pinned,
                        updated_at: now,
                    };
                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: event.clone(),
                        });
                    }
                    let _ = self.event_tx.send(SwiftEvent::Network(event));
                    // Keep the existing local UI signal for back-compat.
                    let _ = self.event_tx.send(SwiftEvent::BoardMetadataUpdated { board_id });
                }

                CommandMsg::MarkRead { scope_id } => {
                    // R11 §3/§5: `scope_id` is a board id — opening the board's chat clears its
                    // dot/count (board-level only, no rollup). Emit UnreadChanged so iOS + the
                    // dock badge update live.
                    let _ = storage::unread_mark_read(&scope_id);
                    if let Ok(counts) = storage::unread_counts() {
                        let _ = self.event_tx.send(SwiftEvent::UnreadChanged { counts });
                    }
                }

                CommandMsg::DeleteFile { file_id } => {
                    // R10FB §F4: user-initiated soft-delete/tombstone that syncs to peers.
                    let now = chrono::Utc::now().timestamp();
                    let group_id = storage::file_get_group_id(&file_id);
                    let _ = storage::file_soft_delete(&file_id, now);
                    let event = NetworkEvent::FileDeleted { id: file_id.clone(), deleted_at: now };
                    if let Some(gid) = group_id {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: event.clone(),
                        });
                    }
                    let _ = self.event_tx.send(SwiftEvent::Network(event));
                }

                // Integration commands
                CommandMsg::AddIntegration { scope_type, scope_id, integration_type, config } => {
                    let id = blake3::hash(format!("integ:{}-{}-{}", scope_type, scope_id, chrono::Utc::now()).as_bytes()).to_hex().to_string();
                    let now = chrono::Utc::now().timestamp();
                    let config_json = serde_json::to_string(&config).unwrap_or_else(|_| "{}".to_string());

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "INSERT INTO integration_bindings (id, scope_type, scope_id, integration_type, config_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![id, scope_type, scope_id, integration_type, config_json, now],
                        );
                    }
                }

                CommandMsg::RemoveIntegration { id } => {
                    let db = self.db.lock_safe();
                    let _ = db.execute("DELETE FROM integration_bindings WHERE id=?1", params![id]);
                }

                // Profile commands
                CommandMsg::UpdateProfile { display_name, avatar_hash } => {
                    let node_id = self.node_id.clone();

                    {
                        let db = self.db.lock_safe();
                        let _ = db.execute(
                            "INSERT OR REPLACE INTO user_profiles (node_id, display_name, avatar_hash, updated_at) VALUES (?1, ?2, ?3, ?4)",
                            params![node_id, display_name, avatar_hash, chrono::Utc::now().timestamp()],
                        );
                    }

                    // Broadcast to all groups
                    let groups = (|| -> rusqlite::Result<Vec<String>> {
                        let db = self.db.lock_safe();
                        let mut stmt = db.prepare("SELECT id FROM groups")?;
                        let mut rows = stmt.query([])?;
                        let mut out = vec![];
                        while let Some(r) = rows.next()? {
                            out.push(r.get::<_, String>(0)?);
                        }
                        Ok(out)
                    })()
                    .unwrap_or_default();

                    for gid in groups {
                        let _ = self.network_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::ProfileUpdated {
                                node_id: node_id.clone(),
                                display_name: display_name.clone(),
                                avatar_hash: avatar_hash.clone(),
                            },
                        });
                    }

                    let _ = self.event_tx.send(SwiftEvent::Network(NetworkEvent::ProfileUpdated {
                        node_id,
                        display_name,
                        avatar_hash,
                    }));
                }

                // ---- Chat History (R11 §1 — board-scoped) ----
                CommandMsg::LoadChatHistory { board_id } => {
                    eprintln!("💬 [CHAT] LoadChatHistory for board {}...", &board_id[..16.min(board_id.len())]);
                    let workspace_id = storage::board_get_workspace_id(&board_id).unwrap_or_default();

                    match storage::chat_list_by_board(&board_id) {
                        Ok(chats) => {
                            let chat_count = chats.len();
                            eprintln!("💬 [CHAT] Found {} chats in DB", chat_count);

                            // Emit each chat as a ChatSent event to the chat_panel buffer
                            for chat in chats {
                                let event = SwiftEvent::Network(NetworkEvent::ChatSent {
                                    id: chat.id,
                                    board_id: chat.board_id.clone(),
                                    workspace_id: chat.workspace_id.clone(),
                                    message: chat.message,
                                    author: chat.author,
                                    parent_id: chat.parent_id,
                                    timestamp: chat.timestamp,
                                });
                                let _ = self.event_tx.send(event);
                            }

                            // Signal history loading complete (board-scoped; workspace kept for back-compat).
                            let _ = self.event_tx.send(SwiftEvent::ChatHistoryComplete {
                                board_id: board_id.clone(),
                                workspace_id,
                            });
                            eprintln!("💬 [CHAT] ChatHistoryComplete ({} msgs)", chat_count);
                        }
                        Err(e) => {
                            eprintln!("💬 [CHAT] 🔴 Failed to load chat history: {}", e);
                        }
                    }
                }

                // ---- Direct Message Commands (handled by NetworkActor) ----
                CommandMsg::StartDirectChat { peer_id, workspace_id } => {
                    let _ = self.network_tx.send(NetworkCommand::StartChatStream {
                        peer_id,
                        workspace_id,
                    });
                }

                CommandMsg::SendDirectMessage { peer_id, workspace_id, message, parent_id } => {
                    let _ = self.network_tx.send(NetworkCommand::SendDirectChat {
                        peer_id,
                        workspace_id,
                        message,
                        parent_id,
                        attachment: None,
                    });
                }

                CommandMsg::LoadDirectMessageHistory { peer_id } => {
                    eprintln!("💬 [DM] LoadDirectMessageHistory for peer {}...", &peer_id[..16.min(peer_id.len())]);

                    // Query DMs from DB
                    match storage::dm_list_by_peer(&peer_id, 100) {
                        Ok(dms) => {
                            eprintln!("💬 [DM] Found {} DMs in DB", dms.len());

                            // Emit each DM as a DirectMessageReceived event
                            // Note: dm_list_by_peer returns (id, message, timestamp, is_incoming)
                            for (id, message, timestamp, is_incoming) in dms {
                                let event = SwiftEvent::DirectMessageReceived {
                                    id,
                                    peer_id: peer_id.clone(),
                                    message,
                                    timestamp,
                                    is_incoming,
                                };
                                let _ = self.event_tx.send(event);
                            }
                        }
                        Err(e) => {
                            eprintln!("💬 [DM] 🔴 Failed to load DM history: {}", e);
                        }
                    }
                }

                // ---- System Commands ----
                CommandMsg::Snapshot {} => {
                    // Snapshot is handled via cyan_snapshot FFI, this triggers tree reload
                    let tree_json = dump_tree_json(&self.db);
                    let _ = self.event_tx.send(SwiftEvent::TreeLoaded(tree_json));
                }

                CommandMsg::SeedDemoIfEmpty => {
                    // R10FB §D: demo seeding REMOVED. Inert no-op kept for ABI/command-shape
                    // stability — never creates a "Demo Group"/"Demo Board".
                    tracing::debug!("SeedDemoIfEmpty is a no-op (demo seeding removed)");
                }

                // ── Ledger sync deltas (CYAN_FORMAT_SPEC §6.2) ─────────────────
                // Engine-internal: queued by `changelist::dispatch` after a LOCAL
                // ledger mutation (via `queue_command`); this loop's only job is to
                // put the matching NetworkEvent on the group topic (tenant == group).
                // The receiver applies through the same idempotent `changelist::`
                // fns, so a re-broadcast or an echo is a no-op.
                CommandMsg::ChangeEntryAppended { tenant_id, entry } => {
                    let _ = self.network_tx.send(NetworkCommand::Broadcast {
                        group_id: tenant_id.clone(),
                        event: NetworkEvent::ChangeEntryAppended { tenant_id, entry },
                    });
                }
                CommandMsg::ChangeEntryLifecycle { tenant_id, delta } => {
                    let _ = self.network_tx.send(NetworkCommand::Broadcast {
                        group_id: tenant_id.clone(),
                        event: NetworkEvent::ChangeEntryLifecycle { tenant_id, delta },
                    });
                }
                CommandMsg::ChangeVersionCreated { tenant_id, version } => {
                    let _ = self.network_tx.send(NetworkCommand::Broadcast {
                        group_id: tenant_id.clone(),
                        event: NetworkEvent::ChangeVersionCreated { tenant_id, version },
                    });
                }
                CommandMsg::ChangeBranchHead { tenant_id, asset_hash, branch, head_version, updated_at } => {
                    let _ = self.network_tx.send(NetworkCommand::Broadcast {
                        group_id: tenant_id.clone(),
                        event: NetworkEvent::ChangeBranchHead {
                            tenant_id, asset_hash, branch, head_version, updated_at,
                        },
                    });
                }

                CommandMsg::SeedDemo => {
                    // Fix A: seed the coherent demo set IN-PROCESS under THIS engine's
                    // identity, so the seeded groups are stamped with our node_id (owned +
                    // manageable) and land in the db the app actually opened — no separate
                    // node process, no db-path/identity divergence. Then emit a fresh tree
                    // snapshot so the Explorer repopulates immediately.
                    match seed::seed_demo(&self.node_id) {
                        Ok(summary) => tracing::info!("SeedDemo (in-process) ok: {summary}"),
                        Err(e) => tracing::error!("SeedDemo failed: {e}"),
                    }
                    let tree_json = dump_tree_json(&self.db);
                    let _ = self.event_tx.send(SwiftEvent::TreeLoaded(tree_json));
                }
            }
        }
    }

    fn get_group_id_for_board(&self, board_id: &str) -> Option<String> {
        let db = self.db.lock_safe();
        let ws_id: Option<String> = db.query_row(
            "SELECT workspace_id FROM objects WHERE id=?1 AND type='whiteboard'",
            params![board_id],
            |r| r.get(0),
        ).ok();

        ws_id.and_then(|ws| {
            db.query_row("SELECT group_id FROM workspaces WHERE id=?1", params![ws], |r| r.get(0)).ok()
        })
    }

    /// R10FB §L (live activity): announce that this board was just edited, so peers refresh
    /// that board's preview live and show a "recently active/edited" marker. Gossiped (when
    /// the board has a group) and surfaced locally as `SwiftEvent::Network(BoardChanged)`.
    /// `group_id` is the board's group, already resolved by the caller (avoid a re-lookup).
    fn note_board_activity(&self, board_id: &str, group_id: Option<&str>) {
        // R11 §9: carry the board's current name + a short content preview so a peer can
        // refresh that board's preview card live (it used to stay blank — the signal carried
        // no content). Receive-only on the peer (no storage write).
        let (name, preview) = storage::board_preview(board_id);
        let event = NetworkEvent::BoardChanged {
            board_id: board_id.to_string(),
            editor: self.node_id.clone(),
            ts: chrono::Utc::now().timestamp(),
            name,
            preview,
        };
        if let Some(gid) = group_id {
            let _ = self.network_tx.send(NetworkCommand::Broadcast {
                group_id: gid.to_string(),
                event: event.clone(),
            });
        }
        let _ = self.event_tx.send(SwiftEvent::Network(event));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

fn dump_tree_json(db: &Arc<Mutex<Connection>>) -> String {
    let db = db.lock_safe();

    let groups: Vec<Group> = (|| -> rusqlite::Result<Vec<Group>> {
        let mut stmt = db.prepare("SELECT id, name, icon, color, created_at FROM groups ORDER BY name")?;
        let rows = stmt.query_map([], |r| {
            Ok(Group {
                id: r.get(0)?,
                name: r.get(1)?,
                icon: r.get(2)?,
                color: r.get(3)?,
                created_at: r.get(4)?,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    })()
    .unwrap_or_default();

    let workspaces: Vec<Workspace> = (|| -> rusqlite::Result<Vec<Workspace>> {
        let mut stmt = db.prepare("SELECT id, group_id, name, created_at, is_system FROM workspaces ORDER BY name")?;
        let rows = stmt.query_map([], |r| {
            Ok(Workspace {
                id: r.get(0)?,
                group_id: r.get(1)?,
                name: r.get(2)?,
                created_at: r.get(3)?,
                system: r.get::<_, i32>(4)? != 0,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    })()
    .unwrap_or_default();

    let whiteboards: Vec<WhiteboardDTO> = (|| -> rusqlite::Result<Vec<WhiteboardDTO>> {
        let mut stmt = db.prepare("SELECT id, workspace_id, name, created_at FROM objects WHERE type='whiteboard' ORDER BY name")?;
        let rows = stmt.query_map([], |r| {
            Ok(WhiteboardDTO {
                id: r.get(0)?,
                workspace_id: r.get(1)?,
                name: r.get(2)?,
                created_at: r.get(3)?,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    })()
    .unwrap_or_default();

    let files: Vec<FileDTO> = (|| -> rusqlite::Result<Vec<FileDTO>> {
        let mut stmt = db.prepare("SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at FROM objects WHERE type='file' ORDER BY name")?;
        let rows = stmt.query_map([], |r| {
            Ok(FileDTO {
                id: r.get(0)?,
                group_id: r.get(1)?,
                workspace_id: r.get(2)?,
                board_id: r.get(3)?,
                name: r.get(4)?,
                hash: r.get(5)?,
                size: r.get::<_, i64>(6)? as u64,
                source_peer: r.get(7)?,
                local_path: r.get(8)?,
                created_at: r.get(9)?,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    })()
    .unwrap_or_default();

    let chats: Vec<ChatDTO> = (|| -> rusqlite::Result<Vec<ChatDTO>> {
        let mut stmt = db.prepare("SELECT id, board_id, workspace_id, name, hash, data, created_at FROM objects WHERE type='chat' ORDER BY created_at")?;
        let rows = stmt.query_map([], |r| {
            let parent_bytes: Option<Vec<u8>> = r.get(5)?;
            let parent_id = parent_bytes.and_then(|b| String::from_utf8(b).ok());
            Ok(ChatDTO {
                id: r.get(0)?,
                board_id: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                workspace_id: r.get(2)?,
                message: r.get(3)?,
                author: r.get(4)?,
                parent_id,
                timestamp: r.get(6)?,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    })()
    .unwrap_or_default();

    let integrations: Vec<IntegrationBindingDTO> = {
        match db.prepare("SELECT id, scope_type, scope_id, integration_type, config_json, created_at FROM integration_bindings ORDER BY created_at") {
            Ok(mut stmt) => {
                stmt.query_map([], |r| {
                    let config_str: String = r.get(4)?;
                    let config = serde_json::from_str(&config_str).unwrap_or(serde_json::Value::Null);
                    Ok(IntegrationBindingDTO {
                        id: r.get(0)?,
                        scope_type: r.get(1)?,
                        scope_id: r.get(2)?,
                        integration_type: r.get(3)?,
                        config,
                        created_at: r.get(5)?,
                    })
                }).map(|rows| rows.filter_map(|r| r.ok()).collect())
                    .unwrap_or_default()
            }
            Err(_) => vec![],
        }
    };

    let board_metadata: Vec<BoardMetadataDTO> = {
        match db.prepare("SELECT board_id, labels, rating, view_count, contains_model, contains_skills, board_type, last_accessed, COALESCE(is_pinned, 0), COALESCE(meta_updated_at, 0), COALESCE(pin_updated_at, 0) FROM board_metadata") {
            Ok(mut stmt) => {
                stmt.query_map([], |row| {
                    let labels_json: String = row.get(1)?;
                    let skills_json: String = row.get(5)?;
                    Ok(BoardMetadataDTO {
                        board_id: row.get(0)?,
                        labels: serde_json::from_str(&labels_json).unwrap_or_default(),
                        rating: row.get(2)?,
                        view_count: row.get(3)?,
                        contains_model: row.get(4)?,
                        contains_skills: serde_json::from_str(&skills_json).unwrap_or_default(),
                        board_type: row.get(6)?,
                        last_accessed: row.get(7)?,
                        is_pinned: row.get::<_, i32>(8)? != 0,
                        meta_updated_at: row.get(9)?,
                        pin_updated_at: row.get(10)?,
                    })
                }).map(|rows| rows.filter_map(|r| r.ok()).collect()).unwrap_or_default()
            }
            Err(_) => vec![],
        }
    };

    let snapshot = TreeSnapshotDTO {
        groups,
        workspaces,
        whiteboards,
        files,
        chats,
        whiteboard_elements: vec![],
        notebook_cells: vec![],
        integrations,
        board_metadata,
    };
    serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string())
}

// R10FB §D: the demo-seed helper has been REMOVED. A fresh/empty DB must never auto-create
// a "Demo Group"/"Demo Board" — the engine creates no data on its own; first-run is the
// app's empty state. The `SeedDemoIfEmpty` command + `cyan_seed_demo_if_empty` FFI are kept
// as inert no-ops (gated) so the C ABI stays stable until iOS stops calling them.

// ═══════════════════════════════════════════════════════════════════════════
// EVENT ROUTING - Routes SwiftEvent to appropriate component buffers
// ═══════════════════════════════════════════════════════════════════════════

/// Route SwiftEvent to appropriate component buffers based on event type.
/// Some events go to multiple buffers (e.g., BoardCreated → file_tree + board_grid).
fn route_event_to_buffers(
    event: &SwiftEvent,
    event_json: &str,
    file_tree: &Arc<Mutex<VecDeque<String>>>,
    chat_panel: &Arc<Mutex<VecDeque<String>>>,
    whiteboard: &Arc<Mutex<VecDeque<String>>>,
    board_grid: &Arc<Mutex<VecDeque<String>>>,
    network_status: &Arc<Mutex<VecDeque<String>>>,
) {
    match event {
        // ═══════════════════════════════════════════════════════════════════
        // FILE TREE EVENTS (structure + sync progress)
        // ═══════════════════════════════════════════════════════════════════
        SwiftEvent::TreeLoaded(_) |
        SwiftEvent::GroupDeleted { .. } |
        SwiftEvent::WorkspaceDeleted { .. } |
        SwiftEvent::BoardDeleted { .. } |
        SwiftEvent::GroupLeft { .. } |
        SwiftEvent::WorkspaceLeft { .. } |
        SwiftEvent::BoardLeft { .. } |
        SwiftEvent::FileDownloadProgress { .. } |
        SwiftEvent::FileDownloaded { .. } |
        SwiftEvent::FileDownloadFailed { .. } |
        // R12 B1: a distinct inbound-file notification, routed to the file + board surfaces
        // (the Files panel) so the receiving peer can raise a "file received" notification.
        SwiftEvent::FileReceived { .. } |
        SwiftEvent::ChatHistoryComplete { .. } => {
            file_tree.lock_safe().push_back(event_json.to_string());
            board_grid.lock_safe().push_back(event_json.to_string());
        }

        // Error events → FileTree (for display)
        SwiftEvent::Error { .. } => {
            file_tree.lock_safe().push_back(event_json.to_string());
        }

        // Sync events → FileTree + NetworkStatus (for StatusBar)
        SwiftEvent::SyncStarted { .. } |
        SwiftEvent::SyncStructureReceived { .. } |
        SwiftEvent::SyncBoardReady { .. } |
        SwiftEvent::SyncFilesReceived { .. } |
        SwiftEvent::SyncComplete { .. } => {
            file_tree.lock_safe().push_back(event_json.to_string());
            network_status.lock_safe().push_back(event_json.to_string());
        }

        // ═══════════════════════════════════════════════════════════════════
        // NETWORK EVENTS - Route based on inner event type
        // ═══════════════════════════════════════════════════════════════════
        SwiftEvent::Network(net_event) => {
            match net_event {
                // Structure changes → FileTree + BoardGrid
                NetworkEvent::GroupCreated(_) |
                NetworkEvent::GroupRenamed { .. } |
                NetworkEvent::GroupDeleted { .. } |
                NetworkEvent::GroupDissolved { .. } |
                NetworkEvent::WorkspaceCreated(_) |
                NetworkEvent::WorkspaceRenamed { .. } |
                NetworkEvent::WorkspaceDeleted { .. } |
                NetworkEvent::WorkspaceDissolved { .. } => {
                    file_tree.lock_safe().push_back(event_json.to_string());
                    board_grid.lock_safe().push_back(event_json.to_string());
                }

                // Board changes → FileTree + BoardGrid
                NetworkEvent::BoardCreated { .. } |
                NetworkEvent::BoardRenamed { .. } |
                NetworkEvent::BoardDeleted { .. } |
                NetworkEvent::BoardDissolved { .. } => {
                    file_tree.lock_safe().push_back(event_json.to_string());
                    board_grid.lock_safe().push_back(event_json.to_string());
                }

                // Board metadata/mode → BoardGrid
                NetworkEvent::BoardModeChanged { .. } |
                NetworkEvent::BoardMetadataUpdated { .. } |
                NetworkEvent::BoardLabelsUpdated { .. } |
                NetworkEvent::BoardRated { .. } => {
                    board_grid.lock_safe().push_back(event_json.to_string());
                }

                // Live activity (R10FB §L) + pin sync (R10FB §B3) → FileTree + BoardGrid.
                // BoardChanged refreshes the board's preview live and feeds the
                // "recently active/edited" marker; BoardPinned flips the pin on the card.
                NetworkEvent::BoardChanged { .. } |
                NetworkEvent::BoardPinned { .. } => {
                    file_tree.lock_safe().push_back(event_json.to_string());
                    board_grid.lock_safe().push_back(event_json.to_string());
                }

                // File changes → FileTree
                NetworkEvent::FileAvailable { .. } |
                NetworkEvent::FileDeleted { .. } => {
                    file_tree.lock_safe().push_back(event_json.to_string());
                }

                // Chat events → Chat panel
                NetworkEvent::ChatSent {  id,  workspace_id, .. } => {
                    eprintln!("📨 [ROUTE] ChatSent → chat_panel buffer");
                    eprintln!("   chat_id: {}...", &id[..16.min(id.len())]);
                    eprintln!("   workspace_id: {}...", &workspace_id[..16.min(workspace_id.len())]);
                    chat_panel.lock_safe().push_back(event_json.to_string());
                }
                NetworkEvent::ChatDeleted { .. } => {
                    chat_panel.lock_safe().push_back(event_json.to_string());
                }

                // Note events → Whiteboard buffer (notes are board-level content; the
                // app reads the authoritative list via cyan_note_list and treats these
                // as change signals). ROUND8 §W2.
                NetworkEvent::NoteAdded { .. } |
                NetworkEvent::NoteUpdated { .. } |
                NetworkEvent::NoteDeleted { .. } => {
                    whiteboard.lock_safe().push_back(event_json.to_string());
                }

                // Pin event → Whiteboard buffer (board-level pinned-workflow state; the
                // app reads the authoritative pin via storage and treats this as a
                // change signal). ROUND8 §W4.
                NetworkEvent::PinSet { .. } => {
                    whiteboard.lock_safe().push_back(event_json.to_string());
                }

                // Whiteboard element events → Whiteboard
                NetworkEvent::WhiteboardElementAdded { .. } |
                NetworkEvent::WhiteboardElementUpdated { .. } |
                NetworkEvent::WhiteboardElementDeleted { .. } |
                NetworkEvent::WhiteboardCleared { .. } => {
                    whiteboard.lock_safe().push_back(event_json.to_string());
                }

                // Notebook cell events → Whiteboard (notebook is a board type)
                NetworkEvent::NotebookCellAdded { .. } |
                NetworkEvent::NotebookCellUpdated { .. } |
                NetworkEvent::NotebookCellDeleted { .. } |
                NetworkEvent::NotebookCellsReordered { .. } => {
                    whiteboard.lock_safe().push_back(event_json.to_string());
                }

                // Profile updates → Chat (for author display name resolution)
                NetworkEvent::ProfileUpdated { .. } => {
                    chat_panel.lock_safe().push_back(event_json.to_string());
                }

                // Anonymous participation → Chat panel
                NetworkEvent::AnonymousJoined { .. } |
                NetworkEvent::IdentityRevealed { .. } => {
                    chat_panel.lock_safe().push_back(event_json.to_string());
                }

                // Snapshot available → NetworkStatus (triggers sync flow)
                NetworkEvent::GroupSnapshotAvailable { .. } => {
                    network_status.lock_safe().push_back(event_json.to_string());
                }

                // MCP plugin relays are mesh pass-through for the super-peer (Lens
                // replica) to enrich — a normal device has no local consumer and
                // surfaces nothing to the app (plugins are files, not events).
                NetworkEvent::PluginRelay { .. } => {}

                // Remote tool invocation is host-to-host mesh traffic consumed by
                // the remote-invoke handler (mesh_invoke::RemoteInvokeHandler), not
                // by the app: there is no UI panel for a Lens-orchestrated tool call
                // running locally. Pass-through here.
                NetworkEvent::RemoteToolCall { .. } | NetworkEvent::RemoteToolResult { .. } => {}

                // Ledger sync deltas (CYAN_FORMAT_SPEC §6.2) are engine-internal
                // replication: the receiver's storage is the surface, and the app
                // reads the ledger via cyan_changelist_command / cyan_review_command.
                // No UI buffer consumes them — pass-through, like PluginRelay.
                NetworkEvent::ChangeEntryAppended { .. }
                | NetworkEvent::ChangeEntryLifecycle { .. }
                | NetworkEvent::ChangeVersionCreated { .. }
                | NetworkEvent::ChangeBranchHead { .. } => {}
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // BOARD EVENTS (metadata only - deletes handled above)
        // ═══════════════════════════════════════════════════════════════════
        SwiftEvent::BoardMetadataUpdated { .. } => {
            board_grid.lock_safe().push_back(event_json.to_string());
        }

        // ═══════════════════════════════════════════════════════════════════
        // CHAT-SPECIFIC EVENTS
        // ═══════════════════════════════════════════════════════════════════
        SwiftEvent::ChatDeleted { .. } |
        SwiftEvent::ChatStreamReady { .. } |
        SwiftEvent::ChatStreamClosed { .. } => {
            chat_panel.lock_safe().push_back(event_json.to_string());
        }
        SwiftEvent::DirectMessageReceived {  id,  peer_id,  message, .. } => {
            eprintln!("📨 [ROUTE] DirectMessageReceived → chat_panel buffer");
            eprintln!("   dm_id: {}...", &id[..16.min(id.len())]);
            eprintln!("   peer_id: {}...", &peer_id[..16.min(peer_id.len())]);
            eprintln!("   message: {}...", &message[..50.min(message.len())]);
            chat_panel.lock_safe().push_back(event_json.to_string());
        }
        SwiftEvent::PeerJoined { .. } |
        SwiftEvent::PeerLeft { .. } => {
            chat_panel.lock_safe().push_back(event_json.to_string());
        }

        // ═══════════════════════════════════════════════════════════════════
        // STATUS EVENTS
        // ═══════════════════════════════════════════════════════════════════
        SwiftEvent::StatusUpdate { .. } |
        SwiftEvent::AIInsight { .. } |
        // Live presence/reachability for the honest status bar (additive, receive-only).
        SwiftEvent::PeerCountChanged { .. } |
        SwiftEvent::MeshReachability { .. } |
        // Workflow dashboard events (DASHBOARD_CONTRACT §A) — receive-only, ride the
        // existing event poll on the "status"/"network" component, like any live update.
        SwiftEvent::WorkflowRunStarted { .. } |
        SwiftEvent::StepStateChanged { .. } |
        SwiftEvent::StepProgress { .. } |
        SwiftEvent::ApprovalRequested { .. } |
        SwiftEvent::ApprovalResolved { .. } |
        SwiftEvent::WorkflowRunFinished { .. } |
        SwiftEvent::WorkflowStatsUpdated { .. } => {
            network_status.lock_safe().push_back(event_json.to_string());
        }

        // Unread counts (R10FB §N) → FileTree + BoardGrid (dots at group/workspace/board)
        // and NetworkStatus (the dock total badge). Receive-only; the app re-reads the map.
        SwiftEvent::UnreadChanged { .. } => {
            file_tree.lock_safe().push_back(event_json.to_string());
            board_grid.lock_safe().push_back(event_json.to_string());
            network_status.lock_safe().push_back(event_json.to_string());
        }
    }
}



#[cfg(test)]
pub mod tests {
    #[test]
    pub fn test_relay_url() {
        use std::str::FromStr;
        use iroh::RelayUrl;
        let url = "https://quic.dev.cyan.blockxaero.io";
        match RelayUrl::from_str(url) {
            Ok(parsed) => println!("parsed: {:?}", parsed),
            Err(e) => eprintln!("Err: {:?}", e)
        }
    }
}

pub mod pipeline;
pub mod seed;
pub mod workflow;
pub mod workflow_bind;
pub mod templates;
pub mod timecode_notes;
pub mod changelist;
pub mod conform_map;
pub mod conform_dispatch;
pub mod review_state;
pub mod review_loop;
pub mod note_inference;
pub mod ops_proposer;
pub mod constitution;
pub mod xfer_policy;
pub mod asset_registry;
pub mod ingest;
pub mod ingest_connectors;
pub mod skills;
pub mod pipeline_executor;
pub mod media_staging;
pub mod plugin_config;
pub mod dashboard;
pub mod exec_plan;
