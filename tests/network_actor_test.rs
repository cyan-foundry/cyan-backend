// tests/network_actor_test.rs
//
// TRUE END-TO-END integration test for Cyan network layer
// Uses ACTUAL NetworkActor, TopicActor, DiscoveryActor from cyan-backend
//
// This test verifies the complete flow:
//   Discovery topic → groups_exchange → peer_introduction →
//   group topic → RequestSnapshot → SnapshotAvailable → direct QUIC transfer
//
// Build:  cargo build --release --bin network_test
// Run:
//   Machine A: ./target/release/network_test host
//   Machine B: ./target/release/network_test join <NODE_ID>
//   Local:     ./target/release/network_test local (both in same process)

use anyhow::Result;
use iroh::{PublicKey, SecretKey};
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rusqlite::Connection;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;

// ═══════════════════════════════════════════════════════════════════════════
// IMPORTS FROM CYAN-BACKEND
// ═══════════════════════════════════════════════════════════════════════════

use cyan_backend::{
    actors::NetworkActor,
    bootstrap_node_id,
    models::{
        commands::NetworkCommand,
        events::SwiftEvent,
        node_config::{DiscoveryPolicy, NodeConfig, RelayPolicy},
    },
    storage, DISCOVERY_KEY, RELAY_URL,
};

// ═══════════════════════════════════════════════════════════════════════════
// PROTOCOL FLOW DIAGRAM
// ═══════════════════════════════════════════════════════════════════════════
//
// This test validates the FULL discovery → sync protocol:
//
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │ STEP 0: INVITE (simulated)                                          │
//   │   - Joiner has group_id in DB (from QR/link)                        │
//   │   - No content yet, just the group record                           │
//   ├──────────────────────────────────────────────────────────────────────┤
//   │ STEP 1: DISCOVERY TOPIC                                             │
//   │   - Both → cyan/discovery/{key} via bootstrap                       │
//   ├──────────────────────────────────────────────────────────────────────┤
//   │ STEP 2: GROUPS EXCHANGE                                             │
//   │   - Host broadcasts: { groups: [TEST_GROUP] }                       │
//   │   - Joiner broadcasts: { groups: [TEST_GROUP] }                     │
//   │   - DiscoveryActor finds shared_groups                              │
//   ├──────────────────────────────────────────────────────────────────────┤
//   │ STEP 3: PEER INTRODUCTION                                           │
//   │   - Host → PeerIntroduction { group, peers: [host] }                │
//   │   - Joiner's DiscoveryActor → JoinPeersToTopic                      │
//   ├──────────────────────────────────────────────────────────────────────┤
//   │ STEP 4: GROUP TOPIC                                                 │
//   │   - TopicActor → cyan/group/{id}                                    │
//   │   - Broadcasts RequestSnapshot                                      │
//   ├──────────────────────────────────────────────────────────────────────┤
//   │ STEP 5: SNAPSHOT TRANSFER                                           │
//   │   - Host responds GroupSnapshotAvailable                            │
//   │   - Direct QUIC: Structure → Content → Metadata → Complete          │
//   │   - SwiftEvent::SyncComplete emitted                                │
//   └──────────────────────────────────────────────────────────────────────┘
//

// ═══════════════════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

const DISCOVERY_KEY_CONST: &str = "cyan-dev";
const RELAY_URL_CONST: &str = "https://quic.dev.cyan.blockxaero.io";

// For local test, we use a fixed "bootstrap" that's actually just peer A
// In production, this would be the real bootstrap server
const TEST_GROUP_ID: &str = "test-group-e2e-1111-2222-3333-444444444444";
const TEST_WORKSPACE_ID: &str = "test-ws-e2e-1111-2222-3333-444444444444";
const TEST_BOARD_ID: &str = "test-board-e2e-1111-2222-3333-444444444444";

/// Initialize test schema - creates base tables that migrations assume exist
fn init_test_schema(db_path: &str) -> Result<()> {
    let conn = Connection::open(db_path)?;

    conn.execute_batch(r#"
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            icon TEXT,
            color TEXT,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );

        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY,
            workspace_id TEXT,
            group_id TEXT,
            board_id TEXT,
            type TEXT NOT NULL,
            name TEXT NOT NULL,
            hash TEXT,
            data TEXT,
            size INTEGER,
            source_peer TEXT,
            local_path TEXT,
            created_at INTEGER NOT NULL,
            board_mode TEXT DEFAULT 'canvas'
        );

        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY,
            board_id TEXT NOT NULL,
            element_type TEXT NOT NULL,
            x REAL NOT NULL,
            y REAL NOT NULL,
            width REAL NOT NULL,
            height REAL NOT NULL,
            z_index INTEGER NOT NULL,
            style_json TEXT,
            content_json TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            cell_id TEXT,
            FOREIGN KEY (board_id) REFERENCES objects(id)
        );

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
            FOREIGN KEY (board_id) REFERENCES objects(id)
        );

    "#)?;

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("iroh_gossip=warn".parse()?)
                .add_directive("iroh=info".parse()?)
                .add_directive("cyan_backend=debug".parse()?),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("host") => run_host().await,
        Some("join") => run_join().await,
        _ => {
            println!("╔═══════════════════════════════════════════════════════════════╗");
            println!("║          Cyan Network Actor E2E Test                          ║");
            println!("╠═══════════════════════════════════════════════════════════════╣");
            println!("║                                                               ║");
            println!("║  Two-machine test (no NODE_ID needed!):                       ║");
            println!("║    Machine A: ./network_test host                             ║");
            println!("║    Machine B: ./network_test join                             ║");
            println!("║                                                               ║");
            println!("║  Both peers discover each other via bootstrap node.           ║");
            println!("║                                                               ║");
            println!("╚═══════════════════════════════════════════════════════════════╝");
            Ok(())
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// HOST - Runs NetworkActor with test data, serves snapshots via actors
// ═══════════════════════════════════════════════════════════════════════════

async fn run_host() -> Result<()> {
    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║              NETWORK ACTOR HOST                               ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!("║  Testing: Discovery → GroupsExchange → PeerIntro → Snapshot   ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    // Initialize storage
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("host.db");
    init_test_schema(db_path.to_str().unwrap())?;  // Create base tables
    storage::init_db(db_path.to_str().unwrap())?;
    println!("📂 DB initialized at {:?}", db_path);

    // Create test data BEFORE starting actor
    // This ensures NetworkActor finds the group and spawns TopicActor
    create_test_data()?;
    println!("📝 Test data created (group + workspace + board + content)");
    println!("   Group: {}...", &TEST_GROUP_ID[..16]);

    // Generate identity
    let mut rng = ChaCha8Rng::from_os_rng();
    let secret_key = SecretKey::generate(&mut rng);
    let node_id = secret_key.public().to_string();

    // Configure globals (must happen before NetworkActor::new)
    let _ = RELAY_URL.set(RELAY_URL_CONST.to_string());
    let _ = DISCOVERY_KEY.set(DISCOVERY_KEY_CONST.to_string());
    // NOTE: NOT setting BOOTSTRAP_NODE_ID - uses default from lib.rs

    println!("📱 My node ID: {}...", &node_id[..16]);

    // Create channels
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<NetworkCommand>();
    let peers_per_group = Arc::new(std::sync::Mutex::new(HashMap::<String, HashSet<PublicKey>>::new()));

    // Create and start NetworkActor
    println!("🚀 Starting NetworkActor...");
    let node_cfg = NodeConfig {
        relay: RelayPolicy::Url(RELAY_URL_CONST.to_string()),
        discovery: DiscoveryPolicy::Bootstrap(bootstrap_node_id().to_string()),
        discovery_key: DISCOVERY_KEY_CONST.to_string(),
    };
    let actor = NetworkActor::new(secret_key, event_tx, peers_per_group, node_cfg).await?;

    // Spawn actor in background
    tokio::spawn(async move {
        actor.start(cmd_rx).await;
    });

    println!("✅ NetworkActor running");
    println!("\n📡 Listening for peers...");
    println!("   (Press Ctrl+C to stop)\n");

    // Event monitoring loop
    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                match &event {
                    SwiftEvent::PeerJoined { group_id, peer_id } => {
                        println!("🟢 [EVENT] PeerJoined: {}... in group {}...",
                            &peer_id[..16.min(peer_id.len())],
                            &group_id[..16.min(group_id.len())]);
                    }
                    SwiftEvent::PeerLeft { group_id, peer_id } => {
                        println!("🔴 [EVENT] PeerLeft: {}... from group {}...",
                            &peer_id[..16.min(peer_id.len())],
                            &group_id[..16.min(group_id.len())]);
                    }
                    SwiftEvent::StatusUpdate { message } => {
                        println!("📋 [EVENT] Status: {}", message);
                    }
                    SwiftEvent::SyncStarted { group_id, .. } => {
                        println!("🔄 [EVENT] SyncStarted for {}...",
                            &group_id[..16.min(group_id.len())]);
                    }
                    _ => {
                        println!("📨 [EVENT] {:?}", event);
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n👋 Shutting down...");
                break;
            }
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// JOIN - Runs NetworkActor, sends JoinGroup, waits for sync
// ═══════════════════════════════════════════════════════════════════════════

async fn run_join() -> Result<()> {
    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║              NETWORK ACTOR JOINER                             ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!("║  Discovers host via production bootstrap - no NODE_ID needed! ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    // Initialize storage
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("joiner.db");
    init_test_schema(db_path.to_str().unwrap())?;  // Create base tables
    storage::init_db(db_path.to_str().unwrap())?;

    // ═══════════════════════════════════════════════════════════════════════════
    // STEP 0: SIMULATE GROUP INVITE
    // ═══════════════════════════════════════════════════════════════════════════
    // In production, this happens via QR code / invite link parsed by Swift
    // The invite contains group_id (and optionally group name, color, etc.)
    // We create an EMPTY group record - no workspaces, boards, or content yet

    println!("📨 Simulating GROUP INVITE...");
    println!("   Creating empty group in DB (as if from QR/invite link)");
    storage::group_insert_simple(TEST_GROUP_ID, "Invited Group", "folder.fill", "#FF6B6B")?;
    println!("   ✓ Group {} created (empty)", &TEST_GROUP_ID[..16]);
    println!();

    // Generate identity
    let mut rng = ChaCha8Rng::from_os_rng();
    let secret_key = SecretKey::generate(&mut rng);
    let node_id = secret_key.public().to_string();

    println!("📱 My node ID: {}...", &node_id[..16]);

    // Configure globals - use BAKED-IN bootstrap (not passed in)
    let _ = RELAY_URL.set(RELAY_URL_CONST.to_string());
    let _ = DISCOVERY_KEY.set(DISCOVERY_KEY_CONST.to_string());
    // NOTE: NOT setting BOOTSTRAP_NODE_ID - uses default from lib.rs

    // Create channels
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<NetworkCommand>();
    let peers_per_group = Arc::new(std::sync::Mutex::new(HashMap::<String, HashSet<PublicKey>>::new()));

    // Create and start NetworkActor
    // This will:
    //   1. Spawn DiscoveryActor (connects to discovery topic via bootstrap)
    //   2. Load TEST_GROUP_ID from DB → spawn TopicActor
    //   3. DiscoveryActor broadcasts GroupsExchange with our groups
    println!("🚀 Starting NetworkActor...");
    let node_cfg = NodeConfig {
        relay: RelayPolicy::Url(RELAY_URL_CONST.to_string()),
        discovery: DiscoveryPolicy::Bootstrap(bootstrap_node_id().to_string()),
        discovery_key: DISCOVERY_KEY_CONST.to_string(),
    };
    let actor = NetworkActor::new(secret_key, event_tx, peers_per_group, node_cfg).await?;

    tokio::spawn(async move {
        actor.start(cmd_rx).await;
    });

    // Wait for actor to initialize and discovery to happen
    println!("⏳ Waiting for network initialization + discovery...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ═══════════════════════════════════════════════════════════════════════════
    // STEP 1: TRIGGER SNAPSHOT SYNC
    // ═══════════════════════════════════════════════════════════════════════════
    // In production, this might be called after invite acceptance, or
    // TopicActor could auto-request when it has no content.
    // For now, we explicitly trigger via JoinGroup (without bootstrap_peer,
    // since discovery should have already found the host)

    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("📤 Triggering snapshot sync for group");
    println!("   group_id: {}...", &TEST_GROUP_ID[..16]);
    println!("   (discovery should have found host peer)");
    println!("═══════════════════════════════════════════════════════════════════\n");

    // Use bootstrap_peer=None to test that discovery found the peer
    // If discovery worked, TopicActor already has the host in known_peers
    // If not, this will still work but we should see it in logs
    cmd_tx.send(NetworkCommand::JoinGroup {
        group_id: TEST_GROUP_ID.to_string(),
        bootstrap_peer: None,  // Rely on discovery!
        grant: None,
    })?;

    // Wait for SyncComplete event
    println!("⏳ Waiting for sync to complete...\n");

    let timeout = Duration::from_secs(60);
    let start = std::time::Instant::now();
    let mut sync_complete = false;

    loop {
        if start.elapsed() > timeout {
            println!("❌ Timeout waiting for sync!");
            break;
        }

        tokio::select! {
            Some(event) = event_rx.recv() => {
                match &event {
                    SwiftEvent::SyncStarted { group_id, group_name } => {
                        println!("🔄 [EVENT] SyncStarted: {} ({})", group_name, &group_id[..16.min(group_id.len())]);
                    }
                    SwiftEvent::SyncStructureReceived { group_id, workspace_count, board_count } => {
                        println!("📦 [EVENT] Structure received: {} workspaces, {} boards",
                            workspace_count, board_count);
                    }
                    SwiftEvent::SyncBoardReady { board_id, element_count, cell_count } => {
                        println!("📋 [EVENT] Board ready: {} elements, {} cells",
                            element_count, cell_count);
                    }
                    SwiftEvent::SyncFilesReceived { group_id, file_count } => {
                        println!("📁 [EVENT] Files received: {}", file_count);
                    }
                    SwiftEvent::SyncComplete { group_id } => {
                        println!("\n✅ [EVENT] SyncComplete for {}...", &group_id[..16.min(group_id.len())]);
                        sync_complete = true;
                        break;
                    }
                    SwiftEvent::StatusUpdate { message } => {
                        println!("📋 [EVENT] Status: {}", message);
                    }
                    SwiftEvent::PeerJoined { peer_id, group_id } => {
                        println!("🟢 [EVENT] PeerJoined: {}...", &peer_id[..16.min(peer_id.len())]);
                    }
                    _ => {
                        println!("📨 [EVENT] {:?}", event);
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Just keep polling
            }
        }
    }

    let elapsed = start.elapsed();

    if sync_complete {
        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║ ✅ ACTOR-BASED SYNC COMPLETE                                  ║");
        println!("╠═══════════════════════════════════════════════════════════════╣");
        println!("║ Total time: {:>10?}                                      ║", elapsed);
        println!("╚═══════════════════════════════════════════════════════════════╝\n");

        // Verify synced data
        verify_synced_data()?;
    } else {
        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║ ❌ SYNC FAILED OR TIMED OUT                                   ║");
        println!("╚═══════════════════════════════════════════════════════════════╝\n");
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST DATA HELPERS
// ═══════════════════════════════════════════════════════════════════════════

fn create_test_data() -> Result<()> {
    // Group
    storage::group_insert_simple(TEST_GROUP_ID, "E2E Test Group", "folder.fill", "#00AEEF")?;

    // Workspace
    storage::workspace_insert_simple(TEST_WORKSPACE_ID, TEST_GROUP_ID, "Main Workspace")?;

    // Board
    storage::board_insert_simple(
        TEST_BOARD_ID,
        TEST_WORKSPACE_ID,
        "Test Canvas",
        chrono::Utc::now().timestamp(),
    )?;

    // Elements
    for i in 0..5 {
        let hex = format!("{:02X}", i * 50);
        let style = format!("{{\"fill\":\"#FF{}00\"}}", hex);
        let content = format!("{{\"text\":\"Element {}\"}}", i);
        storage::element_insert_simple(
            &format!("elem-{:03}", i),
            TEST_BOARD_ID,
            "rectangle",
            (i * 100) as f64,
            (i * 50) as f64,
            200.0,
            100.0,
            i,
            Some(&style),
            Some(&content),
            chrono::Utc::now().timestamp(),
            chrono::Utc::now().timestamp(),
        )?;
    }

    // Cells
    for i in 0..3 {
        storage::cell_insert_simple(
            &format!("cell-{:03}", i),
            TEST_BOARD_ID,
            "code",
            i,
            Some(&format!("# Cell {}\nprint('hello')", i)),
            Some("hello"),
            false,
            Some(100.0),
            None,
            chrono::Utc::now().timestamp(),
            chrono::Utc::now().timestamp(),
        )?;
    }

    // Chats
    for i in 0..3 {
        storage::chat_insert_simple(
            &format!("chat-{:03}", i),
            TEST_WORKSPACE_ID,
            &format!("Test message {}", i),
            "test-author",
            None,
            chrono::Utc::now().timestamp(),
        )?;
    }

    // File metadata
    storage::file_insert_simple(
        "file-001",
        Some(TEST_GROUP_ID),
        Some(TEST_WORKSPACE_ID),
        Some(TEST_BOARD_ID),
        "test-document.pdf",
        "abc123def456",
        1024000,
        None,
        chrono::Utc::now().timestamp(),
    )?;

    Ok(())
}

fn verify_synced_data() -> Result<()> {
    println!("🔍 Verifying synced data...");

    // Check group
    let group = storage::group_get(TEST_GROUP_ID)?
        .ok_or_else(|| anyhow::anyhow!("Group not found after sync"))?;
    println!("  ✅ Group: {}", group.name);

    // Check workspaces
    let workspaces = storage::workspace_list_by_group(TEST_GROUP_ID)?;
    println!("  ✅ Workspaces: {}", workspaces.len());

    // Check boards
    let workspace_ids: Vec<String> = workspaces.iter().map(|w| w.id.clone()).collect();
    let boards = storage::board_list_by_workspaces(&workspace_ids)?;
    println!("  ✅ Boards: {}", boards.len());

    // Check elements
    let board_ids: Vec<String> = boards.iter().map(|b| b.id.clone()).collect();
    let elements = storage::element_list_by_boards(&board_ids)?;
    println!("  ✅ Elements: {}", elements.len());

    // Check cells
    let cells = storage::cell_list_by_boards(&board_ids)?;
    println!("  ✅ Cells: {}", cells.len());

    // Check chats
    let chats = storage::chat_list_by_workspaces(&workspace_ids)?;
    println!("  ✅ Chats: {}", chats.len());

    // Check files
    let files = storage::file_list_by_group(TEST_GROUP_ID)?;
    println!("  ✅ Files: {}", files.len());

    // Validate counts
    if elements.len() != 5 {
        return Err(anyhow::anyhow!("Expected 5 elements, got {}", elements.len()));
    }
    if cells.len() != 3 {
        return Err(anyhow::anyhow!("Expected 3 cells, got {}", cells.len()));
    }
    if chats.len() != 3 {
        return Err(anyhow::anyhow!("Expected 3 chats, got {}", chats.len()));
    }

    println!("\n✅ All data verified successfully!");
    Ok(())
}