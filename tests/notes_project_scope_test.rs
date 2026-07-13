//! A2 §4a — the NEW `project` scope (T26, T28): broadcast-group mapping v2
//! resolves the workspace anchor to its owning group; an orphan workspace
//! REJECTS locally (a stranded row would never replicate) but TOLERATES inbound
//! (TR-1 — convergence over validation).

use std::{
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{
    dispatch_put_note_v3,
    models::{commands::NetworkCommand, events::SwiftEvent},
    snapshot, storage,
};
use serde_json::json;
use tokio::sync::mpsc;

const NODE: &str = "node-project-test";
const GROUP: &str = "proj-group";
const WORKSPACE: &str = "proj-workspace";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_project_scope.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        // The real workspace → group edge the mapping resolves.
        storage::group_insert_simple(GROUP, "Proj Group", "folder", "#00AEEF").expect("group");
        storage::workspace_insert_simple(WORKSPACE, GROUP, "Promo Spring").expect("workspace");
        let _ = DB_PATH.set(path);
        std::mem::forget(dir);
    });
}

fn init_base_schema(db_path: &Path) -> Result<(), rusqlite::Error> {
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
        "#,
    )?;
    Ok(())
}

struct Outcome {
    rejection: Option<String>,
    broadcasts: Vec<NetworkCommand>,
}

fn put_project(anchor: &str, id: &str) -> Outcome {
    let (net_tx, mut net_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    dispatch_put_note_v3(
        NODE,
        &|_b: &str| None, // board_group is only consulted for board scope
        &net_tx,
        &evt_tx,
        anchor.to_string(),
        Some(id.to_string()),
        None,
        "creative brief note".to_string(),
        Some("project".to_string()),
        Some("creative-brief".to_string()),
        None,
        None,
        None,
        None,
        None,
        &|| None,
    );
    let mut rejection = None;
    while let Ok(e) = evt_rx.try_recv() {
        if let SwiftEvent::NoteRejected { reason, .. } = e {
            rejection = Some(reason);
        }
    }
    let mut broadcasts = Vec::new();
    while let Ok(c) = net_rx.try_recv() {
        broadcasts.push(c);
    }
    Outcome { rejection, broadcasts }
}

// ════════════════════════════════════════════════════════════════════════════
// T26 — an orphan project anchor rejects LOCALLY (no row, no event) but the
// same row applied INBOUND is stored (TR-1).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn orphan_project_anchor_rejected_locally_tolerated_inbound() {
    ensure_db();

    let out = put_project("no-such-workspace", "t26-local");
    assert_eq!(out.rejection.as_deref(), Some("project_anchor_unknown"));
    assert!(out.broadcasts.is_empty(), "a rejected project write never gossips");
    assert!(storage::note_get("t26-local").expect("get").is_none());

    // The SAME row inbound (snapshot Metadata frame — the public inbound door)
    // is stored verbatim: convergence over validation.
    let frame: cyan_backend::models::protocol::SnapshotFrame = serde_json::from_value(json!({
        "frame_type": "Metadata",
        "chats": [], "files": [], "integrations": [], "board_metadata": [],
        "notes": [{
            "id": "t26-inbound",
            "board_id": "no-such-workspace",
            "tenant_id": GROUP,
            "author_id": "peer-remote",
            "author_name": "Remote",
            "text": "creative brief note",
            "created_at": 10,
            "updated_at": 10,
            "scope": "project",
            "kind": "creative-brief",
        }]
    }))
    .expect("frame decodes");
    snapshot::apply_snapshot_frame(&frame).expect("inbound apply");
    assert!(
        storage::note_get("t26-inbound").expect("get").is_some(),
        "inbound tolerates the orphan anchor (TR-1)"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T28 — a project write broadcasts to the WORKSPACE'S OWNING GROUP (mapping v2)
// and tenant-stamps with it.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn project_note_broadcast_targets_workspace_group() {
    ensure_db();

    let out = put_project(WORKSPACE, "t28-n1");
    assert!(out.rejection.is_none(), "known workspace passes: {:?}", out.rejection);

    let expected = storage::workspace_get_group_id(WORKSPACE).expect("workspace has a group");
    assert_eq!(expected, GROUP);
    let broadcast_group = out.broadcasts.iter().find_map(|c| match c {
        NetworkCommand::Broadcast { group_id, .. } => Some(group_id.clone()),
        _ => None,
    });
    assert_eq!(
        broadcast_group.as_deref(),
        Some(GROUP),
        "the NoteAdded broadcast targets workspace_get_group_id(anchor)"
    );

    let row = storage::note_get("t28-n1").expect("get").expect("row");
    assert_eq!(row.tenant_id, GROUP, "project rows tenant-stamp with the owning group");
    assert_eq!(row.board_id, WORKSPACE, "board_id stays the scope anchor (the workspace)");
}
