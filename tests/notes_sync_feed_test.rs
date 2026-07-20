//! A2 §4a — the tenant-keyed sync feed (T27): `note_list_for_sync` replaces the
//! anchor-set sweep at all three replication sites (digest, watermark,
//! serializer), so `project` + `role` rows converge like board rows while the
//! sovereign `user` scope appears in NONE of them.

use std::{
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{anti_entropy, models::dto::NoteDTO, snapshot, storage};

const GROUP: &str = "sync-feed-group";
const WORKSPACE: &str = "sync-feed-ws";
const BOARD: &str = "sync-feed-board";
const NODE: &str = "node-sync-feed";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_sync_feed.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        storage::group_insert_simple(GROUP, "Sync Feed", "folder", "#00AEEF").expect("group");
        storage::workspace_insert_simple(WORKSPACE, GROUP, "General").expect("workspace");
        storage::board_insert_simple(BOARD, WORKSPACE, "Cut 1", 1).expect("board");
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
        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, element_type TEXT NOT NULL,
            x REAL, y REAL, width REAL, height REAL, z_index INTEGER DEFAULT 0,
            style_json TEXT, content_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
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

fn note(id: &str, board_id: &str, scope: &str, anchor: Option<(&str, &str)>, at: i64) -> NoteDTO {
    NoteDTO {
        id: id.to_string(),
        board_id: board_id.to_string(),
        tenant_id: GROUP.to_string(),
        author_id: NODE.to_string(),
        author_name: "Sync".to_string(),
        text: format!("note {id}"),
        created_at: at,
        updated_at: at,
        scope: scope.to_string(),
        kind: "constitution".to_string(),
        anchor_kind: anchor.map(|(k, _)| k.to_string()),
        anchor_id: anchor.map(|(_, a)| a.to_string()),
        origin_ref: None,
        payload: None,
        author_role: None,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T27 — a project note + a role note ride the sync feed, the digest, and the
// snapshot serializer; applying the frames back converges both; a user note
// appears in NONE.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn project_and_role_notes_in_sync_feed() {
    ensure_db();

    let (base_count, base_hash) = anti_entropy::group_digest(GROUP);

    // project (workspace anchor), role (group anchor + slug pair), user (sovereign).
    storage::note_upsert(&note("t27-project", WORKSPACE, "project", None, 100)).expect("project");
    storage::note_upsert(&note("t27-role", GROUP, "role", Some(("role", "colorist")), 101))
        .expect("role");
    storage::note_upsert(&note("t27-user", NODE, "user", None, 102)).expect("user");

    // 1 — the tenant-keyed feed.
    let feed = storage::note_list_for_sync(GROUP).expect("feed");
    let ids: Vec<&str> = feed.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"t27-project"), "project row in the feed: {ids:?}");
    assert!(ids.contains(&"t27-role"), "role row in the feed: {ids:?}");
    assert!(!ids.contains(&"t27-user"), "user row NEVER in the feed: {ids:?}");

    // 2 — the digest entry list: exactly the two non-sovereign rows joined it.
    let (count, hash) = anti_entropy::group_digest(GROUP);
    assert_eq!(count, base_count + 2, "project + role rows join the digest; user does not");
    assert_ne!(hash, base_hash, "the digest hash flipped");

    // 3 — the snapshot serializer output.
    let frames = snapshot::build_snapshot_frames(GROUP, None).expect("frames");
    let serialized: Vec<String> = frames
        .iter()
        .filter_map(|f| match f {
            cyan_backend::models::protocol::SnapshotFrame::Metadata { notes, .. } => {
                Some(notes.iter().map(|n| n.id.clone()).collect::<Vec<_>>())
            }
            _ => None,
        })
        .flatten()
        .collect();
    assert!(serialized.contains(&"t27-project".to_string()));
    assert!(serialized.contains(&"t27-role".to_string()));
    assert!(!serialized.contains(&"t27-user".to_string()), "sovereign row never serialized");

    // 4 — the watermark sees them (strictly newer than the base state).
    assert!(snapshot::group_high_water(GROUP) >= 101, "watermark covers the new rows");

    // 5 — convergence: wipe the two rows, re-apply the captured frames — the
    // idempotent LWW apply restores both (a second store converging).
    storage::note_delete("t27-project").expect("del");
    storage::note_delete("t27-role").expect("del");
    assert!(storage::note_get("t27-project").expect("get").is_none());
    snapshot::apply_snapshot_frames(&frames).expect("apply");
    assert!(storage::note_get("t27-project").expect("get").is_some(), "project row converged");
    assert!(storage::note_get("t27-role").expect("get").is_some(), "role row converged");
}
