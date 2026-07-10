//! TIER 3.5 / Phase-0 DoD (FABLE_OVERNIGHT_PROMPT) — a new group auto-creates the
//! default board ("Board 1") in its landing workspace, against the engine's REAL
//! schema (FKs ENFORCED — the bundled SQLite defaults foreign_keys ON).
//!
//! d543ab9 shipped this behavior with no test; the seeded XCUITest world could not
//! prove it either (a seed is exactly what hid Tier-0). These tests drive the same
//! `storage::provision_default_board` path the CreateGroup handler executes, so a
//! provisioning failure (FK, missing column, ordering) fails HERE, not silently in
//! a live app with an empty group.

use std::sync::Once;

use cyan_backend::models::core::Group;
use cyan_backend::storage;

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB carrying the engine's REAL
/// schema (FKs included) + migrations — the same two steps the FFI init path runs.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("default-board.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            cyan_backend::ensure_schema(&conn).expect("engine schema");
        }
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
    });
}

/// Create a group row + the ROUND8 §W3 workspaces, exactly as CreateGroup does.
fn create_group(id: &str) -> String {
    let g = Group {
        id: id.to_string(),
        name: format!("group {id}"),
        icon: "folder".to_string(),
        color: "#00FFFF".to_string(),
        created_at: chrono::Utc::now().timestamp(),
    };
    storage::group_insert(&g).expect("group insert");
    let (default_ws, _plugins) =
        storage::provision_group_workspaces(id, Some("node-under-test")).expect("workspaces");
    default_ws.id
}

fn board_rows(workspace_id: &str) -> Vec<(String, String)> {
    let conn = storage::db().lock().expect("db lock");
    let mut stmt = conn
        .prepare("SELECT id, name FROM objects WHERE type='whiteboard' AND workspace_id=?1")
        .expect("prepare");
    stmt.query_map([workspace_id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows")
}

#[test]
fn new_group_gets_default_board_in_landing_workspace() {
    ensure_db();
    let ws = create_group("g-default-board");

    let (board_id, board_name) =
        storage::provision_default_board(&ws, "node-under-test", chrono::Utc::now().timestamp())
            .expect("default board seed must succeed against the real schema");

    assert_eq!(board_name, "Board 1");
    assert_eq!(board_id, storage::default_board_id(&ws), "deterministic id");

    let rows = board_rows(&ws);
    assert_eq!(rows.len(), 1, "exactly one default board, got {rows:?}");
    assert_eq!(rows[0], (board_id, "Board 1".to_string()));
}

#[test]
fn default_board_seed_is_idempotent_on_redelivery() {
    ensure_db();
    let ws = create_group("g-default-board-idem");
    let now = chrono::Utc::now().timestamp();

    let first = storage::provision_default_board(&ws, "node-under-test", now).expect("first");
    let second =
        storage::provision_default_board(&ws, "node-under-test", now + 5).expect("second");

    assert_eq!(first.0, second.0, "same deterministic board id");
    assert_eq!(board_rows(&ws).len(), 1, "INSERT OR IGNORE ⇒ still exactly one board");
}

#[test]
fn default_board_seed_fails_loudly_for_missing_workspace() {
    ensure_db();
    // FK objects.workspace_id → workspaces(id) is ENFORCED, and OR IGNORE does NOT
    // apply to FK violations — a bad workspace id must surface as Err (the handler
    // logs it), never a silent no-board.
    let err = storage::provision_default_board(
        "no-such-workspace",
        "node-under-test",
        chrono::Utc::now().timestamp(),
    );
    assert!(err.is_err(), "seeding into a nonexistent workspace must error, got {err:?}");
}
