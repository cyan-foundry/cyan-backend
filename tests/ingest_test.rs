//! STAGE 4 — ingest sources + per-asset pipeline materialization.
//!
//! Two rails, matching the module's design seam:
//!   * conn-level tests on isolated DBs (`ensure_schema` + the ingest/asset
//!     migrations on an in-memory or tempdir SQLite) for sources, scans, dedup
//!     and run materialization;
//!   * global-DB tests (the plugin_install_linkage pattern) for the pieces that
//!     ride process-global storage — the explicit-asset bind
//!     (`workflow_bind::bind_step_for_asset`) and the `cyan_ingest_command`
//!     JSON dispatch.

use std::sync::Once;

use cyan_backend::{ingest, storage, workflow_bind};
use cyan_backend::models::core::Group;
use rusqlite::Connection;

// ── conn-level rail ────────────────────────────────────────────────────────────

/// An isolated DB carrying the engine's REAL schema plus the STAGE-4 tables.
fn conn_db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    cyan_backend::ensure_schema(&conn).expect("engine schema");
    cyan_backend::changelist::migrate(&conn).expect("changelist migrate");
    cyan_backend::asset_registry::migrate(&conn).expect("asset migrate");
    ingest::migrate(&conn).expect("ingest migrate");
    conn
}

/// group → workspace → whiteboard rows on the isolated conn (FKs are enforced).
fn mk_board(conn: &Connection, group: &str, board: &str) {
    conn.execute(
        "INSERT INTO groups (id, name, icon, color, created_at) VALUES (?1, ?1, 'folder', '#00FFFF', 1)",
        [group],
    )
    .expect("group row");
    let ws = format!("{group}-ws");
    conn.execute(
        "INSERT INTO workspaces (id, group_id, name, created_at) VALUES (?1, ?2, 'Default', 1)",
        rusqlite::params![ws, group],
    )
    .expect("workspace row");
    conn.execute(
        "INSERT INTO objects (id, group_id, workspace_id, type, name, created_at) \
         VALUES (?1, ?2, ?3, 'whiteboard', ?1, 1)",
        rusqlite::params![board, group, ws],
    )
    .expect("board row");
}

#[test]
fn materialize_run_mints_per_asset_rows_with_tenant() {
    let conn = conn_db();
    mk_board(&conn, "g-runs", "b-runs");

    let r1 = ingest::materialize_run(&conn, "b-runs", "hash-a").expect("run a");
    let r2 = ingest::materialize_run(&conn, "b-runs", "hash-b").expect("run b");
    assert_ne!(r1.run_id, r2.run_id, "each asset gets ITS OWN run");
    assert_eq!(r1.status, "materialized");

    let runs = ingest::runs_for_board(&conn, "b-runs").expect("list");
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].asset_hash, "hash-a");
    assert_eq!(runs[1].asset_hash, "hash-b");

    // The run row carries the board's REAL tenant (group), not a placeholder.
    let tenant: String = conn
        .query_row(
            "SELECT tenant_id FROM workflow_run WHERE run_id=?1",
            [&r1.run_id],
            |r| r.get(0),
        )
        .expect("tenant stamped");
    assert_eq!(tenant, "g-runs");

    // Unknown board ⇒ empty list; blank args ⇒ clear errors.
    assert!(ingest::runs_for_board(&conn, "no-such-board").expect("empty").is_empty());
    assert!(ingest::materialize_run(&conn, "", "h").is_err());
    assert!(ingest::materialize_run(&conn, "b-runs", " ").is_err());
}

// ── global-DB rail ─────────────────────────────────────────────────────────────

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ingest-test.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            cyan_backend::ensure_schema(&conn).expect("engine schema");
        }
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
    });
}

fn create_fresh_group(id: &str, name: &str) -> (String, String) {
    let g = Group {
        id: id.to_string(),
        name: name.to_string(),
        icon: "folder".to_string(),
        color: "#00FFFF".to_string(),
        created_at: chrono::Utc::now().timestamp(),
    };
    storage::group_insert(&g).expect("group insert");
    let (default_ws, plugins_ws) =
        storage::provision_group_workspaces(id, Some("node-under-test")).expect("seed workspaces");
    (default_ws.id, plugins_ws.id)
}

/// A single-tool probe manifest requiring `file_path` (the bind target prop).
fn probe_manifest() -> cyan_mcp::Manifest {
    serde_json::from_value(serde_json::json!({
        "name": "mediaio",
        "version": "0.1.0",
        "description": "probe test plugin",
        "runtime": "python-uv",
        "credentials": null,
        "extra_credentials": [],
        "events_emitted": [],
        "tools": [{
            "name": "probe",
            "when_to_use": "Probe a media file.",
            "aliases": [],
            "io_types": { "input": ["video"], "output": ["json"] },
            "stage": "ingest",
            "side_effects": [],
            "locality": "local",
            "input_schema": {
                "type": "object",
                "properties": { "file_path": {"type": "string"}, "name": {"type": "string"} },
                "required": ["file_path"]
            },
            "output_schema": {"type": "object"}
        }]
    }))
    .expect("strict manifest")
}

/// STAGE 4's load-bearing bind property: with TWO distinct clips on the board
/// (Tier-2 would refuse — ambiguous), each run's EXPLICIT asset binds ITS file;
/// no explicit asset keeps the existing never-guess behavior; and an explicit
/// asset that doesn't resolve stays pending instead of falling back to a guess.
#[test]
fn explicit_run_asset_binds_its_own_file_never_a_guess() {
    ensure_db();
    let group = "ingest-bind-group";
    let (default_ws, _) = create_fresh_group(group, "Ingest Bind Group");
    let board = "ingest-bind-board";
    storage::board_insert(board, &default_ws, "Ingest Board", chrono::Utc::now().timestamp())
        .expect("board insert");

    for (id, name, hash, path) in [
        ("ing-clip-a", "daily_a.mp4", "hash-daily-a", "/data/files/hash-daily-a"),
        ("ing-clip-b", "daily_b.mov", "hash-daily-b", "/data/files/hash-daily-b"),
    ] {
        storage::file_insert(id, Some(group), Some(&default_ws), Some(board), name, hash, 10, "peer", 1)
            .expect("file insert");
        storage::file_set_local_path(id, path).expect("local path");
    }

    let manifest = probe_manifest();
    let mention = workflow_bind::parse_mention("check @mediaio.probe").expect("mention");
    let bind = |explicit: Option<&str>| {
        workflow_bind::bind_with_manifest_for_asset(
            board,
            group,
            "check @mediaio.probe",
            &mention,
            &manifest,
            explicit,
        )
    };

    // Each run binds ITS OWN file — by content hash, and equally by objects id.
    for (explicit, path, name) in [
        ("hash-daily-a", "/data/files/hash-daily-a", "daily_a.mp4"),
        ("hash-daily-b", "/data/files/hash-daily-b", "daily_b.mov"),
        ("ing-clip-a", "/data/files/hash-daily-a", "daily_a.mp4"),
    ] {
        match bind(Some(explicit)) {
            workflow_bind::BindOutcome::Bound(b) => {
                assert_eq!(b.args["file_path"], path, "explicit asset {explicit} binds its file");
                assert_eq!(b.args["name"], name);
                assert!(b.pending.is_empty(), "pending must be empty; got {:?}", b.pending);
            }
            other => panic!("expected Bound, got {other:?}"),
        }
    }

    // NO explicit asset ⇒ the existing behavior, unchanged: two distinct clips
    // are ambiguous ⇒ pending, never a guess.
    match bind(None) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert!(
                b.pending.contains(&"file_path".to_string()),
                "two clips + no explicit asset ⇒ pending; got args {:?}",
                b.args
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }

    // An explicit asset that does NOT resolve on this board stays pending —
    // the Tier-2 fallback must not silently bind some other file.
    match bind(Some("hash-not-on-this-board")) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert!(
                b.pending.contains(&"file_path".to_string()),
                "unresolvable explicit asset ⇒ pending, never a fallback guess; got args {:?}",
                b.args
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }
}

/// With ONE attachment and no explicit asset, the Tier-2 implicit fill still
/// works exactly as before (the additive parameter changes nothing).
#[test]
fn no_explicit_asset_keeps_single_attachment_bind_unchanged() {
    ensure_db();
    let group = "ingest-bind-single";
    let (default_ws, _) = create_fresh_group(group, "Single Group");
    let board = "ingest-bind-single-board";
    storage::board_insert(board, &default_ws, "Single Board", chrono::Utc::now().timestamp())
        .expect("board insert");
    storage::file_insert(
        "single-clip",
        Some(group),
        Some(&default_ws),
        Some(board),
        "master.mp4",
        "hash-single",
        10,
        "peer",
        1,
    )
    .expect("file insert");
    storage::file_set_local_path("single-clip", "/data/files/hash-single").expect("path");

    let manifest = probe_manifest();
    let mention = workflow_bind::parse_mention("check @mediaio.probe").expect("mention");
    match workflow_bind::bind_with_manifest_for_asset(
        board,
        group,
        "check @mediaio.probe",
        &mention,
        &manifest,
        None,
    ) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(
                b.args["file_path"], "/data/files/hash-single",
                "the single attachment still fills implicitly with no explicit asset"
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }
}
