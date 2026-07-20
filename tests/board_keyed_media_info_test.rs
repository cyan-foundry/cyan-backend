//! B3 (asset-class showcase): `review_media_info` is BOARD-KEYED — the app asks
//! with just a `board_id` and the engine resolves the board's current published
//! proxy itself (`current_proxy_ref`). No binding token required. An explicit
//! `proxy_ref` still wins (back-compat for the coordinator's bound path).

use cyan_backend::{asset_registry, changelist, review_loop as rl, review_state as rv};
use rusqlite::Connection;
use serde_json::json;

const T: &str = "device"; // an un-grouped in-memory board resolves to the "device" tenant
const BOARD: &str = "board-keyed-1";
const MASTER: &str = "master-bk-1";
const BRANCH: &str = "main";

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    asset_registry::migrate(&conn).expect("migrate assets");
    rl::migrate(&conn).expect("migrate review_loop");
    conn
}

fn register_master_and_published_proxy(conn: &Connection) {
    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: MASTER.to_string(),
            tenant_id: T.to_string(),
            kind: Some("master".to_string()),
            fps: Some(24.0),
            duration_ms: Some(10_000),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({"path": "/abs/master-bk-1.mp4"}),
            render_profile: None,
            created_at: 1,
        },
    )
    .expect("register master");
    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: "proxy-bk-1".to_string(),
            tenant_id: T.to_string(),
            kind: Some("proxy".to_string()),
            fps: Some(24.0),
            duration_ms: Some(10_000),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: None,
            created_at: 2,
        },
    )
    .expect("register proxy");
    // Derivation + the Frame.io ref = "published" (what latest_published_proxy keys on).
    let v = changelist::get(conn, T, MASTER, BRANCH)
        .expect("changelist view")
        .head_version
        .map(|v| v.version_id)
        .unwrap_or_else(|| "v0".to_string());
    asset_registry::set_derivation(conn, T, "proxy-bk-1", MASTER, &v).expect("derivation");
    asset_registry::set_remote_ref(conn, T, "proxy-bk-1", "frameio", "file_bk_1")
        .expect("remote ref");
}

#[test]
fn board_id_alone_resolves_media_info() {
    let conn = db();
    register_master_and_published_proxy(&conn);
    rl::register(&conn, T, BOARD, MASTER, BRANCH, rv::DEFAULT_MAX_ROUNDS).expect("register loop");

    let info = rl::review_media_info_for_board(&conn, BOARD, None)
        .expect("board-keyed media info resolves without a proxy_ref");
    assert_eq!(info["master_hash"], MASTER);
    assert_eq!(info["master_path"], "/abs/master-bk-1.mp4");
    assert_eq!(info["frameio_ref"], "file_bk_1");
}

#[test]
fn explicit_proxy_ref_still_wins() {
    let conn = db();
    register_master_and_published_proxy(&conn);
    rl::register(&conn, T, BOARD, MASTER, BRANCH, rv::DEFAULT_MAX_ROUNDS).expect("register loop");

    let info = rl::review_media_info_for_board(&conn, BOARD, Some("file_bk_1"))
        .expect("explicit ref resolves");
    assert_eq!(info["frameio_ref"], "file_bk_1");
}

#[test]
fn board_with_no_published_media_is_a_clear_error() {
    let conn = db();
    let err = rl::review_media_info_for_board(&conn, "board-empty", None)
        .expect_err("no loop, no proxy — must be a typed error, not a panic or an empty blob");
    assert!(
        err.to_string().contains("no published review media"),
        "error names the real gap: {err}"
    );
}

/// Two boards, two independent loops: each board resolves ITS OWN proxy —
/// the isolation property the live board grid (B3) depends on.
#[test]
fn two_boards_resolve_their_own_assets() {
    let conn = db();
    register_master_and_published_proxy(&conn);
    rl::register(&conn, T, BOARD, MASTER, BRANCH, rv::DEFAULT_MAX_ROUNDS).expect("loop 1");

    // Board 2 drives a DIFFERENT master with its own published proxy.
    asset_registry::upsert(
        &conn,
        &asset_registry::Asset {
            hash: "master-bk-2".to_string(),
            tenant_id: T.to_string(),
            kind: Some("master".to_string()),
            fps: Some(24.0),
            duration_ms: Some(8_000),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({"path": "/abs/master-bk-2.mp4"}),
            render_profile: None,
            created_at: 3,
        },
    )
    .expect("master 2");
    asset_registry::upsert(
        &conn,
        &asset_registry::Asset {
            hash: "proxy-bk-2".to_string(),
            tenant_id: T.to_string(),
            kind: Some("proxy".to_string()),
            fps: Some(24.0),
            duration_ms: Some(8_000),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: None,
            created_at: 4,
        },
    )
    .expect("proxy 2");
    asset_registry::set_derivation(&conn, T, "proxy-bk-2", "master-bk-2", "v0").expect("deriv 2");
    asset_registry::set_remote_ref(&conn, T, "proxy-bk-2", "frameio", "file_bk_2").expect("ref 2");
    rl::register(&conn, T, "board-keyed-2", "master-bk-2", BRANCH, rv::DEFAULT_MAX_ROUNDS)
        .expect("loop 2");

    let a = rl::review_media_info_for_board(&conn, BOARD, None).expect("board 1");
    let b = rl::review_media_info_for_board(&conn, "board-keyed-2", None).expect("board 2");
    assert_eq!(a["master_hash"], MASTER);
    assert_eq!(b["master_hash"], "master-bk-2");
    assert_ne!(a["frameio_ref"], b["frameio_ref"], "no cross-contamination");
}
