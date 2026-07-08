//! TIER 3 — the decision-relay echo rail, over the REAL `cyan_review_command`
//! JSON surface (`review_state::command`).
//!
//! After the app posts a review decision to Frame.io as a comment, it records
//! the returned comment id via the additive board-keyed `record_relay` verb.
//! The NEXT sense pass must drop that comment (`is_own_source_ref`) — without
//! the record, the loop would ingest its own relay as a fresh reviewer note
//! and auto-approve an open await-sense gate (it would talk to itself).

use std::sync::Once;

use cyan_backend::models::core::Group;
use cyan_backend::{review_state, storage};
use cyan_backend::util::MutexExt;

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("review-relay.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            cyan_backend::ensure_schema(&conn).expect("engine schema");
        }
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir);
    });
}

#[test]
fn record_relay_resolves_board_tenant_and_registers_the_echo_ref() {
    ensure_db();
    let group = "relay-group";
    let g = Group {
        id: group.to_string(),
        name: "Relay Group".to_string(),
        icon: "folder".to_string(),
        color: "#00FFFF".to_string(),
        created_at: chrono::Utc::now().timestamp(),
    };
    storage::group_insert(&g).expect("group insert");
    let (ws, _) = storage::provision_group_workspaces(group, None).expect("workspaces");
    storage::board_insert("relay-board", &ws.id, "Relay Board", 0).expect("board");

    // The verb the app calls right after its Frame.io comment POST returns an id.
    let reply = review_state::command(
        r#"{"op":"record_relay","board_id":"relay-board","source_ref":"c-relay-1"}"#,
    );
    let v: serde_json::Value = serde_json::from_str(&reply).expect("reply json");
    assert!(v.get("error").is_none(), "record_relay must succeed; got {v}");
    assert_eq!(v["recorded"], true);
    assert_eq!(v["source_ref"], "c-relay-1");

    // The echo rail: the ref is now OUR OWN under the BOARD'S GROUP tenant —
    // the exact check `ingest_sense_result` runs per sensed comment.
    {
        let conn = storage::db().lock_safe();
        assert!(
            cyan_backend::changelist::is_own_source_ref(&conn, group, "frameio", "c-relay-1")
                .expect("is_own_source_ref"),
            "the relayed comment id must be recorded as our own write-back"
        );
        assert!(
            !cyan_backend::changelist::is_own_source_ref(&conn, group, "frameio", "c-other")
                .expect("is_own_source_ref"),
            "other ids stay foreign"
        );
    }

    // A missing source_ref is a CLEAR error, never a panic.
    let bad = review_state::command(r#"{"op":"record_relay","board_id":"relay-board"}"#);
    let bv: serde_json::Value = serde_json::from_str(&bad).expect("bad reply json");
    assert!(
        bv.get("error").is_some(),
        "missing source_ref must error clearly; got {bv}"
    );
}
