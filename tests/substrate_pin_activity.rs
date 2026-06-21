//! Substrate — pin sync (§B3), live activity (§L) and demo-seed removal (§D)
//! (ROUND10_FEEDBACK).
//!
//! Pin and board-changed ride the existing group gossip just like chat, so — as in
//! `substrate_chat` — the per-node oracle is the receiver's `SwiftEvent::Network(..)` (the
//! engine DB is process-global). Pin additionally applies to the shared store, asserted via
//! `storage::board_is_pinned`.

mod support;

use cyan_backend::models::events::NetworkEvent;
use cyan_backend::storage;
use support::{meet, serial, spawn_mesh, unique_discovery_key, unique_group_id, NodeCfg, SYNC_TIMEOUT};

fn cfg() -> NodeCfg {
    NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    }
}

/// §B3: pinning a board on one peer appears on the other — pin is a synced board property.
#[tokio::test]
async fn pin_propagates_to_peer() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let board = format!("{group}-board-pin");
    nodes[0].broadcast(
        &group,
        NetworkEvent::BoardPinned { board_id: board.clone(), is_pinned: true, updated_at: 1 },
    );

    let want = board.clone();
    nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::BoardPinned { board_id, is_pinned, .. } if *board_id == want && *is_pinned),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer received the pin");

    // The pin is applied as a synced board property on the receiver.
    assert!(storage::board_is_pinned(&board), "pin landed on the peer's board");
}

/// §L: a board edit emits a board-changed signal that reaches peers (so they refresh the
/// board's preview live). Driven over the same group gossip the real edit handlers use.
#[tokio::test]
async fn board_edit_emits_change_event() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let board = format!("{group}-board-edit");
    nodes[0].broadcast(
        &group,
        NetworkEvent::BoardChanged { board_id: board.clone(), editor: nodes[0].node_id.clone(), ts: 5 },
    );

    let want = board.clone();
    nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::BoardChanged { board_id, .. } if *board_id == want),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer received the board-changed signal");
}

/// §L: the board-changed signal carries WHO edited and WHICH board, so the peer can show a
/// "recently active/edited" marker attributed to the editor.
#[tokio::test]
async fn change_event_carries_editor_and_board() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let board = format!("{group}-board-attr");
    let editor = nodes[0].node_id.clone();
    nodes[0].broadcast(
        &group,
        NetworkEvent::BoardChanged { board_id: board.clone(), editor: editor.clone(), ts: 7 },
    );

    let want_board = board.clone();
    let got = nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::BoardChanged { board_id, .. } if *board_id == want_board),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer received board-changed");

    match got {
        NetworkEvent::BoardChanged { board_id, editor: got_editor, ts } => {
            assert_eq!(board_id, board, "carries the board id");
            assert_eq!(got_editor, editor, "carries the editor");
            assert_eq!(ts, 7, "carries the edit timestamp");
        }
        other => panic!("expected BoardChanged, got {other:?}"),
    }
}

/// §D: a fresh/empty DB NEVER auto-creates a "Demo Group"/"Demo Board". The demo-seed helper
/// is removed and the command/FFI are inert no-ops, so the engine creates no demo data.
#[test]
fn fresh_db_creates_no_demo_group() {
    support::ensure_db();
    let demo_group_id = blake3::hash(b"demo-group").to_hex().to_string();
    let demo_board_id = blake3::hash(b"demo-board").to_hex().to_string();

    let conn = storage::db().lock().expect("lock shared db");
    let demo_groups: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM groups WHERE name = 'Demo Group' OR id = ?1",
            rusqlite::params![demo_group_id],
            |r| r.get(0),
        )
        .expect("count demo groups");
    assert_eq!(demo_groups, 0, "no Demo Group is ever auto-created");

    let demo_boards: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM objects WHERE type = 'whiteboard' AND (name = 'Demo Board' OR id = ?1)",
            rusqlite::params![demo_board_id],
            |r| r.get(0),
        )
        .expect("count demo boards");
    assert_eq!(demo_boards, 0, "no Demo Board is ever auto-created");
}
