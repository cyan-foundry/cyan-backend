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
        NetworkEvent::BoardChanged {
            board_id: board.clone(),
            editor: nodes[0].node_id.clone(),
            ts: 5,
            name: "Edited Board".to_string(),
            preview: "latest cell text".to_string(),
        },
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
        NetworkEvent::BoardChanged {
            board_id: board.clone(),
            editor: editor.clone(),
            ts: 7,
            name: "Attr Board".to_string(),
            preview: "preview snippet".to_string(),
        },
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
        NetworkEvent::BoardChanged { board_id, editor: got_editor, ts, .. } => {
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

// ─────────────────── R11 §9/§9b/PATTERN — board-state CONVERGENT sync ───────────────────
//
// board_metadata sync is per-field convergent LWW, never a whole-record clobber. Three
// independent lanes: descriptive (labels/rating/…) on `meta_updated_at`, pin (`is_pinned`)
// on `pin_updated_at`, and activity counters via MAX. These assert on the process-global
// `storage::*` merge directly — the oracle for "a stale snapshot never clobbers a newer edit".

/// §9: the board-changed signal carries the board's name + a content preview, so a peer can
/// refresh that board's preview card live (it used to stay blank — the signal carried no
/// content). Proves both that the engine BUILDS the preview data and that it survives the wire.
#[tokio::test]
async fn board_change_event_carries_preview_data() {
    let _serial = serial().await;
    support::ensure_db();

    // A real board with content → it has a non-blank preview.
    let board = unique_group_id();
    let ws = unique_group_id();
    storage::board_insert_simple(&board, &ws, "Design Notes", 1).expect("board");
    storage::cell_insert(&format!("{board}-cell"), &board, "step", 0, Some("Ship the preview fix"))
        .expect("cell");

    let (name, preview) = storage::board_preview(&board);
    assert_eq!(name, "Design Notes", "preview carries the board name");
    assert!(
        preview.contains("Ship the preview fix"),
        "preview carries a content snippet, got {preview:?}"
    );

    // …and the BoardChanged the engine emits with that data reaches a peer non-blank.
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");
    nodes[0].broadcast(
        &group,
        NetworkEvent::BoardChanged {
            board_id: board.clone(),
            editor: nodes[0].node_id.clone(),
            ts: 9,
            name: name.clone(),
            preview: preview.clone(),
        },
    );

    let want = board.clone();
    let got = nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::BoardChanged { board_id, .. } if *board_id == want),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer received board-changed");
    match got {
        NetworkEvent::BoardChanged { name, preview, .. } => {
            assert!(!name.is_empty(), "peer gets a non-blank name to refresh its preview");
            assert!(!preview.is_empty(), "peer gets a non-blank content preview");
        }
        other => panic!("expected BoardChanged, got {other:?}"),
    }
}

/// §9b: pins from two peers MERGE — a stale snapshot row never clobbers a board another peer
/// just pinned (the "peer A pins 2 boards → peer B's pins disappear" bug).
#[test]
fn pins_from_two_peers_merge_not_clobber() {
    support::ensure_db();
    let board_a = unique_group_id(); // pinned by "peer A"
    let board_b = unique_group_id(); // pinned by "peer B"

    // Each peer pins its own board (per-board convergent flag, LWW by updated_at).
    storage::board_meta_set_pinned(&board_a, true, 10).expect("peer A pins board_a");
    storage::board_meta_set_pinned(&board_b, true, 10).expect("peer B pins board_b");

    // A snapshot from peer A (who never pinned board_b) carries board_b with is_pinned=false at
    // an OLDER pin clock — it must NOT un-pin board_b.
    storage::board_metadata_upsert(
        &board_b, &[], 0, 0, None, &[], Some("canvas"), 0, /*is_pinned*/ false,
        /*meta_updated_at*/ 0, /*pin_updated_at*/ 3,
    )
    .expect("apply stale snapshot row for board_b");

    // Peer A's pin for board_a arrives (true, newer/equal clock) and lands.
    storage::board_meta_set_pinned(&board_a, true, 10).expect("apply peer A pin for board_a");

    assert!(storage::board_is_pinned(&board_a), "board_a stays pinned (peer A)");
    assert!(
        storage::board_is_pinned(&board_b),
        "board_b stays pinned — a stale snapshot did NOT clobber peer B's pin"
    );
}

/// §9/PATTERN: board_metadata is per-field LWW — a stale whole-record snapshot never clobbers
/// a field another peer edited newer; an independent field's edit doesn't disturb the others.
#[test]
fn board_metadata_field_lww_no_whole_record_clobber() {
    support::ensure_db();
    let board = unique_group_id();

    // Descriptive edit (labels) at t=10, then an independent pin at t=20.
    storage::board_metadata_upsert(
        &board, &["alpha".to_string()], 0, 0, None, &[], Some("canvas"), 0, false,
        /*meta*/ 10, /*pin*/ 0,
    )
    .expect("set labels");
    storage::board_meta_set_pinned(&board, true, 20).expect("pin");

    // A STALE whole-record snapshot (everything reset, OLD clocks on both lanes) must clobber
    // nothing — neither the labels nor the pin.
    storage::board_metadata_upsert(
        &board, &[], 0, 0, None, &[], Some("canvas"), 0, false,
        /*meta*/ 5, /*pin*/ 5,
    )
    .expect("apply stale whole-record snapshot");

    let after_stale = &storage::board_metadata_list_by_boards(std::slice::from_ref(&board)).unwrap()[0];
    assert_eq!(after_stale.labels, vec!["alpha".to_string()], "stale snapshot did NOT clobber labels");
    assert!(after_stale.is_pinned, "stale snapshot did NOT un-pin the board");

    // A NEWER descriptive-only edit updates labels but leaves the (newer) pin untouched.
    storage::board_metadata_upsert(
        &board, &["beta".to_string()], 0, 0, None, &[], Some("canvas"), 0, false,
        /*meta*/ 30, /*pin*/ 0,
    )
    .expect("newer descriptive edit");

    let after_new = &storage::board_metadata_list_by_boards(std::slice::from_ref(&board)).unwrap()[0];
    assert_eq!(after_new.labels, vec!["beta".to_string()], "newer descriptive edit applied");
    assert!(after_new.is_pinned, "independent descriptive edit did NOT disturb the pin lane");
}
