//! Substrate G5/G7 — chat at every level + file-via-chat (SUBSTRATE_TEST_SPEC §3).
//!
//! Chat is carried as `NetworkEvent::ChatSent` broadcast on the group topic; the scope
//! (group / workspace / board) is the `workspace_id` the message is tagged with. The
//! oracle is the receiver's per-node `SwiftEvent::Network(ChatSent { .. })` (the engine
//! DB is process-global, so the event channel — not storage — is the per-node signal).
//!
//! Bounded waits only. iroh 0.95. Relay disabled + mDNS by default (offline path).

mod support;

use cyan_backend::actors::DmAttachment;
use cyan_backend::models::commands::NetworkCommand;
use cyan_backend::models::events::{NetworkEvent, SwiftEvent};
use cyan_backend::storage;
use support::{
    meet, serial, spawn_mesh, stage_file, unique_discovery_key, unique_group_id, NodeCfg,
    SYNC_TIMEOUT,
};

fn cfg() -> NodeCfg {
    NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    }
}

fn chat(id: &str, scope: &str, message: &str) -> NetworkEvent {
    NetworkEvent::ChatSent {
        id: id.to_string(),
        board_id: scope.to_string(),
        workspace_id: scope.to_string(),
        message: message.to_string(),
        author: "author-a".to_string(),
        parent_id: None,
        timestamp: 1,
    }
}

/// Drive a chat at `scope` from node 0 and assert every other node receives it.
async fn chat_reaches_all_peers(n: usize, scope_suffix: &str) {
    let nodes = spawn_mesh(n, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let scope = format!("{group}-{scope_suffix}");
    let chat_id = format!("chat-{scope_suffix}-1");
    nodes[0].broadcast(&group, chat(&chat_id, &scope, "hello scope"));

    for receiver in &nodes[1..] {
        let want = chat_id.clone();
        receiver
            .wait_network(
                move |e| matches!(e, NetworkEvent::ChatSent { id, .. } if *id == want),
                SYNC_TIMEOUT,
            )
            .await
            .unwrap_or_else(|e| panic!("{} never received the chat: {e}", receiver.name));
    }
}

/// G5: a group-scoped chat reaches all peers.
#[tokio::test]
async fn group_chat_reaches_all_peers() {
    let _serial = serial().await;
    chat_reaches_all_peers(2, "group").await;
}

/// G5: a workspace-scoped chat reaches all peers.
#[tokio::test]
async fn workspace_chat_reaches_all_peers() {
    let _serial = serial().await;
    chat_reaches_all_peers(2, "workspace").await;
}

/// G5: a board-scoped chat reaches all peers.
#[tokio::test]
async fn board_chat_reaches_all_peers() {
    let _serial = serial().await;
    chat_reaches_all_peers(2, "board").await;
}

// ───────────────────────────── R11 §1 — chat is BOARD-scoped ─────────────────────────────
//
// Chat used to be keyed by `workspace_id`, so every board in a workspace shared one thread
// (chat bled across boards). It is now keyed by `board_id`. The engine DB is process-global,
// so these assert directly on `storage::*` with unique ids.

/// A chat is stored on a board and listed by that board — it never appears under another board.
#[test]
fn chat_is_board_scoped() {
    support::ensure_db();
    let w = unique_group_id();
    let b1 = unique_group_id();
    let b2 = unique_group_id();
    let id = format!("{b1}-c1");

    storage::chat_insert(&id, &b1, &w, "hi board1", "author", None, 1).expect("insert");

    let on_b1 = storage::chat_list_by_board(&b1).expect("list b1");
    assert!(
        on_b1.iter().any(|c| c.id == id && c.board_id == b1),
        "chat is listed on its own board, tagged with board_id"
    );
    let on_b2 = storage::chat_list_by_board(&b2).expect("list b2");
    assert!(
        !on_b2.iter().any(|c| c.id == id),
        "chat does NOT bleed into another board in the same workspace"
    );
}

/// Two boards in ONE workspace keep SEPARATE chat threads (the core privacy/correctness bug).
#[test]
fn two_boards_same_workspace_have_separate_chats() {
    support::ensure_db();
    let w = unique_group_id();
    let b1 = unique_group_id();
    let b2 = unique_group_id();
    let m1 = format!("{b1}-only");
    let m2 = format!("{b2}-only");

    storage::chat_insert(&m1, &b1, &w, "for board 1", "author", None, 1).expect("insert b1");
    storage::chat_insert(&m2, &b2, &w, "for board 2", "author", None, 2).expect("insert b2");

    let on_b1 = storage::chat_list_by_board(&b1).expect("list b1");
    assert!(on_b1.iter().any(|c| c.id == m1), "board 1 has its own message");
    assert!(!on_b1.iter().any(|c| c.id == m2), "board 1 does not see board 2's message");

    let on_b2 = storage::chat_list_by_board(&b2).expect("list b2");
    assert!(on_b2.iter().any(|c| c.id == m2), "board 2 has its own message");
    assert!(!on_b2.iter().any(|c| c.id == m1), "board 2 does not see board 1's message");
}

/// A legacy (pre-R11) chat — keyed only by workspace, no `board_id` — migrates to the
/// workspace's deterministic default board (its earliest board) so it is never lost.
#[test]
fn legacy_chat_migrates_to_board() {
    support::ensure_db();
    let g = unique_group_id();
    let w = unique_group_id();
    let b = unique_group_id();
    let legacy = format!("{w}-legacy-chat");

    // Real structure: a workspace with one board (the deterministic default thread).
    storage::group_insert_simple(&g, "G", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&w, &g, "WS").expect("workspace");
    storage::board_insert_simple(&b, &w, "Board", 1).expect("board");

    // A legacy chat row: workspace set, board_id NULL (the pre-R11 shape).
    {
        let conn = storage::db().lock().expect("lock db");
        conn.execute(
            "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, hash, created_at)
             VALUES (?1, ?2, 'chat', 'legacy hello', 'author', 5)",
            rusqlite::params![legacy, w],
        )
        .expect("insert legacy chat");
    }
    // Before migration it is on no board.
    assert!(
        !storage::chat_list_by_board(&b).unwrap().iter().any(|c| c.id == legacy),
        "legacy chat has no board before migration"
    );

    storage::migrate_chats_to_boards().expect("migrate");

    let on_board = storage::chat_list_by_board(&b).expect("list board");
    assert!(
        on_board.iter().any(|c| c.id == legacy),
        "legacy chat migrated onto the workspace's default board"
    );
}

/// G7: a chat carrying an attachment shares the file into the scope, leaving the receiver
/// with both the message and the fetched file.
///
/// `SendDirectChat` now takes an optional `attachment: Option<DmAttachment>` (additive). The
/// host stages a file at workspace scope, then sends a DM carrying that attachment to the
/// peer. The receiver must end up with BOTH the message (`DirectMessageReceived`) AND the
/// file fetched into that scope (`FileDownloaded`, bytes blake3-verified) — the engine's DM
/// receive path now registers the attachment in scope and fetches it from the sender.
#[tokio::test]
async fn chat_with_attachment_shares_file_into_scope() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    // Host stages a file at workspace scope so it is servable over the file-transfer protocol.
    let scope = format!("{group}-ws");
    let content: Vec<u8> = (0..(32 * 1024 + 17)).map(|i| (i % 251) as u8).collect();
    let file_id = format!("attach-{}", &group[16..32]);
    let name = format!("{file_id}.bin");
    let hash = stage_file(
        &file_id,
        &group,
        Some(&scope),
        None,
        &content,
        &nodes[0].node_id,
    );

    // Host sends a chat carrying the attachment to the peer (the new optional field).
    nodes[0].cmd(NetworkCommand::SendDirectChat {
        peer_id: nodes[1].node_id.clone(),
        workspace_id: scope.clone(),
        message: "sharing a file with you".to_string(),
        parent_id: None,
        attachment: Some(DmAttachment {
            file_id: file_id.clone(),
            name: name.clone(),
            hash: hash.clone(),
            size: content.len() as u64,
        }),
    });

    // The receiver gets the chat message itself.
    nodes[1]
        .wait_for(
            |e| matches!(e, SwiftEvent::DirectMessageReceived { is_incoming: true, .. }),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer received the chat message");

    // …and the attached file is fetched into that scope, bytes intact.
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, SYNC_TIMEOUT)
        .await
        .expect("peer fetched the attached file into scope");
    let got = std::fs::read(&local_path).expect("read downloaded attachment");
    assert_eq!(got.len(), content.len(), "attachment byte length matches");
    assert_eq!(
        blake3::hash(&got).to_hex().to_string(),
        hash,
        "attachment blake3 matches the source"
    );
}
