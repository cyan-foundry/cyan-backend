//! Substrate G5/G7 — chat at every level + file-via-chat (SUBSTRATE_TEST_SPEC §3).
//!
//! Chat is carried as `NetworkEvent::ChatSent` broadcast on the group topic; the scope
//! (group / workspace / board) is the `workspace_id` the message is tagged with. The
//! oracle is the receiver's per-node `SwiftEvent::Network(ChatSent { .. })` (the engine
//! DB is process-global, so the event channel — not storage — is the per-node signal).
//!
//! Bounded waits only. iroh 0.95. Relay disabled + mDNS by default (offline path).

mod support;

use cyan_backend::models::events::NetworkEvent;
use support::{meet, serial, spawn_mesh, unique_discovery_key, unique_group_id, NodeCfg, SYNC_TIMEOUT};

fn cfg() -> NodeCfg {
    NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    }
}

fn chat(id: &str, scope: &str, message: &str) -> NetworkEvent {
    NetworkEvent::ChatSent {
        id: id.to_string(),
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

/// G7: a chat carrying an attachment shares the file into the scope, leaving the receiver
/// with both the message and the fetched file.
///
/// `#[ignore]` — engine-capability finding: there is **no `NetworkCommand` that carries an
/// attachment**. `SendDirectChat` has only `{peer_id, workspace_id, message, parent_id}`,
/// and `DmAttachment` (defined on the wire `DirectMessage`) is never constructed anywhere
/// in the engine. So a chat-with-attachment cannot be driven through the command interface
/// the substrate exposes; G7 cannot be exercised without first adding that capability.
#[ignore = "no NetworkCommand carries an attachment; DmAttachment is never wired to a command"]
#[tokio::test]
async fn chat_with_attachment_shares_file_into_scope() {
    unimplemented!("needs an attachment-carrying command in the engine — see doc comment");
}
