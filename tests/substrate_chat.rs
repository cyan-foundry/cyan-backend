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
