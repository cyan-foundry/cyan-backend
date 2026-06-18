//! Substrate G9 — the headline: the essential matrix with NO internet
//! (`RelayPolicy::Disabled` + `DiscoveryPolicy::MdnsOnly`), SUBSTRATE_TEST_SPEC §3.
//!
//! `RelayPolicy::Disabled` maps to iroh `RelayMode::Disabled` (proven by the engine's
//! `relay_mode_for` unit tests), so the endpoints have NO relay at all — any path that
//! completes here used only LAN/loopback transport. The harness wires loopback direct
//! addresses between nodes (no relay, no DNS), so these runs genuinely have zero reliance
//! on a non-LAN endpoint. Each test re-runs a slice of G1/G4/G5/G6/G8 under that config.
//!
//! Bounded waits only. iroh 0.95.

mod support;

use cyan_backend::models::events::NetworkEvent;
use support::{
    meet, serial, spawn_mesh, stage_file, unique_discovery_key, unique_group_id, DiscoveryPolicy,
    NodeCfg, RelayPolicy, SYNC_TIMEOUT,
};

/// The offline config: relay fully disabled, mDNS-only discovery.
fn offline_cfg() -> NodeCfg {
    NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        discovery_key: unique_discovery_key(),
    }
}

/// Guard: this suite must never accidentally enable a relay.
fn assert_offline(cfg: &NodeCfg) {
    assert!(
        matches!(cfg.relay, RelayPolicy::Disabled),
        "offline suite requires RelayPolicy::Disabled"
    );
    assert!(
        matches!(cfg.discovery, DiscoveryPolicy::MdnsOnly),
        "offline suite requires DiscoveryPolicy::MdnsOnly"
    );
}

/// G9: discovery + live delta sync with no internet.
#[tokio::test]
async fn offline_discovery_and_sync() {
    let _serial = serial().await;
    let cfg = offline_cfg();
    assert_offline(&cfg);

    let nodes = spawn_mesh(2, cfg).await.expect("offline mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("nodes discover each other offline");

    nodes[0].broadcast(
        &group,
        NetworkEvent::WhiteboardElementAdded {
            id: "offline-elem-1".to_string(),
            board_id: "board-1".to_string(),
            element_type: "rectangle".to_string(),
            x: 1.0,
            y: 1.0,
            width: 10.0,
            height: 10.0,
            z_index: 1,
            style_json: None,
            content_json: None,
            created_at: 1,
            updated_at: 1,
        },
    );
    nodes[1]
        .wait_network(
            |e| matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if id == "offline-elem-1"),
            SYNC_TIMEOUT,
        )
        .await
        .expect("delta propagates offline");
}

/// G9: chat at all levels with no internet.
#[tokio::test]
async fn offline_chat_all_levels() {
    let _serial = serial().await;
    let cfg = offline_cfg();
    assert_offline(&cfg);

    let nodes = spawn_mesh(2, cfg).await.expect("offline mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet offline");

    for level in ["group", "workspace", "board"] {
        let id = format!("offline-chat-{level}");
        nodes[0].broadcast(
            &group,
            NetworkEvent::ChatSent {
                id: id.clone(),
                workspace_id: format!("{group}-{level}"),
                message: format!("offline {level} msg"),
                author: "a".to_string(),
                parent_id: None,
                timestamp: 1,
            },
        );
        let want = id.clone();
        nodes[1]
            .wait_network(
                move |e| matches!(e, NetworkEvent::ChatSent { id, .. } if *id == want),
                SYNC_TIMEOUT,
            )
            .await
            .unwrap_or_else(|e| panic!("offline {level} chat did not propagate: {e}"));
    }
}

/// G9: P2P file share + a multi-MB transfer with no internet, blake3-verified.
#[tokio::test]
async fn offline_file_share_and_large_transfer() {
    let _serial = serial().await;
    let cfg = offline_cfg();
    assert_offline(&cfg);

    let nodes = spawn_mesh(2, cfg).await.expect("offline mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet offline");

    // a small file and a several-MB file, both over the relay-disabled direct path
    for (label, len) in [("small", 4096usize), ("large", 8 * 1024 * 1024)] {
        let mut content = Vec::with_capacity(len);
        for i in 0..len {
            content.push((i as u8) ^ 0x5A);
        }
        let file_id = format!("offline-file-{label}-{}", &group[16..32]);
        let hash = stage_file(&file_id, &group, None, None, &content, &nodes[0].node_id);

        nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
        let local_path = nodes[1]
            .wait_file_downloaded(&file_id, std::time::Duration::from_secs(60))
            .await
            .unwrap_or_else(|e| panic!("offline {label} file did not download: {e}"));

        let got = std::fs::read(&local_path).expect("read offline downloaded file");
        assert_eq!(got.len(), len, "offline {label} byte length matches");
        assert_eq!(
            blake3::hash(&got).to_hex().to_string(),
            hash,
            "offline {label} blake3 matches"
        );
    }
}
