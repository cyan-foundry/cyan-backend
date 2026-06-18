//! Substrate G6/G8 — P2P file share per scope + large-file QUIC (SUBSTRATE_TEST_SPEC §3).
//!
//! A host stages a file (bytes on disk + DB record); a peer that shares the file's group
//! issues `RequestFileDownload`, which opens a direct QUIC stream (FILE_TRANSFER_ALPN) to
//! the host and streams the bytes. Oracle: the downloader's per-node `FileDownloaded`
//! event AND an independent blake3 + length check of the bytes that actually landed on the
//! downloader's disk (the engine also blake3-verifies before emitting the event).
//!
//! Bounded waits only. iroh 0.95. Relay disabled + mDNS by default (offline path).

mod support;

use std::time::{Duration, Instant};

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

/// Deterministic pseudo-random bytes (no Math.random; content varies by length+seed).
fn make_content(len: usize, seed: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    let mut x = seed;
    for i in 0..len {
        x = x.wrapping_mul(31).wrapping_add((i as u8) ^ seed).wrapping_add(7);
        v.push(x);
    }
    v
}

/// Host stages a file at the given scope, peer downloads it, bytes verified intact.
async fn file_shares_at_scope(workspace_id: Option<&str>, board_id: Option<&str>, label: &str) {
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let content = make_content(64 * 1024 + 123, 0xA5); // ~64KB, not chunk-aligned
    let file_id = format!("file-{label}-{}", &group[16..32]);
    let ws = workspace_id.map(|w| format!("{group}-{w}"));
    let bd = board_id.map(|b| format!("{group}-{b}"));
    let hash = stage_file(
        &file_id,
        &group,
        ws.as_deref(),
        bd.as_deref(),
        &content,
        &nodes[0].node_id,
    );

    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, SYNC_TIMEOUT)
        .await
        .expect("peer reports FileDownloaded");

    let got = std::fs::read(&local_path).expect("read downloaded file");
    assert_eq!(got.len(), content.len(), "{label}: byte length matches");
    assert_eq!(
        blake3::hash(&got).to_hex().to_string(),
        hash,
        "{label}: blake3 of downloaded bytes matches the source"
    );
}

/// G6: a file uploaded at group scope is fetchable on a peer, blake3-verified.
#[tokio::test]
async fn file_shared_at_group_scope() {
    let _serial = serial().await;
    file_shares_at_scope(None, None, "group").await;
}

/// G6: a file uploaded at workspace scope is fetchable on a peer, blake3-verified.
#[tokio::test]
async fn file_shared_at_workspace_scope() {
    let _serial = serial().await;
    file_shares_at_scope(Some("ws"), None, "workspace").await;
}

/// G6: a file uploaded at board scope is fetchable on a peer, blake3-verified.
#[tokio::test]
async fn file_shared_at_board_scope() {
    let _serial = serial().await;
    file_shares_at_scope(Some("ws"), Some("board"), "board").await;
}

/// G8: a 100 MB blob transfers intact (blake3 + exact byte length) over direct QUIC.
#[tokio::test]
async fn large_file_100mb_transfers_intact() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let len = 100 * 1024 * 1024;
    let content = make_content(len, 0x3C);
    let file_id = format!("file-100mb-{}", &group[16..32]);
    let hash = stage_file(&file_id, &group, None, None, &content, &nodes[0].node_id);

    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, Duration::from_secs(120))
        .await
        .expect("peer reports FileDownloaded for 100MB");

    let got = std::fs::read(&local_path).expect("read downloaded 100MB file");
    assert_eq!(got.len(), len, "100MB byte length matches");
    assert_eq!(
        blake3::hash(&got).to_hex().to_string(),
        hash,
        "100MB blake3 matches"
    );
}

/// G8: measure direct-QUIC loopback throughput and assert a (conservative) floor.
/// The floor is intentionally low — it is a regression guard against a collapse to a tiny
/// window, not a benchmark. Measured locally well above this; see PROGRESS.md.
#[tokio::test]
async fn large_file_meets_throughput_floor() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let len = 64 * 1024 * 1024; // 64 MB
    let content = make_content(len, 0x7E);
    let file_id = format!("file-tput-{}", &group[16..32]);
    let hash = stage_file(&file_id, &group, None, None, &content, &nodes[0].node_id);

    let start = Instant::now();
    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, Duration::from_secs(120))
        .await
        .expect("peer reports FileDownloaded for throughput probe");
    let elapsed = start.elapsed();

    let got = std::fs::read(&local_path).expect("read downloaded file");
    assert_eq!(
        blake3::hash(&got).to_hex().to_string(),
        hash,
        "throughput-probe blake3 matches"
    );

    let mb = len as f64 / (1024.0 * 1024.0);
    let mbps = mb / elapsed.as_secs_f64();
    eprintln!("📊 direct-QUIC loopback throughput: {mbps:.1} MB/s for {mb:.0} MB");
    const FLOOR_MBPS: f64 = 5.0;
    assert!(
        mbps >= FLOOR_MBPS,
        "throughput {mbps:.1} MB/s below floor {FLOOR_MBPS} MB/s"
    );
}

/// G8: 1 GB blob — `#[ignore]` for CI cost; runnable on demand to confirm the headline.
#[ignore = "1GB transfer is expensive; run on demand"]
#[tokio::test]
async fn large_file_1gb_transfers_intact() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let len = 1024 * 1024 * 1024;
    let content = make_content(len, 0x11);
    let file_id = format!("file-1gb-{}", &group[16..32]);
    let hash = stage_file(&file_id, &group, None, None, &content, &nodes[0].node_id);

    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, Duration::from_secs(600))
        .await
        .expect("peer reports FileDownloaded for 1GB");

    let got = std::fs::read(&local_path).expect("read downloaded 1GB file");
    assert_eq!(got.len(), len, "1GB byte length matches");
    assert_eq!(blake3::hash(&got).to_hex().to_string(), hash, "1GB blake3 matches");
}
