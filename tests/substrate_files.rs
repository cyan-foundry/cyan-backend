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
    harness_tmp_parent, meet, serial, spawn_mesh, stage_file, stage_file_streamed,
    sweep_dead_harness_dirs, unique_discovery_key, unique_group_id, NodeCfg, SYNC_TIMEOUT,
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

/// R12 B1 (P1): an inbound file from a peer raises a DISTINCT board-scoped `FileReceived`
/// event (the file analog of a chat-message notification) — not only the chat event. The
/// sender's own echo must NOT fire it (guarded by `source_peer`).
#[tokio::test]
async fn inbound_file_raises_file_received_event() {
    use cyan_backend::models::events::{NetworkEvent, SwiftEvent};

    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let board = format!("{group}-board-fr");
    let file_id = format!("file-fr-{}", &group[16..32]);
    nodes[0].broadcast(
        &group,
        NetworkEvent::FileAvailable {
            id: file_id.clone(),
            group_id: Some(group.clone()),
            workspace_id: Some(format!("{group}-ws")),
            board_id: Some(board.clone()),
            name: "deck.pdf".to_string(),
            hash: "hash-fr".to_string(),
            size: 4096,
            source_peer: nodes[0].node_id.clone(),
            created_at: 1,
        },
    );

    let want_id = file_id.clone();
    let want_board = board.clone();
    let got = nodes[1]
        .wait_for(
            move |e| matches!(e, SwiftEvent::FileReceived { id, board_id, .. } if *id == want_id && *board_id == want_board),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer raises a distinct FileReceived for the inbound file");
    match got {
        SwiftEvent::FileReceived { name, source_peer, .. } => {
            assert_eq!(name, "deck.pdf", "carries the file name for the notification");
            assert_eq!(source_peer, nodes[0].node_id, "attributes the sender");
        }
        other => panic!("expected FileReceived, got {other:?}"),
    }
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
    // ALWAYS printed — the measured number is the deliverable, whichever floor gates.
    eprintln!("📊 direct-QUIC loopback throughput: {mbps:.1} MB/s for {mb:.0} MB");

    // Two floors (G8 hardening): the strict dailies-grade floor (≥ 80 MB/s) gates under
    // CYAN_PERF=1 — dedicated perf runs, built `--release` (debug builds cap at ~16 MB/s
    // on QUIC crypto alone) — since shared CI machines vary wildly; every run still
    // enforces the absolute regression guard against a collapse to a tiny window.
    //
    // Measured 2026-07-02 (M-series laptop, loopback, release): ~113 MB/s, and ~115 MB/s
    // with 1 stream / 1.25 MB window — i.e. loopback is bound by per-connection QUIC
    // crypto + per-packet UDP syscalls (quinn serializes a connection's crypto on one
    // core; loopback RTT ≈ 0 so windows/streams never bind here — they pay off on real
    // LAN RTTs). Suspects for a future >200 MB/s LAN fast path: multiple QUIC
    // *connections*, UDP GSO/GRO, or a LAN-TCP leg — noted, deliberately NOT built now.
    const FLOOR_MBPS: f64 = 3.0;
    const PERF_FLOOR_MBPS: f64 = 80.0;
    let floor = if std::env::var("CYAN_PERF").ok().as_deref() == Some("1") {
        PERF_FLOOR_MBPS
    } else {
        FLOOR_MBPS
    };
    assert!(
        mbps >= floor,
        "throughput {mbps:.1} MB/s below floor {floor} MB/s"
    );
}

/// G8 hardening: a large transfer must never head-of-line-block chat/sync — transfers run
/// on their own tokio tasks and their own QUIC streams. Oracle: a chat broadcast sent the
/// moment a 512 MB transfer starts is delivered within a strict bound (far below the
/// transfer's duration), and the transfer itself still completes intact afterwards.
#[tokio::test]
async fn chat_delivers_during_large_transfer() {
    use cyan_backend::models::events::NetworkEvent;

    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let len: u64 = 512 * 1024 * 1024;
    let file_id = format!("file-hol-{}", &group[16..32]);
    let hash = stage_file_streamed(&file_id, &group, len, 0x2B, &nodes[0].node_id);

    // Start the big transfer, then immediately send a chat the other way down the
    // same group's gossip.
    let started = Instant::now();
    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let chat_id = format!("chat-hol-{}", &group[16..32]);
    nodes[0].broadcast(
        &group,
        NetworkEvent::ChatSent {
            id: chat_id.clone(),
            board_id: group.clone(),
            workspace_id: group.clone(),
            message: "still responsive?".to_string(),
            author: nodes[0].node_id.clone(),
            parent_id: None,
            timestamp: 1,
        },
    );

    let want = chat_id.clone();
    nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::ChatSent { id, .. } if *id == want),
            Duration::from_secs(5),
        )
        .await
        .expect("chat must deliver promptly while the transfer streams");
    let chat_latency = started.elapsed();

    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, Duration::from_secs(300))
        .await
        .expect("the transfer still completes");
    let transfer_elapsed = started.elapsed();

    eprintln!(
        "📊 chat under load: delivered in {chat_latency:?}; 512 MB transfer took {transfer_elapsed:?}"
    );
    assert!(
        chat_latency < Duration::from_secs(5),
        "chat took {chat_latency:?} — head-of-line blocked by the transfer"
    );
    assert!(
        transfer_elapsed > chat_latency,
        "transfer finished before the chat — the probe never overlapped the transfer"
    );

    // And the transfer landed intact (streamed verify; this file is large).
    use std::io::Read;
    let mut file = std::fs::File::open(&local_path).expect("open downloaded file");
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 4 * 1024 * 1024];
    loop {
        let n = file.read(&mut buf).expect("read downloaded slab");
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    assert_eq!(hasher.finalize().to_hex().to_string(), hash, "landed bytes intact");
}

/// G8 hardening: verification is INCREMENTAL — the receiver hashes chunks as they land
/// (blake3 streaming) and the completion check is a hasher finalize, never a re-read of
/// the landed file. Oracles: the engine's own byte counters — every payload byte was
/// hashed in-loop (`file_verify_streamed_bytes` delta == file length) and ZERO bytes were
/// read back after the last chunk (`file_verify_tail_read_bytes` delta == 0) — plus the
/// usual blake3-intact check of the landed bytes.
#[tokio::test]
async fn verify_is_incremental_no_tail_read() {
    use cyan_backend::metrics;

    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let len = 8 * 1024 * 1024 + 37; // 8 MB, not chunk-aligned
    let content = make_content(len, 0xD1);
    let file_id = format!("file-incr-{}", &group[16..32]);
    let hash = stage_file(&file_id, &group, None, None, &content, &nodes[0].node_id);

    let streamed_before = metrics::file_verify_streamed_bytes();
    let prefix_before = metrics::file_verify_prefix_read_bytes();
    let tail_before = metrics::file_verify_tail_read_bytes();

    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, SYNC_TIMEOUT)
        .await
        .expect("peer reports FileDownloaded for the incremental-verify probe");

    // The transfer completed AND verified (the engine only emits FileDownloaded after the
    // hash gate) — now prove HOW it verified.
    let streamed = metrics::file_verify_streamed_bytes() - streamed_before;
    let prefix = metrics::file_verify_prefix_read_bytes() - prefix_before;
    let tail = metrics::file_verify_tail_read_bytes() - tail_before;
    assert_eq!(
        streamed, len as u64,
        "every payload byte must be hashed in-loop as it lands"
    );
    assert_eq!(prefix, 0, "a fresh (non-resume) download re-reads no prefix");
    assert_eq!(tail, 0, "zero verification reads after the last chunk");

    let got = std::fs::read(&local_path).expect("read downloaded file");
    assert_eq!(got.len(), len, "byte length matches");
    assert_eq!(
        blake3::hash(&got).to_hex().to_string(),
        hash,
        "landed bytes blake3-match the source"
    );
}

/// G8 hardening: a multi-GB transfer is RAM-FLAT — no path (send, receive, verify) ever
/// holds the whole file in memory. 2 GB generated file in CI; the full 20 GB behind
/// `CYAN_BIGFILE=1`. Oracle: this process's PEAK RSS delta across the transfer stays under
/// 256 MB (rusage), plus the landed bytes Blake3-verify via a STREAMED read.
#[tokio::test]
async fn twenty_gb_transfer_stays_ram_flat() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let big = std::env::var("CYAN_BIGFILE").ok().as_deref() == Some("1");
    let len: u64 = if big { 20 * 1024 * 1024 * 1024 } else { 2 * 1024 * 1024 * 1024 };
    let deadline = Duration::from_secs(if big { 3600 } else { 900 });

    // Fixture is itself RAM-flat: generated + hashed in 4 MiB slabs straight to disk.
    let file_id = format!("file-ramflat-{}", &group[16..32]);
    let hash = stage_file_streamed(&file_id, &group, len, 0x5A, &nodes[0].node_id);

    let rss_before = cyan_backend::util::peak_rss_bytes();
    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, deadline)
        .await
        .expect("peer reports FileDownloaded for the RAM-flat probe");
    let rss_after = cyan_backend::util::peak_rss_bytes();

    let delta = rss_after.saturating_sub(rss_before);
    eprintln!(
        "📊 RAM-flat probe: {} GB transferred, peak-RSS delta {} MB (before {} MB, after {} MB)",
        len / (1024 * 1024 * 1024),
        delta / (1024 * 1024),
        rss_before / (1024 * 1024),
        rss_after / (1024 * 1024),
    );
    const RSS_CEILING: u64 = 256 * 1024 * 1024;
    assert!(
        delta < RSS_CEILING,
        "peak RSS grew {} MB during a {} GB transfer — some path materialized the file",
        delta / (1024 * 1024),
        len / (1024 * 1024 * 1024),
    );

    // Verify the landed bytes with a STREAMED read (a whole-file read here would defeat
    // the point of the probe).
    use std::io::Read;
    let mut file = std::fs::File::open(&local_path).expect("open downloaded file");
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 4 * 1024 * 1024];
    let mut got_len = 0u64;
    loop {
        let n = file.read(&mut buf).expect("read downloaded slab");
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        got_len += n as u64;
    }
    assert_eq!(got_len, len, "downloaded byte length matches");
    assert_eq!(
        hasher.finalize().to_hex().to_string(),
        hash,
        "downloaded bytes blake3-match the source"
    );
}

/// G10 hardening: an INTERRUPTED direct transfer resumes from the bytes already on disk
/// instead of re-pulling from zero. Fixture = the exact state a mid-transfer death leaves
/// behind: a tmp file holding the stream prefix + an `in_progress` transfers row naming
/// it. Oracles: the engine's own byte counters — the prefix was RE-HASHED from disk
/// (`prefix delta == prefix len`) while only the REMAINDER was streamed off the wire
/// (`streamed delta == len - prefix`) — and the landed bytes blake3-verify intact.
#[tokio::test]
async fn partial_direct_transfer_resumes_from_tmp_prefix() {
    use cyan_backend::metrics;
    use cyan_backend::storage;

    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let len = 6 * 1024 * 1024 + 11; // 6 MB (< the parallel cutoff → single-stream resume path)
    let prefix_len = 2 * 1024 * 1024 + 5;
    let content = make_content(len, 0x9C);
    let file_id = format!("file-resume-{}", &group[16..32]);
    let hash = stage_file(&file_id, &group, None, None, &content, &nodes[0].node_id);

    // The wreckage of an interrupted transfer on the downloader: the landed prefix in
    // the tmp file + the in_progress transfers row pointing at it.
    let downloads = cyan_backend::DATA_DIR
        .get()
        .cloned()
        .expect("harness sets DATA_DIR")
        .join("downloads");
    std::fs::create_dir_all(&downloads).expect("create downloads dir");
    let tmp = downloads.join(format!("{file_id}.tmp"));
    std::fs::write(&tmp, &content[..prefix_len]).expect("write interrupted prefix");
    storage::transfer_upsert(
        &file_id,
        &format!("{file_id}.bin"),
        len as u64,
        &hash,
        prefix_len as u64,
        tmp.to_string_lossy().as_ref(),
        &nodes[0].node_id,
        "in_progress",
    )
    .expect("seed interrupted transfer row");

    let streamed_before = metrics::file_verify_streamed_bytes();
    let prefix_before = metrics::file_verify_prefix_read_bytes();

    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let local_path = nodes[1]
        .wait_file_downloaded(&file_id, SYNC_TIMEOUT)
        .await
        .expect("peer reports FileDownloaded for the resumed transfer");

    let streamed = metrics::file_verify_streamed_bytes() - streamed_before;
    let prefix = metrics::file_verify_prefix_read_bytes() - prefix_before;
    assert_eq!(
        prefix, prefix_len as u64,
        "the landed prefix must be re-hashed from disk, not re-transferred"
    );
    assert_eq!(
        streamed,
        (len - prefix_len) as u64,
        "only the remainder must come over the wire"
    );

    let got = std::fs::read(&local_path).expect("read resumed file");
    assert_eq!(got.len(), len, "byte length matches");
    assert_eq!(
        blake3::hash(&got).to_hex().to_string(),
        hash,
        "resumed bytes blake3-match the source"
    );
}

/// Hardening (2026-07-05 disk-full gate incident): a transfer that CANNOT complete must
/// surface as a prompt, terminal `FileDownloadFailed` — never a silent stall the caller
/// can only time out on. Fixture: the host KNOWS the file (synced row) but holds no
/// bytes (`local_path` empty), so every transfer path answers NotFound — size ≥ the
/// parallel cutoff so the striped path runs first, errors, falls back to single-stream,
/// and errors again. Oracles: `wait_file_downloaded` returns the engine's terminal
/// failure (not the harness timeout) and it carries the engine's reason.
#[tokio::test]
async fn failed_transfer_surfaces_failure_event_promptly() {
    use cyan_backend::storage;

    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let len: u64 = 16 * 1024 * 1024;
    let hash = blake3::hash(b"ghost").to_hex().to_string();
    let file_id = format!("file-ghost-{}", &group[16..32]);
    let _ = storage::file_insert_simple(
        &file_id,
        Some(&group),
        None,
        None,
        "ghost.bin",
        &hash,
        len,
        Some(&nodes[0].node_id),
        1,
    );

    nodes[1].request_download(&file_id, &hash, &nodes[0].node_id);
    let err = nodes[1]
        .wait_file_downloaded(&file_id, SYNC_TIMEOUT)
        .await
        .expect_err("a transfer with no bytes to serve must fail, not succeed");
    let msg = err.to_string();
    assert!(
        msg.contains("FileDownloadFailed"),
        "must fail via the engine's terminal event, not the harness timeout: {msg}"
    );
    assert!(
        msg.contains("not found on peer"),
        "the terminal event must carry the engine's reason: {msg}"
    );
}

/// Harness hygiene (2026-07-05 gate incident): per-run data dirs are pid-tagged and
/// dirs from DEAD runs are swept at init — the suite must not leak its multi-GB staged
/// fixtures across runs (~1.8k leaked dirs filled the disk, the engine's disk preflight
/// then rightly refused the large transfers, and the tests burned 120s timeouts).
/// Oracles: a dir named for a dead pid is reclaimed; one named for THIS live process
/// survives.
#[tokio::test]
async fn stale_harness_dirs_are_swept() {
    let parent = harness_tmp_parent();
    std::fs::create_dir_all(&parent).expect("create harness parent");

    // A pid that is REALLY dead: spawn a trivial child and reap it.
    let mut child = std::process::Command::new("/usr/bin/true")
        .spawn()
        .expect("spawn /usr/bin/true");
    let dead_pid = child.id();
    child.wait().expect("reap child");

    let dead_dir = parent.join(format!("pid-{dead_pid}-sweeptest"));
    let live_dir = parent.join(format!("pid-{}-sweeptest", std::process::id()));
    std::fs::create_dir_all(&dead_dir).expect("create dead-pid dir");
    std::fs::write(dead_dir.join("marker"), b"stale fixture").expect("write marker");
    std::fs::create_dir_all(&live_dir).expect("create live-pid dir");

    sweep_dead_harness_dirs(&parent);

    assert!(!dead_dir.exists(), "a dead run's dir must be reclaimed");
    assert!(live_dir.exists(), "a live process's dir must survive the sweep");
    std::fs::remove_dir_all(&live_dir).expect("tidy this test's live fixture");
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
