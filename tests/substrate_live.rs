//! Substrate LIVE harness orchestrator (multi-process, macos/loopback tier) — ROUND 8.
//!
//! The honest engine behind `harness/live.sh`. It is NOT part of the default `cargo test`
//! matrix: it runs ONLY when `CYAN_LIVE=1` (set by `live.sh`), spins up N real `cyan_node`
//! OS processes (each its OWN auto-generated identity + OWN SQLite DB — NO login, NO SSO),
//! joins them all to ONE shared group, then drives a chosen scenario where EVERY peer acts
//! and ALL peers must converge. Every assertion is on each peer's OWN `storage::*` counts /
//! OWN blob verify — never on log lines. Every wait is a bounded `tokio::time::timeout`.
//!
//! Toggles (env, set by `live.sh`):
//!   CYAN_LIVE          must be "1" to run at all (otherwise this test returns immediately).
//!   CYAN_LIVE_N        peer count (host + joiners), default 8, min 2.
//!   CYAN_LIVE_SCENARIO sync | files | chat | workflow | all   (default all).
//!   CYAN_LIVE_NET      home | offline   (informational here; both run relay-disabled loopback
//!                      — the macos tier IS the offline/LAN rung. corp / real relay-or-WS is the
//!                      Docker rig's job, routed there by live.sh. See STATUS_ROUND8_HARNESS.md).
//!
//! Output: machine-readable, one line per (scenario, peer):
//!   @@LIVE@@ scenario=<s> peer=<name> result=PASS|FAIL detail=<...>
//! and a final `@@LIVE@@ verdict=PASS|FAIL ...`. `live.sh` parses these into a table + exit code;
//! the test itself also asserts the verdict so the cargo exit code is meaningful on its own.
//!
//! iroh 0.95. Test-only + additive. DO NOT weaken assertions.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::{Duration, Instant};

use anyhow::Result;
use multiprocess::{wire_mesh, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

// Fixture base counts the host seeds before boot (mirrors `cyan_node::seed_fixture`). Joiners
// pull these via snapshot on join, so every scenario's "expected" is base + the live activity.
const FIXTURE_ELEMENTS: usize = 5;
const FIXTURE_CHATS: usize = 3;
const FIXTURE_CELLS: usize = 3;

// Per-scenario live activity knobs (kept small so a live demo converges in seconds).
const EDITS_PER_PEER: usize = 5; // sync:     each peer adds this many whiteboard objects
const CHATS_PER_PEER: usize = 4; // chat:     each peer sends this many board-chat messages
const WF_STEPS: usize = 4; // workflow: the author lays out this many steps
const BLOB_SIZE: usize = 64 * 1024; // files:    per-peer upload size

fn live_n() -> usize {
    std::env::var("CYAN_LIVE_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n >= 2)
        .unwrap_or(8)
}

fn scenario() -> String {
    std::env::var("CYAN_LIVE_SCENARIO").unwrap_or_else(|_| "all".to_string())
}

fn net() -> String {
    std::env::var("CYAN_LIVE_NET").unwrap_or_else(|_| "home".to_string())
}

/// Generous, N-scaled bound for N processes to spawn + form a gossip mesh + converge.
fn converge_timeout(n: usize) -> Duration {
    Duration::from_secs(45 + (n as u64) * 6)
}

/// Spawn `n` peers (index 0 = host, seeds the fixture before boot so it hosts the group topic),
/// full-mesh wire them over loopback, and have every joiner cold-join + sync — one at a time so a
/// late joiner can pull from peers that already synced (avoids the single-host thundering herd).
/// Identical to the proven `substrate_stress::form_group` pattern: NO anti-entropy sweep (every
/// live scenario converges over broadcasts, and a fast sweep across a fresh N-node mesh floods the
/// host's snapshot path and starves the first joiner). Bounded waits throughout.
///
/// Returns the formed peers AND the count that successfully synced. A joiner that does not sync in
/// 2× the bound is NOT fatal here — it is reported as a per-peer `join` FAIL by the caller (a
/// legible table beats a panic with an empty one); the run's verdict still fails on it.
async fn form_group(n: usize, key: &str, group: &str) -> Result<(Vec<MpNode>, usize)> {
    let host = MpNode::spawn("host", key, None, Some(group)).await?;
    let host_id = host.node_id.clone();
    let mut nodes = vec![host];
    for i in 1..n {
        let joiner = MpNode::spawn(&format!("peer{i}"), key, Some(&host_id), None).await?;
        nodes.push(joiner);
    }
    wire_mesh(&mut nodes).await?;

    // host counts as synced (it authored the fixture). Joiners cold-join + wait, one at a time.
    let t = converge_timeout(n);
    let mut synced = 1usize;
    for node in nodes.iter_mut().skip(1) {
        let mut ok = false;
        for _ in 0..2 {
            node.join_group(group, Some(&host_id)).await?;
            if node.wait_sync(group, t).await? {
                ok = true;
                break;
            }
        }
        emit("join", &node.name.clone(), ok, if ok { "synced" } else { "no-snapshot-in-bound" });
        if ok {
            synced += 1;
        }
    }
    emit("join", "host", true, "fixture-host");
    Ok((nodes, synced))
}

/// One machine-readable result line per (scenario, peer) — `live.sh` greps these.
fn emit(scenario: &str, peer: &str, pass: bool, detail: &str) {
    println!(
        "@@LIVE@@ scenario={scenario} peer={peer} result={} detail={detail}",
        if pass { "PASS" } else { "FAIL" }
    );
}

/// Poll until EVERY peer's `count kind group` reaches `expected` (or the bound elapses), then emit
/// a PASS/FAIL line per peer from its FINAL count. Convergence to the EXACT count is the
/// no-dupes / no-loss oracle (id-keyed storage makes a silent over-count impossible). Returns true
/// iff every peer converged.
async fn converge_count_report(
    nodes: &mut [MpNode],
    scenario: &str,
    kind: &str,
    group: &str,
    expected: usize,
    timeout: Duration,
) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all = true;
        for node in nodes.iter_mut() {
            if node.count(kind, group).await? != expected {
                all = false;
                break;
            }
        }
        if all || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Final per-peer verdict from each peer's OWN storage.
    let mut overall = true;
    for node in nodes.iter_mut() {
        let c = node.count(kind, group).await?;
        let pass = c == expected;
        overall &= pass;
        emit(scenario, &node.name.clone(), pass, &format!("{kind}={c}/{expected}"));
    }
    Ok(overall)
}

// ── Scenarios ────────────────────────────────────────────────────────────────────────────

/// sync: every peer creates `EDITS_PER_PEER` whiteboard objects (live `WhiteboardElementAdded`
/// broadcast). Assert every peer's element count == fixture + N*EDITS_PER_PEER.
async fn run_sync(nodes: &mut [MpNode], group: &str) -> Result<bool> {
    let n = nodes.len();
    for node in nodes.iter_mut() {
        node.post_edits(group, EDITS_PER_PEER).await?;
    }
    let expected = FIXTURE_ELEMENTS + n * EDITS_PER_PEER;
    converge_count_report(nodes, "sync", "elements", group, expected, converge_timeout(n)).await
}

/// chat: every peer sends `CHATS_PER_PEER` board-chat messages (live `ChatSent` broadcast).
/// Assert every peer's chat count == fixture + N*CHATS_PER_PEER (ordering-independent; dedupe by id).
async fn run_chat(nodes: &mut [MpNode], group: &str) -> Result<bool> {
    let n = nodes.len();
    for node in nodes.iter_mut() {
        node.post_chat(group, CHATS_PER_PEER).await?;
    }
    let expected = FIXTURE_CHATS + n * CHATS_PER_PEER;
    converge_count_report(nodes, "chat", "chats", group, expected, converge_timeout(n)).await
}

/// workflow: the host authors + lays out a local-placement workflow — a board + `WF_STEPS` step
/// cells + a pinned-gate, broadcast over the mesh. Assert every peer sees the steps (cells ==
/// fixture + WF_STEPS) AND the pinned gate (pins == 1). Execution/placement is local/MCP and out
/// of substrate scope — the mesh carries the authoring, which is what peers must converge on.
async fn run_workflow(nodes: &mut [MpNode], group: &str) -> Result<bool> {
    let n = nodes.len();
    let board = nodes[0].post_workflow(group, WF_STEPS).await?;
    println!("@@LIVE@@ info workflow_board={board} steps={WF_STEPS} author=host");
    let cells_ok = converge_count_report(
        nodes,
        "workflow",
        "cells",
        group,
        FIXTURE_CELLS + WF_STEPS,
        converge_timeout(n),
    )
    .await?;
    let pins_ok =
        converge_count_report(nodes, "workflow", "pins", group, 1, converge_timeout(n)).await?;
    Ok(cells_ok && pins_ok)
}

/// files: every peer uploads one blob (hold + announce). Then every peer fetches every OTHER
/// peer's blob and INDEPENDENTLY re-verifies its blake3 — proving "all peers receive + can read
/// the files". Per-peer PASS = fetched-and-verified all N-1 others.
async fn run_files(nodes: &mut [MpNode], group: &str) -> Result<bool> {
    let n = nodes.len();
    // 1) each peer seeds one blob; collect (owner_index, owner_node_id, file_id, hash).
    let mut seeds: Vec<(usize, String, String, String)> = Vec::with_capacity(n);
    for (i, node) in nodes.iter_mut().enumerate() {
        let owner_id = node.node_id.clone();
        let (file_id, hash) = node.seed_blob(group, BLOB_SIZE).await?;
        seeds.push((i, owner_id, file_id, hash));
    }

    // 2) every peer fetches + verifies every OTHER peer's blob.
    let t = converge_timeout(n);
    let mut overall = true;
    for (i, node) in nodes.iter_mut().enumerate() {
        let mut got = 0usize;
        let mut want = 0usize;
        for (owner, owner_id, file_id, hash) in &seeds {
            if *owner == i {
                continue; // own blob is already local
            }
            want += 1;
            let fetched = node
                .fetch_blob(group, file_id, hash, owner_id, BLOB_SIZE as u64, t)
                .await?;
            if fetched.is_some() && node.verify_blob(file_id, hash).await? {
                got += 1;
            }
        }
        let pass = got == want;
        overall &= pass;
        emit("files", &node.name.clone(), pass, &format!("verified={got}/{want}"));
    }
    Ok(overall)
}

/// Quit AND fully reap every peer before returning — un-reaped `cyan_node` processes would
/// otherwise linger. (`live.sh`'s macos tier owns these as test children; there is no `--keep`
/// here — see STATUS_ROUND8_HARNESS.md for why `--keep` is the Docker tier's knob.)
async fn quit_all(nodes: Vec<MpNode>) {
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn live_run() {
    // Gated: this is the heavy N-process live harness, driven by `harness/live.sh`. A plain
    // `cargo test` (no CYAN_LIVE) returns here instantly so the default matrix stays light.
    if std::env::var("CYAN_LIVE").as_deref() != Ok("1") {
        eprintln!("substrate_live::live_run — set CYAN_LIVE=1 (via harness/live.sh) to run; skipping.");
        return;
    }

    let _serial = serial().await;
    let n = live_n();
    let scenario = scenario();
    let net = net();
    let key = unique_discovery_key();
    let group = unique_group_id();

    eprintln!("[live] N={n} scenario={scenario} net={net} group={group}");
    println!("@@LIVE@@ info n={n} scenario={scenario} net={net}");

    let (mut nodes, synced) = form_group(n, &key, &group)
        .await
        .expect("spawn + wire N peers");

    // If any peer failed to pull the snapshot, the group never fully formed — report it as a FAIL
    // verdict (the per-peer `join` rows above show which peer stalled) and stop before the
    // scenarios, which assume every peer is a synced group member. This is the single-box scale
    // ceiling, not a faked pass: prefer a smaller --peers for a quick demo (see STATUS doc).
    if synced != n {
        println!("@@LIVE@@ verdict=FAIL n={n} scenario={scenario} net={net} reason=only-{synced}-of-{n}-synced");
        quit_all(nodes).await;
        panic!("only {synced}/{n} peers synced the group within bound — lower --peers or see STATUS_ROUND8_HARNESS.md");
    }

    // For net=offline, prove the topology never used a relay (relay is disabled in this tier).
    // 'none'/'unknown' pre-classification is fine; 'relay'/'mixed' would violate offline intent.
    if net == "offline" {
        let host_to: Vec<String> = nodes.iter().skip(1).map(|p| p.node_id.clone()).collect();
        for jid in &host_to {
            let tier = nodes[0].tier(jid).await.expect("query tier");
            assert!(
                tier != "relay" && tier != "mixed",
                "offline/LAN rung must never use a relay tier, got '{tier}'"
            );
        }
        println!("@@LIVE@@ info offline_proof=relay-disabled,direct-only");
    }

    let run_all = scenario == "all";
    let mut overall = true;
    if run_all || scenario == "sync" {
        overall &= run_sync(&mut nodes, &group).await.expect("sync scenario");
    }
    if run_all || scenario == "chat" {
        overall &= run_chat(&mut nodes, &group).await.expect("chat scenario");
    }
    if run_all || scenario == "files" {
        overall &= run_files(&mut nodes, &group).await.expect("files scenario");
    }
    if run_all || scenario == "workflow" {
        overall &= run_workflow(&mut nodes, &group).await.expect("workflow scenario");
    }

    println!(
        "@@LIVE@@ verdict={} n={n} scenario={scenario} net={net}",
        if overall { "PASS" } else { "FAIL" }
    );

    quit_all(nodes).await;

    assert!(overall, "one or more peers failed to converge — see the @@LIVE@@ FAIL lines above");
}
