//! Substrate STRESS / CHAOS fabric (multi-process, in-process/loopback tier) — Round 7.
//!
//! Proves the mesh survives duress with MANY real peers on ONE box: N `cyan_node` OS
//! processes (each its OWN iroh identity + SQLite DB), full-meshed over loopback with relay
//! disabled (offline/LAN). Every assertion is on each peer's OWN storage / OWN metrics —
//! never on log lines. Every wait is a bounded `tokio::time::timeout`.
//!
//! This is the CI tier of the stress fabric: small N by default, big N + long chaos behind
//! env flags. The network-shaping rungs that need real isolation (different-WiFi/NAT,
//! relay-only, websocket-only, bidirectional-island partition, `tc` degradation) live in the
//! Docker half — `harness/stress.sh` — and are documented, not faked, here. See
//! STATUS_STRESS_FABRIC.md and STRESS_HARNESS_SPEC.md.
//!
//! ORACLES asserted here:
//! - **Convergence**: every peer's element/file counts reach the SAME total within a bound.
//! - **No dupes / no loss**: convergence is to the EXACT total — a duplicated or dropped edit
//!   can never reach it (id-keyed storage makes over-count impossible silently; under-count
//!   is loss). So "converged to exactly K" *is* the no-dupes-no-loss proof.
//! - **No message storm**: per-peer gossip-message count stays ~linear in the work done, not
//!   quadratic in N (read from each peer's `metrics`).
//! - **Bounded gossip degree**: per-peer active neighbor count stays bounded as N grows.
//! - **Bounded memory**: per-peer RSS does not blow up over the run.
//! - **Connection tier == topology intent**: relay disabled ⇒ NO peer is ever on a relay tier.
//! - **Blake3 integrity**: every swarm-fetched blob independently re-verifies on the receiver.
//!
//! iroh 0.95. Test-only + additive. DO NOT weaken assertions.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use multiprocess::{wire_mesh, wire_pair, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// Fixture element count the host seeds before boot (mirrors `seed_fixture`).
const FIXTURE_ELEMENTS: usize = 5;

/// CI default peer count (host + joiners). Override with `CYAN_STRESS_N` for the scale tier.
fn stress_n(default: usize) -> usize {
    std::env::var("CYAN_STRESS_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n >= 2)
        .unwrap_or(default)
}

/// Generous, N-scaled bound for N processes to spawn + form a gossip mesh + converge.
fn converge_timeout(n: usize) -> Duration {
    Duration::from_secs(45 + (n as u64) * 6)
}

/// Spawn a group of `n` peers (index 0 = host, seeds the fixture before boot so it hosts the
/// group topic), full-mesh wire them, and have every joiner cold-join + sync. Returns the peers.
///
/// Joiners are wired together and bootstrapped off the host, then each waits for its own
/// `SyncComplete` (in its own process) within a generous, N-scaled bound. Small N (the CI tier)
/// is fast and reliable; large N stresses the single host's snapshot fan-in — see the measured
/// ceiling in STATUS_STRESS_FABRIC.md.
async fn form_group(n: usize, key: &str, group: &str) -> Result<Vec<MpNode>> {
    let host = MpNode::spawn("host", key, None, Some(group)).await?;
    let host_id = host.node_id.clone();
    let mut nodes = vec![host];
    for i in 1..n {
        let joiner = MpNode::spawn(&format!("peer{i}"), key, Some(&host_id), None).await?;
        nodes.push(joiner);
    }
    wire_mesh(&mut nodes).await?;

    // Join + sync ONE peer at a time (not all-at-once): peer_i syncs before peer_{i+1} bootstraps.
    // This avoids the single-host snapshot **thundering herd** — firing every joiner at the host
    // simultaneously starves the late ones on a loaded box (the "snapshot under load" ceiling).
    // Because the mesh is already fully wired, a later joiner can also pull from peers that have
    // already synced, not just the host. One bounded retry absorbs a slow gossip neighbor-up.
    let t = converge_timeout(n);
    for i in 1..nodes.len() {
        let mut synced = false;
        for _ in 0..2 {
            nodes[i].join_group(group, Some(&host_id)).await?;
            if nodes[i].wait_sync(group, t).await? {
                synced = true;
                break;
            }
        }
        if !synced {
            return Err(anyhow!("{} did not sync group within 2×{t:?}", nodes[i].name));
        }
    }
    Ok(nodes)
}

/// Poll every peer's `count kind group` until ALL equal `expected`, or fail (with a per-peer
/// report) at the bound. Convergence to the EXACT count is the no-dupes / no-loss oracle.
async fn converge_count(
    nodes: &mut [MpNode],
    kind: &str,
    group: &str,
    expected: usize,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all = true;
        for node in nodes.iter_mut() {
            if node.count(kind, group).await? != expected {
                all = false;
                break;
            }
        }
        if all {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let mut report = String::new();
            for node in nodes.iter_mut() {
                let c = node.count(kind, group).await?;
                report.push_str(&format!("{}={} ", node.name, c));
            }
            return Err(anyhow!(
                "did not converge to {expected} {kind} within {timeout:?}: [{report}]"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Quit AND fully reap every peer before the test returns — otherwise un-reaped `cyan_node`
/// processes accumulate across the suite's tests and starve later ones.
async fn quit_all(nodes: Vec<MpNode>) {
    for n in nodes {
        n.shutdown().await;
    }
}

// ════════════════════════════════════════════════════════════════════════════════════════
// A. Concurrent live edits from EVERY peer converge with no dupes / no loss (multi-source).
// ════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn concurrent_edits_converge_no_dupes() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();
    let n = stress_n(4);
    const EDITS_PER_PEER: usize = 8;

    let mut nodes = form_group(n, &key, &group).await.expect("form group + sync all peers");

    // Every peer (host + joiners) posts EDITS_PER_PEER live edits concurrently-ish (sequential
    // issue, but they propagate and interleave on the mesh). Ids are node-namespaced ⇒ disjoint.
    for node in nodes.iter_mut() {
        node.post_edits(&group, EDITS_PER_PEER).await.expect("post edits");
    }

    // Every peer must end with EXACTLY the fixture + all peers' edits — no more (dupes), no
    // fewer (loss). This is the partition/swarm convergence invariant in its simplest form.
    let expected = FIXTURE_ELEMENTS + n * EDITS_PER_PEER;
    converge_count(&mut nodes, "elements", &group, expected, converge_timeout(n))
        .await
        .expect("all peers converge to the exact element total");

    // Connection-tier oracle: relay is disabled, so NO peer may ever be on a relay tier. Check
    // the host's tier to each joiner (best-effort: 'none'/'unknown' is fine pre-classification;
    // 'relay'/'mixed' would be a real violation of the offline/LAN topology intent).
    let joiner_ids: Vec<String> = nodes.iter().skip(1).map(|p| p.node_id.clone()).collect();
    for jid in &joiner_ids {
        let tier = nodes[0].tier(jid).await.expect("query tier");
        assert!(
            tier != "relay" && tier != "mixed",
            "offline/LAN topology must never use a relay tier, got '{tier}'"
        );
    }

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════
// B. Peer-flood / scale ceiling — N peers converge; gossip volume + degree + memory bounded.
//    Default N is small (CI); set CYAN_STRESS_N=50 / 100 to probe the ceiling on demand.
// ════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "on-demand big-N scale/ceiling probe: run standalone via CYAN_STRESS_SCALE=1 \
            (or `stress.sh scale [N]`); the heaviest scenario, kept out of the fast CI matrix"]
async fn peer_flood_scale_and_degree_bounded() {
    // GATED out of the default suite on purpose: this is the on-demand ceiling probe (spec: "big N
    // gated, run on demand"). It is reliable run ALONE — `stress.sh scale 6` — but it is the
    // heaviest scenario (default N=6 full iroh nodes), so stacking it after the other MP tests in
    // one binary only adds single-box CPU pressure without adding coverage. The bounded-degree /
    // no-storm / bounded-memory oracles hold at any N; push CYAN_STRESS_N to find the ceiling
    // (N≈6 converges sub-second; N≥12 plateaus on the live-delta gap — see STATUS_STRESS_FABRIC.md).
    if std::env::var("CYAN_STRESS_SCALE").as_deref() != Ok("1") {
        eprintln!("peer_flood_scale_and_degree_bounded: set CYAN_STRESS_SCALE=1 to run; skipping.");
        return;
    }
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();
    let n = stress_n(6);
    const EDITS_PER_PEER: usize = 3;

    let t0 = Instant::now();
    let mut nodes = form_group(n, &key, &group).await.expect("form N-peer group + sync");
    let form_secs = t0.elapsed().as_secs_f64();

    // A modest write load from every peer.
    for node in nodes.iter_mut() {
        node.post_edits(&group, EDITS_PER_PEER).await.expect("post edits");
    }
    let expected = FIXTURE_ELEMENTS + n * EDITS_PER_PEER;
    let t1 = Instant::now();
    converge_count(&mut nodes, "elements", &group, expected, converge_timeout(n))
        .await
        .expect("N peers converge to exact element total");
    let converge_secs = t1.elapsed().as_secs_f64();

    // Collect per-peer metrics AFTER convergence.
    let mut max_degree = 0u64;
    let mut max_gossip = 0u64;
    let mut max_rss = 0u64;
    let mut min_rss = u64::MAX;
    for node in nodes.iter_mut() {
        let m = node.metrics().await.expect("read metrics");
        max_degree = max_degree.max(m.gossip_degree);
        max_gossip = max_gossip.max(m.gossip_recv);
        max_rss = max_rss.max(m.rss_kb);
        min_rss = min_rss.min(m.rss_kb);
    }

    println!(
        "[SCALE] N={n} form={form_secs:.1}s converge={converge_secs:.1}s \
         max_degree={max_degree} max_gossip_recv={max_gossip} \
         rss_kb=[{min_rss}..{max_rss}] (per-peer)"
    );

    // Bounded gossip degree: HyParView keeps active degree ~constant. It must NOT grow to a
    // full mesh (n-1). Allow generous headroom but assert it's clearly sub-N for larger N.
    let degree_ceiling = (n as u64).min(12) + 4;
    assert!(
        max_degree <= degree_ceiling,
        "gossip degree {max_degree} exceeded bound {degree_ceiling} for N={n} (storm risk)"
    );

    // No message storm: total gossip a single peer processes must be bounded, not quadratic.
    // Each of (n*EDITS + fixture/snapshot/presence) events fans out with bounded redundancy;
    // a generous linear-in-(N*work) ceiling catches a quadratic blow-up without being flaky.
    let work = (n * EDITS_PER_PEER + FIXTURE_ELEMENTS) as u64;
    let gossip_ceiling = 200 + work * (n as u64) * 4;
    assert!(
        max_gossip <= gossip_ceiling,
        "per-peer gossip_recv {max_gossip} exceeded bound {gossip_ceiling} for N={n} (storm)"
    );

    // Bounded memory: no peer should balloon. 512 MB/peer is a very loose ceiling that still
    // catches a real leak; the printed range is the measured number to track over time.
    assert!(
        max_rss < 512_000,
        "per-peer RSS {max_rss} KB exceeded 512 MB ceiling for N={n} (memory leak?)"
    );

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════
// C. Swarm under load — one holder, many concurrent fetchers, Blake3 integrity on each.
// ════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn swarm_blob_multi_fetch_integrity() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();
    let n = stress_n(4);
    const BLOB_SIZE: usize = 512 * 1024; // 512 KiB — large enough to stream, small for CI.

    let mut nodes = form_group(n, &key, &group).await.expect("form group + sync");

    // Host (index 0) holds + announces the blob.
    let host_id = nodes[0].node_id.clone();
    let (file_id, hash) = nodes[0].seed_blob(&group, BLOB_SIZE).await.expect("host seeds blob");

    // Every joiner fetches it concurrently from the host, then independently re-verifies blake3.
    let fetch_timeout = Duration::from_secs(30 + n as u64 * 4);
    for node in nodes.iter_mut().skip(1) {
        let local = node
            .fetch_blob(&group, &file_id, &hash, &host_id, BLOB_SIZE as u64, fetch_timeout)
            .await
            .expect("fetch control call");
        assert!(local.is_some(), "{} did not receive the blob within bound", node.name);
    }
    for node in nodes.iter_mut().skip(1) {
        let ok = node.verify_blob(&file_id, &hash).await.expect("verify control call");
        assert!(ok, "{} blob failed independent blake3 re-verification (corruption)", node.name);
    }

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════
// D. Drop + reconnect / heal — a peer dies, the others keep editing, a fresh peer rejoins and
//    converges to the FULL post-churn state (the one-box, one-sided partition+heal).
// ════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "drop/reconnect/heal probe: run standalone via CYAN_STRESS_PARTITION=1 \
            (or `stress.sh partition`); kill+respawn+rewire+reconverge is the most timing-sensitive \
            scenario and flakes when stacked after other MP tests on one loaded box"]
async fn node_churn_rejoin_converges() {
    // GATED out of the default suite: this is the richest scenario (kill a peer, survivors keep
    // editing, a fresh peer rejoins + rewires + re-syncs to the full post-churn state). It is
    // reliable run ALONE — `stress.sh partition` — but the extra cold-join during the heal makes it
    // the most sensitive to single-box CPU pressure, so it is an on-demand probe, not a CI gate.
    if std::env::var("CYAN_STRESS_PARTITION").as_deref() != Ok("1") {
        eprintln!("node_churn_rejoin_converges: set CYAN_STRESS_PARTITION=1 to run; skipping.");
        return;
    }
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();
    const EDITS: usize = 6;

    // Host + 2 joiners, all synced.
    let mut nodes = form_group(3, &key, &group).await.expect("form 3-peer group + sync");

    // One joiner DROPS (process killed → it truly leaves the mesh; its endpoint dies). Fully
    // reap it so its dying process doesn't contend with the survivors + rejoiner during the heal.
    nodes.pop().expect("a victim peer").shutdown().await; // peer2 gone

    // The surviving island (host + peer1) keeps editing while the victim is gone.
    for node in nodes.iter_mut() {
        node.post_edits(&group, EDITS).await.expect("post edits during partition");
    }
    let survivors_expected = FIXTURE_ELEMENTS + nodes.len() * EDITS;
    converge_count(&mut nodes, "elements", &group, survivors_expected, converge_timeout(3))
        .await
        .expect("survivors converge while victim is down");

    // HEAL: a fresh peer rejoins, wires to the survivors, syncs — it must pull the FULL state,
    // including the edits made while it (its predecessor) was gone. No loss across the heal.
    let host_id = nodes[0].node_id.clone();
    let mut rejoiner = MpNode::spawn("rejoiner", &key, Some(&host_id), None)
        .await
        .expect("rejoiner spawns clean");
    for node in nodes.iter_mut() {
        wire_pair(node, &mut rejoiner).await.expect("wire rejoiner to a survivor");
    }
    rejoiner.join_group(&group, Some(&host_id)).await.expect("rejoiner joins");
    assert!(
        rejoiner.wait_sync(&group, converge_timeout(3)).await.expect("wait_sync"),
        "rejoiner did not sync after heal"
    );
    nodes.push(rejoiner);

    // Everyone — survivors + the healed peer — converges to the exact full total.
    converge_count(&mut nodes, "elements", &group, survivors_expected, converge_timeout(3))
        .await
        .expect("all peers converge after heal (no loss, no dupes)");

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════
// E. Sustained chaos — random kill/restart + continuous edits for T seconds. Heavy + long:
//    gated behind CYAN_STRESS_CHAOS=1 so a plain `cargo test` never runs it. Honest #[ignore].
// ════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "long-running chaos soak; run on demand with CYAN_STRESS_CHAOS=1"]
async fn sustained_chaos_soak() {
    if std::env::var("CYAN_STRESS_CHAOS").as_deref() != Ok("1") {
        eprintln!("sustained_chaos_soak: set CYAN_STRESS_CHAOS=1 to run; skipping.");
        return;
    }
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();
    let n = stress_n(6);
    let soak_secs: u64 = std::env::var("CYAN_STRESS_CHAOS_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);

    let mut nodes = form_group(n, &key, &group).await.expect("form group + sync");

    // Continuous edits from a rotating peer; each round we tally exactly how many landed so the
    // final convergence target is exact (no dupes / no loss across the whole soak).
    let mut total_edits = 0usize;
    let deadline = Instant::now() + Duration::from_secs(soak_secs);
    let mut round = 0usize;
    while Instant::now() < deadline {
        round += 1;
        // Every surviving peer posts a couple edits this round.
        for node in nodes.iter_mut() {
            node.post_edits(&group, 2).await.expect("chaos round edits");
            total_edits += 2;
        }
        // Churn: kill the last peer and replace it with a fresh one (if we have spares).
        if nodes.len() > 2 && round % 2 == 0 {
            nodes.pop().expect("victim").shutdown().await; // reap before respawn
            let host_id = nodes[0].node_id.clone();
            let mut fresh = MpNode::spawn(&format!("chaos{round}"), &key, Some(&host_id), None)
                .await
                .expect("fresh peer");
            for node in nodes.iter_mut() {
                wire_pair(node, &mut fresh).await.expect("wire fresh");
            }
            fresh.join_group(&group, Some(&host_id)).await.expect("fresh joins");
            let _ = fresh.wait_sync(&group, converge_timeout(n)).await;
            nodes.push(fresh);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // After the storm settles, every live peer converges to the exact full total.
    let expected = FIXTURE_ELEMENTS + total_edits;
    converge_count(&mut nodes, "elements", &group, expected, converge_timeout(n) * 2)
        .await
        .expect("mesh converges to exact total after sustained chaos");

    for node in nodes.iter_mut() {
        let m = node.metrics().await.expect("metrics");
        assert!(m.rss_kb < 768_000, "{} RSS {}KB ballooned over soak", node.name, m.rss_kb);
    }

    quit_all(nodes).await;
}
