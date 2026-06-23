//! Substrate DISTRIBUTED WORKFLOW RUN harness (multi-process, macos/loopback) — ROUND 10.
//!
//! The existing live harness proves sync + authoring-convergence. THIS suite proves a workflow
//! actually RUNS and its run-state propagates across peers: with N loopback peers in ONE group,
//! the host authors + RUNS a local-placement workflow (the real `run_pipeline_with_plan` wave
//! executor), and we assert
//!   1. the run EXECUTES — steps move pending→running→terminal and the dashboard exec events
//!      (`WorkflowRunStarted`/`StepStateChanged`/`StepProgress`/`WorkflowRunFinished`/
//!      `WorkflowStatsUpdated`) fire (asserted on the producer's OWN captured event stream), and
//!   2. the run-state + results CONVERGE on every peer — each peer observes the run via the SAME
//!      cell-update gossip path that carries chat/boards (asserted on each peer's OWN
//!      `notebook_cells` run-state, never a log line).
//!
//! Gate steps (human-approval) stall only their branch (branch barrier); an approval on ONE peer
//! unblocks the run for ALL peers. The wave executor runs independent steps in a wave concurrently.
//!
//! Out of substrate scope (CLAUDE.md): the LOCAL/MCP step *execution* itself. We drive it with the
//! test-only local step the harness already has — a `local` step whose `mcp_tool` names a NON-
//! installed plugin, so it resolves "not installed" and reaches a terminal state FAST + offline.
//! What is asserted is run-state + exec-events propagating, plus wave concurrency — NOT the step's
//! business result (its terminal verdict is deterministically `failed`, and that verdict converges).
//!
//! Gated: runs only under `CYAN_LIVE=1` (driven by `harness/live.sh --scenario workflow-run`); a
//! plain `cargo test` returns instantly. Every wait is a bounded `tokio::time::timeout`/deadline.
//! iroh 0.95. Test-only + additive. DO NOT weaken assertions.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::{Duration, Instant};

use anyhow::Result;
use multiprocess::{wire_mesh, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// Peer count for the run suite. Smaller than the sync harness's default (each test forms its OWN
/// group), overridable via `CYAN_LIVE_N`. Min 2 (a run + at least one observer).
fn wf_n() -> usize {
    std::env::var("CYAN_LIVE_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n >= 2)
        .unwrap_or(3)
}

fn gated_off() -> bool {
    if std::env::var("CYAN_LIVE").as_deref() == Ok("1") {
        return false;
    }
    eprintln!("substrate_workflow_run — set CYAN_LIVE=1 (via harness/live.sh) to run; skipping.");
    true
}

/// N-scaled bound for N processes to form a mesh + a run to execute + converge.
fn converge_timeout(n: usize) -> Duration {
    Duration::from_secs(45 + (n as u64) * 6)
}

/// Spawn `n` peers (index 0 = host, seeds the fixture so it hosts the group topic), full-mesh wire
/// them over loopback, and have every joiner cold-join + sync — one at a time. Mirrors the proven
/// `substrate_live::form_group` pattern. Panics with a clear message if any peer fails to sync (the
/// run scenarios assume every peer is a synced group member).
async fn form_group(n: usize, key: &str, group: &str) -> Result<Vec<MpNode>> {
    let host = MpNode::spawn("host", key, None, Some(group)).await?;
    let host_id = host.node_id.clone();
    let mut nodes = vec![host];
    for i in 1..n {
        let joiner = MpNode::spawn(&format!("peer{i}"), key, Some(&host_id), None).await?;
        nodes.push(joiner);
    }
    wire_mesh(&mut nodes).await?;

    let t = converge_timeout(n);
    for node in nodes.iter_mut().skip(1) {
        let mut ok = false;
        for _ in 0..2 {
            node.join_group(group, Some(&host_id)).await?;
            if node.wait_sync(group, t).await? {
                ok = true;
                break;
            }
        }
        assert!(ok, "{} failed to sync the group within bound", node.name);
    }
    Ok(nodes)
}

/// Quit AND fully reap every peer (un-reaped `cyan_node` processes would linger across tests).
async fn quit_all(nodes: Vec<MpNode>) {
    for n in nodes {
        n.shutdown().await;
    }
}

/// True iff the run-summary array `key` contains `step` (the captured exec-event buckets).
fn run_has(summary: &serde_json::Value, key: &str, step: &str) -> bool {
    summary[key]
        .as_array()
        .map(|a| a.iter().any(|x| x.as_str() == Some(step)))
        .unwrap_or(false)
}

/// Poll until EVERY peer's run-state for `step` equals `expected` (or the bound elapses). Returns
/// true iff every peer converged. The convergence oracle is each peer's OWN persisted run-state.
async fn converge_state(
    nodes: &mut [MpNode],
    board: &str,
    step: &str,
    expected: &str,
    timeout: Duration,
) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all = true;
        for node in nodes.iter_mut() {
            if node.wf_state(board, step).await? != expected {
                all = false;
                break;
            }
        }
        if all {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ════════════════════════════════════════════════════════════════════════════════════════════
// TEST 1 — the flagship: the run executes AND its run-state converges on every peer.
// ════════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_workflow_run_executes_and_converges() {
    if gated_off() {
        return;
    }
    let _serial = serial().await;
    let n = wf_n();
    let (key, group) = (unique_discovery_key(), unique_group_id());
    let mut nodes = form_group(n, &key, &group).await.expect("form group");

    // Host authors + RUNS a diamond workflow (a→{b,c}→d).
    let board = nodes[0].wf_author(&group, "diamond").await.expect("author");
    let run = nodes[0].wf_run(&board, "wave").await.expect("run");
    println!("@@LIVE@@ info wf_run={run}");

    // ── (1) the run EXECUTED — the exec events fired (DASHBOARD_CONTRACT §A). ──
    assert_eq!(run["started"], 1, "WorkflowRunStarted fired exactly once");
    assert_eq!(run["finished"], 1, "WorkflowRunFinished fired exactly once");
    assert_eq!(run["stats"], 1, "one WorkflowStatsUpdated snapshot");
    assert!(run["progress"].as_u64().unwrap_or(0) >= 1, "StepProgress fired");
    assert_eq!(run["mode"], "wave", "ran via the wave executor");
    // Every executable step moved pending→running→terminal (StepStateChanged transitions).
    for s in ["a", "b", "c", "d"] {
        assert!(run_has(&run, "running", s), "step {s} reached running");
        assert!(run_has(&run, "failed", s), "step {s} reached a terminal state");
    }

    // ── (2) the run-state CONVERGES on every peer (each peer's OWN storage). ──
    let t = converge_timeout(n);
    for s in ["a", "b", "c", "d"] {
        assert!(
            converge_state(&mut nodes, &board, s, "failed", t).await.expect("converge"),
            "step {s} run-state converged to terminal on every peer",
        );
    }

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════════
// TEST 2 — a gate stalls only its branch; an approval on ONE peer unblocks the run for ALL.
// ════════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_barrier_unblocks_run_for_all_peers() {
    if gated_off() {
        return;
    }
    let _serial = serial().await;
    let n = wf_n();
    let (key, group) = (unique_discovery_key(), unique_group_id());
    let mut nodes = form_group(n, &key, &group).await.expect("form group");

    // Host authors + runs a gated workflow: g(gate) ; b depends on g ; x independent.
    let board = nodes[0].wf_author(&group, "gated").await.expect("author");
    let run1 = nodes[0].wf_run(&board, "wave").await.expect("run1");
    println!("@@LIVE@@ info wf_run1={run1}");

    // Run 1: the gate opens, the independent branch proceeds, the gated branch STALLS.
    assert!(run_has(&run1, "awaiting", "g"), "gate g awaits approval");
    assert!(run_has(&run1, "running", "x"), "independent branch x runs");
    assert!(run_has(&run1, "pending", "b"), "gated step b is pending");
    assert!(!run_has(&run1, "running", "b"), "gated step b must NOT run before approval");

    // The independent branch's terminal state converges on every peer regardless of the gate.
    let t = converge_timeout(n);
    assert!(
        converge_state(&mut nodes, &board, "x", "failed", t).await.expect("x converge"),
        "independent branch x converged terminal on every peer",
    );

    // Run 1's gate state propagates to every peer (the gate parked at `scheduled`). Waiting for
    // this BEFORE approving also makes the approval the strictly-latest write on the gate cell —
    // no stale run-1 broadcast can race past it and re-park the gate.
    assert!(
        converge_state(&mut nodes, &board, "g", "scheduled", t).await.expect("g run1 converge"),
        "the gate's run-1 state converged on every peer before approval",
    );

    // Approve the gate on a NON-host peer; the approval is broadcast over the mesh.
    let approver = nodes.len() - 1;
    nodes[approver].wf_approve(&board, "g").await.expect("approve on peer");

    // The approval converges to EVERY peer (incl. the host that re-runs).
    assert!(
        converge_state(&mut nodes, &board, "g", "human_approved", t).await.expect("g converge"),
        "the cross-peer approval converged to every peer",
    );

    // Run 2 on the host: the gate is now satisfied → the gated branch UNBLOCKS and runs.
    let run2 = nodes[0].wf_run(&board, "wave").await.expect("run2");
    println!("@@LIVE@@ info wf_run2={run2}");
    assert!(run_has(&run2, "running", "b"), "gated step b runs after the cross-peer approval");

    // b's terminal state now converges on every peer — the run unblocked for ALL.
    assert!(
        converge_state(&mut nodes, &board, "b", "failed", t).await.expect("b converge"),
        "the unblocked step b converged terminal on every peer",
    );

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════════
// TEST 3 — independent steps in a wave run concurrently (structural concurrency degree).
// ════════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_steps_run_concurrently_in_a_wave() {
    if gated_off() {
        return;
    }
    let _serial = serial().await;
    let n = wf_n();
    let (key, group) = (unique_discovery_key(), unique_group_id());
    let mut nodes = form_group(n, &key, &group).await.expect("form group");

    // Diamond: b and c are mutually independent ⇒ one wave ⇒ launched concurrently.
    let board = nodes[0].wf_author(&group, "diamond").await.expect("author");
    let run = nodes[0].wf_run(&board, "wave").await.expect("run");
    println!("@@LIVE@@ info wf_run={run}");

    // The wave executor launched the independent branch concurrently (peak in-flight degree = 2).
    assert_eq!(run["peak"], 2, "b and c run concurrently in one wave");
    assert!(run_has(&run, "running", "b"), "b ran");
    assert!(run_has(&run, "running", "c"), "c ran");

    // Both independent steps' run-states converge on every peer.
    let t = converge_timeout(n);
    for s in ["b", "c"] {
        assert!(
            converge_state(&mut nodes, &board, s, "failed", t).await.expect("converge"),
            "concurrent step {s} converged on every peer",
        );
    }

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════════
// TEST 4 — the run-finished verdict is consistent across peers (per-step terminal states agree).
// ════════════════════════════════════════════════════════════════════════════════════════════
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_finished_state_consistent_across_peers() {
    if gated_off() {
        return;
    }
    let _serial = serial().await;
    let n = wf_n();
    let (key, group) = (unique_discovery_key(), unique_group_id());
    let mut nodes = form_group(n, &key, &group).await.expect("form group");

    // A linear chain s0→s1→s2 — every step executes.
    let board = nodes[0].wf_author(&group, "linear").await.expect("author");
    let run = nodes[0].wf_run(&board, "wave").await.expect("run");
    println!("@@LIVE@@ info wf_run={run}");

    // The run finished with a definite verdict on the producer (WorkflowRunFinished.state).
    assert_eq!(run["finished"], 1, "the run finished");
    assert_eq!(run["finished_state"], "failed", "the producer's run verdict");

    // WorkflowRunFinished is a LOCAL dashboard event; what GOSSIPS is the per-step run-state. So
    // "consistent across peers" = every peer converges to the SAME terminal state for EVERY step,
    // from which each peer derives the same run verdict. Assert exactly that.
    let t = converge_timeout(n);
    for s in ["s0", "s1", "s2"] {
        assert!(
            converge_state(&mut nodes, &board, s, "failed", t).await.expect("converge"),
            "step {s} terminal state is identical on every peer",
        );
    }

    // The host's own run-state matches the verdict it reported (no producer/store divergence).
    for s in ["s0", "s1", "s2"] {
        assert_eq!(nodes[0].wf_state(&board, s).await.expect("host state"), "failed");
    }

    quit_all(nodes).await;
}

// ════════════════════════════════════════════════════════════════════════════════════════════
// ORCHESTRATOR — `harness/live.sh --scenario workflow-run` drives THIS one, emitting the
// machine-readable `@@LIVE@@` table + verdict (one row per peer). It re-uses the same internals.
// ════════════════════════════════════════════════════════════════════════════════════════════
fn emit(peer: &str, pass: bool, detail: &str) {
    println!(
        "@@LIVE@@ scenario=workflow-run peer={peer} result={} detail={detail}",
        if pass { "PASS" } else { "FAIL" }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_run_live() {
    if gated_off() {
        return;
    }
    let _serial = serial().await;
    let n = wf_n();
    let (key, group) = (unique_discovery_key(), unique_group_id());
    println!("@@LIVE@@ info scenario=workflow-run n={n}");
    let mut nodes = form_group(n, &key, &group).await.expect("form group");

    // Host authors + runs a diamond; assert the run executed on the producer.
    let board = nodes[0].wf_author(&group, "diamond").await.expect("author");
    let run = nodes[0].wf_run(&board, "wave").await.expect("run");
    let executed = run["started"] == 1
        && run["finished"] == 1
        && run["peak"] == 2
        && ["a", "b", "c", "d"].iter().all(|s| run_has(&run, "running", s));
    emit("host", executed, &format!("run-executed-peak={}", run["peak"]));

    // Per-peer convergence: every peer's run-state for every step reaches terminal.
    let t = converge_timeout(n);
    let mut overall = executed;
    let names: Vec<String> = nodes.iter().map(|p| p.name.clone()).collect();
    for (i, name) in names.iter().enumerate() {
        let mut ok = true;
        for s in ["a", "b", "c", "d"] {
            let deadline = Instant::now() + t;
            loop {
                let st = nodes[i].wf_state(&board, s).await.expect("state");
                if st == "failed" {
                    break;
                }
                if Instant::now() >= deadline {
                    ok = false;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        overall &= ok;
        emit(name, ok, if ok { "all-steps-converged" } else { "did-not-converge" });
    }

    println!(
        "@@LIVE@@ verdict={} scenario=workflow-run n={n}",
        if overall { "PASS" } else { "FAIL" }
    );
    quit_all(nodes).await;
    assert!(overall, "workflow-run: a peer failed to execute/converge — see the @@LIVE@@ lines");
}
