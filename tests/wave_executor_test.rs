//! Wave-concurrent executor tests (Round 7, WORKFLOW_MATERIALIZATION §1).
//!
//! Proves the backend EXECUTOR consumes a Lens `PhysicalPlan` and runs it
//! wave-concurrently — independent branches in one wave run together, human-approval
//! gates are BRANCH barriers (not global stalls), cache hits skip re-running, and the
//! plan's batching caps in-flight concurrency — while keeping the SEQUENTIAL toposort
//! as the offline fallback when no plan is present. The same dashboard exec events
//! fire from the concurrent path as from the sequential one.
//!
//! No live deps: executed steps are `local` plugin steps against an empty (offline)
//! plugin root, so they fail fast and deterministically — enough to drive the
//! running→terminal transitions, wave ordering, and the structural concurrency
//! degree (`peak_concurrency`). Every wait is bounded: the run is awaited, then the
//! event channel is drained non-blockingly.

use std::path::Path;
use std::sync::Once;

use cyan_backend::exec_plan::{PhysicalPlan, PlannedStep, Wave};
use cyan_backend::models::commands::CommandMsg;
use cyan_backend::models::events::SwiftEvent;
use cyan_backend::pipeline::{self, PipelineStepConfig, PipelineStepState};
use cyan_backend::storage;
use serde_json::Value;
use tokio::sync::mpsc;

static DB_INIT: Once = Once::new();

/// Init the process-global storage once and point the device plugin root at an empty
/// dir so on-device `mcp_tool` steps fail deterministically ("not installed").
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wave.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime

        let proot = std::env::temp_dir().join(format!("cyan-wave-plugins-{}", std::process::id()));
        std::fs::create_dir_all(&proot).expect("plugins root");
        // SAFETY: single-threaded test setup; this global scopes the device host.
        unsafe {
            std::env::set_var("CYAN_PLUGINS_ROOT", &proot);
        }
    });
}

fn init_base_schema(db_path: &Path) -> Result<(), rusqlite::Error> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, icon TEXT, color TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY, group_id TEXT NOT NULL, name TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY, workspace_id TEXT, group_id TEXT, board_id TEXT,
            type TEXT NOT NULL, name TEXT NOT NULL, hash TEXT, data TEXT, size INTEGER,
            source_peer TEXT, local_path TEXT, created_at INTEGER NOT NULL,
            board_mode TEXT DEFAULT 'canvas'
        );
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL, content TEXT, output TEXT,
            collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

/// A `PipelineStepConfig` with the given id/stage/executor/deps; everything else default.
fn step_config(step_id: &str, executor: &str, depends_on: Vec<&str>) -> PipelineStepConfig {
    PipelineStepConfig {
        step_id: step_id.to_string(),
        depends_on: depends_on.into_iter().map(String::from).collect(),
        stage: Some(step_id.to_string()),
        executor: executor.to_string(),
        model: None,
        model_config: None,
        tools: vec![],
        output_format: "markdown".to_string(),
        command: None,
        timeout_seconds: Some(5),
        retry_count: Some(0),
        auto_advance: false,
        notifications: vec![],
        state: PipelineStepState::default(),
    }
}

/// Cell metadata JSON: a pipeline config plus (for executable steps) an `mcp_tool`
/// spec pointing at a non-installed plugin so execution fails fast and offline.
fn cell_meta(config: &PipelineStepConfig, with_plugin: bool) -> String {
    let mut meta = serde_json::Map::new();
    meta.insert("pipeline".to_string(), serde_json::to_value(config).expect("config json"));
    if with_plugin {
        meta.insert(
            "mcp_tool".to_string(),
            serde_json::json!({ "plugin_id": "nope", "tool": "nope", "args": {} }),
        );
    }
    Value::Object(meta).to_string()
}

/// Seed group → workspace → board, then one cell per step. `local` steps carry the
/// offline mcp_tool; `manual` steps don't. Returns nothing — board id is `board`.
fn seed_board(group: &str, board: &str, steps: &[PipelineStepConfig]) {
    let now = 1_700_000_000i64;
    let ws = format!("{group}-ws");
    storage::group_insert_simple(group, "Wave Group", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&ws, group, "Main").expect("workspace");
    storage::board_insert_simple(board, &ws, "Wave Pipeline", now).expect("board");

    for (i, cfg) in steps.iter().enumerate() {
        let with_plugin = cfg.executor != "manual";
        let meta = cell_meta(cfg, with_plugin);
        storage::cell_insert_simple(
            &format!("{board}-{}", cfg.step_id), board, "markdown", (i + 1) as i32,
            Some(&format!("Step {}", cfg.step_id)), None, false, None, Some(&meta), now, now,
        ).expect("cell");
    }
}

/// A default executable `PlannedStep` (local placement, no cache/gate).
fn pstep(id: &str) -> PlannedStep {
    PlannedStep {
        id: id.to_string(),
        placement: "local".to_string(),
        cache_key: format!("ck-{id}"),
        cache_hit: false,
        is_gate: false,
        gate_barrier: None,
        cost_usd: 0.0,
        concurrency_weight: 1,
    }
}

fn wave(index: u32, steps: Vec<PlannedStep>, batches: Vec<Vec<&str>>) -> Wave {
    Wave {
        index,
        steps,
        batches: batches.into_iter().map(|b| b.into_iter().map(String::from).collect()).collect(),
    }
}

fn plan(tenant: &str, max_concurrency: u32, waves: Vec<Wave>) -> PhysicalPlan {
    PhysicalPlan {
        tenant_id: tenant.to_string(),
        waves,
        max_concurrency,
        max_cost_usd: 100.0,
        total_cost_usd: 0.0,
    }
}

fn drain(rx: &mut mpsc::UnboundedReceiver<SwiftEvent>) -> Vec<SwiftEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

/// Position of the first event matching a predicate over the ordered event stream.
fn pos(events: &[SwiftEvent], pred: impl Fn(&SwiftEvent) -> bool) -> Option<usize> {
    events.iter().position(pred)
}

fn is_state(e: &SwiftEvent, want_step: &str, want_state: &str) -> bool {
    matches!(e, SwiftEvent::StepStateChanged { step_id, state, .. } if step_id == want_step && state == want_state)
}

async fn run(board: &str, p: Option<PhysicalPlan>) -> (Value, Vec<SwiftEvent>) {
    let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<CommandMsg>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    let result = pipeline::run_pipeline_with_plan(board, p, &cmd_tx, &event_tx)
        .await
        .expect("run_pipeline_with_plan");
    (result, drain(&mut event_rx))
}

// ── A→{B,C}→D: B and C share one wave and run concurrently; D follows. ──────────
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diamond_dag_runs_independent_branch_concurrently() {
    ensure_db();
    let group = "wave-grp-diamond";
    let board = "wave-board-diamond";
    seed_board(group, board, &[
        step_config("a", "local", vec![]),
        step_config("b", "local", vec!["a"]),
        step_config("c", "local", vec!["a"]),
        step_config("d", "local", vec!["b", "c"]),
    ]);

    let p = plan(group, 8, vec![
        wave(0, vec![pstep("a")], vec![vec!["a"]]),
        wave(1, vec![pstep("b"), pstep("c")], vec![vec!["b", "c"]]), // B,C one batch
        wave(2, vec![pstep("d")], vec![vec!["d"]]),
    ]);

    let (result, events) = run(board, Some(p)).await;

    assert_eq!(result["mode"], "wave");
    // B and C were launched in the same batch ⇒ peak in-flight is 2.
    assert_eq!(result["peak_concurrency"], 2, "B and C run concurrently in one wave");

    // Both B and C reach `running` before D does (D is gated behind the wave barrier).
    let b_run = pos(&events, |e| is_state(e, "b", "running")).expect("b running");
    let c_run = pos(&events, |e| is_state(e, "c", "running")).expect("c running");
    let d_run = pos(&events, |e| is_state(e, "d", "running")).expect("d running");
    assert!(b_run < d_run, "b runs before d (next wave)");
    assert!(c_run < d_run, "c runs before d (next wave)");

    // A runs before both B and C (earlier wave).
    let a_run = pos(&events, |e| is_state(e, "a", "running")).expect("a running");
    assert!(a_run < b_run && a_run < c_run, "a (wave 0) runs before b/c (wave 1)");
}

// ── A gate stalls only its own branch; an independent branch proceeds. ──────────
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_barrier_stalls_only_its_branch() {
    ensure_db();
    let group = "wave-grp-gate";
    let board = "wave-board-gate";
    // g = manual gate; b depends on g (gated branch); x = independent branch.
    seed_board(group, board, &[
        step_config("g", "manual", vec![]),
        step_config("b", "local", vec!["g"]),
        step_config("x", "local", vec![]),
    ]);

    let mut b = pstep("b");
    b.gate_barrier = Some("g".to_string()); // b waits behind gate g
    let mut g = pstep("g");
    g.is_gate = true;
    let p = plan(group, 8, vec![
        wave(0, vec![g, pstep("x")], vec![vec!["g", "x"]]), // gate + independent branch
        wave(1, vec![b], vec![vec!["b"]]),                   // gated dependent
    ]);

    let (_result, events) = run(board, Some(p)).await;

    // The gate opens (awaiting approval + an approval request).
    assert!(pos(&events, |e| is_state(e, "g", "awaiting_approval")).is_some(), "gate awaiting approval");
    assert!(
        pos(&events, |e| matches!(e, SwiftEvent::ApprovalRequested { step_id, .. } if step_id == "g")).is_some(),
        "gate raises an approval request",
    );

    // The independent branch PROCEEDS (x runs) despite the gate.
    assert!(pos(&events, |e| is_state(e, "x", "running")).is_some(), "independent branch x runs");

    // The gated branch STALLS: b is marked pending and never runs.
    assert!(pos(&events, |e| is_state(e, "b", "pending")).is_some(), "gated step b is pending");
    assert!(pos(&events, |e| is_state(e, "b", "running")).is_none(), "gated step b must NOT run");
}

// ── A cache hit reuses the prior artifact and skips re-execution. ───────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_hit_skips_rerun() {
    ensure_db();
    let group = "wave-grp-cache";
    let board = "wave-board-cache";
    seed_board(group, board, &[step_config("only", "local", vec![])]);

    // Seed a prior artifact on the step's cell — the cache hit must reuse THIS.
    {
        let conn = storage::db().lock().expect("db");
        conn.execute(
            "UPDATE notebook_cells SET output = ?1 WHERE id = ?2",
            rusqlite::params!["PRIOR ARTIFACT", format!("{board}-only")],
        ).expect("seed output");
    }

    let mut s = pstep("only");
    s.cache_hit = true; // optimizer says: reuse, don't re-run
    let p = plan(group, 8, vec![wave(0, vec![s], vec![vec!["only"]])]);

    let (result, events) = run(board, Some(p)).await;

    // The step is DONE (not failed) even though, run live, the offline plugin would
    // have failed — proving execution was skipped.
    assert!(pos(&events, |e| is_state(e, "only", "done")).is_some(), "cache hit → done");
    assert!(pos(&events, |e| is_state(e, "only", "failed")).is_none(), "cache hit must NOT execute (would fail)");
    assert!(pos(&events, |e| is_state(e, "only", "running")).is_none(), "cache hit must NOT enter running");
    let cache_hits = result["results"].as_array().expect("results")
        .iter().filter(|r| r["status"] == "cache_hit").count();
    assert_eq!(cache_hits, 1, "the step reports a cache_hit");

    // The prior artifact is preserved on the cell.
    let conn = storage::db().lock().expect("db");
    let out: Option<String> = conn.query_row(
        "SELECT output FROM notebook_cells WHERE id = ?1",
        rusqlite::params![format!("{board}-only")],
        |row| row.get(0),
    ).expect("read output");
    assert_eq!(out.as_deref(), Some("PRIOR ARTIFACT"), "prior artifact reused, not overwritten");
}

// ── The plan's batching caps how many steps are in flight at once. ──────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_cap_limits_in_flight() {
    ensure_db();
    let group = "wave-grp-budget";
    let board = "wave-board-budget";
    // Four mutually-independent steps; the optimizer batches them 2+2 for a cap of 2.
    seed_board(group, board, &[
        step_config("w", "local", vec![]),
        step_config("x", "local", vec![]),
        step_config("y", "local", vec![]),
        step_config("z", "local", vec![]),
    ]);

    let p = plan(group, 2, vec![
        wave(0, vec![pstep("w"), pstep("x"), pstep("y"), pstep("z")],
             vec![vec!["w", "x"], vec!["y", "z"]]), // one wave, two batches of 2
    ]);

    let (result, _events) = run(board, Some(p)).await;

    // Peak in-flight is exactly the cap (2) — never the full wave of 4.
    assert_eq!(result["peak_concurrency"], 2, "batching held in-flight to the cap");
    // All four still executed (batching limits concurrency, not coverage).
    let executed = result["results"].as_array().expect("results")
        .iter().filter(|r| r["status"] == "ai_complete" || r["status"] == "failed").count();
    assert_eq!(executed, 4, "all four steps ran across the two batches");
}

// ── With no plan, the executor falls back to the sequential toposort. ───────────
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_plan_falls_back_to_sequential() {
    ensure_db();
    let group = "wave-grp-seq";
    let board = "wave-board-seq";
    seed_board(group, board, &[
        step_config("a", "local", vec![]),
        step_config("b", "local", vec!["a"]),
        step_config("c", "local", vec!["a"]),
        step_config("d", "local", vec!["b", "c"]),
    ]);

    let (result, events) = run(board, None).await;

    assert_eq!(result["mode"], "sequential", "no plan ⇒ sequential fallback");
    assert_eq!(result["peak_concurrency"], 1, "sequential runs one step at a time");

    // The root step runs; with no plan the sequential toposort applies the prior
    // dependency-gating, so the dependents (whose deps aren't approved) stay pending
    // — never the wave path's concurrent dispatch.
    assert!(pos(&events, |e| is_state(e, "a", "running")).is_some(), "root step a runs");
    for s in ["b", "c", "d"] {
        assert!(pos(&events, |e| is_state(e, s, "running")).is_none(), "{s} does not run concurrently (sequential gating)");
        assert!(pos(&events, |e| is_state(e, s, "pending")).is_some(), "{s} is pending (deps not met)");
    }
}

// ── The concurrent path emits the SAME dashboard exec events as the sequential. ──
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exec_events_emitted_from_concurrent_path() {
    ensure_db();
    let group = "wave-grp-events";
    let board = "wave-board-events";
    seed_board(group, board, &[
        step_config("a", "local", vec![]),
        step_config("b", "local", vec!["a"]),
    ]);

    let p = plan(group, 8, vec![
        wave(0, vec![pstep("a")], vec![vec!["a"]]),
        wave(1, vec![pstep("b")], vec![vec!["b"]]),
    ]);

    let (_result, events) = run(board, Some(p)).await;

    // Run lifecycle.
    assert!(matches!(events.first(), Some(SwiftEvent::WorkflowRunStarted { board_id, total_steps, .. }) if board_id == board && *total_steps == 2));
    assert!(matches!(events.last(), Some(SwiftEvent::WorkflowRunFinished { board_id, .. }) if board_id == board));

    // Per-step: running, progress, terminal — tagged with the run's tenant.
    assert!(pos(&events, |e| is_state(e, "a", "running")).is_some(), "running emitted");
    assert!(pos(&events, |e| matches!(e, SwiftEvent::StepProgress { step_id, .. } if step_id == "a")).is_some(), "progress emitted");
    assert!(
        pos(&events, |e| is_state(e, "a", "failed") || is_state(e, "a", "done")).is_some(),
        "terminal state emitted",
    );

    // Exactly one stats snapshot, tenant-scoped, with both steps' stages present.
    let stats: Vec<_> = events.iter().filter(|e| matches!(e, SwiftEvent::WorkflowStatsUpdated { .. })).collect();
    assert_eq!(stats.len(), 1, "exactly one WorkflowStatsUpdated");
    if let SwiftEvent::WorkflowStatsUpdated { tenant_id, snapshot, .. } = stats[0] {
        assert_eq!(tenant_id, group, "stats tenant-scoped to the run's group");
        let stages: Vec<&str> = snapshot.per_stage.iter().map(|s| s.stage.as_str()).collect();
        assert!(stages.contains(&"a") && stages.contains(&"b"), "both stages in snapshot: {stages:?}");
    }

    // Every dashboard event carries this run's tenant (no leakage).
    for e in &events {
        if let SwiftEvent::StepStateChanged { tenant_id, .. } = e {
            assert_eq!(tenant_id, group);
        }
    }
}
