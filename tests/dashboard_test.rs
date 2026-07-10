//! Dashboard producer tests (DASHBOARD_CONTRACT §A/§C/§E).
//!
//! Proves the REAL `pipeline::run_pipeline` path emits the additive, receive-only
//! dashboard `SwiftEvent`s in order, tagged with tenant/run/board/workflow/stage/
//! plugin, and computes a correct `DashboardSnapshot` from the obs/cost rail. No
//! live deps: executed steps fail fast against an empty (offline) plugin root, which
//! is enough to exercise the running→terminal transitions and the snapshot shape.
//! Every wait is bounded (the run is awaited, then the event channel is drained).

use std::path::Path;
use std::sync::Once;

use cyan_backend::dashboard::{Actor, RunObs, StepCost, StepObs, StepState};
use cyan_backend::models::commands::CommandMsg;
use cyan_backend::models::events::SwiftEvent;
use cyan_backend::pipeline::{self, PipelineStepConfig, PipelineStepState};
use cyan_backend::storage;
use serde_json::Value;
use tokio::sync::mpsc;

const EPS: f64 = 1e-9;

static DB_INIT: Once = Once::new();

/// Init the process-global storage once, and point the device plugin root at an
/// empty dir so on-device `mcp_tool` steps fail deterministically ("not installed")
/// — fast, offline, no subprocess.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("dashboard.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime

        let proot = std::env::temp_dir().join(format!("cyan-dash-plugins-{}", std::process::id()));
        std::fs::create_dir_all(&proot).expect("plugins root");
        // SAFETY: single-threaded test setup; this global scopes the device host.
        unsafe {
            std::env::set_var("CYAN_PLUGINS_ROOT", &proot);
        }
    });
}

/// Base tables the engine migrations assume exist. Run once before `storage::init_db`.
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

/// A `PipelineStepConfig` with the given id/stage/executor, everything else default.
fn step_config(step_id: &str, stage: &str, executor: &str, depends_on: Vec<String>) -> PipelineStepConfig {
    PipelineStepConfig {
        step_id: step_id.to_string(),
        depends_on,
        stage: Some(stage.to_string()),
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

/// Serialize a cell's `pipeline` metadata (+ optional `mcp_tool`) to a JSON string.
fn cell_metadata(config: &PipelineStepConfig, mcp_tool: Option<&str>) -> String {
    let mut meta = serde_json::Map::new();
    meta.insert("pipeline".to_string(), serde_json::to_value(config).expect("config json"));
    if let Some(tool_json) = mcp_tool {
        let tool: Value = serde_json::from_str(tool_json).expect("valid mcp_tool literal");
        meta.insert("mcp_tool".to_string(), tool);
    }
    Value::Object(meta).to_string()
}

/// Seed a group → workspace → board with one `manual` gate step ("review" stage)
/// followed by one offline-failing `local` plugin step ("ingest" stage). Both are
/// independent and the gate comes FIRST: a failed step HALTS the run (harden S —
/// downstream dependents never surface), so the gate must be reached before the
/// failing step for both stages to emit their events.
fn seed_two_step_board(group: &str, board: &str) {
    let now = 1_700_000_000i64;
    let ws = format!("{group}-ws");
    storage::group_insert_simple(group, "Dash Group", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&ws, group, "Main").expect("workspace");
    storage::board_insert_simple(board, &ws, "Localization Pipeline", now).expect("board");

    // English/asset cell (no pipeline config) — ignored by the runner.
    storage::cell_insert_simple("c0", board, "markdown", 0, Some("https://example/asset.mov"), None, false, None, None, now, now).ok();

    // probe: a local on-device plugin step that fails fast (plugin not installed).
    let probe = step_config("probe", "ingest", "local", vec![]);
    let probe_meta = cell_metadata(&probe, Some(r#"{"plugin_id":"nope","tool":"nope","args":{}}"#));
    storage::cell_insert_simple(&format!("{board}-c1"), board, "markdown", 1, Some("Probe the asset"), None, false, None, Some(&probe_meta), now, now).expect("probe cell");

    // review: a manual gate (awaiting approval), dep-free. petgraph's toposort
    // reverses DFS finish order, so of two independent nodes the LATER-seeded one
    // is visited FIRST — review therefore gates before probe runs and halts.
    let review = step_config("review", "review", "manual", vec![]);
    let review_meta = cell_metadata(&review, None);
    storage::cell_insert_simple(&format!("{board}-c2"), board, "markdown", 2, Some("Human review"), None, false, None, Some(&review_meta), now, now).expect("review cell");
}

/// Drain a `SwiftEvent` receiver into a vec (non-blocking, after the run finished).
fn drain(rx: &mut mpsc::UnboundedReceiver<SwiftEvent>) -> Vec<SwiftEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_emits_dashboard_events_in_order_with_correct_snapshot() {
    ensure_db();
    let group = "dash-grp-order";
    let board = "dash-board-order";
    seed_two_step_board(group, board);

    let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<CommandMsg>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();

    pipeline::run_pipeline(board, &cmd_tx, &event_tx).await.expect("run_pipeline");
    let events = drain(&mut event_rx);

    // ── First event is WorkflowRunStarted with the right tags + step count. ──
    let first = events.first().expect("at least one event");
    match first {
        SwiftEvent::WorkflowRunStarted { tenant_id, run_id, board_id, workflow_id, total_steps, .. } => {
            assert_eq!(tenant_id, group, "tenant is the board's group");
            assert_eq!(board_id, board);
            assert_eq!(workflow_id, board, "the board IS the workflow surface");
            assert!(!run_id.is_empty());
            assert_eq!(*total_steps, 2);
        }
        other => panic!("expected WorkflowRunStarted first, got {other:?}"),
    }

    // ── Last event is WorkflowRunFinished. ──
    match events.last().expect("at least one event") {
        SwiftEvent::WorkflowRunFinished { state, board_id, .. } => {
            assert_eq!(board_id, board);
            assert!(matches!(state.as_str(), "done" | "failed"), "terminal run state, got {state}");
        }
        other => panic!("expected WorkflowRunFinished last, got {other:?}"),
    }

    // Index helpers over the ordered event stream.
    let pos = |pred: &dyn Fn(&SwiftEvent) -> bool| events.iter().position(pred);

    // probe: running → progress → terminal (done|failed), in that order.
    let probe_running = pos(&|e| matches!(e, SwiftEvent::StepStateChanged { step_id, state, .. } if step_id == "probe" && state == "running")).expect("probe running");
    let probe_progress = pos(&|e| matches!(e, SwiftEvent::StepProgress { step_id, .. } if step_id == "probe")).expect("probe progress");
    let probe_term = pos(&|e| matches!(e, SwiftEvent::StepStateChanged { step_id, state, .. } if step_id == "probe" && (state == "failed" || state == "done"))).expect("probe terminal");
    assert!(probe_running < probe_progress, "running before progress");
    assert!(probe_progress < probe_term, "progress before terminal");

    // probe carries its stage + the plugin tag from the mcp_tool spec.
    let probe_state = events.iter().find(|e| matches!(e, SwiftEvent::StepStateChanged { step_id, state, .. } if step_id == "probe" && state == "running")).expect("probe running event");
    if let SwiftEvent::StepStateChanged { stage, plugin, actor, .. } = probe_state {
        assert_eq!(stage, "ingest");
        assert_eq!(plugin.as_deref(), Some("nope"));
        assert_eq!(actor, "ai");
    }

    // review: awaiting_approval + an ApprovalRequested gate. The gate surfaces
    // BEFORE the probe runs (it is first in toposort order); the probe's failure
    // then HALTS the run (harden S), so nothing may come after it but the rollup.
    let review_await = pos(&|e| matches!(e, SwiftEvent::StepStateChanged { step_id, state, .. } if step_id == "review" && state == "awaiting_approval")).expect("review awaiting");
    let review_gate = pos(&|e| matches!(e, SwiftEvent::ApprovalRequested { step_id, .. } if step_id == "review")).expect("review gate");
    assert!(review_await < probe_running, "the gate surfaces before the failing probe halts the run");
    assert!(review_await < review_gate || review_gate == review_await + 1);

    // ── A WorkflowStatsUpdated with a correct snapshot (the first is the live
    // incremental push after the probe executed; a final rollup follows). ──
    let stats = events.iter().find_map(|e| match e {
        SwiftEvent::WorkflowStatsUpdated { snapshot, tenant_id, .. } => Some((snapshot.clone(), tenant_id.clone())),
        _ => None,
    }).expect("a WorkflowStatsUpdated event");
    let (snap, stats_tenant) = stats;
    assert_eq!(stats_tenant, group);
    assert_eq!(snap.tenant_id, group);
    assert_eq!(snap.board_id, board);
    assert_eq!(snap.workflow_id, board);
    assert_eq!(snap.items_total, 2);
    // Both stages are present in the per-stage breakdown.
    let stages: Vec<&str> = snap.per_stage.iter().map(|s| s.stage.as_str()).collect();
    assert!(stages.contains(&"ingest"), "ingest stage present: {stages:?}");
    assert!(stages.contains(&"review"), "review stage present: {stages:?}");
    // Gate attribution exists for the review stage (human work).
    assert!(snap.gate_minutes_by_stage.contains_key("review"));
    // ingest used the plugin.
    let ingest = snap.per_stage.iter().find(|s| s.stage == "ingest").expect("ingest stage");
    assert_eq!(ingest.plugins, vec!["nope".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_tenant_scoped() {
    ensure_db();
    // Two independent runs in two different groups (tenants).
    let runs = [("dash-grp-a", "dash-board-a"), ("dash-grp-b", "dash-board-b")];
    for (group, board) in runs {
        seed_two_step_board(group, board);
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<CommandMsg>();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
        pipeline::run_pipeline(board, &cmd_tx, &event_tx).await.expect("run_pipeline");
        let events = drain(&mut event_rx);
        assert!(!events.is_empty());

        // Every event carries THIS run's tenant + board (no cross-tenant leakage),
        // and a single run_id across the run's events.
        let mut run_ids = std::collections::HashSet::new();
        let mut dashboard_events = 0;
        for ev in &events {
            // Only the dashboard events are tenant/run-tagged; skip plain StatusUpdates.
            let Some((tenant, ev_board, run_id)) = tags_of(ev) else { continue };
            dashboard_events += 1;
            assert_eq!(tenant, group, "event tenant must be the run's group: {ev:?}");
            assert_eq!(ev_board, board, "event board must be the run's board: {ev:?}");
            if !run_id.is_empty() {
                run_ids.insert(run_id);
            }
        }
        assert!(dashboard_events > 0, "the run emitted dashboard events");
        assert_eq!(run_ids.len(), 1, "all run events share one run_id (group {group})");
    }
}

/// Extract `(tenant_id, board_id, run_id)` from a dashboard `SwiftEvent` (None for
/// non-dashboard events like `StatusUpdate`).
fn tags_of(ev: &SwiftEvent) -> Option<(String, String, String)> {
    match ev {
        SwiftEvent::WorkflowRunStarted { tenant_id, board_id, run_id, .. }
        | SwiftEvent::StepStateChanged { tenant_id, board_id, run_id, .. }
        | SwiftEvent::StepProgress { tenant_id, board_id, run_id, .. }
        | SwiftEvent::ApprovalRequested { tenant_id, board_id, run_id, .. }
        | SwiftEvent::ApprovalResolved { tenant_id, board_id, run_id, .. }
        | SwiftEvent::WorkflowRunFinished { tenant_id, board_id, run_id, .. }
        | SwiftEvent::WorkflowStatsUpdated { tenant_id, board_id, run_id, .. } => {
            Some((tenant_id.clone(), board_id.clone(), run_id.clone()))
        }
        _ => None,
    }
}

#[test]
fn stats_snapshot_perstage_minutes_cost_gate_correct() {
    // A scripted run over three stages: an AI ingest, a human gate, an AI deliver
    // that used a plugin. Verifies per-stage minutes/cost/gate + the cost rail
    // (DASHBOARD_CONTRACT §B/§C). All inputs explicit → deterministic.
    let mut obs = RunObs::new("tenant-x", "board-x", "run-x", "board-x", "Workflow X", 3);

    obs.record(StepObs {
        step_id: "ingest".into(),
        name: "Ingest".into(),
        stage: "ingest".into(),
        actor: Actor::Ai,
        plugin: None,
        state: StepState::Done,
        wall_ms: 120_000, // 2 min
        gate_ms: 0,
        cost: StepCost { tokens_in: 1000, tokens_out: 500, gpu_ms: 0, external_usd: 0.0 },
    });
    obs.record(StepObs {
        step_id: "review".into(),
        name: "Review".into(),
        stage: "review".into(),
        actor: Actor::Human,
        plugin: None,
        state: StepState::Approved,
        wall_ms: 0,
        gate_ms: 180_000, // 3 min of human gate time
        cost: StepCost::default(),
    });
    obs.record(StepObs {
        step_id: "deliver".into(),
        name: "Deliver".into(),
        stage: "deliver".into(),
        actor: Actor::Ai,
        plugin: Some("media-probe".into()),
        state: StepState::Done,
        wall_ms: 60_000, // 1 min
        gate_ms: 0,
        cost: StepCost { tokens_in: 0, tokens_out: 0, gpu_ms: 30_000, external_usd: 0.50 },
    });

    let snap = obs.snapshot(123, Some("deliver".into()), Some("deliver".into()));

    // Per-stage breakdown, in first-appearance order.
    assert_eq!(snap.per_stage.len(), 3);
    let ingest = &snap.per_stage[0];
    assert_eq!(ingest.stage, "ingest");
    assert_eq!(ingest.state, "done");
    assert!((ingest.minutes - 2.0).abs() < EPS);
    assert!((ingest.ai_minutes - 2.0).abs() < EPS);
    assert!((ingest.human_minutes - 0.0).abs() < EPS);
    // 1000/1k*0.003 + 500/1k*0.015 = 0.003 + 0.0075 = 0.0105
    assert!((ingest.cost_usd - 0.0105).abs() < EPS, "ingest cost {}", ingest.cost_usd);
    assert!(ingest.plugins.is_empty());

    let review = &snap.per_stage[1];
    assert_eq!(review.stage, "review");
    assert_eq!(review.state, "approved");
    assert!((review.minutes - 0.0).abs() < EPS);
    assert!((review.human_minutes - 3.0).abs() < EPS, "gate time attributed to its stage");
    assert!((review.cost_usd - 0.0).abs() < EPS);

    let deliver = &snap.per_stage[2];
    assert_eq!(deliver.stage, "deliver");
    assert!((deliver.minutes - 1.0).abs() < EPS);
    assert!((deliver.ai_minutes - 1.0).abs() < EPS);
    // 30000ms * 0.00002 + 0.50 external = 0.6 + 0.5 = 1.1
    assert!((deliver.cost_usd - 1.1).abs() < EPS, "deliver cost {}", deliver.cost_usd);
    assert_eq!(deliver.plugins, vec!["media-probe".to_string()]);

    // Totals.
    assert!((snap.totals.wall_minutes - 3.0).abs() < EPS); // 2 + 0 + 1
    assert!((snap.totals.human_minutes - 3.0).abs() < EPS);
    assert!((snap.totals.ai_minutes - 3.0).abs() < EPS); // 2 + 1
    assert_eq!(snap.totals.files_processed, 3); // all three terminal
    assert!((snap.totals.est_cost_usd - 1.1105).abs() < EPS, "total cost {}", snap.totals.est_cost_usd);

    // Gate minutes by stage (the prompt's `gate_minutes_by_stage`).
    assert!((snap.gate_minutes_by_stage["review"] - 3.0).abs() < EPS);
    assert!((snap.gate_minutes_by_stage["ingest"] - 0.0).abs() < EPS);
    assert!((snap.gate_minutes_by_stage["deliver"] - 0.0).abs() < EPS);

    assert_eq!(snap.items_processed, 3);
    assert_eq!(snap.items_total, 3);
    assert_eq!(snap.current_stage.as_deref(), Some("deliver"));
    assert_eq!(snap.updated_at, 123);
}
