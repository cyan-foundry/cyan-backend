// cyan-backend/src/pipeline.rs
//
// Pipeline execution engine for notebook boards.
// Reads notebook cells with pipeline metadata, builds a DAG using petgraph,
// executes steps in topological order, and updates cell state via gossip sync.
//
// Commands:
//   /pipeline compile  → Lens converts English cells to step configs
//   /pipeline run      → Execute DAG locally
//   /pipeline status   → Query current state
//   /pipeline approve  → Human approves a step
//   /pipeline export   → Generate Airflow DAG Python file

#![allow(dead_code)] // Pipeline-step executors are scaffolding moving to the MCP/workflow model; see CLAUDE.md 'Out of scope'.

use anyhow::{anyhow, Result};
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tokio::process::Command as TokioCommand;
use tokio::sync::mpsc::UnboundedSender;

use crate::dashboard::{Actor, RunObs, StepCost, StepObs, StepState};
use crate::models::commands::CommandMsg;
use crate::models::events::SwiftEvent;
use crate::storage;

// ============================================================================
// Pipeline Step Config (matches iOS PipelineTypes.swift)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStepConfig {
    pub step_id: String,
    pub depends_on: Vec<String>,
    /// Optional stage this step belongs to (the dashboard `stage` slicing key,
    /// DASHBOARD_CONTRACT §A). Additive + `serde(default)`: absent ⇒ the step is
    /// its own stage (`step_id`). iOS Codable ignores it when not present.
    #[serde(default)]
    pub stage: Option<String>,
    pub executor: String,           // "local", "cloud", "manual", "lens"
    pub model: Option<String>,      // AI model to use (legacy, prefer model_config)
    #[serde(default)]
    pub model_config: Option<ModelConfig>,  // Full model configuration
    #[serde(default)]
    pub tools: Vec<String>,         // Tools this step needs: ["ffprobe", "ffmpeg", "whisper"]
    #[serde(default = "default_output_format")]
    pub output_format: String,      // "markdown", "srt", "json", "findings"
    pub command: Option<String>,    // resolved command
    pub timeout_seconds: Option<u64>,
    pub retry_count: Option<u32>,
    pub auto_advance: bool,
    /// D/P-4 — REVIEW HOLD: this step's post-effect gate is a PRODUCER-REVIEW
    /// WINDOW (upload lands, then the run parks until the assigned reviewer
    /// approves), not a generic "AI done" acknowledgement. Stamped at compile
    /// for an external upload-for-review step. Additive + `serde(default)`.
    #[serde(default)]
    pub review_hold: bool,
    /// The REAL user the review gate waits on — an sso_user (e.g. "producer"),
    /// never a role string. Resolved at compile from the board's review
    /// assignee; `approve_step` clears the gate ONLY for this user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_on: Option<String>,
    #[serde(default)]
    pub notifications: Vec<StepNotification>,
    pub state: PipelineStepState,
}

fn default_output_format() -> String { "markdown".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub id: String,                     // "llama-3.3-70b-awq", "whisper-large-v3"
    #[serde(default)]
    pub endpoint: Option<String>,       // Override default endpoint
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStepState {
    pub status: String,             // pending, scheduled, running, ai_complete, human_approved, failed, skipped
    pub ai_result: Option<String>,
    pub ai_completed_at: Option<i64>,
    pub human_reviewer: Option<String>,
    pub human_approved_at: Option<i64>,
    pub error: Option<String>,
    pub run_id: Option<String>,
    pub attempt: u32,
    pub started_at: Option<i64>,
    pub duration: Option<f64>,
    pub artifacts: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepNotification {
    pub id: String,
    pub trigger: String,            // ai_complete, human_approved, failed, stale_24h, pipeline_complete
    pub action: String,             // email, dm, activity, webhook
    pub target: String,
    pub message: Option<String>,
}

impl Default for PipelineStepState {
    fn default() -> Self {
        Self {
            status: "pending".to_string(),
            ai_result: None,
            ai_completed_at: None,
            human_reviewer: None,
            human_approved_at: None,
            error: None,
            run_id: None,
            attempt: 0,
            started_at: None,
            duration: None,
            artifacts: None,
        }
    }
}

// ============================================================================
// Pipeline Cell Info (read from notebook_cells)
// ============================================================================

#[derive(Debug, Clone)]
struct PipelineCell {
    cell_id: String,
    board_id: String,
    cell_order: i32,
    content: String,
    pipeline_config: Option<PipelineStepConfig>,
    metadata_json: Option<String>,
}

// ============================================================================
// Compile: English → Pipeline Step Configs
// ============================================================================

/// Read notebook cells and generate pipeline step configs.
/// This creates a structured prompt for Lens to convert English descriptions
/// into executable pipeline steps.
pub fn compile_pipeline(board_id: &str) -> Result<serde_json::Value> {
    let cells = load_pipeline_cells(board_id)?;

    if cells.is_empty() {
        return Err(anyhow!("No cells found in board"));
    }

    // Build structured prompt for Lens
    let mut steps = Vec::new();

    for (i, cell) in cells.iter().enumerate() {
        let step_id = if let Some(ref config) = cell.pipeline_config {
            config.step_id.clone()
        } else {
            // Generate step_id from content (first few words, snake_case)
            generate_step_id(&cell.content, i)
        };

        // If cell already has pipeline config, preserve it
        if let Some(ref config) = cell.pipeline_config {
            steps.push(json!({
                "cell_id": cell.cell_id,
                "step_id": config.step_id,
                "title": first_line(&cell.content),
                "description": cell.content,
                "config": config,
                "already_compiled": true
            }));
        } else {
            // New cell — needs compilation
            steps.push(json!({
                "cell_id": cell.cell_id,
                "step_id": step_id,
                "title": first_line(&cell.content),
                "description": cell.content,
                "needs_compilation": true,
                "cell_order": cell.cell_order
            }));
        }
    }

    Ok(json!({
        "board_id": board_id,
        "total_cells": cells.len(),
        "steps": steps,
        "prompt": build_compile_prompt(&cells)
    }))
}

/// Build a structured prompt for Lens to compile English steps into pipeline configs
fn build_compile_prompt(cells: &[PipelineCell]) -> String {
    let mut prompt = String::from(
        "Convert the following workflow steps into a pipeline configuration. \
         For each step, determine:\n\
         1. A unique step_id (snake_case)\n\
         2. Dependencies (which steps must complete first)\n\
         3. Executor type: 'local' for lightweight tasks, 'lens' for AI analysis, \
            'cloud' for heavy processing, 'manual' for human-only steps\n\
         4. The command to execute (if applicable)\n\
         5. Timeout in seconds\n\n\
         Return JSON array of step configs.\n\n\
         Steps:\n"
    );

    for (i, cell) in cells.iter().enumerate() {
        let title = first_line(&cell.content);
        prompt.push_str(&format!("\nStep {} - {}:\n{}\n", i + 1, title, cell.content));
    }

    prompt.push_str("\nRespond with only a JSON array of pipeline step configs.");
    prompt
}

/// Apply compiled configs back to notebook cells
pub fn apply_compiled_configs(
    board_id: &str,
    compiled_steps: &[serde_json::Value],
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<usize> {
    let cells = load_pipeline_cells(board_id)?;
    let mut applied = 0;

    for step in compiled_steps {
        let cell_id = step["cell_id"].as_str()
            .or_else(|| {
                // Match by step index
                let idx = step["index"].as_u64()? as usize;
                cells.get(idx).map(|c| c.cell_id.as_str())
            });

        let Some(cell_id) = cell_id else { continue };
        let Some(cell) = cells.iter().find(|c| c.cell_id == cell_id) else { continue };

        // Build pipeline config from compiled step
        let config = PipelineStepConfig {
            step_id: step["step_id"].as_str().unwrap_or("unknown").to_string(),
            depends_on: step["depends_on"].as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            stage: step["stage"].as_str().map(String::from),
            executor: step["executor"].as_str().unwrap_or("local").to_string(),
            model: step["model"].as_str().map(String::from).or(Some("cyan-lens".to_string())),
            model_config: None,
            tools: step["tools"].as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            output_format: step["output_format"].as_str().unwrap_or("markdown").to_string(),
            command: step["command"].as_str().map(String::from),
            timeout_seconds: step["timeout_seconds"].as_u64(),
            retry_count: step["retry_count"].as_u64().map(|v| v as u32),
            auto_advance: step["auto_advance"].as_bool().unwrap_or(false),
            review_hold: step["review_hold"].as_bool().unwrap_or(false),
            waiting_on: step["waiting_on"].as_str().map(String::from),
            notifications: vec![],
            state: PipelineStepState::default(),
        };

        // Merge into existing metadata
        let mut metadata: serde_json::Value = cell.metadata_json.as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(json!({}));

        metadata["pipeline"] = serde_json::to_value(&config)?;

        // Update cell via command channel (triggers gossip)
        let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
            id: cell.cell_id.clone(),
            board_id: board_id.to_string(),
            cell_type: "markdown".to_string(),
            cell_order: cell.cell_order,
            content: Some(cell.content.clone()),
            output: None,
            collapsed: false,
            height: None,
            metadata_json: Some(metadata.to_string()),
        });

        applied += 1;
    }

    Ok(applied)
}

// ============================================================================
// Run: Execute Pipeline DAG
// ============================================================================

// ============================================================================
// Dashboard producer (DASHBOARD_CONTRACT §A/§C) — additive, receive-only events
// emitted from the REAL run path, tagged with tenant/run/board/workflow/stage/plugin.
// ============================================================================

/// The scoping keys carried on every dashboard event of a run.
struct RunTags {
    tenant_id: String,
    run_id: String,
    board_id: String,
    workflow_id: String,
}

impl RunTags {
    fn run_started(&self, workflow_label: String, total_steps: u32, started_at: i64) -> SwiftEvent {
        SwiftEvent::WorkflowRunStarted {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id.clone(),
            board_id: self.board_id.clone(),
            workflow_id: self.workflow_id.clone(),
            workflow_label,
            total_steps,
            started_at,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn step_state(
        &self,
        step_id: &str,
        name: &str,
        stage: &str,
        state: StepState,
        actor: Actor,
        plugin: Option<String>,
        at: i64,
    ) -> SwiftEvent {
        SwiftEvent::StepStateChanged {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id.clone(),
            board_id: self.board_id.clone(),
            workflow_id: self.workflow_id.clone(),
            step_id: step_id.to_string(),
            name: name.to_string(),
            stage: stage.to_string(),
            state: state.as_str().to_string(),
            actor: actor.as_str().to_string(),
            plugin,
            at,
        }
    }

    fn step_progress(&self, step_id: &str, stage: &str, processed: u64, total: u64, current_item: &str) -> SwiftEvent {
        SwiftEvent::StepProgress {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id.clone(),
            board_id: self.board_id.clone(),
            workflow_id: self.workflow_id.clone(),
            step_id: step_id.to_string(),
            stage: stage.to_string(),
            processed,
            total,
            current_item: Some(current_item.to_string()),
            detail: None,
        }
    }

    fn approval_requested(&self, step_id: &str, name: &str, stage: &str, requested_at: i64) -> SwiftEvent {
        SwiftEvent::ApprovalRequested {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id.clone(),
            board_id: self.board_id.clone(),
            workflow_id: self.workflow_id.clone(),
            step_id: step_id.to_string(),
            name: name.to_string(),
            stage: stage.to_string(),
            requested_at,
        }
    }

    fn run_finished(&self, state: &str, finished_at: i64) -> SwiftEvent {
        SwiftEvent::WorkflowRunFinished {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id.clone(),
            board_id: self.board_id.clone(),
            workflow_id: self.workflow_id.clone(),
            state: state.to_string(),
            finished_at,
        }
    }

    fn stats(&self, snapshot: crate::dashboard::DashboardSnapshot) -> SwiftEvent {
        SwiftEvent::WorkflowStatsUpdated {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id.clone(),
            board_id: self.board_id.clone(),
            workflow_id: self.workflow_id.clone(),
            snapshot,
        }
    }
}

/// The tenant a workflow run is billed to: the board's group (matches mesh
/// `tenant=group_id`), falling back to `CYAN_TENANT_ID` / `"device"`.
fn workflow_tenant(board_id: &str) -> String {
    storage::board_get_group_id(board_id)
        .filter(|g| !g.is_empty())
        .or_else(|| std::env::var("CYAN_TENANT_ID").ok())
        .unwrap_or_else(|| "device".to_string())
}

/// Human label for the workflow surface (the board name).
fn workflow_label(board_id: &str) -> String {
    storage::db()
        .lock()
        .ok()
        .and_then(|conn| {
            conn.query_row(
                "SELECT name FROM objects WHERE id = ?1 AND type = 'whiteboard' LIMIT 1",
                rusqlite::params![board_id],
                |row| row.get::<_, String>(0),
            )
            .ok()
        })
        .unwrap_or_else(|| board_id.to_string())
}

/// The stage a step belongs to (explicit `stage`, else the step is its own stage).
fn step_stage(config: &PipelineStepConfig) -> String {
    config.stage.clone().unwrap_or_else(|| config.step_id.clone())
}

/// Deterministic step inference (WORKFLOW_MATERIALIZATION): map an English step's
/// text to a (stage, cyan-media tool) without an LLM round-trip. The English still
/// drives the lens ReAct at RUN time; this only materializes the DAG so Compile is
/// instant and reliable (never "0 steps"). Returns the pipeline stage and, when a
/// media verb is recognized, the cyan-media tool the step should bind to.
/// Authored-executor convention (REVIEW_LOOP_ONE_BOARD Stage 3): placeholder /
/// manual steps and the await-sense park are HUMAN gates; everything else
/// defaults to the lens (creative) unless a deterministic bind claims it.
fn infer_executor(content: &str) -> String {
    let c = content.to_lowercase();
    if c.contains("placeholder") || c.starts_with("manual:") || c.contains("(manual)")
        || is_await_sense(content)
    {
        "manual".to_string()
    } else {
        "lens".to_string()
    }
}

/// The await-sense park marker: "await … note(s)/review/feedback".
fn is_await_sense(content: &str) -> bool {
    let c = content.to_lowercase();
    c.contains("await") && (c.contains("note") || c.contains("review") || c.contains("feedback"))
}

fn infer_step(content: &str) -> (String, Option<String>) {
    let c = content.to_lowercase();
    let has = |kws: &[&str]| kws.iter().any(|k| c.contains(k));
    // Order matters: most specific verbs first.
    if has(&["transcribe", "caption", "subtitle", "transcript"]) {
        ("analyze".into(), Some("transcribe".into()))
    } else if has(&["loudness", "lufs", "audio qc", "qc audio", "-14"]) {
        ("analyze".into(), Some("qc_loudness".into()))
    } else if has(&["black", "freeze"]) {
        ("analyze".into(), Some("qc_black_freeze".into()))
    } else if has(&["thumbnail", "poster", "frame grab", "framegrab", "keyframe"]) {
        ("deliver".into(), Some("thumbnail".into()))
    } else if has(&["proxy", "preview"]) {
        ("transform".into(), Some("proxy".into()))
    } else if has(&["transcode", "convert", "codec", "rewrap", "container"]) {
        ("transform".into(), Some("transcode".into()))
    } else if has(&["extract audio", "split audio", "demux"]) {
        ("transform".into(), Some("extract_audio".into()))
    } else if has(&["ingest", "probe", "qc", "inspect", "metadata", "format", "resolution", "duration", "codec check"]) {
        ("ingest".into(), Some("probe".into()))
    } else if has(&["package", "deliver", "master", "export", "wrap up", "ship"]) {
        // Delivery/packaging has no direct cyan-media tool — a lens summary step.
        ("deliver".into(), None)
    } else {
        // Generic analysis step (lens ReAct over the English, no fixed tool).
        ("analyze".into(), None)
    }
}

/// The plugin a step dispatches to on-device, if it is an `mcp_tool` step.
fn step_plugin(cell: &PipelineCell) -> Option<String> {
    cell.metadata_json
        .as_ref()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|m| {
            m.get("mcp_tool")
                .and_then(|t| t.get("plugin_id"))
                .and_then(|p| p.as_str())
                .map(String::from)
        })
}

// ============================================================================
// Wave-concurrent executor (Round 7)
//
// One executor, two entry conditions (WORKFLOW_MATERIALIZATION §1):
//   • A Lens `PhysicalPlan` is present  → run its waves WAVE-CONCURRENTLY
//     (independent steps in a wave run in parallel; gates are branch barriers;
//      cache hits reuse the prior artifact; batching caps in-flight concurrency).
//   • No plan (Lens unreachable / not compiled) → the existing SEQUENTIAL
//     toposort — the offline fallback, byte-for-byte the prior behavior.
// Both paths emit the SAME dashboard exec events (DASHBOARD_CONTRACT §A) so the
// dashboard lights up identically either way.
// ============================================================================

/// An owned, `'static` snapshot of one step — everything a (possibly spawned)
/// execution task needs, with no borrow back into the cell/config tables.
#[derive(Debug, Clone)]
struct ExecStep {
    step_id: String,
    cell_id: String,
    content: String,
    stage: String,
    name: String,
    plugin: Option<String>,
    executor: String,
    depends_on: Vec<String>,
    /// The cell's `mcp_tool` spec, threaded into the executor so the on-device
    /// MCP-tool path can fire from a real run (see `parse_mcp_tool_step`).
    mcp_tool: Option<serde_json::Value>,
    status: String,
    auto_advance: bool,
    notifications: Vec<StepNotification>,
}

/// What running one step produced — folded into the run's accumulator. `result`
/// is `None` for a gate (no result line, matching the prior sequential output).
struct StepOutcome {
    obs: StepObs,
    result: Option<serde_json::Value>,
    failed: bool,
    stage: String,
    name: String,
}

/// Mutable run-wide accumulator (the snapshot inputs + the result lines). Lives in
/// the run task; spawned step tasks return `StepOutcome`s that get folded in here —
/// so there are no locks on this hot state.
struct RunAccum {
    obs: RunObs,
    results: Vec<serde_json::Value>,
    any_failed: bool,
    any_awaiting: bool,
    processed: u64,
    current_stage: Option<String>,
    current_item: Option<String>,
}

impl RunAccum {
    fn fold(&mut self, o: StepOutcome) {
        self.current_stage = Some(o.stage);
        self.current_item = Some(o.name);
        if o.failed {
            self.any_failed = true;
        }
        if o.obs.state == StepState::AwaitingApproval {
            self.any_awaiting = true;
        }
        self.obs.record(o.obs);
        if let Some(r) = o.result {
            self.results.push(r);
        }
    }
}

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Execute a pipeline: load a Lens physical plan if the board has one, else run
/// sequentially. Signature unchanged (FFI/tests call this); plan loading is internal.
pub async fn run_pipeline(
    board_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<serde_json::Value> {
    let plan = load_physical_plan(board_id);
    run_pipeline_with_plan(board_id, plan, command_tx, event_tx).await
}

/// The run a board is CURRENTLY executing — the keystone of the single-run state
/// machine. ONE run-id spans the whole step-through: it is the id stamped on any step
/// that has already run, REUSED on every resume (approve → run the next step), and it
/// lives in the persisted cell state so leaving the board and returning reloads the
/// SAME run. Returns `None` (⇒ a fresh run-id) only when no step has run yet, or when
/// every step is fully resolved (the prior run finished → the next Run is new).
fn active_run_id(steps: &[(&PipelineCell, &PipelineStepConfig)]) -> Option<String> {
    let resolved = |s: &str| matches!(s, "human_approved" | "skipped" | "failed");
    let touched = steps.iter().any(|(_, c)| {
        let s = c.state.status.as_str();
        !s.is_empty() && s != "pending"
    });
    if !touched {
        return None; // fresh run
    }
    if steps.iter().all(|(_, c)| resolved(&c.state.status)) {
        return None; // prior run fully resolved → next Run is a new run
    }
    steps.iter().find_map(|(_, c)| {
        let s = c.state.status.as_str();
        if !s.is_empty() && s != "pending" {
            c.state.run_id.clone()
        } else {
            None
        }
    })
}

/// The executor core. `plan = Some(..)` ⇒ wave-concurrent; `None` ⇒ sequential
/// toposort fallback. Exposed so tests (and a future compile path) can hand the
/// plan in directly without the storage round-trip.
pub async fn run_pipeline_with_plan(
    board_id: &str,
    plan: Option<crate::exec_plan::PhysicalPlan>,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<serde_json::Value> {
    let cells = load_pipeline_cells(board_id)?;

    // Collect steps with pipeline configs.
    let steps: Vec<_> = cells.iter()
        .filter_map(|c| c.pipeline_config.as_ref().map(|p| (c, p)))
        .collect();

    if steps.is_empty() {
        return Err(anyhow!("No pipeline steps configured. Run /pipeline compile first."));
    }

    // Build the DAG with petgraph — used by the sequential path and, either way, to
    // validate the graph is acyclic before we run anything.
    let mut graph = DiGraph::<String, ()>::new();
    let mut node_map: HashMap<String, NodeIndex> = HashMap::new();
    let mut exec_steps: HashMap<String, ExecStep> = HashMap::new();

    for (cell, config) in &steps {
        let idx = graph.add_node(config.step_id.clone());
        node_map.insert(config.step_id.clone(), idx);
        exec_steps.insert(config.step_id.clone(), ExecStep {
            step_id: config.step_id.clone(),
            cell_id: cell.cell_id.clone(),
            content: cell.content.clone(),
            stage: step_stage(config),
            name: first_line(&cell.content),
            plugin: step_plugin(cell),
            executor: config.executor.clone(),
            depends_on: config.depends_on.clone(),
            mcp_tool: cell.metadata_json.as_ref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                .and_then(|m| m.get("mcp_tool").cloned()),
            status: config.state.status.clone(),
            auto_advance: config.auto_advance,
            notifications: config.notifications.clone(),
        });
    }

    for (_cell, config) in &steps {
        if let Some(&to_idx) = node_map.get(&config.step_id) {
            for dep in &config.depends_on {
                if let Some(&from_idx) = node_map.get(dep) {
                    graph.add_edge(from_idx, to_idx, ());
                }
            }
        }
    }

    let order = toposort(&graph, None)
        .map_err(|_| anyhow!("Pipeline has circular dependencies"))?;

    // Single-run state machine: REUSE the active run-id (resume) or mint a fresh one.
    let run_id = active_run_id(&steps)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()[..8].to_string());
    let total_steps = order.len() as u32;

    // ── Dashboard producer: tag this run and announce it (DASHBOARD_CONTRACT §A). ──
    let tags = std::sync::Arc::new(RunTags {
        tenant_id: workflow_tenant(board_id),
        run_id: run_id.clone(),
        board_id: board_id.to_string(),
        workflow_id: board_id.to_string(), // the board IS the workflow surface
    });
    let label = workflow_label(board_id);
    let _ = event_tx.send(tags.run_started(label.clone(), total_steps, now_ts()));

    let mut acc = RunAccum {
        obs: RunObs::new(tags.tenant_id.clone(), board_id, run_id.clone(), board_id, label, total_steps as u64),
        results: Vec::new(),
        any_failed: false,
        any_awaiting: false,
        processed: 0,
        current_stage: None,
        current_item: None,
    };

    // CUMULATIVE COST (single source of truth): seed every ALREADY-RUN step's cost
    // into this run's obs from its persisted duration, so the live snapshot total is
    // the SAME monotonic sum `pipeline_status` reconstructs — and matches across the
    // step-through (it grows, never resets). Already-run steps are skipped/paused-at by
    // the executor, so this is counted exactly ONCE (no double-charge).
    for (cell, config) in &steps {
        if matches!(config.state.status.as_str(), "human_approved" | "ai_complete" | "skipped" | "done")
            && let Some(dur) = config.state.duration {
                let gpu_ms = (dur * 1000.0) as u64;
                acc.obs.record(StepObs {
                    step_id: config.step_id.clone(),
                    name: first_line(&cell.content),
                    stage: step_stage(config),
                    actor: Actor::Ai,
                    plugin: step_plugin(cell),
                    state: StepState::Approved,
                    wall_ms: gpu_ms,
                    gate_ms: 0,
                    cost: StepCost { gpu_ms, ..StepCost::default() },
                });
                acc.processed += 1;
            }
    }

    let (mode, peak) = match plan {
        Some(plan) => {
            tracing::info!("Pipeline run {} started: {} steps, WAVE-CONCURRENT (plan: {} waves)", run_id, total_steps, plan.waves.len());
            let peak = execute_waves(&plan, &exec_steps, board_id, &tags, total_steps as u64, command_tx, event_tx, &mut acc).await;
            ("wave", peak)
        }
        None => {
            tracing::info!("Pipeline run {} started: {} steps in DAG order (sequential)", run_id, total_steps);
            let peak = execute_sequential(&order, &graph, &exec_steps, board_id, &tags, total_steps as u64, command_tx, event_tx, &mut acc).await;
            ("sequential", peak)
        }
    };

    // Fire pipeline_complete notifications iff every step was already terminal
    // before the run (preserves the prior behavior, computed on original state).
    let all_done = exec_steps.values().all(|s| s.status == "human_approved" || s.status == "skipped");
    if all_done {
        for s in exec_steps.values() {
            fire_notifications(&s.notifications, "pipeline_complete", board_id, &s.step_id, event_tx);
        }
    }

    // ── Dashboard producer: the rolled-up read-model + the run-finished marker. ──
    let finished_at = now_ts();
    let snapshot = acc.obs.snapshot(finished_at, acc.current_item.clone(), acc.current_stage.clone());
    let _ = event_tx.send(tags.stats(snapshot));
    // "awaiting_approval" = the run PAUSED at a per-step gate (Rick approves to
    // resume). Only truly "done" when no step is still awaiting and none failed.
    let run_state = if acc.any_failed {
        "failed"
    } else if acc.any_awaiting {
        "awaiting_approval"
    } else {
        "done"
    };
    let _ = event_tx.send(tags.run_finished(run_state, finished_at));

    Ok(json!({
        "run_id": run_id,
        "board_id": board_id,
        "mode": mode,
        "peak_concurrency": peak,
        "steps_executed": acc.results.len(),
        "results": acc.results
    }))
}

/// SEQUENTIAL fallback — the prior toposort loop, run one step at a time. This is
/// the offline path (no Lens plan); behavior is unchanged from before Round 7.
/// Returns peak in-flight concurrency = 1 (sequential).
#[allow(clippy::too_many_arguments)]
async fn execute_sequential(
    order: &[NodeIndex],
    graph: &DiGraph<String, ()>,
    exec_steps: &HashMap<String, ExecStep>,
    board_id: &str,
    tags: &std::sync::Arc<RunTags>,
    total_steps: u64,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
    acc: &mut RunAccum,
) -> usize {
    for node_idx in order {
        let step_id = &graph[*node_idx];
        let Some(step) = exec_steps.get(step_id) else { continue };

        // Already resolved → skip. Its cost is seeded from the persisted duration so
        // the run total stays CUMULATIVE and is never double-counted by resuming.
        if step.status == "human_approved" || step.status == "skipped" {
            tracing::info!("Step {} already {}, skipping", step_id, step.status);
            continue;
        }
        // Already ran, AWAITING approval → this is the current gate. Do NOT re-execute
        // it (re-running double-charged its cost — the $0.30→$0.60 bug). Re-surface the
        // gate and PAUSE the run here; approving it RESUMES the SAME run at the next step.
        if step.status == "ai_complete" {
            let now = now_ts();
            let _ = event_tx.send(tags.step_state(&step.step_id, &step.name, &step.stage, StepState::AwaitingApproval, Actor::Ai, step.plugin.clone(), now));
            let _ = event_tx.send(tags.approval_requested(&step.step_id, &step.name, &step.stage, now));
            acc.any_awaiting = true;
            tracing::info!("Run paused: step '{}' awaiting approval (resume)", step_id);
            break;
        }
        // A failed step stays failed until an explicit Retry resets it to pending.
        if step.status == "failed" {
            acc.any_failed = true;
            continue;
        }

        // Manual steps surface as a gate (awaiting human approval).
        if step.executor == "manual" {
            acc.fold(gate_outcome(board_id, tags, step, command_tx, event_tx));
            continue;
        }

        // Dependency gate (the prior semantics): a dep is "met" if approved, or
        // auto-advancing and AI-complete; unknown deps are considered met.
        let deps_met = step.depends_on.iter().all(|dep| {
            exec_steps.get(dep)
                .map(|c| c.status == "human_approved" || (c.auto_advance && c.status == "ai_complete"))
                .unwrap_or(true)
        });
        if !deps_met {
            acc.fold(pending_outcome(board_id, tags, step, "dependencies_pending", command_tx, event_tx));
            continue;
        }

        let snapshot = acc.processed;
        let outcome = exec_one_step(
            board_id.to_string(), tags.clone(), total_steps, snapshot,
            step.clone(), command_tx.clone(), event_tx.clone(),
        ).await;
        let awaiting = outcome.obs.state == StepState::AwaitingApproval;
        let failed = outcome.failed;
        acc.processed += 1;
        acc.fold(outcome);
        // LIVE cost/progress: push an incremental snapshot after EACH step so the
        // Dashboard's this-workflow cost + step counts INCREMENT step-by-step
        // (not just once at run end) — the "$0.00 stuck" fix.
        let snap = acc.obs.snapshot(now_ts(), acc.current_item.clone(), acc.current_stage.clone());
        let _ = event_tx.send(tags.stats(snap));
        // S — FAILURE HALTS THE RUN: a failed step stops execution here. Downstream
        // dependents do NOT run (no orphan parallel steps); the failed step is left
        // red + actionable (Retry resumes from it, Reject aborts).
        if failed {
            tracing::info!("Run halted: step '{}' FAILED — downstream blocked", step_id);
            break;
        }
        // PER-STEP PAUSE: stop at the first step awaiting approval. Rick approves it
        // (cyan_pipeline_approve), which resumes the run at the next step.
        if awaiting {
            tracing::info!("Run paused: step '{}' awaiting approval", step_id);
            break;
        }
    }
    1
}

/// WAVE-CONCURRENT executor — runs the Lens physical plan. Waves run in `index`
/// order; within a wave, batches run one after another and each batch's steps run
/// concurrently (a `JoinSet` task per step). Cache hits reuse the prior artifact;
/// gate barriers stall only their own branch. Returns the peak in-flight degree
/// (the largest batch the executor launched concurrently).
#[allow(clippy::too_many_arguments)]
async fn execute_waves(
    plan: &crate::exec_plan::PhysicalPlan,
    exec_steps: &HashMap<String, ExecStep>,
    board_id: &str,
    tags: &std::sync::Arc<RunTags>,
    total_steps: u64,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
    acc: &mut RunAccum,
) -> usize {
    let mut peak = 0usize;

    for wave in plan.ordered_waves() {
        // S — FAILURE HALTS THE RUN: once any step has failed, stop launching further
        // waves so downstream dependents never run (Retry resumes, Reject aborts).
        // PER-STEP PAUSE: likewise stop once a step is awaiting approval.
        if acc.any_failed || acc.any_awaiting {
            break;
        }
        for batch in wave.ordered_batches() {
            // Classify each step: skip / cache-hit / gate / stalled-behind-gate /
            // execute. Only "execute" steps are spawned concurrently.
            let mut sync_outcomes: Vec<StepOutcome> = Vec::new();
            let mut to_exec: Vec<ExecStep> = Vec::new();

            for sid in &batch {
                let Some(step) = exec_steps.get(sid) else {
                    tracing::warn!("Plan references step {} not in board; skipping", sid);
                    continue;
                };
                if step.status == "human_approved" || step.status == "skipped" {
                    continue;
                }
                let planned = wave.step(sid);

                // Cache hit → reuse the prior artifact, skip execution.
                if planned.map(|p| p.cache_hit).unwrap_or(false) {
                    sync_outcomes.push(cache_hit_outcome(board_id, tags, step, command_tx, event_tx));
                    continue;
                }

                // A human-approval gate (manual step or plan `is_gate`) surfaces as
                // awaiting-approval; it does not block independent branches.
                if step.executor == "manual" || planned.map(|p| p.is_gate).unwrap_or(false) {
                    sync_outcomes.push(gate_outcome(board_id, tags, step, command_tx, event_tx));
                    continue;
                }

                // Branch barrier: a step behind an unapproved gate stalls (only its
                // branch). A gate counts as satisfied once it is `human_approved`
                // (e.g. on a re-run after approval).
                if let Some(gate) = planned.and_then(|p| p.gate_barrier.clone())
                    && !gate_satisfied(exec_steps, &gate)
                {
                    sync_outcomes.push(pending_outcome(board_id, tags, step, "gate_pending", command_tx, event_tx));
                    continue;
                }

                to_exec.push(step.clone());
            }

            for o in sync_outcomes {
                acc.fold(o);
            }

            if to_exec.is_empty() {
                continue;
            }

            // Launch the batch concurrently. The batch size IS the in-flight degree
            // (we spawn all, then await all) — bounded by the plan's concurrency cap.
            peak = peak.max(to_exec.len());
            let snapshot = acc.processed;
            let mut js: tokio::task::JoinSet<StepOutcome> = tokio::task::JoinSet::new();
            for step in to_exec {
                js.spawn(exec_one_step(
                    board_id.to_string(), tags.clone(), total_steps, snapshot,
                    step, command_tx.clone(), event_tx.clone(),
                ));
            }
            while let Some(joined) = js.join_next().await {
                match joined {
                    Ok(outcome) => {
                        acc.processed += 1;
                        acc.fold(outcome);
                        // LIVE cost/progress: incremental snapshot after each step
                        // completes so the Dashboard cost INCREMENTS step-by-step.
                        let snap = acc.obs.snapshot(now_ts(), acc.current_item.clone(), acc.current_stage.clone());
                        let _ = event_tx.send(tags.stats(snap));
                    }
                    Err(e) => tracing::error!("wave step task failed to join: {}", e),
                }
            }
        }
    }

    peak
}

/// A gate is satisfied (its branch may proceed) once it is `human_approved`.
fn gate_satisfied(exec_steps: &HashMap<String, ExecStep>, gate_id: &str) -> bool {
    exec_steps.get(gate_id).map(|s| s.status == "human_approved").unwrap_or(false)
}

/// Run ONE step: emit running → progress, execute it, emit the terminal state, and
/// build its obs/result. Owned args so it can be spawned. Shared by both paths.
async fn exec_one_step(
    board_id: String,
    tags: std::sync::Arc<RunTags>,
    total_steps: u64,
    processed_snapshot: u64,
    step: ExecStep,
    command_tx: UnboundedSender<CommandMsg>,
    event_tx: UnboundedSender<SwiftEvent>,
) -> StepOutcome {
    let actor = Actor::Ai;
    tracing::info!("Executing step: {} (executor: {})", step.step_id, step.executor);

    if let Err(e) = update_step_state(&board_id, &step.cell_id, "running", None, None, &tags.run_id, &command_tx) {
        tracing::error!("step {} running-state write failed: {}", step.step_id, e);
    }
    let _ = event_tx.send(SwiftEvent::StatusUpdate {
        message: format!("Pipeline: step '{}' running", step.step_id),
    });
    let _ = event_tx.send(tags.step_state(&step.step_id, &step.name, &step.stage, StepState::Running, actor, step.plugin.clone(), now_ts()));
    let _ = event_tx.send(tags.step_progress(&step.step_id, &step.stage, processed_snapshot, total_steps, &step.name));

    let start = std::time::Instant::now();

    let dependency_outputs = gather_dependency_outputs(&board_id, &step.depends_on);
    eprintln!("📺 PIPELINE: Step {} has {} dependency outputs", step.step_id, dependency_outputs.len());

    let mut metadata = crate::pipeline_executor::find_asset_metadata(&board_id).unwrap_or_else(|| json!({}));
    if let Some(mcp_tool) = step.mcp_tool.clone() {
        metadata["mcp_tool"] = mcp_tool;
    }
    let metadata = Some(metadata);

    let result = crate::pipeline_executor::execute_pipeline_step(
        &board_id, &step.step_id, &step.content, &step.executor,
        metadata, dependency_outputs, &command_tx, &event_tx,
    ).await.map(|(summary, _findings)| summary);

    let duration = start.elapsed().as_secs_f64();
    let wall_ms = (duration * 1000.0) as u64;
    // Cost rail: compute wall time is a GPU-time proxy (DASHBOARD_CONTRACT §C).
    let cost = StepCost { gpu_ms: wall_ms, ..StepCost::default() };

    match result {
        Ok(output) => {
            tracing::info!("Step {} completed in {:.1}s", step.step_id, duration);
            // PER-STEP HUMAN-IN-THE-LOOP: the AI work is done; with per-step gating
            // ON (auto_advance=false, the compile default) the step then AWAITS human
            // approval before the run advances — so Rick steps THROUGH the workflow,
            // seeing each step's output and Approve/Reject. auto_advance=true ⇒ Done.
            let needs_approval = !step.auto_advance;
            let cell_status = if needs_approval { "ai_complete" } else { "human_approved" };
            if let Err(e) = update_step_state_full(&board_id, &step.cell_id, cell_status, Some(&output), None, &tags.run_id, Some(duration), &command_tx) {
                tracing::error!("step {} done-state write failed: {}", step.step_id, e);
            }
            let _ = event_tx.send(SwiftEvent::StatusUpdate {
                message: format!("Pipeline: step '{}' complete ({:.1}s)", step.step_id, duration),
            });
            let now = now_ts();
            let state = if needs_approval { StepState::AwaitingApproval } else { StepState::Done };
            let _ = event_tx.send(tags.step_state(&step.step_id, &step.name, &step.stage, state, actor, step.plugin.clone(), now));
            if needs_approval {
                // Surface the per-step gate (the step's output is already persisted).
                let _ = event_tx.send(tags.approval_requested(&step.step_id, &step.name, &step.stage, now));
            }
            fire_notifications(&step.notifications, "ai_complete", &board_id, &step.step_id, &event_tx);
            StepOutcome {
                obs: StepObs {
                    step_id: step.step_id.clone(), name: step.name.clone(), stage: step.stage.clone(),
                    actor, plugin: step.plugin.clone(), state, wall_ms, gate_ms: 0, cost,
                },
                result: Some(json!({ "step_id": step.step_id, "status": cell_status, "duration": duration, "output_length": output.len() })),
                failed: false,
                stage: step.stage,
                name: step.name,
            }
        }
        Err(e) => {
            tracing::error!("Step {} failed: {}", step.step_id, e);
            if let Err(we) = update_step_state(&board_id, &step.cell_id, "failed", None, Some(&e.to_string()), &tags.run_id, &command_tx) {
                tracing::error!("step {} failed-state write failed: {}", step.step_id, we);
            }
            let _ = event_tx.send(tags.step_state(&step.step_id, &step.name, &step.stage, StepState::Failed, actor, step.plugin.clone(), now_ts()));
            fire_notifications(&step.notifications, "failed", &board_id, &step.step_id, &event_tx);
            StepOutcome {
                obs: StepObs {
                    step_id: step.step_id.clone(), name: step.name.clone(), stage: step.stage.clone(),
                    actor, plugin: step.plugin.clone(), state: StepState::Failed, wall_ms, gate_ms: 0, cost,
                },
                result: Some(json!({ "step_id": step.step_id, "status": "failed", "error": e.to_string() })),
                failed: true,
                stage: step.stage,
                name: step.name,
            }
        }
    }
}

/// A manual/gate step → awaiting-approval (DASHBOARD_CONTRACT §A). No execution.
fn gate_outcome(
    board_id: &str,
    tags: &RunTags,
    step: &ExecStep,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> StepOutcome {
    tracing::info!("Step {} is a gate, awaiting approval", step.step_id);
    if let Err(e) = update_step_state(board_id, &step.cell_id, "scheduled", None, None, &tags.run_id, command_tx) {
        tracing::error!("step {} gate-state write failed: {}", step.step_id, e);
    }
    let now = now_ts();
    let _ = event_tx.send(tags.step_state(&step.step_id, &step.name, &step.stage, StepState::AwaitingApproval, Actor::Human, step.plugin.clone(), now));
    let _ = event_tx.send(tags.approval_requested(&step.step_id, &step.name, &step.stage, now));
    StepOutcome {
        obs: StepObs {
            step_id: step.step_id.clone(), name: step.name.clone(), stage: step.stage.clone(),
            actor: Actor::Human, plugin: step.plugin.clone(), state: StepState::AwaitingApproval,
            wall_ms: 0, gate_ms: 0, cost: StepCost::default(),
        },
        result: None,
        failed: false,
        stage: step.stage.clone(),
        name: step.name.clone(),
    }
}

/// A step whose deps/gate aren't met → pending (scheduled). No execution.
fn pending_outcome(
    board_id: &str,
    tags: &RunTags,
    step: &ExecStep,
    reason: &str,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> StepOutcome {
    tracing::info!("Step {} pending ({})", step.step_id, reason);
    if let Err(e) = update_step_state(board_id, &step.cell_id, "scheduled", None, None, &tags.run_id, command_tx) {
        tracing::error!("step {} pending-state write failed: {}", step.step_id, e);
    }
    let _ = event_tx.send(tags.step_state(&step.step_id, &step.name, &step.stage, StepState::Pending, Actor::Ai, step.plugin.clone(), now_ts()));
    StepOutcome {
        obs: StepObs {
            step_id: step.step_id.clone(), name: step.name.clone(), stage: step.stage.clone(),
            actor: Actor::Ai, plugin: step.plugin.clone(), state: StepState::Pending,
            wall_ms: 0, gate_ms: 0, cost: StepCost::default(),
        },
        result: Some(json!({ "step_id": step.step_id, "status": "scheduled", "reason": reason })),
        failed: false,
        stage: step.stage.clone(),
        name: step.name.clone(),
    }
}

/// A cache-hit step → reuse the prior persisted artifact (the cell's existing
/// output), mark done, NO execution (DASHBOARD_CONTRACT §A; plan `cache_hit`).
fn cache_hit_outcome(
    board_id: &str,
    tags: &RunTags,
    step: &ExecStep,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> StepOutcome {
    tracing::info!("Step {} cache hit — reusing prior artifact", step.step_id);
    let prior = read_cell_output(board_id, &step.cell_id);
    if let Err(e) = update_step_state_full(board_id, &step.cell_id, "ai_complete", prior.as_deref(), None, &tags.run_id, Some(0.0), command_tx) {
        tracing::error!("step {} cache-state write failed: {}", step.step_id, e);
    }
    let _ = event_tx.send(SwiftEvent::StatusUpdate {
        message: format!("Pipeline: step '{}' cache hit (reused artifact)", step.step_id),
    });
    let _ = event_tx.send(tags.step_state(&step.step_id, &step.name, &step.stage, StepState::Done, Actor::Ai, step.plugin.clone(), now_ts()));
    StepOutcome {
        obs: StepObs {
            step_id: step.step_id.clone(), name: step.name.clone(), stage: step.stage.clone(),
            actor: Actor::Ai, plugin: step.plugin.clone(), state: StepState::Done,
            wall_ms: 0, gate_ms: 0, cost: StepCost::default(), // reuse = no compute cost
        },
        result: Some(json!({ "step_id": step.step_id, "status": "cache_hit" })),
        failed: false,
        stage: step.stage.clone(),
        name: step.name.clone(),
    }
}

/// Read a cell's current persisted `output` (the reusable artifact for a cache hit).
fn read_cell_output(_board_id: &str, cell_id: &str) -> Option<String> {
    let conn = storage::db().lock().ok()?;
    conn.query_row(
        "SELECT output FROM notebook_cells WHERE id = ?1",
        rusqlite::params![cell_id],
        |row| row.get::<_, Option<String>>(0),
    ).ok().flatten()
}

/// Load the board's Lens physical plan, if one has been persisted (the compile path
/// stores it on the board `objects.data` as `{"physical_plan": {...}}`). Today the
/// Lens compile→backend wiring is deferred, so this is normally `None` ⇒ sequential.
/// A malformed plan degrades to `None` (the run still proceeds, sequentially).
fn load_physical_plan(board_id: &str) -> Option<crate::exec_plan::PhysicalPlan> {
    let conn = storage::db().lock().ok()?;
    let data: Option<String> = conn.query_row(
        "SELECT data FROM objects WHERE id = ?1 AND type = 'whiteboard' LIMIT 1",
        rusqlite::params![board_id],
        |row| row.get::<_, Option<String>>(0),
    ).ok().flatten();
    drop(conn);
    let v: serde_json::Value = serde_json::from_str(&data?).ok()?;
    serde_json::from_value(v.get("physical_plan")?.clone()).ok()
}

// ============================================================================
// Step Executors
// ============================================================================

/// Execute a step locally via subprocess
async fn execute_local_step(config: &PipelineStepConfig) -> Result<String> {
    let command = config.command.as_ref()
        .ok_or_else(|| anyhow!("No command specified for local step"))?;

    let timeout = config.timeout_seconds.unwrap_or(300);

    let output = tokio::time::timeout(
        std::time::Duration::from_secs(timeout),
        TokioCommand::new("sh")
            .arg("-c")
            .arg(command)
            .output()
    ).await
        .map_err(|_| anyhow!("Step timed out after {}s", timeout))?
        .map_err(|e| anyhow!("Process error: {}", e))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!("Command failed (exit {}): {}", output.status, stderr))
    }
}

/// Call vLLM directly for generation (OpenAI-compatible API)
pub async fn call_vllm_public(prompt: &str, max_tokens: u32, temperature: f32) -> Result<String> {
    call_vllm(prompt, max_tokens, temperature).await
}

async fn call_vllm(prompt: &str, max_tokens: u32, temperature: f32) -> Result<String> {
    let vllm_url = std::env::var("CYAN_VLLM_URL")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    let model = std::env::var("CYAN_VLLM_MODEL")
        .unwrap_or_else(|_| "/opt/models/llama-3.3-70b-awq".to_string());

    let client = reqwest::Client::new();

    let response = client.post(format!("{}/v1/chat/completions", vllm_url))
        .json(&json!({
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": temperature
        }))
        .timeout(std::time::Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| anyhow!("vLLM API error: {}", e))?;

    if response.status().is_success() {
        let body: serde_json::Value = response.json().await
            .map_err(|e| anyhow!("vLLM parse error: {}", e))?;
        let content = body["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("No response");
        Ok(content.to_string())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("vLLM returned {}: {}", status, &body[..body.len().min(200)]))
    }
}


/// Call Claude API as fallback when vLLM is unavailable
pub async fn call_claude_fallback(prompt: &str, max_tokens: u32) -> anyhow::Result<String> {
    let api_key = std::env::var("ANTHROPIC_API_KEY")
        .or_else(|_| {
            let home = std::env::var("HOME")?;
            let env_str = std::fs::read_to_string(format!("{}/Documents/.env", home))
                .map_err(|_| std::env::VarError::NotPresent)?;
            env_str.lines()
                .find(|l| l.starts_with("ANTHROPIC_API_KEY="))
                .map(|l| l.trim_start_matches("ANTHROPIC_API_KEY=").trim().to_string())
                .ok_or(std::env::VarError::NotPresent)
        })
        .map_err(|_| anyhow::anyhow!("No Anthropic API key found"))?;

    let client = reqwest::Client::new();
    let response = client.post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]
        }))
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Claude API error: {}", e))?;

    if response.status().is_success() {
        let body: serde_json::Value = response.json().await?;
        let text = body["content"][0]["text"].as_str().unwrap_or("No response");
        Ok(text.to_string())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow::anyhow!("Claude API returned {}: {}", status, &body[..body.len().min(200)]))
    }
}

/// Compile pipeline: send English cells to vLLM, get back step configs
pub async fn compile_via_llm(board_id: &str, command_tx: &UnboundedSender<CommandMsg>) -> Result<serde_json::Value> {
    let cells = load_pipeline_cells(board_id)?;

    if cells.is_empty() {
        return Err(anyhow!("No cells found in board"));
    }

    // Filter out cells that are just URLs (asset references), and — defense in depth
    // for boards an old build already polluted — cells whose content IS a raw result
    // blob: those are run output, never an authored step, and materializing one mints
    // a garbage `rawcontent…` step id from the serialized JSON (authored_ledger_test).
    let step_cells: Vec<_> = cells.iter()
        .filter(|c| {
            let trimmed = c.content.trim();
            !trimmed.starts_with("http://") && !trimmed.starts_with("https://")
                && !trimmed.is_empty()
                && !is_run_result_blob(trimmed)
        })
        .collect();

    if step_cells.is_empty() {
        return Err(anyhow!("No pipeline steps found (only asset references)"));
    }

    // DETERMINISTIC compile (WORKFLOW_MATERIALIZATION): materialize each English step
    // cell into a physical step config with NO LLM round-trip — so Compile is instant
    // and never yields "0 steps" when the 8B is slow or returns unparseable JSON. The
    // English content still drives the lens ReAct at RUN time; compile only lays out
    // the DAG: a linear chain (each step depends on the previous), executor=lens, an
    // inferred stage + cyan-media tool hint, and auto_advance=false so the operator
    // can step THROUGH the workflow with per-step approval.
    tracing::info!("Pipeline compile (deterministic): materializing {} steps", step_cells.len());
    let mut applied: u64 = 0;
    let mut prev_step_id: Option<String> = None;
    let mut configs: Vec<serde_json::Value> = Vec::new();

    for (i, cell) in step_cells.iter().enumerate() {
        let step_id = generate_step_id(&cell.content, i);
        let (stage, media_tool) = infer_step(&cell.content);

        // #5 — EDIT/RECOMPILE ROBUSTNESS: PRESERVE the persisted run state of an
        // UNCHANGED step (same content ⇒ same generated step_id) so re-Review/recompile
        // (after editing or ADDING steps) never wipes an in-progress run. An edited step
        // (content changed ⇒ new step_id) or a brand-new step starts pending — it must
        // re-run. Idempotent: recompiling an unrun board is a no-op on state.
        let preserved_state = cell
            .pipeline_config
            .as_ref()
            .filter(|c| c.step_id == step_id)
            .map(|c| c.state.clone())
            .unwrap_or_default();

        // S5 — PRESERVE the authored executor (esp. "manual", the human-approval gate) so a
        // Run/recompile never drops it to "lens" (which made the package/human step EXECUTE
        // forever instead of pausing as a gate). A cell with a bound config keeps its
        // executor; a brand-new/unconfigured cell defaults by AUTHORED CONVENTION
        // (REVIEW_LOOP_ONE_BOARD Stage 3): a step that says "placeholder" /
        // "manual:" / "(manual)" is a HUMAN step (the Pro Tools / Resolve "done"
        // placeholders — parked for Complete, never dispatched anywhere), and an
        // "await … notes/review" step is the AWAIT-SENSE PARK (a human-shaped
        // gate the app auto-approves when the reviewer's note is SENSED).
        let executor = cell
            .pipeline_config
            .as_ref()
            .map(|c| c.executor.clone())
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| infer_executor(&cell.content));

        // Rung-1 DETERMINISTIC BIND runs FIRST so the step's config carries the
        // TRUTH of where it executes. `command` is the authoring surface's
        // route label — for a bound step it must read `@plugin.tool`, never the
        // "cyan-lens" placeholder (found live: every bound step displayed
        // "unbound + send to AI (Lens)" because the view only had the
        // placeholder to render).
        let bind_outcome = crate::workflow_bind::bind_step(board_id, &cell.content);
        let bound_command = match &bind_outcome {
            crate::workflow_bind::BindOutcome::Bound(b) =>
                Some(format!("@{}.{}", b.plugin_id, b.tool)),
            _ => None,
        };

        // D/P-4 — REVIEW HOLD detection: an external upload FOR REVIEW is the
        // producer-review window. Its post-effect gate must park "waiting on
        // <the board's assigned reviewer>" (a REAL user) and hold until THAT
        // user approves — the window in which the reviewer watches the proxy
        // and leaves frame-anchored comments before the sense step reads them.
        let review_hold = is_review_upload(bound_command.as_deref(), &cell.content);
        let waiting_on = if review_hold { review_assignee(board_id) } else { None };

        let config = PipelineStepConfig {
            step_id: step_id.clone(),
            depends_on: prev_step_id.clone().into_iter().collect(),
            stage: Some(stage),
            executor: executor.clone(),
            model: Some("cyan-lens".to_string()),
            model_config: None,
            tools: media_tool.iter().cloned().collect(),
            output_format: "markdown".to_string(),
            command: bound_command,
            timeout_seconds: Some(300),
            retry_count: Some(1),
            auto_advance: false,
            review_hold,
            waiting_on,
            notifications: vec![],
            state: preserved_state,
        };

        // Merge into cell metadata: the pipeline config AND, when a media verb was
        // recognized, the mcp_tool hint the run path binds cyan-media through.
        let mut metadata: serde_json::Value = cell.metadata_json.as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(json!({}));
        metadata["pipeline"] = serde_json::to_value(&config)?;
        if is_await_sense(&cell.content) {
            // The AWAIT-SENSE PARK marker: the app's live loop senses Frame.io
            // notes while this gate is open and fires the SAME approve/resume
            // machinery when one lands — the loop is one continuous pass.
            metadata["await_sense"] = json!(true);
        }
        if let Some(tool) = &media_tool {
            metadata["mcp_tool"] = json!({ "plugin_id": "cyan-media", "tool": tool });
        }
        // Rung-1 DETERMINISTIC BIND (the load-bearing fix): an `@plugin.tool`
        // step whose required args resolve from inline `key=value` + `#file`
        // references binds at Review time — the run path dispatches it
        // through the LOCAL cyan-mcp host with zero LLM turns, so the
        // mechanical spine never needs the GPU. A miss is stamped for
        // authoring feedback and the step stays on the lens path (creative).
        match bind_outcome {
            crate::workflow_bind::BindOutcome::Bound(b) => {
                metadata["mcp_tool"] = json!({
                    "plugin_id": b.plugin_id,
                    "tool": b.tool,
                    "args": b.args,
                    "side_effects": b.side_effects,
                    // Required props not resolvable at Review: the dispatch fills
                    // them from UPSTREAM step outputs by key (e.g. list_comments'
                    // file_id from the upload step's result) — still zero-LLM.
                    "pending": b.pending,
                    "bound": true,
                });
                if let Some(m) = metadata.as_object_mut() {
                    m.remove("mcp_tool_miss");
                }
            }
            crate::workflow_bind::BindOutcome::Miss { mention, reason } => {
                metadata["mcp_tool_miss"] = json!({ "mention": mention, "reason": reason });
            }
            crate::workflow_bind::BindOutcome::None => {}
        }

        let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
            id: cell.cell_id.clone(),
            board_id: board_id.to_string(),
            cell_type: "markdown".to_string(),
            cell_order: cell.cell_order,
            content: Some(cell.content.clone()),
            output: None,
            collapsed: false,
            height: None,
            metadata_json: Some(metadata.to_string()),
        });

        configs.push(serde_json::to_value(&config)?);
        prev_step_id = Some(step_id);
        applied += 1;
    }

    // NOTE: the FFI (cyan_pipeline_compile) reads data["applied"] for its status line —
    // emit BOTH keys so the "N steps configured" message is correct (was always 0).
    Ok(json!({
        "success": true,
        "applied": applied,
        "steps_compiled": applied,
        "configs": configs
    }))
}

/// Execute a step via vLLM (for AI analysis tasks)
async fn execute_lens_step(_config: &PipelineStepConfig, cell_content: &str) -> Result<String> {
    let prompt = format!(
        "Execute this pipeline step and provide the result. Be specific and structured.\n\n\
         Step: {}\n\n\
         Provide a detailed result with findings, any issues detected, and recommendations.",
        cell_content
    );

    call_vllm(&prompt, 500, 0.3).await
}

/// Execute a step on cloud (placeholder — would trigger Airflow/ECS)
async fn execute_cloud_step(config: &PipelineStepConfig) -> Result<String> {
    let command = config.command.as_ref()
        .ok_or_else(|| anyhow!("No command specified for cloud step"))?;

    // For now, execute locally as a fallback
    // TODO: Deploy to Airflow on EC2 or trigger ECS task
    tracing::warn!("Cloud executor not yet implemented, running locally: {}", command);
    execute_local_step(config).await
}

// ============================================================================
// Status: Query Pipeline State
// ============================================================================

pub fn pipeline_status(board_id: &str) -> Result<serde_json::Value> {
    let cells = load_pipeline_cells(board_id)?;

    let mut steps = Vec::new();
    let mut total = 0;
    let mut ai_complete = 0;
    let mut human_approved = 0;
    let mut running = 0;
    let mut failed = 0;
    let mut pending = 0;
    // Single-run identity + monotonic cost reconstructed from the persisted cells.
    let mut run_id: Option<String> = None;
    let mut total_cost_usd = 0.0_f64;
    let mut awaiting_step: Option<String> = None;

    for cell in &cells {
        if let Some(ref config) = cell.pipeline_config {
            total += 1;
            match config.state.status.as_str() {
                "ai_complete" => ai_complete += 1,
                "human_approved" => { human_approved += 1; ai_complete += 1; }
                "running" | "scheduled" => running += 1,
                "failed" => failed += 1,
                _ => pending += 1,
            }
            // The run-id stamped on any step that has run (the active run).
            if run_id.is_none()
                && let Some(rid) = config.state.run_id.as_ref().filter(|s| !s.is_empty()) {
                    run_id = Some(rid.clone());
                }
            // The first step awaiting approval (the current gate) — drives the UI.
            if awaiting_step.is_none() && config.state.status == "ai_complete" {
                awaiting_step = Some(config.step_id.clone());
            }
            // Monotonic cost: ONE source of truth = Σ(step wall-seconds × GPU rate),
            // identical to the live dashboard rail (gpu_ms = duration×1000).
            let step_cost = config.state.duration.unwrap_or(0.0) * 1000.0
                * crate::dashboard::USD_PER_GPU_MS;
            total_cost_usd += step_cost;

            steps.push(json!({
                "step_id": config.step_id,
                "title": first_line(&cell.content),
                "status": config.state.status,
                "stage": config.stage.clone().unwrap_or_else(|| config.step_id.clone()),
                "executor": config.executor,
                "depends_on": config.depends_on,
                "ai_result": config.state.ai_result,
                "error": config.state.error,
                "duration": config.state.duration,
                "cost_usd": step_cost,
                // D/P-4 — the review-hold gate identity, so the app renders
                // "In review — waiting on <user>" instead of a generic gate.
                // The LIVE assignee wins when the compiled snapshot is unset.
                "review_hold": config.review_hold,
                "waiting_on": config.waiting_on.clone().filter(|w| !w.is_empty())
                    .or_else(|| if config.review_hold { review_assignee(board_id) } else { None }),
            }));
        }
    }

    // Derived run-level status (the keystone): failed > awaiting > running > done > idle.
    let status = if failed > 0 { "failed" }
        else if awaiting_step.is_some() { "awaiting_approval" }
        else if running > 0 { "running" }
        else if total > 0 && human_approved == total { "done" }
        else if human_approved > 0 || ai_complete > 0 { "in_progress" }
        else { "idle" };

    Ok(json!({
        "board_id": board_id,
        "run_id": run_id,
        "status": status,
        "total_steps": total,
        "ai_complete": ai_complete,
        "human_approved": human_approved,
        "running": running,
        "failed": failed,
        "pending": pending,
        "progress_pct": if total > 0 { (human_approved * 100) / total } else { 0 },
        "total_cost_usd": total_cost_usd,
        "awaiting_step": awaiting_step,
        "steps": steps
    }))
}

// ============================================================================
// Approve: Human Approves a Step
// ============================================================================

// ============================================================================
// D/P-4 — the board's REVIEW ASSIGNEE (the real user review gates wait on)
// ============================================================================

/// Lazily-created k/v: board → the sso_user its review gates wait on. Lives
/// here (not storage.rs) as a self-contained review-gate concern; CREATE TABLE
/// IF NOT EXISTS makes every call safe on any DB.
fn ensure_review_assignee_table(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS review_assignees (
            board_id   TEXT PRIMARY KEY,
            user       TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )",
        [],
    )?;
    Ok(())
}

/// Assign the REAL user this board's review gates wait on (e.g. "producer").
pub fn set_review_assignee(board_id: &str, user: &str) -> Result<()> {
    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
    ensure_review_assignee_table(&conn)?;
    conn.execute(
        "INSERT INTO review_assignees (board_id, user, updated_at) VALUES (?1, ?2, ?3) \
         ON CONFLICT(board_id) DO UPDATE SET user=excluded.user, updated_at=excluded.updated_at",
        rusqlite::params![board_id, user, chrono::Utc::now().timestamp()],
    )?;
    tracing::info!("review assignee for board {} → {}", board_id, user);
    Ok(())
}

/// The board's assigned reviewer, if one is set.
pub fn review_assignee(board_id: &str) -> Option<String> {
    let conn = storage::db().lock().ok()?;
    ensure_review_assignee_table(&conn).ok()?;
    conn.query_row(
        "SELECT user FROM review_assignees WHERE board_id = ?1",
        rusqlite::params![board_id],
        |row| row.get(0),
    )
    .ok()
}

/// D/P-4 — is this step the producer-review upload (the review WINDOW)?
/// Pure: an external Frame.io upload whose authored text says "review" —
/// and not the round-N "publish revised cut" delivery leg.
pub(crate) fn is_review_upload(bound_command: Option<&str>, content: &str) -> bool {
    let content_lower = content.to_lowercase();
    bound_command
        .map(|c| c.starts_with("@frameio.upload"))
        .unwrap_or(false)
        && content_lower.contains("review")
        && !content_lower.contains("publish")
}

/// D/P-4 — the LIVE-assignee wrapper: the effective reviewer is the compiled
/// `waiting_on` OR the board's current `review_assignees` row, so assigning a
/// reviewer takes effect immediately — even on an already-parked hold, no
/// recompile needed.
fn enforce_review_gate_for(
    board_id: &str,
    config: &PipelineStepConfig,
    reviewer: Option<&str>,
) -> Result<()> {
    if !config.review_hold {
        return Ok(());
    }
    let mut cfg = config.clone();
    if cfg.waiting_on.as_deref().unwrap_or("").is_empty() {
        cfg.waiting_on = review_assignee(board_id);
    }
    enforce_review_gate(&cfg, reviewer)
}

/// D/P-4 — the review-gate guard shared by approve/reject: a `review_hold`
/// step clears ONLY for the assigned reviewer. `Ok(())` for non-hold steps,
/// for holds with no assignee configured (nothing to enforce), and for the
/// assignee themself; a typed error naming the waited-on user otherwise.
fn enforce_review_gate(config: &PipelineStepConfig, reviewer: Option<&str>) -> Result<()> {
    if !config.review_hold {
        return Ok(());
    }
    let Some(want) = config.waiting_on.as_deref().filter(|w| !w.is_empty()) else {
        return Ok(());
    };
    match reviewer {
        Some(who) if who == want => Ok(()),
        Some(who) => Err(anyhow!(
            "review gate is waiting on '{}' — '{}' cannot clear it",
            want, who
        )),
        None => Err(anyhow!(
            "review gate is waiting on '{}' — approve as that user (no reviewer identity supplied)",
            want
        )),
    }
}

pub fn approve_step(
    board_id: &str,
    step_id: &str,
    reviewer: Option<&str>,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: Option<&UnboundedSender<SwiftEvent>>,
) -> Result<()> {
    let cells = load_pipeline_cells(board_id)?;

    let cell = cells.iter()
        .find(|c| c.pipeline_config.as_ref().map(|p| p.step_id.as_str()) == Some(step_id))
        .ok_or_else(|| anyhow!("Step {} not found", step_id))?;

    // D/P-4 — a review hold clears only for its assigned reviewer (live lookup).
    if let Some(config) = cell.pipeline_config.as_ref() {
        enforce_review_gate_for(board_id, config, reviewer)?;
    }
    let stage = cell.pipeline_config.as_ref().map(step_stage).unwrap_or_else(|| step_id.to_string());
    let name = first_line(&cell.content);

    // CRITICAL: Re-read cell from DB to get latest metadata
    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
    let (content, cell_order, current_metadata_json): (String, i32, Option<String>) = conn.query_row(
        "SELECT content, cell_order, metadata_json FROM notebook_cells WHERE id = ?1",
        rusqlite::params![cell.cell_id],
        |row| Ok((
            row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            row.get(1)?,
            row.get(2)?,
        )),
    ).map_err(|e| anyhow!("Cell not found: {}", e))?;
    drop(conn);

    let mut metadata: serde_json::Value = current_metadata_json.as_ref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!({}));

    metadata["pipeline"]["state"]["status"] = json!("human_approved");
    metadata["pipeline"]["state"]["human_reviewer"] = json!(reviewer.unwrap_or("anonymous"));
    metadata["pipeline"]["state"]["human_approved_at"] = json!(chrono::Utc::now().timestamp());

    // SYNCHRONOUS persist (N): the immediately-spawned resume-run must read
    // `human_approved` NOW (not the stale `ai_complete`) so it advances to the next
    // step instead of re-pausing here. The command_tx below still gossips to peers.
    let _ = storage::cell_update(&crate::models::dto::NotebookCellDTO {
        id: cell.cell_id.clone(),
        board_id: board_id.to_string(),
        cell_type: "markdown".to_string(),
        cell_order,
        content: Some(content.clone()),
        output: None,
        collapsed: false,
        height: None,
        metadata_json: Some(metadata.to_string()),
        created_at: 0,
        updated_at: 0,
    });
    let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
        id: cell.cell_id.clone(),
        board_id: board_id.to_string(),
        cell_type: "markdown".to_string(),
        cell_order,
        content: Some(content),
        output: None,
        collapsed: false,
        height: None,
        metadata_json: Some(metadata.to_string()),
    });

    tracing::info!("Step {} approved by {}", step_id, reviewer.unwrap_or("anonymous"));

    // Dashboard: resolve the gate (approved) + flip the step to approved
    // (DASHBOARD_CONTRACT §A `ApprovalResolved`/`StepStateChanged`). Additive,
    // receive-only; tagged with tenant/run/board/workflow. `run_id` is unknown at
    // approval time (a separate FFI call), so it is empty — the snapshot recomputes
    // on the next run; the dashboard keys the gate by step_id.
    if let Some(tx) = event_tx {
        let by = reviewer.unwrap_or("anonymous").to_string();
        let now = chrono::Utc::now().timestamp();
        let tags = RunTags {
            tenant_id: workflow_tenant(board_id),
            run_id: String::new(),
            board_id: board_id.to_string(),
            workflow_id: board_id.to_string(),
        };
        let _ = tx.send(SwiftEvent::ApprovalResolved {
            tenant_id: tags.tenant_id.clone(),
            run_id: tags.run_id.clone(),
            board_id: tags.board_id.clone(),
            workflow_id: tags.workflow_id.clone(),
            step_id: step_id.to_string(),
            stage: stage.clone(),
            decision: "approved".to_string(),
            by,
            at: now,
        });
        let _ = tx.send(tags.step_state(step_id, &name, &stage, StepState::Approved, Actor::Human, None, now));
    }
    Ok(())
}

/// Reject a pipeline step (item C): mark it FAILED so the run stops at the gate. The
/// operator rejected the step's output. Surfaces via ApprovalResolved(rejected) +
/// StepStateChanged(Failed) + run_finished(failed). A later Retry resets it to pending.
pub fn reject_step(
    board_id: &str,
    step_id: &str,
    reviewer: Option<&str>,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: Option<&UnboundedSender<SwiftEvent>>,
) -> Result<()> {
    let cells = load_pipeline_cells(board_id)?;
    let cell = cells.iter()
        .find(|c| c.pipeline_config.as_ref().map(|p| p.step_id.as_str()) == Some(step_id))
        .ok_or_else(|| anyhow!("Step {} not found", step_id))?;
    // D/P-4 — a review hold is the assigned reviewer's decision, reject included.
    if let Some(config) = cell.pipeline_config.as_ref() {
        enforce_review_gate_for(board_id, config, reviewer)?;
    }
    let stage = cell.pipeline_config.as_ref().map(step_stage).unwrap_or_else(|| step_id.to_string());
    let name = first_line(&cell.content);

    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
    let (content, cell_order, current_metadata_json): (String, i32, Option<String>) = conn.query_row(
        "SELECT content, cell_order, metadata_json FROM notebook_cells WHERE id = ?1",
        rusqlite::params![cell.cell_id],
        |row| Ok((
            row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            row.get(1)?,
            row.get(2)?,
        )),
    ).map_err(|e| anyhow!("Cell not found: {}", e))?;
    drop(conn);

    let mut metadata: serde_json::Value = current_metadata_json.as_ref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!({}));
    metadata["pipeline"]["state"]["status"] = json!("failed");
    metadata["pipeline"]["state"]["error"] = json!("Rejected by reviewer");
    metadata["pipeline"]["state"]["human_reviewer"] = json!(reviewer.unwrap_or("anonymous"));

    let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
        id: cell.cell_id.clone(),
        board_id: board_id.to_string(),
        cell_type: "markdown".to_string(),
        cell_order,
        content: Some(content),
        output: None,
        collapsed: false,
        height: None,
        metadata_json: Some(metadata.to_string()),
    });
    tracing::info!("Step {} rejected by {}", step_id, reviewer.unwrap_or("anonymous"));

    if let Some(tx) = event_tx {
        let by = reviewer.unwrap_or("anonymous").to_string();
        let now = chrono::Utc::now().timestamp();
        let tags = RunTags {
            tenant_id: workflow_tenant(board_id),
            run_id: String::new(),
            board_id: board_id.to_string(),
            workflow_id: board_id.to_string(),
        };
        let _ = tx.send(SwiftEvent::ApprovalResolved {
            tenant_id: tags.tenant_id.clone(),
            run_id: tags.run_id.clone(),
            board_id: tags.board_id.clone(),
            workflow_id: tags.workflow_id.clone(),
            step_id: step_id.to_string(),
            stage: stage.clone(),
            decision: "rejected".to_string(),
            by,
            at: now,
        });
        let _ = tx.send(tags.step_state(step_id, &name, &stage, StepState::Failed, Actor::Human, None, now));
        let _ = tx.send(tags.run_finished("failed", now));
    }
    Ok(())
}

/// Retry a pipeline step — reset to pending while preserving all metadata
pub fn retry_step(
    board_id: &str,
    step_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<()> {
    step_state_surgery(board_id, step_id, command_tx, StepSurgery::Retry)
}

/// B4 — per-step RESET: back to `pending`, result cleared, attempt counter
/// ZEROED and any human decision cleared (a reset is a clean slate, not a
/// retry — it never inflates the attempt/metering trail). The app decides
/// whether to run afterwards; this is state surgery only.
pub fn reset_step(
    board_id: &str,
    step_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<()> {
    step_state_surgery(board_id, step_id, command_tx, StepSurgery::Reset)
}

#[derive(Clone, Copy, PartialEq)]
enum StepSurgery {
    /// → pending, result cleared, attempt += 1 (an attempt is being spent).
    Retry,
    /// → pending, result cleared, attempt = 0, human decision cleared.
    Reset,
}

fn step_state_surgery(
    board_id: &str,
    step_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
    surgery: StepSurgery,
) -> Result<()> {
    let cells = load_pipeline_cells(board_id)?;

    let cell = cells.iter()
        .find(|c| c.pipeline_config.as_ref().map(|p| p.step_id.as_str()) == Some(step_id))
        .ok_or_else(|| anyhow!("Step {} not found", step_id))?;

    // CRITICAL: Re-read cell from DB to get latest metadata
    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
    let (content, cell_order, current_metadata_json): (String, i32, Option<String>) = conn.query_row(
        "SELECT content, cell_order, metadata_json FROM notebook_cells WHERE id = ?1",
        rusqlite::params![cell.cell_id],
        |row| Ok((
            row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            row.get(1)?,
            row.get(2)?,
        )),
    ).map_err(|e| anyhow!("Cell not found: {}", e))?;
    drop(conn);

    let mut metadata: serde_json::Value = current_metadata_json.as_ref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!({}));

    metadata["pipeline"]["state"]["status"] = json!("pending");
    metadata["pipeline"]["state"]["error"] = json!(null);
    metadata["pipeline"]["state"]["ai_result"] = json!(null);
    metadata["pipeline"]["state"]["duration"] = json!(null);
    metadata["pipeline"]["state"]["started_at"] = json!(null);
    metadata["pipeline"]["state"]["ai_completed_at"] = json!(null);

    match surgery {
        StepSurgery::Retry => {
            // An attempt is being spent — the counter tells the truth.
            let attempt = metadata["pipeline"]["state"]["attempt"].as_u64().unwrap_or(0);
            metadata["pipeline"]["state"]["attempt"] = json!(attempt + 1);
        }
        StepSurgery::Reset => {
            // Clean slate: no attempts spent, no lingering human decision.
            metadata["pipeline"]["state"]["attempt"] = json!(0);
            metadata["pipeline"]["state"]["approved_by"] = json!(null);
            metadata["pipeline"]["state"]["approved_at"] = json!(null);
        }
    }

    // SYNCHRONOUS persist (C/S): the resume-run spawned right after Retry must read the
    // step as `pending` NOW so it re-executes it (resume from the failed step).
    let _ = storage::cell_update(&crate::models::dto::NotebookCellDTO {
        id: cell.cell_id.clone(),
        board_id: board_id.to_string(),
        cell_type: "markdown".to_string(),
        cell_order,
        content: Some(content.clone()),
        output: None,
        collapsed: false,
        height: None,
        metadata_json: Some(metadata.to_string()),
        created_at: 0,
        updated_at: 0,
    });
    let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
        id: cell.cell_id.clone(),
        board_id: board_id.to_string(),
        cell_type: "markdown".to_string(),
        cell_order,
        content: Some(content),
        output: None,
        collapsed: false,
        height: None,
        metadata_json: Some(metadata.to_string()),
    });

    tracing::info!(
        "Step {} reset to pending ({})",
        step_id,
        if surgery == StepSurgery::Retry { "retry" } else { "reset" }
    );
    Ok(())
}

/// Reset all pipeline steps to pending, clear outputs and timecoded notes
pub fn reset_pipeline(
    board_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<()> {
    let cells = load_pipeline_cells(board_id)?;
    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;

    // Reset each step
    for cell in &cells {
        // RECOVERY runs on EVERY authored-kind cell (before the config guard — a
        // scrambled cell's metadata may not even parse as a config any more).
        if cell.pipeline_config.is_none() && !is_run_result_blob(&cell.content) { continue; }

        let (content, cell_order, current_metadata_json): (String, i32, Option<String>) = conn.query_row(
            "SELECT content, cell_order, metadata_json FROM notebook_cells WHERE id = ?1",
            rusqlite::params![cell.cell_id],
            |row| Ok((
                row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                row.get(1)?,
                row.get(2)?,
            )),
        ).map_err(|e| anyhow!("Cell not found: {}", e))?;

        let mut metadata: serde_json::Value = current_metadata_json.as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(json!({}));

        // RECOVERY (authored_ledger_test): a cell whose content is a raw result blob
        // is run output an OLD build wrote into the authored ledger — Reset restores
        // the clean authored set by ARCHIVING it (kept, reversible, no data loss),
        // exactly like the §W1 migration parks non-authorable kinds.
        if is_run_result_blob(&content) {
            metadata["original_cell_type"] = json!("step");
            metadata["archived_reason"] = json!("run_result_pollution");
            let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
                id: cell.cell_id.clone(),
                board_id: board_id.to_string(),
                cell_type: "archived".to_string(),
                cell_order,
                content: Some(content),
                output: None,
                collapsed: false,
                height: None,
                metadata_json: Some(metadata.to_string()),
            });
            continue;
        }

        metadata["pipeline"]["state"]["status"] = json!("pending");
        metadata["pipeline"]["state"]["error"] = json!(null);
        metadata["pipeline"]["state"]["ai_result"] = json!(null);
        metadata["pipeline"]["state"]["duration"] = json!(null);
        metadata["pipeline"]["state"]["started_at"] = json!(null);
        metadata["pipeline"]["state"]["ai_completed_at"] = json!(null);
        metadata["pipeline"]["state"]["human_approved_at"] = json!(null);
        metadata["pipeline"]["state"]["run_id"] = json!(null);
        metadata["pipeline"]["state"]["attempt"] = json!(0);

        let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
            id: cell.cell_id.clone(),
            board_id: board_id.to_string(),
            cell_type: "markdown".to_string(),
            cell_order,
            content: Some(content),
            output: None,
            collapsed: false,
            height: None,
            metadata_json: Some(metadata.to_string()),
        });
    }

    // Delete timecoded notes for this board
    conn.execute(
        "DELETE FROM notebook_cells WHERE board_id = ?1 AND cell_type = 'timecode_note'",
        rusqlite::params![board_id],
    )?;

    drop(conn);
    tracing::info!("Pipeline reset: all steps pending, notes cleared for board {}", &board_id[..8]);
    Ok(())
}

// ============================================================================
// Export: Generate Airflow DAG
// ============================================================================

pub fn export_airflow_dag(board_id: &str, dag_name: Option<&str>) -> Result<String> {
    let cells = load_pipeline_cells(board_id)?;
    let steps: Vec<_> = cells.iter()
        .filter_map(|c| c.pipeline_config.as_ref().map(|p| (c, p)))
        .collect();

    if steps.is_empty() {
        return Err(anyhow!("No pipeline steps configured"));
    }

    let name = dag_name.unwrap_or("cyan_pipeline");

    let mut py = format!(
        "# Generated by Cyan Pipeline\n\
         # Board: {}\n\
         from airflow import DAG\n\
         from airflow.operators.bash import BashOperator\n\
         from airflow.operators.python import PythonOperator\n\
         from datetime import datetime, timedelta\n\n\
         default_args = {{\n\
         \t'owner': 'cyan',\n\
         \t'retries': 1,\n\
         \t'retry_delay': timedelta(minutes=5),\n\
         }}\n\n\
         with DAG(\n\
         \t'{}',\n\
         \tdefault_args=default_args,\n\
         \tstart_date=datetime(2026, 1, 1),\n\
         \tschedule_interval=None,\n\
         \tcatchup=False,\n\
         ) as dag:\n\n",
        board_id, name
    );

    // Generate task for each step
    for (cell, config) in &steps {
        let title = first_line(&cell.content).replace("'", "\\'");
        let command = config.command.as_deref().unwrap_or("echo 'No command configured'").replace("'", "\\'");

        match config.executor.as_str() {
            "local" | "cloud" => {
                py.push_str(&format!(
                    "\t{} = BashOperator(\n\
                     \t\ttask_id='{}',\n\
                     \t\tbash_command='{}',\n\
                     \t\texecution_timeout=timedelta(seconds={}),\n\
                     \t)\n\n",
                    config.step_id, config.step_id, command,
                    config.timeout_seconds.unwrap_or(300)
                ));
            }
            "lens" => {
                py.push_str(&format!(
                    "\t{} = PythonOperator(\n\
                     \t\ttask_id='{}',\n\
                     \t\tpython_callable=lambda: print('Lens step: {}'),\n\
                     \t\texecution_timeout=timedelta(seconds={}),\n\
                     \t)\n\n",
                    config.step_id, config.step_id, title,
                    config.timeout_seconds.unwrap_or(300)
                ));
            }
            "manual" => {
                py.push_str(&format!(
                    "\t# {} - manual step (requires human approval)\n\
                     \t{} = PythonOperator(\n\
                     \t\ttask_id='{}',\n\
                     \t\tpython_callable=lambda: print('Awaiting human approval: {}'),\n\
                     \t)\n\n",
                    title, config.step_id, config.step_id, title
                ));
            }
            _ => {}
        }
    }

    // Generate dependencies
    py.push_str("\t# Dependencies\n");
    for (_cell, config) in &steps {
        for dep in &config.depends_on {
            py.push_str(&format!("\t{} >> {}\n", dep, config.step_id));
        }
    }

    Ok(py)
}

// ============================================================================
// Helpers
// ============================================================================

/// Read outputs from dependency cells in the DB.
/// This is the key to proper output chaining — each step reads its inputs
/// from the persisted output column of its dependency cells.
fn gather_dependency_outputs(board_id: &str, depends_on: &[String]) -> Vec<serde_json::Value> {
    if depends_on.is_empty() {
        return vec![];
    }

    let conn = match storage::db().lock() {
        Ok(c) => c,
        Err(_) => return vec![],
    };

    depends_on.iter().filter_map(|dep_step_id| {
        let result: Option<(String, Option<String>, Option<String>)> = conn.query_row(
            "SELECT json_extract(metadata_json, '$.pipeline.step_id'), \
                    output, \
                    json_extract(metadata_json, '$.pipeline.output_format') \
             FROM notebook_cells \
             WHERE board_id = ?1 \
             AND json_extract(metadata_json, '$.pipeline.step_id') = ?2",
            rusqlite::params![board_id, dep_step_id],
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        ).ok();

        result.map(|(step_id, output, format)| {
            json!({
                "step_id": step_id,
                "output": output.unwrap_or_default(),
                "format": format.unwrap_or_else(|| "markdown".to_string()),
            })
        })
    }).collect()
}

/// Load cells with pipeline metadata from a board
fn load_pipeline_cells(board_id: &str) -> Result<Vec<PipelineCell>> {
    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;

    // THE AUTHORED-LEDGER READ (authored_ledger_test.rs): every pipeline verb —
    // compile, run, approve, reset, export — operates on the AUTHORED cells only,
    // selected by an explicit kind WHITELIST. Run outputs live in system-kind cells
    // (`timecode_note`) on the same board; a blacklist here is how a run's result
    // JSON got swept into the plan, rewritten as a step, and destroyed the authored
    // workflow (SEV-HIGH, 2026-07-09). `step` is the one authorable kind (§W1);
    // `markdown` is grandfathered for pre-migration rows.
    let mut stmt = conn.prepare(
        "SELECT id, board_id, cell_order, content, metadata_json \
         FROM notebook_cells WHERE board_id = ?1 AND cell_type IN ('step','markdown') \
         ORDER BY cell_order"
    )?;

    let cells = stmt.query_map(rusqlite::params![board_id], |row| {
        let cell_id: String = row.get(0)?;
        let board_id: String = row.get(1)?;
        let cell_order: i32 = row.get(2)?;
        let content: String = row.get::<_, Option<String>>(3)?.unwrap_or_default();
        let metadata_json: Option<String> = row.get(4)?;

        // Parse pipeline config from metadata
        let pipeline_config = metadata_json.as_ref().and_then(|json_str| {
            let val: serde_json::Value = serde_json::from_str(json_str).ok()?;
            let pipeline = val.get("pipeline")?;
            serde_json::from_value(pipeline.clone()).ok()
        });

        Ok(PipelineCell {
            cell_id,
            board_id,
            cell_order,
            content,
            pipeline_config,
            metadata_json,
        })
    })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(cells)
}

/// Update step state in cell metadata
fn update_step_state(
    board_id: &str,
    cell_id: &str,
    status: &str,
    ai_result: Option<&str>,
    error: Option<&str>,
    run_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<()> {
    update_step_state_full(board_id, cell_id, status, ai_result, error, run_id, None, command_tx)
}

fn update_step_state_full(
    board_id: &str,
    cell_id: &str,
    status: &str,
    ai_result: Option<&str>,
    error: Option<&str>,
    run_id: &str,
    duration: Option<f64>,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<()> {
    let cmd = {
        // CRITICAL: Re-read cell from DB to get latest metadata (avoids race condition)
        let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
        update_step_state_full_on(&conn, board_id, cell_id, status, ai_result, error, run_id, duration)?
    }; // Release lock before sending command
    let _ = command_tx.send(cmd);
    Ok(())
}

/// Conn-explicit core of `update_step_state_full` — the review-loop CONFIRM
/// interlock calls this on a connection it already holds (`approve_review_gate_steps`).
/// Reads the cell's latest metadata, folds the new step state in, persists it
/// SYNCHRONOUSLY on `conn`, and returns the `UpdateNotebookCell` command for the
/// caller to gossip AFTER its lock is released (sync-before-gossip).
#[allow(clippy::too_many_arguments)]
fn update_step_state_full_on(
    conn: &rusqlite::Connection,
    board_id: &str,
    cell_id: &str,
    status: &str,
    ai_result: Option<&str>,
    error: Option<&str>,
    run_id: &str,
    duration: Option<f64>,
) -> Result<CommandMsg> {
    let (content, cell_order, current_metadata_json): (String, i32, Option<String>) = conn.query_row(
        "SELECT content, cell_order, metadata_json FROM notebook_cells WHERE id = ?1",
        rusqlite::params![cell_id],
        |row| Ok((
            row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            row.get(1)?,
            row.get(2)?,
        )),
    ).map_err(|e| anyhow!("Cell not found: {}", e))?;

    let mut metadata: serde_json::Value = current_metadata_json.as_ref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!({}));

    let now = chrono::Utc::now().timestamp();

    metadata["pipeline"]["state"]["status"] = json!(status);
    metadata["pipeline"]["state"]["run_id"] = json!(run_id);

    if status == "running" {
        metadata["pipeline"]["state"]["started_at"] = json!(now);
    }
    if let Some(result) = ai_result {
        metadata["pipeline"]["state"]["ai_result"] = json!(result);
        metadata["pipeline"]["state"]["ai_completed_at"] = json!(now);
    }
    if let Some(err) = error {
        metadata["pipeline"]["state"]["error"] = json!(err);
    }
    if let Some(dur) = duration {
        metadata["pipeline"]["state"]["duration"] = json!(dur);
    }

    // SYNCHRONOUS persist (N/L/M): write the new state to storage NOW so the resume-run
    // and the Dashboard reconstruct read the latest status immediately (the returned
    // command still gossips it to peers). Without this, a resume raced the async
    // command and re-paused at the just-finished step, and reconstruct showed stale cost.
    let _ = conn.execute(
        "UPDATE notebook_cells SET cell_type=?2, cell_order=?3, content=?4, output=?5, \
            collapsed=?6, height=?7, metadata_json=?8, updated_at=?9 WHERE id=?1",
        rusqlite::params![
            cell_id,
            "markdown",
            cell_order,
            Some(content.clone()),
            ai_result,
            0i32,
            Option::<f64>::None,
            metadata.to_string(),
            now,
        ],
    );

    tracing::info!("📝 Pipeline step {} → {} (metadata: {}B)", cell_id.get(..8).unwrap_or(cell_id), status, metadata.to_string().len());

    Ok(CommandMsg::UpdateNotebookCell {
        id: cell_id.to_string(),
        board_id: board_id.to_string(),
        cell_type: "markdown".to_string(),
        cell_order,
        content: Some(content),
        output: ai_result.map(|s| s.to_string()),
        collapsed: false,
        height: None,
        metadata_json: Some(metadata.to_string()),
    })
}

/// CONFIRM-step ↔ review-loop interlock (CYAN_FORMAT_QA gap 6). When the review
/// machine advances NOTES_IN → CONFORMING (every proposal resolved through the
/// human confirm gate), the tenant's parked manual CONFIRM steps — cells marked
/// with a top-level `"review_gate": true` in `metadata_json` — flip to
/// `human_approved`: the workflow was parked on the SAME human decision the review
/// gate just recorded, so it un-parks without a second approval tap.
///
/// Writes through the `update_step_state_full` core on the CALLER's connection
/// (synchronous persist first), then gossips via `queue_command`
/// (sync-before-gossip; a no-op without a running system). Only parked steps move:
/// already-approved, skipped, or failed (explicitly rejected) gates are left
/// alone. A DB without the workflow tables (bare ledger-only stores) has zero
/// boards — never an error.
pub fn approve_review_gate_steps(conn: &rusqlite::Connection, tenant_id: &str) -> Result<usize> {
    let Ok(mut board_stmt) = conn.prepare(
        "SELECT id FROM objects WHERE type='whiteboard' AND (group_id=?1 \
            OR workspace_id IN (SELECT id FROM workspaces WHERE group_id=?1))",
    ) else {
        return Ok(0); // no workflow tables on this DB
    };
    let board_ids: Vec<String> = board_stmt
        .query_map(rusqlite::params![tenant_id], |r| r.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(board_stmt);

    let mut approved = 0usize;
    for board_id in &board_ids {
        let mut cell_stmt = conn.prepare(
            "SELECT id, metadata_json FROM notebook_cells \
             WHERE board_id=?1 AND cell_type != 'archived'",
        )?;
        let cells: Vec<(String, Option<String>)> = cell_stmt
            .query_map(rusqlite::params![board_id], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(cell_stmt);

        for (cell_id, meta_json) in cells {
            let Some(meta) = meta_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            else {
                continue;
            };
            if meta.get("review_gate").and_then(|v| v.as_bool()) != Some(true) {
                continue;
            }
            // Only a manual (human-approval) pipeline step can be a review gate.
            let executor = meta["pipeline"]["executor"].as_str().unwrap_or("");
            if executor != "manual" {
                continue;
            }
            let status = meta["pipeline"]["state"]["status"].as_str().unwrap_or("pending");
            if matches!(status, "human_approved" | "skipped" | "failed") {
                continue; // resolved gates never move (idempotent; a rejection stands)
            }
            let run_id = meta["pipeline"]["state"]["run_id"].as_str().unwrap_or("").to_string();
            let cmd = update_step_state_full_on(
                conn, board_id, &cell_id, "human_approved", None, None, &run_id, None,
            )?;
            crate::queue_command(cmd);
            approved += 1;
        }
    }
    Ok(approved)
}

/// Fire notifications for a step event
fn fire_notifications(
    notifications: &[StepNotification],
    trigger: &str,
    board_id: &str,
    step_id: &str,
    event_tx: &UnboundedSender<SwiftEvent>,
) {
    for notif in notifications {
        if notif.trigger == trigger {
            let message = notif.message.as_deref().unwrap_or("Pipeline step update");

            match notif.action.as_str() {
                "dm" => {
                    let _ = event_tx.send(SwiftEvent::StatusUpdate {
                        message: format!("[Pipeline DM -> {}] Step '{}': {}", notif.target, step_id, trigger),
                    });
                }
                "email" => {
                    // Use local sendmail or SMTP
                    tracing::info!("Email notification to {}: step {} {}", notif.target, step_id, trigger);
                    // TODO: actual email sending
                }
                "activity" => {
                    let _ = event_tx.send(SwiftEvent::StatusUpdate {
                        message: format!("Pipeline: step '{}' is now {}", step_id, trigger),
                    });
                }
                "webhook" => {
                    // Fire webhook async
                    let url = notif.target.clone();
                    let payload = json!({
                        "board_id": board_id,
                        "step_id": step_id,
                        "trigger": trigger,
                        "message": message
                    });
                    tokio::spawn(async move {
                        let _ = reqwest::Client::new()
                            .post(&url)
                            .json(&payload)
                            .send()
                            .await;
                    });
                }
                _ => {}
            }
        }
    }
}

/// True iff `content` is a serialized run-result blob (a JSON object/array), never
/// authored English. An authored step can not parse as a JSON container, so this is
/// the discriminator the compile filter and Reset recovery share: run output must
/// never be materialized as a step nor mint a step id (`rawcontent…` was the tell).
fn is_run_result_blob(content: &str) -> bool {
    let trimmed = content.trim_start();
    if !trimmed.starts_with(['{', '[']) {
        return false;
    }
    matches!(
        serde_json::from_str::<serde_json::Value>(content),
        Ok(serde_json::Value::Object(_)) | Ok(serde_json::Value::Array(_))
    )
}

/// Generate a step_id from cell content
fn generate_step_id(content: &str, index: usize) -> String {
    let first = first_line(content);
    let words: Vec<&str> = first.split_whitespace()
        .take(3)
        .collect();

    if words.is_empty() {
        return format!("step_{}", index);
    }

    words.join("_")
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>()
}

/// Get first non-empty line from content, strip markdown headers
fn first_line(content: &str) -> String {
    content.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("Untitled")
        .trim()
        .trim_start_matches('#')
        .trim()
        .to_string()
}

// ============================================================================
// FFI-friendly compile (called from cyan_pipeline_compile)
// ============================================================================

/// Compile pipeline via vLLM — blocking FFI wrapper
pub fn compile_pipeline_sync(
    board_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<serde_json::Value> {
    // Use the existing RUNTIME to run the async function
    let rt = crate::RUNTIME.get().ok_or_else(|| anyhow!("Runtime not available"))?;
    rt.block_on(compile_via_llm(board_id, command_tx))
}

/// Run pipeline — blocking FFI wrapper
pub fn run_pipeline_sync(
    board_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<serde_json::Value> {
    let rt = crate::RUNTIME.get().ok_or_else(|| anyhow!("Runtime not available"))?;
    rt.block_on(run_pipeline(board_id, command_tx, event_tx))
}

/// Find video URI from the board's cells (first cell with a video URL)
fn find_video_uri(board_id: &str) -> Option<String> {
    let conn = storage::db().lock().ok()?;
    let mut stmt = conn.prepare(
        "SELECT content FROM notebook_cells WHERE board_id = ?1 ORDER BY cell_order LIMIT 20"
    ).ok()?;

    let rows: Vec<String> = stmt.query_map(rusqlite::params![board_id], |row| {
        row.get::<_, Option<String>>(0)
    }).ok()?
        .filter_map(|r| r.ok().flatten())
        .collect();

    for content in &rows {
        for word in content.split_whitespace() {
            if (word.starts_with("http://") || word.starts_with("https://") || word.starts_with("s3://"))
                && (word.contains(".mp4") || word.contains(".mov") || word.contains(".mxf") || word.contains(".mkv"))
            {
                return Some(word.to_string());
            }
        }
    }
    None
}

/// Find scope_id for the board's workspace
fn find_scope_id(board_id: &str) -> Option<String> {
    let conn = storage::db().lock().ok()?;

    // The board's workspace_id IS the scope_id
    conn.query_row(
        "SELECT workspace_id FROM objects WHERE id = ?1",
        rusqlite::params![board_id],
        |row| row.get::<_, Option<String>>(0),
    ).ok()?
        .or_else(|| {
            // Fallback: check integration_bindings for any scope
            conn.query_row(
                "SELECT scope_id FROM integration_bindings LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            ).ok()
        })
}
// ============================================================================
// D/P-4 — review-gate unit tests (pure fns; no engine/system required)
// ============================================================================

#[cfg(test)]
mod review_gate_tests {
    use super::*;

    fn hold_config(waiting_on: Option<&str>) -> PipelineStepConfig {
        PipelineStepConfig {
            step_id: "upload_review".into(),
            depends_on: vec![],
            stage: None,
            executor: "local".into(),
            model: None,
            model_config: None,
            tools: vec![],
            output_format: "markdown".into(),
            command: Some("@frameio.upload_file".into()),
            timeout_seconds: None,
            retry_count: None,
            auto_advance: false,
            review_hold: true,
            waiting_on: waiting_on.map(String::from),
            notifications: vec![],
            state: PipelineStepState::default(),
        }
    }

    #[test]
    fn review_upload_detection_is_exact() {
        // The producer-review upload IS the window…
        assert!(is_review_upload(
            Some("@frameio.upload_file"),
            "upload to @frameio.upload for producer review /needs-approval"
        ));
        // …the round-N delivery leg is NOT (publish)…
        assert!(!is_review_upload(
            Some("@frameio.upload_file"),
            "publish revised cut to @frameio.upload /needs-approval"
        ));
        // …nor a non-upload bind, nor an unbound step, nor a non-review upload.
        assert!(!is_review_upload(Some("@frameio.list_comments"), "get review comments"));
        assert!(!is_review_upload(None, "upload for producer review"));
        assert!(!is_review_upload(Some("@frameio.upload_file"), "upload the master archive"));
    }

    #[test]
    fn review_hold_clears_only_for_the_assigned_reviewer() {
        let cfg = hold_config(Some("producer"));
        // The assignee clears it.
        assert!(enforce_review_gate(&cfg, Some("producer")).is_ok());
        // Anyone else is refused — and the error NAMES who is being waited on.
        let err = enforce_review_gate(&cfg, Some("alice")).unwrap_err().to_string();
        assert!(err.contains("waiting on 'producer'") && err.contains("'alice'"), "{err}");
        // No identity at all is refused too (the legacy anonymous verb can't bypass).
        let err = enforce_review_gate(&cfg, None).unwrap_err().to_string();
        assert!(err.contains("waiting on 'producer'"), "{err}");
    }

    #[test]
    fn non_hold_and_unassigned_holds_stay_open() {
        // A plain gate: anyone (or no one) approves — unchanged legacy behavior.
        let mut plain = hold_config(None);
        plain.review_hold = false;
        assert!(enforce_review_gate(&plain, None).is_ok());
        assert!(enforce_review_gate(&plain, Some("anyone")).is_ok());
        // A hold with NO assignee configured has nothing to enforce.
        assert!(enforce_review_gate(&hold_config(None), None).is_ok());
        // An empty-string assignee is "unset", not a lockout.
        assert!(enforce_review_gate(&hold_config(Some("")), None).is_ok());
    }
}
