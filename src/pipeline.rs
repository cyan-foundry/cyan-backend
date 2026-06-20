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

/// Execute a pipeline by reading cell configs, building DAG, and running steps
pub async fn run_pipeline(
    board_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<serde_json::Value> {
    let cells = load_pipeline_cells(board_id)?;

    // Collect steps with pipeline configs
    let steps: Vec<_> = cells.iter()
        .filter_map(|c| c.pipeline_config.as_ref().map(|p| (c, p)))
        .collect();

    if steps.is_empty() {
        return Err(anyhow!("No pipeline steps configured. Run /pipeline compile first."));
    }

    // Build DAG with petgraph
    let mut graph = DiGraph::<String, ()>::new();
    let mut node_map: HashMap<String, NodeIndex> = HashMap::new();
    let mut cell_map: HashMap<String, &PipelineCell> = HashMap::new();
    let mut config_map: HashMap<String, &PipelineStepConfig> = HashMap::new();

    // Add nodes
    for (cell, config) in &steps {
        let idx = graph.add_node(config.step_id.clone());
        node_map.insert(config.step_id.clone(), idx);
        cell_map.insert(config.step_id.clone(), cell);
        config_map.insert(config.step_id.clone(), config);
    }

    // Add edges (dependencies)
    for (_cell, config) in &steps {
        if let Some(&to_idx) = node_map.get(&config.step_id) {
            for dep in &config.depends_on {
                if let Some(&from_idx) = node_map.get(dep) {
                    graph.add_edge(from_idx, to_idx, ());
                }
            }
        }
    }

    // Topological sort
    let order = toposort(&graph, None)
        .map_err(|_| anyhow!("Pipeline has circular dependencies"))?;

    let run_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let mut results = Vec::new();

    tracing::info!("Pipeline run {} started: {} steps in DAG order", run_id, order.len());

    // ── Dashboard producer: tag this run and announce it (DASHBOARD_CONTRACT §A). ──
    let total_steps = order.len() as u32;
    let tags = RunTags {
        tenant_id: workflow_tenant(board_id),
        run_id: run_id.clone(),
        board_id: board_id.to_string(),
        workflow_id: board_id.to_string(), // the board IS the workflow surface
    };
    let label = workflow_label(board_id);
    let _ = event_tx.send(tags.run_started(label.clone(), total_steps, chrono::Utc::now().timestamp()));
    let mut obs = RunObs::new(
        tags.tenant_id.clone(),
        board_id,
        run_id.clone(),
        board_id,
        label,
        total_steps as u64,
    );
    let mut processed: u64 = 0;
    let mut current_stage: Option<String> = None;
    let mut current_item: Option<String> = None;
    let mut any_failed = false;

    // Execute in topological order
    for node_idx in order {
        let step_id = &graph[node_idx];
        let cell = cell_map[step_id];
        let config = config_map[step_id];

        // Skip already completed steps
        if config.state.status == "human_approved" || config.state.status == "skipped" {
            tracing::info!("Step {} already {}, skipping", step_id, config.state.status);
            continue;
        }

        let stage = step_stage(config);
        let name = first_line(&cell.content);
        let plugin = step_plugin(cell);

        // Skip manual steps (need human action) — surface them as a gate.
        if config.executor == "manual" {
            tracing::info!("Step {} is manual, skipping execution", step_id);
            update_step_state(board_id, &cell.cell_id, cell, "scheduled", None, None, &run_id, command_tx)?;
            let now = chrono::Utc::now().timestamp();
            let _ = event_tx.send(tags.step_state(
                step_id, &name, &stage, StepState::AwaitingApproval, Actor::Human, plugin.clone(), now,
            ));
            let _ = event_tx.send(tags.approval_requested(step_id, &name, &stage, now));
            obs.record(StepObs {
                step_id: step_id.clone(),
                name: name.clone(),
                stage: stage.clone(),
                actor: Actor::Human,
                plugin,
                state: StepState::AwaitingApproval,
                wall_ms: 0,
                gate_ms: 0,
                cost: StepCost::default(),
            });
            current_stage = Some(stage);
            current_item = Some(name);
            continue;
        }

        // Check if dependencies are met
        let deps_met = config.depends_on.iter().all(|dep| {
            config_map.get(dep)
                .map(|c| c.state.status == "human_approved" ||
                    (c.auto_advance && c.state.status == "ai_complete"))
                .unwrap_or(true) // unknown deps are considered met
        });

        if !deps_met {
            tracing::info!("Step {} dependencies not met, marking scheduled", step_id);
            update_step_state(board_id, &cell.cell_id, cell, "scheduled", None, None, &run_id, command_tx)?;
            let _ = event_tx.send(tags.step_state(
                step_id, &name, &stage, StepState::Pending, Actor::Ai, plugin.clone(), chrono::Utc::now().timestamp(),
            ));
            obs.record(StepObs {
                step_id: step_id.clone(),
                name: name.clone(),
                stage: stage.clone(),
                actor: Actor::Ai,
                plugin,
                state: StepState::Pending,
                wall_ms: 0,
                gate_ms: 0,
                cost: StepCost::default(),
            });
            results.push(json!({ "step_id": step_id, "status": "scheduled", "reason": "dependencies_pending" }));
            continue;
        }

        // Execute step
        tracing::info!("Executing step: {} (executor: {})", step_id, config.executor);
        update_step_state(board_id, &cell.cell_id, cell, "running", None, None, &run_id, command_tx)?;

        // Notify UI that step is running
        let _ = event_tx.send(SwiftEvent::StatusUpdate {
            message: format!("Pipeline: step '{}' running", step_id),
        });

        // Dashboard: step → running + progress through the DAG. Executed steps are
        // agentic/compute work (`ai`); plugin steps are AI-actor with a `plugin` tag.
        let actor = Actor::Ai;
        current_stage = Some(stage.clone());
        current_item = Some(name.clone());
        let _ = event_tx.send(tags.step_state(
            step_id, &name, &stage, StepState::Running, actor, plugin.clone(), chrono::Utc::now().timestamp(),
        ));
        let _ = event_tx.send(tags.step_progress(step_id, &stage, processed, total_steps as u64, &name));

        let start = std::time::Instant::now();

        // ── Gather inputs from dependency cells (DB reads, not in-memory) ──
        let dependency_outputs = gather_dependency_outputs(board_id, &config.depends_on);
        eprintln!("📺 PIPELINE: Step {} has {} dependency outputs: {:?}",
            step_id,
            dependency_outputs.len(),
            dependency_outputs.iter().map(|d| format!("{}({}B)", d["step_id"].as_str().unwrap_or("?"), d["output"].as_str().map(|s| s.len()).unwrap_or(0))).collect::<Vec<_>>()
        );

        // Asset metadata for ordinary steps, PLUS the cell's own `mcp_tool` spec
        // (if any) threaded through so the local MCP-tool path can fire. The
        // executor reads `metadata.mcp_tool` to decide whether a `local` step is a
        // plugin-tool dispatch — without this merge the on-device MCP path can
        // never trigger from a real run (it was only reachable by calling
        // `execute_pipeline_step` directly in unit tests). See `parse_mcp_tool_step`.
        let mut metadata = crate::pipeline_executor::find_asset_metadata(board_id)
            .unwrap_or_else(|| json!({}));
        if let Some(mcp_tool) = cell.metadata_json.as_ref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .and_then(|m| m.get("mcp_tool").cloned())
        {
            metadata["mcp_tool"] = mcp_tool;
        }
        let metadata = Some(metadata);

        // ── Build step execution context ──
        let _model_endpoint = config.model_config.as_ref()
            .and_then(|m| m.endpoint.clone());
        let _model_id = config.model_config.as_ref()
            .map(|m| m.id.clone())
            .or_else(|| config.model.clone());

        let result = match crate::pipeline_executor::execute_pipeline_step(
            board_id, step_id, &cell.content, &config.executor,
            metadata, dependency_outputs, command_tx, event_tx,
        ).await {
            Ok((summary, findings)) => {
                Ok(summary)
            }
            Err(e) => Err(e),
        };

        let duration = start.elapsed().as_secs_f64();
        let wall_ms = (duration * 1000.0) as u64;
        // Cost rail: a step's compute wall time is a GPU-time proxy on the cost rail
        // (DASHBOARD_CONTRACT §C `gpu_ms`). External (plugin) USD is attributed on the
        // executor's own obs and is not threaded back here yet (noted in STATUS).
        let cost = StepCost { gpu_ms: wall_ms, ..StepCost::default() };
        processed += 1;

        match result {
            Ok(output) => {
                tracing::info!("Step {} completed in {:.1}s", step_id, duration);
                update_step_state_full(
                    board_id, &cell.cell_id, cell, "ai_complete",
                    Some(&output), None, &run_id, Some(duration), command_tx,
                )?;

                // Send notification
                let _ = event_tx.send(SwiftEvent::StatusUpdate {
                    message: format!("Pipeline: step '{}' complete ({:.1}s)", step_id, duration),
                });

                // Dashboard: step → done.
                let _ = event_tx.send(tags.step_state(
                    step_id, &name, &stage, StepState::Done, actor, plugin.clone(), chrono::Utc::now().timestamp(),
                ));
                obs.record(StepObs {
                    step_id: step_id.clone(),
                    name: name.clone(),
                    stage: stage.clone(),
                    actor,
                    plugin: plugin.clone(),
                    state: StepState::Done,
                    wall_ms,
                    gate_ms: 0,
                    cost: cost.clone(),
                });

                // Process notifications for this step
                fire_notifications(config, "ai_complete", board_id, step_id, event_tx);

                results.push(json!({
                    "step_id": step_id,
                    "status": "ai_complete",
                    "duration": duration,
                    "output_length": output.len()
                }));
            }
            Err(e) => {
                tracing::error!("Step {} failed: {}", step_id, e);
                update_step_state(
                    board_id, &cell.cell_id, cell, "failed",
                    None, Some(&e.to_string()), &run_id, command_tx,
                )?;

                // Dashboard: step → failed.
                any_failed = true;
                let _ = event_tx.send(tags.step_state(
                    step_id, &name, &stage, StepState::Failed, actor, plugin.clone(), chrono::Utc::now().timestamp(),
                ));
                obs.record(StepObs {
                    step_id: step_id.clone(),
                    name: name.clone(),
                    stage: stage.clone(),
                    actor,
                    plugin: plugin.clone(),
                    state: StepState::Failed,
                    wall_ms,
                    gate_ms: 0,
                    cost: cost.clone(),
                });

                fire_notifications(config, "failed", board_id, step_id, event_tx);

                results.push(json!({
                    "step_id": step_id,
                    "status": "failed",
                    "error": e.to_string()
                }));

                // Don't break — continue with independent steps
            }
        }
    }

    // Check if pipeline is complete
    let all_done = steps.iter().all(|(_, config)| {
        config.state.status == "human_approved" || config.state.status == "skipped"
    });

    if all_done {
        // Fire pipeline_complete notifications
        for (_, config) in &steps {
            fire_notifications(config, "pipeline_complete", board_id, &config.step_id, event_tx);
        }
    }

    // ── Dashboard producer: the rolled-up read-model + the run-finished marker. ──
    let finished_at = chrono::Utc::now().timestamp();
    let snapshot = obs.snapshot(finished_at, current_item, current_stage);
    let _ = event_tx.send(tags.stats(snapshot));
    let run_state = if any_failed { "failed" } else { "done" };
    let _ = event_tx.send(tags.run_finished(run_state, finished_at));

    Ok(json!({
        "run_id": run_id,
        "board_id": board_id,
        "steps_executed": results.len(),
        "results": results
    }))
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
            let home = std::env::var("HOME").map_err(|e| e)?;
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

    // Filter out cells that are just URLs (asset references)
    let step_cells: Vec<_> = cells.iter()
        .filter(|c| {
            let trimmed = c.content.trim();
            !trimmed.starts_with("http://") && !trimmed.starts_with("https://") && !trimmed.is_empty()
        })
        .collect();

    if step_cells.is_empty() {
        return Err(anyhow!("No pipeline steps found (only asset references)"));
    }

    // Build prompt
    let mut prompt = String::from(
        "Return ONLY a JSON array of pipeline step configs. No explanation, no markdown, just the JSON array.\n\n\
         Steps:\n"
    );

    for (i, cell) in step_cells.iter().enumerate() {
        let title = first_line(&cell.content);
        prompt.push_str(&format!("{}. {}\n", i + 1, title));
    }

    prompt.push_str(
        "\nEach object needs: step_id (snake_case string), depends_on (array of step_id strings that must complete first), \
         executor (one of: local, lens, cloud, manual), command (string or null), timeout_seconds (integer)"
    );

    tracing::info!("Pipeline compile: sending {} steps to vLLM", step_cells.len());

    // Call vLLM
    let response = call_vllm(&prompt, 1000, 0.1).await?;

    // Parse JSON array from response (might have markdown fences)
    let cleaned = response
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let configs: Vec<serde_json::Value> = serde_json::from_str(cleaned)
        .map_err(|e| anyhow!("Failed to parse vLLM response as JSON: {}. Raw: {}", e, &cleaned[..cleaned.len().min(300)]))?;

    tracing::info!("Pipeline compile: got {} step configs from vLLM", configs.len());

    // Apply configs to cells
    let mut applied = 0;
    for (i, config_val) in configs.iter().enumerate() {
        if i >= step_cells.len() { break; }
        let cell = step_cells[i];

        let step_id = config_val["step_id"].as_str().unwrap_or(&format!("step_{}", i)).to_string();

        let config = PipelineStepConfig {
            step_id: step_id.clone(),
            depends_on: config_val["depends_on"].as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            stage: config_val["stage"].as_str().map(String::from),
            executor: config_val["executor"].as_str().unwrap_or("lens").to_string(),
            model: Some("cyan-lens".to_string()),
            model_config: None,
            tools: vec![],
            output_format: "markdown".to_string(),
            command: config_val["command"].as_str().map(String::from),
            timeout_seconds: config_val["timeout_seconds"].as_u64(),
            retry_count: Some(1),
            auto_advance: false,
            notifications: vec![],
            state: PipelineStepState::default(),
        };

        // Merge into cell metadata
        let mut metadata: serde_json::Value = cell.metadata_json.as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(json!({}));

        metadata["pipeline"] = serde_json::to_value(&config)?;

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

    Ok(json!({
        "success": true,
        "steps_compiled": applied,
        "configs": configs
    }))
}

/// Execute a step via vLLM (for AI analysis tasks)
async fn execute_lens_step(config: &PipelineStepConfig, cell_content: &str) -> Result<String> {
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

            steps.push(json!({
                "step_id": config.step_id,
                "title": first_line(&cell.content),
                "status": config.state.status,
                "executor": config.executor,
                "depends_on": config.depends_on,
                "ai_result": config.state.ai_result,
                "error": config.state.error,
                "duration": config.state.duration,
            }));
        }
    }

    Ok(json!({
        "board_id": board_id,
        "total_steps": total,
        "ai_complete": ai_complete,
        "human_approved": human_approved,
        "running": running,
        "failed": failed,
        "pending": pending,
        "progress_pct": if total > 0 { (human_approved * 100) / total } else { 0 },
        "steps": steps
    }))
}

// ============================================================================
// Approve: Human Approves a Step
// ============================================================================

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

/// Retry a pipeline step — reset to pending while preserving all metadata
pub fn retry_step(
    board_id: &str,
    step_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
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
    
    // Increment attempt counter
    let attempt = metadata["pipeline"]["state"]["attempt"].as_u64().unwrap_or(0);
    metadata["pipeline"]["state"]["attempt"] = json!(attempt + 1);

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

    tracing::info!("Step {} reset to pending (retry)", step_id);
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
        if cell.pipeline_config.is_none() { continue; }

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

    let mut stmt = conn.prepare(
        "SELECT id, board_id, cell_order, content, metadata_json \
         FROM notebook_cells WHERE board_id = ?1 ORDER BY cell_order"
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
    cell: &PipelineCell,
    status: &str,
    ai_result: Option<&str>,
    error: Option<&str>,
    run_id: &str,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<()> {
    update_step_state_full(board_id, cell_id, cell, status, ai_result, error, run_id, None, command_tx)
}

fn update_step_state_full(
    board_id: &str,
    cell_id: &str,
    _cell: &PipelineCell, // kept for signature compat, but we re-read from DB
    status: &str,
    ai_result: Option<&str>,
    error: Option<&str>,
    run_id: &str,
    duration: Option<f64>,
    command_tx: &UnboundedSender<CommandMsg>,
) -> Result<()> {
    // CRITICAL: Re-read cell from DB to get latest metadata (avoids race condition)
    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;

    let (content, cell_order, current_metadata_json): (String, i32, Option<String>) = conn.query_row(
        "SELECT content, cell_order, metadata_json FROM notebook_cells WHERE id = ?1",
        rusqlite::params![cell_id],
        |row| Ok((
            row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            row.get(1)?,
            row.get(2)?,
        )),
    ).map_err(|e| anyhow!("Cell not found: {}", e))?;

    drop(conn); // Release lock before sending command

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

    let _ = command_tx.send(CommandMsg::UpdateNotebookCell {
        id: cell_id.to_string(),
        board_id: board_id.to_string(),
        cell_type: "markdown".to_string(),
        cell_order,
        content: Some(content),
        output: ai_result.map(|s| s.to_string()),
        collapsed: false,
        height: None,
        metadata_json: Some(metadata.to_string()),
    });

    tracing::info!("📝 Pipeline step {} → {} (metadata: {}B)", cell_id.get(..8).unwrap_or(cell_id), status, metadata.to_string().len());

    Ok(())
}

/// Fire notifications for a step event
fn fire_notifications(
    config: &PipelineStepConfig,
    trigger: &str,
    board_id: &str,
    step_id: &str,
    event_tx: &UnboundedSender<SwiftEvent>,
) {
    for notif in &config.notifications {
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