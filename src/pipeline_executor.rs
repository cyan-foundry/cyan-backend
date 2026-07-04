// cyan-backend/src/pipeline_executor.rs
//
// Pipeline step executor that routes through Cyan Lens.
// 
// Flow:
//   1. Pipeline sends step to Lens: POST /api/v1/execute
//   2. Lens runs ReAct loop, returns either:
//      a) Final result (cloud step — Lens ran everything)
//      b) Pending tool calls (local step — needs client to run tools)
//   3. For local steps, backend runs tools and sends results back
//   4. Loop until Lens returns final result
//   5. Save findings as timecoded notes
//   6. Publish pipeline events to Iggy for enrichment
//
// This replaces the direct skill execution in pipeline.rs

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc::UnboundedSender;

use crate::models::commands::CommandMsg;
use crate::models::events::SwiftEvent;

// ============================================================================
// Lens API Types
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct LensExecuteRequest {
    pub step_id: String,
    pub board_id: String,
    pub cell_content: String,
    pub executor_type: String,              // "local" or "cloud"
    pub metadata: Option<serde_json::Value>,
    pub previous_outputs: Vec<serde_json::Value>,
    pub human_input: Option<String>,
    pub tools_markdown: Option<String>,     // client-defined tools
    pub skills_markdown: Option<String>,    // client-defined skills
}

#[derive(Debug, Clone, Deserialize)]
pub struct LensExecuteResponse {
    pub success: bool,
    pub run_id: String,
    pub status: String,                     // "complete", "failed", "needs_tool_execution", "needs_human"
    #[serde(default)]
    pub result: Option<StepResult>,
    #[serde(default)]
    pub pending_tool_calls: Vec<ToolCall>,   // tools for client to execute locally
    #[serde(default)]
    pub status_markers: Vec<StatusMarker>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StepResult {
    pub step_id: String,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<Finding>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub reasoning_trace: Vec<serde_json::Value>,
    #[serde(default)]
    pub tools_used: Vec<String>,
    #[serde(default)]
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub timecode_seconds: f64,
    pub content: String,
    pub finding_type: String,
    pub severity: String,
    #[serde(default)]
    pub suggested_action: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub call_id: String,
    pub tool_id: String,
    pub args: Vec<String>,
    #[serde(default)]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub call_id: String,
    pub tool_id: String,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct LensContinueRequest {
    pub run_id: String,
    pub tool_results: Vec<ToolResult>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatusMarker {
    pub timestamp: i64,
    pub icon: String,
    pub message: String,
}

// ============================================================================
// Iggy Pipeline Event Types
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct PipelineEvent {
    pub event_type: String,         // step_started, step_completed, step_failed, finding_created, human_approved
    pub board_id: String,
    pub step_id: String,
    pub run_id: String,
    pub timestamp: i64,
    pub data: serde_json::Value,
}

// ============================================================================
// Execute Step via Lens (with local/cloud routing)
// ============================================================================

/// Execute a pipeline step through Cyan Lens.
/// For cloud steps: Lens runs everything.
/// For local steps: Lens orchestrates, client executes tools locally.
pub async fn execute_step_via_lens(
    lens_url: &str,
    board_id: &str,
    step_id: &str,
    cell_content: &str,
    executor_type: &str,
    metadata: Option<serde_json::Value>,
    previous_outputs: Vec<serde_json::Value>,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<(String, Vec<Finding>)> {
    
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()?;
    
    let run_id = format!("run_{}_{}", &step_id[..step_id.len().min(8)], chrono::Utc::now().timestamp() % 10000);
    
    // Publish: step started
    publish_pipeline_event(event_tx, PipelineEvent {
        event_type: "step_started".into(),
        board_id: board_id.into(),
        step_id: step_id.into(),
        run_id: run_id.clone(),
        timestamp: chrono::Utc::now().timestamp(),
        data: json!({ "executor_type": executor_type, "cell_content": &cell_content[..cell_content.len().min(100)] }),
    });
    
    // Step 1: Send initial execute request
    let request = LensExecuteRequest {
        step_id: step_id.into(),
        board_id: board_id.into(),
        cell_content: cell_content.into(),
        executor_type: executor_type.into(),
        metadata,
        previous_outputs,
        human_input: None,
        tools_markdown: None,
        skills_markdown: None,
    };
    
    eprintln!("📺 PIPELINE: Step {} → Lens API ({} executor)", step_id, executor_type);
    
    let _ = event_tx.send(SwiftEvent::StatusUpdate {
        message: format!("🔄 Step '{}' → Cyan Lens", step_id),
    });
    
    let mut response = call_lens_execute(&client, lens_url, &request).await?;
    
    // Step 2: Handle back-and-forth for local tool execution
    let mut iteration = 0;
    let max_iterations = 20; // safety limit
    
    while response.status == "needs_tool_execution" && iteration < max_iterations {
        iteration += 1;
        
        eprintln!("📺 PIPELINE: Step {} needs {} local tool calls (iteration {})", 
            step_id, response.pending_tool_calls.len(), iteration);
        
        // Send status markers to UI
        for marker in &response.status_markers {
            let _ = event_tx.send(SwiftEvent::StatusUpdate {
                message: format!("{} {}", marker.icon, marker.message),
            });
        }
        
        // Execute tools locally
        let mut tool_results = Vec::new();
        for tool_call in &response.pending_tool_calls {
            let _ = event_tx.send(SwiftEvent::StatusUpdate {
                message: format!("🔧 Running {} locally...", tool_call.tool_id),
            });
            
            let result = execute_tool_locally(tool_call).await;
            
            eprintln!("📺 PIPELINE: Local {} → exit={}", 
                tool_call.tool_id, result.exit_code);
            
            tool_results.push(result);
        }
        
        // Send results back to Lens
        let continue_req = LensContinueRequest {
            run_id: run_id.clone(),
            tool_results,
        };
        
        response = call_lens_continue(&client, lens_url, &continue_req).await?;
    }
    
    // Step 3: Process final response
    // Send remaining status markers
    for marker in &response.status_markers {
        let _ = event_tx.send(SwiftEvent::StatusUpdate {
            message: format!("{} {}", marker.icon, marker.message),
        });
    }
    
    if let Some(ref result) = response.result {
        // Save findings as timecoded notes
        let findings = result.findings.clone();
        for finding in &findings {
            let note = crate::timecode_notes::TimecodeNote {
                id: uuid::Uuid::new_v4().to_string(),
                board_id: board_id.to_string(),
                timecode_seconds: finding.timecode_seconds,
                content: finding.content.clone(),
                note_type: finding.finding_type.clone(),
                author: format!("AI/{}", step_id),
                created_at: chrono::Utc::now().timestamp() as f64,
                pipeline_step_id: Some(step_id.to_string()),
                pipeline_phase: Some("during".to_string()),
                ai_reviewed: true,
                human_approved: false,
                action_skill: None,
                action_status: Some("complete".to_string()),
                action_result: finding.suggested_action.clone(),
                action_model: result.tools_used.first().cloned(),
                ai_flags_nearby: vec![],
                reply_to: None,
                thread_count: 0,
            };
            let _ = crate::timecode_notes::save_note(&note, command_tx);
        }
        
        if !findings.is_empty() {
            eprintln!("📺 PIPELINE: Saved {} timecoded notes for step {}", findings.len(), step_id);
        }
        
        // Publish: step completed
        publish_pipeline_event(event_tx, PipelineEvent {
            event_type: "step_completed".into(),
            board_id: board_id.into(),
            step_id: step_id.into(),
            run_id: run_id.clone(),
            timestamp: chrono::Utc::now().timestamp(),
            data: json!({
                "summary": result.summary,
                "findings_count": findings.len(),
                "tools_used": result.tools_used,
                "duration_ms": result.duration_ms,
            }),
        });
        
        // Publish each finding as a separate event (for graph enrichment)
        for finding in &findings {
            publish_pipeline_event(event_tx, PipelineEvent {
                event_type: "finding_created".into(),
                board_id: board_id.into(),
                step_id: step_id.into(),
                run_id: run_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
                data: json!({
                    "timecode_seconds": finding.timecode_seconds,
                    "content": finding.content,
                    "finding_type": finding.finding_type,
                    "severity": finding.severity,
                }),
            });
        }
        
        Ok((result.summary.clone(), findings))
    } else if response.status == "needs_human" {
        let question = response.error.unwrap_or_else(|| "Human input needed".into());
        
        publish_pipeline_event(event_tx, PipelineEvent {
            event_type: "step_needs_human".into(),
            board_id: board_id.into(),
            step_id: step_id.into(),
            run_id: run_id.clone(),
            timestamp: chrono::Utc::now().timestamp(),
            data: json!({ "question": question }),
        });
        
        Err(anyhow!("needs_human: {}", question))
    } else {
        let error = response.error.unwrap_or_else(|| "Unknown error".into());
        
        publish_pipeline_event(event_tx, PipelineEvent {
            event_type: "step_failed".into(),
            board_id: board_id.into(),
            step_id: step_id.into(),
            run_id: run_id.clone(),
            timestamp: chrono::Utc::now().timestamp(),
            data: json!({ "error": error }),
        });
        
        Err(anyhow!("Lens execution failed: {}", error))
    }
}

// ============================================================================
// Local Tool Execution
// ============================================================================

async fn execute_tool_locally(tool_call: &ToolCall) -> ToolResult {
    let timeout = if tool_call.timeout_seconds > 0 { tool_call.timeout_seconds } else { 60 };
    
    let binary = tool_call.tool_id.as_str();
    
    match tokio::time::timeout(
        std::time::Duration::from_secs(timeout),
        tokio::process::Command::new(binary)
            .args(&tool_call.args)
            .output()
    ).await {
        Ok(Ok(output)) => ToolResult {
            call_id: tool_call.call_id.clone(),
            tool_id: tool_call.tool_id.clone(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code().unwrap_or(-1),
        },
        Ok(Err(e)) => ToolResult {
            call_id: tool_call.call_id.clone(),
            tool_id: tool_call.tool_id.clone(),
            stdout: String::new(),
            stderr: format!("Execution error: {}", e),
            exit_code: -1,
        },
        Err(_) => ToolResult {
            call_id: tool_call.call_id.clone(),
            tool_id: tool_call.tool_id.clone(),
            stdout: String::new(),
            stderr: format!("Timed out after {}s", timeout),
            exit_code: -1,
        },
    }
}

// ============================================================================
// Lens API Calls
// ============================================================================

async fn call_lens_execute(
    client: &reqwest::Client,
    lens_url: &str,
    request: &LensExecuteRequest,
) -> Result<LensExecuteResponse> {
    let url = format!("{}/api/v1/execute", lens_url);
    
    let response = client.post(&url)
        .json(request)
        .send()
        .await
        .map_err(|e| anyhow!("Lens API unreachable: {}", e))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("Lens API returned {}: {}", status, &body[..body.len().min(200)]));
    }
    
    response.json().await
        .map_err(|e| anyhow!("Failed to parse Lens response: {}", e))
}

async fn call_lens_continue(
    client: &reqwest::Client,
    lens_url: &str,
    request: &LensContinueRequest,
) -> Result<LensExecuteResponse> {
    let url = format!("{}/api/v1/execute/continue", lens_url);
    
    let response = client.post(&url)
        .json(request)
        .send()
        .await
        .map_err(|e| anyhow!("Lens continue API unreachable: {}", e))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("Lens continue API returned {}: {}", status, &body[..body.len().min(200)]));
    }
    
    response.json().await
        .map_err(|e| anyhow!("Failed to parse Lens continue response: {}", e))
}

// ============================================================================
// Pipeline Event Publishing (→ Iggy → Lens enricher → Graph)
// ============================================================================

fn publish_pipeline_event(
    event_tx: &UnboundedSender<SwiftEvent>,
    event: PipelineEvent,
) {
    eprintln!("📡 PIPELINE EVENT: {} [{}] step={}", event.event_type, &event.board_id[..8], event.step_id);
    
    // Send as SwiftEvent::GenericEvent which gets routed to Iggy via the network actor
    let _ = event_tx.send(SwiftEvent::StatusUpdate { message: format!("📡 pipeline.{}: step={}", event.event_type, event.step_id) });
}

// ============================================================================
// Integration with existing pipeline.rs
// ============================================================================

/// Drop-in replacement for the skill execution block in run_pipeline().
/// Call this instead of the old skill_match + execute_skill path.
pub async fn execute_pipeline_step(
    board_id: &str,
    step_id: &str,
    cell_content: &str,
    executor_type: &str,
    metadata: Option<serde_json::Value>,
    previous_outputs: Vec<serde_json::Value>,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<(String, Vec<Finding>)> {

    // ── DIRECT PLUGIN-TOOL BIND: dispatch on-device, intent resolver STANDS DOWN ──
    // A step whose metadata names a plugin tool (`{ "mcp_tool": { plugin_id, tool,
    // args } }`) is a RESOLVED bind (an `@plugin.tool` the author chose). It is
    // dispatched through the supervised cyan-mcp host on this device — the local
    // mirror of the lens cloud `McpTool` path.
    //
    // BURST C4: the direct bind is authoritative, so the SkillRegistry/intent resolver
    // (whether the local `resolve_intent` or the Lens ReAct loop) must NOT run — running
    // it wastes a turn and risks binding the WRONG tool (the live run "resolved"
    // ingest→qc_analysis / upload→ssai_break_detection at score 1 BEFORE the @-bind
    // overrode it). We therefore take this path for ANY executor_type when a bind is
    // present, not just `local` — a bound step never reaches the resolver.
    // Ordinary steps (no `mcp_tool`) fall straight through unchanged.
    if let Some(step) = metadata.as_ref().and_then(parse_mcp_tool_step) {
        return execute_local_mcp_tool_step(board_id, step_id, step, command_tx, event_tx).await;
    }

    // ── LICENSE GATE: a genuinely-cloud (Lens) run is a paid surface ────
    // Round 8 / W11. Local-placement steps (handled above and below) run
    // unconditionally offline; only a cloud/Lens run consults the installed
    // license. With NO gate installed this is a no-op, so existing behavior and
    // the local test rigs are unchanged. A denied tenant gets a clear "needs a
    // license" state without blocking the local steps.
    if matches!(executor_type, "cloud" | "lens")
        && let Err(reason) =
            crate::licensing::gate_cloud_action(crate::licensing::CloudAction::RunWorkflow)
    {
        let _ = event_tx.send(SwiftEvent::StatusUpdate {
            message: format!("🔒 Step '{step_id}' needs a license: {reason}"),
        });
        return Err(anyhow!("cloud step gated: {reason}"));
    }

    // ── DEMO CACHE: Check for cached results first ──────────────────────
    // Remove this block when productionizing. It plays back pre-computed
    // results with realistic delays so the demo doesn't depend on GPU/Lens.
    if let Some(cached) = load_cached_step_result(step_id) {
        eprintln!("📺 PIPELINE: Cache hit for step '{}' — simulating execution", step_id);
        
        let _ = event_tx.send(SwiftEvent::StatusUpdate {
            message: format!("🔄 Step '{}' → Cyan Lens", step_id),
        });
        
        // Simulate model inference with progressive status updates
        for marker in &cached.status_markers {
            tokio::time::sleep(std::time::Duration::from_millis(marker.delay_ms)).await;
            let _ = event_tx.send(SwiftEvent::StatusUpdate {
                message: format!("{} {}", marker.icon, marker.message),
            });
        }
        
        // Final pause before "completing"
        tokio::time::sleep(std::time::Duration::from_millis(cached.final_delay_ms)).await;
        
        // Save findings as timecoded notes
        for finding in &cached.findings {
            let note = crate::timecode_notes::TimecodeNote {
                id: uuid::Uuid::new_v4().to_string(),
                board_id: board_id.to_string(),
                timecode_seconds: finding.timecode_seconds,
                content: finding.content.clone(),
                note_type: finding.finding_type.clone(),
                author: format!("AI/{}", step_id),
                created_at: chrono::Utc::now().timestamp() as f64,
                pipeline_step_id: Some(step_id.to_string()),
                pipeline_phase: Some("during".to_string()),
                ai_reviewed: true,
                human_approved: false,
                action_skill: None,
                action_status: Some("complete".to_string()),
                action_result: finding.suggested_action.clone(),
                action_model: cached.model_used.clone(),
                ai_flags_nearby: vec![],
                reply_to: None,
                thread_count: 0,
            };
            let _ = crate::timecode_notes::save_note(&note, command_tx);
        }
        
        if !cached.findings.is_empty() {
            eprintln!("📺 PIPELINE: Saved {} cached timecoded notes for step {}", cached.findings.len(), step_id);
        }
        
        let _ = event_tx.send(SwiftEvent::StatusUpdate {
            message: format!("✅ Step '{}' complete ({:.1}s)", step_id, cached.simulated_duration),
        });
        
        return Ok((cached.summary, cached.findings));
    }
    // ── END DEMO CACHE ──────────────────────────────────────────────────

    let lens_url = std::env::var("CYAN_LENS_URL")
        .unwrap_or_else(|_| "http://localhost:9080".to_string());
    
    // Try Lens first
    match execute_step_via_lens(
        &lens_url, board_id, step_id, cell_content, executor_type,
        metadata, previous_outputs.clone(), command_tx, event_tx,
    ).await {
        Ok(result) => Ok(result),
        Err(lens_err) => {
            eprintln!("📺 PIPELINE: Lens failed for step {}: {}. Falling back to local.", step_id, lens_err);
            
            let _ = event_tx.send(SwiftEvent::StatusUpdate {
                message: format!("⚠️ Lens unavailable, running '{}' locally", step_id),
            });
            
            // Fall back to local skill execution
            execute_step_locally(
                board_id, step_id, cell_content,
                previous_outputs, command_tx, event_tx,
            ).await
        }
    }
}

/// Local fallback — uses the existing skill system in cyan-backend
async fn execute_step_locally(
    board_id: &str,
    step_id: &str,
    cell_content: &str,
    previous_outputs: Vec<serde_json::Value>,
    command_tx: &UnboundedSender<CommandMsg>,
    _event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<(String, Vec<Finding>)> {
    let registry = crate::skills::registry();
    let skill_match = registry.resolve_intent(cell_content);
    
    if let Some(skill_def) = skill_match {
        let video_uri = find_video_uri(board_id);
        let scope_id = find_scope_id(board_id);
        
        let skill_ctx = crate::skills::SkillContext {
            board_id: board_id.to_string(),
            step_id: step_id.to_string(),
            credentials: std::collections::HashMap::new(),
            cell_content: cell_content.to_string(),
            previous_outputs: previous_outputs.iter()
                .filter_map(|v| {
                    let output = v["output"].as_str()
                        .or_else(|| v["summary"].as_str())?;
                    Some(crate::skills::StepOutput {
                        step_id: v["step_id"].as_str()?.to_string(),
                        output: output.to_string(),
                        output_type: crate::skills::OutputType::Summary,
                        artifacts: std::collections::HashMap::new(),
                    })
                })
                .collect(),
            video_uri,
            scope_id,
        };
        
        match crate::skills::execute_skill(&skill_def.id, &skill_ctx).await {
            Ok(skill_result) => {
                let mut findings = Vec::new();
                
                // Convert skill findings to our Finding type and save as notes
                if let Some(ref sf) = skill_result.timecoded_findings {
                    for f in sf {
                        let finding = Finding {
                            timecode_seconds: f.timecode_seconds,
                            content: f.content.clone(),
                            finding_type: f.finding_type.clone(),
                            severity: f.severity.clone(),
                            suggested_action: f.suggested_action.clone(),
                        };
                        
                        let note = crate::timecode_notes::TimecodeNote {
                            id: uuid::Uuid::new_v4().to_string(),
                            board_id: board_id.to_string(),
                            timecode_seconds: f.timecode_seconds,
                            content: f.content.clone(),
                            note_type: f.finding_type.clone(),
                            author: format!("AI/{}", skill_def.id),
                            created_at: chrono::Utc::now().timestamp() as f64,
                            pipeline_step_id: Some(step_id.to_string()),
                            pipeline_phase: Some("during".to_string()),
                            ai_reviewed: true,
                            human_approved: false,
                            action_skill: None,
                            action_status: Some("complete".to_string()),
                            action_result: f.suggested_action.clone(),
                            action_model: skill_def.tools.first().cloned(),
                            ai_flags_nearby: vec![],
                            reply_to: None,
                            thread_count: 0,
                        };
                        let _ = crate::timecode_notes::save_note(&note, command_tx);
                        findings.push(finding);
                    }
                }
                
                Ok((skill_result.summary, findings))
            }
            Err(e) => Err(e),
        }
    } else {
        // No skill match — try raw vLLM call
        let prompt = format!("Execute this pipeline step:\n\n{}", cell_content);
        let response = crate::pipeline::call_vllm_public(&prompt, 800, 0.3).await?;
        Ok((response, vec![]))
    }
}

// ============================================================================
// Local MCP Plugin Tool Dispatch (device host — no cloud round-trip)
// ============================================================================
//
// A `local` pipeline step can name a plugin tool to run ON-DEVICE through the
// supervised cyan-mcp host (the local mirror of the lens cloud `McpTool` path).
// The dispatch LOGIC (initialize → call_tool → thread the result, the external
// cost rail, the approval gate) lives in `mcp_host.rs` and is unit-tested via
// cyan-mcp's `ScriptedTransport`. THIS is the prod wiring: it resolves the tool
// in the installed registry, reads the human-approval gate, spawns a real
// `StdioTransport` from the bundle, and dispatches. (Cred injection at spawn and
// the runtime→entrypoint mapping are the deferred device lifecycle; today the
// bundle ships a `run` entrypoint.)

use crate::mcp_host::{McpDispatch, McpTool, PluginHost, RunCostLedger, RunScope};
use cyan_mcp::PluginTransport as _; // brings `spawn`/`send`/`recv` into scope
use std::sync::Arc;

/// Parse an optional `McpTool` step from a step's metadata:
/// `{ "mcp_tool": { "plugin_id": ..., "tool": ..., "args": {...} } }`.
/// `None` (the common case) means an ordinary step — the caller falls through.
fn parse_mcp_tool_step(metadata: &serde_json::Value) -> Option<McpTool> {
    let spec = metadata.get("mcp_tool")?;
    Some(McpTool {
        plugin_id: spec.get("plugin_id")?.as_str()?.to_string(),
        tool: spec.get("tool")?.as_str()?.to_string(),
        args: spec.get("args").cloned().unwrap_or(serde_json::Value::Null),
    })
}

/// The device's installed-plugins root: one subdir per plugin (the unpacked
/// `.cyanplugin` bundles the file-swarm fetched). Overridable for tests/ops.
fn plugins_root() -> std::path::PathBuf {
    if let Ok(root) = std::env::var("CYAN_PLUGINS_ROOT") {
        return std::path::PathBuf::from(root);
    }
    let home = std::env::var("HOME").unwrap_or_default();
    std::path::PathBuf::from(home).join(".cyan").join("plugins")
}

/// The tenant this device acts as (carried on every external cost obs line).
fn device_tenant() -> String {
    std::env::var("CYAN_TENANT_ID").unwrap_or_else(|_| "device".to_string())
}

/// Whether the human-approval gate is open for this step. Reuses the existing
/// pipeline approval path, which sets `pipeline.state.status = "human_approved"`
/// (see `pipeline.rs::approve_step`).
fn step_is_approved(board_id: &str, step_id: &str) -> bool {
    let conn = match crate::storage::db().lock() {
        Ok(c) => c,
        Err(_) => return false,
    };
    let mut stmt = match conn.prepare("SELECT metadata_json FROM notebook_cells WHERE board_id = ?1")
    {
        Ok(s) => s,
        Err(_) => return false,
    };
    let rows = match stmt.query_map(rusqlite::params![board_id], |row| {
        row.get::<_, Option<String>>(0)
    }) {
        Ok(r) => r,
        Err(_) => return false,
    };
    for meta in rows.flatten().flatten() {
        let v: serde_json::Value = match serde_json::from_str(&meta) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v["pipeline"]["step_id"] == json!(step_id)
            && v["pipeline"]["state"]["status"] == json!("human_approved")
        {
            return true;
        }
    }
    false
}

/// Dispatch a `local` plugin-tool step on-device through the cyan-mcp host.
///
/// Emits the same dashboard exec events the cloud (lens) path emits
/// (`step_started` / `step_completed` / `finding_created`, or `step_needs_human`
/// when gated) so the dashboard read-model lights up for on-device plugin steps
/// exactly as it does for cloud steps (DASHBOARD_CONTRACT §A/§C). The plugin's
/// JSON result becomes a finding so the step produces a reviewable result.
async fn execute_local_mcp_tool_step(
    board_id: &str,
    step_id: &str,
    mut step: McpTool,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<(String, Vec<Finding>)> {
    // BURST C4 (path resolution): a cyan-media tool needs a concrete input path. The
    // compiled step only carries `{plugin_id, tool}` (no args), so EVERY consuming step —
    // ingest AND proxy/conform/etc. — must have the board's asset resolved to the SAME
    // container path here, or the plugin reports `path_denied` / `input_bytes:0`. We resolve
    // it ONCE, through the same `find_video_uri` the ingest path uses, and inject it into the
    // args under the keys cyan-media accepts — unless the author already supplied one.
    resolve_media_args(board_id, &mut step);

    let run_id = format!(
        "run_{}_{}",
        &step_id[..step_id.len().min(8)],
        chrono::Utc::now().timestamp() % 10000
    );

    let _ = event_tx.send(SwiftEvent::StatusUpdate {
        message: format!(
            "🔌 Step '{}' → local plugin {}.{}",
            step_id, step.plugin_id, step.tool
        ),
    });

    // Dashboard exec event: the step started (on-device, plugin actor).
    publish_pipeline_event(event_tx, PipelineEvent {
        event_type: "step_started".into(),
        board_id: board_id.into(),
        step_id: step_id.into(),
        run_id: run_id.clone(),
        timestamp: chrono::Utc::now().timestamp(),
        data: json!({ "executor_type": "local", "plugin_id": step.plugin_id, "tool": step.tool }),
    });

    let root = plugins_root();
    let tenant = device_tenant();

    // Device plugin host. The tool call uses its own throwaway sink (relayed
    // events are incidental here — the tool *result* threads back into the step).
    let host = PluginHost::new(
        Arc::new(cyan_mcp::RecordingSink::new()) as Arc<dyn cyan_mcp::EventSink>,
        Arc::new(cyan_mcp::LogEmitter::new()) as Arc<dyn cyan_mcp::Emitter>,
        Arc::new(cyan_mcp::SystemClock::new()) as Arc<dyn cyan_mcp::Clock>,
        cyan_mcp::BackoffPolicy {
            base: std::time::Duration::from_millis(500),
            max: std::time::Duration::from_secs(30),
            max_restarts: 3,
        },
        tenant.clone(),
    );

    // Resolve the tool in the installed registry → its manifest `side_effects`
    // (which drive the gate). Not installed = a real, surfaced error.
    let side_effects = host
        .resolve_installed_tool(&root, &step.tool)
        .map_err(|e| anyhow!("resolve plugin tool {}: {}", step.tool, e))?
        .map(|(_, tb)| tb.side_effects)
        .ok_or_else(|| {
            anyhow!(
                "tool '{}' is not installed (no bundle in {})",
                step.tool,
                root.display()
            )
        })?;

    let approved = step_is_approved(board_id, step_id);
    let scope = RunScope {
        tenant_id: tenant,
        run_id: step_id.to_string(),
    };
    let ledger = RunCostLedger::new();

    // Spawn the plugin process LAZILY: a gated tool is never spawned (the closure
    // only runs when `dispatch_mcp_tool` decides to execute).
    let bundle_dir = root.join(&step.plugin_id);
    let plugin_id = step.plugin_id.clone();
    let connect = move || -> Result<Box<dyn cyan_mcp::PluginTransport>> {
        let mut transport = cyan_mcp::StdioTransport::new();
        let config = cyan_mcp::SpawnConfig {
            plugin_id: plugin_id.clone(),
            command: bundle_dir.join("run").to_string_lossy().to_string(),
            args: vec![],
            creds: vec![],
        };
        transport
            .spawn(&config)
            .map_err(|e| anyhow!("spawn plugin {}: {}", plugin_id, e))?;
        Ok(Box::new(transport))
    };

    match host
        .dispatch_mcp_tool(&scope, &step, &side_effects, approved, &ledger, connect)
        .map_err(|e| anyhow!("local plugin dispatch failed: {}", e))?
    {
        McpDispatch::Ran(result) => {
            let summary = serde_json::to_string(&result.result)
                .unwrap_or_else(|_| result.result.to_string());

            // The plugin's JSON result becomes a finding so the step produces a
            // reviewable result (and a timecoded note, like every other step).
            let finding = Finding {
                timecode_seconds: 0.0,
                content: summary.clone(),
                finding_type: "plugin_result".to_string(),
                severity: "info".to_string(),
                suggested_action: None,
            };
            let note = crate::timecode_notes::TimecodeNote {
                id: uuid::Uuid::new_v4().to_string(),
                board_id: board_id.to_string(),
                timecode_seconds: finding.timecode_seconds,
                content: finding.content.clone(),
                note_type: finding.finding_type.clone(),
                author: format!("plugin/{}", step.plugin_id),
                created_at: chrono::Utc::now().timestamp() as f64,
                pipeline_step_id: Some(step_id.to_string()),
                pipeline_phase: Some("during".to_string()),
                ai_reviewed: true,
                human_approved: false,
                action_skill: None,
                action_status: Some("complete".to_string()),
                action_result: None,
                action_model: Some(format!("{}.{}", step.plugin_id, step.tool)),
                ai_flags_nearby: vec![],
                reply_to: None,
                thread_count: 0,
            };
            let _ = crate::timecode_notes::save_note(&note, command_tx);

            let _ = event_tx.send(SwiftEvent::StatusUpdate {
                message: format!("✅ Step '{}' plugin complete", step_id),
            });

            // Dashboard exec events: step completed + the finding it produced.
            // `cost_usd` rides on the completion event so the cost rail can
            // attribute the external (plugin) bill to this run/plugin.
            publish_pipeline_event(event_tx, PipelineEvent {
                event_type: "step_completed".into(),
                board_id: board_id.into(),
                step_id: step_id.into(),
                run_id: run_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
                data: json!({
                    "summary": summary,
                    "findings_count": 1,
                    "plugin_id": step.plugin_id,
                    "tool": step.tool,
                    "duration_ms": result.duration_ms,
                    "cost_usd": result.cost_usd,
                    "source": "external",
                }),
            });
            publish_pipeline_event(event_tx, PipelineEvent {
                event_type: "finding_created".into(),
                board_id: board_id.into(),
                step_id: step_id.into(),
                run_id: run_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
                data: json!({
                    "timecode_seconds": finding.timecode_seconds,
                    "content": finding.content,
                    "finding_type": finding.finding_type,
                    "severity": finding.severity,
                }),
            });

            Ok((summary, vec![finding]))
        }
        McpDispatch::Gated { side_effects } => {
            // Reuse the human-approval path: surface needs_human so the user can
            // approve the side-effecting call; a re-run then flips `approved`.
            let effects = side_effects.join(", ");
            let _ = event_tx.send(SwiftEvent::StatusUpdate {
                message: format!("⏸️ Step '{}' needs approval (side effects: {})", step_id, effects),
            });
            publish_pipeline_event(event_tx, PipelineEvent {
                event_type: "step_needs_human".into(),
                board_id: board_id.into(),
                step_id: step_id.into(),
                run_id: run_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
                data: json!({ "side_effects": side_effects }),
            });
            Err(anyhow!(
                "needs_human: plugin tool '{}' requires approval (side effects: {})",
                step.tool,
                effects
            ))
        }
    }
}

// ============================================================================
// Helpers (imported from pipeline.rs)
// ============================================================================

/// Resolve the board's video asset to a URI the cyan-media probe (ffprobe) can open.
///
/// Fix B: the seeded steps reference the clip by BARE FILENAME (e.g. "sintel-clip.mp4"),
/// not an http URL, so the old `starts_with("http")`-only filter returned `None` and the
/// local cyan-media probe threw "No video URI provided". This now resolves, in priority:
///   1. an explicit http/https/s3/file URL in ANY step cell (the old path, widened);
///   2. a bound asset whose `local_path` points at a real file on disk → probe it directly;
///   3. a bare media filename (from the bound asset, else mentioned in a cell) joined to the
///      configured media root `CYAN_MEDIA_ROOT` (a local dir OR an http base).
///
/// Returns `None` only when nothing resolvable is found AND no media root is configured —
/// the caller then surfaces the same clear error instead of probing garbage.
fn find_video_uri(board_id: &str) -> Option<String> {
    // Read everything we need under ONE lock, then release before resolving (the storage
    // helpers below lock independently — never hold the lock across them).
    let (cell_texts, bound_files): (Vec<String>, Vec<(String, Option<String>)>) = {
        let conn = crate::storage::db().lock().ok()?;
        let cells = {
            let mut stmt = conn.prepare(
                "SELECT content FROM notebook_cells WHERE board_id = ?1 AND cell_type = 'markdown' ORDER BY cell_order"
            ).ok()?;
            let rows = stmt
                .query_map(rusqlite::params![board_id], |row| row.get::<_, Option<String>>(0))
                .ok()?;
            rows.filter_map(|r| r.ok()).flatten().collect::<Vec<_>>()
        };
        let files = {
            let mut stmt = conn.prepare(
                "SELECT name, local_path FROM objects WHERE type='file' AND board_id = ?1 AND COALESCE(deleted,0)=0 ORDER BY created_at"
            ).ok()?;
            let rows = stmt
                .query_map(rusqlite::params![board_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })
                .ok()?;
            rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
        };
        (cells, files)
    };

    // (1) explicit URL in any cell.
    for c in &cell_texts {
        if let Some(u) = c.split_whitespace().find(|w| {
            (w.starts_with("http://") || w.starts_with("https://") || w.starts_with("s3://") || w.starts_with("file://"))
                && is_media_filename(w.trim_end_matches(['.', ',', ';', ':', ')', '"', '\'']))
        }) {
            return Some(u.trim_end_matches(['.', ',', ';', ':', ')', '"', '\'']).to_string());
        }
    }

    // (2) a bound asset with a real local file → probe it directly.
    for (_name, local_path) in &bound_files {
        if let Some(p) = local_path
            && !p.is_empty() && std::path::Path::new(p).exists() {
                return Some(p.clone());
            }
    }

    // (3) a bare media filename — bound asset first, else mentioned in a cell — joined to
    //     the configured media root.
    let candidate = bound_files
        .iter()
        .map(|(n, _)| n.clone())
        .find(|n| is_media_filename(n))
        .or_else(|| cell_texts.iter().find_map(|c| extract_media_filename(c)));
    candidate.and_then(|name| resolve_media_uri(&name))
}

/// The arg keys a cyan-media tool accepts as its input clip. `src` is the canonical one
/// (matches the plugin + the `mcp_tool_test` fixture); the rest are tolerated aliases.
const MEDIA_INPUT_KEYS: &[&str] = &["src", "input", "uri", "path", "input_uri", "source_url"];

/// BURST C4: ensure a `cyan-media` step's args carry a concrete, resolvable input path — the
/// SAME container path `find_video_uri` yields for the board — so every consumer (not just
/// ingest) resolves identically instead of failing `path_denied` / `input_bytes:0`.
///
/// No-op unless `step.plugin_id == "cyan-media"`. If the author already supplied any of
/// `MEDIA_INPUT_KEYS` with a non-empty value, we leave it (explicit author intent wins). Only
/// injects when we can resolve a URI; if resolution fails we leave args untouched so the
/// plugin surfaces its own clear "no input" error rather than us fabricating a bad path.
fn resolve_media_args(board_id: &str, step: &mut McpTool) {
    if step.plugin_id != "cyan-media" {
        return;
    }
    // Already has an input path? Respect it.
    if let Some(obj) = step.args.as_object() {
        let has_input = MEDIA_INPUT_KEYS.iter().any(|k| {
            obj.get(*k)
                .and_then(|v| v.as_str())
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
        });
        if has_input {
            return;
        }
    }
    let Some(uri) = find_video_uri(board_id) else {
        return;
    };
    // Normalize to an object we can extend (a Null/array/scalar args becomes `{}`).
    if !step.args.is_object() {
        step.args = json!({});
    }
    if let Some(obj) = step.args.as_object_mut() {
        // `src` is the canonical key; mirror onto `input`/`uri` so tools keyed differently
        // still find it (extra keys are harmless — the plugin reads the one it wants).
        obj.insert("src".to_string(), json!(uri));
        obj.insert("input".to_string(), json!(uri));
        obj.insert("uri".to_string(), json!(uri));
    }
}

/// True if `s` ends with a recognized video container extension (case-insensitive).
fn is_media_filename(s: &str) -> bool {
    let l = s.to_ascii_lowercase();
    [".mp4", ".mov", ".mxf", ".mkv", ".webm", ".m4v"].iter().any(|e| l.ends_with(e))
}

/// Pull the first bare media filename out of free-text cell content (e.g. the seed line
/// "the local file sintel-clip.mp4 (in the media root)." → "sintel-clip.mp4").
fn extract_media_filename(content: &str) -> Option<String> {
    content
        .split(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | ',' | '"' | '\''))
        .map(|tok| tok.trim_end_matches(['.', ',', ';', ':', ')', '*', '`']))
        .find(|tok| is_media_filename(tok))
        .map(|s| s.to_string())
}

/// Turn a bare clip filename into a probeable URI. Pass-through for things that already are
/// a URI or an existing absolute path; otherwise join to `CYAN_MEDIA_ROOT` (the same env the
/// cyan-media plugin confines paths to). Returns `None` when it's a bare name and no media
/// root is configured — there is nothing safe to hand ffprobe.
fn resolve_media_uri(name: &str) -> Option<String> {
    if name.starts_with("http://") || name.starts_with("https://") || name.starts_with("s3://") || name.starts_with("file://") {
        return Some(name.to_string());
    }
    if name.starts_with('/') && std::path::Path::new(name).exists() {
        return Some(name.to_string());
    }
    let root = std::env::var("CYAN_MEDIA_ROOT").ok().filter(|r| !r.trim().is_empty())?;
    let root = root.trim_end_matches('/');
    // Works for both a local dir ("/opt/cyan/media") and an http base ("http://host/media").
    Some(format!("{root}/{name}"))
}

/// The board's bound primary clip filename: the seed inserts one file row named after the
/// clip; fall back to a bare media filename mentioned in any step cell.
fn board_bound_asset(board_id: &str) -> Option<String> {
    if let Ok(files) = crate::storage::file_list_by_board(board_id)
        && let Some(f) = files.into_iter().find(|f| is_media_filename(&f.name)) {
            return Some(f.name);
        }
    let conn = crate::storage::db().lock().ok()?;
    let mut stmt = conn
        .prepare("SELECT content FROM notebook_cells WHERE board_id = ?1 AND cell_type='markdown' ORDER BY cell_order")
        .ok()?;
    let rows = stmt
        .query_map(rusqlite::params![board_id], |row| row.get::<_, Option<String>>(0))
        .ok()?;
    rows.filter_map(|r| r.ok()).flatten().find_map(|c| extract_media_filename(&c))
}

fn find_scope_id(board_id: &str) -> Option<String> {
    let conn = crate::storage::db().lock().ok()?;
    let mut stmt = conn.prepare(
        "SELECT workspace_id FROM objects WHERE id = ?1 LIMIT 1"
    ).ok()?;
    
    stmt.query_row(rusqlite::params![board_id], |row| row.get::<_, String>(0)).ok()
}

/// Fix B: the board's REAL bound-asset metadata. Was a hardcoded "Tears of Steel" stub that
/// labeled every board's run with the same wrong asset (and a bogus source_url). Now derives
/// `title` from the board name, `asset` from the bound clip filename, and `source_url` from
/// the resolved probe URI when a media root is configured. None of the old localization
/// fields (target_languages/markets/cpm/…) were read by the engine or the lens, so dropping
/// them is safe; add real per-asset fields here when a MAM lookup exists.
pub fn find_asset_metadata(board_id: &str) -> Option<serde_json::Value> {
    let asset = board_bound_asset(board_id);

    // Board name (objects.type='whiteboard'); fall back to the asset filename, then the id.
    let board_name: Option<String> = {
        crate::storage::db().lock().ok().and_then(|conn| {
            conn.query_row(
                "SELECT name FROM objects WHERE id = ?1 AND type = 'whiteboard' LIMIT 1",
                rusqlite::params![board_id],
                |row| row.get::<_, String>(0),
            )
            .ok()
        })
    };
    let title = board_name
        .or_else(|| asset.clone())
        .unwrap_or_else(|| board_id.to_string());

    let mut m = json!({ "title": title });
    if let Some(a) = asset {
        m["asset"] = json!(a);
        if let Some(uri) = resolve_media_uri(&a) {
            m["source_url"] = json!(uri);
        }
    }
    Some(m)
}

// ============================================================================
// DEMO CACHE — Remove this section when productionizing
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct CachedStepResult {
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<Finding>,
    #[serde(default)]
    pub status_markers: Vec<CachedStatusMarker>,
    #[serde(default = "default_final_delay")]
    pub final_delay_ms: u64,
    #[serde(default = "default_simulated_duration")]
    pub simulated_duration: f64,
    #[serde(default)]
    pub model_used: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CachedStatusMarker {
    pub icon: String,
    pub message: String,
    #[serde(default = "default_marker_delay")]
    pub delay_ms: u64,
}

fn default_final_delay() -> u64 { 1500 }
fn default_simulated_duration() -> f64 { 29.6 }
fn default_marker_delay() -> u64 { 800 }

/// Load cached step result from ~/.cyan/pipeline_cache/{step_id}.json
fn load_cached_step_result(step_id: &str) -> Option<CachedStepResult> {
    let home = std::env::var("HOME").unwrap_or_else(|e| {
        eprintln!("📺 CACHE DEBUG: HOME env not set: {}", e);
        String::new()
    });
    let cache_path = format!("{}/Documents/pipeline_cache/{}.json", home, step_id);
    eprintln!("📺 CACHE DEBUG: Looking for cache at: {}", cache_path);
    let data = match std::fs::read_to_string(&cache_path) {
        Ok(d) => { eprintln!("📺 CACHE DEBUG: File read OK, {} bytes", d.len()); d },
        Err(e) => { eprintln!("📺 CACHE DEBUG: File read FAILED: {}", e); return None; },
    };
    let cached: CachedStepResult = match serde_json::from_str(&data) {
        Ok(c) => { eprintln!("📺 CACHE DEBUG: JSON parse OK"); c },
        Err(e) => { eprintln!("📺 CACHE DEBUG: JSON parse FAILED: {}", e); return None; },
    };
    Some(cached)
}

#[cfg(test)]
mod video_uri_tests {
    use super::{extract_media_filename, is_media_filename, resolve_media_uri};

    #[test]
    fn detects_media_extensions() {
        assert!(is_media_filename("sintel-clip.mp4"));
        assert!(is_media_filename("A.MOV"));
        assert!(is_media_filename("bars-smpte-720p-15s.mkv"));
        assert!(!is_media_filename("notes.txt"));
        assert!(!is_media_filename("sintel-clip"));
    }

    #[test]
    fn pulls_bare_filename_from_seed_cell_text() {
        let cell = "Ingest the broadcast master: the local file sintel-clip.mp4 (in the media root).";
        assert_eq!(extract_media_filename(cell).as_deref(), Some("sintel-clip.mp4"));
        // trailing sentence punctuation must not eat the extension
        let cell2 = "run probe on tears-of-steel-clip.mov.";
        assert_eq!(extract_media_filename(cell2).as_deref(), Some("tears-of-steel-clip.mov"));
        assert_eq!(extract_media_filename("no clip here, just words.").as_deref(), None);
    }

    #[test]
    fn resolves_bare_name_against_media_root_and_passes_urls_through() {
        // URL pass-through regardless of env
        assert_eq!(
            resolve_media_uri("https://x/y.mp4").as_deref(),
            Some("https://x/y.mp4")
        );
        // bare name joins CYAN_MEDIA_ROOT (local dir or http base), trailing slash trimmed
        unsafe { std::env::set_var("CYAN_MEDIA_ROOT", "/opt/cyan/media/"); }
        assert_eq!(
            resolve_media_uri("sintel-clip.mp4").as_deref(),
            Some("/opt/cyan/media/sintel-clip.mp4")
        );
        unsafe { std::env::set_var("CYAN_MEDIA_ROOT", "http://lens:9080/media"); }
        assert_eq!(
            resolve_media_uri("sintel-clip.mp4").as_deref(),
            Some("http://lens:9080/media/sintel-clip.mp4")
        );
        // no media root + bare name → None (caller surfaces a clear error, not garbage)
        unsafe { std::env::remove_var("CYAN_MEDIA_ROOT"); }
        assert_eq!(resolve_media_uri("sintel-clip.mp4"), None);
    }
}

#[cfg(test)]
mod media_args_tests {
    use super::{resolve_media_args, McpTool};
    use serde_json::json;

    // BURST C4: a non-cyan-media step is never touched (no DB access, args unchanged).
    #[test]
    fn non_cyan_media_step_is_untouched() {
        let mut step = McpTool {
            plugin_id: "frameio".into(),
            tool: "upload".into(),
            args: json!({ "folder_id": "abc" }),
        };
        resolve_media_args("board-x", &mut step);
        assert_eq!(step.args, json!({ "folder_id": "abc" }));
    }

    // An author-supplied input path wins — we must not overwrite explicit intent (and the
    // early return means no DB lookup, so this is DB-free).
    #[test]
    fn author_supplied_src_is_respected() {
        let mut step = McpTool {
            plugin_id: "cyan-media".into(),
            tool: "proxy".into(),
            args: json!({ "src": "s3://bucket/master.mov", "profile": "h264" }),
        };
        resolve_media_args("board-x", &mut step);
        assert_eq!(step.args["src"], json!("s3://bucket/master.mov"));
        assert_eq!(step.args["profile"], json!("h264"));
    }

    // Any of the accepted input aliases (here `input`) counts as author-supplied.
    #[test]
    fn author_supplied_input_alias_is_respected() {
        let mut step = McpTool {
            plugin_id: "cyan-media".into(),
            tool: "transcode".into(),
            args: json!({ "input": "/opt/cyan/media/clip.mp4" }),
        };
        resolve_media_args("board-x", &mut step);
        assert_eq!(step.args["input"], json!("/opt/cyan/media/clip.mp4"));
        // No `src` was fabricated over the existing `input`.
        assert!(step.args.get("src").is_none());
    }
}
