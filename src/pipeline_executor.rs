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

    // ── LOCAL MCP PLUGIN TOOL: run on-device, NO cloud round-trip ───────
    // A `local` step whose metadata names a plugin tool (`{ "mcp_tool": {
    // plugin_id, tool, args } }`) is dispatched through the supervised cyan-mcp
    // host on this device — the local mirror of the lens cloud `McpTool` path.
    // Guarded: ordinary steps (no `mcp_tool`) fall straight through unchanged.
    if executor_type == "local"
        && let Some(step) = metadata.as_ref().and_then(parse_mcp_tool_step)
    {
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
    step: McpTool,
    command_tx: &UnboundedSender<CommandMsg>,
    event_tx: &UnboundedSender<SwiftEvent>,
) -> Result<(String, Vec<Finding>)> {
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

fn find_video_uri(board_id: &str) -> Option<String> {
    let conn = crate::storage::db().lock().ok()?;
    let mut stmt = conn.prepare(
        "SELECT content FROM notebook_cells WHERE board_id = ?1 AND cell_type = 'markdown' ORDER BY cell_order LIMIT 1"
    ).ok()?;
    
    let content: Option<String> = stmt.query_row(rusqlite::params![board_id], |row| row.get(0)).ok();
    
    content.filter(|c| c.starts_with("http") && (c.contains(".mp4") || c.contains(".mov") || c.contains(".mxf")))
}

fn find_scope_id(board_id: &str) -> Option<String> {
    let conn = crate::storage::db().lock().ok()?;
    let mut stmt = conn.prepare(
        "SELECT workspace_id FROM objects WHERE id = ?1 LIMIT 1"
    ).ok()?;
    
    stmt.query_row(rusqlite::params![board_id], |row| row.get::<_, String>(0)).ok()
}

pub fn find_asset_metadata(_board_id: &str) -> Option<serde_json::Value> {
    // For demo: return BigBuckBunny metadata
    // In production: read from first cell or MAM API
    Some(json!({
        "title": "Tears of Steel",
        "source_url": "https://download.blender.org/demo/movies/ToS/tears_of_steel_720p.mov",
        "content_type": "sci_fi_drama",
        "genre": ["sci-fi", "drama", "action"],
        "source_language": "en",
        "target_languages": ["hi", "ta", "te"],
        "target_markets": ["IN", "SG", "AE"],
        "resolution": "HD",
        "duration_seconds": 734.0,
        "rating": "M18",
        "ad_tier": "premium",
        "historical_cpm": 300.0,
        "engagement_curve": [0.95, 0.88, 0.82, 0.85, 0.88, 0.91, 0.89, 0.82, 0.78, 0.75],
        "delivery_platforms": [
            {"platform": "JioStar", "format": "HEVC_1080p"},
            {"platform": "YouTube India", "format": "H264_HD"},
            {"platform": "Hotstar", "format": "ABR_ladder"}
        ]
    }))
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
