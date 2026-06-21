// src/ai_bridge.rs
//
// Bridges AI capabilities into Cyan's FFI layer.
//
// CLOUD INTEGRATION:
// - LensSearch, AskAnalyst, FeedEvent → Cyan Lens Cloud API
// - Playbook ops → Cloud Postgres storage
// - ImageToMermaid → Claude API (user's own key, private)
//
// LOCAL:
// - RegisterModel/InferModel → Local P2P model registry experiment
//
// HARDWIRED CLOUD URL - change this for deployment:
#![allow(dead_code)] // AI/Lens enrichment scaffolding (analysis/inference/feedback types) for the in-progress MCP/workflow migration; see CLAUDE.md 'Out of scope'. Some types/fields are not yet wired up but are kept for shape parity.

const CYAN_LENS_CLOUD_URL: &str = "http://localhost:8080";

use crate::cyan_lens_client::{CyanLensClient, CyanLensConfig};
use crate::SwiftEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use rusqlite::Connection;

// ============================================================================
// Public Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AIIntegrationEvent {
    pub anchor_id: String,
    pub anchor_kind: String,
    pub anchor_ref: String,
    pub mention_count: u32,
    pub last_mention: i64,
    pub context: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProactiveInsight {
    pub id: String,
    pub message: String,
    pub severity: String,
    pub anchor_ids: Vec<String>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MermaidResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mermaid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citations: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSummary {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub capabilities: Vec<String>,
    pub board_id: String,
    pub cell_id: String,
    pub file_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timing_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BulletFeedbackInput {
    bullet_id: String,
    tag: String,
}

#[derive(Debug, Clone, Deserialize)]
struct LensCorrectionInput {
    wrong_sql: Option<String>,
    correct_sql: Option<String>,
    explanation: String,
}

// ============================================================================
// Claude API Types (for image→mermaid)
// ============================================================================

#[derive(Debug, Serialize)]
struct ClaudeRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<ClaudeMessage>,
}

#[derive(Debug, Serialize)]
struct ClaudeMessage {
    role: String,
    content: Vec<ClaudeContent>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClaudeContent {
    Text { text: String },
    Image { source: ClaudeImageSource },
}

#[derive(Debug, Serialize)]
struct ClaudeImageSource {
    #[serde(rename = "type")]
    source_type: String,
    media_type: String,
    data: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeResponse {
    content: Vec<ClaudeResponseContent>,
}

#[derive(Debug, Deserialize)]
struct ClaudeResponseContent {
    #[serde(rename = "type")]
    content_type: String,
    text: Option<String>,
}

// ============================================================================
// Command/Response
// ============================================================================

#[derive(Debug, Deserialize)]
struct AICommandWrapper {
    #[serde(default)]
    cmd_id: Option<String>,
    #[serde(flatten)]
    command: AICommand,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum AICommand {
    Initialize { models_dir: String },
    SetClaudeApiKey { api_key: String },
    ImageToMermaid { image_base64: String },
    AskAnalyst { question: String },
    FeedEvent { event: AIIntegrationEvent },
    SetProactive { enabled: bool },
    // LOCAL: P2P Model Registry
    RegisterModel { cell_id: String, board_id: String, model_path: String, model_kind: String },
    RegisterModelV2 { cell_id: String, board_id: String, file_id: String, skill_md: String },
    ImportModel { cell_id: String, board_id: String, file_path: String, model_kind: String },
    UnloadModel { cell_id: String },
    InferModel { cell_id: String, input: serde_json::Value },
    ListModels { group_id: String },
    GetCellModel { cell_id: String, board_id: String },
    // CLOUD: Lens
    LensSearch { query: String },
    LensSearchWithContext {
        query: String,
        current_board_id: Option<String>,
        current_workspace_id: Option<String>,
    },
    LensFeedback {
        request_id: String,
        was_helpful: bool,
        #[serde(default)]
        bullet_feedback: Vec<BulletFeedbackInput>,
        correction: Option<LensCorrectionInput>,
    },
    // CLOUD: Nudges, Asks, Decisions
    GetNudges { group_id: String },
    GetAsks { group_id: String, limit: Option<i32> },
    GetDecisions { group_id: String, limit: Option<i32> },
    Summarize { query_type: String, topic: Option<String>, hours: Option<u64> },
    // CANVAS: Diagram Generation via Claude API
    GenerateDiagram {
        source_type: String,
        prompt: Option<String>,
        github_url: Option<String>,
        image_base64: Option<String>,
        current_mermaid: Option<String>,
        diagram_type: String,
        board_id: String,
    },
}

#[derive(Debug, Serialize)]
struct CommandResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    cmd_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

impl CommandResponse {
    fn ok() -> Self { Self { success: true, cmd_id: None, error: None, data: None } }
    fn ok_with_data(data: serde_json::Value) -> Self { Self { success: true, cmd_id: None, error: None, data: Some(data) } }
    fn err(msg: impl Into<String>) -> Self { Self { success: false, cmd_id: None, error: Some(msg.into()), data: None } }
    fn with_cmd_id(mut self, cmd_id: Option<String>) -> Self { self.cmd_id = cmd_id; self }
}

// ============================================================================
// AI Bridge
// ============================================================================

pub struct AIBridge {
    db: Arc<Mutex<Connection>>,
    event_tx: tokio::sync::mpsc::UnboundedSender<SwiftEvent>,
    // Claude API for image→mermaid (user's own key)
    claude_api_key: RwLock<Option<String>>,
    claude_client: reqwest::Client,
    // LOCAL: Cell models for P2P registry
    cell_models: RwLock<HashMap<String, CellModel>>,
    // Cloud client for Lens
    cloud_client: RwLock<Option<CyanLensClient>>,
    // State
    initialized: RwLock<bool>,
    // Config
    group_id: RwLock<String>,
    workspace_id: RwLock<Option<String>>,
    // Proactive insights
    insight_queue: RwLock<std::collections::VecDeque<ProactiveInsight>>,
    proactive_enabled: RwLock<bool>,
}

struct CellModel {
    cell_id: String,
    board_id: String,
    model_id: String,
    model_name: String,
    model_kind: String,
    file_id: Option<String>,
    capabilities: Vec<String>,
    skill_md: Option<String>,
    model_path: PathBuf,
}

impl AIBridge {
    pub fn new(db: Arc<Mutex<Connection>>, event_tx: tokio::sync::mpsc::UnboundedSender<SwiftEvent>) -> Self {
        let claude_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            db,
            event_tx,
            claude_api_key: RwLock::new(
                std::env::var("ANTHROPIC_API_KEY").ok().or_else(|| {
                    let home = std::env::var("HOME").ok()?;
                    let env_str = std::fs::read_to_string(format!("{}/Documents/.env", home)).ok()?;
                    env_str.lines()
                        .find(|l| l.starts_with("ANTHROPIC_API_KEY="))
                        .map(|l| l.trim_start_matches("ANTHROPIC_API_KEY=").trim().to_string())
                })
            ),
            claude_client,
            cell_models: RwLock::new(HashMap::new()),
            cloud_client: RwLock::new(None),
            initialized: RwLock::new(false),
            group_id: RwLock::new("default".to_string()),
            workspace_id: RwLock::new(None),
            insight_queue: RwLock::new(std::collections::VecDeque::new()),
            proactive_enabled: RwLock::new(false),
        }
    }

    /// Set group/workspace context for cloud queries
    pub async fn set_context(&self, group_id: String, workspace_id: Option<String>) {
        *self.group_id.write().await = group_id.clone();
        *self.workspace_id.write().await = workspace_id.clone();
        
        if let Some(client) = self.cloud_client.write().await.as_mut() {
            client.set_group_id(group_id);
            if let Some(wid) = workspace_id {
                client.set_workspace_id(Some(wid));
            }
        }
    }

    /// Legacy method for compatibility - no longer uses local DB
    /// Kept for backward compatibility with lib.rs and ffi/core.rs
    pub async fn set_cyan_db_path(&self, _path: PathBuf) {
        // No-op: Cloud integration doesn't use local cyan.db
        // The path was used for local playbook storage, now handled by cloud
        tracing::info!("set_cyan_db_path called (no-op in cloud mode)");
    }

    pub fn start_insight_generator(self: &Arc<Self>) {
        let bridge = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if !*bridge.proactive_enabled.read().await || !*bridge.initialized.read().await {
                    continue;
                }
                if let Some(insight) = bridge.generate_cloud_insight().await {
                    bridge.insight_queue.write().await.push_back(insight.clone());
                    let _ = bridge.event_tx.send(SwiftEvent::AIInsight {
                        insight_json: serde_json::to_string(&insight).unwrap_or_default(),
                    });
                }
            }
        });
    }

    async fn generate_cloud_insight(&self) -> Option<ProactiveInsight> {
        let client = self.cloud_client.read().await;
        let client = client.as_ref()?;
        
        match client.get_nudges().await {
            Ok(report) => {
                if report.nudges.is_empty() { return None; }
                let nudge = &report.nudges[0];
                Some(ProactiveInsight {
                    id: uuid::Uuid::new_v4().to_string(),
                    message: format!("{}: {}", nudge.nudge_type, 
                        nudge.question.as_deref().or(nudge.decision.as_deref()).unwrap_or("")),
                    severity: match nudge.nudge_type.as_str() {
                        "stale_blocker" => "high",
                        "stale_ask" => "medium",
                        _ => "low",
                    }.to_string(),
                    anchor_ids: nudge.external_id.clone().map(|id| vec![id]).unwrap_or_default(),
                    timestamp: chrono::Utc::now().timestamp(),
                })
            }
            Err(_) => None,
        }
    }

    // ========================================================================
    // Command Handler
    // ========================================================================

    pub async fn handle_command(&self, json: &str) -> String {
        tracing::debug!("🔍 AIBridge command: {}", &json[..json.len().min(200)]);
        let (cmd_id, response) = match serde_json::from_str::<AICommandWrapper>(json) {
            Ok(wrapper) => (wrapper.cmd_id, self.dispatch(wrapper.command).await),
            Err(e) => (None, CommandResponse::err(format!("Invalid JSON: {}", e))),
        };
        serde_json::to_string(&response.with_cmd_id(cmd_id)).unwrap_or_else(|_| r#"{"success":false}"#.to_string())
    }

    pub async fn poll_insights(&self) -> Option<ProactiveInsight> {
        self.insight_queue.write().await.pop_front()
    }

    async fn dispatch(&self, cmd: AICommand) -> CommandResponse {
        match cmd {
            AICommand::Initialize { models_dir } => self.cmd_initialize(&models_dir).await,
            AICommand::SetClaudeApiKey { api_key } => self.cmd_set_claude_api_key(&api_key).await,
            
            // CLAUDE API: Image to Mermaid (user's own key, private)
            AICommand::ImageToMermaid { image_base64 } => self.cmd_image_to_mermaid_claude(&image_base64).await,
            
            // CLOUD: Query/Analysis via Cyan Lens
            AICommand::AskAnalyst { question } => self.cmd_ask_analyst_cloud(&question).await,
            AICommand::LensSearch { query } => self.cmd_lens_search_cloud(&query).await,
            AICommand::LensSearchWithContext { query, .. } => self.cmd_lens_search_cloud(&query).await,
            AICommand::FeedEvent { event } => self.cmd_feed_event_cloud(event).await,
            AICommand::LensFeedback { request_id, was_helpful, .. } =>
                self.cmd_lens_feedback_cloud(&request_id, was_helpful).await,
            AICommand::GetNudges { group_id } => self.cmd_get_nudges(&group_id).await,
            AICommand::GetAsks { group_id, limit } => self.cmd_get_asks(&group_id, limit.unwrap_or(20)).await,
            AICommand::GetDecisions { group_id, limit } => self.cmd_get_decisions(&group_id, limit.unwrap_or(20)).await,
            AICommand::Summarize { query_type, topic, hours } => 
                self.cmd_summarize_cloud(&query_type, topic, hours).await,
            
            // LOCAL: Proactive
            AICommand::SetProactive { enabled } => self.cmd_set_proactive(enabled).await,
            
            // LOCAL: P2P Model Registry
            AICommand::RegisterModel { cell_id, board_id, model_path, model_kind } =>
                self.cmd_register_model(&cell_id, &board_id, &model_path, &model_kind).await,
            AICommand::RegisterModelV2 { cell_id, board_id, file_id, skill_md } =>
                self.cmd_register_model_v2(&cell_id, &board_id, &file_id, &skill_md).await,
            AICommand::ImportModel { cell_id, board_id, file_path, model_kind } =>
                self.cmd_import_model(&cell_id, &board_id, &file_path, &model_kind).await,
            AICommand::UnloadModel { cell_id } => self.cmd_unload_model(&cell_id).await,
            AICommand::InferModel { cell_id, input } => self.cmd_infer_model(&cell_id, input).await,
            AICommand::ListModels { group_id } => self.cmd_list_models(&group_id).await,
            AICommand::GetCellModel { cell_id, board_id } => self.cmd_get_cell_model(&cell_id, &board_id).await,
            
            // CANVAS: Diagram Generation
            AICommand::GenerateDiagram { source_type, prompt, github_url, image_base64, current_mermaid, diagram_type, board_id } =>
                self.cmd_generate_diagram(source_type, prompt, github_url, image_base64, current_mermaid, diagram_type, board_id).await,
        }
    }

    // ========================================================================
    // Initialize
    // ========================================================================

    async fn cmd_initialize(&self, _models_dir: &str) -> CommandResponse {
        tracing::info!("🔍 cmd_initialize");

        // Initialize Cyan Lens cloud client
        let config = CyanLensConfig {
            base_url: CYAN_LENS_CLOUD_URL.to_string(),
            group_id: self.group_id.read().await.clone(),
            workspace_id: self.workspace_id.read().await.clone(),
            timeout_secs: 30,
        };
        let client = CyanLensClient::new(config);
        
        let cloud_available = client.is_available().await;
        if cloud_available {
            tracing::info!("✅ Connected to Cyan Lens Cloud at {}", CYAN_LENS_CLOUD_URL);
        } else {
            tracing::warn!("⚠️ Cyan Lens Cloud not available at {}", CYAN_LENS_CLOUD_URL);
        }
        *self.cloud_client.write().await = Some(client);

        *self.initialized.write().await = true;
        
        let has_claude_key = self.claude_api_key.read().await.is_some();
        
        tracing::info!("✅ AI Bridge initialized");
        CommandResponse::ok_with_data(serde_json::json!({
            "cloud_url": CYAN_LENS_CLOUD_URL,
            "cloud_available": cloud_available,
            "claude_api_configured": has_claude_key,
        }))
    }

    // ========================================================================
    // Claude API Key (user's own key for image→mermaid)
    // ========================================================================

    async fn cmd_set_claude_api_key(&self, api_key: &str) -> CommandResponse {
        if api_key.is_empty() {
            *self.claude_api_key.write().await = None;
            return CommandResponse::ok_with_data(serde_json::json!({ "configured": false }));
        }
        
        // Validate key format
        if !api_key.starts_with("sk-ant-") {
            return CommandResponse::err("Invalid Claude API key format");
        }
        
        *self.claude_api_key.write().await = Some(api_key.to_string());
        tracing::info!("✅ Claude API key configured");
        CommandResponse::ok_with_data(serde_json::json!({ "configured": true }))
    }

    // ========================================================================
    // CLAUDE API: Image → Mermaid (user's own key, private)
    // ========================================================================

    async fn cmd_image_to_mermaid_claude(&self, image_base64: &str) -> CommandResponse {
        let api_key = self.claude_api_key.read().await;
        let api_key = match api_key.as_ref() {
            Some(k) => k.clone(),
            None => return CommandResponse::err("Claude API key not configured. Set your API key in Settings."),
        };

        // Detect media type from base64 header (JPEG magic), else default to png
        // (covers the PNG "iVBOR" header and anything else).
        let media_type = if image_base64.starts_with("/9j/") {
            "image/jpeg"
        } else {
            "image/png"
        };

        let request = ClaudeRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            max_tokens: 4096,
            messages: vec![ClaudeMessage {
                role: "user".to_string(),
                content: vec![
                    ClaudeContent::Image {
                        source: ClaudeImageSource {
                            source_type: "base64".to_string(),
                            media_type: media_type.to_string(),
                            data: image_base64.to_string(),
                        },
                    },
                    ClaudeContent::Text {
                        text: r#"Analyze this whiteboard/diagram image and convert it to Mermaid diagram syntax.

Instructions:
1. Identify all shapes (boxes, circles, diamonds, etc.) and their text content
2. Identify all connections/arrows between shapes
3. Determine the best Mermaid diagram type (flowchart, sequence, etc.)
4. Output ONLY the Mermaid code, no explanation

If the image is not a diagram or cannot be converted, respond with:
```mermaid
flowchart TD
    A[Could not parse diagram]
```

Output the Mermaid code in a ```mermaid code block."#.to_string(),
                    },
                ],
            }],
        };

        let response = self.claude_client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await;

        match response {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let status = resp.status();
                    let error_text = resp.text().await.unwrap_or_default();
                    return CommandResponse::ok_with_data(serde_json::to_value(MermaidResult {
                        success: false,
                        mermaid: None,
                        error: Some(format!("Claude API error {}: {}", status, error_text)),
                    }).unwrap_or_default());
                }

                match resp.json::<ClaudeResponse>().await {
                    Ok(claude_resp) => {
                        // Extract mermaid from response
                        let text = claude_resp.content.iter()
                            .filter_map(|c| c.text.as_ref())
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join("\n");

                        // Extract mermaid code block
                        let mermaid = if let Some(start) = text.find("```mermaid") {
                            let start = start + 10;
                            if let Some(end) = text[start..].find("```") {
                                text[start..start + end].trim().to_string()
                            } else {
                                text[start..].trim().to_string()
                            }
                        } else {
                            text.trim().to_string()
                        };

                        CommandResponse::ok_with_data(serde_json::to_value(MermaidResult {
                            success: true,
                            mermaid: Some(mermaid),
                            error: None,
                        }).unwrap_or_default())
                    }
                    Err(e) => CommandResponse::ok_with_data(serde_json::to_value(MermaidResult {
                        success: false,
                        mermaid: None,
                        error: Some(format!("Failed to parse Claude response: {}", e)),
                    }).unwrap_or_default()),
                }
            }
            Err(e) => CommandResponse::ok_with_data(serde_json::to_value(MermaidResult {
                success: false,
                mermaid: None,
                error: Some(format!("Request failed: {}", e)),
            }).unwrap_or_default()),
        }
    }

    // ========================================================================
    // CANVAS: Diagram Generation (SVG + Mermaid via Claude API)
    // ========================================================================

    async fn cmd_generate_diagram(
        &self,
        source_type: String,
        prompt: Option<String>,
        github_url: Option<String>,
        image_base64: Option<String>,
        current_mermaid: Option<String>,
        diagram_type: String,
        board_id: String,
    ) -> CommandResponse {
        let api_key = self.claude_api_key.read().await;
        let api_key = match api_key.as_ref() {
            Some(k) => k.clone(),
            None => return CommandResponse::err("Claude API key not configured. Set your API key in Settings."),
        };

        let source = match source_type.as_str() {
            "description" => {
                crate::diagram_gen::DiagramSource::Description {
                    prompt: prompt.unwrap_or_default(),
                }
            }
            "github" => {
                crate::diagram_gen::DiagramSource::Github {
                    url: github_url.unwrap_or_default(),
                    prompt,
                }
            }
            "image" => {
                crate::diagram_gen::DiagramSource::Image {
                    image_base64: image_base64.unwrap_or_default(),
                    prompt,
                }
            }
            "edit" => {
                crate::diagram_gen::DiagramSource::Edit {
                    current_mermaid: current_mermaid.unwrap_or_default(),
                    instruction: prompt.unwrap_or_default(),
                }
            }
            _ => return CommandResponse::err(format!("Unknown source type: {}", source_type)),
        };

        let dt = match diagram_type.as_str() {
            "flowchart" => crate::diagram_gen::DiagramType::Flowchart,
            "sequence" => crate::diagram_gen::DiagramType::Sequence,
            "class_diagram" => crate::diagram_gen::DiagramType::ClassDiagram,
            "architecture" => crate::diagram_gen::DiagramType::Architecture,
            _ => crate::diagram_gen::DiagramType::Auto,
        };

        let request = crate::diagram_gen::DiagramRequest {
            source,
            diagram_type: dt,
            board_id,
        };

        // TODO: get actual peer_id from system
        let peer_id = "local";

        let result = crate::diagram_gen::generate_diagram(
            &self.claude_client,
            &api_key,
            &request,
            peer_id,
        ).await;

        CommandResponse::ok_with_data(serde_json::to_value(result).unwrap_or_default())
    }

    // ========================================================================
    // CLOUD: Ask Analyst (Query with ReAct reasoning)
    // ========================================================================

    async fn cmd_ask_analyst_cloud(&self, question: &str) -> CommandResponse {
        let client = self.cloud_client.read().await;
        let client = match client.as_ref() {
            Some(c) => c,
            None => return CommandResponse::err("Cloud client not initialized"),
        };

        match client.query(question).await {
            Ok(response) => {
                let citations: Vec<String> = response.evidence.iter()
                    .map(|e| format!("[{}] {}", e.source, e.external_id))
                    .collect();

                CommandResponse::ok_with_data(serde_json::json!({
                    "success": true,
                    "response": response.answer,
                    "citations": citations,
                    "confidence": response.confidence,
                    "evidence": response.evidence,
                    "reasoning_trace": response.reasoning_trace,
                    "suggested_actions": response.suggested_actions,
                }))
            }
            Err(e) => CommandResponse::err(format!("Query failed: {}", e)),
        }
    }

    // ========================================================================
    // CLOUD: Lens Search
    // ========================================================================

    async fn cmd_lens_search_cloud(&self, query: &str) -> CommandResponse {
        let client = self.cloud_client.read().await;
        let client = match client.as_ref() {
            Some(c) => c,
            None => return CommandResponse::err("Cloud client not initialized"),
        };

        let request_id = uuid::Uuid::new_v4().to_string();

        match client.query(query).await {
            Ok(response) => {
                let results: Vec<serde_json::Value> = response.evidence.iter()
                    .map(|e| serde_json::json!({
                        "id": e.id,
                        "name": e.external_id,
                        "result_type": e.source,
                        "snippet": e.summary,
                        "relevance": e.relevance,
                    }))
                    .collect();

                CommandResponse::ok_with_data(serde_json::json!({
                    "request_id": request_id,
                    "query": query,
                    "answer": response.answer,
                    "results": results,
                    "confidence": response.confidence,
                    "reasoning_trace": response.reasoning_trace,
                }))
            }
            Err(e) => CommandResponse::err(format!("Search failed: {}", e)),
        }
    }

    // ========================================================================
    // CLOUD: Feed Event
    // ========================================================================

    async fn cmd_feed_event_cloud(&self, event: AIIntegrationEvent) -> CommandResponse {
        let client = self.cloud_client.read().await;
        let client = match client.as_ref() {
            Some(c) => c,
            None => return CommandResponse::err("Cloud client not initialized"),
        };

        let group_id = self.group_id.read().await.clone();
        let workspace_id = self.workspace_id.read().await.clone().unwrap_or_else(|| "default".to_string());

        let event_request = crate::cyan_lens_client::EventRequest {
            id: uuid::Uuid::new_v4().to_string(),
            group_id,
            workspace_id,
            source: event.source.clone(),
            content_kind: event.anchor_kind.clone(),
            external_id: event.anchor_ref.clone(),
            content: event.context.clone(),
            author_id: "".to_string(),
            author_name: "".to_string(),
            url: "".to_string(),
            title: Some(event.anchor_ref.clone()),
            thread_id: None,
            parent_id: None,
            ts: event.last_mention as u64,
            captured_at: chrono::Utc::now().timestamp() as u64,
        };

        match client.send_event(event_request).await {
            Ok(response) => CommandResponse::ok_with_data(serde_json::json!({
                "event_id": response.id,
                "status": response.status,
            })),
            Err(e) => CommandResponse::err(format!("Failed to send event: {}", e)),
        }
    }

    // ========================================================================
    // CLOUD: Feedback, Nudges, Asks, Decisions, Summarize
    // ========================================================================

    async fn cmd_lens_feedback_cloud(&self, request_id: &str, was_helpful: bool) -> CommandResponse {
        tracing::info!("📝 lens_feedback: id={}, helpful={}", &request_id[..8.min(request_id.len())], was_helpful);
        CommandResponse::ok_with_data(serde_json::json!({ "request_id": request_id, "recorded": true }))
    }

    async fn cmd_get_nudges(&self, _group_id: &str) -> CommandResponse {
        let client = self.cloud_client.read().await;
        match client.as_ref() {
            Some(c) => match c.get_nudges().await {
                Ok(report) => CommandResponse::ok_with_data(serde_json::to_value(report).unwrap_or_default()),
                Err(e) => CommandResponse::err(format!("Failed to get nudges: {}", e)),
            },
            None => CommandResponse::err("Cloud client not initialized"),
        }
    }

    async fn cmd_get_asks(&self, _group_id: &str, limit: i32) -> CommandResponse {
        let client = self.cloud_client.read().await;
        match client.as_ref() {
            Some(c) => match c.get_asks(limit).await {
                Ok(asks) => CommandResponse::ok_with_data(serde_json::json!({ "asks": asks })),
                Err(e) => CommandResponse::err(format!("Failed to get asks: {}", e)),
            },
            None => CommandResponse::err("Cloud client not initialized"),
        }
    }

    async fn cmd_get_decisions(&self, _group_id: &str, limit: i32) -> CommandResponse {
        let client = self.cloud_client.read().await;
        match client.as_ref() {
            Some(c) => match c.get_decisions(limit).await {
                Ok(decisions) => CommandResponse::ok_with_data(serde_json::json!({ "decisions": decisions })),
                Err(e) => CommandResponse::err(format!("Failed to get decisions: {}", e)),
            },
            None => CommandResponse::err("Cloud client not initialized"),
        }
    }

    async fn cmd_summarize_cloud(&self, query_type: &str, topic: Option<String>, hours: Option<u64>) -> CommandResponse {
        let client = self.cloud_client.read().await;
        let client = match client.as_ref() {
            Some(c) => c,
            None => return CommandResponse::err("Cloud client not initialized"),
        };

        let result = match query_type {
            "topic" => client.summarize_topic(&topic.unwrap_or_else(|| "general".to_string())).await,
            "recent" => client.summarize_recent(hours.unwrap_or(24)).await,
            "status" => client.status_report().await,
            _ => return CommandResponse::err(format!("Unknown query_type: {}", query_type)),
        };

        match result {
            Ok(summary) => CommandResponse::ok_with_data(serde_json::to_value(summary).unwrap_or_default()),
            Err(e) => CommandResponse::err(format!("Summary failed: {}", e)),
        }
    }

    async fn cmd_set_proactive(&self, enabled: bool) -> CommandResponse {
        *self.proactive_enabled.write().await = enabled;
        CommandResponse::ok_with_data(serde_json::json!({ "proactive": enabled }))
    }

    // ========================================================================
    // LOCAL: P2P Model Registry
    // ========================================================================

    async fn cmd_register_model(&self, cell_id: &str, board_id: &str, model_path: &str, model_kind: &str) -> CommandResponse {
        let path = PathBuf::from(model_path);
        if !path.exists() { return CommandResponse::err(format!("Model not found: {}", model_path)); }
        
        let name = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let model_id = uuid::Uuid::new_v4().to_string();
        
        self.cell_models.write().await.insert(cell_id.to_string(), CellModel {
            cell_id: cell_id.to_string(),
            board_id: board_id.to_string(),
            model_id: model_id.clone(),
            model_name: name.clone(),
            model_kind: model_kind.to_string(),
            file_id: None,
            capabilities: vec![],
            skill_md: None,
            model_path: path,
        });
        
        CommandResponse::ok_with_data(serde_json::json!({ "model_id": model_id, "name": name, "capabilities": [] }))
    }

    async fn cmd_register_model_v2(&self, cell_id: &str, board_id: &str, file_id: &str, skill_md: &str) -> CommandResponse {
        let model_id = uuid::Uuid::new_v4().to_string();
        let capabilities: Vec<String> = skill_md.lines()
            .filter(|l| l.starts_with("- "))
            .map(|l| l.trim_start_matches("- ").to_string())
            .collect();

        self.cell_models.write().await.insert(cell_id.to_string(), CellModel {
            cell_id: cell_id.to_string(),
            board_id: board_id.to_string(),
            model_id: model_id.clone(),
            model_name: format!("model_{}", &file_id[..8.min(file_id.len())]),
            model_kind: "skill".to_string(),
            file_id: Some(file_id.to_string()),
            capabilities: capabilities.clone(),
            skill_md: Some(skill_md.to_string()),
            model_path: PathBuf::new(),
        });
        
        CommandResponse::ok_with_data(serde_json::json!({ "model_id": model_id, "capabilities": capabilities }))
    }

    async fn cmd_import_model(&self, cell_id: &str, board_id: &str, file_path: &str, model_kind: &str) -> CommandResponse {
        let path = PathBuf::from(file_path);
        if !path.exists() { return CommandResponse::err(format!("File not found: {}", file_path)); }
        self.cmd_register_model(cell_id, board_id, file_path, model_kind).await
    }

    async fn cmd_unload_model(&self, cell_id: &str) -> CommandResponse {
        if self.cell_models.write().await.remove(cell_id).is_some() {
            CommandResponse::ok_with_data(serde_json::json!({ "unloaded": true }))
        } else {
            CommandResponse::err("Model not found")
        }
    }

    async fn cmd_infer_model(&self, cell_id: &str, _input: serde_json::Value) -> CommandResponse {
        let models = self.cell_models.read().await;
        match models.get(cell_id) {
            Some(m) => CommandResponse::ok_with_data(serde_json::json!({
                "success": true,
                "output": { "model_id": m.model_id, "model_name": m.model_name, "note": "P2P inference TBD" },
                "timing_ms": 0
            })),
            None => CommandResponse::err("Model not registered"),
        }
    }

    async fn cmd_list_models(&self, _group_id: &str) -> CommandResponse {
        let models = self.cell_models.read().await;
        let summaries: Vec<ModelSummary> = models.values().map(|m| ModelSummary {
            id: m.model_id.clone(),
            name: m.model_name.clone(),
            kind: m.model_kind.clone(),
            capabilities: m.capabilities.clone(),
            board_id: m.board_id.clone(),
            cell_id: m.cell_id.clone(),
            file_id: m.file_id.clone(),
        }).collect();
        
        CommandResponse::ok_with_data(serde_json::json!({ "models": summaries }))
    }

    async fn cmd_get_cell_model(&self, cell_id: &str, _board_id: &str) -> CommandResponse {
        let models = self.cell_models.read().await;
        match models.get(cell_id) {
            Some(m) => CommandResponse::ok_with_data(serde_json::json!({
                "model_id": m.model_id, "name": m.model_name, "kind": m.model_kind,
                "capabilities": m.capabilities, "file_id": m.file_id,
            })),
            None => CommandResponse::err("No model for cell"),
        }
    }
}
