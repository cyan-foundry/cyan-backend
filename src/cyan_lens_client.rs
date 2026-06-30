// src/cyan_lens_client.rs
//
// HTTP client for Cyan Lens Cloud API
// Replaces local xaeroai inference with cloud calls
//
// Used by ai_bridge.rs to route:
// - LensSearch → POST /api/v1/query
// - Summarize → POST /api/v1/summarize  
// - Nudges → GET /api/v1/nudges/:gid
// - Graph ops → GET /api/v1/graph/*
//
// Events are forwarded via Iggy (separate path)

use serde::{Deserialize, Serialize};
use std::time::Duration;

// ============================================================================
// Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct CyanLensConfig {
    pub base_url: String,
    pub group_id: String,
    pub workspace_id: Option<String>,
    pub timeout_secs: u64,
}

impl Default for CyanLensConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8080".to_string(),
            group_id: "default".to_string(),
            workspace_id: None,
            timeout_secs: 30,
        }
    }
}

impl CyanLensConfig {
    pub fn from_env() -> Self {
        Self {
            base_url: std::env::var("CYAN_LENS_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),
            group_id: std::env::var("CYAN_GROUP_ID")
                .unwrap_or_else(|_| "default".to_string()),
            workspace_id: std::env::var("CYAN_WORKSPACE_ID").ok(),
            timeout_secs: std::env::var("CYAN_LENS_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
        }
    }
}

// ============================================================================
// API Types - Requests
// ============================================================================

#[derive(Debug, Serialize)]
pub struct QueryRequest {
    pub query_id: String,
    pub group_id: String,
    pub question: String,
    pub max_steps: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct SummaryRequest {
    pub group_id: String,
    pub workspace_id: Option<String>,
    pub query: SummaryQuery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_filter: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SummaryQuery {
    Topic { topic: String },
    Entity { external_id: String, source: Option<String> },
    Recent { hours: u64 },
    Workspace { workspace_id: String },
    StatusReport,
    Board { board_id: String },
    Group,
    Files { scope_type: String, scope_id: Option<String> },
    JiraEpic { epic_key: String },
    PrActivity { hours: u64, repo: Option<String>, related_epic: Option<String> },
}

#[derive(Debug, Serialize)]
pub struct EventRequest {
    pub id: String,
    pub group_id: String,
    pub workspace_id: String,
    pub source: String,
    pub content_kind: String,
    pub external_id: String,
    pub content: String,
    pub author_id: String,
    pub author_name: String,
    pub url: String,
    pub title: Option<String>,
    pub thread_id: Option<String>,
    pub parent_id: Option<String>,
    pub ts: u64,
    pub captured_at: u64,
}

#[derive(Debug, Serialize)]
pub struct GraphSearchRequest {
    pub q: String,
    pub group_id: String,
    pub limit: i32,
}

// ============================================================================
// API Types - Responses
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryResponse {
    pub query_id: String,
    pub answer: String,
    pub confidence: f32,
    pub evidence: Vec<EvidenceNode>,
    pub suggested_actions: Vec<String>,
    pub reasoning_trace: Vec<ReasoningStep>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EvidenceNode {
    pub id: String,
    pub external_id: String,
    pub source: String,
    pub summary: Option<String>,
    pub relevance: f32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReasoningStep {
    pub step: u32,
    pub thought: String,
    pub action: String,
    pub observation: String,
}

// Tolerant: the live /pulse payload carries `request` + `text` + `generated_at` but
// no `context` — a strict deser here errored the whole Lens-AI panel. Default fills it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct SummaryResponse {
    pub text: String,
    pub generated_at: u64,
    pub context: SummaryContext,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct SummaryContext {
    pub nodes_examined: usize,
    pub key_decisions: Vec<DecisionBrief>,
    pub open_asks: Vec<AskBrief>,
    pub blockers: Vec<BlockerBrief>,
    pub recent_activity: Vec<ActivityBrief>,
    pub workspace_summaries: Option<Vec<WorkspaceBrief>>,
    pub board_summaries: Option<Vec<BoardBrief>>,
    pub file_summaries: Option<Vec<FileBrief>>,
    pub pr_summaries: Option<Vec<PrBrief>>,
    pub jira_issues: Option<Vec<JiraBrief>>,
    pub health_score: Option<f32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DecisionBrief {
    pub content: String,
    pub decider: String,
    pub source_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AskBrief {
    pub question: String,
    pub assignee: Option<String>,
    pub age_hours: i64,
    pub source_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockerBrief {
    pub description: String,
    pub external_id: String,
    pub blocks_count: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ActivityBrief {
    pub external_id: String,
    pub source: String,
    pub summary: Option<String>,
    pub author: Option<String>,
    pub ts: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkspaceBrief {
    pub workspace_id: String,
    pub name: String,
    pub board_count: usize,
    pub open_asks: usize,
    pub blockers: usize,
    pub recent_decisions: usize,
    pub health_score: f32,
    pub summary: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BoardBrief {
    pub board_id: String,
    pub name: String,
    pub chat_count: usize,
    pub file_count: usize,
    pub linked_jira: Vec<String>,
    pub linked_prs: Vec<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileBrief {
    pub file_id: String,
    pub filename: String,
    pub file_type: String,
    pub size_bytes: Option<u64>,
    pub summary: String,
    pub extracted_topics: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PrBrief {
    pub pr_number: u32,
    pub title: String,
    pub author: String,
    pub status: String,
    pub repo: String,
    pub linked_jira: Vec<String>,
    pub files_changed: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JiraBrief {
    pub key: String,
    pub summary: String,
    pub status: String,
    pub assignee: Option<String>,
    pub priority: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct NudgeReport {
    pub group_id: String,
    pub generated_at: u64,
    pub nudges: Vec<Nudge>,
    pub summary: NudgeSummary,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Nudge {
    pub nudge_type: String,
    pub question: Option<String>,
    pub external_id: Option<String>,
    pub decision: Option<String>,
    pub age_hours: Option<i64>,
    pub stale_days: Option<i64>,
    pub source_node_id: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct NudgeSummary {
    pub stale_asks: usize,
    pub stale_blockers: usize,
    pub unanswered_mentions: usize,
    pub unimplemented_decisions: usize,
    pub total: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GraphNode {
    pub id: String,
    pub group_id: String,
    pub workspace_id: String,
    pub source: String,
    pub content_kind: String,
    pub external_id: String,
    pub content: Option<String>,
    pub summary: Option<String>,
    pub title: Option<String>,
    pub url: Option<String>,
    pub author_id: Option<String>,
    pub author_name: Option<String>,
    pub is_blocker: Option<bool>,
    pub status: Option<String>,
    pub ts: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GraphEdge {
    pub id: String,
    pub source_node_id: String,
    pub target_node_id: String,
    pub relation: String,
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct AsksResponse {
    pub asks: Vec<AskRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AskRow {
    pub id: String,
    pub source_node_id: String,
    pub group_id: String,
    pub content: String,
    pub asker_name: String,
    pub assignee_name: Option<String>,
    pub status: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct DecisionsResponse {
    pub decisions: Vec<DecisionRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DecisionRow {
    pub id: String,
    pub source_node_id: String,
    pub group_id: String,
    pub content: String,
    pub decider_name: String,
    pub rationale: Option<String>,
    pub created_at: i64,
}

/// Tolerant by design: the lens health `data` shape drifts (it dropped `iggy` from
/// the payload and added `commit`), and a STRICT deser here made the whole app show
/// "Lens offline" against a perfectly live lens. `#[serde(default)]` + Default means a
/// missing field is a default (never a parse error); unknown fields are ignored by
/// serde already. Mirrors the lens-side "tolerant deser" invariant.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct HealthResponse {
    pub postgres: bool,
    pub iggy: bool,
    pub vllm: bool,
    pub lens: bool,
    /// The deployed lens build sha (added by the lens; absent on older lenses).
    pub commit: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventResponse {
    pub id: String,
    pub status: String,
}

// ============================================================================
// Client
// ============================================================================

pub struct CyanLensClient {
    client: reqwest::Client,
    config: CyanLensConfig,
}

impl CyanLensClient {
    pub fn new(config: CyanLensConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .expect("Failed to create HTTP client");

        Self { client, config }
    }

    pub fn from_env() -> Self {
        Self::new(CyanLensConfig::from_env())
    }

    /// Update group_id for subsequent requests
    pub fn set_group_id(&mut self, group_id: String) {
        self.config.group_id = group_id;
    }

    /// Update workspace_id for subsequent requests
    pub fn set_workspace_id(&mut self, workspace_id: Option<String>) {
        self.config.workspace_id = workspace_id;
    }

    /// Get current config
    pub fn config(&self) -> &CyanLensConfig {
        &self.config
    }

    // ========================================================================
    // Health
    // ========================================================================

    pub async fn health(&self) -> Result<HealthResponse, CyanLensError> {
        let url = format!("{}/api/v1/health", self.config.base_url);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<HealthResponse> = resp.json().await?;
        body.data.ok_or_else(|| CyanLensError::Api(body.error.unwrap_or_default()))
    }

    pub async fn is_available(&self) -> bool {
        self.health().await.is_ok()
    }

    // ========================================================================
    // Query (Graph Reasoning)
    // ========================================================================

    pub async fn query(&self, question: &str) -> Result<QueryResponse, CyanLensError> {
        let url = format!("{}/api/v1/query", self.config.base_url);
        let request = QueryRequest {
            query_id: generate_id("q"),
            group_id: self.config.group_id.clone(),
            question: question.to_string(),
            max_steps: Some(10),
        };

        let resp = self.client.post(&url).json(&request).send().await?;
        let body: ApiResponse<QueryResponse> = resp.json().await?;
        body.data.ok_or_else(|| CyanLensError::Api(body.error.unwrap_or_default()))
    }

    // ========================================================================
    // Summarize
    // ========================================================================

    pub async fn summarize_topic(&self, topic: &str) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::Topic { topic: topic.to_string() }).await
    }

    pub async fn summarize_recent(&self, hours: u64) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::Recent { hours }).await
    }

    pub async fn status_report(&self) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::StatusReport).await
    }

    pub async fn summarize_entity(&self, external_id: &str, source: Option<&str>) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::Entity {
            external_id: external_id.to_string(),
            source: source.map(String::from),
        }).await
    }

    async fn summarize(&self, query: SummaryQuery) -> Result<SummaryResponse, CyanLensError> {
        self.summarize_filtered(query, None).await
    }

    async fn summarize_filtered(&self, query: SummaryQuery, source_filter: Option<Vec<String>>) -> Result<SummaryResponse, CyanLensError> {
        let url = format!("{}/api/v1/summarize", self.config.base_url);
        let request = SummaryRequest {
            group_id: self.config.group_id.clone(),
            workspace_id: self.config.workspace_id.clone(),
            query,
            source_filter,
        };

        let resp = self.client.post(&url).json(&request).send().await?;
        let body: ApiResponse<SummaryResponse> = resp.json().await?;
        body.data.ok_or_else(|| CyanLensError::Api(body.error.unwrap_or_default()))
    }

    pub async fn summarize_board(&self, board_id: &str) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::Board { board_id: board_id.to_string() }).await
    }

    pub async fn summarize_group(&self) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::Group).await
    }

    pub async fn summarize_workspace(&self, workspace_id: &str) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::Workspace { workspace_id: workspace_id.to_string() }).await
    }

    pub async fn summarize_files(&self, scope_type: &str, scope_id: Option<&str>) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::Files {
            scope_type: scope_type.to_string(),
            scope_id: scope_id.map(String::from),
        }).await
    }

    pub async fn summarize_jira_epic(&self, epic_key: &str) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::JiraEpic { epic_key: epic_key.to_string() }).await
    }

    pub async fn summarize_pr_activity(&self, hours: u64, repo: Option<&str>, epic: Option<&str>) -> Result<SummaryResponse, CyanLensError> {
        self.summarize(SummaryQuery::PrActivity {
            hours,
            repo: repo.map(String::from),
            related_epic: epic.map(String::from),
        }).await
    }

    /// Summarize with source filter (e.g. only slack, only confluence)
    pub async fn summarize_with_filter(&self, query: SummaryQuery, sources: Vec<String>) -> Result<SummaryResponse, CyanLensError> {
        self.summarize_filtered(query, Some(sources)).await
    }

    // ========================================================================
    // Pulse
    // ========================================================================

    pub async fn pulse(&self, days: u32) -> Result<SummaryResponse, CyanLensError> {
        let url = format!("{}/api/v1/pulse/{}?days={}", self.config.base_url, self.config.group_id, days);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<SummaryResponse> = resp.json().await?;
        body.data.ok_or_else(|| CyanLensError::Api(body.error.unwrap_or_default()))
    }

    // ========================================================================
    // Nudges
    // ========================================================================

    pub async fn get_nudges(&self) -> Result<NudgeReport, CyanLensError> {
        let url = format!("{}/api/v1/nudges/{}", self.config.base_url, self.config.group_id);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<NudgeReport> = resp.json().await?;
        body.data.ok_or_else(|| CyanLensError::Api(body.error.unwrap_or_default()))
    }

    // ========================================================================
    // Asks & Decisions
    // ========================================================================

    pub async fn get_asks(&self, limit: i32) -> Result<Vec<AskRow>, CyanLensError> {
        let url = format!("{}/api/v1/asks/{}?limit={}", self.config.base_url, self.config.group_id, limit);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<AsksResponse> = resp.json().await?;
        Ok(body.data.map(|r| r.asks).unwrap_or_default())
    }

    pub async fn get_decisions(&self, limit: i32) -> Result<Vec<DecisionRow>, CyanLensError> {
        let url = format!("{}/api/v1/decisions/{}?limit={}", self.config.base_url, self.config.group_id, limit);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<DecisionsResponse> = resp.json().await?;
        Ok(body.data.map(|r| r.decisions).unwrap_or_default())
    }

    // ========================================================================
    // Graph Operations
    // ========================================================================

    pub async fn search_nodes(&self, query: &str, limit: i32) -> Result<Vec<GraphNode>, CyanLensError> {
        let url = format!(
            "{}/api/v1/graph/search?q={}&group_id={}&limit={}",
            self.config.base_url,
            urlencoding::encode(query),
            self.config.group_id,
            limit
        );
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<serde_json::Value> = resp.json().await?;
        
        if let Some(data) = body.data {
            let nodes: Vec<GraphNode> = serde_json::from_value(data["nodes"].clone()).unwrap_or_default();
            Ok(nodes)
        } else {
            Err(CyanLensError::Api(body.error.unwrap_or_default()))
        }
    }

    pub async fn get_node(&self, id: &str) -> Result<Option<GraphNode>, CyanLensError> {
        let url = format!("{}/api/v1/graph/node/{}", self.config.base_url, id);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<serde_json::Value> = resp.json().await?;
        
        if let Some(data) = body.data {
            let node: Option<GraphNode> = serde_json::from_value(data["node"].clone()).ok();
            Ok(node)
        } else {
            Ok(None)
        }
    }

    pub async fn get_edges(&self, node_id: &str) -> Result<(Vec<GraphEdge>, Vec<GraphEdge>), CyanLensError> {
        let url = format!("{}/api/v1/graph/edges/{}", self.config.base_url, node_id);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<serde_json::Value> = resp.json().await?;
        
        if let Some(data) = body.data {
            let outgoing: Vec<GraphEdge> = serde_json::from_value(data["outgoing"].clone()).unwrap_or_default();
            let incoming: Vec<GraphEdge> = serde_json::from_value(data["incoming"].clone()).unwrap_or_default();
            Ok((outgoing, incoming))
        } else {
            Err(CyanLensError::Api(body.error.unwrap_or_default()))
        }
    }

    pub async fn traverse(&self, node_id: &str, relation: Option<&str>, direction: &str) -> Result<Vec<GraphNode>, CyanLensError> {
        let mut url = format!(
            "{}/api/v1/graph/traverse/{}?direction={}",
            self.config.base_url, node_id, direction
        );
        if let Some(rel) = relation {
            url = format!("{}&relation={}", url, rel);
        }

        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<serde_json::Value> = resp.json().await?;
        
        if let Some(data) = body.data {
            let nodes: Vec<GraphNode> = serde_json::from_value(data["nodes"].clone()).unwrap_or_default();
            Ok(nodes)
        } else {
            Err(CyanLensError::Api(body.error.unwrap_or_default()))
        }
    }

    // ========================================================================
    // Events (for direct submission, alternative to Iggy)
    // ========================================================================

    pub async fn send_event(&self, event: EventRequest) -> Result<EventResponse, CyanLensError> {
        let url = format!("{}/api/v1/events", self.config.base_url);
        let resp = self.client.post(&url).json(&event).send().await?;
        let body: ApiResponse<EventResponse> = resp.json().await?;
        body.data.ok_or_else(|| CyanLensError::Api(body.error.unwrap_or_default()))
    }
}

// ============================================================================
// Error
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum CyanLensError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    
    #[error("API error: {0}")]
    Api(String),
    
    #[error("Not available")]
    NotAvailable,
}

// ============================================================================
// Helpers
// ============================================================================

fn generate_id(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}_{}", prefix, ts)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = CyanLensConfig::default();
        assert_eq!(config.base_url, "http://localhost:8080");
        assert_eq!(config.group_id, "default");
    }

    // Contract tests: the EXACT live lens payloads (captured 2026-06-25 from the
    // deployed c3b809f lens) must parse — these are the shapes that made a strict
    // deser show "Lens offline" / Lens-AI "Error" against a live lens.

    #[test]
    fn health_parses_live_shape_without_iggy_and_with_commit() {
        // Live: no `iggy`, plus a `commit` the old struct didn't know about.
        let live = r#"{"success":true,"data":{"postgres":true,"vllm":true,"lens":true,"commit":"c3b809f"}}"#;
        let r: ApiResponse<HealthResponse> = serde_json::from_str(live).unwrap();
        let h = r.data.expect("health data parsed");
        assert!(h.vllm && h.lens && h.postgres);
        assert!(!h.iggy, "missing iggy defaults to false, never a parse error");
        assert_eq!(h.commit.as_deref(), Some("c3b809f"));
    }

    #[test]
    fn pulse_parses_live_shape_without_context() {
        // Live /pulse carries request+text+generated_at but NO `context`.
        let live = r#"{"success":true,"data":{"request":{"group_id":"default"},"generated_at":1782427784,"text":"summary"}}"#;
        let r: ApiResponse<SummaryResponse> = serde_json::from_str(live).unwrap();
        let s = r.data.expect("pulse data parsed");
        assert_eq!(s.text, "summary");
        assert_eq!(s.context.nodes_examined, 0, "missing context defaults");
    }

    #[test]
    fn nudges_asks_decisions_parse_live_shapes() {
        let n = r#"{"success":true,"data":{"tenant_id":"default","group_id":"default","generated_at":1,"nudges":[],"summary":{"stale_asks":0,"stale_blockers":0,"unanswered_mentions":0,"unimplemented_decisions":0,"total":0}}}"#;
        let nr: ApiResponse<NudgeReport> = serde_json::from_str(n).unwrap();
        assert_eq!(nr.data.unwrap().summary.total, 0);
        let a: ApiResponse<AsksResponse> = serde_json::from_str(r#"{"success":true,"data":{"asks":[]}}"#).unwrap();
        assert!(a.data.unwrap().asks.is_empty());
        let d: ApiResponse<DecisionsResponse> = serde_json::from_str(r#"{"success":true,"data":{"decisions":[]}}"#).unwrap();
        assert!(d.data.unwrap().decisions.is_empty());
    }
}
