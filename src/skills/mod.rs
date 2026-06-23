// cyan-backend/src/skills/mod.rs
//
// Skill registry for Cyan Lens pipeline execution.
// Skills are pluggable units that connect intent (English markdown)
// to execution (API calls, CLI commands, AI analysis).
//
// The pipeline executor resolves cell intent → skill → tools → execute.

pub mod slack;
pub mod github;
pub mod jira;
pub mod email;
pub mod cyan_dm;
pub mod media;
pub mod ssai;
pub mod localization;
pub mod qc_fix;
pub mod ad_splice;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Skill Definition
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDef {
    pub id: String,
    pub name: String,
    pub description: String,
    pub keywords: Vec<String>,        // for intent matching
    pub tools: Vec<String>,           // tool IDs this skill uses
    pub output_type: OutputType,
    pub requires_auth: Vec<String>,   // e.g. ["slack", "github"]
    pub default_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OutputType {
    Summary,          // text → cell output
    TimecodedNotes,   // findings[] → video notes  
    Json,             // structured → cell metadata
    Action,           // side effect → confirmation
    Passthrough,      // raw output → next step context
}

// ============================================================================
// Skill Execution Context
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillContext {
    /// Board and pipeline info
    pub board_id: String,
    pub step_id: String,
    pub cell_content: String,
    
    /// Output from previous pipeline steps (chained context)
    pub previous_outputs: Vec<StepOutput>,
    
    /// Integration credentials (resolved by executor)
    pub credentials: HashMap<String, String>,
    
    /// Video asset URI (if media pipeline)
    pub video_uri: Option<String>,
    
    /// Scope ID for integration queries
    pub scope_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepOutput {
    pub step_id: String,
    pub output: String,
    pub output_type: OutputType,
    pub artifacts: HashMap<String, serde_json::Value>,  // named outputs: "transcript" → json, "metadata" → json
}

// ============================================================================
// Skill Result
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillResult {
    pub skill_id: String,
    pub output_type: OutputType,
    pub summary: String,                          // human-readable summary
    pub data: serde_json::Value,                  // structured output
    pub timecoded_findings: Option<Vec<Finding>>,  // for TimecodedNotes output
    pub action_taken: Option<String>,              // for Action output (email sent, DM sent)
    pub artifacts: Vec<String>,                    // file paths, URLs
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub timecode_seconds: f64,
    pub content: String,
    pub finding_type: String,     // qc_issue, comment, action, revision
    pub severity: String,         // info, warning, critical
    pub source: String,           // which tool generated this
    pub suggested_action: Option<String>,
}

// ============================================================================
// Inference Status — real-time feedback during model execution
// ============================================================================

/// Send inference status updates to the UI during pipeline execution.
/// These appear as live-updating messages in the pipeline view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceStatus {
    pub step_id: String,
    pub model: String,          // "whisper-large-v3", "llama-3.3-70b", "indictrans2-1b", "ffprobe"
    pub phase: String,          // "loading", "processing", "analyzing", "generating", "complete"
    pub message: String,        // human-readable: "Transcribing audio segment 45/119..."
    pub progress: Option<f32>,  // 0.0-1.0 if known
    pub detail: Option<String>, // optional technical detail
}

impl InferenceStatus {
    pub fn new(step_id: &str, model: &str, phase: &str, message: &str) -> Self {
        Self {
            step_id: step_id.to_string(),
            model: model.to_string(),
            phase: phase.to_string(),
            message: message.to_string(),
            progress: None,
            detail: None,
        }
    }

    pub fn with_progress(mut self, progress: f32) -> Self {
        self.progress = Some(progress);
        self
    }

    pub fn with_detail(mut self, detail: &str) -> Self {
        self.detail = Some(detail.to_string());
        self
    }

    /// Format for display in the UI
    pub fn display(&self) -> String {
        let icon = match self.phase.as_str() {
            "loading" => "⏳",
            "processing" => "🔄",
            "analyzing" => "🔍",
            "generating" => "✨",
            "complete" => "✅",
            "error" => "❌",
            _ => "🤖",
        };
        if let Some(pct) = self.progress {
            format!("{} [{}] {} ({:.0}%)", icon, self.model, self.message, pct * 100.0)
        } else {
            format!("{} [{}] {}", icon, self.model, self.message)
        }
    }
}

// ============================================================================
// Skill Registry
// ============================================================================

pub struct SkillRegistry {
    skills: HashMap<String, SkillDef>,
}

impl Default for SkillRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SkillRegistry {
    /// Create registry with all built-in skills
    pub fn new() -> Self {
        let mut skills = HashMap::new();
        
        // SaaS skills
        for skill in slack::register() { skills.insert(skill.id.clone(), skill); }
        for skill in github::register() { skills.insert(skill.id.clone(), skill); }
        for skill in jira::register() { skills.insert(skill.id.clone(), skill); }
        for skill in email::register() { skills.insert(skill.id.clone(), skill); }
        for skill in cyan_dm::register() { skills.insert(skill.id.clone(), skill); }
        
        // Media skills
        for skill in media::register() { skills.insert(skill.id.clone(), skill); }
        
        // SSAI skills
        for skill in ssai::register() { skills.insert(skill.id.clone(), skill); }
        
        // Localization skills (transcription, translation, compliance)
        for skill in localization::register() { skills.insert(skill.id.clone(), skill); }
        
        // QC Fix skill (execute corrections from user feedback)
        for skill in qc_fix::register() { skills.insert(skill.id.clone(), skill); }
        
        // Ad Splice skill (insert ads with proper encoding)
        for skill in ad_splice::register() { skills.insert(skill.id.clone(), skill); }
        
        tracing::info!("🔧 Skill registry loaded: {} skills", skills.len());
        for (id, skill) in &skills {
            tracing::debug!("  - {}: {} ({})", id, skill.name, skill.output_type.label());
        }
        
        Self { skills }
    }
    
    /// Get a skill by ID
    pub fn get(&self, id: &str) -> Option<&SkillDef> {
        self.skills.get(id)
    }
    
    /// List all skills
    pub fn list(&self) -> Vec<&SkillDef> {
        self.skills.values().collect()
    }
    
    /// Resolve intent to skill using keyword matching + LLM
    pub fn resolve_intent(&self, cell_content: &str) -> Option<&SkillDef> {
        let content_lower = cell_content.to_lowercase();
        
        // First pass: keyword matching (fast, no LLM needed)
        let mut best_match: Option<(&SkillDef, usize)> = None;
        
        for skill in self.skills.values() {
            let score = skill.keywords.iter()
                .filter(|kw| content_lower.contains(&kw.to_lowercase()))
                .count();
            
            if score > 0 {
                if let Some((_, best_score)) = best_match {
                    if score > best_score {
                        best_match = Some((skill, score));
                    }
                } else {
                    best_match = Some((skill, score));
                }
            }
        }
        
        if let Some((skill, score)) = best_match {
            tracing::info!("🔧 Resolved intent to skill '{}' (score: {})", skill.id, score);
            return Some(skill);
        }
        
        tracing::debug!("🔧 No skill matched for: {}", &content_lower[..content_lower.len().min(80)]);
        None
    }
    
    /// Build prompt for LLM to resolve ambiguous intent
    pub fn build_resolution_prompt(&self, cell_content: &str) -> String {
        let skill_list: Vec<String> = self.skills.values()
            .map(|s| format!("  - {}: {} (keywords: {})", s.id, s.description, s.keywords.join(", ")))
            .collect();
        
        format!(
            "Given this workflow step, which skill should execute it?\n\n\
             Step: \"{}\"\n\n\
             Available skills:\n{}\n\n\
             Return ONLY the skill_id, nothing else.",
            cell_content, skill_list.join("\n")
        )
    }
}

impl OutputType {
    pub fn label(&self) -> &str {
        match self {
            OutputType::Summary => "summary",
            OutputType::TimecodedNotes => "timecoded_notes",
            OutputType::Json => "json",
            OutputType::Action => "action",
            OutputType::Passthrough => "passthrough",
        }
    }
}

// ============================================================================
// Skill Executor Trait
// ============================================================================

/// All skills implement this async trait
#[async_trait::async_trait]
pub trait SkillExecutor: Send + Sync {
    /// Execute the skill with given context
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult>;
    
    /// Skill definition
    fn definition(&self) -> &SkillDef;
}

// ============================================================================
// Execute a skill by ID
// ============================================================================

pub async fn execute_skill(skill_id: &str, ctx: &SkillContext) -> Result<SkillResult> {
    match skill_id {
        // SaaS skills
        "slack_digest" => slack::SlackDigest.execute(ctx).await,
        "slack_search" => slack::SlackSearch.execute(ctx).await,
        "github_pr_review" => github::PrReview.execute(ctx).await,
        "github_commit_summary" => github::CommitSummary.execute(ctx).await,
        "jira_my_board" => jira::MyBoard.execute(ctx).await,
        "jira_scan" => jira::TicketScan.execute(ctx).await,
        "draft_email" => email::DraftEmail.execute(ctx).await,
        "send_email" => email::SendEmail.execute(ctx).await,
        "cyan_nudge" => cyan_dm::Nudge.execute(ctx).await,
        
        // Media skills
        "ffprobe_metadata" => media::FfprobeMetadata.execute(ctx).await,
        "qc_analysis" => media::QcAnalysis.execute(ctx).await,
        "loudness_scan" => media::LoudnessScan.execute(ctx).await,
        "scene_detect" => media::SceneDetect.execute(ctx).await,
        
        // SSAI skills
        "ssai_break_detection" => ssai::AdBreakDetection.execute(ctx).await,
        "ssai_compliance" => ssai::AdComplianceCheck.execute(ctx).await,
        
        // Localization skills (NEW — wired)
        "transcription" => localization::Transcription.execute(ctx).await,
        "translation" => localization::Translation.execute(ctx).await,
        "compliance_check" => localization::ComplianceCheck.execute(ctx).await,
        
        // QC Fix skill (NEW — user-flagged corrections)
        "qc_fix" => qc_fix::QcFix.execute(ctx).await,
        
        // Ad Splice skill (NEW — encoding-safe ad insertion)
        "ad_splice" => ad_splice::AdSplice.execute(ctx).await,
        
        _ => Err(anyhow!("Unknown skill: {}", skill_id)),
    }
}

// ============================================================================
// Global registry (lazy init)
// ============================================================================

use std::sync::OnceLock;

static REGISTRY: OnceLock<SkillRegistry> = OnceLock::new();

pub fn registry() -> &'static SkillRegistry {
    REGISTRY.get_or_init(SkillRegistry::new)
}
