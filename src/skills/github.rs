// cyan-backend/src/skills/github.rs

#![allow(dead_code)] // Skills scaffolding moving to the MCP/workflow model; see CLAUDE.md 'Out of scope'.

use anyhow::{anyhow, Result};
use serde_json::json;

use super::*;

pub fn register() -> Vec<SkillDef> {
    vec![
        SkillDef {
            id: "github_pr_review".into(),
            name: "GitHub PR Review Status".into(),
            description: "Scan open PRs, flag stale reviews, identify blockers".into(),
            keywords: vec!["github".into(), "pr".into(), "pull request".into(), "review".into(), "blocked".into(), "stale".into(), "open prs".into()],
            tools: vec!["github_api".into(), "vllm".into()],
            output_type: OutputType::Summary,
            requires_auth: vec!["github".into()],
            default_timeout: 120,
        },
        SkillDef {
            id: "github_commit_summary".into(),
            name: "GitHub Commit Summary".into(),
            description: "Summarize recent commits and code changes".into(),
            keywords: vec!["commits".into(), "code changes".into(), "what was pushed".into(), "merged".into()],
            tools: vec!["github_api".into(), "vllm".into()],
            output_type: OutputType::Summary,
            requires_auth: vec!["github".into()],
            default_timeout: 120,
        },
    ]
}

pub struct PrReview;
pub struct CommitSummary;

#[async_trait::async_trait]
impl SkillExecutor for PrReview {
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        tracing::info!("🔧 [github_pr_review] Executing");
        
        // Load PR data from local nodes DB
        let scope_id = ctx.scope_id.as_deref().unwrap_or("");
        let prs = load_github_prs(scope_id)?;
        
        if prs.is_empty() {
            return Ok(SkillResult {
                skill_id: "github_pr_review".into(),
                output_type: OutputType::Summary,
                summary: "No open PRs found.".into(),
                data: json!({"pr_count": 0}),
                timecoded_findings: None,
                action_taken: None,
                artifacts: vec![],
            });
        }
        
        // Build LLM prompt
        let pr_text: Vec<String> = prs.iter()
            .map(|pr| format!(
                "PR #{}: {} (by {}, opened {}, reviewers: {}, status: {})",
                pr.number, pr.title, pr.author, pr.created_at, pr.reviewers, pr.status
            ))
            .collect();
        
        let prompt = format!(
            "Analyze these GitHub PRs and identify:\n\
             1. PRs waiting for review for more than 2 days\n\
             2. PRs that appear blocked (review requested but not started)\n\
             3. PRs that are ready to merge\n\
             4. Any patterns (one reviewer overloaded, etc.)\n\n\
             Context: {}\n\n\
             PRs:\n{}\n\n\
             Provide actionable summary. Flag urgency.",
            ctx.cell_content,
            pr_text.join("\n")
        );
        
        let response = crate::pipeline::call_vllm_public(&prompt, 600, 0.3).await?;
        
        let blocked: Vec<_> = prs.iter().filter(|pr| pr.status == "review_pending" || pr.days_open > 2).collect();
        
        Ok(SkillResult {
            skill_id: "github_pr_review".into(),
            output_type: OutputType::Summary,
            summary: response,
            data: json!({
                "total_prs": prs.len(),
                "blocked_count": blocked.len(),
                "blocked_prs": blocked.iter().map(|pr| json!({
                    "number": pr.number,
                    "title": pr.title,
                    "author": pr.author,
                    "days_open": pr.days_open,
                    "reviewers": pr.reviewers,
                })).collect::<Vec<_>>(),
            }),
            timecoded_findings: None,
            action_taken: None,
            artifacts: vec![],
        })
    }
    
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[0].clone())
    }
}

#[async_trait::async_trait]
impl SkillExecutor for CommitSummary {
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let scope_id = ctx.scope_id.as_deref().unwrap_or("");
        let commits = load_github_commits(scope_id, 24)?;
        
        let commit_text: Vec<String> = commits.iter()
            .map(|c| format!("{}: {} (by {})", &c.sha[..7], c.message, c.author))
            .collect();
        
        let prompt = format!(
            "Summarize these recent commits:\n{}\n\nHighlight: major changes, patterns, areas of activity.",
            commit_text.join("\n")
        );
        
        let response = crate::pipeline::call_vllm_public(&prompt, 400, 0.3).await?;
        
        Ok(SkillResult {
            skill_id: "github_commit_summary".into(),
            output_type: OutputType::Summary,
            summary: response,
            data: json!({"commit_count": commits.len()}),
            timecoded_findings: None,
            action_taken: None,
            artifacts: vec![],
        })
    }
    
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[1].clone())
    }
}

// ============================================================================
// GitHub Data Loaders
// ============================================================================

#[derive(Debug, Clone, serde::Serialize)]
struct GithubPr {
    number: u32,
    title: String,
    author: String,
    created_at: String,
    reviewers: String,
    status: String,
    days_open: u32,
    url: String,
}

#[derive(Debug, Clone)]
struct GithubCommit {
    sha: String,
    message: String,
    author: String,
    ts: u64,
}

/// Load GitHub PR data from imported PR boards
fn load_github_prs(_scope_id: &str) -> Result<Vec<GithubPr>> {
    let conn = crate::storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
    
    // Find PR board content — imported boards have names like "org/repo — Pull Requests"
    let mut stmt = conn.prepare(
        "SELECT nc.content, o.name \
         FROM notebook_cells nc \
         JOIN objects o ON nc.board_id = o.id \
         WHERE (o.name LIKE '%Pull Request%' OR o.name LIKE '%PR%') \
         ORDER BY nc.cell_order LIMIT 5"
    )?;
    
    let mut prs = Vec::new();
    let rows: Vec<(String, String)> = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?.filter_map(|r| r.ok()).collect();
    
    for (content, _board_name) in &rows {
        // Parse PR markdown: "- [x] **#12**: title → @Author _Nmo ago_"
        for line in content.lines() {
            if line.contains("**#") {
                let is_merged = line.contains("[x]");
                let number = line.split("**#").nth(1)
                    .and_then(|s| s.split("**").next())
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);
                let title = line.split("**: ").nth(1)
                    .and_then(|s| s.split(" → ").next())
                    .unwrap_or("").to_string();
                let author = line.split("@").nth(1)
                    .and_then(|s| s.split_whitespace().next())
                    .unwrap_or("unknown").to_string();
                let age = line.split("_").nth(1).unwrap_or("").to_string();
                
                prs.push(GithubPr {
                    number,
                    title,
                    author,
                    created_at: age.clone(),
                    reviewers: if is_merged { "merged".into() } else { "pending".into() },
                    status: if is_merged { "merged".into() } else { "open".into() },
                    days_open: if age.contains("mo") { 60 } else if age.contains("d") { 
                        age.chars().filter(|c| c.is_ascii_digit()).collect::<String>().parse().unwrap_or(0)
                    } else { 0 },
                    url: format!("https://github.com/block-xaero/cyan-backend/pull/{}", number),
                });
            }
        }
    }
    
    Ok(prs)
}

/// Load GitHub commit data from imported Activity Dashboard boards
fn load_github_commits(_scope_id: &str, _hours: u64) -> Result<Vec<GithubCommit>> {
    let conn = crate::storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
    
    let mut stmt = conn.prepare(
        "SELECT nc.content \
         FROM notebook_cells nc \
         JOIN objects o ON nc.board_id = o.id \
         WHERE o.name LIKE '%Activity Dashboard%' \
         ORDER BY nc.cell_order LIMIT 5"
    )?;
    
    let mut commits = Vec::new();
    let rows: Vec<String> = stmt.query_map([], |row| {
        row.get::<_, String>(0)
    })?.filter_map(|r| r.ok()).collect();
    
    for content in &rows {
        // Parse commit lines from Activity Dashboard markdown
        for line in content.lines() {
            if line.contains("` ") && (line.contains("@") || line.contains("by")) {
                let sha = line.split('`').nth(1).unwrap_or("unknown").to_string();
                let message = line.split("` ").nth(1)
                    .and_then(|s| s.split(" — ").next().or(s.split(" by ").next()))
                    .unwrap_or(line).trim().to_string();
                let author = line.split("@").nth(1)
                    .and_then(|s| s.split_whitespace().next())
                    .unwrap_or("unknown").to_string();
                
                commits.push(GithubCommit {
                    sha,
                    message,
                    ts: 0,
                    author,
                });
            }
        }
    }
    
    Ok(commits)
}
