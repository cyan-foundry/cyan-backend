// src/diagram_gen.rs
//
// Canvas diagram generation via Claude API.
// Returns both SVG (beautiful rendering) and Mermaid (portable source).
//
// Sources:
//   1. Description → Claude generates diagram from natural language
//   2. GitHub repo → Fetch repo tree + source → Claude generates diagram
//   3. Image → Claude vision analyzes photo → generates diagram
//   4. Edit → Take existing mermaid + instruction → Claude modifies
//
// Prompt genesis: every diagram stores its origin prompt + source for
// collaborative refinement. Peers can view/edit/regenerate.

#![allow(dead_code)] // Canvas diagram-generation scaffolding (SVG/Mermaid extractors, serde response shape) not all wired up yet; see CLAUDE.md 'Out of scope'.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::time::Instant;

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "source_type", rename_all = "snake_case")]
pub enum DiagramSource {
    Description { prompt: String },
    Github { url: String, prompt: Option<String> },
    Image { image_base64: String, prompt: Option<String> },
    Edit { current_mermaid: String, instruction: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagramType {
    Flowchart,
    Sequence,
    ClassDiagram,
    Architecture,
    Auto, // Claude decides
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagramRequest {
    pub source: DiagramSource,
    pub diagram_type: DiagramType,
    pub board_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagramResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub svg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mermaid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_genesis: Option<PromptGenesis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptGenesis {
    pub original_prompt: String,
    pub source_type: String,
    pub source_ref: Option<String>, // GitHub URL, image hash, etc.
    pub diagram_type: String,
    pub created_by: String, // peer_id
    pub created_at: i64,
    pub revisions: Vec<PromptRevision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptRevision {
    pub peer_id: String,
    pub instruction: String,
    pub timestamp: i64,
}

// ============================================================================
// Claude API Types (reuse existing pattern from ai_bridge.rs)
// ============================================================================

#[derive(Debug, Serialize)]
struct ClaudeRequest {
    model: String,
    max_tokens: u32,
    system: String,
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
// System Prompt — Mermaid diagram generation
// ============================================================================

const MERMAID_SYSTEM_PROMPT: &str = r##"You are a diagram architect for Cyan, a collaborative workspace app.

When asked to create a diagram, return ONLY valid Mermaid syntax. No explanation, no markdown fences, no JSON wrapper — just the raw Mermaid code.

## Diagram Types
Choose the best type based on the request:
- flowchart TD (top-down) or LR (left-right) — for processes, data flows, architectures
- sequenceDiagram — for interactions between actors over time
- classDiagram — for types, fields, methods, relationships
- stateDiagram-v2 — for state machines
- erDiagram — for database schemas

## Style Rules
- Use subgraph blocks to group related nodes (e.g. subgraph Backend [...] end)
- Use meaningful node IDs: AUTH[Auth Service] not A[Auth Service]
- Add descriptive edge labels: AUTH -->|validates token| API
- Keep diagrams clean: max 15-20 nodes, summarize if source is larger
- Use classDef for styling categories:
  classDef service fill:#3C3489,stroke:#534AB7,color:#CECBF6
  classDef storage fill:#085041,stroke:#0F6E56,color:#9FE1CB
  classDef external fill:#712B13,stroke:#993C1D,color:#F5C4B3
  classDef database fill:#0C447C,stroke:#185FA5,color:#B5D4F4
- Apply classes: class AUTH,GATEWAY service

## Output
Return ONLY the Mermaid code. No wrapping, no explanation.
Example output format:
flowchart TD
    A[Start] --> B[Process]
    B --> C[End]
"##;

// ============================================================================
// GitHub Repo Fetching
// ============================================================================

/// Fetch repo tree and key source files from GitHub API
async fn fetch_github_context(
    client: &reqwest::Client,
    url: &str,
) -> Result<String> {
    let (owner, repo) = parse_github_url(url)?;
    
    // Optional GitHub token from env (raises rate limit from 60 to 5000/hr)
    let github_token = std::env::var("GITHUB_TOKEN").ok();
    
    // Step 1: Get default branch from repo metadata
    let repo_url = format!("https://api.github.com/repos/{}/{}", owner, repo);
    let mut req = client
        .get(&repo_url)
        .header("User-Agent", "Cyan-App")
        .header("Accept", "application/vnd.github.v3+json");
    if let Some(token) = github_token.as_ref() {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    let repo_resp = req.send().await?;
    
    if !repo_resp.status().is_success() {
        let status = repo_resp.status();
        let body = repo_resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Failed to fetch repo info for {}/{}: {} — {}",
            owner, repo, status,
            if status.as_u16() == 403 { "GitHub API rate limit exceeded. Try again later or add a GitHub token." }
            else if status.as_u16() == 404 { "Repository not found. Check the URL and ensure it's public." }
            else { &body }
        ));
    }
    
    let repo_json: serde_json::Value = repo_resp.json().await?;
    let default_branch = repo_json["default_branch"]
        .as_str()
        .unwrap_or("main")
        .to_string();
    
    // Step 2: Fetch tree using actual default branch
    let tree_url = format!(
        "https://api.github.com/repos/{}/{}/git/trees/{}?recursive=1",
        owner, repo, default_branch
    );
    
    let mut tree_req = client
        .get(&tree_url)
        .header("User-Agent", "Cyan-App")
        .header("Accept", "application/vnd.github.v3+json");
    if let Some(token) = github_token.as_ref() {
        tree_req = tree_req.header("Authorization", format!("Bearer {}", token));
    }
    let tree_resp = tree_req.send().await?;
    
    if !tree_resp.status().is_success() {
        let status = tree_resp.status();
        return Err(anyhow!(
            "Failed to fetch repo tree for {}/{} (branch: {}): {}",
            owner, repo, default_branch, status
        ));
    }
    
    parse_tree_response(client, &owner, &repo, tree_resp).await
}

async fn parse_tree_response(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    resp: reqwest::Response,
) -> Result<String> {
    let tree: serde_json::Value = resp.json().await?;
    
    let entries = tree["tree"]
        .as_array()
        .ok_or_else(|| anyhow!("No tree in response"))?;
    
    // Build directory structure
    let mut structure = String::from("## Repository Structure\n\n");
    let mut source_files: Vec<(String, String)> = Vec::new();
    
    // Filter to source files only
    let source_extensions = ["rs", "swift", "ts", "tsx", "js", "jsx", "py", "go", "java", "kt", "dart"];
    let config_files = ["Cargo.toml", "Package.swift", "package.json", "go.mod", "build.gradle"];
    
    for entry in entries {
        let path = entry["path"].as_str().unwrap_or("");
        let entry_type = entry["type"].as_str().unwrap_or("");
        
        if entry_type == "blob" {
            // Add to structure listing
            let depth = path.matches('/').count();
            if depth <= 3 {
                structure.push_str(&format!("  {}\n", path));
            }
            
            // Fetch key source files (limit to ~10 important ones)
            let is_source = source_extensions.iter().any(|ext| path.ends_with(&format!(".{}", ext)));
            let is_config = config_files.iter().any(|f| path.ends_with(f));
            let is_mod = path.contains("mod.rs") || path.contains("lib.rs") || path.contains("main.rs")
                || path.contains("index.ts") || path.contains("__init__.py");
            
            if ((is_source && is_mod) || is_config)
                && source_files.len() < 10 {
                    // Fetch file content
                    if let Ok(content) = fetch_file_content(client, owner, repo, path).await {
                        source_files.push((path.to_string(), content));
                    }
                }
        }
    }
    
    // Build context string
    let mut context = structure;
    context.push_str("\n## Key Source Files\n\n");
    
    for (path, content) in &source_files {
        // Truncate large files
        let truncated = if content.len() > 2000 {
            format!("{}...\n[truncated, {} total bytes]", &content[..2000], content.len())
        } else {
            content.clone()
        };
        context.push_str(&format!("### {}\n```\n{}\n```\n\n", path, truncated));
    }
    
    Ok(context)
}

async fn fetch_file_content(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    path: &str,
) -> Result<String> {
    let url = format!(
        "https://api.github.com/repos/{}/{}/contents/{}",
        owner, repo, path
    );
    
    let github_token = std::env::var("GITHUB_TOKEN").ok();
    let mut req = client
        .get(&url)
        .header("User-Agent", "Cyan-App")
        .header("Accept", "application/vnd.github.v3.raw");
    if let Some(token) = github_token.as_ref() {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    let resp = req.send().await?;
    
    if resp.status().is_success() {
        Ok(resp.text().await?)
    } else {
        Err(anyhow!("Failed to fetch {}", path))
    }
}

fn parse_github_url(url: &str) -> Result<(String, String)> {
    // Handle: github.com/owner/repo, https://github.com/owner/repo, owner/repo
    let cleaned = url
        .trim()
        .trim_end_matches('/')
        .trim_end_matches(".git");
    
    let parts: Vec<&str> = cleaned.split('/').collect();
    
    // Find owner/repo from the end
    if parts.len() >= 2 {
        let repo = parts[parts.len() - 1].to_string();
        let owner = parts[parts.len() - 2].to_string();
        Ok((owner, repo))
    } else {
        Err(anyhow!("Invalid GitHub URL: {}", url))
    }
}

// ============================================================================
// Diagram Generation
// ============================================================================

pub async fn generate_diagram(
    client: &reqwest::Client,
    api_key: &str,
    request: &DiagramRequest,
    peer_id: &str,
) -> DiagramResult {
    let start = Instant::now();
    
    // Build the user message based on source
    let user_content = match build_user_content(client, &request.source, request.diagram_type).await {
        Ok(content) => content,
        Err(e) => {
            return DiagramResult {
                success: false,
                svg: None,
                mermaid: None,
                prompt_genesis: None,
                error: Some(format!("Failed to prepare request: {}", e)),
                latency_ms: start.elapsed().as_millis() as u64,
            };
        }
    };
    
    // Build Claude API request
    let claude_request = ClaudeRequest {
        model: "claude-sonnet-4-20250514".to_string(),
        max_tokens: 4096,
        system: MERMAID_SYSTEM_PROMPT.to_string(),
        messages: vec![ClaudeMessage {
            role: "user".to_string(),
            content: user_content,
        }],
    };
    
    // Call Claude API
    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&claude_request)
        .send()
        .await;
    
    let latency_ms = start.elapsed().as_millis() as u64;
    
    match response {
        Ok(resp) => {
            if !resp.status().is_success() {
                let status = resp.status();
                let error_text = resp.text().await.unwrap_or_default();
                return DiagramResult {
                    success: false,
                    svg: None,
                    mermaid: None,
                    prompt_genesis: None,
                    error: Some(format!("Claude API error {}: {}", status, error_text)),
                    latency_ms,
                };
            }
            
            match resp.json::<ClaudeResponse>().await {
                Ok(claude_resp) => {
                    let text = claude_resp.content.iter()
                        .filter_map(|c| c.text.as_ref())
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join("\n");
                    
                    // Parse JSON response
                    parse_diagram_response(&text, request, peer_id, latency_ms)
                }
                Err(e) => DiagramResult {
                    success: false,
                    svg: None,
                    mermaid: None,
                    prompt_genesis: None,
                    error: Some(format!("Failed to parse Claude response: {}", e)),
                    latency_ms,
                },
            }
        }
        Err(e) => DiagramResult {
            success: false,
            svg: None,
            mermaid: None,
            prompt_genesis: None,
            error: Some(format!("Request failed: {}", e)),
            latency_ms,
        },
    }
}

async fn build_user_content(
    client: &reqwest::Client,
    source: &DiagramSource,
    diagram_type: DiagramType,
) -> Result<Vec<ClaudeContent>> {
    let type_hint = match diagram_type {
        DiagramType::Flowchart => "Create a flowchart diagram.",
        DiagramType::Sequence => "Create a sequence diagram.",
        DiagramType::ClassDiagram => "Create a class diagram showing types, fields, methods, and relationships.",
        DiagramType::Architecture => "Create an architecture/structural diagram with nested containers.",
        DiagramType::Auto => "Choose the best diagram type for this content.",
    };
    
    match source {
        DiagramSource::Description { prompt } => {
            Ok(vec![ClaudeContent::Text {
                text: format!("{}\n\n{}", type_hint, prompt),
            }])
        }
        
        DiagramSource::Github { url, prompt } => {
            let repo_context = fetch_github_context(client, url).await?;
            let user_prompt = prompt.as_deref().unwrap_or("Generate a diagram from this codebase.");
            Ok(vec![ClaudeContent::Text {
                text: format!(
                    "{}\n\n{}\n\nRepository: {}\n\n{}",
                    type_hint, user_prompt, url, repo_context
                ),
            }])
        }
        
        DiagramSource::Image { image_base64, prompt } => {
            let media_type = if image_base64.starts_with("/9j/") {
                "image/jpeg"
            } else {
                "image/png"
            };
            
            let user_prompt = prompt.as_deref()
                .unwrap_or("Analyze this image and recreate the diagram accurately.");
            
            Ok(vec![
                ClaudeContent::Image {
                    source: ClaudeImageSource {
                        source_type: "base64".to_string(),
                        media_type: media_type.to_string(),
                        data: image_base64.to_string(),
                    },
                },
                ClaudeContent::Text {
                    text: format!("{}\n\n{}", type_hint, user_prompt),
                },
            ])
        }
        
        DiagramSource::Edit { current_mermaid, instruction } => {
            Ok(vec![ClaudeContent::Text {
                text: format!(
                    "Here is an existing diagram in Mermaid syntax:\n\n```mermaid\n{}\n```\n\nModify it according to this instruction: {}\n\n{}",
                    current_mermaid, instruction, type_hint
                ),
            }])
        }
    }
}

fn parse_diagram_response(
    text: &str,
    request: &DiagramRequest,
    peer_id: &str,
    latency_ms: u64,
) -> DiagramResult {
    let cleaned = text.trim();
    
    // Strip markdown code fences if present
    let mermaid = if cleaned.starts_with("```mermaid") {
        let start = cleaned.find('\n').unwrap_or(10) + 1;
        let end = cleaned.rfind("```").unwrap_or(cleaned.len());
        cleaned[start..end].trim().to_string()
    } else if cleaned.starts_with("```") {
        let start = cleaned.find('\n').unwrap_or(3) + 1;
        let end = cleaned.rfind("```").unwrap_or(cleaned.len());
        cleaned[start..end].trim().to_string()
    } else {
        // Raw mermaid text — use as-is
        cleaned.to_string()
    };
    
    if mermaid.is_empty() {
        return DiagramResult {
            success: false,
            svg: None,
            mermaid: None,
            prompt_genesis: None,
            error: Some("Empty mermaid response from Claude".into()),
            latency_ms,
        };
    }
    
    let genesis = build_genesis(request, peer_id);
    
    DiagramResult {
        success: true,
        svg: None,
        mermaid: Some(mermaid),
        prompt_genesis: Some(genesis),
        error: None,
        latency_ms,
    }
}

fn build_genesis(request: &DiagramRequest, peer_id: &str) -> PromptGenesis {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    
    let (original_prompt, source_type, source_ref) = match &request.source {
        DiagramSource::Description { prompt } => {
            (prompt.clone(), "description".to_string(), None)
        }
        DiagramSource::Github { url, prompt } => {
            let p = prompt.as_deref().unwrap_or("Generate diagram from repo").to_string();
            (p, "github".to_string(), Some(url.clone()))
        }
        DiagramSource::Image { prompt, .. } => {
            let p = prompt.as_deref().unwrap_or("Generate diagram from image").to_string();
            (p, "image".to_string(), None)
        }
        DiagramSource::Edit { instruction, .. } => {
            (instruction.clone(), "edit".to_string(), None)
        }
    };
    
    let diagram_type = match request.diagram_type {
        DiagramType::Flowchart => "flowchart",
        DiagramType::Sequence => "sequence",
        DiagramType::ClassDiagram => "class_diagram",
        DiagramType::Architecture => "architecture",
        DiagramType::Auto => "auto",
    };
    
    PromptGenesis {
        original_prompt,
        source_type,
        source_ref,
        diagram_type: diagram_type.to_string(),
        created_by: peer_id.to_string(),
        created_at: now,
        revisions: vec![],
    }
}

fn extract_svg(text: &str) -> Option<String> {
    if let Some(start) = text.find("<svg")
        && let Some(end) = text.find("</svg>") {
            return Some(text[start..end + 6].to_string());
        }
    None
}

fn extract_mermaid(text: &str) -> Option<String> {
    if let Some(start) = text.find("```mermaid") {
        let start = start + 10;
        if let Some(end) = text[start..].find("```") {
            return Some(text[start..start + end].trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_github_url() {
        let (owner, repo) = parse_github_url("https://github.com/block-xaero/cyan-backend").unwrap();
        assert_eq!(owner, "block-xaero");
        assert_eq!(repo, "cyan-backend");
        
        let (owner, repo) = parse_github_url("github.com/block-xaero/xaeroflux").unwrap();
        assert_eq!(owner, "block-xaero");
        assert_eq!(repo, "xaeroflux");
        
        let (owner, repo) = parse_github_url("block-xaero/cyan-backend").unwrap();
        assert_eq!(owner, "block-xaero");
        assert_eq!(repo, "cyan-backend");
    }
    
    #[test]
    fn test_parse_diagram_json() {
        let json = r#"{"mermaid": "flowchart TD\n  A[Start] --> B[End]", "svg": "<svg viewBox=\"0 0 680 200\"><rect/></svg>"}"#;
        let request = DiagramRequest {
            source: DiagramSource::Description { prompt: "test".into() },
            diagram_type: DiagramType::Auto,
            board_id: "b1".into(),
        };
        let result = parse_diagram_response(json, &request, "peer1", 100);
        assert!(result.success);
        assert!(result.svg.is_some());
        assert!(result.mermaid.is_some());
        assert!(result.prompt_genesis.is_some());
    }
}
