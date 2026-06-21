// cyan-backend/src/lens_commands.rs
//
// Command parser for CyanLens chat.
// Parses slash commands like /summarize g\Sales\Workspace 1
// Resolves paths to IDs from local SQLite.
// Extracts text from files (PDF, TXT, MD, DOCX).
//
// Used by both iOS and Flutter via FFI.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::storage;
use rusqlite::Connection;

// ============================================================================
// Path Types
// ============================================================================

/// A hierarchical path like g\Sales\Workspace 1\Board A\file.pdf
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CyanPath {
    Group { group: String },
    Workspace { group: String, workspace: String },
    Board { group: String, workspace: String, board: String },
    File { group: String, workspace: String, board: String, file: String },
}

/// Resolved path with actual IDs from SQLite
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolvedPath {
    Group { group_id: String, group_name: String },
    Workspace { group_id: String, workspace_id: String, workspace_name: String },
    Board { group_id: String, workspace_id: String, board_id: String, board_name: String },
    File { group_id: String, workspace_id: String, board_id: String, file_name: String, file_path: Option<String> },
}

// ============================================================================
// Command Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LensCommand {
    /// Summarize a scope: /summarize g\Sales\Workspace 1
    Summarize { path: CyanPath },
    
    /// Summarize a file (extract text first): /summarize file g\Sales\...\report.pdf
    SummarizeFile { path: CyanPath },
    
    /// Pin last summary as a board: /pin
    Pin,
    
    /// Search for term in scope: /grep "OAuth" g\Engineering
    Grep { term: String, path: CyanPath },
    
    /// Status report: /status or /status g\Sales
    Status { path: Option<CyanPath> },
    
    /// Quick pulse: /pulse or /pulse g\Sales\Workspace 1
    Pulse { path: Option<CyanPath> },
    
    /// Show help: /help
    Help,
    
    /// Import from external service:
    /// /import jira g\Sales\Workspace 1           → list projects, set scope
    /// /import jira AUTH g\Sales\Workspace 1       → import project into workspace
    /// /import jira all g\Sales\Workspace 1        → import all into workspace
    Import { source: String, target: Option<String>, path: Option<CyanPath> },
    
    /// Pipeline commands:
    /// /pipeline compile g\Sales\Workspace 1\Board  → compile English steps to configs
    /// /pipeline run g\Sales\Workspace 1\Board      → execute pipeline DAG
    /// /pipeline status g\Sales\Workspace 1\Board   → query pipeline state
    /// /pipeline approve step_id g\...\Board        → human approves a step
    /// /pipeline export g\Sales\Workspace 1\Board   → export as Airflow DAG
    Pipeline { action: String, step_id: Option<String>, path: Option<CyanPath> },
    
    /// Natural language (not a command): forward to query/summarize intent
    NaturalLanguage { text: String },
}

/// Result of executing a command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    pub success: bool,
    pub command: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

// ============================================================================
// Parser
// ============================================================================

pub fn parse_command(input: &str) -> LensCommand {
    let trimmed = input.trim();
    
    // Not a command — natural language
    if !trimmed.starts_with('/') {
        return LensCommand::NaturalLanguage { text: trimmed.to_string() };
    }
    
    // Split into command and args
    let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
    let cmd = parts[0].to_lowercase();
    let args = parts.get(1).map(|s| s.trim()).unwrap_or("");
    
    match cmd.as_str() {
        "/summarize" | "/sum" | "/s" => {
            if args.is_empty() {
                return LensCommand::NaturalLanguage { text: "summarize".to_string() };
            }
            
            // Check if it's a file summarize: /summarize file g\...\report.pdf
            if args.starts_with("file ") || args.starts_with("file\t") {
                let file_path = args.strip_prefix("file").unwrap_or("").trim();
                match parse_path(file_path) {
                    Ok(path) => LensCommand::SummarizeFile { path },
                    Err(_) => LensCommand::NaturalLanguage { text: trimmed.to_string() },
                }
            } else {
                match parse_path(args) {
                    Ok(path) => LensCommand::Summarize { path },
                    Err(_) => {
                        // Fall back to natural language summarize
                        LensCommand::NaturalLanguage { text: format!("summarize {}", args) }
                    }
                }
            }
        }
        
        "/pin" | "/p" => LensCommand::Pin,
        
        "/grep" | "/search" | "/find" => {
            // /grep "term" g\path or /grep term g\path
            match parse_grep_args(args) {
                Ok((term, path)) => LensCommand::Grep { term, path },
                Err(_) => LensCommand::NaturalLanguage { text: trimmed.to_string() },
            }
        }
        
        "/status" | "/st" => {
            if args.is_empty() {
                LensCommand::Status { path: None }
            } else {
                match parse_path(args) {
                    Ok(path) => LensCommand::Status { path: Some(path) },
                    Err(_) => LensCommand::Status { path: None },
                }
            }
        }
        
        "/pulse" | "/pl" => {
            if args.is_empty() {
                LensCommand::Pulse { path: None }
            } else {
                match parse_path(args) {
                    Ok(path) => LensCommand::Pulse { path: Some(path) },
                    Err(_) => LensCommand::Pulse { path: None },
                }
            }
        }
        
        "/import" | "/i" => {
            if args.is_empty() {
                return LensCommand::Import { source: String::new(), target: None, path: None };
            }
            
            // Split args: source [target] path
            // /import jira g\Sales\Workspace       → source=jira, target=None, path=g\Sales\Workspace
            // /import jira AUTH g\Sales\Workspace   → source=jira, target=AUTH, path=g\Sales\Workspace
            // /import jira                          → source=jira, target=None, path=None
            
            let parts: Vec<&str> = args.splitn(2, ' ').collect();
            let source = parts[0].to_lowercase();
            let rest = parts.get(1).map(|s| s.trim()).unwrap_or("");
            
            if rest.is_empty() {
                // Just source: /import jira
                LensCommand::Import { source, target: None, path: None }
            } else if rest.starts_with("g\\") || rest.starts_with("g/") {
                // Source + path: /import jira g\Sales\Workspace
                let path = parse_path(rest).ok();
                LensCommand::Import { source, target: None, path }
            } else {
                // Source + target + maybe path: /import jira AUTH g\Sales\Workspace
                let target_parts: Vec<&str> = rest.splitn(2, ' ').collect();
                let target = Some(target_parts[0].to_string());
                let path = target_parts.get(1)
                    .and_then(|p| parse_path(p.trim()).ok());
                LensCommand::Import { source, target, path }
            }
        }
        
        "/help" | "/h" | "/?" => LensCommand::Help,
        
        "/pipeline" | "/pipe" | "/pl" => {
            
            if args.is_empty() {
                return LensCommand::Pipeline { action: "help".to_string(), step_id: None, path: None };
            }
            
            let parts: Vec<&str> = args.splitn(3, ' ').collect();
            let action = parts[0].to_lowercase();
            
            match action.as_str() {
                "compile" | "run" | "status" | "export" => {
                    // /pipeline compile g\...\Board
                    let path = if parts.len() > 1 {
                        parse_path(parts[1..].join(" ").trim()).ok()
                    } else {
                        None
                    };
                    LensCommand::Pipeline { action, step_id: None, path }
                }
                "approve" | "reject" | "retry" => {
                    // /pipeline approve step_id g\...\Board
                    let step_id = parts.get(1).map(|s| s.to_string());
                    let path = if parts.len() > 2 {
                        parse_path(parts[2]).ok()
                    } else {
                        None
                    };
                    LensCommand::Pipeline { action, step_id, path }
                }
                _ => {
                    // Treat as path: /pipeline g\...\Board → status
                    match parse_path(args) {
                        Ok(path) => LensCommand::Pipeline { action: "status".to_string(), step_id: None, path: Some(path) },
                        Err(_) => LensCommand::Pipeline { action: "help".to_string(), step_id: None, path: None },
                    }
                }
            }
        }
        
        _ => LensCommand::NaturalLanguage { text: trimmed.to_string() },
    }
}

/// Parse a backslash-separated path: g\Sales\Workspace 1\Board A
pub fn parse_path(input: &str) -> Result<CyanPath> {
    let trimmed = input.trim();
    
    // Must start with g\ (group prefix)
    if !trimmed.starts_with("g\\") && !trimmed.starts_with("g/") {
        return Err(anyhow!("Path must start with g\\ (e.g., g\\Sales\\Workspace 1)"));
    }
    
    // Split on backslash (also accept forward slash for convenience)
    let parts: Vec<&str> = trimmed[2..] // skip "g\"
        .split(|c| c == '\\' || c == '/')
        .filter(|s| !s.is_empty())
        .collect();
    
    match parts.len() {
        0 => Err(anyhow!("Empty path after g\\")),
        1 => Ok(CyanPath::Group { group: parts[0].to_string() }),
        2 => Ok(CyanPath::Workspace {
            group: parts[0].to_string(),
            workspace: parts[1].to_string(),
        }),
        3 => {
            // Could be a board or a file at board level
            let last = parts[2];
            if looks_like_file(last) {
                // g\Group\Workspace\file.pdf — file in workspace (no board)
                Ok(CyanPath::File {
                    group: parts[0].to_string(),
                    workspace: parts[1].to_string(),
                    board: String::new(),
                    file: last.to_string(),
                })
            } else {
                Ok(CyanPath::Board {
                    group: parts[0].to_string(),
                    workspace: parts[1].to_string(),
                    board: last.to_string(),
                })
            }
        }
        4 => Ok(CyanPath::File {
            group: parts[0].to_string(),
            workspace: parts[1].to_string(),
            board: parts[2].to_string(),
            file: parts[3].to_string(),
        }),
        _ => Err(anyhow!("Path too deep — max: g\\group\\workspace\\board\\file")),
    }
}

/// Parse grep args: "term" g\path or term g\path
fn parse_grep_args(input: &str) -> Result<(String, CyanPath)> {
    let trimmed = input.trim();
    
    if trimmed.starts_with('"') {
        // Quoted term: "OAuth implementation" g\Engineering
        if let Some(end_quote) = trimmed[1..].find('"') {
            let term = trimmed[1..=end_quote].to_string();
            let rest = trimmed[end_quote + 2..].trim();
            let path = parse_path(rest)?;
            return Ok((term, path));
        }
    }
    
    // Unquoted: first word is term, rest is path
    let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
    if parts.len() < 2 {
        return Err(anyhow!("Usage: /grep <term> g\\path"));
    }
    
    let term = parts[0].to_string();
    let path = parse_path(parts[1])?;
    Ok((term, path))
}

fn looks_like_file(name: &str) -> bool {
    let extensions = [
        ".pdf", ".txt", ".md", ".docx", ".doc", ".csv",
        ".json", ".xml", ".html", ".rtf", ".xlsx", ".pptx",
        ".png", ".jpg", ".jpeg", ".gif", ".heic", ".svg",
    ];
    let lower = name.to_lowercase();
    extensions.iter().any(|ext| lower.ends_with(ext))
}

// ============================================================================
// Path Resolution (local SQLite)
// ============================================================================

pub fn resolve_path(path: &CyanPath) -> Result<ResolvedPath> {
    let conn = storage::db().lock().map_err(|e| anyhow!("DB lock: {}", e))?;
    
    match path {
        CyanPath::Group { group } => {
            let (id, name) = resolve_group_by_name(&conn, group)?;
            Ok(ResolvedPath::Group { group_id: id, group_name: name })
        }
        CyanPath::Workspace { group, workspace } => {
            let (gid, _) = resolve_group_by_name(&conn, group)?;
            let (wid, wname) = resolve_workspace_by_name(&conn, &gid, workspace)?;
            Ok(ResolvedPath::Workspace {
                group_id: gid,
                workspace_id: wid,
                workspace_name: wname,
            })
        }
        CyanPath::Board { group, workspace, board } => {
            let (gid, _) = resolve_group_by_name(&conn, group)?;
            let (wid, _) = resolve_workspace_by_name(&conn, &gid, workspace)?;
            let (bid, bname) = resolve_board_by_name(&conn, &wid, board)?;
            Ok(ResolvedPath::Board {
                group_id: gid,
                workspace_id: wid,
                board_id: bid,
                board_name: bname,
            })
        }
        CyanPath::File { group, workspace, board, file } => {
            let (gid, _) = resolve_group_by_name(&conn, group)?;
            let (wid, _) = resolve_workspace_by_name(&conn, &gid, workspace)?;
            let bid = if board.is_empty() {
                String::new()
            } else {
                resolve_board_by_name(&conn, &wid, board)?.0
            };
            // Find file by name in the scope
            let file_path = resolve_file_by_name(&conn, &wid, &bid, file)?;
            Ok(ResolvedPath::File {
                group_id: gid,
                workspace_id: wid,
                board_id: bid,
                file_name: file.clone(),
                file_path: Some(file_path),
            })
        }
    }
}

fn resolve_group_by_name(conn: &Connection, name: &str) -> Result<(String, String)> {
    conn.query_row(
        "SELECT id, name FROM groups WHERE name = ?1 COLLATE NOCASE",
        rusqlite::params![name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|_| anyhow!("Group '{}' not found", name))
}

fn resolve_workspace_by_name(conn: &Connection, group_id: &str, name: &str) -> Result<(String, String)> {
    conn.query_row(
        "SELECT id, name FROM workspaces WHERE group_id = ?1 AND name = ?2 COLLATE NOCASE",
        rusqlite::params![group_id, name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|_| anyhow!("Workspace '{}' not found", name))
}

fn resolve_board_by_name(conn: &Connection, workspace_id: &str, name: &str) -> Result<(String, String)> {
    conn.query_row(
        "SELECT id, name FROM objects WHERE workspace_id = ?1 AND name = ?2 COLLATE NOCASE",
        rusqlite::params![workspace_id, name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|_| anyhow!("Board '{}' not found", name))
}

fn resolve_file_by_name(conn: &Connection, workspace_id: &str, board_id: &str, name: &str) -> Result<String> {
    // Try board-scoped first
    if !board_id.is_empty() {
        if let Ok(path) = conn.query_row(
            "SELECT local_path FROM objects WHERE board_id = ?1 AND name = ?2 COLLATE NOCASE AND type = 'file'",
            rusqlite::params![board_id, name],
            |row| row.get::<_, Option<String>>(0),
        ) {
            if let Some(p) = path {
                return Ok(p);
            }
        }
    }
    
    // Fallback: workspace-scoped
    conn.query_row(
        "SELECT local_path FROM objects WHERE workspace_id = ?1 AND name = ?2 COLLATE NOCASE AND type = 'file'",
        rusqlite::params![workspace_id, name],
        |row| row.get::<_, Option<String>>(0),
    )
    .map_err(|_| anyhow!("File '{}' not found", name))?
    .ok_or_else(|| anyhow!("File '{}' has no local path", name))
}

// ============================================================================
// File Text Extraction
// ============================================================================

/// Extract text from a file based on its extension.
/// Pure Rust — works on all platforms.
pub fn extract_text_from_file(path: &str) -> Result<String> {
    let lower = path.to_lowercase();
    let bytes = std::fs::read(path)
        .map_err(|e| anyhow!("Cannot read file '{}': {}", path, e))?;
    
    // Try by extension first
    if lower.ends_with(".txt") || lower.ends_with(".md") || lower.ends_with(".csv")
        || lower.ends_with(".json") || lower.ends_with(".xml") || lower.ends_with(".html")
        || lower.ends_with(".rtf") || lower.ends_with(".rs") || lower.ends_with(".swift")
        || lower.ends_with(".py") || lower.ends_with(".js") || lower.ends_with(".ts")
    {
        return String::from_utf8(bytes)
            .map_err(|e| anyhow!("File is not valid UTF-8: {}", e));
    }
    
    if lower.ends_with(".pdf") {
        return extract_text_from_pdf(&bytes);
    }
    
    // No known extension — detect format from magic bytes
    match detect_format_from_bytes(&bytes) {
        Some("pdf") => extract_text_from_pdf(&bytes),
        Some("text") => String::from_utf8(bytes)
            .map_err(|e| anyhow!("File is not valid UTF-8: {}", e)),
        _ => Err(anyhow!("Unsupported file format: {}", path)),
    }
}

/// Detect file format from magic bytes
fn detect_format_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() < 4 {
        return None;
    }
    
    // PDF: starts with %PDF
    if bytes.starts_with(b"%PDF") {
        return Some("pdf");
    }
    
    // DOCX/ZIP: starts with PK
    if bytes.starts_with(b"PK") {
        return Some("docx");
    }
    
    // PNG
    if bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47]) {
        return Some("image");
    }
    
    // JPEG
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("image");
    }
    
    // Try as UTF-8 text (check first 1KB)
    let check_len = bytes.len().min(1024);
    if std::str::from_utf8(&bytes[..check_len]).is_ok() {
        return Some("text");
    }
    
    None
}

/// Extract text from a PDF using pdf-extract crate
fn extract_text_from_pdf(bytes: &[u8]) -> Result<String> {
    pdf_extract::extract_text_from_mem(bytes)
        .map_err(|e| anyhow!("PDF extraction failed: {}", e))
}

/// Truncate text to fit within a token budget (rough estimate: 4 chars ≈ 1 token)
pub fn truncate_to_token_budget(text: &str, max_tokens: usize) -> String {
    let max_chars = max_tokens * 4;
    if text.len() <= max_chars {
        text.to_string()
    } else {
        let truncated = &text[..max_chars];
        format!("{}\n\n[... truncated to ~{} tokens]", truncated, max_tokens)
    }
}

// ============================================================================
// Help Text
// ============================================================================

pub fn help_text() -> String {
    r#"CyanLens Commands:

  /summarize g\Group\Workspace\Board    Summarize a scope
  /summarize file g\...\file.pdf        Extract and summarize a file
  /grep "term" g\Group\Workspace        Search for term in scope
  /pin                                  Pin last summary as a board
  /status                               Status report (current scope)
  /status g\Group                       Status report for specific scope
  /pulse                                Quick pulse (current scope)
  /import jira                          List Jira projects
  /import jira AUTH                     Import Jira project as boards
  /import jira all                      Import all Jira projects
  /import confluence                    List Confluence spaces
  /import confluence ENG                Import Confluence space as boards
  /import gdocs                         List Google Docs
  /import gdocs all                     Import all docs as boards
  /pipeline compile g\...\Board          Compile steps to pipeline config
  /pipeline run g\...\Board              Execute pipeline DAG
  /pipeline status g\...\Board           Show pipeline state
  /pipeline approve step_id             Approve a pipeline step
  /pipeline export g\...\Board           Export as Airflow DAG
  /help                                 Show this help

Path format: g\GroupName\WorkspaceName\BoardName\file.ext
Shortcuts: /s = /summarize, /st = /status, /pl = /pulse, /p = /pin, /i = /import, /pipe = /pipeline"#
        .to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_path_group() {
        let path = parse_path(r"g\Sales").unwrap();
        assert!(matches!(path, CyanPath::Group { group } if group == "Sales"));
    }
    
    #[test]
    fn test_parse_path_workspace() {
        let path = parse_path(r"g\Sales\Sales Workspace 1").unwrap();
        assert!(matches!(path, CyanPath::Workspace { group, workspace }
            if group == "Sales" && workspace == "Sales Workspace 1"));
    }
    
    #[test]
    fn test_parse_path_board() {
        let path = parse_path(r"g\Sales\Sales Workspace 1\Sprint Board").unwrap();
        assert!(matches!(path, CyanPath::Board { group, workspace, board }
            if group == "Sales" && workspace == "Sales Workspace 1" && board == "Sprint Board"));
    }
    
    #[test]
    fn test_parse_path_file() {
        let path = parse_path(r"g\Sales\Workspace 1\Board A\report.pdf").unwrap();
        assert!(matches!(path, CyanPath::File { file, .. } if file == "report.pdf"));
    }
    
    #[test]
    fn test_parse_path_forward_slash_also_works() {
        let path = parse_path("g/Sales/Workspace 1").unwrap();
        assert!(matches!(path, CyanPath::Workspace { group, workspace }
            if group == "Sales" && workspace == "Workspace 1"));
    }
    
    #[test]
    fn test_parse_command_summarize() {
        let cmd = parse_command(r"/summarize g\Sales\Workspace 1");
        assert!(matches!(cmd, LensCommand::Summarize { .. }));
    }
    
    #[test]
    fn test_parse_command_summarize_file() {
        let cmd = parse_command(r"/summarize file g\Sales\Workspace 1\report.pdf");
        assert!(matches!(cmd, LensCommand::SummarizeFile { .. }));
    }
    
    #[test]
    fn test_parse_command_pin() {
        let cmd = parse_command("/pin");
        assert!(matches!(cmd, LensCommand::Pin));
    }
    
    #[test]
    fn test_parse_command_grep() {
        let cmd = parse_command(r#"/grep "OAuth" g\Engineering"#);
        assert!(matches!(cmd, LensCommand::Grep { term, .. } if term == "OAuth"));
    }
    
    #[test]
    fn test_parse_command_natural_language() {
        let cmd = parse_command("what happened in the sales meeting?");
        assert!(matches!(cmd, LensCommand::NaturalLanguage { .. }));
    }
    
    #[test]
    fn test_parse_command_help() {
        let cmd = parse_command("/help");
        assert!(matches!(cmd, LensCommand::Help));
    }
    
    #[test]
    fn test_truncate() {
        let text = "a".repeat(20000);
        let truncated = truncate_to_token_budget(&text, 1000);
        assert!(truncated.len() < 5000);
        assert!(truncated.contains("truncated"));
    }
}
