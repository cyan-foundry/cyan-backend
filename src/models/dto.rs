// src/models/dto.rs
//
// Data Transfer Objects for storage and network serialization

use crate::models::core::{Group, Workspace};
use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════════
// TREE SNAPSHOT DTO (for UI refresh)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Serialize, Deserialize)]
pub struct TreeSnapshotDTO {
    pub groups: Vec<Group>,
    pub workspaces: Vec<Workspace>,
    pub whiteboards: Vec<WhiteboardDTO>,
    pub files: Vec<FileDTO>,
    pub chats: Vec<ChatDTO>,
    #[serde(default)]
    pub whiteboard_elements: Vec<WhiteboardElementDTO>,
    #[serde(default)]
    pub notebook_cells: Vec<NotebookCellDTO>,
    #[serde(default)]
    pub integrations: Vec<IntegrationBindingDTO>,
    #[serde(default)]
    pub board_metadata: Vec<BoardMetadataDTO>,
}

// ═══════════════════════════════════════════════════════════════════════════
// BOARD METADATA
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BoardMetadataDTO {
    pub board_id: String,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub rating: i32,
    #[serde(default)]
    pub view_count: i32,
    pub contains_model: Option<String>,
    #[serde(default)]
    pub contains_skills: Vec<String>,
    #[serde(default = "default_board_type")]
    pub board_type: String,
    #[serde(default)]
    pub last_accessed: i64,
    #[serde(default)]
    pub is_pinned: bool,
    /// LWW clock for the **descriptive** lane (labels/rating/contains_model/contains_skills/
    /// board_type) — the fields edited together by `UpdateBoardMetadata`. The merge applies
    /// these only when this is strictly newer, so a stale snapshot never clobbers them
    /// (R11 §9/PATTERN — per-field convergent LWW, never whole-record replace).
    #[serde(default)]
    pub meta_updated_at: i64,
    /// LWW clock for the **pin** lane (`is_pinned`) — set independently of the descriptive
    /// fields, so pins from multiple peers MERGE rather than clobber (R11 §9b).
    #[serde(default)]
    pub pin_updated_at: i64,
}

fn default_board_type() -> String {
    "canvas".to_string()
}

// ═══════════════════════════════════════════════════════════════════════════
// INTEGRATION BINDING
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationBindingDTO {
    pub id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub integration_type: String,
    #[serde(default)]
    pub config: serde_json::Value,
    pub created_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// WHITEBOARD (Board shell)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhiteboardDTO {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub created_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// FILE
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDTO {
    pub id: String,
    pub group_id: Option<String>,
    pub workspace_id: Option<String>,
    pub board_id: Option<String>,
    pub name: String,
    pub hash: String,
    pub size: u64,
    pub source_peer: Option<String>,
    pub local_path: Option<String>,
    pub created_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// CHAT
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatDTO {
    pub id: String,
    /// The board this chat belongs to (R11 §1 — chat is board-scoped). `#[serde(default)]`
    /// so a snapshot from a pre-R11 peer (no `board_id`) still deserializes; the migration
    /// and apply path back-fill it.
    #[serde(default)]
    pub board_id: String,
    pub workspace_id: String,
    pub message: String,
    pub author: String,
    pub parent_id: Option<String>,
    pub timestamp: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// NOTE (ROUND8 §W2 — board-level, authored, LWW ledger)
// ═══════════════════════════════════════════════════════════════════════════

/// A board-level, authored note. Its own store + own sync stream — NOT a notebook
/// cell. Editable; conflict resolution is LWW on `updated_at`; the store upserts by
/// `id` (idempotent, so snapshot apply / anti-entropy repair converge without churn).
/// `author_name` is resolved from the author's XaeroID profile at authoring time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteDTO {
    pub id: String,
    pub board_id: String,
    pub tenant_id: String,
    pub author_id: String,
    pub author_name: String,
    pub text: String,
    pub created_at: i64,
    pub updated_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// TEMPLATE (ROUND8 §W4 — a pre-written English workflow cloned into a board)
// ═══════════════════════════════════════════════════════════════════════════

/// One pre-written workflow step inside a template: the plain-English step `text`
/// (the W1 authoring primitive) plus an optional **bound plugin** (the `@plugin` the
/// step runs on). Cloning a template materializes one real `step` cell per step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateStep {
    pub text: String,
    /// The plugin bound to this step (e.g. `"contido"`), if any. `None` ⇒ unbound.
    #[serde(default)]
    pub plugin: Option<String>,
}

/// A workflow **template** — a pre-written English workflow (steps + bound plugins)
/// you clone into a board. Two sources: built-in **media seeds** (`source =
/// "builtin"`, tenant-agnostic) and user **save-as-template** results (`source =
/// "user"`, tenant-scoped). Cloning never mutates the template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Template {
    pub id: String,
    /// The tenant (group) that owns a user template. Empty for built-in seeds, which
    /// are global defaults surfaced to every tenant.
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// `"builtin"` (seed) or `"user"` (save-as-template).
    pub source: String,
    pub steps: Vec<TemplateStep>,
    pub created_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// PIN (ROUND8 §W4 — board-level pinned-workflow state; replicated, LWW)
// ═══════════════════════════════════════════════════════════════════════════

/// Pinned-workflow state for a board: a pinned workflow surfaces for fast cloning.
/// Its own store (NOT board_metadata), so it rides the existing anti-entropy digest +
/// snapshot path exactly like a note. Replicated team state; conflict resolution is
/// **LWW on `updated_at`**, idempotent upsert-by-`board_id`, so unpin/pin converge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinDTO {
    pub board_id: String,
    pub tenant_id: String,
    pub pinned: bool,
    pub updated_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// WHITEBOARD ELEMENT
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhiteboardElementDTO {
    pub id: String,
    pub board_id: String,
    pub element_type: String,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    pub z_index: i32,
    pub style_json: Option<String>,
    pub content_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// NOTEBOOK CELL
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotebookCellDTO {
    pub id: String,
    pub board_id: String,
    pub cell_type: String,
    pub cell_order: i32,
    pub content: Option<String>,
    pub output: Option<String>,
    #[serde(default)]
    pub collapsed: bool,
    pub height: Option<f64>,
    pub metadata_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}