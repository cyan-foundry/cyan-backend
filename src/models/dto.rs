// src/models/dto.rs
//
// Data Transfer Objects for storage and network serialization

use serde::{Deserialize, Serialize};

use crate::models::core::{Group, Workspace};

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
    /// CHAT C1 (Anchored Lane, additive): `"step"` | `"board"`; absent ⇒ `#board`.
    /// Serde defaults keep pre-C1 snapshots/rows decoding unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_kind: Option<String>,
    /// CHAT C1: the stable `step_uid` when `anchor_kind == "step"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_id: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════════
// NOTE (ROUND8 §W2 — board-level, authored, LWW ledger)
// ═══════════════════════════════════════════════════════════════════════════

/// The closed note SCOPE vocabulary (feat/notes-constitution + LENS_AI_NOTES P1 +
/// A1 structured notes). `board_id` doubles as the scope ANCHOR: the board id for
/// `board`, the group id for `group`, the tenant id for `tenant` (tenant == group id
/// in this engine), the workflow id for `workflow`, the producer id for `producer`,
/// the user id for `user`, the **workspace id** for `project` (A2 populates its
/// chain link), and the **GROUP id** for `role` (the craft slug rides the anchor
/// pair `anchor_kind:"role"` / `anchor_id:"<PRODUCTION_ROLE_VOCAB slug>"` — a slug
/// anchor would be replication-dead AND a cross-tenant shared key). The merge chain
/// runs tenant → group → project → board → workflow → producer → role → user,
/// most-specific LAST. `user` is SOVEREIGN: local-only, never gossiped or snapshot.
pub const NOTE_SCOPE_VOCAB: [&str; 8] =
    ["tenant", "group", "board", "workflow", "producer", "user", "project", "role"];
/// The closed note KIND vocabulary. `constitution` + `preference` feed the merge
/// resolver (→ `ProposeCtx.constitution` / `.preferences`); `editor-note` is the
/// pre-existing board-note behavior; `decision` (CHAT C7) is a board decision
/// promoted from the chat lane — a local ledger row, offline-capable; `creative-dna`
/// (LENS_AI_NOTES P1) carries producer/house/director/studio/genre/feel/episodic
/// material at any scope and rides the constitution rail as a labeled subsection.
///
/// A1 structured notes (additive, one edit 5→13): `creative-brief`, `shot-log` (one
/// note = one entry), `lined-script`, `continuity`, `script` (whole script,
/// .fdx-import-ready), `legal-clearance` (first-class; payload REQUIRED — the one
/// exception, `note_payload` §4.9), and the two OFF-RAIL operational RECORDS (like
/// `legal-clearance`, records not rules — never on the constitution rail, never
/// promotable): `turnover` (the AE orchestration-bridge handoff record, §4.10) and
/// `qc-report` (QC-against-house-rules result record, §4.11). Per-kind payload
/// schemas + defaults live in `crate::note_payload`.
pub const NOTE_KIND_VOCAB: [&str; 13] = [
    "constitution",
    "preference",
    "editor-note",
    "decision",
    "creative-dna",
    "creative-brief",
    "shot-log",
    "lined-script",
    "continuity",
    "script",
    "legal-clearance",
    "turnover",
    "qc-report",
];

pub fn note_scope_valid(s: &str) -> bool {
    NOTE_SCOPE_VOCAB.contains(&s)
}

pub fn note_kind_valid(k: &str) -> bool {
    NOTE_KIND_VOCAB.contains(&k)
}

/// The closed ANCHOR-KIND vocabulary (LENS_AI_NOTES P4, unified): what a chat
/// message or note anchors to WITHIN a board. `step` → a stable `step_uid`;
/// `board` → the board's general slot; `run` → a `workflow_run.run_id`; `frame` →
/// `"<asset>@<master_frame>"` (an opaque string — the engine never parses it);
/// `scene` (A1) → a `scene_id` (`note_payload::scene_id`, opaque to the engine —
/// the `frame` precedent); `role` (A1) → a `PRODUCTION_ROLE_VOCAB` slug, VALID
/// ONLY on `scope == "role"` (the pair is reserved — `dispatch_put_note` rejects
/// `role_anchor_invalid` in every other combination).
/// Anchors stay a free (kind, id) pair on the wire (pair-normalization in SendChat
/// is unchanged); this vocab validates where notes are PUT.
pub const ANCHOR_KIND_VOCAB: [&str; 6] = ["step", "board", "run", "frame", "scene", "role"];

pub fn anchor_kind_valid(k: &str) -> bool {
    ANCHOR_KIND_VOCAB.contains(&k)
}

/// The SINGLE craft-role vocabulary (A1 owns the const's home; A3 owns the VALUES
/// and consumes it via `use crate::models::dto::PRODUCTION_ROLE_VOCAB` — never a
/// second const). The array ORDER below is CANONICAL program-wide (cyan-identity
/// mirrors this exact ordered literal); value OR order drift is a test failure
/// (T8b). Slugs are the only valid `anchor_id` values for `anchor_kind:"role"`,
/// and the craft half of `author_role`. Orthogonal to org-RBAC: a `producer` may
/// hold org role `member`.
pub const PRODUCTION_ROLE_VOCAB: [&str; 7] =
    ["producer", "assistant_editor", "editor", "director", "colorist", "sound", "studio_exec"];

/// Authorship-provenance ONLY: `agent` marks a note written WITHOUT a per-note
/// human confirm (a true agent run / an explicit auto-accept policy). NOT a
/// selector role, NOT a valid role-scope anchor slug — `author_role_valid` is the
/// union, `production_role_valid` is not.
pub const AUTHOR_ROLE_EXTRA: [&str; 1] = ["agent"];

/// Is `r` a craft-role slug (selector roles + role-scope anchor slugs)?
pub fn production_role_valid(r: &str) -> bool {
    PRODUCTION_ROLE_VOCAB.contains(&r)
}

/// Is `r` a valid `author_role` (craft slugs ∪ `agent`)? Invalid/empty values are
/// COERCED to `None` at the write door — provenance never blocks a write (but a
/// coerced-None role then fails any legal transition needing `"producer"`).
pub fn author_role_valid(r: &str) -> bool {
    production_role_valid(r) || AUTHOR_ROLE_EXTRA.contains(&r)
}

pub fn default_note_scope() -> String {
    "board".to_string()
}

pub fn default_note_kind() -> String {
    "editor-note".to_string()
}

/// A board-level, authored note. Its own store + own sync stream — NOT a notebook
/// cell. Editable; conflict resolution is LWW on `updated_at`; the store upserts by
/// `id` (idempotent, so snapshot apply / anti-entropy repair converge without churn).
/// `author_name` is resolved from the author's XaeroID profile at authoring time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteDTO {
    pub id: String,
    /// The scope ANCHOR (see `NOTE_SCOPE_VOCAB`): board id / group id / tenant id.
    pub board_id: String,
    pub tenant_id: String,
    pub author_id: String,
    pub author_name: String,
    pub text: String,
    pub created_at: i64,
    pub updated_at: i64,
    /// `tenant` | `group` | `board`. Defaults keep pre-scope peers/rows wire- and
    /// DB-compatible: a payload or row without it is a plain board note.
    #[serde(default = "default_note_scope")]
    pub scope: String,
    /// `constitution` | `preference` | `editor-note` | `decision`. Same compat
    /// contract as `scope`.
    #[serde(default = "default_note_kind")]
    pub kind: String,
    /// CHAT C7 (additive): note anchor — `"step"` | `"board"`; absent ⇒ unanchored
    /// (every pre-C7 note). Distinct from the scope-anchor `board_id` above: this is
    /// the WITHIN-board anchor (a step), that is the scope key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_kind: Option<String>,
    /// CHAT C7: the stable `step_uid` when `anchor_kind == "step"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_id: Option<String>,
    /// CHAT C7: provenance — `chat:<message_id>` for a note promoted from chat.
    /// A1 grammar v2 (`note_payload` module docs): `<lane>:<opaque>` — the engine
    /// never parses past the first `:`; unknown prefixes are legal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_ref: Option<String>,
    /// A1 (additive): per-kind typed payload (`note_payload` §4 schemas). Absent ⇒
    /// a plain freeform note of its kind (exception: kind `legal-clearance`
    /// REQUIRES it, §4.9). SQLite column `payload_json TEXT` (nullable — a pre-A1
    /// row reads back `None`, never `Some(json!({}))`); read-back parse failure ⇒
    /// `None` + warn, row still returned (GC-3 / TR-1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    /// A1 (additive): the author's CRAFT role at authoring time. Provenance, NOT
    /// authz. Valid set = `PRODUCTION_ROLE_VOCAB` ∪ `AUTHOR_ROLE_EXTRA`; invalid ⇒
    /// COERCED `None` at the write door (never a reject). Column `author_role TEXT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_role: Option<String>,
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
    /// A3 (additive): the display stage this step belongs to — **DISPLAY-ONLY**.
    /// Never cloned into cells, never read by compile (the real `infer_step`
    /// derives stage from cell text and accepts no hint); stripped when a step
    /// is serialized into a lens `GenTemplateStep`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
}

/// A3 (additive): one plugin a roletype template names. `status` ∈
/// `role_templates::PLUGIN_STATUS_VOCAB` (`live`|`roadmap` — exists-or-doesn't,
/// a DIFFERENT axis from task status); `execution` ∈ `PLUGIN_EXECUTION_VOCAB`
/// (`device`|`cloud`|`both`); `flagship_tool` = the ONE tool whose installed
/// presence marks the plugin installed (bare tool name, `resolve_installed_tool`
/// verifies the OWNING plugin id matches).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplatePlugin {
    pub id: String,
    pub status: String,
    pub execution: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flagship_tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<String>,
}

/// A workflow **template** — a pre-written English workflow (steps + bound plugins)
/// you clone into a board. Two sources: built-in **media seeds** (`source =
/// "builtin"`, tenant-agnostic) and user **save-as-template** results (`source =
/// "user"`, tenant-scoped). Cloning never mutates the template.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    // ── A3 roletype extension (all additive, serde defaults; a pre-A3 template
    //    decodes with every field at its default — T39) ──
    /// `role_templates::FORMAT_TYPE_VOCAB` member; `None` = a legacy template
    /// (in `list_templates`, NEVER in the selector — T50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_type: Option<String>,
    /// Display shape: the stage rail the selector UI renders.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<String>,
    /// The note kinds this format works from — every entry ⊆ `NOTE_KIND_VOCAB`
    /// **by const reference** (13 at REV 2; save validation tracks A1's vocab
    /// automatically if it grows again).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub note_kinds: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<TemplatePlugin>,
    /// `role_templates::TEMPLATE_MATURITY_VOCAB` (`mvp`|`extensible`); all five
    /// builtins ship `"mvp"` (D-A3.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maturity: Option<String>,
    /// `role_templates::ROLETYPE_CATALOG_VERSION` at save time; a NULL-column
    /// user row is a legacy (pre-A3) template and never enters the selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
    /// `"tenant"` live; `"user"`/`"group"` stored-but-treated-as-tenant (roadmap).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
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
// WORKFLOW LIFECYCLE STATE (R12 D2/E1)
// ═══════════════════════════════════════════════════════════════════════════

/// Per-board workflow lifecycle state — the engine-side support state iOS gates the board
/// face on (R12 D2/E1). `deployed` + `dashboard_available` let the app surface the running
/// DASHBOARD instead of the editor; `locked` (set on deploy) means edits are frozen and an
/// UNLOCK requires an org-XaeroID grant (W17). Defaults (no row) = authoring/editable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStateDTO {
    pub board_id: String,
    /// The workflow has been deployed (it is running / live), not just authored.
    pub deployed: bool,
    /// A live dashboard exists for this workflow → iOS shows the dashboard, not the editor.
    pub dashboard_available: bool,
    /// Edits are locked (set on deploy). Unlocking mid-flight requires an org grant.
    pub locked: bool,
    pub updated_at: i64,
}

impl WorkflowStateDTO {
    /// The default authoring state for a board with no deployment yet: editable, unlocked,
    /// no dashboard.
    pub fn authoring(board_id: &str) -> Self {
        WorkflowStateDTO {
            board_id: board_id.to_string(),
            deployed: false,
            dashboard_available: false,
            locked: false,
            updated_at: 0,
        }
    }
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
