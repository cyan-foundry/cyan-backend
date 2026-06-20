//! Workflow authoring model (ROUND8 §W1).
//!
//! The notebook collapses to **one** authoring primitive: the plain-English
//! **step**. The six former cell kinds (markdown/mermaid/canvas/image/code/model)
//! cease to be authorable — markdown *content* is kept as the step text, and
//! mermaid/DAG is compiled OUTPUT on the dashboard, never an input.
//!
//! This module owns:
//!   * the canonical step kind + the coercion every authoring write goes through;
//!   * the `@`/`#`/`/` autocomplete index query (plugins / artifacts / actions),
//!     tenant-scoped to the board's group.
//!
//! The legacy-board migration lives in `storage::migrate_legacy_authoring_cells`
//! (it needs the DB connection the engine migrations run on).

use serde::Serialize;

use crate::mcp_host::{PLUGINS_WORKSPACE_NAME, PLUGIN_BUNDLE_SUFFIX};
use crate::storage;

/// The one and only authorable cell kind.
pub const STEP_KIND: &str = "step";

/// Sentinel kind for legacy cells that are kept (never authorable, never dropped)
/// because they carried non-text content (mermaid/canvas/image/model).
pub const ARCHIVED_KIND: &str = "archived";

/// The legacy authorable cell kinds that collapse into the single step primitive.
pub const LEGACY_AUTHORING_KINDS: &[&str] =
    &["markdown", "mermaid", "canvas", "image", "code", "model"];

/// The controlled `/` action verb set surfaced in autocomplete. Closed set — the
/// authoring surface only offers these workflow control verbs.
pub const CONTROL_ACTIONS: &[(&str, &str)] = &[
    ("run", "Run the workflow"),
    ("approve", "Approve this step"),
    ("needs-approval", "Require approval before continuing"),
    ("send-to", "Send the result to a destination"),
    ("connect", "Connect a plugin"),
    ("compile", "Compile the workflow"),
    ("retry", "Retry a failed step"),
    ("skip", "Skip this step"),
];

/// The complete, fixed set of authorable kinds — exactly `["step"]`.
pub fn authorable_kinds() -> &'static [&'static str] {
    &[STEP_KIND]
}

/// True iff `kind` is an authorable cell kind (only the step is).
pub fn is_authorable_kind(kind: &str) -> bool {
    kind == STEP_KIND
}

/// True iff `kind` is a system-generated (non-authored) cell kind that must pass
/// through write coercion unchanged (e.g. timecode notes the engine inserts).
pub fn is_system_kind(kind: &str) -> bool {
    matches!(kind, "timecode_note")
}

/// Canonicalize a cell kind on the authoring write path. Any authorable intent
/// collapses to the single step primitive; system kinds and the archived sentinel
/// pass through unchanged.
pub fn coerce_authoring_cell_type(requested: &str) -> String {
    if is_system_kind(requested) || requested == ARCHIVED_KIND {
        requested.to_string()
    } else {
        STEP_KIND.to_string()
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Autocomplete index — @ plugins, # artifacts, / actions (tenant-scoped).
// ════════════════════════════════════════════════════════════════════════════

/// One autocomplete suggestion: the trigger that summons it, its kind, the token
/// inserted on accept (`value`), and the human label shown.
#[derive(Debug, Clone, Serialize)]
pub struct AutocompleteEntry {
    /// `'@'` (plugin), `'#'` (artifact), or `'/'` (action).
    pub trigger: char,
    /// `"plugin"`, `"file"`, `"step_output"`, or `"action"`.
    pub kind: String,
    /// The token inserted into the step text when accepted.
    pub value: String,
    /// Display label.
    pub label: String,
}

/// The tenant-scoped autocomplete index backing the three trigger vocabularies.
#[derive(Debug, Clone, Serialize)]
pub struct AutocompleteIndex {
    /// The board's group — the tenant every entry is scoped to.
    pub tenant_id: String,
    /// `@` — installed plugins (the index).
    pub plugins: Vec<AutocompleteEntry>,
    /// `#` — artifacts/files: board files + prior-step outputs.
    pub artifacts: Vec<AutocompleteEntry>,
    /// `/` — actions & control: the controlled verb set.
    pub actions: Vec<AutocompleteEntry>,
}

/// Build the autocomplete index for a board, tenant-scoped to the board's group.
///
/// `@` plugins  = installed `.cyanplugin` bundles in the group's Plugins workspace.
/// `#` artifacts = the group's files + this board's prior-step outputs.
/// `/` actions   = the fixed `CONTROL_ACTIONS` verb set.
///
/// Best-effort: storage errors degrade to empty lists rather than failing the query.
pub fn autocomplete_index(board_id: &str) -> AutocompleteIndex {
    let tenant_id = storage::board_get_group_id(board_id)
        .filter(|g| !g.is_empty())
        .unwrap_or_else(|| "device".to_string());

    // @ — installed plugins in this tenant's Plugins workspace.
    let plugins = storage::plugin_bundles_in_group(
        &tenant_id,
        PLUGINS_WORKSPACE_NAME,
        PLUGIN_BUNDLE_SUFFIX,
    )
    .unwrap_or_default()
    .into_iter()
    .map(|p| {
        let value = p
            .name
            .strip_suffix(PLUGIN_BUNDLE_SUFFIX)
            .unwrap_or(&p.name)
            .to_string();
        AutocompleteEntry {
            trigger: '@',
            kind: "plugin".to_string(),
            value,
            label: p.name,
        }
    })
    .collect();

    // # — the tenant's files, then this board's prior-step outputs.
    let mut artifacts: Vec<AutocompleteEntry> = storage::file_list_by_group(&tenant_id)
        .unwrap_or_default()
        .into_iter()
        .map(|f| AutocompleteEntry {
            trigger: '#',
            kind: "file".to_string(),
            value: f.id,
            label: f.name,
        })
        .collect();

    for c in storage::cell_list_by_boards(&[board_id.to_string()]).unwrap_or_default() {
        if c.output.as_deref().map(|o| !o.is_empty()).unwrap_or(false) {
            let label = c
                .content
                .as_deref()
                .map(first_line)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| c.id.clone());
            artifacts.push(AutocompleteEntry {
                trigger: '#',
                kind: "step_output".to_string(),
                value: c.id,
                label,
            });
        }
    }

    // / — the controlled action verb set.
    let actions = CONTROL_ACTIONS
        .iter()
        .map(|(verb, label)| AutocompleteEntry {
            trigger: '/',
            kind: "action".to_string(),
            value: (*verb).to_string(),
            label: (*label).to_string(),
        })
        .collect();

    AutocompleteIndex { tenant_id, plugins, artifacts, actions }
}

/// First non-empty line of a step's text, trimmed — the artifact display label.
fn first_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string()
}
