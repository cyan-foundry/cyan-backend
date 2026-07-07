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

use anyhow::{anyhow, bail, Result};
use cyan_identity::{OrgGrantVerifier, Role, SignedRevocationList};
use secrecy::SecretString;
use serde::Serialize;

use crate::mcp_host::{PLUGINS_WORKSPACE_NAME, PLUGIN_BUNDLE_SUFFIX};
use crate::models::dto::WorkflowStateDTO;
use crate::sso_grant::SsoSession;
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

    // @ — installed plugins in this tenant's Plugins workspace, PLUS each
    // plugin's manifest tools (`@plugin.tool`) read from the unpacked bundle —
    // the per-group answer to "`@frameio.` shows no functions". The plugin list
    // is the group's installed set; the tool list comes from the device registry
    // (unpacked on install, lazily unpacked here for swarm-fetched bundles).
    let mut plugins: Vec<AutocompleteEntry> = Vec::new();
    for p in storage::plugin_bundles_in_group(
        &tenant_id,
        PLUGINS_WORKSPACE_NAME,
        PLUGIN_BUNDLE_SUFFIX,
    )
    .unwrap_or_default()
    {
        let plugin_id = p
            .name
            .strip_suffix(PLUGIN_BUNDLE_SUFFIX)
            .unwrap_or(&p.name)
            .to_string();
        plugins.push(AutocompleteEntry {
            trigger: '@',
            kind: "plugin".to_string(),
            value: plugin_id.clone(),
            label: p.name.clone(),
        });
        if let Some(bundle_dir) = storage::ensure_bundle_unpacked(&plugin_id)
            && let Ok(manifest) = cyan_mcp::Manifest::from_bundle(&bundle_dir)
        {
            // CURATED tools first (upload_file, list_comments, …) — the raw
            // machine-generated endpoint names (get_v4_…/post_v4_…) rank last
            // so they never crowd the flagship verbs out of the picker's
            // visible rows (the UI shows 6).
            let mut tools: Vec<&cyan_mcp::ToolBlock> = manifest.tools.iter().collect();
            tools.sort_by_key(|t| (is_generated_tool_name(&t.name), t.name.clone()));
            for tool in tools {
                plugins.push(AutocompleteEntry {
                    trigger: '@',
                    kind: "tool".to_string(),
                    value: format!("{plugin_id}.{}", tool.name),
                    label: tool.when_to_use.clone(),
                });
            }
        }
    }

    // # — this board's files FIRST, then the tenant's, deduped by content hash
    // (same bytes = one entry) and listed by NAME (the value inserted into the
    // step text is the file NAME the author can read, not an opaque id; binding
    // resolves it back to the row). A name with whitespace falls back to the id
    // so the inserted token survives whitespace-delimited parsing.
    let mut artifacts: Vec<AutocompleteEntry> = Vec::new();
    let mut seen_files: std::collections::HashSet<String> = std::collections::HashSet::new();
    let board_files = storage::file_list_by_board(board_id).unwrap_or_default();
    let group_files = storage::file_list_by_group(&tenant_id).unwrap_or_default();
    for f in board_files.into_iter().chain(group_files) {
        let dedup_key = if f.hash.is_empty() { f.id.clone() } else { f.hash.clone() };
        if !seen_files.insert(dedup_key) {
            continue;
        }
        let value = if !f.name.is_empty() && !f.name.contains(char::is_whitespace) {
            f.name.clone()
        } else {
            f.id.clone()
        };
        artifacts.push(AutocompleteEntry {
            trigger: '#',
            kind: "file".to_string(),
            value,
            label: f.name,
        });
    }

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

// ════════════════════════════════════════════════════════════════════════════
// Deploy + lock lifecycle (R12 D2/E1).
//
// A deployed workflow is LOCKED for editing; unlocking it mid-flight requires an
// **org-XaeroID grant (W17)** — the org signing key is the approval authority, not
// an ad-hoc flag. This is the engine round-trip: request → org-grant check → unlock.
// ════════════════════════════════════════════════════════════════════════════

/// Mark a board's workflow DEPLOYED — it is now running (optionally with a live dashboard)
/// and LOCKED for editing (E1). iOS reads `workflow_state_get` to gate the face: a deployed
/// board with `dashboard_available` surfaces the dashboard, not the editor (D2).
pub fn mark_deployed(board_id: &str, dashboard_available: bool, now_unix: i64) -> Result<WorkflowStateDTO> {
    storage::workflow_state_set_deployed(board_id, dashboard_available, now_unix)?;
    Ok(storage::workflow_state_get(board_id))
}

/// Request an UNLOCK of a deployed (locked) workflow, gated by an org-XaeroID grant (E1/W17).
///
/// The unlock is approved **iff** the presented `token`:
///   1. verifies org-signed against the tenant's pinned org key (offline; binding + `exp` +
///      grace), optionally rejected by an org-signed `revocation` list (the fired-employee
///      case); AND
///   2. is scoped to THIS board's `tenant` (a grant never authorizes another tenant); AND
///   3. carries at least `Admin` authority (the approver, not the requester, holds the grant).
///
/// On approval the `locked` lane is cleared and the fresh state returned. On any failure the
/// board STAYS locked and an error explains why — no unsigned/ad-hoc unlock path exists.
pub fn request_unlock(
    board_id: &str,
    tenant: &str,
    token: &SecretString,
    verifier: &OrgGrantVerifier,
    xaero_pubkey: &str,
    now_unix: u64,
    revocation: Option<&SignedRevocationList>,
) -> Result<WorkflowStateDTO> {
    let session = match revocation {
        Some(rl) => SsoSession::from_org_token_checked(token, verifier, xaero_pubkey, now_unix, rl),
        None => SsoSession::from_org_token(token, verifier, xaero_pubkey, now_unix),
    }
    .map_err(|e| anyhow!("unlock denied: org grant did not verify: {e}"))?;

    if session.tenant() != tenant {
        bail!(
            "unlock denied: grant tenant '{}' does not own board tenant '{}'",
            session.tenant(),
            tenant
        );
    }
    if session.role().level() < Role::Admin.level() {
        bail!(
            "unlock denied: role '{}' is below the Admin approval authority",
            session.role().as_str()
        );
    }

    storage::workflow_state_set_locked(board_id, false, now_unix as i64)?;
    Ok(storage::workflow_state_get(board_id))
}

/// The trailing autocomplete trigger token in `partial`: the last run that begins with a
/// `@`/`#`/`/` trigger and has no whitespace after the trigger. Returns `(trigger, query)`
/// where `query` is the text typed after the trigger (may be empty → list all for that
/// trigger). `None` means there is no active trigger at the cursor.
///
/// Examples: `"probe @sl"` → `('@', "sl")`, `"use #"` → `('#', "")`, `"/ru"` → `('/', "ru")`,
/// `"plain text"` → `None`.
pub fn parse_trigger(partial: &str) -> Option<(char, String)> {
    // The token at the cursor is the text after the last whitespace.
    let tail = partial.rsplit(|c: char| c.is_whitespace()).next().unwrap_or(partial);
    let mut chars = tail.chars();
    let trigger = chars.next()?;
    if !matches!(trigger, '@' | '#' | '/') {
        return None;
    }
    let query = chars.as_str();
    // A query with an embedded trigger char isn't an active completion (e.g. "a@b/c").
    if query.contains(['@', '#', '/']) {
        return None;
    }
    Some((trigger, query.to_string()))
}

/// Case-insensitive substring match used by the autocomplete filter: an entry matches when
/// its `value` OR `label` contains `query` (empty query matches everything).
fn entry_matches(entry: &AutocompleteEntry, query: &str) -> bool {
    if query.is_empty() {
        return true;
    }
    let q = query.to_ascii_lowercase();
    entry.value.to_ascii_lowercase().contains(&q) || entry.label.to_ascii_lowercase().contains(&q)
}

/// Filter a board's `autocomplete_index` by the trailing trigger + query in `partial`
/// (BURST C1). When `partial` carries a `@`/`#`/`/` trigger at the cursor, only that
/// trigger's list is populated (filtered by the query); the other two are empty. When there
/// is no active trigger, the full index passes through (all three lists, unfiltered) so the
/// caller can decide what to show.
pub fn filter_autocomplete(board_id: &str, partial: &str) -> AutocompleteIndex {
    let mut idx = autocomplete_index(board_id);
    let Some((trigger, query)) = parse_trigger(partial) else {
        return idx;
    };
    match trigger {
        '@' => {
            idx.plugins.retain(|e| entry_matches(e, &query));
            idx.artifacts.clear();
            idx.actions.clear();
        }
        '#' => {
            idx.artifacts.retain(|e| entry_matches(e, &query));
            idx.plugins.clear();
            idx.actions.clear();
        }
        '/' => {
            idx.actions.retain(|e| entry_matches(e, &query));
            idx.plugins.clear();
            idx.artifacts.clear();
        }
        _ => {}
    }
    idx
}

/// True for a machine-generated (OpenAPI path-shaped) tool name like
/// `get_v4_accounts_account_id_files_file_id_comments` — real tools, but they
/// rank BELOW curated names in the picker.
fn is_generated_tool_name(name: &str) -> bool {
    ["get_", "post_", "put_", "patch_", "delete_"]
        .iter()
        .any(|m| name.starts_with(m))
        && name.contains("_v4_")
}

/// First non-empty line of a step's text, trimmed — the artifact display label.
fn first_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string()
}
