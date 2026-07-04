//! Workflow templates (ROUND8 §W4).
//!
//! A **template** = a pre-written English workflow (steps + bound plugins) you clone
//! into a board. This module owns:
//!   * the built-in **media seed set** (code constants — always present, every tenant);
//!   * the merged tenant view (`list_templates` / `get_template` = seeds + the tenant's
//!     user-saved templates from `storage`);
//!   * **save-as-template** (persist a board's steps as a reusable user template);
//!   * **clone-to-board** (materialize a template's steps as real W1 `step` cells).
//!
//! Built-in seeds are deliberately NOT persisted — they live here so they are always
//! present and need no migration seeding. Only user (`save-as`) templates hit `storage`,
//! and those are tenant-scoped (a user template never crosses the tenant boundary).
//!
//! Pin state (the other half of §W4) is a replicated board-level store; see
//! `storage::pin_*`, the `group_digest`, and the snapshot Metadata frame.

use crate::models::dto::{NotebookCellDTO, Template, TemplateStep};
use crate::storage;
use anyhow::Result;

/// `source` for built-in seed templates (global defaults, surfaced to every tenant).
pub const SOURCE_BUILTIN: &str = "builtin";
/// `source` for user-saved (save-as-template) templates (tenant-scoped).
pub const SOURCE_USER: &str = "user";

/// The W1 step cell kind a cloned template step materializes into.
const STEP_KIND: &str = "step";

// ── The built-in media seed set (the spec's verbatim names) ──────────────────
pub const SEED_TRANSCODE_DELIVER_NAME: &str = "Transcode master → deliver to Contido";
pub const SEED_TRANSCRIBE_QC_NAME: &str = "Transcribe + compliance QC";
pub const SEED_CONFORM_APPROVE_MASTER_NAME: &str = "Conform + approve + master";
pub const SEED_FRAMEIO_REVIEW_LOOP_NAME: &str = "Frame.io review loop";

/// The built-in media seed templates — always present, for every tenant. `tenant_id`
/// is empty (they are global defaults, not tenant-owned); `created_at` is a fixed epoch
/// so the set is deterministic across peers.
pub fn seed_templates() -> Vec<Template> {
    let s = |text: &str, plugin: Option<&str>| TemplateStep {
        text: text.to_string(),
        plugin: plugin.map(str::to_string),
    };
    vec![
        Template {
            id: "builtin:transcode-deliver".to_string(),
            tenant_id: String::new(),
            name: SEED_TRANSCODE_DELIVER_NAME.to_string(),
            description: "Transcode the master to a delivery mezzanine and send it to Contido."
                .to_string(),
            source: SOURCE_BUILTIN.to_string(),
            steps: vec![
                s("Transcode the master to a delivery mezzanine", None),
                s("Send the mezzanine to Contido for delivery @contido", Some("contido")),
            ],
            created_at: 0,
        },
        Template {
            id: "builtin:transcribe-qc".to_string(),
            tenant_id: String::new(),
            name: SEED_TRANSCRIBE_QC_NAME.to_string(),
            description: "Transcribe the master, then run compliance QC on the transcript."
                .to_string(),
            source: SOURCE_BUILTIN.to_string(),
            steps: vec![
                s("Transcribe the master", None),
                s("Run compliance QC on the transcript", None),
            ],
            created_at: 0,
        },
        Template {
            id: "builtin:conform-approve-master".to_string(),
            tenant_id: String::new(),
            name: SEED_CONFORM_APPROVE_MASTER_NAME.to_string(),
            description: "Conform the edit, gate it for approval, then master the approved cut."
                .to_string(),
            source: SOURCE_BUILTIN.to_string(),
            steps: vec![
                s("Conform the edit from the AAF", None),
                s("Gate the cut for sign-off /needs-approval", None),
                s("Master the approved cut", None),
            ],
            created_at: 0,
        },
        // The review LOOP as an authorable workflow (CYAN_CHANGELIST_STORE_AND_
        // REVIEW_LOOP §Part 2; step text per E2E_LIVE_SCRIPT §4). A tenant
        // instantiates this per asset from the template picker; the `@frameio.*`
        // references bind at compile. Both external sends are human-gated
        // (`/needs-approval` — external_send is ALWAYS human-fired, per the
        // transition contract).
        Template {
            id: "builtin:frameio-review-loop".to_string(),
            tenant_id: String::new(),
            name: SEED_FRAMEIO_REVIEW_LOOP_NAME.to_string(),
            description: "Publish a proxy to Frame.io, pull review comments each round, \
                          confirm mechanical edits, and loop until the producer approves."
                .to_string(),
            source: SOURCE_BUILTIN.to_string(),
            steps: vec![
                s("ingest and probe the dailies", None),
                s("proxy for review", None),
                s(
                    "upload to @frameio.upload for producer review /needs-approval",
                    Some("frameio"),
                ),
                s("get review comments from @frameio.list_comments", Some("frameio")),
                s(
                    "apply confirmed mechanical edits and conform proxy via @cyan-media.conform",
                    Some("cyan-media"),
                ),
                s(
                    "publish revised cut to @frameio.upload /needs-approval",
                    Some("frameio"),
                ),
            ],
            created_at: 0,
        },
    ]
}

/// All templates visible to `tenant_id`: the built-in seeds (always) followed by the
/// tenant's own user-saved templates. User templates from other tenants never appear.
pub fn list_templates(tenant_id: &str) -> Vec<Template> {
    let mut out = seed_templates();
    out.extend(storage::template_list_by_tenant(tenant_id).unwrap_or_default());
    out
}

/// Fetch one template by id for `tenant_id`: a built-in seed (tenant-agnostic) or one
/// of the tenant's own user templates. Returns `None` for an unknown id OR a user
/// template owned by a different tenant (no cross-tenant read).
pub fn get_template(id: &str, tenant_id: &str) -> Option<Template> {
    if let Some(seed) = seed_templates().into_iter().find(|t| t.id == id) {
        return Some(seed);
    }
    storage::template_get(id, tenant_id).ok().flatten()
}

/// Persist a board's steps as a reusable **user** template, tenant-scoped to `tenant_id`.
/// Returns the created template (with its generated id).
pub fn save_as_template(
    tenant_id: &str,
    name: &str,
    description: &str,
    steps: Vec<TemplateStep>,
) -> Result<Template> {
    let now = chrono::Utc::now().timestamp();
    let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(now);
    let id = blake3::hash(format!("template:{tenant_id}:{name}:{nanos}").as_bytes())
        .to_hex()
        .to_string();
    let template = Template {
        id,
        tenant_id: tenant_id.to_string(),
        name: name.to_string(),
        description: description.to_string(),
        source: SOURCE_USER.to_string(),
        steps,
        created_at: now,
    };
    storage::template_insert(&template)?;
    tracing::info!(tenant_id = %tenant_id, "obs template_save id={} steps={}", template.id, template.steps.len());
    Ok(template)
}

/// Clone a template into `board_id` as real W1 `step` cells — one cell per template
/// step, appended after any existing cells, in template order. The step text is cloned
/// verbatim; a bound plugin is recorded in the cell's `metadata_json` (`{"plugin":..}`)
/// so the W1 guided-compile chip can surface it. Returns the created cells (so the
/// caller can broadcast each as a `NotebookCellAdded`). The template is never mutated.
pub fn clone_to_board(
    template_id: &str,
    board_id: &str,
    tenant_id: &str,
) -> Result<Vec<NotebookCellDTO>> {
    let template = get_template(template_id, tenant_id)
        .ok_or_else(|| anyhow::anyhow!("template '{template_id}' not found for tenant '{tenant_id}'"))?;

    let now = chrono::Utc::now().timestamp();
    let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(now);
    // Append after existing cells so cloning into a non-empty board never reorders it.
    let start = storage::cell_list_by_boards(&[board_id.to_string()])
        .map(|c| c.len())
        .unwrap_or(0);

    let mut created = Vec::with_capacity(template.steps.len());
    for (i, step) in template.steps.iter().enumerate() {
        let order = (start + i) as i32;
        let id = blake3::hash(format!("clone:{template_id}:{board_id}:{i}:{nanos}").as_bytes())
            .to_hex()
            .to_string();
        // Record the bound plugin in the cell's metadata (built without the `json!`
        // macro, whose internal `unwrap` trips the repo's disallowed-methods lint).
        let metadata_json = step.plugin.as_ref().map(|p| {
            let mut m = serde_json::Map::new();
            m.insert("plugin".to_string(), serde_json::Value::String(p.clone()));
            serde_json::Value::Object(m).to_string()
        });
        storage::cell_insert_simple(
            &id,
            board_id,
            STEP_KIND,
            order,
            Some(step.text.as_str()),
            None,
            false,
            None,
            metadata_json.as_deref(),
            now,
            now,
        )?;
        created.push(NotebookCellDTO {
            id,
            board_id: board_id.to_string(),
            cell_type: STEP_KIND.to_string(),
            cell_order: order,
            content: Some(step.text.clone()),
            output: None,
            collapsed: false,
            height: None,
            metadata_json,
            created_at: now,
            updated_at: now,
        });
    }
    tracing::info!(tenant_id = %tenant_id, "obs template_clone id={template_id} board={board_id} steps={}", created.len());
    Ok(created)
}
