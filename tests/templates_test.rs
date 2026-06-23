//! W4 — Templates + pinned workflows (ROUND8 §W4).
//!
//! A **template** = a pre-written English workflow (steps + bound plugins) you clone
//! into a board. This suite proves the backend contract:
//!   * a built-in **media seed set** is always present (`seed_templates_present`);
//!   * cloning a template materializes **real W1 step cells** on the target board
//!     (`clone_template_creates_workflow_steps`);
//!   * **save-as-template** captures a board's steps as a reusable template
//!     (`save_as_template`);
//!   * user-saved templates are **tenant-scoped** — they never leak across tenants
//!     (`template_tenant_scoped`).
//!
//! The cross-peer **pin** convergence proof lives in `tests/substrate_templates_mp.rs`
//! (`pin_state_syncs_across_peers`) — pin state rides the existing anti-entropy
//! digest + snapshot path, like notes.
//!
//! No live deps: pure storage + template path. Every assertion is synchronous on the
//! engine's own `storage::*` / `templates::*` (no waits needed).

use std::path::Path;
use std::sync::Once;

use cyan_backend::models::dto::TemplateStep;
use cyan_backend::{storage, templates};

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB with the base schema the engine
/// migrations assume exist. `templates`/`pins` are NOT created here, so the engine
/// migration that adds them is exercised on a DB that predates them.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("templates.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
    });
}

/// Base tables the engine migrations assume exist. Run once before `storage::init_db`.
fn init_base_schema(db_path: &Path) -> Result<(), rusqlite::Error> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, icon TEXT, color TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY, group_id TEXT NOT NULL, name TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY, workspace_id TEXT, group_id TEXT, board_id TEXT,
            type TEXT NOT NULL, name TEXT NOT NULL, hash TEXT, data TEXT, size INTEGER,
            source_peer TEXT, local_path TEXT, created_at INTEGER NOT NULL,
            board_mode TEXT DEFAULT 'canvas'
        );
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL, content TEXT, output TEXT,
            collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

/// Seed group → workspace → board, returning the board id. Tenant == group id.
fn seed_board(group: &str, board: &str) {
    let now = 1_700_000_000i64;
    let ws = format!("{group}-ws");
    storage::group_insert_simple(group, "WF Group", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&ws, group, "Main").expect("workspace");
    storage::board_insert_simple(board, &ws, "Workflow Board", now).expect("board");
}

// ════════════════════════════════════════════════════════════════════════════
// 1. The built-in media seed set is always present.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn seed_templates_present() {
    ensure_db();

    // Listed for ANY tenant, the three media seeds are always present (built-in
    // defaults, not tenant-owned). Names are the spec's verbatim seed set.
    let list = templates::list_templates("any-tenant");
    let names: Vec<&str> = list.iter().map(|t| t.name.as_str()).collect();
    for want in [
        templates::SEED_TRANSCODE_DELIVER_NAME,
        templates::SEED_TRANSCRIBE_QC_NAME,
        templates::SEED_CONFORM_APPROVE_MASTER_NAME,
    ] {
        assert!(names.contains(&want), "seed template '{want}' must be present, got {names:?}");
    }

    // Every seed is flagged built-in and carries at least one pre-written step.
    let seeds: Vec<_> = list.iter().filter(|t| t.source == templates::SOURCE_BUILTIN).collect();
    assert_eq!(seeds.len(), 3, "exactly three built-in seed templates");
    for s in &seeds {
        assert!(!s.steps.is_empty(), "seed '{}' has pre-written steps", s.name);
    }

    // The "Transcode master → deliver to Contido" seed binds the Contido plugin on
    // its delivery step (a template = steps + bound plugins).
    let transcode = seeds
        .iter()
        .find(|t| t.name == templates::SEED_TRANSCODE_DELIVER_NAME)
        .expect("transcode seed");
    assert!(
        transcode.steps.iter().any(|s| s.plugin.as_deref() == Some("contido")),
        "the delivery seed binds the Contido plugin to a step"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Cloning a template materializes real W1 step cells on the target board.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn clone_template_creates_workflow_steps() {
    ensure_db();
    let (group, board) = ("tpl-clone-grp", "tpl-clone-board");
    seed_board(group, board);

    // The board starts with no cells.
    assert_eq!(
        storage::cell_list_by_boards(&[board.to_string()]).expect("list").len(),
        0,
        "board starts empty"
    );

    // Clone the "Transcribe + compliance QC" seed into the board.
    let seed = templates::list_templates(group)
        .into_iter()
        .find(|t| t.name == templates::SEED_TRANSCRIBE_QC_NAME)
        .expect("seed present");
    let created = templates::clone_to_board(&seed.id, board, group).expect("clone");

    // One real step cell per template step, in order, content = the step text.
    let cells = storage::cell_list_by_boards(&[board.to_string()]).expect("list");
    assert_eq!(cells.len(), seed.steps.len(), "one cell per template step");
    assert_eq!(created.len(), seed.steps.len(), "clone reports each created cell id");
    for (i, step) in seed.steps.iter().enumerate() {
        assert_eq!(cells[i].cell_type, "step", "cloned cell is the W1 step primitive");
        assert_eq!(cells[i].cell_order, i as i32, "steps preserve template order");
        assert_eq!(
            cells[i].content.as_deref(),
            Some(step.text.as_str()),
            "step text cloned verbatim"
        );
    }

    // The clone compiles to a real plan (the cloned steps ARE authorable W1 steps).
    let plan = cyan_backend::pipeline::compile_pipeline(board).expect("compile cloned workflow");
    assert_eq!(
        plan["total_cells"].as_u64(),
        Some(seed.steps.len() as u64),
        "every cloned step is in the compiled plan"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 3. Save-as-template captures a board's steps as a reusable template.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn save_as_template() {
    ensure_db();
    let group = "tpl-save-grp";

    let steps = vec![
        TemplateStep { text: "Conform the edit from the AAF".to_string(), plugin: None },
        TemplateStep {
            text: "Send the master to delivery".to_string(),
            plugin: Some("contido".to_string()),
        },
    ];
    let saved = templates::save_as_template(group, "My Delivery Flow", "Saved from a board", steps.clone())
        .expect("save");

    // It is a user template (not built-in), tenant-scoped, with the steps preserved.
    assert_eq!(saved.source, templates::SOURCE_USER, "save-as yields a user template");
    assert_eq!(saved.tenant_id, group, "saved template is tenant-scoped to the group");
    assert_eq!(saved.steps.len(), 2, "both steps captured");
    assert_eq!(saved.steps[1].plugin.as_deref(), Some("contido"), "bound plugin captured");

    // It is retrievable by id and shows up in the tenant's list alongside the seeds.
    let got = templates::get_template(&saved.id, group).expect("get saved template");
    assert_eq!(got.name, "My Delivery Flow");
    assert_eq!(got.steps.len(), 2);

    let list = templates::list_templates(group);
    assert!(list.iter().any(|t| t.id == saved.id), "saved template appears in the tenant list");
    // The seeds are still present (save-as adds to, never replaces, the seed set).
    assert!(
        list.iter().any(|t| t.name == templates::SEED_CONFORM_APPROVE_MASTER_NAME),
        "seed set survives save-as"
    );

    // And it can itself be cloned into a board — round-trips as a real workflow.
    let board = "tpl-save-board";
    seed_board(group, board);
    let created = templates::clone_to_board(&saved.id, board, group).expect("clone saved");
    assert_eq!(created.len(), 2, "saved template clones two steps");
}

// ════════════════════════════════════════════════════════════════════════════
// 4. User-saved templates are tenant-scoped — no cross-tenant leak.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn template_tenant_scoped() {
    ensure_db();
    let (group_a, group_b) = ("tpl-tenant-a", "tpl-tenant-b");

    // Tenant A saves a private template.
    let steps = vec![TemplateStep { text: "Tenant A secret step".to_string(), plugin: None }];
    let saved = templates::save_as_template(group_a, "A Private Flow", "", steps).expect("save A");

    // Tenant A sees it…
    assert!(
        templates::list_templates(group_a).iter().any(|t| t.id == saved.id),
        "tenant A sees its own saved template"
    );
    // …tenant B must NOT (no cross-tenant leak of user templates).
    assert!(
        templates::list_templates(group_b).iter().all(|t| t.id != saved.id),
        "tenant B must not see tenant A's saved template"
    );
    assert!(
        templates::list_templates(group_b).iter().all(|t| t.name != "A Private Flow"),
        "tenant B must not see tenant A's template by name either"
    );
    // A get scoped to tenant B must refuse A's template.
    assert!(
        templates::get_template(&saved.id, group_b).is_none(),
        "get scoped to the wrong tenant returns nothing"
    );

    // The built-in seeds, however, are present for BOTH tenants (they are global
    // defaults, not tenant-owned).
    for g in [group_a, group_b] {
        assert!(
            templates::list_templates(g)
                .iter()
                .any(|t| t.name == templates::SEED_TRANSCODE_DELIVER_NAME),
            "seeds are present for every tenant ({g})"
        );
    }
}
