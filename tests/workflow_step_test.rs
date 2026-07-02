//! W1 — Notebook → Workflow surface (ROUND8 §W1).
//!
//! Proves the cell/component model has collapsed to ONE authoring primitive: the
//! plain-English workflow **step**. markdown/mermaid/canvas/image/code/model cease
//! to be authorable cell kinds (mermaid/DAG is compiled OUTPUT, never an input).
//! `cyan_pipeline_compile` keeps producing a plan; legacy boards migrate with no
//! data loss; the `@`/`#`/`/` autocomplete index query returns plugins/artifacts/
//! actions, tenant-scoped.
//!
//! No live deps: pure storage + compile-plan path. Every wait is bounded (none are
//! needed — synchronous storage assertions on the receiver's `storage::*`).

use std::path::Path;
use std::sync::Once;

use cyan_backend::{anti_entropy, pipeline, storage, workflow};

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB with the base schema the
/// engine migrations assume exist.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("workflow.db");
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
        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, element_type TEXT NOT NULL,
            x REAL, y REAL, width REAL, height REAL, z_index INTEGER DEFAULT 0,
            style_json TEXT, content_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
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
// 1. The step is the ONLY authorable kind.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn single_step_type_is_the_only_authorable_kind() {
    // There is exactly one authorable kind, and it is the step.
    assert_eq!(workflow::authorable_kinds(), &["step"]);
    assert!(workflow::is_authorable_kind("step"));

    // None of the six former cell kinds is authorable any more; each collapses to
    // the single step primitive when an (old) client tries to author it.
    for legacy in ["markdown", "mermaid", "canvas", "image", "code", "model"] {
        assert!(
            !workflow::is_authorable_kind(legacy),
            "{legacy} must no longer be an authorable kind"
        );
        assert_eq!(
            workflow::coerce_authoring_cell_type(legacy),
            "step",
            "{legacy} must coerce to the step primitive"
        );
    }

    // System-generated kinds (not authored by the user) pass through unchanged.
    assert_eq!(workflow::coerce_authoring_cell_type("timecode_note"), "timecode_note");
    // The archived sentinel is preserved (migration target), not re-coerced.
    assert_eq!(workflow::coerce_authoring_cell_type("archived"), "archived");
}

// ════════════════════════════════════════════════════════════════════════════
// 2. compile still produces a plan (from step cells).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn compile_still_produces_plan() {
    ensure_db();
    let (group, board) = ("wf-compile-grp", "wf-compile-board");
    seed_board(group, board);

    let now = 1_700_000_000i64;
    storage::cell_insert_simple(
        &format!("{board}-s0"), board, "step", 0,
        Some("Transcode the master to a mezzanine"), None, false, None, None, now, now,
    )
    .expect("step 0");
    storage::cell_insert_simple(
        &format!("{board}-s1"), board, "step", 1,
        Some("Run compliance QC on the mezzanine"), None, false, None, None, now, now,
    )
    .expect("step 1");

    let plan = pipeline::compile_pipeline(board).expect("compile produces a plan");

    assert_eq!(plan["total_cells"].as_u64(), Some(2), "both step cells in the plan");
    let steps = plan["steps"].as_array().expect("steps array");
    assert_eq!(steps.len(), 2, "one plan step per authored step");
    assert_eq!(steps[0]["title"].as_str(), Some("Transcode the master to a mezzanine"));
    assert!(plan["prompt"].as_str().is_some(), "a compile prompt is produced");
}

// ════════════════════════════════════════════════════════════════════════════
// 3. legacy boards migrate without data loss.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn legacy_cells_migrate_without_data_loss() {
    ensure_db();
    let (group, board) = ("wf-legacy-grp", "wf-legacy-board");
    seed_board(group, board);

    let now = 1_700_000_000i64;
    // Six legacy cells, one of each former authorable kind, each with content.
    let seeds: [(&str, &str, &str); 6] = [
        ("md", "markdown", "Transcode the master"),
        ("code", "code", "ffmpeg -i in.mov out.mp4"),
        ("mer", "mermaid", "graph TD; A-->B"),
        ("can", "canvas", "{\"shapes\":[]}"),
        ("img", "image", "https://example/frame.png"),
        ("mod", "model", "llama-3.3-70b"),
    ];
    for (i, (suffix, kind, content)) in seeds.iter().enumerate() {
        storage::cell_insert_simple(
            &format!("{board}-{suffix}"), board, kind, i as i32,
            Some(content), None, false, None, None, now, now,
        )
        .unwrap_or_else(|_| panic!("seed {kind}"));
    }

    let migrated = storage::migrate_legacy_authoring_cells().expect("migration runs");
    assert!(migrated >= 6, "all six legacy cells migrated, got {migrated}");

    let cells = storage::cell_list_by_boards(&[board.to_string()]).expect("list");
    // NOT ONE row was dropped.
    assert_eq!(cells.len(), 6, "no cell silently dropped");

    let by_id = |suffix: &str| {
        cells
            .iter()
            .find(|c| c.id == format!("{board}-{suffix}"))
            .unwrap_or_else(|| panic!("cell {suffix} still present"))
    };

    // Every original content survives the migration (no data loss).
    for (suffix, _kind, content) in &seeds {
        assert_eq!(
            by_id(suffix).content.as_deref(),
            Some(*content),
            "content preserved for {suffix}"
        );
    }

    // Text-bearing kinds (markdown, code) are REPRESENTED as steps.
    assert_eq!(by_id("md").cell_type, "step", "markdown → step");
    assert_eq!(by_id("code").cell_type, "step", "code → step");

    // Non-text kinds are ARCHIVED (kept, never authorable), original kind recorded.
    for (suffix, kind) in [("mer", "mermaid"), ("can", "canvas"), ("img", "image"), ("mod", "model")] {
        let c = by_id(suffix);
        assert_eq!(c.cell_type, "archived", "{kind} → archived");
        let meta: serde_json::Value =
            serde_json::from_str(c.metadata_json.as_deref().unwrap_or("{}")).expect("meta json");
        assert_eq!(
            meta["original_cell_type"].as_str(),
            Some(kind),
            "original kind recorded for {kind} (reversible, no data loss)"
        );
    }

    // Archived cells do NOT appear as compile steps; only the two real steps do.
    let plan = pipeline::compile_pipeline(board).expect("compile after migration");
    assert_eq!(plan["total_cells"].as_u64(), Some(2), "archived cells excluded from compile");

    // Migration is idempotent — re-running changes nothing.
    let again = storage::migrate_legacy_authoring_cells().expect("re-run migration");
    assert_eq!(again, 0, "second migration is a no-op");
}

// ════════════════════════════════════════════════════════════════════════════
// 4. step text roundtrips through the snapshot path and rides the digest.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn step_text_roundtrips_and_syncs() {
    ensure_db();
    let (group, board) = ("wf-rt-grp", "wf-rt-board");
    seed_board(group, board);

    let now = 1_700_000_000i64;
    let text = "Transcribe the master then run compliance QC and gate for approval";
    storage::cell_insert_simple(
        &format!("{board}-s0"), board, "step", 0, Some(text), None, false, None, None, now, now,
    )
    .expect("author step");

    // Sender side: the snapshot serializer emits the step as a cell DTO.
    let dtos = storage::cell_list_by_boards(&[board.to_string()]).expect("serialize");
    let dto = dtos.iter().find(|c| c.id == format!("{board}-s0")).expect("step in snapshot");
    assert_eq!(dto.cell_type, "step");
    assert_eq!(dto.content.as_deref(), Some(text), "step text serialized verbatim");

    // The step rides the existing anti-entropy digest (converges like any cell).
    let (count, hash) = anti_entropy::group_digest(group);
    assert!(count >= 1, "step counted in the group digest");

    // Receiver side: applying the snapshot content roundtrips the step text.
    storage::snapshot_insert_content(&[], &dtos).expect("apply snapshot");
    let after = storage::cell_list_by_boards(&[board.to_string()]).expect("re-read");
    let again = after.iter().find(|c| c.id == format!("{board}-s0")).expect("step survived");
    assert_eq!(again.cell_type, "step");
    assert_eq!(again.content.as_deref(), Some(text), "step text roundtrips after sync");

    // Re-applying identical content keeps the digest stable (convergent, no churn).
    let (count2, hash2) = anti_entropy::group_digest(group);
    assert_eq!((count, hash), (count2, hash2), "digest stable after re-apply");
}

// ════════════════════════════════════════════════════════════════════════════
// 5. autocomplete index query — @ plugins, # artifacts, / actions, tenant-scoped.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn autocomplete_index_query_returns_tools_artifacts_actions() {
    ensure_db();
    let now = 1_700_000_000i64;

    // Tenant A: a board, an installed plugin bundle, a board file, a prior-step output.
    let (group_a, board_a) = ("wf-idx-grp-a", "wf-idx-board-a");
    seed_board(group_a, board_a);
    // Plugins workspace + an installed bundle file (local_path set ⇒ installed).
    let plugins_ws_a = format!("{group_a}-plugins");
    storage::workspace_insert_simple(&plugins_ws_a, group_a, "Plugins").expect("plugins ws");
    storage::file_insert(
        "plg-a", Some(group_a), Some(&plugins_ws_a), None,
        "slack.cyanplugin", "hashA", 10, "peerA", now,
    )
    .expect("plugin file");
    storage::file_set_local_path("plg-a", "/tmp/slack.cyanplugin").expect("local path");
    // A board artifact file.
    storage::file_insert(
        "file-a", Some(group_a), Some(&format!("{group_a}-ws")), Some(board_a),
        "master.mov", "hashF", 99, "peerA", now,
    )
    .expect("artifact file");
    // A prior-step output (a step cell that already produced output).
    storage::cell_insert_simple(
        &format!("{board_a}-s0"), board_a, "step", 0,
        Some("Probe the master"), Some("duration=01:32:00"), false, None, None, now, now,
    )
    .expect("step with output");

    // Tenant B: its own plugin + file, must NEVER leak into A's index.
    let (group_b, board_b) = ("wf-idx-grp-b", "wf-idx-board-b");
    seed_board(group_b, board_b);
    let plugins_ws_b = format!("{group_b}-plugins");
    storage::workspace_insert_simple(&plugins_ws_b, group_b, "Plugins").expect("plugins ws b");
    storage::file_insert(
        "plg-b", Some(group_b), Some(&plugins_ws_b), None,
        "groupb-secret.cyanplugin", "hashB", 10, "peerB", now,
    )
    .expect("plugin file b");
    storage::file_set_local_path("plg-b", "/tmp/groupb-secret.cyanplugin").expect("local path b");
    storage::file_insert(
        "file-b", Some(group_b), Some(&format!("{group_b}-ws")), Some(board_b),
        "groupb-file.mov", "hashFB", 99, "peerB", now,
    )
    .expect("artifact file b");

    let idx = workflow::autocomplete_index(board_a);

    // Tenant tag is the board's group.
    assert_eq!(idx.tenant_id, group_a);

    // @ plugins — the installed bundle, trigger '@'.
    assert!(
        idx.plugins.iter().any(|e| e.value.contains("slack")),
        "installed plugin surfaced under @"
    );
    assert!(idx.plugins.iter().all(|e| e.trigger == '@'), "all plugins use the @ trigger");

    // # artifacts — board file AND prior-step output, trigger '#'.
    assert!(
        idx.artifacts.iter().any(|e| e.label.contains("master.mov")),
        "board file surfaced under #"
    );
    assert!(
        idx.artifacts.iter().any(|e| e.kind == "step_output"),
        "prior-step output surfaced under #"
    );
    assert!(idx.artifacts.iter().all(|e| e.trigger == '#'), "all artifacts use the # trigger");

    // / actions — the controlled verb set, trigger '/'.
    assert!(!idx.actions.is_empty(), "controlled action verbs present");
    assert!(idx.actions.iter().any(|e| e.value == "run"), "core action 'run' present");
    assert!(idx.actions.iter().all(|e| e.trigger == '/'), "all actions use the / trigger");

    // Tenant isolation — none of B's plugins/files leak into A's index.
    assert!(
        idx.plugins.iter().all(|e| !e.value.contains("groupb")),
        "tenant B plugin must not appear in tenant A index"
    );
    assert!(
        idx.artifacts.iter().all(|e| !e.label.contains("groupb")),
        "tenant B file must not appear in tenant A index"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// S5 — compile MUST preserve executor="manual" (the human-approval gate).
// The LLM/deterministic recompile (compile_via_llm) used to hardcode executor="lens",
// dropping the manual gate so the package/human step EXECUTED forever instead of pausing.
// This drives the REAL compile_via_llm and asserts the manual executor survives.
// ════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn compile_preserves_manual_executor() {
    ensure_db();
    let (group, board) = ("wf-manual-grp", "wf-manual-board");
    seed_board(group, board);
    let now = 1_700_000_000i64;

    let lens_meta = r#"{"pipeline":{"step_id":"ingest","depends_on":[],"executor":"lens","model":"cyan-lens","timeout_seconds":300,"retry_count":1,"auto_advance":false,"notifications":[],"state":{"status":"pending","attempt":0}}}"#;
    let manual_meta = r#"{"pipeline":{"step_id":"package","depends_on":["ingest"],"executor":"manual","model":"cyan-lens","timeout_seconds":300,"retry_count":1,"auto_advance":false,"notifications":[],"state":{"status":"pending","attempt":0}}}"#;
    // Authored as "step" (the ONLY authorable kind per W1). Using a legacy kind
    // here would also race the legacy-migration test above: both share the
    // process-global DB, and the global legacy sweep would migrate these rows,
    // breaking that test's second-run-is-a-no-op assertion.
    storage::cell_insert_simple(
        &format!("{board}-ingest"), board, "step", 0,
        Some("Ingest the master big-buck-bunny.mp4"), None, false, None, Some(lens_meta), now, now,
    ).expect("ingest cell");
    storage::cell_insert_simple(
        &format!("{board}-package"), board, "step", 1,
        Some("Package: deliver big-buck-bunny.mp4 at -14 LUFS and write the sidecar"),
        None, false, None, Some(manual_meta), now, now,
    ).expect("package cell");

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<cyan_backend::models::commands::CommandMsg>();
    pipeline::compile_via_llm(board, &tx).await.expect("compile_via_llm");

    // After compile, the package (human) step's executor MUST still be "manual".
    let cells = storage::cell_list_by_boards(&[board.to_string()]).expect("cells");
    let pkg = cells.iter().find(|c| c.id.ends_with("-package")).expect("package cell present");
    let meta: serde_json::Value =
        serde_json::from_str(pkg.metadata_json.as_deref().unwrap_or("{}")).expect("meta json");
    assert_eq!(
        meta["pipeline"]["executor"].as_str(), Some("manual"),
        "compile dropped the manual gate to '{}' — the human step would run forever",
        meta["pipeline"]["executor"].as_str().unwrap_or("?")
    );
    println!("S5-PROOF package executor after compile = {}", meta["pipeline"]["executor"]);
}
