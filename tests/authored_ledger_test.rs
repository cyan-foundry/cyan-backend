//! AUTHORED-LEDGER INVARIANT (FABLE_FULL_AUDIT headline 1, SEV-HIGH).
//!
//! Review/resolve/compile is a **pure function of the AUTHORED steps**. It may never
//! write a run's output text into an authored cell, never add cells, and never mint a
//! step/artifact id from a raw result blob (`rawcontent…` is the tell). Run outputs
//! (`timecode_note` cells, `ai_result` state) live in a separate store the compile
//! read can not reach.
//!
//! Live repro this encodes (BUG_run_output_pollutes_authored_steps, 2026-07-09):
//! clone a template → materialize/execute a run (probe/proxy result JSON persisted as
//! `timecode_note` cells) → edit a step → Review again → the authored cells were
//! OVERWRITTEN with raw result JSON and `rawcontenttextn_…` ids, because the compile
//! read swept run-output cells into the authored plan.
//!
//! No live deps: pure storage + deterministic compile. The drained `CommandMsg`s are
//! applied exactly as the lib.rs command loop applies them (cell-kind coercion + row
//! update), so the assertions hold at the STORAGE level, not just the message level.

use std::path::Path;
use std::sync::Once;

use cyan_backend::models::commands::CommandMsg;
use cyan_backend::models::dto::NotebookCellDTO;
use cyan_backend::{pipeline, storage, templates, timecode_notes, workflow};

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB with the base schema the
/// engine migrations assume exist.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("authored_ledger.db");
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

/// Seed group → workspace → board. Tenant == group id.
fn seed_board(group: &str, board: &str) {
    let now = 1_700_000_000i64;
    let ws = format!("{group}-ws");
    storage::group_insert_simple(group, "Ledger Group", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&ws, group, "Main").expect("workspace");
    storage::board_insert_simple(board, &ws, "Ledger Board", now).expect("board");
}

/// The exact raw MCP result envelopes the live run persisted as `timecode_note`
/// content (BUG_run_output_pollutes_authored_steps). `generate_step_id` over these
/// mints the `rawcontenttextn_…` garbage ids.
const PROBE_RESULT_ENVELOPE: &str = r#"{"raw":{"content":[{"text":"{\"container\":\"mov,mp4,m4a,3gp,3g2,mj2\",\"duration_s\":142.4,\"bit_rate\":9200000,\"streams\":[{\"codec\":\"h264\"}]}"}]},"tool":"cyan-media.probe"}"#;
const PROXY_RESULT_ENVELOPE: &str = r#"{"raw":{"content":[{"text":"{\"output_path\":\"/media/.cyan-derived/proxy/1094216ab9165cb0.mp4\",\"ok\":true}"}]},"tool":"cyan-media.proxy"}"#;

/// Run the deterministic Review compile and apply every drained `CommandMsg` exactly
/// as the lib.rs command loop does (W1 cell-kind coercion + full-row update). Returns
/// (compile result, drained messages) so callers can assert on the message stream too.
fn compile_and_apply(board: &str) -> (serde_json::Value, Vec<CommandMsg>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CommandMsg>();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("test runtime");
    let result = rt
        .block_on(pipeline::compile_via_llm(board, &tx))
        .expect("compile succeeds");
    drop(tx);

    let mut msgs = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        apply_like_command_loop(&msg);
        msgs.push(msg);
    }
    (result, msgs)
}

/// Apply one drained message the way `lib.rs`'s command loop applies it.
fn apply_like_command_loop(msg: &CommandMsg) {
    match msg {
        CommandMsg::UpdateNotebookCell {
            id,
            board_id,
            cell_type,
            cell_order,
            content,
            output,
            collapsed,
            height,
            metadata_json,
        } => {
            let coerced = workflow::coerce_authoring_cell_type(cell_type);
            storage::cell_update(&NotebookCellDTO {
                id: id.clone(),
                board_id: board_id.clone(),
                cell_type: coerced,
                cell_order: *cell_order,
                content: content.clone(),
                output: output.clone(),
                collapsed: *collapsed,
                height: *height,
                metadata_json: metadata_json.clone(),
                created_at: 0,
                updated_at: 0,
            })
            .expect("apply update");
        }
        CommandMsg::AddNotebookCell { .. } => {
            panic!("compile/reset must NEVER create cells (authored ledger is read-only to it)");
        }
        _ => {}
    }
}

/// Persist a run-output note through the REAL producer (`timecode_notes::save_note`),
/// exactly as `pipeline_executor` does after a step completes.
fn persist_run_output_note(board: &str, id_suffix: &str, timecode: f64, envelope: &str) {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CommandMsg>();
    let note = timecode_notes::TimecodeNote {
        id: format!("{board}-note-{id_suffix}"),
        board_id: board.to_string(),
        timecode_seconds: timecode,
        content: envelope.to_string(),
        note_type: "action".to_string(),
        author: "cyan-media".to_string(),
        created_at: 1_700_000_100.0,
        reply_to: None,
        thread_count: 0,
        pipeline_step_id: Some("ingest_and_probe".to_string()),
        pipeline_phase: Some("post_approval".to_string()),
        ai_reviewed: true,
        human_approved: true,
        action_skill: None,
        action_status: Some("complete".to_string()),
        action_result: Some(envelope.to_string()),
        action_model: None,
        ai_flags_nearby: vec![],
    };
    timecode_notes::save_note(&note, &tx).expect("persist run output note");
}

/// The board's cells split into (authored, run_output) by kind.
fn ledger_split(board: &str) -> (Vec<NotebookCellDTO>, Vec<NotebookCellDTO>) {
    let mut cells = storage::cell_list_by_boards(&[board.to_string()]).expect("list cells");
    cells.sort_by_key(|c| c.cell_order);
    let (notes, authored): (Vec<_>, Vec<_>) = cells
        .into_iter()
        .partition(|c| c.cell_type == "timecode_note");
    let authored: Vec<_> = authored
        .into_iter()
        .filter(|c| c.cell_type != "archived")
        .collect();
    (authored, notes)
}

/// Assert no compiled step id was minted from a raw result blob.
fn assert_no_rawcontent_ids(authored: &[NotebookCellDTO], compile_result: &serde_json::Value) {
    for cell in authored {
        let meta = cell.metadata_json.as_deref().unwrap_or("{}");
        assert!(
            !meta.contains("rawcontent"),
            "authored cell {} carries a step id minted from a raw result blob: {}",
            cell.id,
            meta
        );
    }
    let configs = compile_result["configs"].as_array().cloned().unwrap_or_default();
    for cfg in &configs {
        let sid = cfg["step_id"].as_str().unwrap_or_default();
        assert!(
            !sid.contains("rawcontent"),
            "compile minted a step id from a raw result blob: {sid}"
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 1. Author N steps → run → Review again → the N authored cells are byte-identical.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn review_recompile_never_absorbs_run_output() {
    ensure_db();
    let (group, board) = ("ledger-grp-a", "ledger-board-a");
    seed_board(group, board);

    let now = 1_700_000_000i64;
    let authored_texts = [
        "ingest and probe the dailies",
        "proxy for review",
        "upload to @frameio.upload for producer review /needs-approval",
    ];
    for (i, text) in authored_texts.iter().enumerate() {
        storage::cell_insert_simple(
            &format!("{board}-s{i}"), board, "step", i as i32,
            Some(text), None, false, None, None, now, now,
        )
        .expect("author step");
    }

    // Review #1 compiles the authored steps.
    let (first, _) = compile_and_apply(board);
    assert_eq!(first["applied"].as_u64(), Some(3), "three authored steps compiled");

    // A run completes: its outputs land in the run store (timecode_note cells),
    // via the REAL producer the executor uses.
    persist_run_output_note(board, "probe", 0.0, PROBE_RESULT_ENVELOPE);
    persist_run_output_note(board, "proxy", 1.0, PROXY_RESULT_ENVELOPE);

    // The author edits step 1 (the live trigger), then hits Review again.
    let (authored_before, _) = ledger_split(board);
    let edited = authored_before
        .iter()
        .find(|c| c.id == format!("{board}-s1"))
        .expect("edited cell present")
        .clone();
    let mut edited_dto = edited;
    edited_dto.content = Some("proxy for review at half resolution".to_string());
    storage::cell_update(&edited_dto).expect("author edit");

    let (second, msgs) = compile_and_apply(board);

    // THE INVARIANT — the authored ledger holds exactly the 3 authored cells,
    // byte-identical (modulo the author's own edit); the run store is untouched.
    let (authored, notes) = ledger_split(board);
    assert_eq!(authored.len(), 3, "no cells added to or removed from the authored ledger");
    let expect = [
        authored_texts[0],
        "proxy for review at half resolution",
        authored_texts[2],
    ];
    for (i, want) in expect.iter().enumerate() {
        let cell = authored
            .iter()
            .find(|c| c.id == format!("{board}-s{i}"))
            .unwrap_or_else(|| panic!("authored cell s{i} survived"));
        assert_eq!(
            cell.content.as_deref(),
            Some(*want),
            "authored cell s{i} content is byte-identical after re-Review"
        );
    }

    // Run outputs stayed in their own store, unmodified — and the compile never
    // addressed them.
    assert_eq!(notes.len(), 2, "run-output notes survive untouched");
    assert!(
        notes.iter().any(|n| n.content.as_deref() == Some(PROBE_RESULT_ENVELOPE)),
        "probe result envelope intact in the run store"
    );
    for msg in &msgs {
        if let CommandMsg::UpdateNotebookCell { id, .. } = msg {
            assert!(
                !id.contains("-note-"),
                "compile wrote a run-output cell ({id}) — the authored ledger absorbed run output"
            );
        }
    }

    assert_eq!(second["applied"].as_u64(), Some(3), "re-Review compiles exactly the 3 authored steps");
    assert_no_rawcontent_ids(&authored, &second);
}

// ════════════════════════════════════════════════════════════════════════════
// 2. The LIVE repro path: clone template → materialized run persists outputs →
//    edit a step → resolve again → the cloned ledger is clean.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn clone_materialize_edit_resolve_keeps_ledger_clean() {
    ensure_db();
    let (group, board) = ("ledger-grp-b", "ledger-board-b");
    seed_board(group, board);

    // Clone the exact template from the live repro.
    let created = templates::clone_to_board("builtin:frameio-review-loop", board, group)
        .expect("clone template");
    let n = created.len();
    assert_eq!(n, 6, "review-loop template clones six steps");

    // Review #1 (compile the cloned board).
    let (first, _) = compile_and_apply(board);
    assert_eq!(first["applied"].as_u64(), Some(n as u64));

    // The materialized run executes; probe + proxy results persist to the run store.
    persist_run_output_note(board, "probe", 0.0, PROBE_RESULT_ENVELOPE);
    persist_run_output_note(board, "proxy", 1.0, PROXY_RESULT_ENVELOPE);

    // Edit the first step, then Review/resolve again — the live corruption trigger.
    let (authored_before, _) = ledger_split(board);
    assert_eq!(authored_before.len(), n);
    let mut first_cell = authored_before[0].clone();
    let edited_text = "ingest and probe the dailies from the C2C drop";
    first_cell.content = Some(edited_text.to_string());
    storage::cell_update(&first_cell).expect("author edit");

    let (second, _) = compile_and_apply(board);

    let (authored, notes) = ledger_split(board);
    assert_eq!(
        authored.len(),
        n,
        "re-resolve kept exactly the {n} cloned cells (no run output materialized as steps)"
    );
    assert_eq!(authored[0].content.as_deref(), Some(edited_text), "the author's edit survives");
    for cell in &authored[1..] {
        let content = cell.content.as_deref().unwrap_or_default();
        assert!(
            !content.trim_start().starts_with(['{', '[']),
            "authored cell {} absorbed raw run output: {content}",
            cell.id
        );
    }
    assert_eq!(notes.len(), 2, "run store intact");
    assert_no_rawcontent_ids(&authored, &second);
}

// ════════════════════════════════════════════════════════════════════════════
// 3. A board already polluted by the old build: compile SKIPS raw-result-blob
//    cells (never re-mints `rawcontent…` ids), and Reset ARCHIVES them —
//    restoring the board to its clean authored set with no data loss.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn compile_skips_and_reset_archives_polluted_cells() {
    ensure_db();
    let (group, board) = ("ledger-grp-c", "ledger-board-c");
    seed_board(group, board);

    let now = 1_700_000_000i64;
    storage::cell_insert_simple(
        &format!("{board}-s0"), board, "step", 0,
        Some("ingest and probe the dailies"), None, false, None, None, now, now,
    )
    .expect("clean step 0");
    // The polluted cell an OLD build left behind: an authored-kind cell whose content
    // IS a raw result envelope, with a garbage pipeline step id.
    let polluted_meta = r#"{"pipeline":{"step_id":"rawcontenttextn_container_movmp4m4a3gp3g2mj2","depends_on":[],"executor":"lens","output_format":"markdown","auto_advance":false,"state":{"status":"completed"},"tools":[],"notifications":[]}}"#;
    storage::cell_insert_simple(
        &format!("{board}-polluted"), board, "step", 1,
        Some(PROBE_RESULT_ENVELOPE), None, false, None, Some(polluted_meta), now, now,
    )
    .expect("polluted cell");
    storage::cell_insert_simple(
        &format!("{board}-s2"), board, "step", 2,
        Some("proxy for review"), None, false, None, None, now, now,
    )
    .expect("clean step 2");

    // Review on the polluted board: the blob cell is NOT a step — no new
    // `rawcontent…` id may be minted from it, and its row must not be rewritten.
    let (result, msgs) = compile_and_apply(board);
    assert_eq!(
        result["applied"].as_u64(),
        Some(2),
        "compile materializes only the two authored English steps"
    );
    for msg in &msgs {
        if let CommandMsg::UpdateNotebookCell { id, .. } = msg {
            assert_ne!(
                id, &format!("{board}-polluted"),
                "compile must not rewrite a raw-result-blob cell"
            );
        }
    }
    let configs = result["configs"].as_array().cloned().unwrap_or_default();
    for cfg in &configs {
        let sid = cfg["step_id"].as_str().unwrap_or_default();
        assert!(!sid.contains("rawcontent"), "no id minted from the blob: {sid}");
    }

    // Reset recovers the board: the polluted cell is ARCHIVED (kept — no data
    // loss), the clean authored cells stay.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CommandMsg>();
    pipeline::reset_pipeline(board, &tx).expect("reset");
    drop(tx);
    while let Ok(msg) = rx.try_recv() {
        apply_like_command_loop(&msg);
    }

    let cells = storage::cell_list_by_boards(&[board.to_string()]).expect("list");
    let polluted = cells
        .iter()
        .find(|c| c.id == format!("{board}-polluted"))
        .expect("polluted cell preserved (no data loss)");
    assert_eq!(
        polluted.cell_type, "archived",
        "reset archives a run-result-blob cell out of the authored ledger"
    );
    assert_eq!(
        polluted.content.as_deref(),
        Some(PROBE_RESULT_ENVELOPE),
        "archived content preserved verbatim"
    );
    let clean: Vec<_> = cells.iter().filter(|c| c.cell_type == "step").collect();
    assert_eq!(clean.len(), 2, "the clean authored set survives reset");
}
