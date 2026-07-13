//! PLAN 3.8 (the INVESTIGATE) — T55: the COMPILE path's metadata write MERGES
//! into the cell's existing `metadata_json` (it parses the stored blob and sets
//! only the `pipeline` / `mcp_tool*` / `await_sense` keys), so a device-landed
//! `origin_ref` (gen:/note: provenance from `addStep`) SURVIVES compile.
//!
//! Investigation record (read-only pass over `src/pipeline.rs`, which another
//! workstream owns): BOTH compile paths merge — `apply_compiled_configs` and
//! the deterministic `compile_via_llm` each start from
//! `cell.metadata_json.and_then(parse).unwrap_or(json!({}))` and assign
//! individual keys. This test PINS that behavior green; if a refactor ever
//! replaces the parse-then-assign with a fresh object, T55 goes red and the
//! merge-write must be restored.

use std::{
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{models::commands::CommandMsg, storage};

const GROUP: &str = "meta-merge-group";
const BOARD: &str = "meta-merge-board";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pipeline_metadata_merge.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        storage::group_insert_simple(GROUP, "Merge", "folder", "#00AEEF").expect("group");
        storage::workspace_insert_simple("meta-merge-ws", GROUP, "General").expect("ws");
        storage::board_insert_simple(BOARD, "meta-merge-ws", "Merge Cut", 1).expect("board");
        let _ = DB_PATH.set(path);
        std::mem::forget(dir);
    });
}

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

// ════════════════════════════════════════════════════════════════════════════
// T55 — a cell landed with metadata_json {"origin_ref":"note:n1"} compiled ⇒
// the compile's UpdateNotebookCell metadata holds BOTH origin_ref AND pipeline.
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn compile_preserves_origin_ref_metadata() {
    ensure_db();

    // The addStep landing: one step cell whose metadata carries the provenance.
    storage::cell_insert_simple(
        "t55-cell-1",
        BOARD,
        "step",
        0,
        Some("Transcode the master to a delivery mezzanine"),
        None,
        false,
        None,
        Some(r#"{"origin_ref":"note:n1"}"#),
        10,
        10,
    )
    .expect("cell insert");

    let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
    let result = cyan_backend::pipeline::compile_via_llm(BOARD, &command_tx)
        .await
        .expect("compile");
    assert_eq!(result["applied"].as_u64(), Some(1));

    let mut merged: Option<serde_json::Value> = None;
    while let Ok(msg) = command_rx.try_recv() {
        if let CommandMsg::UpdateNotebookCell { id, metadata_json, .. } = msg
            && id == "t55-cell-1"
        {
            merged = Some(
                serde_json::from_str(&metadata_json.expect("metadata present")).expect("json"),
            );
        }
    }
    let merged = merged.expect("compile updated the cell");
    assert_eq!(
        merged["origin_ref"],
        serde_json::json!("note:n1"),
        "the device-landed provenance SURVIVES compile (the merge-write): {merged}"
    );
    assert!(
        merged.get("pipeline").is_some(),
        "…and the pipeline config landed beside it: {merged}"
    );
    assert_eq!(merged["pipeline"]["executor"], serde_json::json!("lens"));
}
