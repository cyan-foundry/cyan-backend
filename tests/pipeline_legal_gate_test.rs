//! PLAN 3.9 — T56: a `Manual: legal clearance…` cell (the A5 `inject_legal_gate`
//! landing shape) compiles to executor "manual" (the `Manual:` prefix is
//! load-bearing — zero backend change), and the FOLLOWING deliver step
//! `depends_on` it (the linear-chain DAG the deterministic compile lays out).

use std::{
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{models::commands::CommandMsg, storage};

const GROUP: &str = "legal-gate-group";
const BOARD: &str = "legal-gate-board";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pipeline_legal_gate.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        storage::group_insert_simple(GROUP, "Legal", "folder", "#00AEEF").expect("group");
        storage::workspace_insert_simple("legal-gate-ws", GROUP, "General").expect("ws");
        storage::board_insert_simple(BOARD, "legal-gate-ws", "Legal Cut", 1).expect("board");
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
// T56 — the injected gate parks; the deliver step depends on it.
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn manual_legal_step_compiles_to_human_gate() {
    ensure_db();

    let cell = |id: &str, order: i32, text: &str| {
        storage::cell_insert_simple(id, BOARD, "step", order, Some(text), None, false, None, None, 10, 10)
            .expect("cell insert");
    };
    cell("t56-cut", 0, "Cut the promo per the creative brief — creative cut (manual)");
    cell(
        "t56-gate",
        1,
        "Manual: legal clearance — producer confirms all legal-clearance notes are cleared before delivery (pending: music: needle drop)",
    );
    cell("t56-deliver", 2, "Package and deliver the master per delivery house rules");

    let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
    let result = cyan_backend::pipeline::compile_via_llm(BOARD, &command_tx)
        .await
        .expect("compile");
    assert_eq!(result["applied"].as_u64(), Some(3));

    let mut metas: Vec<(String, serde_json::Value)> = Vec::new();
    while let Ok(msg) = command_rx.try_recv() {
        if let CommandMsg::UpdateNotebookCell { id, metadata_json, .. } = msg {
            metas.push((id, serde_json::from_str(&metadata_json.expect("meta")).expect("json")));
        }
    }
    assert_eq!(metas.len(), 3);
    let pipeline_of = |cell_id: &str| {
        metas
            .iter()
            .find(|(id, _)| id == cell_id)
            .map(|(_, m)| m["pipeline"].clone())
            .unwrap_or_else(|| panic!("{cell_id} compiled"))
    };

    // The `Manual:` prefix is load-bearing: the gate parks as executor manual.
    let gate = pipeline_of("t56-gate");
    assert_eq!(gate["executor"], serde_json::json!("manual"), "the legal gate is a HUMAN gate");
    let gate_step_id = gate["step_id"].as_str().expect("gate step id").to_string();

    // The FOLLOWING deliver step depends_on the gate (the linear-chain DAG).
    let deliver = pipeline_of("t56-deliver");
    let depends: Vec<&str> = deliver["depends_on"]
        .as_array()
        .expect("depends_on")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert_eq!(depends, vec![gate_step_id.as_str()], "deliver waits on the legal gate");
}
