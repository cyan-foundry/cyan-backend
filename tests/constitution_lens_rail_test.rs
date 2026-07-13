//! A2 §5 — the cloud-rail resolve + the execute-wire send table (T34, T34b).
//!
//! NOTE (deferred half, file ownership): the pipeline_executor call-site swap —
//! the `try_db_read` guard, the `constitution_read_budget_missed` obs (send-table
//! row 1), and the `LensExecuteRequest.constitution_hash` field — lives in files
//! owned by another workstream this pass; rows 2/3 and the Rust-seam contract
//! are pinned HERE via `board_constitution_markdown_chain` + `execute_wire_pair`
//! + the `cyan_constitution_effective` verb.

use std::{
    ffi::{CStr, CString},
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{
    constitution::{self},
    models::dto::NoteDTO,
    storage,
};

const T: &str = "rail-group";
const BOARD_RULES: &str = "rail-board-rules";
const BOARD_EMPTY: &str = "rail-board-empty";
const NODE: &str = "node-rail-test";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("constitution_lens_rail.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        // The board → group edge the chain populates from.
        storage::group_insert_simple(T, "Rail", "folder", "#00AEEF").expect("group");
        storage::workspace_insert_simple("rail-ws", T, "General").expect("ws");
        storage::board_insert_simple(BOARD_RULES, "rail-ws", "Rules", 1).expect("board");
        storage::board_insert_simple(BOARD_EMPTY, "rail-ws", "Empty", 1).expect("board");
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
        "#,
    )?;
    Ok(())
}

fn put(id: &str, anchor: &str, scope: &str, kind: &str, text: &str, at: i64) {
    storage::note_upsert(&NoteDTO {
        id: id.to_string(),
        board_id: anchor.to_string(),
        tenant_id: T.to_string(),
        author_id: NODE.to_string(),
        author_name: "Rail".to_string(),
        text: text.to_string(),
        created_at: at,
        updated_at: at,
        scope: scope.to_string(),
        kind: kind.to_string(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: None,
        payload: None,
        author_role: None,
    })
    .expect("upsert");
}

fn call_effective(board: &str) -> serde_json::Value {
    let arg = CString::new(board).expect("cstring");
    let out = cyan_backend::ffi::core::cyan_constitution_effective(arg.as_ptr());
    assert!(!out.is_null());
    let s = unsafe { CStr::from_ptr(out) }.to_string_lossy().to_string();
    cyan_backend::ffi::core::cyan_free_string(out);
    serde_json::from_str(&s).expect("verb returns JSON")
}

// ════════════════════════════════════════════════════════════════════════════
// T34 — the cloud rail excludes the user link BY CONSTRUCTION; the on-device
// preview (include_user: true) contains it.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn lens_rail_excludes_user_link() {
    ensure_db();
    put("t34-b", BOARD_RULES, "board", "constitution", "board rail rule", 1);
    put("t34-u", NODE, "user", "constitution", "sovereign user rail rule", 2);

    let conn = storage::db().lock().expect("db lock");
    let rail = constitution::board_constitution_markdown_chain(&conn, BOARD_RULES).expect("rail");
    assert!(!rail.constitution.contains("## User"), "no user section on the cloud rail");
    assert!(!rail.constitution.contains("sovereign user rail rule"));
    assert!(
        !rail.contributing.iter().any(|c| c.id == "t34-u"),
        "the user note id is NOT in contributing on the cloud rail"
    );

    // The on-device preview WITH the user link (SYN-8's surface): resolve the
    // same board with include_user: true over the same connection.
    let opts = constitution::ResolveOpts { include_user: true, ..Default::default() };
    let chain = constitution::resolve_chain(&conn, BOARD_RULES, NODE, &opts);
    let preview = constitution::resolve_with_provenance(&conn, &chain).expect("preview");
    assert!(preview.constitution.contains("sovereign user rail rule"), "preview includes user");
    assert!(preview.contributing.iter().any(|c| c.id == "t34-u"));
}

// ════════════════════════════════════════════════════════════════════════════
// T34b (rows 2 + 3) — the send table: rules present ⇒ BOTH fields; resolved-
// empty ⇒ NEITHER field on the wire, while `cyan_constitution_effective` on the
// SAME board returns {"markdown":"", hash, "hard":[]} — empty and unknown are
// distinct on the verb, collapsed on the wire. (Row 1 — the budget-miss obs —
// rides the deferred pipeline_executor call site; see the module docs.)
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn execute_wire_send_table_pinned() {
    ensure_db();
    put("t34b-rule", BOARD_RULES, "board", "constitution", "wire rule present", 3);

    // Row 2 — resolved, markdown non-empty ⇒ BOTH fields, the pair atomic.
    {
        let conn = storage::db().lock().expect("db lock");
        let resolved =
            constitution::board_constitution_markdown_chain(&conn, BOARD_RULES).expect("resolve");
        let (md, hash) = constitution::execute_wire_pair(&resolved);
        assert!(md.as_deref().is_some_and(|m| m.contains("wire rule present")));
        assert_eq!(hash.as_deref(), Some(resolved.hash.as_str()), "hash travels WITH its markdown");
    }

    // Row 3 — resolved EMPTY ⇒ NEITHER field (live wire behavior preserved,
    // zero step-key churn)…
    let empty_hash;
    {
        let conn = storage::db().lock().expect("db lock");
        let resolved =
            constitution::board_constitution_markdown_chain(&conn, BOARD_EMPTY).expect("resolve");
        assert_eq!(resolved.constitution, "");
        empty_hash = resolved.hash.clone();
        let (md, hash) = constitution::execute_wire_pair(&resolved);
        assert_eq!(md, None, "resolved-empty stays ABSENT on the execute wire");
        assert_eq!(hash, None, "the pair is atomic — no hash without markdown");
    }

    // …while the VERB keeps empty and unknown structurally distinct: markdown
    // "" WITH a real hash + empty contributing + empty hard.
    let verb = call_effective(BOARD_EMPTY);
    assert_eq!(verb["markdown"], serde_json::json!(""));
    assert_eq!(verb["hash"], serde_json::json!(empty_hash), "the verb's hash is REAL over the chain");
    assert_eq!(verb["contributing_ids"], serde_json::json!([] as [&str; 0]));
    assert_eq!(verb["hard"], serde_json::json!([] as [&str; 0]));
    assert!(verb.get("error").is_none());
}
