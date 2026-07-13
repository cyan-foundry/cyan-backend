//! A2 §5 — the `project` chain link (T29) + the five precedence headers (T30).

use std::{
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{
    constitution::{self, ScopeChain},
    models::dto::NoteDTO,
    storage,
};

const H1: &str = "Precedence: board > group > tenant — the most specific section wins on conflict.";
const H2: &str = "Precedence: user > producer > workflow > board > group > tenant — the most specific section wins on conflict.";
const H3: &str = "Precedence: user > producer > workflow > board > project > group > tenant — the most specific section wins on conflict.";
const H4: &str = "Precedence: user > role > producer > workflow > board > group > tenant — the most specific section wins on conflict.";
const H5: &str = "Precedence: user > role > producer > workflow > board > project > group > tenant — the most specific section wins on conflict.";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("constitution_project.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
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

fn put(tenant: &str, id: &str, anchor: &str, scope: &str, kind: &str, text: &str, at: i64) {
    storage::note_upsert(&NoteDTO {
        id: id.to_string(),
        board_id: anchor.to_string(),
        tenant_id: tenant.to_string(),
        author_id: "node-proj".to_string(),
        author_name: "Proj".to_string(),
        text: text.to_string(),
        created_at: at,
        updated_at: at,
        scope: scope.to_string(),
        kind: kind.to_string(),
        anchor_kind: (scope == "role").then(|| "role".to_string()),
        anchor_id: (scope == "role").then(|| "colorist".to_string()),
        origin_ref: None,
        payload: None,
        author_role: None,
    })
    .expect("note upsert");
}

fn resolve_markdown(chain: &ScopeChain) -> String {
    let conn = storage::db().lock().expect("db lock");
    constitution::effective_notes_chain_with(&conn, chain).expect("resolve").constitution
}

// ════════════════════════════════════════════════════════════════════════════
// T29 — a project-scope note (anchored at the board's workspace) merges AFTER
// Group and BEFORE Board, under the exact H3 header.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn project_scope_note_resolves_between_group_and_board() {
    ensure_db();
    const T: &str = "t29-group";
    put(T, "t29-g", T, "group", "constitution", "group project rule", 1);
    put(T, "t29-p", "t29-ws", "project", "constitution", "project creative brief rule", 2);
    put(T, "t29-b", "t29-board", "board", "constitution", "board project rule", 3);

    let chain = ScopeChain {
        tenant_id: T.to_string(),
        group_id: Some(T.to_string()),
        project_id: Some("t29-ws".to_string()),
        board_id: "t29-board".to_string(),
        workflow_id: None,
        producer_id: None,
        role_id: None,
        user_id: None,
    };
    let c = resolve_markdown(&chain);
    let idx = |s: &str| c.find(s).unwrap_or_else(|| panic!("{s:?} missing:\n{c}"));

    assert!(c.contains("## Project"), "project section labeled:\n{c}");
    assert!(idx("group project rule") < idx("project creative brief rule"), "group → project:\n{c}");
    assert!(idx("project creative brief rule") < idx("board project rule"), "project → board:\n{c}");
    assert!(c.starts_with(H3), "H3 verbatim (project, no role):\n{c}");
}

// ════════════════════════════════════════════════════════════════════════════
// T30 — all five §5 headers selected top-down by CHAIN SHAPE; H1/H2 stay
// byte-identical to the pre-A2 strings when project_id/role_id are None.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn precedence_headers_h1_to_h5() {
    ensure_db();
    const T: &str = "t30-group";
    // One board rule so every chain shape emits a non-empty merge (headers only
    // render over sections).
    put(T, "t30-b", "t30-board", "board", "constitution", "board header rule", 1);

    let base = ScopeChain {
        tenant_id: T.to_string(),
        group_id: Some(T.to_string()),
        project_id: None,
        board_id: "t30-board".to_string(),
        workflow_id: None,
        producer_id: None,
        role_id: None,
        user_id: None,
    };

    // H1 — all extended links None (frozen 3-scope header, byte-identical).
    assert!(resolve_markdown(&base).starts_with(H1), "H1 frozen");

    // H2 — any of workflow/producer/user Some, project + role None (frozen).
    let mut h2 = base.clone();
    h2.user_id = Some("t30-user".to_string());
    assert!(resolve_markdown(&h2).starts_with(H2), "H2 frozen");
    let mut h2b = base.clone();
    h2b.producer_id = Some("t30-producer".to_string());
    assert!(resolve_markdown(&h2b).starts_with(H2), "H2 via producer");

    // H3 — project Some, role None.
    let mut h3 = base.clone();
    h3.project_id = Some("t30-ws".to_string());
    assert!(resolve_markdown(&h3).starts_with(H3), "H3");

    // H4 — role Some, project None.
    let mut h4 = base.clone();
    h4.role_id = Some("colorist".to_string());
    assert!(resolve_markdown(&h4).starts_with(H4), "H4");

    // H5 — role AND project Some (top row, first match).
    let mut h5 = base.clone();
    h5.role_id = Some("colorist".to_string());
    h5.project_id = Some("t30-ws".to_string());
    h5.user_id = Some("t30-user".to_string());
    assert!(resolve_markdown(&h5).starts_with(H5), "H5 wins top-down");
}
