//! A2 §5 — the ROLE chain link (T31, ships with the 2.7 commit): golden section
//! strings, placement between Producer and User, H4/H5 selection, byte-frozen
//! legacy output, slug-gated resolution, and producer+role coexistence.

use std::{
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{
    constitution::{self, ScopeChain},
    models::dto::NoteDTO,
    storage,
};

const T: &str = "role-link-group";
const BOARD: &str = "role-link-board";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("constitution_role_link.db");
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

fn put_role(id: &str, slug: &str, kind: &str, text: &str, at: i64) {
    storage::note_upsert(&NoteDTO {
        id: id.to_string(),
        board_id: T.to_string(), // role rules are GROUP-anchored (§1)
        tenant_id: T.to_string(),
        author_id: "node-role".to_string(),
        author_name: "Role".to_string(),
        text: text.to_string(),
        created_at: at,
        updated_at: at,
        scope: "role".to_string(),
        kind: kind.to_string(),
        anchor_kind: Some("role".to_string()),
        anchor_id: Some(slug.to_string()),
        origin_ref: None,
        payload: None,
        author_role: Some(slug.to_string()),
    })
    .expect("role note upsert");
}

fn put_plain(id: &str, anchor: &str, scope: &str, text: &str, at: i64) {
    storage::note_upsert(&NoteDTO {
        id: id.to_string(),
        board_id: anchor.to_string(),
        tenant_id: T.to_string(),
        author_id: "node-role".to_string(),
        author_name: "Role".to_string(),
        text: text.to_string(),
        created_at: at,
        updated_at: at,
        scope: scope.to_string(),
        kind: "constitution".to_string(),
        anchor_kind: None,
        anchor_id: None,
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

fn chain(role: Option<&str>, project: Option<&str>, producer: Option<&str>, user: Option<&str>) -> ScopeChain {
    ScopeChain {
        tenant_id: T.to_string(),
        group_id: Some(T.to_string()),
        project_id: project.map(str::to_string),
        board_id: BOARD.to_string(),
        workflow_id: None,
        producer_id: producer.map(str::to_string),
        role_id: role.map(str::to_string),
        user_id: user.map(str::to_string),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T31 — the role link merges between Producer and User with the golden strings
// `## Role: colorist` / `## Creative DNA (Role: colorist)` + the exact H4
// header; H5 with a project; legacy chains stay byte-identical; a role note
// resolves ONLY when the chain carries that slug; producer + role coexist.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn role_link_merges_between_producer_and_user() {
    ensure_db();
    put_role("t31-r1", "colorist", "constitution", "colorist grades warm", 1);
    put_role("t31-r2", "colorist", "creative-dna", "colorist dna: film emulation", 2);
    put_role("t31-r3", "sound", "constitution", "sound mixes to -14 LUFS", 3);
    put_plain("t31-p", "t31-producer", "producer", "producer wants tight cuts", 4);
    put_plain("t31-u", "t31-user", "user", "my personal rule", 5);
    put_plain("t31-b", BOARD, "board", "board baseline rule", 6);

    // Full chain: producer AND role simultaneously, plus the sovereign user.
    let full = chain(Some("colorist"), None, Some("t31-producer"), Some("t31-user"));
    let c = resolve_markdown(&full);
    let idx = |s: &str| c.find(s).unwrap_or_else(|| panic!("{s:?} missing:\n{c}"));

    // Golden strings, mechanical from push_section + the Role label.
    assert!(c.contains("## Role: colorist"), "golden role section:\n{c}");
    assert!(c.contains("## Creative DNA (Role: colorist)"), "golden role DNA section:\n{c}");
    // Placement: Producer → Role → User (most specific LAST).
    assert!(idx("producer wants tight cuts") < idx("colorist grades warm"), "producer → role:\n{c}");
    assert!(idx("colorist grades warm") < idx("my personal rule"), "role → user:\n{c}");
    // The exact H4 header (role, no project).
    assert!(
        c.starts_with("Precedence: user > role > producer > workflow > board > group > tenant — the most specific section wins on conflict."),
        "H4 verbatim:\n{c}"
    );
    // Slug isolation: the sound rule is NOT in a colorist chain.
    assert!(!c.contains("sound mixes"), "only the chain's slug resolves:\n{c}");

    // With a project link too → H5.
    let with_project = chain(Some("colorist"), Some("t31-ws"), Some("t31-producer"), None);
    let c5 = resolve_markdown(&with_project);
    assert!(
        c5.starts_with("Precedence: user > role > producer > workflow > board > project > group > tenant — the most specific section wins on conflict."),
        "H5 verbatim:\n{c5}"
    );

    // The sound chain resolves ITS slug only.
    let sound = chain(Some("sound"), None, None, None);
    let cs = resolve_markdown(&sound);
    assert!(cs.contains("sound mixes to -14 LUFS"));
    assert!(!cs.contains("colorist grades warm"));

    // No role link ⇒ no role section at all.
    let none = chain(None, None, None, None);
    assert!(!resolve_markdown(&none).contains("## Role:"), "role link absent when role_id None");
}

// ════════════════════════════════════════════════════════════════════════════
// T31 (frozen halves) — the 6-link and legacy 3-link outputs are byte-identical
// to the pre-change resolver for chains whose new links are None.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn legacy_chain_outputs_byte_identical() {
    ensure_db();
    const T2: &str = "role-frozen-group";
    let note = |id: &str, anchor: &str, scope: &str, text: &str, at: i64| {
        storage::note_upsert(&NoteDTO {
            id: id.to_string(),
            board_id: anchor.to_string(),
            tenant_id: T2.to_string(),
            author_id: "node-frozen".to_string(),
            author_name: "Frozen".to_string(),
            text: text.to_string(),
            created_at: at,
            updated_at: at,
            scope: scope.to_string(),
            kind: "constitution".to_string(),
            anchor_kind: None,
            anchor_id: None,
            origin_ref: None,
            payload: None,
            author_role: None,
        })
        .expect("upsert");
    };
    note("fr-t", T2, "tenant", "tenant frozen rule", 1);
    note("fr-b", "fr-board", "board", "board frozen rule", 2);
    note("fr-u", "fr-user", "user", "user frozen rule", 3);

    // Legacy 3-link chain: EXACT pre-change bytes (H1 + labeled sections).
    let conn = storage::db().lock().expect("db lock");
    let three = constitution::effective_notes_with(&conn, T2, Some(T2), "fr-board").expect("3-link");
    assert_eq!(
        three.constitution,
        "Precedence: board > group > tenant — the most specific section wins on conflict.\n\n\
         ## Tenant\ntenant frozen rule\n\n## Board\nboard frozen rule",
        "legacy 3-scope output byte-identical"
    );

    // 6-link chain (user set, project/role None): the frozen H2 + User last.
    let six = constitution::effective_notes_chain_with(
        &conn,
        &ScopeChain {
            tenant_id: T2.to_string(),
            group_id: Some(T2.to_string()),
            project_id: None,
            board_id: "fr-board".to_string(),
            workflow_id: None,
            producer_id: None,
            role_id: None,
            user_id: Some("fr-user".to_string()),
        },
    )
    .expect("6-link");
    assert_eq!(
        six.constitution,
        "Precedence: user > producer > workflow > board > group > tenant — the most specific section wins on conflict.\n\n\
         ## Tenant\ntenant frozen rule\n\n## Board\nboard frozen rule\n\n## User\nuser frozen rule",
        "6-link output byte-identical"
    );
}
