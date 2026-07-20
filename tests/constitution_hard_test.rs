//! A6 backend (PLAN 2.11) — the deterministic hard/soft classifier (T-A6-1,
//! T-A6-2): §9i category rules + the hard lexicon, order preservation over
//! A2's typed rows, the UNCHANGED `constitution.v1` pin, and the verb's
//! additive `"hard"` key.

use std::{
    ffi::{CStr, CString},
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{
    constitution::{self, ResolvedNote, ScopeChain},
    constitution_hard::classify_hard,
    ffi::core as ffi,
    models::dto::NoteDTO,
    storage,
};
use serde_json::json;

/// THE T32 pin, restated (constitution_hash_test.rs owns the fixture): 2.11 is
/// a PURE POST-PASS — the hash input set is untouched, so this constant must
/// not move.
const PINNED_HASH: &str = "29ba88a60d45bc8e865b661b3460fdb35773a73a68def7cf2aa8eaad306735a9";

const T32_GROUP: &str = "hash-group";
const T32_WORKSPACE: &str = "hash-ws";
const T32_BOARD: &str = "hash-board";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("constitution_hard.db");
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

fn rn(id: &str, kind: &str, text: &str, payload: Option<serde_json::Value>) -> ResolvedNote {
    ResolvedNote {
        id: id.to_string(),
        scope: "board".to_string(),
        kind: kind.to_string(),
        text: text.to_string(),
        payload,
        author_role: None,
        updated_at: 1,
        link_index: 3,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T-A6-1 — the §9i category rules, exact and in order.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn legal_category_always_hard_brand_never() {
    // §4.1 payload, category legal ⇒ HARD always (any text).
    let legal = rn(
        "h1",
        "constitution",
        "clear every needle drop",
        Some(json!({"v":1, "rule":"music", "value":"clear it", "category":"legal"})),
    );
    let hits = classify_hard(&[legal]);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].category, "legal");
    assert_eq!(hits[0].text, "clear every needle drop", "HARD text = the DTO text verbatim");

    // brand/creative ⇒ soft even WITH modals.
    let brand = rn(
        "h2",
        "constitution",
        "the logo must always be huge",
        Some(json!({"v":1, "rule":"logo", "value":"must always be huge", "category":"brand"})),
    );
    let creative = rn(
        "h3",
        "constitution",
        "never lose the handmade feel",
        Some(json!({"v":1, "rule":"feel", "value":"never lose it", "category":"creative"})),
    );
    assert!(classify_hard(&[brand, creative]).is_empty(), "brand/creative are soft by category");

    // technical + a number+unit token ⇒ HARD.
    let lufs = rn(
        "h4",
        "constitution",
        "mix to -14 LUFS",
        Some(json!({"v":1, "rule":"loudness", "value":"-14 LUFS", "category":"technical"})),
    );
    let hits = classify_hard(&[lufs]);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].category, "technical");

    // technical PROSE without the lexicon ⇒ soft.
    let prose = rn(
        "h5",
        "constitution",
        "keep the grade warm",
        Some(json!({"v":1, "rule":"grade", "value":"keep it warm and cozy", "category":"technical"})),
    );
    assert!(classify_hard(&[prose]).is_empty());

    // Payload-less + lexicon ⇒ HARD, category recorded "technical".
    let bare = rn("h6", "constitution", "Never crop the logo", None);
    let hits = classify_hard(&[bare]);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].category, "technical");

    // preference / creative-dna kinds ⇒ never hard, whatever the text.
    let pref = rn("h7", "preference", "always render at max quality", None);
    let dna = rn("h8", "creative-dna", "must feel like 16mm", None);
    assert!(classify_hard(&[pref, dna]).is_empty(), "non-constitution kinds are never hard");

    // v:2 payload falls to the TEXT rule (rule 4 — treated payload-less).
    let v2 = rn(
        "h9",
        "constitution",
        "deliver 3840x2160 only",
        Some(json!({"v":2, "category":"legal", "junk": true})),
    );
    let hits = classify_hard(&[v2]);
    assert_eq!(hits.len(), 1, "v2 payload treated payload-less; the text lexicon decides");
    assert_eq!(hits[0].category, "technical");
}

// ════════════════════════════════════════════════════════════════════════════
// T-A6-2 — the post-pass preserves the traversal order; the T32 pin is
// UNCHANGED (the hash input set never saw 2.11); the verb carries the additive
// "hard" key with {id, scope, category, text} entries.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn hard_post_pass_preserves_order_hash_unchanged() {
    ensure_db();

    // Rebuild the EXACT T32 fixture (ids/timestamps/chain pinned there).
    let put = |id: &str, anchor: &str, scope: &str, kind: &str, text: &str, at: i64| {
        storage::note_upsert(&NoteDTO {
            id: id.to_string(),
            board_id: anchor.to_string(),
            tenant_id: T32_GROUP.to_string(),
            author_id: "node-hard".to_string(),
            author_name: "Hard".to_string(),
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
    };
    put("hash-n1", T32_GROUP, "tenant", "constitution", "studio: never crop the logo", 100);
    put("hash-n2", T32_BOARD, "board", "creative-dna", "hand-made feel", 110);
    put("hash-n3", T32_BOARD, "board", "preference", "prefer 23.976 fps", 120);

    let chain = ScopeChain {
        tenant_id: T32_GROUP.to_string(),
        group_id: Some(T32_GROUP.to_string()),
        project_id: Some(T32_WORKSPACE.to_string()),
        board_id: T32_BOARD.to_string(),
        workflow_id: None,
        producer_id: None,
        role_id: None,
        user_id: None,
    };
    let resolved = {
        let conn = storage::db().lock().expect("db lock");
        constitution::resolve_with_provenance(&conn, &chain).expect("resolve")
    };
    assert_eq!(
        resolved.hash, PINNED_HASH,
        "the T32 constant is UNCHANGED by 2.11 — the hash input set never saw the post-pass"
    );

    // The post-pass output order == the notes traversal order (kind-major):
    // hash-n1 is the only hard hit here ("never crop" via the lexicon), and the
    // classifier walks the rows verbatim.
    let hard = classify_hard(&resolved.notes);
    assert_eq!(hard.len(), 1);
    assert_eq!(hard[0].id, "hash-n1");
    assert_eq!(hard[0].scope, "tenant");
    assert_eq!(hard[0].text, "studio: never crop the logo");

    // Order over a multi-hit slice: input order preserved verbatim.
    let a = rn("ord-1", "constitution", "must do A first", None);
    let b = rn("ord-2", "constitution", "never do B", None);
    let out = classify_hard(&[a, b]);
    assert_eq!(
        out.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(),
        vec!["ord-1", "ord-2"],
        "classify_hard preserves the traversal order"
    );

    // The VERB carries the additive "hard" key with the {id,scope,category,text}
    // schema (2.9 + 2.11 land in the same commit — this pins the joint shape).
    storage::group_insert_simple(T32_GROUP, "Hard", "folder", "#00AEEF").expect("group");
    storage::workspace_insert_simple(T32_WORKSPACE, T32_GROUP, "General").expect("ws");
    storage::board_insert_simple(T32_BOARD, T32_WORKSPACE, "Cut", 1).expect("board");
    let arg = CString::new(T32_BOARD).expect("cstring");
    let out = ffi::cyan_constitution_effective(arg.as_ptr());
    assert!(!out.is_null());
    let s = unsafe { CStr::from_ptr(out) }.to_string_lossy().to_string();
    ffi::cyan_free_string(out);
    let verb: serde_json::Value = serde_json::from_str(&s).expect("verb JSON");
    let hard = verb["hard"].as_array().expect("the additive hard key is an array");
    assert!(!hard.is_empty(), "the tenant lexicon rule classifies hard: {verb}");
    let entry = &hard[0];
    assert_eq!(entry["id"], serde_json::json!("hash-n1"));
    assert_eq!(entry["scope"], serde_json::json!("tenant"));
    assert_eq!(entry["category"], serde_json::json!("technical"));
    assert_eq!(entry["text"], serde_json::json!("studio: never crop the logo"));
}
