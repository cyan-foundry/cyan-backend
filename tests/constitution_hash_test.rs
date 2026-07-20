//! A2 §5 — the `constitution.v1` hash (T32, T33, T32b, T32c): pinned constant,
//! contributing-edit sensitivity, chain discrimination, the empty-vs-Err seam,
//! and the typed-rows 1:1 alignment (the C-A6-A2 contract, D-A2.25).
//!
//! Each test owns its own TENANT (tests share the process-global DB and run in
//! parallel — tenant isolation is the resolver's own boundary, reused here).

use std::{
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{
    constitution::{self, ScopeChain},
    models::dto::NoteDTO,
    storage,
};
use serde_json::json;

/// THE pinned `constitution.v1` constant over the FIXED T32 fixture — the A6
/// distillation-cache tripwire: any change to part ordering/content/traversal
/// flips this and REQUIRES a `.v2` bump. (`constitution_hard_test.rs` rebuilds
/// the same fixture and asserts the SAME constant — 2.11 must not move it.)
const PINNED_HASH: &str = "29ba88a60d45bc8e865b661b3460fdb35773a73a68def7cf2aa8eaad306735a9";

const T32_GROUP: &str = "hash-group";
const T32_WORKSPACE: &str = "hash-ws";
const T32_BOARD: &str = "hash-board";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("constitution_hash.db");
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

#[allow(clippy::too_many_arguments)]
fn put_full(
    tenant: &str,
    id: &str,
    anchor: &str,
    scope: &str,
    kind: &str,
    text: &str,
    at: i64,
    payload: Option<serde_json::Value>,
    author_role: Option<&str>,
) {
    storage::note_upsert(&NoteDTO {
        id: id.to_string(),
        board_id: anchor.to_string(),
        tenant_id: tenant.to_string(),
        author_id: "node-hash".to_string(),
        author_name: "Hash".to_string(),
        text: text.to_string(),
        created_at: at,
        updated_at: at,
        scope: scope.to_string(),
        kind: kind.to_string(),
        anchor_kind: (scope == "role").then(|| "role".to_string()),
        anchor_id: (scope == "role").then(|| "colorist".to_string()),
        origin_ref: None,
        payload,
        author_role: author_role.map(str::to_string),
    })
    .expect("note upsert");
}

fn put(tenant: &str, id: &str, anchor: &str, scope: &str, kind: &str, text: &str, at: i64) {
    put_full(tenant, id, anchor, scope, kind, text, at, None, None);
}

/// Re-implements the §5 length-prefix discipline over the EXPECTED part
/// sequence written in the test — the oracle the pinned constant is checked
/// against, so a silent traversal change cannot re-pin itself.
fn hash_of_parts(parts: &[String]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"constitution.v1");
    for p in parts {
        hasher.update(&[0u8]);
        hasher.update(&(p.len() as u64).to_le_bytes());
        hasher.update(p.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn resolve(chain: &ScopeChain) -> constitution::ResolvedConstitution {
    let conn = storage::db().lock().expect("db lock");
    constitution::resolve_with_provenance(&conn, chain).expect("resolve")
}

fn chain(tenant: &str, group: Option<&str>, project: Option<&str>, board: &str) -> ScopeChain {
    ScopeChain {
        tenant_id: tenant.to_string(),
        group_id: group.map(str::to_string),
        project_id: project.map(str::to_string),
        board_id: board.to_string(),
        workflow_id: None,
        producer_id: None,
        role_id: None,
        user_id: None,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T32 — the hash equals ONE pinned 64-hex constant over the fixed fixture;
// a contributing edit flips it; an editor-note write does NOT; re-resolving is
// deterministic (hash + contributing order).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn hash_pinned_and_flips_on_contributing_edit() {
    ensure_db();
    put(T32_GROUP, "hash-n1", T32_GROUP, "tenant", "constitution", "studio: never crop the logo", 100);
    put(T32_GROUP, "hash-n2", T32_BOARD, "board", "creative-dna", "hand-made feel", 110);
    put(T32_GROUP, "hash-n3", T32_BOARD, "board", "preference", "prefer 23.976 fps", 120);
    let chain = chain(T32_GROUP, Some(T32_GROUP), Some(T32_WORKSPACE), T32_BOARD);

    // The EXPECTED part sequence, written out: chain_canonical, then the
    // kind-major traversal — constitution pass (per link: constitution rows
    // then creative-dna rows), then the preference pass.
    let expected_parts = vec![
        format!(
            "tenant={T32_GROUP}\u{1f}group={T32_GROUP}\u{1f}project={T32_WORKSPACE}\u{1f}board={T32_BOARD}\u{1f}workflow=-\u{1f}producer=-\u{1f}role=-\u{1f}user=-"
        ),
        "tenant\u{1f}constitution\u{1f}hash-n1\u{1f}100".to_string(),
        "board\u{1f}creative-dna\u{1f}hash-n2\u{1f}110".to_string(),
        "board\u{1f}preference\u{1f}hash-n3\u{1f}120".to_string(),
    ];
    let expected = hash_of_parts(&expected_parts);
    assert_eq!(expected, PINNED_HASH, "the oracle recomputes the pinned constant");

    let resolved = resolve(&chain);
    assert_eq!(resolved.hash, PINNED_HASH, "resolve hashes to the pinned constant");
    assert_eq!(
        resolved.contributing.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
        vec!["hash-n1", "hash-n2", "hash-n3"],
        "kind-major contributing order"
    );

    // Determinism: a second resolve is byte-identical.
    let again = resolve(&chain);
    assert_eq!(again.hash, resolved.hash);
    assert_eq!(again.contributing, resolved.contributing);

    // A NON-contributing kind never moves it.
    put(T32_GROUP, "hash-editor", T32_BOARD, "board", "editor-note", "random editor note", 130);
    assert_eq!(resolve(&chain).hash, PINNED_HASH, "editor-note writes never flip the hash");

    // A contributing edit flips it (LWW bumps updated_at).
    put(T32_GROUP, "hash-n2", T32_BOARD, "board", "creative-dna", "hand-made feel", 111);
    assert_ne!(resolve(&chain).hash, PINNED_HASH, "a contributing edit flips the hash");
}

// ════════════════════════════════════════════════════════════════════════════
// T33 — identical text over chains differing only in role_id hashes
// differently (the chain_canonical part).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn hash_distinguishes_chains_over_identical_text() {
    ensure_db();
    const T: &str = "hash33-group";
    put(T, "h33-n1", T, "tenant", "constitution", "identical text", 100);

    let plain = chain(T, Some(T), None, "hash33-board");
    let mut with_role = plain.clone();
    with_role.role_id = Some("colorist".to_string());

    let a = resolve(&plain);
    let b = resolve(&with_role);
    assert_ne!(a.hash, b.hash, "role_id alone separates the hashes");
}

// ════════════════════════════════════════════════════════════════════════════
// T32b — the Rust-seam empty-vs-Err discrimination (D-A2.21): an EMPTY board
// resolves Ok with markdown "" + a REAL hash over chain_canonical alone; the
// hash still differs across chains; a resolver Err is Err — never a hash.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn empty_resolve_has_real_hash_distinct_from_absent() {
    ensure_db();

    let empty_chain = chain("empty-tenant", None, None, "empty-board");
    let resolved = resolve(&empty_chain);
    assert_eq!(resolved.constitution, "", "empty board resolves to the empty string");
    assert!(resolved.contributing.is_empty());
    assert!(resolved.notes.is_empty());
    let expected = hash_of_parts(&[
        "tenant=empty-tenant\u{1f}group=-\u{1f}project=-\u{1f}board=empty-board\u{1f}workflow=-\u{1f}producer=-\u{1f}role=-\u{1f}user=-".to_string(),
    ]);
    assert_eq!(resolved.hash, expected, "a REAL hash over chain_canonical + zero parts");

    // Different chain ⇒ different empty-hash (the chain_canonical part).
    let mut other = empty_chain.clone();
    other.board_id = "empty-board-2".to_string();
    assert_ne!(resolve(&other).hash, resolved.hash);

    // A resolver Err is Err — never a hash: resolve against a connection whose
    // schema has no notes table at all.
    let bare = rusqlite::Connection::open_in_memory().expect("mem conn");
    let err = constitution::resolve_with_provenance(&bare, &empty_chain);
    assert!(err.is_err(), "missing store is Err, never an empty-with-hash");
}

// ════════════════════════════════════════════════════════════════════════════
// T32c — the typed rows align 1:1 with contributing (kind-major), carry
// payload/author_role verbatim + byte-equal text, and link_index == §1's
// 0-based Chain pos column (one convention).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn structured_notes_align_one_to_one_with_contributing() {
    ensure_db();
    const T: &str = "t32c-group";
    const B: &str = "t32c-board";

    // A 3-scope fixture (group / role / board), typed fields set.
    let payload = json!({"v": 1, "rule": "loudness", "value": "-14 LUFS", "category": "technical"});
    put_full(T, "t32c-g", T, "group", "constitution", "group rule — must hold", 200, Some(payload.clone()), Some("producer"));
    put_full(T, "t32c-r", T, "role", "constitution", "colorist craft rule", 210, None, Some("colorist"));
    put_full(T, "t32c-b", B, "board", "constitution", "board rule", 220, None, None);

    let mut c = chain(T, Some(T), None, B);
    c.role_id = Some("colorist".to_string());
    let resolved = resolve(&c);

    assert_eq!(resolved.notes.len(), resolved.contributing.len(), "1:1 by construction");
    for (n, cc) in resolved.notes.iter().zip(resolved.contributing.iter()) {
        assert_eq!(n.id, cc.id, "notes[i] and contributing[i] describe the SAME row");
        assert_eq!(n.scope, cc.scope);
        assert_eq!(n.kind, cc.kind);
        assert_eq!(n.updated_at, cc.updated_at);
    }

    // Kind-major, CHAIN order: group (pos 1) → board (pos 3) → role (pos 6).
    assert_eq!(
        resolved.notes.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
        vec!["t32c-g", "t32c-b", "t32c-r"],
        "chain-ordered constitution pass"
    );

    let by_id = |id: &str| resolved.notes.iter().find(|n| n.id == id).expect("row");
    // Typed fields verbatim + byte-equal text.
    let g = by_id("t32c-g");
    assert_eq!(g.payload.as_ref(), Some(&payload), "payload carried verbatim");
    assert_eq!(g.author_role.as_deref(), Some("producer"));
    assert_eq!(g.text, "group rule — must hold", "text byte-equal to the stored row");
    // link_index == §1's Chain pos: group 1, board 3, role 6.
    assert_eq!(g.link_index, 1);
    assert_eq!(by_id("t32c-b").link_index, 3);
    assert_eq!(by_id("t32c-r").link_index, 6);
}
