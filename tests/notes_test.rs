//! W2 — Notes as a board-level, authored, LWW ledger (ROUND8 §W2).
//!
//! Notes own their store and their sync stream — they are NOT notebook cells. Each
//! `Note { id, board_id, tenant_id, author_id, author_name, text, created_at,
//! updated_at }` is editable; conflict resolution is **LWW on `updated_at`** with an
//! idempotent **upsert-by-id** (so snapshot apply stays idempotent). Notes ride the
//! existing anti-entropy **digest** so they converge exactly like chats, and every
//! row/query carries `tenant_id`.
//!
//! No live deps: pure storage + digest path. Every assertion is synchronous on the
//! engine's own `storage::*` (no waits needed). Multi-process convergence lives in
//! `tests/substrate_notes_mp.rs`.

use std::path::Path;
use std::sync::Once;

use cyan_backend::models::dto::NoteDTO;
use cyan_backend::{anti_entropy, storage};

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB with the base schema the
/// engine migrations assume exist.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
    });
}

/// Base tables the engine migrations assume exist. Run once before `storage::init_db`.
/// (`notes` is created by the engine migration — deliberately NOT seeded here, so the
/// migration is exercised on a DB that predates it.)
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

/// Seed group → workspace → board, returning the board id. Tenant == group id (the
/// board's group is its tenant, consistent with the rest of the engine).
fn seed_board(group: &str, board: &str) {
    let now = 1_700_000_000i64;
    let ws = format!("{group}-ws");
    storage::group_insert_simple(group, "Notes Group", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&ws, group, "Main").expect("workspace");
    storage::board_insert_simple(board, &ws, "Notes Board", now).expect("board");
}

fn note(id: &str, board: &str, tenant: &str, text: &str, created: i64, updated: i64) -> NoteDTO {
    NoteDTO {
        id: id.to_string(),
        board_id: board.to_string(),
        tenant_id: tenant.to_string(),
        author_id: "node-author-1".to_string(),
        author_name: "Ada Lovelace".to_string(),
        text: text.to_string(),
        created_at: created,
        updated_at: updated,
        // feat/notes-constitution: the pre-scope defaults — these tests assert the
        // original board-note behavior, which must stay byte-for-byte intact.
        scope: "board".to_string(),
        kind: "editor-note".to_string(),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 1. A note carries its author identity + creation/update timestamps.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn note_carries_author_and_timestamps() {
    ensure_db();
    let (group, board) = ("note-auth-grp", "note-auth-board");
    seed_board(group, board);

    let n = note("n-auth-1", board, group, "Deliver the master to Contido", 1000, 1000);
    assert!(storage::note_upsert(&n).expect("upsert"), "first put applies");

    let got = storage::note_list_by_board(board, group).expect("list");
    let row = got.iter().find(|x| x.id == "n-auth-1").expect("note present");

    assert_eq!(row.board_id, board, "note is board-level");
    assert_eq!(row.tenant_id, group, "note carries its tenant");
    assert_eq!(row.author_id, "node-author-1", "note carries the author id (XaeroID)");
    assert_eq!(row.author_name, "Ada Lovelace", "note carries the resolved author name");
    assert_eq!(row.text, "Deliver the master to Contido");
    assert_eq!(row.created_at, 1000, "creation time recorded");
    assert_eq!(row.updated_at, 1000, "update time recorded");
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Editing is LWW by `updated_at`; older/equal writes are no-ops (idempotent).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn note_update_is_lww_by_updated_at() {
    ensure_db();
    let (group, board) = ("note-lww-grp", "note-lww-board");
    seed_board(group, board);

    // Initial note at t=1000.
    assert!(storage::note_upsert(&note("n-lww", board, group, "v1", 1000, 1000)).expect("v1"));

    // A NEWER edit (t=2000) wins — text replaced, created_at preserved.
    assert!(
        storage::note_upsert(&note("n-lww", board, group, "v2-newer", 1000, 2000)).expect("v2"),
        "newer update applies"
    );
    let after_newer = storage::note_list_by_board(board, group).expect("list");
    let row = after_newer.iter().find(|x| x.id == "n-lww").expect("note");
    assert_eq!(row.text, "v2-newer", "newer write wins");
    assert_eq!(row.updated_at, 2000, "updated_at advanced");
    assert_eq!(row.created_at, 1000, "created_at preserved across edits");

    // An OLDER edit (t=1500 < 2000) is dropped — LWW keeps the latest.
    assert!(
        !storage::note_upsert(&note("n-lww", board, group, "v3-stale", 1000, 1500)).expect("v3"),
        "older update is a no-op"
    );
    let after_stale = storage::note_list_by_board(board, group).expect("list");
    let row = after_stale.iter().find(|x| x.id == "n-lww").expect("note");
    assert_eq!(row.text, "v2-newer", "stale write must NOT clobber the newer value");
    assert_eq!(row.updated_at, 2000, "updated_at unchanged by stale write");

    // Re-applying the SAME write (equal updated_at) is an idempotent no-op — this is
    // what makes snapshot apply / anti-entropy repair converge without churn.
    assert!(
        !storage::note_upsert(&note("n-lww", board, group, "v2-newer", 1000, 2000)).expect("dup"),
        "equal-timestamp re-apply is idempotent (no change)"
    );
    let count = storage::note_list_by_board(board, group).expect("list").len();
    assert_eq!(count, 1, "upsert-by-id never duplicates");
}

// ════════════════════════════════════════════════════════════════════════════
// 3. Notes are their OWN store — not notebook cells.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn notes_are_not_notebook_cells() {
    ensure_db();
    let (group, board) = ("note-decouple-grp", "note-decouple-board");
    seed_board(group, board);

    storage::note_upsert(&note("n-dec-1", board, group, "a board note", 10, 10)).expect("note");

    // The note lives in the notes store…
    let notes = storage::note_list_by_board(board, group).expect("notes");
    assert_eq!(notes.len(), 1, "note recorded in the notes store");

    // …and creates ZERO notebook cells (fully decoupled from the cell/step model).
    let cells = storage::cell_list_by_boards(&[board.to_string()]).expect("cells");
    assert!(cells.is_empty(), "a note must not create any notebook cell");
}

// ════════════════════════════════════════════════════════════════════════════
// 4. Notes are included in the anti-entropy digest (converge like chats).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn notes_included_in_digest() {
    ensure_db();
    let (group, board) = ("note-digest-grp", "note-digest-board");
    seed_board(group, board);

    let (base_count, base_hash) = anti_entropy::group_digest(group);

    // Adding a note advances the digest (count AND hash) — the sweep can now detect a
    // peer that is missing it.
    storage::note_upsert(&note("n-dig-1", board, group, "digest me", 100, 100)).expect("note");
    let (c1, h1) = anti_entropy::group_digest(group);
    assert_eq!(c1, base_count + 1, "note counted in the group digest");
    assert_ne!(h1, base_hash, "note changes the digest hash");

    // Editing the note (LWW bump of updated_at) changes the digest hash but not the
    // count — divergence is still detected, convergence is to the latest value.
    storage::note_upsert(&note("n-dig-1", board, group, "edited", 100, 200)).expect("edit");
    let (c2, h2) = anti_entropy::group_digest(group);
    assert_eq!(c2, c1, "an edit does not change the item count");
    assert_ne!(h2, h1, "an edit (new updated_at) flips the digest hash");

    // Re-applying the identical state leaves the digest stable (convergent, no churn).
    storage::note_upsert(&note("n-dig-1", board, group, "edited", 100, 200)).expect("reapply");
    let (c3, h3) = anti_entropy::group_digest(group);
    assert_eq!((c2, h2), (c3, h3), "digest stable after idempotent re-apply");
}

// ════════════════════════════════════════════════════════════════════════════
// 6. Notes are tenant-scoped — a query carries the tenant and never crosses it.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn note_tenant_scoped() {
    ensure_db();
    // Two tenants (groups), each with its own board.
    let (tenant_a, board_a) = ("note-tenant-a", "note-tenant-a-board");
    let (tenant_b, board_b) = ("note-tenant-b", "note-tenant-b-board");
    seed_board(tenant_a, board_a);
    seed_board(tenant_b, board_b);

    storage::note_upsert(&note("n-a", board_a, tenant_a, "A's note", 1, 1)).expect("a");
    storage::note_upsert(&note("n-b", board_b, tenant_b, "B's note", 1, 1)).expect("b");

    // Each tenant sees only its own note.
    let a = storage::note_list_by_board(board_a, tenant_a).expect("list a");
    assert_eq!(a.len(), 1, "tenant A sees its own note");
    assert_eq!(a[0].id, "n-a");

    let b = storage::note_list_by_board(board_b, tenant_b).expect("list b");
    assert_eq!(b.len(), 1, "tenant B sees its own note");
    assert_eq!(b[0].id, "n-b");

    // The query is tenant-enforced: asking for board A under tenant B returns nothing —
    // a note never leaks across the tenant boundary even when the board id is known.
    let cross = storage::note_list_by_board(board_a, tenant_b).expect("cross-tenant list");
    assert!(cross.is_empty(), "tenant B must not read tenant A's notes");
}
