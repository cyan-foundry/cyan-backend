//! Notes SCOPE + KIND (feat/notes-constitution) — the constitution foundation.
//!
//! Every note gains a **scope** (`tenant` | `group` | `board`) and a **kind**
//! (`constitution` | `preference` | `editor-note`), ADDITIVE to the ROUND8 §W2 LWW
//! ledger: legacy rows and legacy wire payloads read back as `board`/`editor-note`,
//! board notes keep behaving exactly as before, and every new query carries the
//! tenant. `board_id` doubles as the SCOPE ANCHOR: the board id for board scope, the
//! group id for group scope, the tenant id for tenant scope (tenant == group id in
//! this engine).
//!
//! No live deps: pure storage + digest path, synchronous assertions on `storage::*`.

use std::path::Path;
use std::sync::Once;

use cyan_backend::models::dto::{self, NoteDTO};
use cyan_backend::{anti_entropy, storage};

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB. The `notes` table is seeded
/// in its PRE-scope/kind shape with one legacy row, so the additive column migration
/// is exercised against a DB that predates it.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_scope_kind.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
    });
}

/// Base tables the engine migrations assume exist, PLUS an old-shape `notes` table
/// (no `scope`/`kind` columns) holding one legacy row — the migration must add the
/// columns and default the legacy row to `board`/`editor-note`.
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
        -- OLD-shape notes table (pre-scope/kind) with one legacy row.
        CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY,
            board_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            author_id TEXT NOT NULL,
            author_name TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        INSERT OR IGNORE INTO notes
            (id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at)
        VALUES
            ('legacy-note-1', 'legacy-board', 'legacy-tenant', 'node-legacy',
             'Legacy Author', 'a pre-migration board note', 500, 500);
        "#,
    )?;
    Ok(())
}

/// Seed group → workspace → board. Tenant == group id, consistent with the engine.
fn seed_board(group: &str, board: &str) {
    let now = 1_700_000_000i64;
    let ws = format!("{group}-ws");
    storage::group_insert_simple(group, "Scope Group", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&ws, group, "Main").expect("workspace");
    storage::board_insert_simple(board, &ws, "Scope Board", now).expect("board");
}

/// A scoped note. `anchor` is the scope anchor carried in `board_id`.
#[allow(clippy::too_many_arguments)]
fn scoped_note(
    id: &str,
    anchor: &str,
    tenant: &str,
    scope: &str,
    kind: &str,
    text: &str,
    created: i64,
    updated: i64,
) -> NoteDTO {
    NoteDTO {
        id: id.to_string(),
        board_id: anchor.to_string(),
        tenant_id: tenant.to_string(),
        author_id: "node-author-1".to_string(),
        author_name: "Ada Lovelace".to_string(),
        text: text.to_string(),
        created_at: created,
        updated_at: updated,
        scope: scope.to_string(),
        kind: kind.to_string(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: None,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 1. The additive migration: a predating DB gains the columns; the legacy row
//    reads back as scope="board", kind="editor-note".
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn migration_defaults_legacy_rows_to_board_editor_note() {
    ensure_db();

    let legacy = storage::note_get("legacy-note-1")
        .expect("get")
        .expect("legacy row survives the migration");
    assert_eq!(legacy.scope, "board", "legacy note defaults to board scope");
    assert_eq!(legacy.kind, "editor-note", "legacy note defaults to editor-note kind");
    assert_eq!(legacy.text, "a pre-migration board note", "legacy content untouched");
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Scope + kind persist through the LWW upsert and read back on every path.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn scoped_note_round_trips_scope_and_kind() {
    ensure_db();
    let (group, board) = ("sk-rt-grp", "sk-rt-board");
    seed_board(group, board);

    // A board-scoped constitution note…
    let n = scoped_note(
        "sk-rt-1", board, group, "board", "constitution",
        "Never trim the sponsor tag", 1000, 1000,
    );
    assert!(storage::note_upsert(&n).expect("upsert"), "first put applies");

    let got = storage::note_get("sk-rt-1").expect("get").expect("present");
    assert_eq!(got.scope, "board");
    assert_eq!(got.kind, "constitution");

    // …also visible through the board listing (a board note stays a board note).
    let listed = storage::note_list_by_board(board, group).expect("list");
    let row = listed.iter().find(|x| x.id == "sk-rt-1").expect("listed");
    assert_eq!(row.kind, "constitution", "kind survives the list path");

    // A group-scoped preference note anchored at the GROUP id.
    let g = scoped_note(
        "sk-rt-2", group, group, "group", "preference",
        "Producer prefers cuts on action", 1001, 1001,
    );
    assert!(storage::note_upsert(&g).expect("upsert group note"));
    let got = storage::note_get("sk-rt-2").expect("get").expect("present");
    assert_eq!(got.scope, "group");
    assert_eq!(got.kind, "preference");
}

// ════════════════════════════════════════════════════════════════════════════
// 3. Scope/kind ride the same LWW lane: a newer edit updates them, a stale one
//    cannot clobber them.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn scope_kind_are_lww_with_the_row() {
    ensure_db();
    let (group, board) = ("sk-lww-grp", "sk-lww-board");
    seed_board(group, board);

    let v1 = scoped_note("sk-lww", board, group, "board", "editor-note", "v1", 1000, 1000);
    assert!(storage::note_upsert(&v1).expect("v1"));

    // A newer edit promotes the note to a constitution entry.
    let v2 = scoped_note("sk-lww", board, group, "board", "constitution", "v2", 1000, 2000);
    assert!(storage::note_upsert(&v2).expect("v2"), "newer edit applies");
    let row = storage::note_get("sk-lww").expect("get").expect("present");
    assert_eq!(row.kind, "constitution", "newer kind wins");

    // A stale write cannot demote it back.
    let stale = scoped_note("sk-lww", board, group, "board", "editor-note", "v3", 1000, 1500);
    assert!(!storage::note_upsert(&stale).expect("stale"), "stale write is a no-op");
    let row = storage::note_get("sk-lww").expect("get").expect("present");
    assert_eq!(row.kind, "constitution", "stale write must not clobber kind");
    assert_eq!(row.text, "v2", "stale write must not clobber text");
}

// ════════════════════════════════════════════════════════════════════════════
// 4. note_list_scoped: tenant-scoped, anchor-scoped, kind-scoped — and a tenant
//    NEVER reads another tenant's notes even with the anchor id known.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn note_list_scoped_filters_and_isolates_tenants() {
    ensure_db();
    let (tenant_a, board_a) = ("sk-iso-a", "sk-iso-a-board");
    let (tenant_b, board_b) = ("sk-iso-b", "sk-iso-b-board");
    seed_board(tenant_a, board_a);
    seed_board(tenant_b, board_b);

    storage::note_upsert(&scoped_note(
        "sk-iso-1", tenant_a, tenant_a, "tenant", "constitution", "A: -14 LUFS", 1, 1,
    ))
    .expect("a tenant note");
    storage::note_upsert(&scoped_note(
        "sk-iso-2", board_a, tenant_a, "board", "constitution", "A: board rule", 2, 2,
    ))
    .expect("a board note");
    storage::note_upsert(&scoped_note(
        "sk-iso-3", tenant_b, tenant_b, "tenant", "constitution", "B: -16 LUFS", 1, 1,
    ))
    .expect("b tenant note");

    // Kind + scope + anchor + tenant all filter.
    let a_tenant = storage::note_list_scoped(tenant_a, "tenant", tenant_a, "constitution")
        .expect("list a tenant");
    assert_eq!(a_tenant.len(), 1);
    assert_eq!(a_tenant[0].id, "sk-iso-1");

    let a_board = storage::note_list_scoped(tenant_a, "board", board_a, "constitution")
        .expect("list a board");
    assert_eq!(a_board.len(), 1);
    assert_eq!(a_board[0].id, "sk-iso-2");

    // Kind filters: no preference notes exist for tenant A.
    let a_prefs = storage::note_list_scoped(tenant_a, "tenant", tenant_a, "preference")
        .expect("list a prefs");
    assert!(a_prefs.is_empty(), "kind filter must exclude other kinds");

    // Tenant isolation: tenant B asking with tenant A's anchor id gets NOTHING.
    let cross = storage::note_list_scoped(tenant_b, "tenant", tenant_a, "constitution")
        .expect("cross list");
    assert!(cross.is_empty(), "a tenant never reads another tenant's notes");
}

// ════════════════════════════════════════════════════════════════════════════
// 5. Wire compat: a legacy JSON payload (no scope/kind) deserializes to the
//    defaults; the new fields round-trip.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn legacy_wire_payload_defaults_scope_and_kind() {
    let legacy_json = r#"{
        "id": "wire-1", "board_id": "b1", "tenant_id": "t1",
        "author_id": "n1", "author_name": "Ada", "text": "old peer note",
        "created_at": 10, "updated_at": 10
    }"#;
    let n: NoteDTO = serde_json::from_str(legacy_json).expect("legacy payload deserializes");
    assert_eq!(n.scope, "board", "missing scope defaults to board");
    assert_eq!(n.kind, "editor-note", "missing kind defaults to editor-note");

    let round = serde_json::to_string(&n).expect("serialize");
    let back: NoteDTO = serde_json::from_str(&round).expect("round trip");
    assert_eq!(back.scope, "board");
    assert_eq!(back.kind, "editor-note");
}

// ════════════════════════════════════════════════════════════════════════════
// 6. Vocab: the closed scope/kind sets are validated.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn scope_and_kind_vocab_are_closed() {
    // LENS_AI_NOTES P1: the scope chain grows to workflow/producer/user (user
    // innermost, most specific). Still a CLOSED vocabulary — garbage rejects.
    for s in ["tenant", "group", "board", "workflow", "producer", "user"] {
        assert!(dto::note_scope_valid(s), "{s} is a valid scope");
    }
    for s in ["", "Board", "workspace", "cell", "zzz", "User"] {
        assert!(!dto::note_scope_valid(s), "{s:?} must be rejected");
    }
    // LENS_AI_NOTES P1: `creative-dna` carries producer/house/director/studio/
    // genre/feel/episodic material at any scope.
    for k in ["constitution", "preference", "editor-note", "decision", "creative-dna"] {
        assert!(dto::note_kind_valid(k), "{k} is a valid kind");
    }
    for k in ["", "Constitution", "note", "editor note", "zzz", "creative_dna"] {
        assert!(!dto::note_kind_valid(k), "{k:?} must be rejected");
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 7. Sync visibility: group/tenant-anchored notes are counted in the group's
//    anti-entropy digest (they must converge like board notes).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn group_anchored_notes_ride_the_group_digest() {
    ensure_db();
    let (group, board) = ("sk-dig-grp", "sk-dig-board");
    seed_board(group, board);

    let (base_count, base_hash) = anti_entropy::group_digest(group);

    // A group-scoped constitution note is anchored at the GROUP id — no board row
    // points at it, but it must still be visible to the sweep.
    storage::note_upsert(&scoped_note(
        "sk-dig-1", group, group, "group", "constitution", "house rule", 100, 100,
    ))
    .expect("group note");

    let (c1, h1) = anti_entropy::group_digest(group);
    assert_eq!(c1, base_count + 1, "group-anchored note counted in the digest");
    assert_ne!(h1, base_hash, "group-anchored note changes the digest hash");
}

// ════════════════════════════════════════════════════════════════════════════
// 8. Board listing is UNCHANGED for board notes: a group-anchored note never
//    shows up in a board's note list (additive — board notes don't break).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn group_anchored_notes_stay_out_of_board_listings() {
    ensure_db();
    let (group, board) = ("sk-list-grp", "sk-list-board");
    seed_board(group, board);

    storage::note_upsert(&scoped_note(
        "sk-list-g", group, group, "group", "constitution", "group-wide", 1, 1,
    ))
    .expect("group note");
    storage::note_upsert(&scoped_note(
        "sk-list-b", board, group, "board", "editor-note", "board-local", 2, 2,
    ))
    .expect("board note");

    let listed = storage::note_list_by_board(board, group).expect("list");
    let ids: Vec<&str> = listed.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"sk-list-b"), "board note listed as before");
    assert!(
        !ids.contains(&"sk-list-g"),
        "group-anchored note must not appear in a board listing"
    );
}
