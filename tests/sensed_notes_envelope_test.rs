//! A2 (live 2026-07-08): the review rail reads the LEDGER, but sensed reviewer
//! comments only reach the ledger when the sensed file is a registered derived
//! proxy — so a plain sense left the rail at "Nothing here." while the notes
//! existed as board `timecode_note` cells. `board_envelope` now merges those
//! cells as synthetic read-only `note` entries, deduped by comment id.

use cyan_backend::changelist::{self, ChangeEntry};
use cyan_backend::review_loop;
use cyan_backend::review_state as rv;
use rusqlite::Connection;
use serde_json::json;

const T: &str = "tenant-notes";
const A: &str = "asset-notes";
const B: &str = "main";
const BOARD: &str = "board-notes-1";

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    cyan_backend::asset_registry::migrate(&conn).expect("migrate assets");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS notebook_cells (id TEXT PRIMARY KEY, board_id TEXT,
             cell_type TEXT, cell_order INTEGER, content TEXT, output TEXT,
             collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
             created_at INTEGER, updated_at INTEGER);",
    )
    .expect("notebook_cells");
    conn
}

fn insert_note_cell(conn: &Connection, id: &str, order_ms: i64, content: &str) {
    conn.execute(
        "INSERT INTO notebook_cells (id, board_id, cell_type, cell_order, content, metadata_json)
         VALUES (?1, ?2, 'timecode_note', ?3, ?4, ?5)",
        rusqlite::params![
            id,
            BOARD,
            order_ms,
            content,
            json!({"note_type": "review_comment", "author": "Riya"}).to_string()
        ],
    )
    .expect("insert note cell");
}

fn ledger_note(source_ref: &str, tc_in: i64) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: A.to_string(),
        tenant_id: T.to_string(),
        branch: None,
        track: None,
        tc_in,
        tc_out: None,
        kind: "note".to_string(),
        op: None,
        params: json!({}),
        intent: format!("ledger note {source_ref}"),
        source: Some("frameio".to_string()),
        source_ref: Some(source_ref.to_string()),
        author: Some("Riya".to_string()),
        role: Some("reviewer".to_string()),
        proposed_by: Some("human".to_string()),
        created_at: 0,
        state: String::new(),
        active: true,
        approved_by: None,
        approved_at: None,
        supersedes: None,
        superseded_by: None,
        seq: 0,
        depends_on: None,
        version_ref: None,
        outcome: None,
        updated_at: 0,
        updated_by: None,
    }
}

#[test]
fn envelope_merges_sensed_notes_and_dedupes_against_ledger() {
    let conn = db();
    // Ledger already carries comment c2; the board also has note cells for a
    // NEW sensed comment c1 and (again) c2, plus a manual note that must NOT
    // be promoted into the review rail.
    changelist::append(&conn, A, B, ledger_note("c2", 48)).expect("append c2");
    insert_note_cell(&conn, "frameio-comment-c1", 2500, "tighten the open (frame 60)");
    insert_note_cell(&conn, "frameio-comment-c2", 2000, "duplicate of the ledger note");
    insert_note_cell(&conn, "user-note-1", 1000, "my own scratch note");

    let env = review_loop::board_envelope(&conn, BOARD, T, A, B).expect("envelope");
    let entries = env["entries"].as_array().expect("entries");

    let sensed: Vec<_> = entries
        .iter()
        .filter(|e| e["id"].as_str().unwrap_or_default().starts_with("frameio-comment-"))
        .collect();
    assert_eq!(sensed.len(), 1, "only the un-ledgered comment merges, got {sensed:?}");
    let c1 = sensed[0];
    assert_eq!(c1["source_ref"], "c1");
    assert_eq!(c1["kind"], "note");
    assert_eq!(c1["intent"], "tighten the open (frame 60)");
    assert_eq!(c1["author"], "Riya");
    // c2 appears exactly once (the REAL ledger row, not the note cell).
    let c2s: Vec<_> = entries
        .iter()
        .filter(|e| e["source_ref"].as_str() == Some("c2"))
        .collect();
    assert_eq!(c2s.len(), 1, "ledgered comment must not duplicate");
    // The manual scratch note never enters the review rail.
    assert!(
        entries.iter().all(|e| e["id"] != "user-note-1"),
        "manual notes must not be promoted"
    );
}
