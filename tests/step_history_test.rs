//! B4 — per-step edit undo/redo semantics (pure, in-memory).

use cyan_backend::step_history as sh;
use rusqlite::Connection;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("db");
    sh::migrate(&conn).expect("migrate");
    conn
}

#[test]
fn undo_then_redo_round_trips_an_edit() {
    let conn = db();
    // v1 -> v2 (edit records v1)
    sh::record_edit(&conn, "c1", "v1", "v2", 1).unwrap();
    // undo from v2 -> back to v1; v2 parked on redo
    assert_eq!(sh::undo(&conn, "c1", "v2", 2).unwrap().as_deref(), Some("v1"));
    // redo from v1 -> forward to v2; v1 back on undo
    assert_eq!(sh::redo(&conn, "c1", "v1", 3).unwrap().as_deref(), Some("v2"));
    // and undo works again
    assert_eq!(sh::undo(&conn, "c1", "v2", 4).unwrap().as_deref(), Some("v1"));
}

#[test]
fn multi_edit_history_unwinds_in_order() {
    let conn = db();
    sh::record_edit(&conn, "c1", "v1", "v2", 1).unwrap();
    sh::record_edit(&conn, "c1", "v2", "v3", 2).unwrap();
    sh::record_edit(&conn, "c1", "v3", "v4", 3).unwrap();
    assert_eq!(sh::undo(&conn, "c1", "v4", 4).unwrap().as_deref(), Some("v3"));
    assert_eq!(sh::undo(&conn, "c1", "v3", 5).unwrap().as_deref(), Some("v2"));
    assert_eq!(sh::undo(&conn, "c1", "v2", 6).unwrap().as_deref(), Some("v1"));
    assert_eq!(sh::undo(&conn, "c1", "v1", 7).unwrap(), None, "history floor is honest");
}

#[test]
fn a_new_edit_kills_the_redo_branch() {
    let conn = db();
    sh::record_edit(&conn, "c1", "v1", "v2", 1).unwrap();
    assert_eq!(sh::undo(&conn, "c1", "v2", 2).unwrap().as_deref(), Some("v1"));
    // Editing from the undone state invalidates the v2 future.
    sh::record_edit(&conn, "c1", "v1", "v1b", 3).unwrap();
    assert_eq!(sh::redo(&conn, "c1", "v1b", 4).unwrap(), None, "redo branch was killed");
    assert_eq!(sh::undo(&conn, "c1", "v1b", 5).unwrap().as_deref(), Some("v1"));
}

#[test]
fn identical_content_records_nothing() {
    let conn = db();
    sh::record_edit(&conn, "c1", "same", "same", 1).unwrap();
    let (u, r) = sh::depths(&conn, "c1").unwrap();
    assert_eq!((u, r), (0, 0), "a no-change save never pollutes history");
}

#[test]
fn cells_are_isolated() {
    let conn = db();
    sh::record_edit(&conn, "c1", "a1", "a2", 1).unwrap();
    sh::record_edit(&conn, "c2", "b1", "b2", 2).unwrap();
    assert_eq!(sh::undo(&conn, "c2", "b2", 3).unwrap().as_deref(), Some("b1"));
    assert_eq!(sh::undo(&conn, "c1", "a2", 4).unwrap().as_deref(), Some("a1"));
}
