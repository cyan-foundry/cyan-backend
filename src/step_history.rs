//! B4 — per-STEP edit history: undo / redo for an authored step cell's content.
//!
//! Text-editor semantics, persisted (survives restarts, rides the local store):
//!   - an EDIT records the PREVIOUS content on the undo stack and clears redo;
//!   - UNDO moves current → redo, restores the newest undo entry;
//!   - REDO moves current → undo, restores the newest redo entry.
//! One table, two stacks (`stack` column). Pure functions over a `Connection`
//! — unit-testable with an in-memory DB; the FFI verbs own the global lock.

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS cell_edit_history (
            cell_id   TEXT NOT NULL,
            stack     TEXT NOT NULL CHECK (stack IN ('undo','redo')),
            seq       INTEGER NOT NULL,
            content   TEXT NOT NULL,
            edited_at INTEGER NOT NULL,
            PRIMARY KEY (cell_id, stack, seq)
        );",
    )?;
    Ok(())
}

fn top_seq(conn: &Connection, cell_id: &str, stack: &str) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT MAX(seq) FROM cell_edit_history WHERE cell_id=?1 AND stack=?2",
        params![cell_id, stack],
        |r| r.get::<_, Option<i64>>(0),
    )
    .optional()
    .map(|v| v.flatten())
    .map_err(Into::into)
}

fn push(conn: &Connection, cell_id: &str, stack: &str, content: &str, now: i64) -> Result<()> {
    let next = top_seq(conn, cell_id, stack)?.map(|s| s + 1).unwrap_or(0);
    conn.execute(
        "INSERT INTO cell_edit_history (cell_id, stack, seq, content, edited_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![cell_id, stack, next, content, now],
    )?;
    Ok(())
}

fn pop(conn: &Connection, cell_id: &str, stack: &str) -> Result<Option<String>> {
    let Some(seq) = top_seq(conn, cell_id, stack)? else {
        return Ok(None);
    };
    let content: String = conn.query_row(
        "SELECT content FROM cell_edit_history WHERE cell_id=?1 AND stack=?2 AND seq=?3",
        params![cell_id, stack, seq],
        |r| r.get(0),
    )?;
    conn.execute(
        "DELETE FROM cell_edit_history WHERE cell_id=?1 AND stack=?2 AND seq=?3",
        params![cell_id, stack, seq],
    )?;
    Ok(Some(content))
}

/// A human edit landed: the step's PREVIOUS content becomes undoable; any redo
/// future is invalidated (the classic branch-kill). No-op when nothing changed.
pub fn record_edit(
    conn: &Connection,
    cell_id: &str,
    old_content: &str,
    new_content: &str,
    now: i64,
) -> Result<()> {
    if old_content == new_content {
        return Ok(());
    }
    push(conn, cell_id, "undo", old_content, now)?;
    conn.execute(
        "DELETE FROM cell_edit_history WHERE cell_id=?1 AND stack='redo'",
        params![cell_id],
    )?;
    Ok(())
}

/// Undo one edit: returns the content to restore, or `None` when there is no
/// history (the caller leaves the cell untouched — never a fabricated state).
pub fn undo(conn: &Connection, cell_id: &str, current: &str, now: i64) -> Result<Option<String>> {
    let Some(prev) = pop(conn, cell_id, "undo")? else {
        return Ok(None);
    };
    push(conn, cell_id, "redo", current, now)?;
    Ok(Some(prev))
}

/// Redo one undone edit: returns the content to restore, or `None` when there
/// is nothing to redo.
pub fn redo(conn: &Connection, cell_id: &str, current: &str, now: i64) -> Result<Option<String>> {
    let Some(next) = pop(conn, cell_id, "redo")? else {
        return Ok(None);
    };
    push(conn, cell_id, "undo", current, now)?;
    Ok(Some(next))
}

/// How many undo / redo entries a cell has — the UI enables/disables buttons on this.
pub fn depths(conn: &Connection, cell_id: &str) -> Result<(i64, i64)> {
    let count = |stack: &str| -> Result<i64> {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM cell_edit_history WHERE cell_id=?1 AND stack=?2",
            params![cell_id, stack],
            |r| r.get(0),
        )?)
    };
    Ok((count("undo")?, count("redo")?))
}
