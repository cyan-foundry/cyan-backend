//! TWO-WAY REVIEW glue (fix #6): a `frameio.list_comments` step result becomes
//! PER-COMMENT timecoded notes on the board (`timecode_seconds = frame / fps`,
//! fps read back from the board's persisted probe output), so the Video face
//! renders each reviewer note AT its frame. Re-sensing upserts (stable note id
//! per comment id) — never duplicates. Non-sense tools stay untouched.

use std::sync::Once;

use cyan_backend::models::commands::CommandMsg;
use cyan_backend::{pipeline_executor, storage};
use serde_json::json;
use tokio::sync::mpsc;

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sense.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir);
    });
}

fn init_base_schema(db_path: &std::path::Path) -> Result<(), rusqlite::Error> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS groups (id TEXT PRIMARY KEY, name TEXT, icon TEXT, color TEXT, created_at INTEGER);
         CREATE TABLE IF NOT EXISTS workspaces (id TEXT PRIMARY KEY, group_id TEXT, name TEXT, created_at INTEGER);
         CREATE TABLE IF NOT EXISTS objects (id TEXT PRIMARY KEY, workspace_id TEXT, board_id TEXT, type TEXT,
             name TEXT, local_path TEXT, hash TEXT, size INTEGER, added_by TEXT, created_at INTEGER, deleted INTEGER DEFAULT 0);
         CREATE TABLE IF NOT EXISTS notebook_cells (id TEXT PRIMARY KEY, board_id TEXT, cell_type TEXT,
             cell_order INTEGER, content TEXT, output TEXT, collapsed INTEGER DEFAULT 0, height REAL,
             metadata_json TEXT, created_at INTEGER, updated_at INTEGER);",
    )?;
    Ok(())
}

fn note_rows(board: &str) -> Vec<(String, i64, String)> {
    let conn = storage::db().lock().expect("lock");
    let mut stmt = conn
        .prepare(
            "SELECT id, cell_order, content FROM notebook_cells
             WHERE board_id=?1 AND cell_type='timecode_note' ORDER BY cell_order",
        )
        .expect("prepare");
    let rows = stmt
        .query_map(rusqlite::params![board], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?))
        })
        .expect("query");
    rows.flatten().collect()
}

#[test]
fn sensed_comments_become_per_comment_timecoded_notes() {
    ensure_db();
    let board = "sense-board-1";

    // The promo workflow probes before it senses — the probe's persisted output
    // carries the clip's frame rate (ffprobe rational), which anchors seconds.
    {
        let conn = storage::db().lock().expect("lock");
        conn.execute(
            "INSERT OR REPLACE INTO notebook_cells
               (id, board_id, cell_type, cell_order, content, output, created_at, updated_at)
             VALUES ('probe-cell', ?1, 'step', 0, 'Probe the master',
                     '{\"video\":{\"frame_rate\":\"30/1\"},\"format\":{}}', 1, 1)",
            rusqlite::params![board],
        )
        .expect("probe output cell");
    }

    let step = cyan_backend::mcp_host::McpTool {
        plugin_id: "frameio".into(),
        tool: "list_comments".into(),
        args: json!({ "account_id": "a", "file_id": "file-xyz" }),
    };
    // THE LIVE SHAPE: the MCP tool-result ENVELOPE with the V4 comments payload
    // ({"data":[…]}) JSON-encoded inside content[].text (island DB, 2026-07-08).
    let payload = json!({ "data": [
        { "id": "c-60", "text": "trim the logo here", "timestamp": 60,
          "owner": { "name": "Rick the Reviewer" } },
        { "id": "c-gen", "text": "love the pacing overall" },
    ]});
    let result = json!({
        "content": [{ "type": "text", "text": payload.to_string() }],
        "isError": false
    });
    let (tx, _rx) = mpsc::unbounded_channel::<CommandMsg>();

    pipeline_executor::ingest_sensed_comments(board, "step-sense", &step, &result, &tx);

    let notes = note_rows(board);
    assert_eq!(notes.len(), 2, "one note PER comment, got {notes:?}");
    // frame 60 @ 30fps = 2.0s → cell_order = seconds*1000 = 2000.
    let anchored = notes.iter().find(|(id, _, _)| id == "frameio-comment-c-60").expect("anchored note");
    assert_eq!(anchored.1, 2000, "the note lands AT its frame (2.0s @ 30fps)");
    assert!(anchored.2.contains("trim the logo"));
    // The general comment (no anchor) sits at the start of media.
    let general = notes.iter().find(|(id, _, _)| id == "frameio-comment-c-gen").expect("general note");
    assert_eq!(general.1, 0);

    // RE-SENSE: the same comments upsert onto the SAME note ids — no dupes.
    pipeline_executor::ingest_sensed_comments(board, "step-sense", &step, &result, &tx);
    assert_eq!(note_rows(board).len(), 2, "re-sensing must not duplicate notes");
}

#[test]
fn non_sense_tools_produce_no_comment_notes() {
    ensure_db();
    let board = "sense-board-2";
    let step = cyan_backend::mcp_host::McpTool {
        plugin_id: "frameio".into(),
        tool: "create_comment".into(),
        args: json!({ "file_id": "f" }),
    };
    let result = json!({ "data": [ { "id": "x", "text": "y", "timestamp": 1 } ] });
    let (tx, _rx) = mpsc::unbounded_channel::<CommandMsg>();
    pipeline_executor::ingest_sensed_comments(board, "s", &step, &result, &tx);
    assert!(note_rows(board).is_empty(), "create_comment is the PUSH verb, not sense");
}
