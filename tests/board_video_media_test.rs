//! A1 contract (plays-black fix): `board_video_media` hands the app ONLY an
//! absolute on-disk path or a real URI — NEVER a bare filename. A bare name
//! reads as neither local nor remote in the player, falls into the S3 presign
//! branch, and renders black (found live 2026-07-08, ROUND 2).

use std::sync::Once;

use cyan_backend::{pipeline_executor, storage};

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("videomedia.db");
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

fn bind_file(board: &str, name: &str, local_path: Option<&str>) {
    let conn = storage::db().lock().expect("lock");
    conn.execute(
        "INSERT OR REPLACE INTO objects (id, workspace_id, board_id, type, name, local_path, created_at)
         VALUES (?1, 'ws', ?2, 'file', ?3, ?4, 0)",
        rusqlite::params![format!("{board}-{name}"), board, name, local_path],
    )
    .expect("insert file object");
}

/// One test, scenarios in sequence: `CYAN_MEDIA_ROOT` is process-global, so the
/// cases that set/unset it must not interleave with each other.
#[test]
fn player_media_is_absolute_or_uri_never_bare() {
    ensure_db();

    // (1) env root SET + a bound bare clip name → master_uri joins the root
    //     (absolute), exactly what the tools receive.
    let root = tempfile::tempdir().expect("media root");
    let clip = root.path().join("clip.mp4");
    std::fs::write(&clip, b"not-really-mp4").expect("write clip");
    unsafe { std::env::set_var("CYAN_MEDIA_ROOT", root.path()) };
    let board = "bvm-board-envroot";
    bind_file(board, "clip.mp4", None);
    let v = pipeline_executor::board_video_media(board);
    let master = v["master_uri"].as_str().expect("master resolves under env root");
    assert!(
        master.starts_with('/') && master.ends_with("/clip.mp4"),
        "env-root case must yield an absolute path, got {master:?}"
    );

    // (2) env root UNSET + the clip present in the DEFAULT confined root
    //     (~/.cyan-phase3/media under a scratch HOME) → still absolute.
    unsafe { std::env::remove_var("CYAN_MEDIA_ROOT") };
    let home = tempfile::tempdir().expect("scratch home");
    let old_home = std::env::var("HOME").ok();
    unsafe { std::env::set_var("HOME", home.path()) };
    let default_root = home.path().join(".cyan-phase3").join("media");
    std::fs::create_dir_all(&default_root).expect("mk default root");
    std::fs::write(default_root.join("daily.mp4"), b"x").expect("write daily");
    let board2 = "bvm-board-defaultroot";
    bind_file(board2, "daily.mp4", None);
    let v2 = pipeline_executor::board_video_media(board2);
    let master2 = v2["master_uri"].as_str().expect("master resolves under default root");
    assert!(
        master2.starts_with('/') && master2.ends_with("/daily.mp4"),
        "default-root case must yield an absolute path, got {master2:?}"
    );

    // (3) env root UNSET + the clip NOWHERE on disk → master_uri is null,
    //     never the bare name (the plays-black bug shape).
    let board3 = "bvm-board-nowhere";
    bind_file(board3, "ghost.mp4", None);
    let v3 = pipeline_executor::board_video_media(board3);
    assert!(
        v3["master_uri"].is_null(),
        "an unresolvable clip must yield null, got {:?}",
        v3["master_uri"]
    );

    // Restore HOME for the rest of the process.
    match old_home {
        Some(h) => unsafe { std::env::set_var("HOME", h) },
        None => unsafe { std::env::remove_var("HOME") },
    }
}
