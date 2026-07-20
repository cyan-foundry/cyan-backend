//! A2 §7 — the device-local `production_role` pref (T35): set/get round-trip,
//! empty-clears, invalid-rejects, and the sovereignty proof — no local_prefs
//! material ever enters the sync feed, the snapshot serializer, or the digest.

use std::{
    ffi::{CStr, CString},
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{anti_entropy, ffi::core as ffi, snapshot, storage};

const GROUP: &str = "prefs-group";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("local_prefs.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        storage::group_insert_simple(GROUP, "Prefs", "folder", "#00AEEF").expect("group");
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

fn set_role(role: &str) -> bool {
    let arg = CString::new(role).expect("cstring");
    ffi::cyan_set_production_role(arg.as_ptr())
}

fn get_role() -> String {
    let out = ffi::cyan_get_production_role();
    assert!(!out.is_null());
    let s = unsafe { CStr::from_ptr(out) }.to_string_lossy().to_string();
    ffi::cyan_free_string(out);
    s
}

// ════════════════════════════════════════════════════════════════════════════
// T35 — the pref is DEVICE-LOCAL: round-trips, clears, validates, and never
// enters any replication surface.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn production_role_pref_is_device_local() {
    ensure_db();

    // Replication baselines BEFORE any pref write.
    let feed_before = storage::note_list_for_sync(GROUP).expect("feed");
    let (digest_count_before, digest_hash_before) = anti_entropy::group_digest(GROUP);
    let frames_before =
        serde_json::to_string(&snapshot::build_snapshot_frames(GROUP, None).expect("frames"))
            .expect("encode");

    // Unset reads "".
    assert_eq!(get_role(), "");

    // Set/get round-trips; the vocab is enforced.
    assert!(set_role("colorist"), "valid slug accepted");
    assert_eq!(get_role(), "colorist");
    assert!(!set_role("dj"), "invalid role rejected");
    assert!(!set_role("agent"), "agent is authorship provenance, NOT a production role");
    assert_eq!(get_role(), "colorist", "invalid writes leave the pref untouched");

    // Last write wins (the XP-1 two-writer contract).
    assert!(set_role("producer"));
    assert_eq!(get_role(), "producer");

    // Empty string CLEARS.
    assert!(set_role(""));
    assert_eq!(get_role(), "");
    assert!(set_role("sound"));

    // Sovereignty: the sync feed, the digest, and the snapshot serializer are
    // ALL byte-identical to the pre-pref state — local_prefs is never wired in.
    let feed_after = storage::note_list_for_sync(GROUP).expect("feed");
    assert_eq!(feed_after.len(), feed_before.len(), "no pref material in the sync feed");
    let (digest_count_after, digest_hash_after) = anti_entropy::group_digest(GROUP);
    assert_eq!(digest_count_after, digest_count_before, "no pref entries in the digest");
    assert_eq!(digest_hash_after, digest_hash_before, "the digest hash never moved");
    let frames_after =
        serde_json::to_string(&snapshot::build_snapshot_frames(GROUP, None).expect("frames"))
            .expect("encode");
    assert_eq!(frames_after, frames_before, "the snapshot serializer output never moved");
    assert!(!frames_after.contains("local_prefs"));
    assert!(!frames_after.contains("production_role"));
}
