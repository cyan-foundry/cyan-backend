//! A2 — the FFI contracts (T36-T38): `cyan_constitution_resolved` (the
//! on-device preview, SYN-8), `cyan_constitution_effective` (the ONE cloud
//! verb, SYN-7), and `cyan_note_list_scoped` (SYN-6, engine-derived anchors).

use std::{
    ffi::{CStr, CString},
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{ffi::core as ffi, models::dto::NoteDTO, storage};

const NODE: &str = "node-resolved-test";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("constitution_resolved.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        // The engine identity the `user` anchors derive from (the test seam the
        // FFI identity path exposes — `cyan_set_xaero_id` persists NODE_ID).
        let id = CString::new(NODE).expect("cstring");
        assert!(ffi::cyan_set_xaero_id(id.as_ptr()));
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
fn put(id: &str, anchor: &str, tenant: &str, scope: &str, kind: &str, text: &str, at: i64) {
    storage::note_upsert(&NoteDTO {
        id: id.to_string(),
        board_id: anchor.to_string(),
        tenant_id: tenant.to_string(),
        author_id: NODE.to_string(),
        author_name: "Resolved".to_string(),
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
}

fn json_call(out: *mut std::os::raw::c_char) -> serde_json::Value {
    assert!(!out.is_null());
    let s = unsafe { CStr::from_ptr(out) }.to_string_lossy().to_string();
    ffi::cyan_free_string(out);
    serde_json::from_str(&s).unwrap_or_else(|e| panic!("verb returns JSON ({e}): {s}"))
}

fn resolved(request: serde_json::Value) -> serde_json::Value {
    let arg = CString::new(request.to_string()).expect("cstring");
    json_call(ffi::cyan_constitution_resolved(arg.as_ptr()))
}

fn list_scoped(board: &str, scope: &str, kind: Option<&str>) -> serde_json::Value {
    let b = CString::new(board).expect("cstring");
    let s = CString::new(scope).expect("cstring");
    let k = kind.map(|k| CString::new(k).expect("cstring"));
    json_call(ffi::cyan_note_list_scoped(
        b.as_ptr(),
        s.as_ptr(),
        k.as_ref().map(|k| k.as_ptr()).unwrap_or(std::ptr::null()),
    ))
}

// ════════════════════════════════════════════════════════════════════════════
// T36 — the preview verb: defaults resolve (include_user defaults TRUE on this
// surface); an invalid production_role is a typed error; the response carries
// the hash + the ordered contributing tuples.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn constitution_resolved_ffi_contract() {
    ensure_db();
    // Own tenant/board (tests share the process DB and run in parallel).
    let (g, ws, b) = ("t36-group", "t36-ws", "t36-board");
    storage::group_insert_simple(g, "T36", "folder", "#00AEEF").expect("group");
    storage::workspace_insert_simple(ws, g, "General").expect("ws");
    storage::board_insert_simple(b, ws, "Cut", 1).expect("board");
    put("t36-g", g, g, "group", "constitution", "group preview rule", 1);
    put("t36-b", b, g, "board", "constitution", "board preview rule", 2);
    // A sovereign user rule, anchored at THIS node under the board's tenant —
    // include_user defaults TRUE on the preview surface, so it resolves.
    put("t36-u", NODE, g, "user", "constitution", "user preview rule", 3);

    let out = resolved(serde_json::json!({ "board_id": b }));
    assert!(out.get("error").is_none(), "defaults resolve: {out}");
    let md = out["markdown"].as_str().expect("markdown");
    assert!(md.contains("group preview rule"));
    assert!(md.contains("user preview rule"), "include_user defaults true HERE: {md}");
    assert_eq!(out["hash"].as_str().map(str::len), Some(64), "64-hex hash");
    let contributing = out["contributing"].as_array().expect("contributing");
    let ids: Vec<&str> = contributing.iter().filter_map(|c| c["id"].as_str()).collect();
    assert_eq!(ids, vec!["t36-g", "t36-b", "t36-u"], "ordered contributing tuples");

    // Invalid production_role ⇒ typed error, never silently ignored.
    let err = resolved(serde_json::json!({ "board_id": b, "production_role": "dj" }));
    assert_eq!(err["error"], serde_json::json!("invalid_production_role"));
    assert_eq!(err["given"], serde_json::json!("dj"));
    assert_eq!(err["allowed"].as_array().map(Vec::len), Some(7));
}

// ════════════════════════════════════════════════════════════════════════════
// T37 — the cloud verb: {markdown, hash, contributing_ids}; a user rule NEVER
// appears (include_user: false by construction); an empty board still hashes.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn constitution_effective_ffi_contract() {
    ensure_db();
    let (g, ws, b_id) = ("t37-group", "t37-ws", "t37-board");
    storage::group_insert_simple(g, "T37", "folder", "#00AEEF").expect("group");
    storage::workspace_insert_simple(ws, g, "General").expect("ws");
    storage::board_insert_simple(b_id, ws, "Cut", 1).expect("board");
    put("t37-b", b_id, g, "board", "constitution", "board effective rule", 4);
    put("t37-u", NODE, g, "user", "constitution", "user effective rule", 5);

    let b = CString::new(b_id).expect("cstring");
    let out = json_call(ffi::cyan_constitution_effective(b.as_ptr()));
    let md = out["markdown"].as_str().expect("markdown");
    assert!(md.contains("board effective rule"));
    assert!(!md.contains("## User"), "NO user section by construction: {md}");
    assert!(!md.contains("user effective rule"));
    assert_eq!(out["hash"].as_str().map(str::len), Some(64));
    let ids: Vec<&str> =
        out["contributing_ids"].as_array().expect("ids").iter().filter_map(|v| v.as_str()).collect();
    assert!(ids.contains(&"t37-b"));
    assert!(!ids.contains(&"t37-u"));

    // Empty board ⇒ markdown "" WITH a real hash (empty ≠ unknown).
    storage::board_insert_simple("t37-empty-board", ws, "Empty", 1).expect("board");
    let b = CString::new("t37-empty-board").expect("cstring");
    let empty = json_call(ffi::cyan_constitution_effective(b.as_ptr()));
    assert_eq!(empty["markdown"], serde_json::json!(""));
    assert_eq!(empty["hash"].as_str().map(str::len), Some(64));
}

// ════════════════════════════════════════════════════════════════════════════
// T38 — the scoped-list verb derives anchors engine-side: tenant/group at the
// board's group id; board at the board; user at the node id (tenant = node id);
// kind NULL ⇒ all kinds; an unloadable scope ⇒ {"error":"unsupported_scope"}.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn note_list_scoped_ffi_derives_anchors() {
    ensure_db();
    let (g, ws, b) = ("t38-group", "t38-ws", "t38-board");
    storage::group_insert_simple(g, "T38", "folder", "#00AEEF").expect("group");
    storage::workspace_insert_simple(ws, g, "General").expect("ws");
    storage::board_insert_simple(b, ws, "Cut", 1).expect("board");
    put("t38-t", g, g, "tenant", "constitution", "tenant scoped row", 6);
    put("t38-g", g, g, "group", "constitution", "group scoped row", 7);
    put("t38-g2", g, g, "group", "preference", "group pref row", 8);
    put("t38-b", b, g, "board", "editor-note", "board scoped row", 9);
    // The user row is SELF-tenanted (tenant = anchor = node id — write-side
    // stamping for a no-group sovereign write).
    put("t38-u", NODE, NODE, "user", "editor-note", "user scoped row", 10);

    let tenant_rows = list_scoped(b, "tenant", Some("constitution"));
    let ids = |v: &serde_json::Value| -> Vec<String> {
        v.as_array()
            .expect("array")
            .iter()
            .map(|n| n["id"].as_str().expect("id").to_string())
            .collect()
    };
    assert_eq!(ids(&tenant_rows), vec!["t38-t"], "tenant rows anchored at the board's group");

    let group_rows = list_scoped(b, "group", Some("constitution"));
    assert_eq!(ids(&group_rows), vec!["t38-g"]);

    // kind NULL ⇒ every kind at the anchor.
    let group_all = list_scoped(b, "group", None);
    assert_eq!(ids(&group_all), vec!["t38-g", "t38-g2"], "NULL kind lists all kinds");

    let board_rows = list_scoped(b, "board", None);
    assert_eq!(ids(&board_rows), vec!["t38-b"]);

    let user_rows = list_scoped(b, "user", None);
    assert_eq!(ids(&user_rows), vec!["t38-u"], "user rows at the node id with tenant = node id");

    let err = list_scoped(b, "workflow", None);
    assert_eq!(err["error"], serde_json::json!("unsupported_scope"));
}
