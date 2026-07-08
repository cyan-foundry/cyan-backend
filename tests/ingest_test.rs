//! STAGE 4 — ingest sources + per-asset pipeline materialization.
//!
//! Two rails, matching the module's design seam:
//!   * conn-level tests on isolated DBs (`ensure_schema` + the ingest/asset
//!     migrations on an in-memory or tempdir SQLite) for sources, scans, dedup
//!     and run materialization;
//!   * global-DB tests (the plugin_install_linkage pattern) for the pieces that
//!     ride process-global storage — the explicit-asset bind
//!     (`workflow_bind::bind_step_for_asset`) and the `cyan_ingest_command`
//!     JSON dispatch.

use std::sync::Once;

use cyan_backend::{ingest, storage, workflow_bind};
use cyan_backend::models::core::Group;
use rusqlite::Connection;

// ── conn-level rail ────────────────────────────────────────────────────────────

/// An isolated DB carrying the engine's REAL schema + migrations (the same two
/// steps the FFI init runs — `run_migrations` covers `objects.deleted` and the
/// changelist/asset/ingest tables).
fn conn_db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    cyan_backend::ensure_schema(&conn).expect("engine schema");
    storage::run_migrations(&conn).expect("storage migrations");
    conn
}

/// group → workspace → whiteboard rows on the isolated conn (FKs are enforced).
fn mk_board(conn: &Connection, group: &str, board: &str) {
    conn.execute(
        "INSERT INTO groups (id, name, icon, color, created_at) VALUES (?1, ?1, 'folder', '#00FFFF', 1)",
        [group],
    )
    .expect("group row");
    let ws = format!("{group}-ws");
    conn.execute(
        "INSERT INTO workspaces (id, group_id, name, created_at) VALUES (?1, ?2, 'Default', 1)",
        rusqlite::params![ws, group],
    )
    .expect("workspace row");
    conn.execute(
        "INSERT INTO objects (id, group_id, workspace_id, type, name, created_at) \
         VALUES (?1, ?2, ?3, 'whiteboard', ?1, 1)",
        rusqlite::params![board, group, ws],
    )
    .expect("board row");
}

#[test]
fn materialize_run_mints_per_asset_rows_with_tenant() {
    let conn = conn_db();
    mk_board(&conn, "g-runs", "b-runs");

    let r1 = ingest::materialize_run(&conn, "b-runs", "hash-a").expect("run a");
    let r2 = ingest::materialize_run(&conn, "b-runs", "hash-b").expect("run b");
    assert_ne!(r1.run_id, r2.run_id, "each asset gets ITS OWN run");
    assert_eq!(r1.status, "materialized");

    let runs = ingest::runs_for_board(&conn, "b-runs").expect("list");
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].asset_hash, "hash-a");
    assert_eq!(runs[1].asset_hash, "hash-b");

    // The run row carries the board's REAL tenant (group), not a placeholder.
    let tenant: String = conn
        .query_row(
            "SELECT tenant_id FROM workflow_run WHERE run_id=?1",
            [&r1.run_id],
            |r| r.get(0),
        )
        .expect("tenant stamped");
    assert_eq!(tenant, "g-runs");

    // Unknown board ⇒ empty list; blank args ⇒ clear errors.
    assert!(ingest::runs_for_board(&conn, "no-such-board").expect("empty").is_empty());
    assert!(ingest::materialize_run(&conn, "", "h").is_err());
    assert!(ingest::materialize_run(&conn, "b-runs", " ").is_err());
}

// ── sources ────────────────────────────────────────────────────────────────────

#[test]
fn source_add_list_remove_round_trip_and_closed_vocab() {
    let conn = conn_db();
    mk_board(&conn, "g-src", "b-src");

    // Closed vocab: an unknown kind is rejected with the vocab named.
    let err = ingest::source_add(&conn, "g-src", "b-src", "dropbox", "/watch", None)
        .expect_err("dropbox is not a v1 kind");
    assert!(err.to_string().contains("folder"), "error names the vocab; got: {err}");
    // Blank uri / non-positive schedule are rejected.
    assert!(ingest::source_add(&conn, "g-src", "b-src", "folder", " ", None).is_err());
    assert!(ingest::source_add(&conn, "g-src", "b-src", "folder", "/watch", Some(0)).is_err());

    let s1 = ingest::source_add(&conn, "g-src", "b-src", "folder", "/watch/dailies", Some(300))
        .expect("folder source");
    let s2 = ingest::source_add(&conn, "g-src", "b-src", "s3", "s3://bucket/dailies", None)
        .expect("s3 source registers (scan is the gated part)");
    let listed = ingest::source_list(&conn, "g-src").expect("list");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, s1.id);
    assert_eq!(listed[0].schedule_secs, Some(300));
    assert_eq!(listed[1].kind, "s3");
    assert!(listed[0].last_scan_at.is_none(), "never scanned yet");

    // Tenant-scoped list + remove; removing twice errors clearly.
    assert!(ingest::source_list(&conn, "other-tenant").expect("empty").is_empty());
    assert!(ingest::source_remove(&conn, "other-tenant", &s1.id).is_err());
    ingest::source_remove(&conn, "g-src", &s2.id).expect("remove");
    assert_eq!(ingest::source_list(&conn, "g-src").expect("list again").len(), 1);
    assert!(ingest::source_remove(&conn, "g-src", &s2.id).is_err());
}

#[test]
fn s3_and_frameio_scans_are_typed_not_supported_yet() {
    let conn = conn_db();
    mk_board(&conn, "g-nsy", "b-nsy");
    for kind in ["s3", "frameio_c2c"] {
        let s = ingest::source_add(&conn, "g-nsy", "b-nsy", kind, "remote://somewhere", Some(60))
            .expect("registers");
        let err = ingest::scan(&conn, &s.id).expect_err("scan must be honest");
        let typed = err
            .downcast_ref::<ingest::NotSupportedYet>()
            .unwrap_or_else(|| panic!("expected typed NotSupportedYet, got: {err}"));
        assert_eq!(typed.kind, kind, "the error names the kind");
        // A failed scan must NOT advance the scheduling clock.
        let listed = ingest::source_list(&conn, "g-nsy").expect("list");
        assert!(listed.iter().all(|s| s.last_scan_at.is_none()));
    }
}

// ── the live folder scan ───────────────────────────────────────────────────────

#[test]
fn folder_scan_ingests_dedups_and_rematerializes_on_content_change() {
    let conn = conn_db();
    mk_board(&conn, "g-scan", "b-scan");
    let dir = tempfile::tempdir().expect("watched dir");
    std::fs::write(dir.path().join("daily_a.mp4"), b"clip A bytes v1").expect("a");
    std::fs::write(dir.path().join("daily_b.MOV"), b"clip B bytes").expect("b (upper ext)");
    std::fs::write(dir.path().join("notes.txt"), b"not media").expect("txt ignored");
    std::fs::create_dir(dir.path().join("subdir")).expect("subdir ignored (non-recursive v1)");
    std::fs::write(dir.path().join("subdir").join("nested.mp4"), b"nested").expect("nested");

    let src = ingest::source_add(
        &conn,
        "g-scan",
        "b-scan",
        "folder",
        dir.path().to_str().expect("utf8"),
        None,
    )
    .expect("source");

    // 1 — two distinct files ⇒ two assets, two runs, two board attachments.
    let report = ingest::scan(&conn, &src.id).expect("scan 1");
    assert_eq!(report, ingest::ScanReport { discovered: 2, ingested: 2, deduped: 0 });
    let runs = ingest::runs_for_board(&conn, "b-scan").expect("runs");
    assert_eq!(runs.len(), 2, "one run PER ASSET");
    assert_ne!(runs[0].asset_hash, runs[1].asset_hash);
    assert!(runs.iter().all(|r| r.status == "materialized"));

    // Each run's asset is registered as a located clip master…
    for run in &runs {
        let asset = cyan_backend::asset_registry::get(&conn, "g-scan", &run.asset_hash)
            .expect("asset registered");
        assert_eq!(asset.kind.as_deref(), Some("master"));
        let (class, location) =
            cyan_backend::asset_registry::class_location(&conn, "g-scan", &run.asset_hash)
                .expect("class/location");
        assert_eq!(class.as_deref(), Some("clip"));
        let location = location.expect("ingested master carries its location");
        assert!(location.starts_with("file://"), "canonical file location; got {location}");
        // …and each run's objects row is on the board with real local bytes,
        // hash-matched to ITS run (the explicit-bind seam).
        let (name, local_path): (String, Option<String>) = conn
            .query_row(
                "SELECT name, local_path FROM objects WHERE type='file' AND board_id='b-scan' AND hash=?1",
                [&run.asset_hash],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("objects row for the run's asset");
        let local_path = local_path.expect("local path stamped at insert");
        assert!(std::path::Path::new(&local_path).is_file(), "local bytes exist: {local_path}");
        assert!(location.ends_with(&name), "location and attachment agree on the file");
    }

    // 2 — re-scan, nothing changed ⇒ all deduped, no new runs.
    let report = ingest::scan(&conn, &src.id).expect("scan 2");
    assert_eq!(report, ingest::ScanReport { discovered: 2, ingested: 0, deduped: 2 });
    assert_eq!(ingest::runs_for_board(&conn, "b-scan").expect("runs").len(), 2);

    // 3 — CONTENT change ⇒ new hash ⇒ new asset + new run (b stays deduped).
    std::fs::write(dir.path().join("daily_a.mp4"), b"clip A bytes v2 RESHOT").expect("a v2");
    let report = ingest::scan(&conn, &src.id).expect("scan 3");
    assert_eq!(report, ingest::ScanReport { discovered: 2, ingested: 1, deduped: 1 });
    let runs = ingest::runs_for_board(&conn, "b-scan").expect("runs after edit");
    assert_eq!(runs.len(), 3, "the edited content is a NEW asset with ITS OWN run");
    let hashes: std::collections::HashSet<&str> =
        runs.iter().map(|r| r.asset_hash.as_str()).collect();
    assert_eq!(hashes.len(), 3, "three distinct content identities");
}

// ── scheduling (polling; the app drives the tick) ──────────────────────────────

#[test]
fn due_sources_and_scan_due_follow_the_poll_cadence() {
    let conn = conn_db();
    mk_board(&conn, "g-due", "b-due");
    let dir = tempfile::tempdir().expect("watched dir");
    std::fs::write(dir.path().join("clip.mp4"), b"scheduled clip").expect("clip");

    let scheduled = ingest::source_add(
        &conn,
        "g-due",
        "b-due",
        "folder",
        dir.path().to_str().expect("utf8"),
        Some(60),
    )
    .expect("scheduled folder");
    let manual = ingest::source_add(&conn, "g-due", "b-due", "folder", "/nowhere", None)
        .expect("manual-only folder");
    let remote = ingest::source_add(&conn, "g-due", "b-due", "s3", "s3://b/p", Some(60))
        .expect("scheduled s3");

    let now = chrono::Utc::now().timestamp();

    // Never-scanned scheduled sources are due immediately; manual-only never is.
    let due: Vec<String> = ingest::due_sources(&conn, now)
        .expect("due")
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert!(due.contains(&scheduled.id) && due.contains(&remote.id));
    assert!(!due.contains(&manual.id), "manual-only sources are never due");

    // One sweep: the folder scans (and ingests), the s3 failure is CARRIED.
    let outcomes = ingest::scan_due(&conn, now).expect("sweep");
    assert_eq!(outcomes.len(), 2);
    let folder_outcome = outcomes.iter().find(|o| o.source_id == scheduled.id).expect("folder");
    assert_eq!(
        folder_outcome.report,
        Some(ingest::ScanReport { discovered: 1, ingested: 1, deduped: 0 })
    );
    let s3_outcome = outcomes.iter().find(|o| o.source_id == remote.id).expect("s3");
    assert!(s3_outcome.report.is_none());
    assert!(
        s3_outcome.error.as_deref().unwrap_or("").contains("not supported yet"),
        "the s3 failure is carried, not thrown; got {:?}",
        s3_outcome.error
    );

    // The successful scan advanced the clock: not due until the cadence elapses.
    let due_soon: Vec<String> = ingest::due_sources(&conn, now + 30)
        .expect("due at +30")
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert!(!due_soon.contains(&scheduled.id), "inside the cadence window");
    let due_later: Vec<String> = ingest::due_sources(&conn, now + 61)
        .expect("due at +61")
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert!(due_later.contains(&scheduled.id), "cadence elapsed ⇒ due again");
}

// ── global-DB rail ─────────────────────────────────────────────────────────────

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ingest-test.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            cyan_backend::ensure_schema(&conn).expect("engine schema");
        }
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
    });
}

fn create_fresh_group(id: &str, name: &str) -> (String, String) {
    let g = Group {
        id: id.to_string(),
        name: name.to_string(),
        icon: "folder".to_string(),
        color: "#00FFFF".to_string(),
        created_at: chrono::Utc::now().timestamp(),
    };
    storage::group_insert(&g).expect("group insert");
    let (default_ws, plugins_ws) =
        storage::provision_group_workspaces(id, Some("node-under-test")).expect("seed workspaces");
    (default_ws.id, plugins_ws.id)
}

/// A single-tool probe manifest requiring `file_path` (the bind target prop).
fn probe_manifest() -> cyan_mcp::Manifest {
    serde_json::from_value(serde_json::json!({
        "name": "mediaio",
        "version": "0.1.0",
        "description": "probe test plugin",
        "runtime": "python-uv",
        "credentials": null,
        "extra_credentials": [],
        "events_emitted": [],
        "tools": [{
            "name": "probe",
            "when_to_use": "Probe a media file.",
            "aliases": [],
            "io_types": { "input": ["video"], "output": ["json"] },
            "stage": "ingest",
            "side_effects": [],
            "locality": "local",
            "input_schema": {
                "type": "object",
                "properties": { "file_path": {"type": "string"}, "name": {"type": "string"} },
                "required": ["file_path"]
            },
            "output_schema": {"type": "object"}
        }]
    }))
    .expect("strict manifest")
}

/// STAGE 4's load-bearing bind property: with TWO distinct clips on the board
/// (Tier-2 would refuse — ambiguous), each run's EXPLICIT asset binds ITS file;
/// no explicit asset keeps the existing never-guess behavior; and an explicit
/// asset that doesn't resolve stays pending instead of falling back to a guess.
#[test]
fn explicit_run_asset_binds_its_own_file_never_a_guess() {
    ensure_db();
    let group = "ingest-bind-group";
    let (default_ws, _) = create_fresh_group(group, "Ingest Bind Group");
    let board = "ingest-bind-board";
    storage::board_insert(board, &default_ws, "Ingest Board", chrono::Utc::now().timestamp())
        .expect("board insert");

    for (id, name, hash, path) in [
        ("ing-clip-a", "daily_a.mp4", "hash-daily-a", "/data/files/hash-daily-a"),
        ("ing-clip-b", "daily_b.mov", "hash-daily-b", "/data/files/hash-daily-b"),
    ] {
        storage::file_insert(id, Some(group), Some(&default_ws), Some(board), name, hash, 10, "peer", 1)
            .expect("file insert");
        storage::file_set_local_path(id, path).expect("local path");
    }

    let manifest = probe_manifest();
    let mention = workflow_bind::parse_mention("check @mediaio.probe").expect("mention");
    let bind = |explicit: Option<&str>| {
        workflow_bind::bind_with_manifest_for_asset(
            board,
            group,
            "check @mediaio.probe",
            &mention,
            &manifest,
            explicit,
        )
    };

    // Each run binds ITS OWN file — by content hash, and equally by objects id.
    for (explicit, path, name) in [
        ("hash-daily-a", "/data/files/hash-daily-a", "daily_a.mp4"),
        ("hash-daily-b", "/data/files/hash-daily-b", "daily_b.mov"),
        ("ing-clip-a", "/data/files/hash-daily-a", "daily_a.mp4"),
    ] {
        match bind(Some(explicit)) {
            workflow_bind::BindOutcome::Bound(b) => {
                assert_eq!(b.args["file_path"], path, "explicit asset {explicit} binds its file");
                assert_eq!(b.args["name"], name);
                assert!(b.pending.is_empty(), "pending must be empty; got {:?}", b.pending);
            }
            other => panic!("expected Bound, got {other:?}"),
        }
    }

    // NO explicit asset ⇒ the existing behavior, unchanged: two distinct clips
    // are ambiguous ⇒ pending, never a guess.
    match bind(None) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert!(
                b.pending.contains(&"file_path".to_string()),
                "two clips + no explicit asset ⇒ pending; got args {:?}",
                b.args
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }

    // An explicit asset that does NOT resolve on this board stays pending —
    // the Tier-2 fallback must not silently bind some other file.
    match bind(Some("hash-not-on-this-board")) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert!(
                b.pending.contains(&"file_path".to_string()),
                "unresolvable explicit asset ⇒ pending, never a fallback guess; got args {:?}",
                b.args
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }
}

// ── the cyan_ingest_command JSON dialect ───────────────────────────────────────

fn cmd(json: serde_json::Value) -> serde_json::Value {
    let out = ingest::command(&json.to_string());
    serde_json::from_str(&out).expect("dispatch output is always valid JSON")
}

/// The FFI dialect end-to-end on the process-global DB: source_add → scan_now
/// (a real folder) → runs_for_board → source_list/remove, plus
/// produce_master_plan over a real frozen version.
#[test]
fn ingest_command_dialect_round_trip() {
    ensure_db();
    let group = "ingest-ffi-group";
    let (default_ws, _) = create_fresh_group(group, "FFI Group");
    let board = "ingest-ffi-board";
    storage::board_insert(board, &default_ws, "FFI Board", chrono::Utc::now().timestamp())
        .expect("board insert");

    let dir = tempfile::tempdir().expect("watched dir");
    std::fs::write(dir.path().join("ffi_daily.mp4"), b"ffi clip bytes").expect("clip");

    // source_add (with the Schedule button's cadence) → the row comes back.
    let added = cmd(serde_json::json!({
        "op": "source_add", "tenant_id": group, "board_id": board,
        "kind": "folder", "uri": dir.path().to_str().expect("utf8"), "schedule_secs": 120
    }));
    assert!(added.get("error").is_none(), "source_add must succeed; got {added}");
    let source_id = added["id"].as_str().expect("source id").to_string();
    assert_eq!(added["schedule_secs"], 120);

    // scan_now → one ingested; runs_for_board sees the materialized run.
    let report = cmd(serde_json::json!({ "op": "scan_now", "source_id": source_id }));
    assert_eq!(report["discovered"], 1, "got {report}");
    assert_eq!(report["ingested"], 1);
    let runs = cmd(serde_json::json!({ "op": "runs_for_board", "board_id": board }));
    let runs = runs.as_array().expect("runs array");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["status"], "materialized");
    let run_asset = runs[0]["asset_hash"].as_str().expect("asset hash").to_string();

    // scan_due at +121s: due again, all content deduped (carried per-source).
    let now = chrono::Utc::now().timestamp();
    let outcomes = cmd(serde_json::json!({ "op": "scan_due", "now": now + 121 }));
    let outcome = outcomes
        .as_array()
        .expect("outcomes array")
        .iter()
        .find(|o| o["source_id"] == source_id.as_str())
        .expect("our source swept")
        .clone();
    assert_eq!(outcome["report"]["deduped"], 1, "got {outcome}");

    // produce_master_plan: freeze a version whose cut uses the ingested master
    // (anchor) + one located insert, then resolve the SELECTIVE retrieve list.
    {
        use cyan_backend::util::MutexExt;
        let conn = storage::db().lock_safe();
        cyan_backend::asset_registry::upsert(
            &conn,
            &cyan_backend::asset_registry::Asset {
                hash: "ffi-insert-master".to_string(),
                tenant_id: group.to_string(),
                kind: Some("master".to_string()),
                fps: None,
                duration_ms: None,
                derived_from_asset: None,
                derived_from_version: None,
                remote_refs: serde_json::json!({}),
                profile_json: serde_json::json!({}),
                render_profile: None,
                created_at: 0,
            },
        )
        .expect("insert master registered");
        cyan_backend::asset_registry::set_class_location(
            &conn,
            group,
            "ffi-insert-master",
            Some("clip"),
            Some("s3://bucket/ffi-insert-master.mxf"),
        )
        .expect("insert master located");
    }
    let appended = serde_json::from_str::<serde_json::Value>(&cyan_backend::changelist::command(
        &serde_json::json!({
            "op": "append", "tenant_id": group, "asset_hash": run_asset, "branch": "main",
            "entry": {
                "id": "", "entry_hash": "", "asset_hash": run_asset, "tenant_id": group,
                "track": "V1", "tc_in": 24, "tc_out": 48, "kind": "op", "op": "insert",
                "params": { "asset_hash": "ffi-insert-master", "at": 24 },
                "intent": "insert pickup", "proposed_by": "human",
                "created_at": 0, "state": "", "active": true, "seq": 0, "updated_at": 0
            }
        })
        .to_string(),
    ))
    .expect("append json");
    assert!(appended.get("error").is_none(), "append must succeed; got {appended}");
    // The human gate: produce_master_plan rides conform_plan, which carries only
    // APPROVED ops — approve through the same JSON dialect the app drives.
    let approved = serde_json::from_str::<serde_json::Value>(&cyan_backend::changelist::command(
        &serde_json::json!({
            "op": "set_state", "tenant_id": group,
            "entry_id": appended["id"].as_str().expect("appended id"),
            "state": "approved", "by": "rick"
        })
        .to_string(),
    ))
    .expect("set_state json");
    assert!(approved.get("error").is_none(), "approve must succeed; got {approved}");
    let version = serde_json::from_str::<serde_json::Value>(&cyan_backend::changelist::command(
        &serde_json::json!({
            "op": "snapshot", "tenant_id": group, "asset_hash": run_asset, "branch": "main"
        })
        .to_string(),
    ))
    .expect("snapshot json");
    let version_id = version["version_id"].as_str().expect("version id");

    let plan = cmd(serde_json::json!({
        "op": "produce_master_plan", "tenant_id": group, "version_id": version_id
    }));
    let masters = plan["masters"].as_array().expect("masters array");
    let listed: Vec<&str> = masters
        .iter()
        .map(|m| m["asset"]["hash"].as_str().expect("hash"))
        .collect();
    assert_eq!(
        listed,
        vec![run_asset.as_str(), "ffi-insert-master"],
        "the retrieve list is exactly the used masters, anchor first"
    );
    assert!(
        masters[0]["location"].as_str().expect("loc").starts_with("file://"),
        "the ingested anchor resolves to its file:// location"
    );
    assert_eq!(masters[1]["location"], "s3://bucket/ffi-insert-master.mxf");

    // source_list / source_remove close the loop.
    let listed = cmd(serde_json::json!({ "op": "source_list", "tenant_id": group }));
    assert_eq!(listed.as_array().expect("list").len(), 1);
    let removed = cmd(serde_json::json!({ "op": "source_remove", "tenant_id": group, "id": source_id }));
    assert_eq!(removed["removed"], true);
    let listed = cmd(serde_json::json!({ "op": "source_list", "tenant_id": group }));
    assert!(listed.as_array().expect("list").is_empty());
}

/// Bad input never panics across the boundary — {"error": ...} JSON, always
/// (the cyan_changelist_command regression rail, mirrored).
#[test]
fn ingest_command_dispatch_returns_clean_json_errors() {
    ensure_db();
    for bad in [
        "not json at all",
        r#"{"no_op_field": 1}"#,
        r#"{"op": "definitely_not_an_op"}"#,
        r#"{"op": "scan_now", "source_id": "no-such-source"}"#,
        r#"{"op": "source_add", "tenant_id": "t", "board_id": "b", "kind": "dropbox", "uri": "u"}"#,
        r#"{"op": "produce_master_plan", "tenant_id": "t", "version_id": "no-such-version"}"#,
    ] {
        let out = ingest::command(bad);
        let v: serde_json::Value =
            serde_json::from_str(&out).expect("dispatch output is always valid JSON");
        assert!(
            v.get("error").and_then(|e| e.as_str()).is_some(),
            "bad command {bad:?} surfaces a clean error, got: {out}"
        );
    }

    // The typed transport gap surfaces through the dialect too.
    let group = "ingest-ffi-nsy";
    let (default_ws, _) = create_fresh_group(group, "FFI NSY Group");
    let board = "ingest-ffi-nsy-board";
    storage::board_insert(board, &default_ws, "NSY Board", chrono::Utc::now().timestamp())
        .expect("board insert");
    let added = cmd(serde_json::json!({
        "op": "source_add", "tenant_id": group, "board_id": board,
        "kind": "frameio_c2c", "uri": "c2c://project-1"
    }));
    let scan = cmd(serde_json::json!({ "op": "scan_now", "source_id": added["id"] }));
    assert!(
        scan["error"].as_str().unwrap_or("").contains("frameio_c2c"),
        "the seam error names the kind; got {scan}"
    );
}

/// With ONE attachment and no explicit asset, the Tier-2 implicit fill still
/// works exactly as before (the additive parameter changes nothing).
#[test]
fn no_explicit_asset_keeps_single_attachment_bind_unchanged() {
    ensure_db();
    let group = "ingest-bind-single";
    let (default_ws, _) = create_fresh_group(group, "Single Group");
    let board = "ingest-bind-single-board";
    storage::board_insert(board, &default_ws, "Single Board", chrono::Utc::now().timestamp())
        .expect("board insert");
    storage::file_insert(
        "single-clip",
        Some(group),
        Some(&default_ws),
        Some(board),
        "master.mp4",
        "hash-single",
        10,
        "peer",
        1,
    )
    .expect("file insert");
    storage::file_set_local_path("single-clip", "/data/files/hash-single").expect("path");

    let manifest = probe_manifest();
    let mention = workflow_bind::parse_mention("check @mediaio.probe").expect("mention");
    match workflow_bind::bind_with_manifest_for_asset(
        board,
        group,
        "check @mediaio.probe",
        &mention,
        &manifest,
        None,
    ) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(
                b.args["file_path"], "/data/files/hash-single",
                "the single attachment still fills implicitly with no explicit asset"
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }
}
