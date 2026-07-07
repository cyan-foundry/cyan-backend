//! REVIEW_LOOP_ONE_BOARD — the load-bearing fix, engine leg.
//!
//! Proves the mechanical spine's device half with NO lens and NO GPU anywhere
//! near the path:
//!   1. installing a REAL tar `.cyanplugin` unpacks it for the device registry;
//!   2. a board's `@plugin.` autocomplete lists the manifest's TOOLS for the board's GROUP (Bug A);
//!   3. `#` lists board files BY NAME, content-deduped (Bugs B/4);
//!   4. Review-time rung-1 binding resolves `@plugin.tool` + inline `key=value` plus a `#file`
//!      reference to the SPECIFIC attached file's real local path (Bug 5 — the wrong-file bug),
//!      never guessing when args are missing.

use std::sync::Once;

use cyan_backend::{models::core::Group, storage, util::MutexExt, workflow, workflow_bind};

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("workflow-bind.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            cyan_backend::ensure_schema(&conn).expect("engine schema");
        }
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        let plugins = tempfile::tempdir().expect("tmp plugins dir");
        unsafe { std::env::set_var("CYAN_PLUGINS_ROOT", plugins.path()) };
        std::mem::forget(plugins);
        std::mem::forget(dir);
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

/// A minimal REAL bundle: manifest.json (strict cyan-mcp shape, mirroring the
/// live forge frameio manifest) + src/plugin.py, tarred exactly like the forge
/// artifact (`<plugin_id>/…` top-level dir, POSIX tar).
fn make_bundle_tar(plugin_id: &str) -> Vec<u8> {
    let stage = tempfile::tempdir().expect("stage dir");
    let pdir = stage.path().join(plugin_id);
    std::fs::create_dir_all(pdir.join("src")).expect("mkdir src");
    let manifest = serde_json::json!({
        "name": plugin_id,
        "version": "0.1.0",
        "description": "test plugin",
        "runtime": "python-uv",
        "credentials": { "kind": "oauth2", "provider": "adobe_ims", "locality": "tenant" },
        "extra_credentials": [],
        "events_emitted": [],
        "tools": [
            {
                "name": "upload_file",
                "when_to_use": "Push a finished cut to review.",
                "aliases": ["upload", "push"],
                "io_types": { "input": ["video"], "output": ["json"] },
                "stage": "review",
                "side_effects": ["external_send"],
                "locality": "local",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "account_id": {"type": "string"},
                        "folder_id": {"type": "string"},
                        "name": {"type": "string"},
                        "file_path": {"type": "string"}
                    },
                    "required": ["account_id", "folder_id", "name", "file_path"]
                },
                "output_schema": {"type": "object"}
            },
            {
                "name": "list_comments",
                "when_to_use": "List review comments on a file.",
                "aliases": ["comments"],
                "io_types": { "input": ["json"], "output": ["json"] },
                "stage": "comms",
                "side_effects": [],
                "locality": "local",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "account_id": {"type": "string"},
                        "file_id": {"type": "string"}
                    },
                    "required": ["account_id", "file_id"]
                },
                "output_schema": {"type": "object"}
            }
        ]
    });
    std::fs::write(
        pdir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    std::fs::write(pdir.join("src").join("plugin.py"), b"# test plugin\n").expect("write plugin");

    let tar_path = stage.path().join("bundle.tar");
    let status = std::process::Command::new("/usr/bin/tar")
        .arg("-cf")
        .arg(&tar_path)
        .arg("-C")
        .arg(stage.path())
        .arg(plugin_id)
        .status()
        .expect("tar spawn");
    assert!(status.success(), "tar create failed");
    std::fs::read(&tar_path).expect("read tar")
}

#[test]
fn install_unpacks_and_autocomplete_lists_manifest_tools_per_group() {
    ensure_db();
    let group = "bind-group-a";
    let (default_ws, _) = create_fresh_group(group, "Bind Group A");
    let board = "bind-board-a";
    storage::board_insert(
        board,
        &default_ws,
        "Board A",
        chrono::Utc::now().timestamp(),
    )
    .expect("board insert");

    let tar = make_bundle_tar("testio");
    storage::install_plugin_bundle(group, "testio", &tar).expect("install");

    // Unpacked for the device registry.
    let unpacked = storage::ensure_bundle_unpacked("testio").expect("bundle unpacked");
    assert!(unpacked.join("manifest.json").is_file());

    // `@testio.` surfaces the manifest TOOLS for the board's group (Bug A).
    let idx = workflow::filter_autocomplete(board, "use @testio.");
    let values: Vec<&str> = idx.plugins.iter().map(|e| e.value.as_str()).collect();
    assert!(
        values.contains(&"testio.upload_file") && values.contains(&"testio.list_comments"),
        "@testio. must list the plugin's tools; got {values:?}"
    );
    // The tool label is the when_to_use guidance, not an opaque id.
    let upload = idx
        .plugins
        .iter()
        .find(|e| e.value == "testio.upload_file")
        .expect("upload tool entry");
    assert_eq!(upload.label, "Push a finished cut to review.");

    // A board in ANOTHER group (no install) must NOT see the tools — per-group.
    let group_b = "bind-group-b";
    let (ws_b, _) = create_fresh_group(group_b, "Bind Group B");
    let board_b = "bind-board-b";
    storage::board_insert(board_b, &ws_b, "Board B", chrono::Utc::now().timestamp())
        .expect("board insert b");
    let idx_b = workflow::filter_autocomplete(board_b, "use @testio.");
    assert!(
        idx_b.plugins.is_empty(),
        "a group without the install must not see its tools; got {:?}",
        idx_b.plugins
    );
}

#[test]
fn hash_autocomplete_lists_board_files_by_name_deduped() {
    ensure_db();
    let group = "bind-group-files";
    let (default_ws, _) = create_fresh_group(group, "Files Group");
    let board = "bind-board-files";
    storage::board_insert(
        board,
        &default_ws,
        "Files Board",
        chrono::Utc::now().timestamp(),
    )
    .expect("board insert");

    // The SAME bytes attached twice (two rows, one content hash) + one other file.
    storage::file_insert(
        "fid-1",
        Some(group),
        Some(&default_ws),
        Some(board),
        "sig_source.mp4",
        "cafedead01",
        10,
        "peer",
        1,
    )
    .expect("file 1");
    // Simulate the legacy duplicate (different id, same content hash, no board).
    {
        let conn = storage::db().lock_safe();
        conn.execute(
            "INSERT INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, \
             source_peer, created_at)
             VALUES ('fid-dup', ?1, ?2, NULL, 'file', 'sig_source.mp4', 'cafedead01', 10, 'peer', \
             2)",
            rusqlite::params![group, default_ws],
        )
        .expect("dup row");
    }
    storage::file_insert(
        "fid-2",
        Some(group),
        Some(&default_ws),
        Some(board),
        "other.mov",
        "beefbeef02",
        5,
        "peer",
        3,
    )
    .expect("file 2");

    let idx = workflow::filter_autocomplete(board, "grab #");
    let files: Vec<(&str, &str)> = idx
        .artifacts
        .iter()
        .filter(|e| e.kind == "file")
        .map(|e| (e.value.as_str(), e.label.as_str()))
        .collect();
    // By NAME (value == the readable name), content-deduped: ONE sig_source entry.
    assert_eq!(
        files.iter().filter(|(v, _)| *v == "sig_source.mp4").count(),
        1,
        "same bytes must be ONE #entry; got {files:?}"
    );
    assert!(files.iter().any(|(v, _)| *v == "other.mov"));
}

#[test]
fn review_time_bind_resolves_specific_file_and_inline_args() {
    ensure_db();
    let group = "bind-group-c";
    let (default_ws, _) = create_fresh_group(group, "Bind Group C");
    let board = "bind-board-c";
    storage::board_insert(
        board,
        &default_ws,
        "Board C",
        chrono::Utc::now().timestamp(),
    )
    .expect("board insert");
    let tar = make_bundle_tar("testio");
    storage::install_plugin_bundle(group, "testio", &tar).expect("install");

    // The attached clip with a REAL local path — the file the bind must pick.
    storage::file_insert(
        "clip-id-1",
        Some(group),
        Some(&default_ws),
        Some(board),
        "sig_source.mp4",
        "feedfacecafe",
        42,
        "peer",
        1,
    )
    .expect("file insert");
    storage::file_set_local_path("clip-id-1", "/data/files/feedfacecafe").expect("set path");
    // A decoy file that must NOT be picked (the big-buck-bunny class of bug).
    storage::file_insert(
        "decoy-id",
        Some(group),
        Some(&default_ws),
        None,
        "big-buck-bunny.mp4",
        "0ddba11",
        99,
        "peer",
        0,
    )
    .expect("decoy insert");
    storage::file_set_local_path("decoy-id", "/data/files/0ddba11").expect("decoy path");

    let content =
        "push the cut @testio.upload_file account_id=acct-1 folder_id=fold-9 #sig_source.mp4";
    match workflow_bind::bind_step(board, content) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(b.plugin_id, "testio");
            assert_eq!(b.tool, "upload_file");
            assert_eq!(b.args["account_id"], "acct-1");
            assert_eq!(b.args["folder_id"], "fold-9");
            assert_eq!(
                b.args["file_path"], "/data/files/feedfacecafe",
                "the #reference must bind the SPECIFIC attached file"
            );
            assert_eq!(b.args["name"], "sig_source.mp4");
            assert_eq!(b.side_effects, vec!["external_send".to_string()]);
        }
        other => panic!("expected Bound, got {other:?}"),
    }

    // Required args not resolvable at Review ⇒ still BOUND (mechanical by
    // declaration), with the gaps stamped `pending` for dispatch-time
    // completion from upstream outputs — never guessed by a model.
    match workflow_bind::bind_step(board, "push @testio.upload_file #sig_source.mp4") {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(b.args["file_path"], "/data/files/feedfacecafe");
            let mut pending = b.pending.clone();
            pending.sort();
            assert_eq!(pending, vec!["account_id".to_string(), "folder_id".to_string()]);
        }
        other => panic!("expected Bound with pending, got {other:?}"),
    }

    // Unknown tool ⇒ Miss tool_not_in_manifest.
    match workflow_bind::bind_step(board, "run @testio.nonexistent x=1") {
        workflow_bind::BindOutcome::Miss { reason, .. } => {
            assert_eq!(reason, "tool_not_in_manifest")
        }
        other => panic!("expected Miss, got {other:?}"),
    }

    // Plugin not installed in THIS group ⇒ Miss (per-group scoping).
    let group_d = "bind-group-d";
    let (ws_d, _) = create_fresh_group(group_d, "Bind Group D");
    let board_d = "bind-board-d";
    storage::board_insert(board_d, &ws_d, "Board D", chrono::Utc::now().timestamp())
        .expect("board insert d");
    match workflow_bind::bind_step(board_d, "push @testio.upload_file a=1") {
        workflow_bind::BindOutcome::Miss { reason, .. } => {
            assert_eq!(reason, "plugin_not_installed_in_group")
        }
        other => panic!("expected Miss, got {other:?}"),
    }

    // No mention ⇒ None (an ordinary creative step).
    assert!(matches!(
        workflow_bind::bind_step(board, "write a moody synopsis of the cut"),
        workflow_bind::BindOutcome::None
    ));
}

#[test]
fn spawn_config_maps_python_uv_and_injects_creds() {
    ensure_db();
    let group = "bind-group-e";
    create_fresh_group(group, "Bind Group E");
    let tar = make_bundle_tar("credio");
    storage::install_plugin_bundle(group, "credio", &tar).expect("install");
    let dir = storage::ensure_bundle_unpacked("credio").expect("unpacked");

    // Credential env var by the shared convention: CREDIO_IMS_TOKEN.
    unsafe { std::env::set_var("CREDIO_IMS_TOKEN", "sekret") };
    let cfg = cyan_backend::mcp_host::bundle_spawn_config("credio", &dir, "tenant-x")
        .expect("spawn config");
    assert_eq!(cfg.command, "uv");
    assert_eq!(cfg.args[0], "run");
    assert!(cfg.args.contains(&"src/plugin.py".to_string()));
    assert!(cfg.creds.iter().any(|(k, _)| k == "CREDIO_IMS_TOKEN"));
    assert!(cfg.creds.iter().any(|(k, _)| k == "CYAN_TENANT_ID"));

    // A missing credential REFUSES the spawn with a clear error.
    unsafe { std::env::remove_var("CREDIO_IMS_TOKEN") };
    match cyan_backend::mcp_host::bundle_spawn_config("credio", &dir, "tenant-x") {
        Err(err) => assert!(err.to_string().contains("CREDIO_IMS_TOKEN")),
        Ok(_) => panic!("must refuse a spawn with a missing credential"),
    }
}
