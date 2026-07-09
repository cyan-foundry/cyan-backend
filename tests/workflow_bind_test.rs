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

/// The create_comment shape (found live): required [account_id, file_id, text] —
/// `account_id` resolves from the plugin's ENV CONTEXT (the same context the
/// spawn injects the token from), `text` from the authored intent, and only
/// `file_id` stays pending for the upstream (upload output) fill. Without this,
/// a locally-bound comment "completed" while posting NOTHING to Frame.io.
#[test]
fn comment_binds_account_from_env_context_and_text_from_intent() {
    ensure_db();
    let group = "bind-group-env";
    let (default_ws, _) = create_fresh_group(group, "Env Group");
    let board = "bind-board-env";
    storage::board_insert(board, &default_ws, "Env Board", chrono::Utc::now().timestamp())
        .expect("board insert");
    // env-context isolation: match the workflow_bind test plugin id below.
    unsafe { std::env::set_var("ENVIO_ACCOUNT_ID", "acct-from-env") };

    let manifest_json = serde_json::json!({
        "name": "envio",
        "version": "0.1.0",
        "description": "env-context test plugin",
        "runtime": "python-uv",
        "credentials": null,
        "extra_credentials": [],
        "events_emitted": [],
        "tools": [{
            "name": "create_comment",
            "when_to_use": "Leave a review comment.",
            "aliases": [],
            "io_types": { "input": ["json"], "output": ["json"] },
            "stage": "review",
            "side_effects": ["external_send"],
            "locality": "local",
            "input_schema": {
                "type": "object",
                "properties": {
                    "account_id": {"type": "string"},
                    "file_id": {"type": "string"},
                    "text": {"type": "string"},
                    "timestamp": {}
                },
                "required": ["account_id", "file_id", "text"]
            },
            "output_schema": {"type": "object"}
        }]
    });
    let manifest: cyan_mcp::Manifest =
        serde_json::from_value(manifest_json).expect("strict manifest");
    let mention = workflow_bind::parse_mention(
        "tell the reviewer the v2 is up @envio.create_comment timestamp=60",
    )
    .expect("mention");
    match workflow_bind::bind_with_manifest(
        board,
        group,
        "tell the reviewer the v2 is up @envio.create_comment timestamp=60",
        &mention,
        &manifest,
    ) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(b.args["account_id"], "acct-from-env", "env-context fill");
            assert_eq!(
                b.args["text"], "tell the reviewer the v2 is up",
                "the authored intent is the comment body"
            );
            assert_eq!(b.args["timestamp"], "60");
            assert_eq!(
                b.pending,
                vec!["file_id".to_string()],
                "only the upstream-fed file_id stays pending"
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }

    // An INLINE account_id/text always wins over the env/intent fallback.
    match workflow_bind::bind_with_manifest(
        board,
        group,
        "note @envio.create_comment account_id=inline-a text=inline-note file_id=f1",
        &workflow_bind::parse_mention("@envio.create_comment").expect("m"),
        &manifest,
    ) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(b.args["account_id"], "inline-a");
            assert_eq!(b.args["text"], "inline-note");
            assert!(b.pending.is_empty());
        }
        other => panic!("expected Bound, got {other:?}"),
    }
    unsafe { std::env::remove_var("ENVIO_ACCOUNT_ID") };
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

/// PLUGIN_CREDENTIAL_ONBOARDING §C — the credential is resolved FRESH at every
/// spawn from the cred dotenv file, so a token the loader refreshed ON DISK is
/// what the next plugin spawn injects (the 401-mid-session bug: the app process
/// env is a launch-time snapshot and never sees the refresh).
#[test]
fn spawn_config_reads_the_cred_file_fresh_per_spawn() {
    ensure_db();
    let group = "bind-group-cred-file";
    create_fresh_group(group, "Bind Group CredFile");
    let tar = make_bundle_tar("credio2");
    storage::install_plugin_bundle(group, "credio2", &tar).expect("install");
    let dir = storage::ensure_bundle_unpacked("credio2").expect("unpacked");

    let cred_file = tempfile::NamedTempFile::new().expect("cred file");
    std::fs::write(cred_file.path(), "export CREDIO2_IMS_TOKEN=\"launch-token\"\n").unwrap();
    unsafe {
        std::env::set_var("CYAN_VAULT", "mem"); // never prompt a Keychain in tests
        std::env::set_var("CYAN_CRED_ENV_FILE", cred_file.path());
        std::env::remove_var("CREDIO2_IMS_TOKEN"); // NO process-env fallback in play
    }

    let cfg = cyan_backend::mcp_host::bundle_spawn_config("credio2", &dir, "tenant-x")
        .expect("spawn config");
    let tok = cfg.creds.iter().find(|(k, _)| k == "CREDIO2_IMS_TOKEN").expect("token injected");
    assert_eq!(tok.1.expose(), "launch-token");

    // The loader refreshes the file mid-session → the NEXT spawn sees the new
    // token, with no app restart and no plugin change.
    std::fs::write(cred_file.path(), "CREDIO2_IMS_TOKEN=refreshed-token\n").unwrap();
    let cfg2 = cyan_backend::mcp_host::bundle_spawn_config("credio2", &dir, "tenant-x")
        .expect("spawn config after refresh");
    let tok2 = cfg2.creds.iter().find(|(k, _)| k == "CREDIO2_IMS_TOKEN").expect("token injected");
    assert_eq!(tok2.1.expose(), "refreshed-token", "fresh-per-spawn, not a launch snapshot");

    unsafe {
        std::env::remove_var("CYAN_CRED_ENV_FILE");
        std::env::remove_var("CYAN_VAULT");
    }
}

/// PLUGIN_CREDENTIAL_ONBOARDING §B — required tool props resolve from the
/// per-WORKFLOW / per-TENANT `plugin_config` store BEFORE the ambient env
/// stopgap: two producers on one device get their own Frame.io folder.
#[test]
fn bind_fills_required_props_from_plugin_config_store() {
    ensure_db();
    let group = "bind-group-cfg";
    let (default_ws, _) = create_fresh_group(group, "Bind Group Cfg");
    let board = "bind-board-cfg";
    storage::board_insert(board, &default_ws, "Cfg Board", chrono::Utc::now().timestamp())
        .expect("board insert");

    // Tenant default + a workflow override for folder_id; env holds a decoy.
    {
        let conn = storage::db().lock_safe();
        cyan_backend::plugin_config::set(&conn, group, None, "cfgio", "account_id", "acct-store", 1)
            .expect("tenant account");
        cyan_backend::plugin_config::set(&conn, group, None, "cfgio", "folder_id", "tenant-folder", 1)
            .expect("tenant folder");
        cyan_backend::plugin_config::set(&conn, group, Some(board), "cfgio", "folder_id", "board-folder", 2)
            .expect("board folder");
    }
    unsafe { std::env::set_var("CFGIO_FOLDER_ID", "env-decoy") };

    let manifest_json = serde_json::json!({
        "name": "cfgio",
        "version": "0.1.0",
        "description": "plugin_config test plugin",
        "runtime": "python-uv",
        "credentials": null,
        "extra_credentials": [],
        "events_emitted": [],
        "tools": [{
            "name": "upload_file",
            "when_to_use": "Push a finished cut to review.",
            "aliases": [],
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
        }]
    });
    let manifest: cyan_mcp::Manifest =
        serde_json::from_value(manifest_json).expect("strict manifest");
    let mention = workflow_bind::parse_mention("@cfgio.upload_file").expect("mention");
    match workflow_bind::bind_with_manifest(
        board,
        group,
        "@cfgio.upload_file name=cut.mp4 file_path=/tmp/cut.mp4",
        &mention,
        &manifest,
    ) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(b.args["account_id"], "acct-store", "tenant config row fills");
            assert_eq!(
                b.args["folder_id"], "board-folder",
                "the WORKFLOW row wins over tenant row AND the env stopgap"
            );
            assert!(b.pending.is_empty(), "nothing left pending: {:?}", b.pending);
        }
        other => panic!("expected Bound, got {other:?}"),
    }
    unsafe { std::env::remove_var("CFGIO_FOLDER_ID") };
}

/// TIER 2 — the IMPLICIT "attached master": a step with NO `#reference` binds
/// the board's REAL attachment when it is the only content-distinct board file
/// with local bytes; two DIFFERENT clips stay pending (never a guess), and a
/// group-scoped decoy (big-buck-bunny) is never picked.
#[test]
fn implicit_attachment_binds_the_single_board_file_never_a_guess() {
    ensure_db();
    let group = "bind-group-implicit";
    let (default_ws, _) = create_fresh_group(group, "Implicit Group");
    let board = "bind-board-implicit";
    storage::board_insert(board, &default_ws, "Implicit Board", chrono::Utc::now().timestamp())
        .expect("board insert");
    let tar = make_bundle_tar("testio");
    storage::install_plugin_bundle(group, "testio", &tar).expect("install");

    // A GROUP decoy that must never be picked implicitly.
    storage::file_insert(
        "imp-decoy",
        Some(group),
        Some(&default_ws),
        None,
        "big-buck-bunny.mp4",
        "0ddba11baad",
        99,
        "peer",
        0,
    )
    .expect("decoy");
    storage::file_set_local_path("imp-decoy", "/data/files/0ddba11baad").expect("decoy path");

    // No board attachment yet ⇒ the path prop stays PENDING (clear, not guessed).
    match workflow_bind::bind_step(board, "push the cut @testio.upload_file account_id=a folder_id=f name=n") {
        workflow_bind::BindOutcome::Bound(b) => {
            assert!(
                b.pending.contains(&"file_path".to_string()),
                "no attachment ⇒ file_path pending, never the group decoy; got args {:?}",
                b.args
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }

    // ONE board attachment (twice, same bytes — the dedup case) ⇒ implicit fill.
    storage::file_insert(
        "imp-clip",
        Some(group),
        Some(&default_ws),
        Some(board),
        "master.mp4",
        "feedface99",
        42,
        "peer",
        1,
    )
    .expect("clip");
    storage::file_set_local_path("imp-clip", "/data/files/feedface99").expect("clip path");
    storage::file_insert(
        "imp-clip-dup",
        Some(group),
        Some(&default_ws),
        Some(board),
        "master.mp4",
        "feedface99",
        42,
        "peer",
        2,
    )
    .expect("clip dup");
    storage::file_set_local_path("imp-clip-dup", "/data/files/feedface99").expect("dup path");
    match workflow_bind::bind_step(board, "push the cut @testio.upload_file account_id=a folder_id=f") {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(
                b.args["file_path"], "/data/files/feedface99",
                "the board's single attachment fills the implicit master"
            );
            assert_eq!(b.args["name"], "master.mp4");
            assert!(b.pending.is_empty(), "pending must be empty; got {:?}", b.pending);
        }
        other => panic!("expected Bound, got {other:?}"),
    }

    // A SECOND, DIFFERENT clip ⇒ ambiguous ⇒ pending again (no guessing).
    storage::file_insert(
        "imp-clip-2",
        Some(group),
        Some(&default_ws),
        Some(board),
        "other.mp4",
        "c0ffee77",
        7,
        "peer",
        3,
    )
    .expect("clip 2");
    storage::file_set_local_path("imp-clip-2", "/data/files/c0ffee77").expect("clip2 path");
    match workflow_bind::bind_step(board, "push the cut @testio.upload_file account_id=a folder_id=f name=n") {
        workflow_bind::BindOutcome::Bound(b) => {
            assert!(
                b.pending.contains(&"file_path".to_string()),
                "two distinct clips ⇒ ambiguous ⇒ pending; got args {:?}",
                b.args
            );
        }
        other => panic!("expected Bound, got {other:?}"),
    }
}

/// TIER 0 (found live 2026-07-07): the unpack freshness oracle must be the
/// BUNDLE'S CONTENT, never mtimes — tar restores the archive's stored (old)
/// mtimes, so "bundle newer than manifest" was true FOREVER after any install
/// and every autocomplete keystroke / Review bind re-ran tar over a live dir.
/// Once the plugin had SPAWNED (uv drops `.venv` inside the unpacked dir), the
/// re-tar collided and the manifest became intermittently unreadable — the
/// installed plugin stopped binding in the live app.
#[test]
fn unpack_short_circuits_on_same_bundle_and_survives_spawn_debris() {
    ensure_db();
    let group = "bind-group-debris";
    create_fresh_group(group, "Debris Group");
    let tar = make_bundle_tar("debrio");
    storage::install_plugin_bundle(group, "debrio", &tar).expect("install");
    let dir = storage::ensure_bundle_unpacked("debrio").expect("unpacked");

    // Reproduce the live dir state: a spawn dropped `.venv` (with a symlink)
    // into the unpack, and the manifest's restored mtime is far OLDER than the
    // bundle file (tar restores archived mtimes — the live smoking gun).
    let venv_bin = dir.join(".venv").join("bin");
    std::fs::create_dir_all(&venv_bin).expect("venv dirs");
    std::os::unix::fs::symlink("/usr/bin/true", venv_bin.join("python")).expect("venv symlink");
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
    let manifest = dir.join("manifest.json");
    let f = std::fs::File::options().append(true).open(&manifest).expect("open manifest");
    f.set_modified(old).expect("age manifest");
    drop(f);

    // Same bundle bytes ⇒ the unpack STANDS: no re-extract, spawn debris intact.
    let again = storage::ensure_bundle_unpacked("debrio").expect("short-circuit");
    assert_eq!(again, dir);
    assert!(
        venv_bin.join("python").symlink_metadata().is_ok(),
        "a same-content call must not re-extract (the spawn's .venv survives)"
    );

    // A CORRUPT unpack self-heals from the bundle even with debris present.
    std::fs::write(&manifest, b"{ definitely not json }").expect("corrupt manifest");
    let healed = storage::ensure_bundle_unpacked("debrio").expect("self-heal");
    let m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(healed.join("manifest.json")).expect("read"))
            .expect("healed manifest parses");
    assert_eq!(m["name"], "debrio");
}

/// TIER 0 — the live call pattern: autocomplete (every keystroke), Review-time
/// binds, and installs all hit `ensure_bundle_unpacked` CONCURRENTLY. Every
/// caller must receive a directory whose manifest parses — a half-extracted
/// manifest visible to a reader is exactly the live "installed plugin won't
/// bind" failure.
#[test]
fn concurrent_unpack_callers_always_get_a_readable_manifest() {
    ensure_db();
    let group = "bind-group-race";
    create_fresh_group(group, "Race Group");
    let tar = make_bundle_tar("racio");
    storage::install_plugin_bundle(group, "racio", &tar).expect("install");

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Chaos writer: keep deleting the freshness marker so readers must re-extract.
    let chaos = {
        let stop = stop.clone();
        let marker = storage::plugin_bundles_dir().join("racio").join(".cyan_bundle_hash");
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = std::fs::remove_file(&marker);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        })
    };
    let readers: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..25 {
                    let dir = storage::ensure_bundle_unpacked("racio")
                        .expect("every concurrent caller gets an unpacked dir");
                    cyan_mcp::Manifest::from_bundle(&dir)
                        .expect("every concurrent caller reads a WHOLE manifest");
                }
            })
        })
        .collect();
    for r in readers {
        r.join().expect("reader thread");
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    chaos.join().expect("chaos thread");
}

/// TIER 0 — the Review (compile) pass must stamp ENGINE TRUTH the authoring
/// surface can render: a bound step's `pipeline.command` reads `@plugin.tool`
/// and `metadata.mcp_tool.bound == true`; a step naming an UNINSTALLED plugin
/// stamps `mcp_tool_miss` with the actionable reason. (Found live: the view
/// only had the "cyan-lens" placeholder, so every step displayed
/// "unbound + send to AI (Lens)" even when the engine had bound it.)
#[test]
fn compile_stamps_bound_tool_command_and_miss_reason() {
    ensure_db();
    let group = "bind-group-compile";
    let (default_ws, _) = create_fresh_group(group, "Compile Group");
    let board = "bind-board-compile";
    storage::board_insert(board, &default_ws, "Compile Board", chrono::Utc::now().timestamp())
        .expect("board insert");
    let tar = make_bundle_tar("testio");
    storage::install_plugin_bundle(group, "testio", &tar).expect("install");
    storage::file_insert(
        "compile-clip",
        Some(group),
        Some(&default_ws),
        Some(board),
        "cut_v1.mp4",
        "c0ffee01",
        7,
        "peer",
        1,
    )
    .expect("file insert");
    storage::file_set_local_path("compile-clip", "/data/files/c0ffee01").expect("path");

    storage::cell_insert(
        "cell-bound",
        board,
        "step",
        0,
        Some("push the cut @testio.upload_file account_id=a folder_id=f #cut_v1.mp4"),
    )
    .expect("cell 1");
    storage::cell_insert(
        "cell-miss",
        board,
        "step",
        1,
        Some("publish via @notinstalled.publish now"),
    )
    .expect("cell 2");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let out = rt
        .block_on(cyan_backend::pipeline::compile_via_llm(board, &tx))
        .expect("compile succeeds");
    assert_eq!(out["applied"], 2);

    let mut by_cell: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    while let Ok(msg) = rx.try_recv() {
        if let cyan_backend::models::commands::CommandMsg::UpdateNotebookCell {
            id,
            metadata_json: Some(meta),
            ..
        } = msg
        {
            by_cell.insert(id, serde_json::from_str(&meta).expect("metadata json"));
        }
    }

    let bound = &by_cell["cell-bound"];
    assert_eq!(bound["mcp_tool"]["bound"], true, "engine truth: bound");
    assert_eq!(bound["mcp_tool"]["plugin_id"], "testio");
    assert_eq!(bound["mcp_tool"]["tool"], "upload_file");
    assert_eq!(
        bound["pipeline"]["command"], "@testio.upload_file",
        "the config's command must carry the REAL route for the authoring surface"
    );

    let miss = &by_cell["cell-miss"];
    assert_eq!(miss["mcp_tool_miss"]["mention"], "@notinstalled.publish");
    assert_eq!(miss["mcp_tool_miss"]["reason"], "plugin_not_installed_in_group");
    assert!(
        miss["pipeline"]["command"].is_null(),
        "a missed mention must not fabricate a command"
    );
}

/// A RE-installed (newer) bundle refreshes the unpack — a plugin update must
/// land its new tools on the device (the old short-circuit kept the stale
/// manifest forever).
#[test]
fn reinstall_refreshes_a_stale_unpack() {
    ensure_db();
    let group = "bind-group-fresh";
    create_fresh_group(group, "Fresh Group");
    let tar1 = make_bundle_tar("freshio");
    storage::install_plugin_bundle(group, "freshio", &tar1).expect("install v1");
    let dir = storage::ensure_bundle_unpacked("freshio").expect("unpacked v1");
    // Simulate a STALE unpack: age the manifest far behind, then re-install.
    let manifest = dir.join("manifest.json");
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    let f = std::fs::File::options()
        .append(true)
        .open(&manifest)
        .expect("open manifest");
    f.set_modified(old).expect("age manifest");
    drop(f);
    std::fs::write(&manifest, b"{ not even json }").expect("corrupt stale manifest");
    let f2 = std::fs::File::options()
        .append(true)
        .open(&manifest)
        .expect("reopen manifest");
    f2.set_modified(old).expect("re-age manifest");
    drop(f2);
    // Re-install (a NEWER bundle file) — the unpack must refresh.
    let tar2 = make_bundle_tar("freshio");
    storage::install_plugin_bundle(group, "freshio", &tar2).expect("re-install");
    let dir2 = storage::ensure_bundle_unpacked("freshio").expect("unpacked v2");
    let m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir2.join("manifest.json")).expect("read"))
            .expect("the refreshed manifest parses again");
    assert_eq!(m["name"], "freshio");
}

// ════════════════════════════════════════════════════════════════════════════
// ALIAS CONTRACT (FABLE_FULL_AUDIT headline 2): an alias that matches more than
// one manifest tool HARD-FAILS the bind — the resolver never picks arbitrarily.
// Live repro: the stale installed frameio bundle carried `upload` on BOTH the
// curated `upload_file` AND the raw generated twin
// `post_v4_…_files_local_upload` (which the plugin process never registers);
// first-match bound the twin → "Unknown tool" at dispatch, intermittently.
// ════════════════════════════════════════════════════════════════════════════

/// A strict manifest with `tools`, for driving `bind_with_manifest` directly.
fn manifest_with_tools(name: &str, tools: serde_json::Value) -> cyan_mcp::Manifest {
    let manifest_json = serde_json::json!({
        "name": name,
        "version": "0.1.0",
        "description": "alias contract fixture",
        "runtime": "python-uv",
        "credentials": { "kind": "oauth2", "provider": "adobe_ims", "locality": "tenant" },
        "extra_credentials": [],
        "events_emitted": [],
        "tools": tools
    });
    serde_json::from_value(manifest_json).expect("strict manifest")
}

fn upload_tool_json(name: &str, aliases: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "when_to_use": "Push a finished cut to review.",
        "aliases": aliases,
        "io_types": { "input": ["video"], "output": ["json"] },
        "stage": "review",
        "side_effects": ["external_send"],
        "locality": "local",
        "input_schema": {
            "type": "object",
            "properties": { "file_path": {"type": "string"} },
            "required": ["file_path"]
        },
        "output_schema": {"type": "object"}
    })
}

#[test]
fn ambiguous_alias_hard_fails_never_binds_arbitrarily() {
    ensure_db();
    let group = "bind-group-alias";
    let (default_ws, _) = create_fresh_group(group, "Bind Group Alias");
    let board = "bind-board-alias";
    storage::board_insert(board, &default_ws, "Alias Board", chrono::Utc::now().timestamp())
        .expect("board insert");

    const RAW_TWIN: &str = "post_v4_accounts_account_id_folders_folder_id_files_local_upload";

    // The STALE-BUNDLE shape: two tools both carry the `upload` alias.
    let stale = manifest_with_tools(
        "staleio",
        serde_json::json!([
            upload_tool_json("upload_file", &["upload", "push"]),
            upload_tool_json(RAW_TWIN, &["upload", "push"]),
        ]),
    );

    let content = "upload to @staleio.upload for producer review /needs-approval";
    let mention = workflow_bind::parse_mention(content).expect("mention");
    match workflow_bind::bind_with_manifest(board, group, content, &mention, &stale) {
        workflow_bind::BindOutcome::Miss { mention, reason } => {
            assert_eq!(mention, "@staleio.upload");
            assert!(
                reason.starts_with("alias_ambiguous"),
                "the multi-match alias is a HARD compile failure, got reason {reason:?}"
            );
            // The authoring surface must be able to tell the user what collided.
            assert!(
                reason.contains("upload_file") && reason.contains(RAW_TWIN),
                "the miss names every candidate so the author can pin one: {reason:?}"
            );
        }
        other => panic!("a 2-tool alias must NEVER bind arbitrarily, got {other:?}"),
    }

    // An exact tool NAME stays deterministic even on the stale bundle — the
    // live workaround (`@frameio.upload_file`) must keep working.
    let pinned = "upload to @staleio.upload_file for review";
    let mention = workflow_bind::parse_mention(pinned).expect("mention");
    match workflow_bind::bind_with_manifest(board, group, pinned, &mention, &stale) {
        workflow_bind::BindOutcome::Bound(b) => assert_eq!(b.tool, "upload_file"),
        other => panic!("exact-name pin must bind, got {other:?}"),
    }

    // The CURATED shape (one upload verb, not two): the alias binds to exactly
    // `upload_file`, deterministically.
    let curated = manifest_with_tools(
        "curatedio",
        serde_json::json!([upload_tool_json("upload_file", &["upload", "push"])]),
    );
    let content = "upload to @curatedio.upload for producer review";
    let mention = workflow_bind::parse_mention(content).expect("mention");
    match workflow_bind::bind_with_manifest(board, group, content, &mention, &curated) {
        workflow_bind::BindOutcome::Bound(b) => {
            assert_eq!(b.tool, "upload_file", "the curated alias binds to exactly upload_file");
        }
        other => panic!("curated alias must bind, got {other:?}"),
    }
}
