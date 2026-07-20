//! P0 (Frame.io Phase 1) — group ↔ workspace ↔ board ↔ installed-plugin linkage.
//!
//! Repro'd in the app: fresh (non-demo) group → Marketplace → Install → toast
//! "install failed: FOREIGN KEY constraint failed", and the board's Workflow face
//! stays on "No plugins installed in this group".
//!
//! Root cause: the bundled SQLite is compiled with `SQLITE_DEFAULT_FOREIGN_KEYS=1`,
//! so `workspaces.group_id → groups(id)` is ENFORCED — an install aimed at a group
//! id with no `groups` row (the app passed a stale/`"default"` id) dies inside
//! `provision_group_workspaces` with SQLite's cryptic FK message.
//!
//! These tests run the ENGINE's real DDL (`cyan_backend::ensure_schema`) — not the
//! FK-less copy older test files carry — so FK enforcement behaves exactly as in
//! the shipping app.

use std::sync::Once;

use cyan_backend::{storage, workflow};
use cyan_backend::mcp_host::{PLUGINS_WORKSPACE_NAME, PLUGIN_BUNDLE_SUFFIX};
use cyan_backend::models::core::Group;

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB carrying the engine's REAL
/// schema (FKs included), then the storage migrations — the same two steps the
/// FFI init path runs (`ensure_schema` + `storage::init_db`).
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("install-linkage.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            cyan_backend::ensure_schema(&conn).expect("engine schema");
        }
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");

        // Isolate the on-disk bundle dir so tests never touch a real ~/.cyan/plugins.
        let plugins = tempfile::tempdir().expect("tmp plugins dir");
        unsafe { std::env::set_var("CYAN_PLUGINS_ROOT", plugins.path()) };
        std::mem::forget(plugins);
        std::mem::forget(dir); // leak for the process lifetime
    });
}

/// Create a fresh group exactly as the engine's CreateGroup command does: a `groups`
/// row plus the ROUND8 §W3 auto-seeded default + system "Plugins" workspaces.
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

/// The P0 gate: a fresh group's install must succeed with NO FK error, land the
/// plugin in THAT group's system Plugins workspace, and surface it in a board's
/// `@` autocomplete (⇒ "No plugins installed in this group" is gone).
#[test]
fn fresh_group_install_lands_in_that_groups_plugins_workspace_and_autocomplete() {
    ensure_db();

    let group = "fresh-frameio-group";
    let (default_ws, plugins_ws) = create_fresh_group(group, "Fresh Group");

    // The two ROUND8 §W3 workspaces exist and the Plugins one is system-flagged.
    let wss = storage::workspace_list_by_group(group).expect("list workspaces");
    assert!(
        wss.iter().any(|w| w.id == default_ws && w.name == storage::DEFAULT_WORKSPACE_NAME),
        "fresh group must carry the default General workspace"
    );
    let plugins = wss
        .iter()
        .find(|w| w.id == plugins_ws)
        .expect("fresh group must carry the system Plugins workspace");
    assert_eq!(plugins.name, PLUGINS_WORKSPACE_NAME);
    assert!(plugins.system, "the Plugins workspace is system/non-deletable");

    // A board in the group's default workspace — the surface the user authors on.
    let board = "fresh-group-board";
    storage::board_insert(board, &default_ws, "Board 1", chrono::Utc::now().timestamp())
        .expect("board insert");

    // The install the app performs (FFI → storage::install_plugin_bundle) — this is
    // the call that toasted "FOREIGN KEY constraint failed" in the app.
    let file_id = storage::install_plugin_bundle(group, "frameio", b"frameio-cyanplugin-bytes")
        .expect("install into a fresh, existing group must succeed — no FK error");

    // Linkage: the bundle row lives in THIS group's Plugins workspace.
    assert_eq!(
        storage::plugins_workspace_id(group),
        plugins_ws,
        "deterministic Plugins workspace id"
    );
    let bundles = storage::plugin_bundles_in_group(group, PLUGINS_WORKSPACE_NAME, PLUGIN_BUNDLE_SUFFIX)
        .expect("list bundles");
    let installed = bundles
        .iter()
        .find(|b| b.file_id == file_id)
        .expect("installed bundle row is in the group's Plugins workspace");
    assert_eq!(installed.name, format!("frameio{PLUGIN_BUNDLE_SUFFIX}"));

    // The board's `@` autocomplete sees it — the "No plugins installed" face is gone.
    let idx = workflow::autocomplete_index(board);
    assert_eq!(idx.tenant_id, group, "autocomplete is tenant-scoped to the board's group");
    assert!(
        idx.plugins.iter().any(|e| e.value == "frameio"),
        "@frameio must appear in the board's autocomplete after install; got: {:?}",
        idx.plugins
    );
}

/// An install aimed at a group id that has NO `groups` row (what the app actually
/// sent: a stale or placeholder `"default"` id) must fail as a CLEAR precondition
/// error naming the group — never SQLite's raw "FOREIGN KEY constraint failed" —
/// and must not leave orphan workspace rows behind.
#[test]
fn install_into_unknown_group_is_a_clear_error_not_a_raw_fk_failure() {
    ensure_db();

    let phantom = "default"; // the app's literal fallback group id
    let err = storage::install_plugin_bundle(phantom, "frameio", b"frameio-cyanplugin-bytes")
        .expect_err("install into a nonexistent group must fail");
    let msg = format!("{err:#}");

    assert!(
        !msg.contains("FOREIGN KEY"),
        "SQLite's cryptic FK message must not leak to the caller/toast; got: {msg}"
    );
    assert!(
        msg.contains(phantom) && msg.to_lowercase().contains("unknown group"),
        "the error must name the unknown group so the app/user can act on it; got: {msg}"
    );

    // No half-provisioned state: the phantom group must not have gained workspaces.
    let wss = storage::workspace_list_by_group(phantom).expect("list workspaces");
    assert!(
        wss.is_empty(),
        "no workspaces may be provisioned for a nonexistent group; got: {wss:?}"
    );
}

/// Whether `plugin_id` currently lists as installed in `group`.
fn is_listed(group: &str, plugin_id: &str) -> bool {
    storage::plugin_bundles_in_group(group, PLUGINS_WORKSPACE_NAME, PLUGIN_BUNDLE_SUFFIX)
        .expect("list bundles")
        .iter()
        .any(|b| b.name == format!("{plugin_id}{PLUGIN_BUNDLE_SUFFIX}"))
}

/// UNINSTALL, rung 1 (found live 2026-07-15 — Rick installed a plugin by accident and
/// could not remove it).
///
/// A plugin is removed by the ORDINARY file tombstone — the same `file_soft_delete`
/// path `CommandMsg::DeleteFile` drives, which broadcasts `NetworkEvent::FileDeleted`
/// and converges to peers. There is no second removal path.
///
/// The bug this pins: `plugin_bundles_in_group` was the ONE file read that ignored the
/// tombstone (R10FB §F4 says all of them filter `deleted=0`), so the delete path ran,
/// the row tombstoned, peers converged — and NOTHING observable changed. The bundle
/// kept listing and `@plugin.` kept resolving in authoring. Uninstall was unreachable
/// not because the path was missing, but because the read ignored it.
#[test]
fn tombstoned_plugin_bundle_is_uninstalled_and_leaves_authoring() {
    ensure_db();

    let group = "uninstall-group";
    let (default_ws, _plugins_ws) = create_fresh_group(group, "Uninstall Group");
    let board = "uninstall-board";
    storage::board_insert(board, &default_ws, "Board 1", chrono::Utc::now().timestamp())
        .expect("board insert");

    let file_id = storage::install_plugin_bundle(group, "frameio", b"frameio-cyanplugin-bytes")
        .expect("install");
    assert!(is_listed(group, "frameio"), "precondition: the install lists");
    assert!(
        workflow::autocomplete_index(board).plugins.iter().any(|e| e.value == "frameio"),
        "precondition: @frameio resolves in authoring after install"
    );

    // Uninstall == the ordinary file tombstone, addressed by install's DETERMINISTIC
    // file id. No new path, no bytes touched.
    storage::file_soft_delete(&file_id, chrono::Utc::now().timestamp()).expect("tombstone");

    assert!(
        !is_listed(group, "frameio"),
        "a tombstoned bundle must NOT list as installed — this is the assertion that \
         was false before the deleted=0 filter, and the reason an accidental install \
         could not be removed"
    );
    assert!(
        !workflow::autocomplete_index(board).plugins.iter().any(|e| e.value == "frameio"),
        "@frameio must leave the board's authoring surface once uninstalled"
    );
}

/// Idempotence: tombstoning an already-tombstoned bundle is a no-op, not an error —
/// uninstalling something already uninstalled must never fail.
#[test]
fn repeat_uninstall_is_a_no_op_not_an_error() {
    ensure_db();

    let group = "uninstall-idem-group";
    create_fresh_group(group, "Idempotent Uninstall");
    let file_id = storage::install_plugin_bundle(group, "frameio", b"frameio-cyanplugin-bytes")
        .expect("install");
    let now = chrono::Utc::now().timestamp();

    storage::file_soft_delete(&file_id, now).expect("first tombstone");
    storage::file_soft_delete(&file_id, now).expect("repeat tombstone must be a no-op, not an error");
    assert!(!is_listed(group, "frameio"), "still uninstalled after a repeat tombstone");

    // A never-installed plugin's deterministic id: tombstoning it is also a no-op.
    storage::file_soft_delete("no-such-plugin-file-id", now)
        .expect("tombstoning an absent bundle must be a no-op, not an error");
}

/// Bytes are device-global; install records are per-group. Uninstalling in group A must
/// NOT disturb group B, which shares the SAME on-disk bundle bytes
/// (`plugin_bundles_dir()` is keyed by plugin id alone).
///
/// This is why uninstall tombstones the row and leaves the bytes: the `objects` row is
/// the per-group install fact, the file is a shared device cache. No refcount needed —
/// nothing ever deletes the bytes.
#[test]
fn uninstall_in_one_group_leaves_another_groups_install_intact() {
    ensure_db();

    let (a, b) = ("shared-bytes-group-a", "shared-bytes-group-b");
    create_fresh_group(a, "Group A");
    let (b_default_ws, _) = create_fresh_group(b, "Group B");
    let b_board = "shared-bytes-board-b";
    storage::board_insert(b_board, &b_default_ws, "B Board", chrono::Utc::now().timestamp())
        .expect("board insert");

    let a_file = storage::install_plugin_bundle(a, "frameio", b"frameio-cyanplugin-bytes")
        .expect("install into A");
    let b_file = storage::install_plugin_bundle(b, "frameio", b"frameio-cyanplugin-bytes")
        .expect("install into B");
    assert_ne!(a_file, b_file, "install rows are per-group (distinct deterministic ids)");

    storage::file_soft_delete(&a_file, chrono::Utc::now().timestamp()).expect("uninstall from A");

    assert!(!is_listed(a, "frameio"), "A is uninstalled");
    assert!(
        is_listed(b, "frameio"),
        "B must keep its install — A's uninstall may never reach across groups"
    );
    assert!(
        workflow::autocomplete_index(b_board).plugins.iter().any(|e| e.value == "frameio"),
        "@frameio must still resolve in B's authoring — the shared device bytes are untouched"
    );
}
