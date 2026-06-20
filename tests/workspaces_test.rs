//! W3 — Auto-provision workspaces on group creation; no empty groups (ROUND8 §W3).
//!
//! Creating a group **auto-seeds** two workspaces and never leaves the group empty: a
//! **default** workspace — where the user lands (non-system, deletable) — and a system
//! **"Plugins"** workspace (per group) that holds that group's installed plugin files,
//! flagged `system` and **non-deletable**.
//!
//! Both ride the existing snapshot/digest replication (see
//! `tests/substrate_workspaces_mp.rs` for the cross-peer convergence proof) — no new
//! transfer. Every row is tenant-scoped (tenant == group).
//!
//! No live deps: pure storage path. Every assertion is synchronous on the engine's own
//! `storage::*` (no waits needed).

use std::path::Path;
use std::sync::Once;

use cyan_backend::storage;

static DB_INIT: Once = Once::new();

/// Init the process-global storage once over a temp DB with the base schema the engine
/// migrations assume exist. `workspaces` is created here WITHOUT `is_system`, so the
/// engine migration that adds the column is exercised on a DB that predates it.
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("workspaces.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
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

/// The create path: insert a group, then auto-provision its workspaces. This mirrors
/// what `CommandActor::CreateGroup` does — a group is never born without workspaces.
fn create_group(group: &str) -> (cyan_backend::models::core::Workspace, cyan_backend::models::core::Workspace) {
    storage::group_insert_simple(group, "A Group", "folder.fill", "#00AEEF").expect("group");
    storage::provision_group_workspaces(group, None).expect("provision")
}

// ════════════════════════════════════════════════════════════════════════════
// 1. Group creation seeds a default workspace (the user lands here).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn create_group_seeds_default_workspace() {
    ensure_db();
    let group = "ws-default-grp";
    let (default, _plugins) = create_group(group);

    // The returned default workspace is the non-system landing workspace.
    assert!(!default.system, "the default workspace is NOT a system workspace");
    assert_eq!(default.group_id, group, "default workspace is tenant-scoped to its group");
    assert_eq!(default.name, storage::DEFAULT_WORKSPACE_NAME, "default workspace has the default name");
    assert_eq!(
        default.id,
        storage::default_workspace_id(group),
        "default workspace id is deterministic from the group (idempotent provisioning)"
    );

    // It is actually persisted and visible in the group's workspace list.
    let list = storage::workspace_list_by_group(group).expect("list");
    let landing: Vec<_> = list.iter().filter(|w| !w.system).collect();
    assert_eq!(landing.len(), 1, "exactly one default (landing) workspace");
    assert_eq!(landing[0].id, default.id, "the listed default matches the seeded one");
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Group creation seeds a system "Plugins" workspace (per group).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn create_group_seeds_plugins_workspace() {
    ensure_db();
    let group = "ws-plugins-grp";
    let (_default, plugins) = create_group(group);

    assert!(plugins.system, "the Plugins workspace is flagged system");
    assert_eq!(plugins.name, storage::PLUGINS_WORKSPACE_NAME, "the system workspace is named 'Plugins'");
    assert_eq!(plugins.group_id, group, "the Plugins workspace is per-group (tenant-scoped)");
    assert_eq!(
        plugins.id,
        storage::plugins_workspace_id(group),
        "Plugins workspace id is deterministic from the group"
    );

    // Exactly one system Plugins workspace exists for the group.
    let list = storage::workspace_list_by_group(group).expect("list");
    let system: Vec<_> = list.iter().filter(|w| w.system).collect();
    assert_eq!(system.len(), 1, "exactly one system (Plugins) workspace per group");
    assert_eq!(system[0].id, plugins.id);
    assert_eq!(system[0].name, storage::PLUGINS_WORKSPACE_NAME);
}

// ════════════════════════════════════════════════════════════════════════════
// 3. The Plugins workspace is system AND non-deletable.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn plugins_workspace_is_system_nondeletable() {
    ensure_db();
    let group = "ws-nondelete-grp";
    let (default, plugins) = create_group(group);

    // The flag is queryable directly.
    assert!(storage::workspace_is_system(&plugins.id), "Plugins workspace reads back as system");
    assert!(!storage::workspace_is_system(&default.id), "the default workspace is not system");

    // Deleting a system workspace is refused — the row survives.
    assert!(
        storage::workspace_delete(&plugins.id).is_err(),
        "deleting the system Plugins workspace must be refused"
    );
    let after = storage::workspace_list_by_group(group).expect("list");
    assert!(
        after.iter().any(|w| w.id == plugins.id),
        "the Plugins workspace must still exist after a refused delete"
    );

    // A normal (non-system) workspace remains deletable.
    assert!(
        storage::workspace_delete(&default.id).is_ok(),
        "a non-system workspace can still be deleted"
    );
    let after = storage::workspace_list_by_group(group).expect("list");
    assert!(
        !after.iter().any(|w| w.id == default.id),
        "the default workspace was deleted"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 4. No create path yields an empty group; provisioning is idempotent.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn no_api_path_yields_empty_group() {
    ensure_db();
    let group = "ws-noempty-grp";

    // Before provisioning the group has no workspaces…
    storage::group_insert_simple(group, "Empty?", "folder.fill", "#00AEEF").expect("group");
    assert_eq!(
        storage::workspace_list_by_group(group).expect("list").len(),
        0,
        "a bare group record starts with no workspaces"
    );

    // …but the create path provisions, so the group is never left empty: it has BOTH
    // the default and the Plugins workspace.
    storage::provision_group_workspaces(group, None).expect("provision");
    let list = storage::workspace_list_by_group(group).expect("list");
    assert_eq!(list.len(), 2, "the create path seeds exactly two workspaces (default + Plugins)");
    assert!(list.iter().any(|w| !w.system), "one is the default landing workspace");
    assert!(list.iter().any(|w| w.system), "one is the system Plugins workspace");

    // Provisioning is idempotent (deterministic ids) — re-running never duplicates, so
    // a replayed gossip/snapshot of the seed converges instead of multiplying.
    storage::provision_group_workspaces(group, None).expect("re-provision");
    assert_eq!(
        storage::workspace_list_by_group(group).expect("list").len(),
        2,
        "re-provisioning is idempotent — still exactly two workspaces"
    );
}
