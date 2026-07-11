// src/storage.rs
//
// Unified storage layer for Cyan
// All database operations go through this module

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::models::core::{Group, Workspace};
use crate::util::MutexExt;
use crate::models::dto::*;

static DB: OnceLock<Mutex<Connection>> = OnceLock::new();

/// Resolve the on-disk path of the cyan SQLite database.
///
/// The FFI/app contract passes an explicit db path; that always wins so shipping
/// behavior is unchanged. When no explicit path is given we fall back to
/// `$CYAN_DATA_DIR/cyan.db` (the env `run_multi` / the app set per instance), and
/// finally to `./cyan.db`. Resolution is pure and deterministic, so a relaunch
/// with the same inputs always resolves to the SAME database file — which is what
/// lets identity and groups persist across launches.
pub fn resolve_db_path(requested: &str) -> PathBuf {
    if !requested.trim().is_empty() {
        return PathBuf::from(requested);
    }
    let dir = std::env::var("CYAN_DATA_DIR").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(dir).join("cyan.db")
}

/// Open a SQLite connection at `db_path`, creating the parent directory first.
///
/// A fresh instance whose data dir does not exist yet (e.g. a brand new
/// `CYAN_DATA_DIR`) used to panic with `CannotOpen` and take storage down for the
/// whole engine. We now `create_dir_all` the parent and return a typed error on
/// failure instead of panicking — a bad data dir degrades gracefully.
pub fn open_db(db_path: &Path) -> Result<Connection> {
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                tracing::error!(
                    "Failed to create data dir {}: {} (os error)",
                    parent.display(),
                    e
                );
                anyhow!("create data dir {}: {e}", parent.display())
            })?;
        }
    tracing::info!("Opening cyan database at {}", db_path.display());
    Connection::open(db_path).map_err(|e| {
        tracing::error!(
            "Failed to open database {}: {} (os error)",
            db_path.display(),
            e
        );
        anyhow!("open database {}: {e}", db_path.display())
    })
}

pub fn init_db(path: &str) -> Result<()> {
    let resolved = resolve_db_path(path);
    let conn = open_db(&resolved)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    run_migrations(&conn)?;
    DB.set(Mutex::new(conn)).map_err(|_| anyhow::anyhow!("DB already initialized"))?;
    Ok(())
}

pub fn db() -> &'static Mutex<Connection> {
    DB.get().expect("DB not initialized - call init_db first")
}

/// Non-panicking accessor for the JSON dispatch paths (`changelist::command`,
/// `review_state::command`): a command arriving before `init_db` must surface as a
/// clean `{"error": ...}` JSON string, never a panic across the FFI boundary.
pub fn try_db() -> Option<&'static Mutex<Connection>> {
    DB.get()
}

/// How long a READ/UI path may wait for the DB before giving up and returning
/// its empty shape. Short enough that a click never feels hung; long enough
/// that ordinary write contention (single statements) always wins.
pub const READ_LOCK_BUDGET: std::time::Duration = std::time::Duration::from_millis(250);

/// Bounded acquire for READ paths that cross the FFI into the UI. NEVER parks
/// the calling thread on the DB mutex: spins `try_lock` up to `budget`, then
/// yields `None` (callers return their empty shape and the UI's own cadence
/// retries). Poison recovers like `lock_safe`. Same-thread re-entrancy cannot
/// deadlock through this door — it burns the budget and returns `None` — but
/// the envelope read path also threads one `&Connection` down so re-entrant
/// acquisition doesn't arise in the first place.
pub fn try_db_read(budget: std::time::Duration) -> Option<std::sync::MutexGuard<'static, Connection>> {
    let db = try_db()?;
    let deadline = std::time::Instant::now() + budget;
    loop {
        match db.try_lock() {
            Ok(guard) => return Some(guard),
            Err(std::sync::TryLockError::Poisoned(p)) => return Some(p.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => {
                if std::time::Instant::now() >= deadline {
                    return None;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// GROUPS
// ═══════════════════════════════════════════════════════════════════════════

pub fn group_insert(g: &Group) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO groups (id, name, icon, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![g.id, g.name, g.icon, g.color, g.created_at],
    )?;
    Ok(())
}

pub fn group_rename(id: &str, name: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("UPDATE groups SET name=?1 WHERE id=?2", params![name, id])?;
    Ok(())
}

pub fn group_delete(id: &str) -> Result<bool> {
    let conn = db().lock_safe();

    // Get workspace IDs first
    let workspace_ids: Vec<String> = {
        let mut stmt = conn.prepare("SELECT id FROM workspaces WHERE group_id=?1")?;
        stmt.query_map(params![id], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect()
    };

    // Get board IDs (objects with type='whiteboard' in these workspaces)
    let board_ids: Vec<String> = if !workspace_ids.is_empty() {
        let placeholders: String = workspace_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("SELECT id FROM objects WHERE type='whiteboard' AND workspace_id IN ({})", placeholders);
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::ToSql> = workspace_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        stmt.query_map(params.as_slice(), |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect()
    } else {
        vec![]
    };

    // Delete integration_bindings for group and its workspaces
    {
        let mut all_scope_ids = vec![id.to_string()];
        all_scope_ids.extend(workspace_ids.clone());
        if !all_scope_ids.is_empty() {
            let placeholders: String = all_scope_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!("DELETE FROM integration_bindings WHERE scope_id IN ({})", placeholders);
            let params: Vec<&dyn rusqlite::ToSql> = all_scope_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
            let _ = conn.execute(&sql, params.as_slice());
        }
    }

    // Delete file_transfers for files at any level in this group
    // Board-level files
    if !board_ids.is_empty() {
        let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM file_transfers WHERE file_id IN (SELECT id FROM objects WHERE board_id IN ({}))", placeholders);
        let params: Vec<&dyn rusqlite::ToSql> = board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let _ = conn.execute(&sql, params.as_slice());
    }
    // Workspace-level files
    if !workspace_ids.is_empty() {
        let placeholders: String = workspace_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM file_transfers WHERE file_id IN (SELECT id FROM objects WHERE workspace_id IN ({}))", placeholders);
        let params: Vec<&dyn rusqlite::ToSql> = workspace_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let _ = conn.execute(&sql, params.as_slice());
    }
    // Group-level files
    conn.execute("DELETE FROM file_transfers WHERE file_id IN (SELECT id FROM objects WHERE group_id=?1)", params![id])?;

    // Delete board content (whiteboard_elements, notebook_cells, board_metadata)
    if !board_ids.is_empty() {
        let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let params: Vec<&dyn rusqlite::ToSql> = board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

        let sql = format!("DELETE FROM whiteboard_elements WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());

        let sql = format!("DELETE FROM notebook_cells WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());

        let sql = format!("DELETE FROM board_metadata WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());

        // Delete files attached to boards
        let sql = format!("DELETE FROM objects WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());
    }

    // Delete objects at workspace level (boards, chats, workspace-level files)
    if !workspace_ids.is_empty() {
        let placeholders: String = workspace_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM objects WHERE workspace_id IN ({})", placeholders);
        let params: Vec<&dyn rusqlite::ToSql> = workspace_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let _ = conn.execute(&sql, params.as_slice());
    }

    // Delete objects at group level (group-level files)
    conn.execute("DELETE FROM objects WHERE group_id=?1", params![id])?;

    // Delete workspaces and group
    conn.execute("DELETE FROM workspaces WHERE group_id=?1", params![id])?;
    let deleted = conn.execute("DELETE FROM groups WHERE id=?1", params![id])? > 0;
    Ok(deleted)
}

pub fn group_list_ids() -> HashSet<String> {
    (|| -> rusqlite::Result<HashSet<String>> {
        let conn = db().lock_safe();
        let mut stmt = conn.prepare("SELECT id FROM groups")?;
        let mut rows = stmt.query([])?;
        let mut out = HashSet::new();
        while let Some(r) = rows.next()? {
            out.insert(r.get::<_, String>(0)?);
        }
        Ok(out)
    })()
    .unwrap_or_default()
}

// ═══════════════════════════════════════════════════════════════════════════
// OWNERSHIP HELPERS
// ═══════════════════════════════════════════════════════════════════════════

/// Check if node owns this group
pub fn group_is_owner(group_id: &str, node_id: &str) -> bool {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT owner_node_id FROM groups WHERE id = ?1",
        params![group_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten().as_deref() == Some(node_id)
}

/// Check if node owns this workspace
pub fn workspace_is_owner(workspace_id: &str, node_id: &str) -> bool {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT owner_node_id FROM workspaces WHERE id = ?1",
        params![workspace_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten().as_deref() == Some(node_id)
}

/// Check if node owns this board
pub fn board_is_owner(board_id: &str, node_id: &str) -> bool {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT owner_node_id FROM objects WHERE id = ?1 AND type = 'whiteboard'",
        params![board_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten().as_deref() == Some(node_id)
}

/// Get owner_node_id of a group
pub fn group_get_owner(group_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT owner_node_id FROM groups WHERE id = ?1",
        params![group_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten()
}

/// Stamp the owner identity on a group (mirrors the `owner_node_id` the CreateGroup path
/// writes on INSERT). Used by the in-process demo seed so seeded groups are owned by the
/// app identity and pass the owner-gated rename/delete/deploy checks.
pub fn group_set_owner(group_id: &str, owner_node_id: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "UPDATE groups SET owner_node_id=?1 WHERE id=?2",
        params![owner_node_id, group_id],
    )?;
    Ok(())
}

/// Get owner_node_id of a workspace
pub fn workspace_get_owner(workspace_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT owner_node_id FROM workspaces WHERE id = ?1",
        params![workspace_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten()
}

/// Get owner_node_id of a board
pub fn board_get_owner(board_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT owner_node_id FROM objects WHERE id = ?1 AND type = 'whiteboard'",
        params![board_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten()
}

pub fn group_get(id: &str) -> Result<Option<Group>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT id, name, icon, color, created_at FROM groups WHERE id=?1")?;
    stmt.query_row(params![id], |r| {
        Ok(Group {
            id: r.get(0)?,
            name: r.get(1)?,
            icon: r.get(2)?,
            color: r.get(3)?,
            created_at: r.get(4)?,
        })
    }).optional().map_err(Into::into)
}

/// List all groups
pub fn group_list() -> Result<Vec<Group>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT id, name, icon, color, created_at FROM groups ORDER BY name")?;
    let rows = stmt.query_map([], |r| {
        Ok(Group {
            id: r.get(0)?,
            name: r.get(1)?,
            icon: r.get(2)?,
            color: r.get(3)?,
            created_at: r.get(4)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// WORKSPACES
// ═══════════════════════════════════════════════════════════════════════════

// ROUND8 §W3 — every group is born with these two workspaces (no empty groups):
//   • a **default** landing workspace (where the user lands), and
//   • a system **"Plugins"** workspace (per group; holds that group's installed plugin
//     files; flagged `system` / non-deletable).
/// Name of the default landing workspace seeded on group creation.
pub const DEFAULT_WORKSPACE_NAME: &str = "General";
/// Name of the per-group system workspace that holds installed plugin files.
pub const PLUGINS_WORKSPACE_NAME: &str = "Plugins";

/// Deterministic id of a group's default (landing) workspace. Deterministic so
/// provisioning is idempotent and a replayed gossip/snapshot of the seed converges
/// instead of creating duplicates.
pub fn default_workspace_id(group_id: &str) -> String {
    blake3::hash(format!("default-ws:{group_id}").as_bytes()).to_hex().to_string()
}

/// Deterministic id of a group's system "Plugins" workspace.
pub fn plugins_workspace_id(group_id: &str) -> String {
    blake3::hash(format!("plugins-ws:{group_id}").as_bytes()).to_hex().to_string()
}

/// Auto-seed a group's two workspaces so the group is never empty (ROUND8 §W3): the
/// default landing workspace and the system "Plugins" workspace. Idempotent (deterministic
/// ids + `INSERT OR IGNORE`). Returns `(default, plugins)` so the caller can broadcast the
/// matching `WorkspaceCreated` events. Both ride the existing snapshot/digest replication.
pub fn provision_group_workspaces(
    group_id: &str,
    owner_node_id: Option<&str>,
) -> Result<(Workspace, Workspace)> {
    let now = chrono::Utc::now().timestamp();
    let default = Workspace {
        id: default_workspace_id(group_id),
        group_id: group_id.to_string(),
        name: DEFAULT_WORKSPACE_NAME.to_string(),
        created_at: now,
        system: false,
    };
    let plugins = Workspace {
        id: plugins_workspace_id(group_id),
        group_id: group_id.to_string(),
        name: PLUGINS_WORKSPACE_NAME.to_string(),
        created_at: now,
        system: true,
    };
    {
        let conn = db().lock_safe();
        for ws in [&default, &plugins] {
            conn.execute(
                "INSERT OR IGNORE INTO workspaces (id, group_id, name, created_at, is_system, owner_node_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![ws.id, ws.group_id, ws.name, ws.created_at, ws.system as i32, owner_node_id],
            )?;
        }
    }
    Ok((default, plugins))
}

/// TIER 3.5 (AUTHORING_FIXES_ROUND2): deterministic id of a workspace's default board
/// ("Board 1"). Deterministic ⇒ `INSERT OR IGNORE` makes seeding idempotent on
/// re-delivery.
pub fn default_board_id(workspace_id: &str) -> String {
    blake3::hash(format!("board:{workspace_id}-Board 1").as_bytes()).to_hex().to_string()
}

/// Seed the default board ("Board 1") in a group's landing workspace so a new group is
/// never born board-less. Returns `(board_id, board_name)`. Errors surface (FK to the
/// workspaces row is ENFORCED — the bundled SQLite defaults foreign_keys ON), so a
/// failed seed is a loud log line in the CreateGroup handler, never a silent no-board.
pub fn provision_default_board(
    workspace_id: &str,
    owner_node_id: &str,
    now: i64,
) -> Result<(String, String)> {
    let board_name = "Board 1".to_string();
    let board_id = default_board_id(workspace_id);
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, created_at, owner_node_id) VALUES (?1, ?2, 'whiteboard', ?3, ?4, ?5)",
        params![board_id, workspace_id, board_name, now, owner_node_id],
    )?;
    Ok((board_id, board_name))
}

pub fn workspace_insert(ws: &Workspace) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO workspaces (id, group_id, name, created_at, is_system) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![ws.id, ws.group_id, ws.name, ws.created_at, ws.system as i32],
    )?;
    Ok(())
}

pub fn workspace_rename(id: &str, name: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("UPDATE workspaces SET name=?1 WHERE id=?2", params![name, id])?;
    Ok(())
}

/// ROUND8 §W3: is this a system (non-deletable) workspace — the per-group "Plugins"
/// workspace? Returns false for unknown ids.
pub fn workspace_is_system(id: &str) -> bool {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT is_system FROM workspaces WHERE id = ?1",
        params![id],
        |r| r.get::<_, i32>(0),
    )
    .optional()
    .ok()
    .flatten()
    .map(|v| v != 0)
    .unwrap_or(false)
}

pub fn workspace_delete(id: &str) -> Result<()> {
    // ROUND8 §W3: a system workspace (the per-group Plugins workspace) is non-deletable.
    // Refuse before touching anything so the row — and any installed plugin files it
    // holds — survive. (Deleting the whole group still cascades; this only guards the
    // standalone "delete this workspace" path.)
    if workspace_is_system(id) {
        return Err(anyhow::anyhow!("workspace {id} is a system workspace and cannot be deleted"));
    }

    let conn = db().lock_safe();

    // Delete integration_bindings for this workspace
    conn.execute("DELETE FROM integration_bindings WHERE scope_id=?1", params![id])?;

    // Get board IDs in this workspace
    let board_ids: Vec<String> = {
        let mut stmt = conn.prepare("SELECT id FROM objects WHERE type='whiteboard' AND workspace_id=?1")?;
        stmt.query_map(params![id], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect()
    };

    // Delete file_transfers for files at board level
    if !board_ids.is_empty() {
        let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM file_transfers WHERE file_id IN (SELECT id FROM objects WHERE board_id IN ({}))", placeholders);
        let params: Vec<&dyn rusqlite::ToSql> = board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let _ = conn.execute(&sql, params.as_slice());
    }
    // Delete file_transfers for files at workspace level
    conn.execute("DELETE FROM file_transfers WHERE file_id IN (SELECT id FROM objects WHERE workspace_id=?1)", params![id])?;

    // Delete board content
    if !board_ids.is_empty() {
        let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let params: Vec<&dyn rusqlite::ToSql> = board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

        let sql = format!("DELETE FROM whiteboard_elements WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());

        let sql = format!("DELETE FROM notebook_cells WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());

        let sql = format!("DELETE FROM board_metadata WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());

        // Delete files attached to boards
        let sql = format!("DELETE FROM objects WHERE board_id IN ({})", placeholders);
        let _ = conn.execute(&sql, params.as_slice());
    }

    // Delete objects at workspace level (boards, chats, files)
    conn.execute("DELETE FROM objects WHERE workspace_id=?1", params![id])?;
    conn.execute("DELETE FROM workspaces WHERE id=?1", params![id])?;
    Ok(())
}

pub fn workspace_get_group_id(workspace_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT group_id FROM workspaces WHERE id=?1 LIMIT 1").ok()?;
    stmt.query_row(params![workspace_id], |r| r.get(0)).optional().ok()?
}

/// Get group_id for a board (via its workspace)
pub fn board_get_group_id(board_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    board_get_group_id_with(&conn, board_id)
}

/// `board_get_group_id` against an ALREADY-HELD connection — for callers running
/// inside a dispatch that owns the global DB mutex (re-locking self-deadlocks;
/// the std Mutex is not reentrant).
pub fn board_get_group_id_with(conn: &rusqlite::Connection, board_id: &str) -> Option<String> {
    // Board -> workspace_id -> group_id
    let mut stmt = conn.prepare(
        "SELECT w.group_id FROM workspaces w
         INNER JOIN objects o ON o.workspace_id = w.id
         WHERE o.id = ?1 AND o.type = 'whiteboard' LIMIT 1"
    ).ok()?;
    stmt.query_row(params![board_id], |r| r.get(0)).optional().ok()?
}

pub fn workspace_list_by_group(group_id: &str) -> Result<Vec<Workspace>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT id, group_id, name, created_at, is_system FROM workspaces WHERE group_id=?1 ORDER BY name")?;
    let rows = stmt.query_map(params![group_id], |r| {
        Ok(Workspace {
            id: r.get(0)?,
            group_id: r.get(1)?,
            name: r.get(2)?,
            created_at: r.get(3)?,
            system: r.get::<_, i32>(4)? != 0,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// List workspace IDs for a group (lightweight version)
pub fn workspace_list_ids_by_group(group_id: &str) -> Vec<String> {
    let conn = db().lock_safe();
    let mut stmt = match conn.prepare("SELECT id FROM workspaces WHERE group_id=?1") {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let mut rows = match stmt.query(params![group_id]) {
        Ok(r) => r,
        Err(_) => return vec![],
    };
    let mut out = Vec::new();
    while let Ok(Some(r)) = rows.next() {
        if let Ok(id) = r.get::<_, String>(0) {
            out.push(id);
        }
    }
    out
}

// ═══════════════════════════════════════════════════════════════════════════
// BOARDS (whiteboards stored in objects table)
// ═══════════════════════════════════════════════════════════════════════════

pub fn board_insert(id: &str, workspace_id: &str, name: &str, created_at: i64) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, created_at) VALUES (?1, ?2, 'whiteboard', ?3, ?4)",
        params![id, workspace_id, name, created_at],
    )?;
    Ok(())
}

pub fn board_rename(id: &str, name: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("UPDATE objects SET name=?1 WHERE id=?2 AND type='whiteboard'", params![name, id])?;
    Ok(())
}

pub fn board_delete(id: &str) -> Result<()> {
    let conn = db().lock_safe();
    // Delete file_transfers for files attached to this board
    conn.execute("DELETE FROM file_transfers WHERE file_id IN (SELECT id FROM objects WHERE board_id=?1)", params![id])?;
    // Delete files attached to this board
    conn.execute("DELETE FROM objects WHERE board_id=?1", params![id])?;
    // Delete board content
    conn.execute("DELETE FROM whiteboard_elements WHERE board_id=?1", params![id])?;
    conn.execute("DELETE FROM notebook_cells WHERE board_id=?1", params![id])?;
    conn.execute("DELETE FROM board_metadata WHERE board_id=?1", params![id])?;
    // Delete the board itself
    conn.execute("DELETE FROM objects WHERE id=?1 AND type='whiteboard'", params![id])?;
    Ok(())
}

pub fn board_set_mode(board_id: &str, mode: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("UPDATE objects SET board_mode=?1 WHERE id=?2", params![mode, board_id])?;
    Ok(())
}

pub fn board_get_workspace_id(board_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT workspace_id FROM objects WHERE id=?1 AND type='whiteboard' LIMIT 1").ok()?;
    stmt.query_row(params![board_id], |r| r.get(0)).optional().ok()?
}

pub fn board_list_by_workspaces(workspace_ids: &[String]) -> Result<Vec<WhiteboardDTO>> {
    if workspace_ids.is_empty() { return Ok(vec![]); }
    let conn = db().lock_safe();
    let placeholders: String = workspace_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!("SELECT id, workspace_id, name, created_at FROM objects WHERE type='whiteboard' AND workspace_id IN ({}) ORDER BY name", placeholders);
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> = workspace_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let rows = stmt.query_map(params.as_slice(), |r| {
        Ok(WhiteboardDTO {
            id: r.get(0)?,
            workspace_id: r.get(1)?,
            name: r.get(2)?,
            created_at: r.get(3)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// CHATS
// ═══════════════════════════════════════════════════════════════════════════

/// Insert a chat keyed to a **board** (R11 §1). Chat is board-scoped: the row carries both
/// `board_id` (the scope key chat is listed by) and `workspace_id` (kept so the existing
/// workspace→group snapshot scoping and group gossip resolution are unchanged).
/// CHAT C1 (additive): `anchor_kind`/`anchor_id` persist the message's step/board anchor;
/// `None` (every pre-C1 row and caller) means the board's general slot.
#[allow(clippy::too_many_arguments)]
pub fn chat_insert(id: &str, board_id: &str, workspace_id: &str, message: &str, author: &str, parent_id: Option<&str>, timestamp: i64, anchor_kind: Option<&str>, anchor_id: Option<&str>) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, board_id, workspace_id, type, name, hash, data, created_at, anchor_kind, anchor_id) VALUES (?1, ?2, ?3, 'chat', ?4, ?5, ?6, ?7, ?8, ?9)",
        params![id, board_id, workspace_id, message, author, parent_id.map(|s| s.as_bytes()), timestamp, anchor_kind, anchor_id],
    )?;
    Ok(())
}

pub fn chat_delete(id: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("DELETE FROM objects WHERE id=?1 AND type='chat'", params![id])?;
    Ok(())
}

pub fn chat_get_workspace_id(chat_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT workspace_id FROM objects WHERE id=?1 AND type='chat' LIMIT 1").ok()?;
    stmt.query_row(params![chat_id], |r| r.get(0)).optional().ok()?
}

/// Map a `ChatDTO` out of an `objects` chat row selected as
/// `(id, board_id, workspace_id, name, hash, data, created_at, anchor_kind, anchor_id)`.
fn chat_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ChatDTO> {
    let parent_bytes: Option<Vec<u8>> = r.get(5)?;
    let parent_id = parent_bytes.and_then(|b| String::from_utf8(b).ok());
    Ok(ChatDTO {
        id: r.get(0)?,
        board_id: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
        workspace_id: r.get(2)?,
        message: r.get(3)?,
        author: r.get(4)?,
        parent_id,
        timestamp: r.get(6)?,
        anchor_kind: r.get(7)?,
        anchor_id: r.get(8)?,
    })
}

/// List chats by workspace IDs (for snapshot). Each `ChatDTO` carries its `board_id` so the
/// receiver re-keys it to the right board (chat is board-scoped, R11 §1).
pub fn chat_list_by_workspaces(workspace_ids: &[String]) -> Result<Vec<ChatDTO>> {
    if workspace_ids.is_empty() { return Ok(vec![]); }

    let conn = db().lock_safe();
    let placeholders: String = workspace_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, board_id, workspace_id, name, hash, data, created_at, anchor_kind, anchor_id
         FROM objects WHERE type = 'chat' AND workspace_id IN ({}) ORDER BY created_at",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> = workspace_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let rows = stmt.query_map(params.as_slice(), chat_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Get chats for a single **board** (R11 §1 — chat is board-scoped). This is the read the
/// chat panel opens with; two boards in one workspace no longer share a thread.
pub fn chat_list_by_board(board_id: &str) -> Result<Vec<ChatDTO>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare(
        "SELECT id, board_id, workspace_id, name, hash, data, created_at, anchor_kind, anchor_id
         FROM objects WHERE type = 'chat' AND board_id = ?1 ORDER BY created_at"
    )?;
    let rows = stmt.query_map(params![board_id], chat_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Get chats for a single workspace (legacy/snapshot helper; chat reads now go through
/// [`chat_list_by_board`]). Retained for the workspace→group snapshot scoping.
pub fn chat_list_by_workspace(workspace_id: &str) -> Result<Vec<ChatDTO>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare(
        "SELECT id, board_id, workspace_id, name, hash, data, created_at, anchor_kind, anchor_id
         FROM objects WHERE type = 'chat' AND workspace_id = ?1 ORDER BY created_at"
    )?;
    let rows = stmt.query_map(params![workspace_id], chat_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// R11 §1 migration — assign every legacy (workspace-scoped) chat row a `board_id`. Chat used
/// to be keyed only by `workspace_id`, so all boards in a workspace shared one thread. Each
/// legacy chat is assigned the workspace's **deterministic default board** (its earliest board
/// by `created_at, id`). A chat whose workspace has NO board is kept on a deterministic default
/// board id equal to its `workspace_id` (so it is never dropped and the assignment is stable).
/// Idempotent: only rows with a missing `board_id` are touched. Returns the rows migrated.
pub fn migrate_chats_to_boards() -> Result<usize> {
    let conn = db().lock_safe();
    migrate_chats_to_boards_conn(&conn)
}

/// The chat re-key migration on a given connection — usable both from [`run_migrations`]
/// (which runs before the global DB handle is set) and the public [`migrate_chats_to_boards`].
fn migrate_chats_to_boards_conn(conn: &Connection) -> Result<usize> {
    // First: the workspace's earliest board (the deterministic default thread).
    let by_board = conn.execute(
        "UPDATE objects SET board_id = (
             SELECT b.id FROM objects b
             WHERE b.type = 'whiteboard' AND b.workspace_id = objects.workspace_id
             ORDER BY b.created_at, b.id LIMIT 1
         )
         WHERE type = 'chat' AND (board_id IS NULL OR board_id = '')
           AND workspace_id IS NOT NULL",
        [],
    )?;
    // Fallback: workspaces with no board at all — keep the chat on a stable default board
    // id (the workspace id itself), noted so it is never lost.
    let by_ws = conn.execute(
        "UPDATE objects SET board_id = workspace_id
         WHERE type = 'chat' AND (board_id IS NULL OR board_id = '')
           AND workspace_id IS NOT NULL",
        [],
    )?;
    Ok(by_board + by_ws)
}

// ═══════════════════════════════════════════════════════════════════════════
// NOTES (ROUND8 §W2 — board-level, authored, LWW ledger; own store, not cells)
// ═══════════════════════════════════════════════════════════════════════════

/// Idempotent **upsert-by-id** with **LWW on `updated_at`**: insert a new note, or, on
/// an existing id, replace its mutable fields ONLY IF the incoming `updated_at` is
/// strictly newer. Older OR equal writes are no-ops (so snapshot apply / anti-entropy
/// repair re-apply the same state without churn). `created_at` is preserved across
/// edits. Returns `true` iff a row was inserted or updated (i.e. state changed).
pub fn note_upsert(n: &NoteDTO) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let changed = conn.execute(
        "INSERT INTO notes (id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at, scope, kind, anchor_kind, anchor_id, origin_ref)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(id) DO UPDATE SET
            board_id    = excluded.board_id,
            tenant_id   = excluded.tenant_id,
            author_id   = excluded.author_id,
            author_name = excluded.author_name,
            text        = excluded.text,
            updated_at  = excluded.updated_at,
            scope       = excluded.scope,
            kind        = excluded.kind,
            anchor_kind = excluded.anchor_kind,
            anchor_id   = excluded.anchor_id,
            origin_ref  = excluded.origin_ref
         WHERE excluded.updated_at > notes.updated_at",
        params![
            n.id, n.board_id, n.tenant_id, n.author_id, n.author_name, n.text,
            n.created_at, n.updated_at, n.scope, n.kind,
            n.anchor_kind, n.anchor_id, n.origin_ref
        ],
    )?;
    Ok(changed > 0)
}

/// List a board's notes, **tenant-scoped** — a note never crosses the tenant boundary
/// even when the board id is known. Ordered by creation time.
pub fn note_list_by_board(board_id: &str, tenant_id: &str) -> Result<Vec<NoteDTO>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let mut stmt = conn.prepare(
        "SELECT id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at, scope, kind, anchor_kind, anchor_id, origin_ref
         FROM notes WHERE board_id = ?1 AND tenant_id = ?2 ORDER BY created_at",
    )?;
    let rows = stmt.query_map(params![board_id, tenant_id], note_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// List notes by (tenant, scope, anchor, kind) — the merge-resolver query
/// (feat/notes-constitution). `anchor_id` is what `board_id` holds for the scope:
/// the board id (`board`), the group id (`group`), or the tenant id (`tenant`).
/// Tenant-enforced like every note query; deterministic order (created_at, id).
pub fn note_list_scoped(
    tenant_id: &str,
    scope: &str,
    anchor_id: &str,
    kind: &str,
) -> Result<Vec<NoteDTO>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    note_list_scoped_with(&conn, tenant_id, scope, anchor_id, kind)
}

/// `note_list_scoped` against an ALREADY-HELD connection — for callers running
/// inside a dispatch that owns the global DB mutex (re-locking self-deadlocks;
/// the std Mutex is not reentrant). Same pattern as `board_get_group_id_with`.
pub fn note_list_scoped_with(
    conn: &rusqlite::Connection,
    tenant_id: &str,
    scope: &str,
    anchor_id: &str,
    kind: &str,
) -> Result<Vec<NoteDTO>> {
    let mut stmt = conn.prepare(
        "SELECT id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at, scope, kind, anchor_kind, anchor_id, origin_ref
         FROM notes
         WHERE tenant_id = ?1 AND scope = ?2 AND board_id = ?3 AND kind = ?4
         ORDER BY created_at, id",
    )?;
    let rows = stmt.query_map(params![tenant_id, scope, anchor_id, kind], note_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// List all notes attached to the given boards (for the digest + snapshot serializer).
/// A group is a single tenant, so this is naturally tenant-scoped by the board set.
///
/// LENS_AI_NOTES P1 — USER SCOPE IS SOVEREIGN: `scope = 'user'` rows are excluded
/// OUTRIGHT. This is the single feed behind snapshot + anti-entropy, so filtering
/// here guarantees a user-scoped note never leaves the device on either lane, even
/// if its anchor id ever collided with a board/group id.
pub fn note_list_by_boards(board_ids: &[String]) -> Result<Vec<NoteDTO>> {
    if board_ids.is_empty() {
        return Ok(vec![]);
    }
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at, scope, kind, anchor_kind, anchor_id, origin_ref
         FROM notes WHERE board_id IN ({}) AND scope != 'user' ORDER BY created_at",
        placeholders
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> =
        board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let rows = stmt.query_map(params.as_slice(), note_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Fetch a single note by id (used to resolve its board/group for broadcast + to
/// preserve `created_at` across edits).
pub fn note_get(id: &str) -> Result<Option<NoteDTO>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let mut stmt = conn.prepare(
        "SELECT id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at, scope, kind, anchor_kind, anchor_id, origin_ref
         FROM notes WHERE id = ?1",
    )?;
    stmt.query_row(params![id], note_from_row)
        .optional()
        .map_err(Into::into)
}

/// Delete a note by id (hard delete, mirrors `chat_delete`).
pub fn note_delete(id: &str) -> Result<()> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    conn.execute("DELETE FROM notes WHERE id = ?1", params![id])?;
    Ok(())
}

fn note_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<NoteDTO> {
    Ok(NoteDTO {
        id: r.get(0)?,
        board_id: r.get(1)?,
        tenant_id: r.get(2)?,
        author_id: r.get(3)?,
        author_name: r.get(4)?,
        text: r.get(5)?,
        created_at: r.get(6)?,
        updated_at: r.get(7)?,
        scope: r.get(8)?,
        kind: r.get(9)?,
        anchor_kind: r.get(10)?,
        anchor_id: r.get(11)?,
        origin_ref: r.get(12)?,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// TEMPLATES (ROUND8 §W4 — user-saved workflow templates; seeds live in code)
// ═══════════════════════════════════════════════════════════════════════════

/// Persist a user template (idempotent upsert-by-id). Steps are stored as JSON. Only
/// `source = "user"` templates are persisted — built-in seeds are code constants
/// (see `templates::seed_templates`) and are never written to the DB.
pub fn template_insert(t: &Template) -> Result<()> {
    let steps_json = serde_json::to_string(&t.steps)?;
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    conn.execute(
        "INSERT OR REPLACE INTO templates (id, tenant_id, name, description, source, steps_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![t.id, t.tenant_id, t.name, t.description, t.source, steps_json, t.created_at],
    )?;
    Ok(())
}

/// List the user templates owned by `tenant_id` (tenant-scoped — a user template never
/// crosses the tenant boundary). Built-in seeds are merged in by `templates::list_templates`.
pub fn template_list_by_tenant(tenant_id: &str) -> Result<Vec<Template>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let mut stmt = conn.prepare(
        "SELECT id, tenant_id, name, description, source, steps_json, created_at
         FROM templates WHERE tenant_id = ?1 ORDER BY created_at",
    )?;
    let rows = stmt.query_map(params![tenant_id], template_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Fetch a single user template by id, **tenant-scoped** — returns `None` if the id is
/// unknown OR belongs to a different tenant (no cross-tenant read).
pub fn template_get(id: &str, tenant_id: &str) -> Result<Option<Template>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let mut stmt = conn.prepare(
        "SELECT id, tenant_id, name, description, source, steps_json, created_at
         FROM templates WHERE id = ?1 AND tenant_id = ?2",
    )?;
    stmt.query_row(params![id, tenant_id], template_from_row)
        .optional()
        .map_err(Into::into)
}

fn template_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Template> {
    let steps_json: String = r.get(5)?;
    let steps = serde_json::from_str(&steps_json).unwrap_or_default();
    Ok(Template {
        id: r.get(0)?,
        tenant_id: r.get(1)?,
        name: r.get(2)?,
        description: r.get(3)?,
        source: r.get(4)?,
        steps,
        created_at: r.get(6)?,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// PINS (ROUND8 §W4 — board-level pinned-workflow state; replicated, LWW)
// ═══════════════════════════════════════════════════════════════════════════

/// Idempotent **upsert-by-board_id** with **LWW on `updated_at`**: set a board's pin
/// state, or, on an existing row, replace it ONLY IF the incoming `updated_at` is
/// strictly newer. Older OR equal writes are no-ops (so snapshot apply / anti-entropy
/// repair re-apply the same state without churn). Returns `true` iff state changed.
pub fn pin_upsert(p: &PinDTO) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let changed = conn.execute(
        "INSERT INTO pins (board_id, tenant_id, pinned, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(board_id) DO UPDATE SET
            tenant_id  = excluded.tenant_id,
            pinned     = excluded.pinned,
            updated_at = excluded.updated_at
         WHERE excluded.updated_at > pins.updated_at",
        params![p.board_id, p.tenant_id, p.pinned as i32, p.updated_at],
    )?;
    Ok(changed > 0)
}

/// Fetch a single board's pin state, if any.
pub fn pin_get(board_id: &str) -> Result<Option<PinDTO>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let mut stmt = conn.prepare(
        "SELECT board_id, tenant_id, pinned, updated_at FROM pins WHERE board_id = ?1",
    )?;
    stmt.query_row(params![board_id], pin_from_row)
        .optional()
        .map_err(Into::into)
}

/// List all pin rows attached to the given boards (for the digest + snapshot serializer).
/// A group is a single tenant, so this is naturally tenant-scoped by the board set.
pub fn pin_list_by_boards(board_ids: &[String]) -> Result<Vec<PinDTO>> {
    if board_ids.is_empty() {
        return Ok(vec![]);
    }
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT board_id, tenant_id, pinned, updated_at FROM pins WHERE board_id IN ({}) ORDER BY board_id",
        placeholders
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> =
        board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let rows = stmt.query_map(params.as_slice(), pin_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn pin_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<PinDTO> {
    Ok(PinDTO {
        board_id: r.get(0)?,
        tenant_id: r.get(1)?,
        pinned: r.get::<_, i32>(2)? != 0,
        updated_at: r.get(3)?,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// WORKFLOW LIFECYCLE STATE (R12 D2/E1)
// ═══════════════════════════════════════════════════════════════════════════

/// Read a board's workflow lifecycle state. A board with no row is in the default authoring
/// state (editable, unlocked, no dashboard) — never an error.
pub fn workflow_state_get(board_id: &str) -> WorkflowStateDTO {
    let Ok(conn) = db().lock() else {
        return WorkflowStateDTO::authoring(board_id);
    };
    conn.query_row(
        "SELECT board_id, deployed, dashboard_available, locked, updated_at
         FROM board_workflow_state WHERE board_id = ?1",
        params![board_id],
        |r| {
            Ok(WorkflowStateDTO {
                board_id: r.get(0)?,
                deployed: r.get::<_, i32>(1)? != 0,
                dashboard_available: r.get::<_, i32>(2)? != 0,
                locked: r.get::<_, i32>(3)? != 0,
                updated_at: r.get(4)?,
            })
        },
    )
    .optional()
    .ok()
    .flatten()
    .unwrap_or_else(|| WorkflowStateDTO::authoring(board_id))
}

/// Mark a workflow DEPLOYED (D2/E1): it is now running, optionally with a live dashboard, and
/// is LOCKED for editing. Idempotent upsert keyed by board_id; LWW on `updated_at` so a stale
/// write never reverts a newer state.
pub fn workflow_state_set_deployed(
    board_id: &str,
    dashboard_available: bool,
    updated_at: i64,
) -> Result<()> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    conn.execute(
        "INSERT INTO board_workflow_state (board_id, deployed, dashboard_available, locked, updated_at)
         VALUES (?1, 1, ?2, 1, ?3)
         ON CONFLICT(board_id) DO UPDATE SET
            deployed            = 1,
            dashboard_available = excluded.dashboard_available,
            locked              = 1,
            updated_at          = excluded.updated_at
         WHERE excluded.updated_at >= board_workflow_state.updated_at",
        params![board_id, dashboard_available as i32, updated_at],
    )?;
    Ok(())
}

/// Delete `board_workflow_state` rows whose board no longer exists (orphans left by a
/// truncate-then-seed that doesn't cascade this table). Keeps the deploy-state table in
/// lockstep with the boards — no stale rows after a re-seed.
pub fn workflow_state_prune_orphans() -> Result<usize> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let n = conn.execute(
        "DELETE FROM board_workflow_state WHERE board_id NOT IN \
         (SELECT id FROM objects WHERE type='whiteboard')",
        [],
    )?;
    Ok(n)
}

/// Set just the `locked` lane (LWW on `updated_at`). Unlocking is gated upstream by an org
/// grant (see `workflow::request_unlock`); this is the storage primitive it calls on success.
pub fn workflow_state_set_locked(board_id: &str, locked: bool, updated_at: i64) -> Result<()> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    conn.execute(
        "INSERT INTO board_workflow_state (board_id, deployed, dashboard_available, locked, updated_at)
         VALUES (?1, 0, 0, ?2, ?3)
         ON CONFLICT(board_id) DO UPDATE SET
            locked     = excluded.locked,
            updated_at = excluded.updated_at
         WHERE excluded.updated_at >= board_workflow_state.updated_at",
        params![board_id, locked as i32, updated_at],
    )?;
    Ok(())
}

/// List the workflow-lifecycle rows for `board_ids` (only boards that actually have a row —
/// a default-authoring board contributes nothing, so two peers with no deployments produce
/// identical lists). The version column the anti-entropy digest + snapshot use is `updated_at`.
/// Used by the digest (detect a missed deploy/lock) and the snapshot (carry it for repair).
pub fn workflow_state_list_by_boards(board_ids: &[String]) -> Result<Vec<WorkflowStateDTO>> {
    if board_ids.is_empty() {
        return Ok(Vec::new());
    }
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let placeholders = vec!["?"; board_ids.len()].join(",");
    let mut stmt = conn.prepare(&format!(
        "SELECT board_id, deployed, dashboard_available, locked, updated_at
         FROM board_workflow_state WHERE board_id IN ({placeholders})"
    ))?;
    let params = rusqlite::params_from_iter(board_ids.iter());
    let rows = stmt
        .query_map(params, |r| {
            Ok(WorkflowStateDTO {
                board_id: r.get(0)?,
                deployed: r.get::<_, i32>(1)? != 0,
                dashboard_available: r.get::<_, i32>(2)? != 0,
                locked: r.get::<_, i32>(3)? != 0,
                updated_at: r.get(4)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Full-record **upsert-by-board_id** with **LWW on `updated_at`**: apply a whole workflow-state
/// row (the shape the snapshot/anti-entropy repair carries), replacing the local row ONLY IF the
/// incoming `updated_at` is **strictly newer**. Equal/older writes are no-ops, so re-applying the
/// same state (a replayed snapshot frame, a debounced repair pull) never churns and a stale clock
/// never clobbers a newer local deploy/lock — the same LWW guard as `pin_upsert`/`board_metadata_upsert`.
pub fn workflow_state_upsert(s: &WorkflowStateDTO) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let changed = conn.execute(
        "INSERT INTO board_workflow_state (board_id, deployed, dashboard_available, locked, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(board_id) DO UPDATE SET
            deployed            = excluded.deployed,
            dashboard_available = excluded.dashboard_available,
            locked              = excluded.locked,
            updated_at          = excluded.updated_at
         WHERE excluded.updated_at > board_workflow_state.updated_at",
        params![
            s.board_id,
            s.deployed as i32,
            s.dashboard_available as i32,
            s.locked as i32,
            s.updated_at
        ],
    )?;
    Ok(changed > 0)
}

// ═══════════════════════════════════════════════════════════════════════════
// LEDGER SYNC (CYAN_FORMAT_SPEC §6) — process-global wrappers over the explicit-
// connection `changelist::` / `review_state::` fns, so the gossip apply path
// (`topic_actor::persist_event`), the snapshot build/apply, and the anti-entropy
// digest all read/write the ledger the same way they do notes/pins. Tenant == the
// group id.
// ═══════════════════════════════════════════════════════════════════════════

/// Apply an inbound `ChangeEntryAppended` (or one snapshot entry row) — union by
/// `entry_hash` + lifecycle LWW, via `changelist::apply_entry`.
pub fn change_entry_apply(e: &crate::changelist::ChangeEntry) -> Result<()> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::apply_entry(&conn, e).map(|_| ())
}

/// Apply an inbound `ChangeEntryLifecycle` — audit unions, lifecycle LWW.
pub fn change_lifecycle_apply(tenant_id: &str, d: &crate::changelist::LifecycleDelta) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::apply_lifecycle(&conn, tenant_id, d)
}

/// Apply an inbound `ChangeVersionCreated` (or one snapshot version row) —
/// immutable union by `version_id`.
pub fn change_version_apply(v: &crate::changelist::ChangeVersion) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::apply_version(&conn, v)
}

/// Apply an inbound `ChangeBranchHead` (or one snapshot branch row) — LWW upsert.
pub fn change_branch_head_apply(
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    head_version: Option<&str>,
    updated_at: i64,
) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::apply_branch_head(&conn, tenant_id, asset_hash, branch, head_version, updated_at)
}

/// Apply one snapshot audit row — union by `audit_hash`.
pub fn change_audit_apply(a: &crate::changelist::ChangeAudit) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::apply_audit(&conn, a)
}

/// Every ledger entry the tenant (group) holds — the snapshot/digest read.
pub fn change_entry_list_by_tenant(tenant_id: &str) -> Result<Vec<crate::changelist::ChangeEntry>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::list_entries_by_tenant(&conn, tenant_id)
}

/// Every version the tenant holds.
pub fn change_version_list_by_tenant(tenant_id: &str) -> Result<Vec<crate::changelist::ChangeVersion>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::list_versions_by_tenant(&conn, tenant_id)
}

/// Every branch head-pointer the tenant holds.
pub fn change_branch_list_by_tenant(tenant_id: &str) -> Result<Vec<crate::changelist::ChangeBranch>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::list_branches_by_tenant(&conn, tenant_id)
}

/// Every audit row the tenant holds.
pub fn change_audit_list_by_tenant(tenant_id: &str) -> Result<Vec<crate::changelist::ChangeAudit>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::changelist::list_audits_by_tenant(&conn, tenant_id)
}

/// Every review-loop state row the tenant holds (the `rs` lane).
pub fn review_state_list_by_tenant(tenant_id: &str) -> Result<Vec<crate::review_state::ReviewState>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::review_state::list_by_tenant(&conn, tenant_id)
        .map_err(|e| anyhow::anyhow!("review_state list: {}", e))
}

/// Apply one snapshot review-state row — LWW on `updated_at`.
pub fn review_state_apply(rs: &crate::review_state::ReviewState) -> Result<bool> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    crate::review_state::apply_remote(&conn, rs)
        .map_err(|e| anyhow::anyhow!("review_state apply: {}", e))
}

// ═══════════════════════════════════════════════════════════════════════════
// FILES
// ═══════════════════════════════════════════════════════════════════════════

/// Idempotent insert of an inbound file delta.
///
/// R12 B3: applying the same file twice must collapse to ONE row. Two layers of idempotency:
///   * `INSERT OR IGNORE` on the `id` primary key — the SAME delta replayed is a no-op (snapshot
///     convergence relies on the id being preserved verbatim);
///   * a content-addressed `WHERE NOT EXISTS` guard on `(board scope, hash)` — the SAME content
///     re-announced under a DIFFERENT id (a file followed by a message re-shared the file →
///     rendered twice on the receiver) collapses to the row already present in that board scope.
///
/// Soft-deleted (tombstoned) rows don't suppress a re-share, so a delete→re-add still lands.
pub fn file_insert(
    id: &str, group_id: Option<&str>, workspace_id: Option<&str>, board_id: Option<&str>,
    name: &str, hash: &str, size: u64, source_peer: &str, created_at: i64,
) -> Result<()> {
    let conn = db().lock_safe();
    file_insert_conn(&conn, id, group_id, workspace_id, board_id, name, hash, size, source_peer, None, created_at)?;
    Ok(())
}

/// `file_insert` against an ALREADY-HELD connection (the `board_get_group_id_with`
/// pattern) — same two idempotency layers — plus an optional `local_path` stamped
/// at insert time (the ingest leg knows the file's real on-disk path up front).
/// Returns whether a row was actually inserted (`false` = the content-dedup guard
/// collapsed it onto an existing row in that board scope).
#[allow(clippy::too_many_arguments)]
pub fn file_insert_conn(
    conn: &Connection,
    id: &str, group_id: Option<&str>, workspace_id: Option<&str>, board_id: Option<&str>,
    name: &str, hash: &str, size: u64, source_peer: &str, local_path: Option<&str>,
    created_at: i64,
) -> Result<bool> {
    let n = conn.execute(
        "INSERT OR IGNORE INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, local_path, created_at)
         SELECT ?1, ?2, ?3, ?4, 'file', ?5, ?6, ?7, ?8, ?9, ?10
         WHERE NOT EXISTS (
             SELECT 1 FROM objects
             WHERE type = 'file' AND hash = ?6 AND COALESCE(deleted, 0) = 0
               AND COALESCE(board_id, '') = COALESCE(?4, '')
         )",
        params![id, group_id, workspace_id, board_id, name, hash, size as i64, source_peer, local_path, created_at],
    )?;
    Ok(n > 0)
}

pub fn file_set_local_path(id: &str, local_path: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("UPDATE objects SET local_path=?1 WHERE id=?2 AND type='file'", params![local_path, id])?;
    Ok(())
}

pub fn file_get_for_transfer(id: &str, hash: &str) -> Option<(String, String, u64)> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT name, local_path, size FROM objects WHERE id=?1 AND type='file' AND hash=?2",
        params![id, hash],
        |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? as u64)),
    ).optional().ok()?
}

pub fn file_get_local_path(id: &str) -> Option<String> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT local_path FROM objects WHERE id=?1 AND type='file'").ok()?;
    stmt.query_row(params![id], |r| r.get(0)).optional().ok()?
}

/// Get the group_id for a file (for routing file downloads to correct TopicActor)
pub fn file_get_group_id(file_id: &str) -> Option<String> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT group_id FROM objects WHERE id = ?1 AND type = 'file'",
        params![file_id],
        |r| r.get(0),
    ).optional().ok()?
}

/// List files by group (for snapshot). Tombstoned (soft-deleted) files are excluded.
pub fn file_list_by_group(group_id: &str) -> Result<Vec<FileDTO>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare(
        "SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at
         FROM objects WHERE type = 'file' AND group_id = ?1 AND COALESCE(deleted, 0) = 0 ORDER BY name"
    )?;

    let rows = stmt.query_map(params![group_id], |r| {
        Ok(FileDTO {
            id: r.get(0)?,
            group_id: r.get(1)?,
            workspace_id: r.get(2)?,
            board_id: r.get(3)?,
            name: r.get(4)?,
            hash: r.get(5)?,
            size: r.get::<_, i64>(6)? as u64,
            source_peer: r.get(7)?,
            local_path: r.get(8)?,
            created_at: r.get(9)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// List the active (non-tombstoned) files scoped to a board (R10FB §F1 — files persist
/// at board level). The board dimension is `objects.board_id`.
pub fn file_list_by_board(board_id: &str) -> Result<Vec<FileDTO>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare(
        "SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at
         FROM objects WHERE type = 'file' AND board_id = ?1 AND COALESCE(deleted, 0) = 0 ORDER BY name",
    )?;
    let rows = stmt.query_map(params![board_id], |r| {
        Ok(FileDTO {
            id: r.get(0)?,
            group_id: r.get(1)?,
            workspace_id: r.get(2)?,
            board_id: r.get(3)?,
            name: r.get(4)?,
            hash: r.get(5)?,
            size: r.get::<_, i64>(6)? as u64,
            source_peer: r.get(7)?,
            local_path: r.get(8)?,
            created_at: r.get(9)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Resolve a file by its stable workflow handle `group_id:workspace_id:board_id:file_name`
/// (R10FB §F3). Returns the active (non-tombstoned) board-scoped file with that name, or
/// `None`. Names are unique per level (see `file_insert_dedup`), so this is unambiguous.
pub fn file_resolve_handle(
    group_id: &str,
    workspace_id: &str,
    board_id: &str,
    file_name: &str,
) -> Option<FileDTO> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at
         FROM objects
         WHERE type = 'file' AND COALESCE(deleted, 0) = 0
           AND group_id = ?1 AND workspace_id = ?2 AND board_id = ?3 AND name = ?4
         LIMIT 1",
        params![group_id, workspace_id, board_id, file_name],
        |r| {
            Ok(FileDTO {
                id: r.get(0)?,
                group_id: r.get(1)?,
                workspace_id: r.get(2)?,
                board_id: r.get(3)?,
                name: r.get(4)?,
                hash: r.get(5)?,
                size: r.get::<_, i64>(6)? as u64,
                source_peer: r.get(7)?,
                local_path: r.get(8)?,
                created_at: r.get(9)?,
            })
        },
    )
    .optional()
    .ok()?
}

/// Soft-delete (tombstone) a file (R10FB §F4). The engine never hard-deletes a file; the
/// tombstone syncs to peers via `NetworkEvent::FileDeleted`. Idempotent.
pub fn file_soft_delete(id: &str, deleted_at: i64) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "UPDATE objects SET deleted = 1 WHERE id = ?1 AND type = 'file'",
        params![id],
    )?;
    // The deletion order is carried by the FileDeleted event; `deleted_at` is accepted for
    // a stable signature and future durable-tombstone use.
    let _ = deleted_at;
    Ok(())
}

/// Whether a file is tombstoned. `None` if the file id is unknown.
pub fn file_is_deleted(id: &str) -> Option<bool> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT COALESCE(deleted, 0) FROM objects WHERE id = ?1 AND type = 'file'",
        params![id],
        |r| Ok(r.get::<_, i64>(0)? != 0),
    )
    .optional()
    .ok()?
}

/// Insert a user-shared file enforcing **unique names per level + dedupe** (R10FB §F2).
///
/// The "level" is the most specific scope present (board → workspace → group). Among the
/// active files at that level:
/// - same name **and** same content (`hash`) ⇒ **dedupe**: no new row, returns the
///   existing `(id, name)` (idempotent re-share).
/// - same name, different content ⇒ **rename**: the new file gets `name (2)`, `name (3)`…
///   so names stay unique within the level.
///
/// Returns the `(id, name)` actually used. The plain `file_insert*` paths (snapshot/sync)
/// are unchanged — they preserve ids verbatim so replicas converge.
pub fn file_insert_dedup(
    id: &str,
    group_id: Option<&str>,
    workspace_id: Option<&str>,
    board_id: Option<&str>,
    name: &str,
    hash: &str,
    size: u64,
    source_peer: &str,
    created_at: i64,
) -> Result<(String, String)> {
    let conn = db().lock_safe();
    let existing = files_at_level(&conn, group_id, workspace_id, board_id)?;

    // Dedupe: identical content already shared at this level → reuse it.
    if let Some((eid, ename, _)) = existing.iter().find(|(_, n, h)| n == name && h == hash) {
        return Ok((eid.clone(), ename.clone()));
    }

    // Otherwise pick a name unique within the level.
    let taken: HashSet<&str> = existing.iter().map(|(_, n, _)| n.as_str()).collect();
    let unique_name = unique_file_name(name, &taken);

    conn.execute(
        "INSERT OR IGNORE INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, created_at, deleted)
         VALUES (?1, ?2, ?3, ?4, 'file', ?5, ?6, ?7, ?8, ?9, 0)",
        params![id, group_id, workspace_id, board_id, unique_name, hash, size as i64, source_peer, created_at],
    )?;
    Ok((id.to_string(), unique_name))
}

/// Active (non-tombstoned) files at the most-specific scope level of (group, ws, board).
fn files_at_level(
    conn: &Connection,
    group_id: Option<&str>,
    workspace_id: Option<&str>,
    board_id: Option<&str>,
) -> Result<Vec<(String, String, String)>> {
    let (sql, key): (&str, &str) = if let Some(b) = board_id {
        (
            "SELECT id, name, hash FROM objects WHERE type='file' AND COALESCE(deleted,0)=0 AND board_id = ?1",
            b,
        )
    } else if let Some(w) = workspace_id {
        (
            "SELECT id, name, hash FROM objects WHERE type='file' AND COALESCE(deleted,0)=0 AND workspace_id = ?1 AND board_id IS NULL",
            w,
        )
    } else if let Some(g) = group_id {
        (
            "SELECT id, name, hash FROM objects WHERE type='file' AND COALESCE(deleted,0)=0 AND group_id = ?1 AND workspace_id IS NULL AND board_id IS NULL",
            g,
        )
    } else {
        return Ok(Vec::new());
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Produce a name not already in `taken`, appending ` (n)` before the extension.
fn unique_file_name(name: &str, taken: &HashSet<&str>) -> String {
    if !taken.contains(name) {
        return name.to_string();
    }
    let (stem, ext) = match name.rfind('.') {
        Some(i) if i > 0 => (&name[..i], &name[i..]),
        _ => (name, ""),
    };
    let mut n = 2;
    loop {
        let candidate = format!("{stem} ({n}){ext}");
        if !taken.contains(candidate.as_str()) {
            return candidate;
        }
        n += 1;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// BOARD PINNED (R10FB §B3 — synced board property)
// ═══════════════════════════════════════════════════════════════════════════

/// Set a board's pinned flag (`board_metadata.is_pinned`) as a **per-board convergent LWW
/// flag** (R11 §9b). Upserts only the pin lane, applying the new value only when
/// `updated_at` is strictly newer — so pins from multiple peers MERGE and a stale `BoardPinned`
/// (or snapshot row) never clobbers a newer pin. The descriptive fields are untouched.
pub fn board_meta_set_pinned(board_id: &str, is_pinned: bool, updated_at: i64) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT INTO board_metadata (board_id, is_pinned, pin_updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(board_id) DO UPDATE SET
            is_pinned      = CASE WHEN excluded.pin_updated_at > board_metadata.pin_updated_at THEN excluded.is_pinned ELSE board_metadata.is_pinned END,
            pin_updated_at = MAX(excluded.pin_updated_at, board_metadata.pin_updated_at)",
        params![board_id, is_pinned as i32, updated_at],
    )?;
    Ok(())
}

/// Build a board's **preview** for the live board-changed signal (R11 §9): its display name
/// plus a short content snippet (latest notebook cell text, then latest note text), truncated.
/// Returns `(name, preview)`; either may be empty for an empty/unknown board.
pub fn board_preview(board_id: &str) -> (String, String) {
    let conn = db().lock_safe();
    let name: String = conn
        .query_row(
            "SELECT name FROM objects WHERE id = ?1 AND type = 'whiteboard' LIMIT 1",
            params![board_id],
            |r| r.get(0),
        )
        .optional()
        .ok()
        .flatten()
        .unwrap_or_default();

    // Latest non-empty content snippet: prefer a notebook cell, else a note.
    let snippet: Option<String> = conn
        .query_row(
            "SELECT content FROM notebook_cells
             WHERE board_id = ?1 AND content IS NOT NULL AND content <> ''
             ORDER BY updated_at DESC LIMIT 1",
            params![board_id],
            |r| r.get(0),
        )
        .optional()
        .ok()
        .flatten()
        .or_else(|| {
            conn.query_row(
                "SELECT text FROM notes
                 WHERE board_id = ?1 AND text IS NOT NULL AND text <> ''
                 ORDER BY updated_at DESC LIMIT 1",
                params![board_id],
                |r| r.get(0),
            )
            .optional()
            .ok()
            .flatten()
        });

    let preview = match snippet {
        Some(s) => {
            let trimmed = s.trim();
            trimmed.chars().take(140).collect::<String>()
        }
        None => name.clone(),
    };
    (name, preview)
}

/// Whether a board is pinned.
pub fn board_is_pinned(board_id: &str) -> bool {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT COALESCE(is_pinned, 0) FROM board_metadata WHERE board_id = ?1",
        params![board_id],
        |r| Ok(r.get::<_, i64>(0)? != 0),
    )
    .optional()
    .ok()
    .flatten()
    .unwrap_or(false)
}

// ═══════════════════════════════════════════════════════════════════════════
// UNREAD (R10FB §N — per-reader unread, idempotent by message_id)
// ═══════════════════════════════════════════════════════════════════════════

/// Record a message as unread for this reader, **board-scoped** (R11 §2/§3). Idempotent by
/// `message_id`: a message counts once, ever — re-delivery (gossip echo, re-sync) never
/// re-increments, and opening a chat (a read) never calls this. Returns `true` iff this is
/// the first time the message is recorded (so the caller emits `UnreadChanged` only on a real
/// change). The dot/count lives on the **board** only — there is no workspace/group rollup
/// (dropping it killed the doubled `1→2 / 2→4` counts where one message rolled up to several
/// scopes). `kind` is the notification-type seam ('chat' now; nudges/asks/decisions later).
pub fn unread_record(
    message_id: &str,
    kind: &str,
    board_id: &str,
    created_at: i64,
) -> Result<bool> {
    let conn = db().lock_safe();
    let changed = conn.execute(
        "INSERT OR IGNORE INTO unread (message_id, kind, board_id, read, created_at)
         VALUES (?1, ?2, ?3, 0, ?4)",
        params![message_id, kind, board_id, created_at],
    )?;
    Ok(changed > 0)
}

/// The live `{board_id: count}` map of unread counts (R11 §3 — **board-level only**). Each
/// open (unread) item contributes +1 to its board id; the dock badge total is the sum of the
/// map. No workspace/group rollup — that doubling is the bug this kills.
pub fn unread_counts() -> Result<std::collections::HashMap<String, i64>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare(
        "SELECT board_id, COUNT(*) FROM unread
         WHERE read = 0 AND board_id IS NOT NULL AND board_id <> ''
         GROUP BY board_id",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;
    let mut counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for row in rows {
        let (b, c) = row?;
        counts.insert(b, c);
    }
    Ok(counts)
}

/// Mark a **board's** unread items read (R11 §3/§5) — `board_id` is a board scope. Opening the
/// board's chat clears its dot/count. Returns the number of items cleared. Read state is sticky
/// (the row stays, so the message stays counted-once and never re-increments on re-delivery).
pub fn unread_mark_read(board_id: &str) -> Result<usize> {
    let conn = db().lock_safe();
    let n = conn.execute(
        "UPDATE unread SET read = 1 WHERE read = 0 AND board_id = ?1",
        params![board_id],
    )?;
    Ok(n)
}

/// One installed plugin bundle file discovered in a group's Plugins workspace.
#[derive(Debug, Clone, PartialEq)]
pub struct PluginBundleFile {
    /// The object (file) id in the file scope.
    pub file_id: String,
    /// The bundle file name (e.g. `slack.cyanplugin`).
    pub name: String,
    /// On-disk path of the fetched bundle (file-swarm sets this on download).
    pub local_path: String,
}

/// List the downloaded plugin bundle files in a group's "Plugins" workspace.
///
/// This is the "registry = files" pickup path: a `<suffix>` bundle landing in the
/// workspace named `workspace_name`, once the file-swarm has fetched it (so
/// `local_path` is set), is an installed plugin the local MCP host should run.
/// It reuses the existing files/objects scope — no new tables and no new FFI; the
/// app just sees a file appear in a workspace.
pub fn plugin_bundles_in_group(
    group_id: &str,
    workspace_name: &str,
    suffix: &str,
) -> Result<Vec<PluginBundleFile>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare(
        "SELECT o.id, o.name, o.local_path
         FROM objects o
         JOIN workspaces w ON o.workspace_id = w.id
         WHERE o.type = 'file'
           AND w.group_id = ?1
           AND w.name = ?2
           AND o.local_path IS NOT NULL
           AND o.name LIKE '%' || ?3
         ORDER BY o.name",
    )?;
    let rows = stmt.query_map(params![group_id, workspace_name, suffix], |r| {
        Ok(PluginBundleFile {
            file_id: r.get(0)?,
            name: r.get(1)?,
            local_path: r.get(2)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Where installed `.cyanplugin` bundle files are written on disk. Mirrors the
/// pipeline executor's `plugins_root` (env `CYAN_PLUGINS_ROOT`, else
/// `$HOME/.cyan/plugins`) so a bundle written here is the same file the on-device
/// MCP host later reads. Kept in storage so the install receiver and the reader
/// agree on one path.
pub fn plugin_bundles_dir() -> PathBuf {
    if let Ok(root) = std::env::var("CYAN_PLUGINS_ROOT") {
        return PathBuf::from(root);
    }
    let home = std::env::var("HOME").unwrap_or_default();
    PathBuf::from(home).join(".cyan").join("plugins")
}

/// Marker file stamped inside an unpacked bundle dir: the blake3 of the EXACT
/// `.cyanplugin` bytes it was extracted from. Content-addressed freshness — the
/// only trustworthy oracle here, because tar RESTORES the archive's stored (old)
/// mtimes on extraction, so the installed bundle file is "newer" than its own
/// unpack forever and an mtime comparison re-extracts on every call.
const BUNDLE_HASH_MARKER: &str = ".cyan_bundle_hash";

/// Serializes unpack work process-wide. `ensure_bundle_unpacked` is called from
/// every `@` autocomplete keystroke, every Review-time bind, and every spawn —
/// concurrently. Unserialized, two tar extractions into the same dir collide
/// ("Can't create …: File exists") and a reader sees a HALF-EXTRACTED manifest
/// (found live 2026-07-07: installed plugins intermittently failed to bind).
static UNPACK_LOCK: Mutex<()> = Mutex::new(());

/// Ensure the installed bundle for `plugin_id` is UNPACKED at
/// `plugin_bundles_dir()/<plugin_id>/` so the on-device MCP host (registry index,
/// tool autocomplete, rung-1 binding, spawn) can read its manifest. Best-effort
/// by design: a bundle that fails to unpack/parse degrades to "not locally
/// dispatchable" (the step stays on the lens path) — it must never fail the
/// install that recorded the file row.
///
/// Hardened (Tier 0, 2026-07-07):
///   * SHORT-CIRCUIT is content-addressed: the unpack stands iff its
///     `.cyan_bundle_hash` marker equals the bundle's current blake3 AND the
///     manifest parses (a corrupt/partial unpack self-heals on the next call).
///   * REFRESH is atomic: extract into a temp sibling, verify the manifest,
///     stamp the marker, then swap into place — a concurrent reader never sees
///     a half-written manifest, and spawn debris in the old dir (`.venv`) can
///     never fail the extraction (tar-over-existing-dir did, found live).
///   * All unpack work is serialized process-wide (`UNPACK_LOCK`).
pub fn ensure_bundle_unpacked(plugin_id: &str) -> Option<PathBuf> {
    let root = plugin_bundles_dir();
    let dest = root.join(plugin_id);
    let bundle = root.join(format!("{plugin_id}{}", crate::mcp_host::PLUGIN_BUNDLE_SUFFIX));

    let _guard = UNPACK_LOCK.lock_safe();

    // No bundle file: a pre-placed dir with a valid manifest is still usable
    // (dev drop-ins); otherwise there is nothing to unpack.
    if !bundle.is_file() {
        return cyan_mcp::Manifest::from_bundle(&dest).ok().map(|_| dest);
    }
    let bundle_hash = match std::fs::read(&bundle) {
        Ok(bytes) => blake3::hash(&bytes).to_hex().to_string(),
        Err(e) => {
            tracing::warn!("ensure_bundle_unpacked({plugin_id}): read bundle: {e}");
            return None;
        }
    };

    // Fast path: same bytes already unpacked AND readable — nothing to do.
    // (`.venv` and other spawn debris in the dir is expected and preserved.)
    let marker = dest.join(BUNDLE_HASH_MARKER);
    if std::fs::read_to_string(&marker).is_ok_and(|h| h.trim() == bundle_hash)
        && cyan_mcp::Manifest::from_bundle(&dest).is_ok()
    {
        return Some(dest);
    }

    // Refresh: extract into a TEMP sibling under the same root (same volume ⇒
    // rename is atomic), verify, stamp, swap. bsdtar without -P already refuses
    // absolute and `..`-traversing member paths, so extraction cannot escape
    // the plugins root. The served bundle is a POSIX tar whose single top-level
    // dir is the plugin id (verified against the live forge artifact).
    let staging = root.join(format!(".unpack-{plugin_id}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&staging);
    if let Err(e) = std::fs::create_dir_all(&staging) {
        tracing::warn!("ensure_bundle_unpacked({plugin_id}): staging dir: {e}");
        return None;
    }
    let status = std::process::Command::new("/usr/bin/tar")
        .arg("-xf")
        .arg(&bundle)
        .arg("-C")
        .arg(&staging)
        .status();
    let cleanup = |reason: &str| {
        tracing::warn!("ensure_bundle_unpacked({plugin_id}): {reason}");
        let _ = std::fs::remove_dir_all(&staging);
    };
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => {
            cleanup(&format!("tar exited {s}"));
            return None;
        }
        Err(e) => {
            cleanup(&format!("spawn tar: {e}"));
            return None;
        }
    }
    let extracted = staging.join(plugin_id);
    // Only swap in a bundle the registry can index — otherwise the previous
    // (still-valid) unpack keeps serving.
    match cyan_mcp::Manifest::from_bundle(&extracted) {
        Err(e) => {
            cleanup(&format!("manifest: {e}"));
            return cyan_mcp::Manifest::from_bundle(&dest).ok().map(|_| dest);
        }
        Ok(manifest) => {
            // INDEX-TIME CONTRACT FLAG (FABLE_FULL_AUDIT headline 2): an alias
            // carried by more than one tool is the signature of a stale or
            // mis-curated bundle — every `@plugin.<alias>` bind over it is
            // ambiguous (the binder hard-fails those at Review). Flag it loudly
            // the moment the bundle lands, so the operator learns at INSTALL
            // time, not mid-run.
            for (alias, carriers) in manifest.ambiguous_aliases() {
                tracing::warn!(
                    "plugin '{plugin_id}' manifest contract: alias '{alias}' is carried by \
                     {} tools [{}] — @{plugin_id}.{alias} cannot bind; reinstall a curated \
                     bundle (stale/mis-curated)",
                    carriers.len(),
                    carriers.join(", ")
                );
            }
        }
    }
    if let Err(e) = std::fs::write(extracted.join(BUNDLE_HASH_MARKER), &bundle_hash) {
        cleanup(&format!("write marker: {e}"));
        return None;
    }
    if dest.exists()
        && let Err(e) = std::fs::remove_dir_all(&dest)
    {
        cleanup(&format!("clear old unpack: {e}"));
        return None;
    }
    if let Err(e) = std::fs::rename(&extracted, &dest) {
        cleanup(&format!("swap unpack into place: {e}"));
        return None;
    }
    let _ = std::fs::remove_dir_all(&staging);
    Some(dest)
}

/// Install a `.cyanplugin` bundle into a group's "Plugins" workspace as a real
/// installed file — the receiver half of the Market install leg (BURST C2).
///
/// The already-decoded tar `bytes` are written to `plugin_bundles_dir()/<plugin_id>.cyanplugin`
/// and an `objects` file row is inserted into the group's system "Plugins" workspace with its
/// `local_path` set, so both `plugin_bundles_in_group` and `workflow::autocomplete_index` find
/// it immediately (they key on: type='file', that workspace, a set `local_path`, name ending
/// `.cyanplugin`).
///
/// Idempotent: the file id is deterministic (`blake3("plugin-bundle:{group}:{plugin_id}")`) so a
/// re-install REPLACES the prior row and overwrites the bytes on disk instead of duplicating.
///
/// The bundle's own XaeroID signature (over its embedded `manifest.yaml`) is a cyan-forge
/// artifact; this repo has no unpack-and-verify path for the `.cyanplugin` internal layout yet,
/// so signature verification is a documented TODO (see the FFI wrapper) — the install still
/// records the bundle so the authoring surface can reference it. Returns the file id used.
pub fn install_plugin_bundle(group_id: &str, plugin_id: &str, bytes: &[u8]) -> Result<String> {
    if group_id.trim().is_empty() {
        return Err(anyhow!("install_plugin_bundle: empty group_id"));
    }
    if plugin_id.trim().is_empty() {
        return Err(anyhow!("install_plugin_bundle: empty plugin_id"));
    }

    // The workspace/objects rows below reference the group by FK (enforced — the
    // bundled SQLite defaults foreign_keys ON). A group id with no `groups` row
    // (e.g. a stale or placeholder id from the caller) must fail as a clear
    // precondition, not SQLite's cryptic "FOREIGN KEY constraint failed".
    {
        let conn = db().lock_safe();
        let exists: Option<i64> = conn
            .query_row("SELECT 1 FROM groups WHERE id = ?1", params![group_id], |r| r.get(0))
            .optional()?;
        if exists.is_none() {
            return Err(anyhow!(
                "install_plugin_bundle: unknown group '{group_id}' — select an existing group before installing a plugin"
            ));
        }
    }

    // Ensure the group's system "Plugins" workspace exists (deterministic id, INSERT OR IGNORE).
    provision_group_workspaces(group_id, None)?;
    let plugins_ws = plugins_workspace_id(group_id);

    // Write the bundle bytes to the shared bundles dir (overwrite on re-install).
    let dir = plugin_bundles_dir();
    std::fs::create_dir_all(&dir)
        .map_err(|e| anyhow!("install_plugin_bundle: mkdir {}: {e}", dir.display()))?;
    let file_name = format!("{plugin_id}{}", crate::mcp_host::PLUGIN_BUNDLE_SUFFIX);
    let path = dir.join(&file_name);
    std::fs::write(&path, bytes)
        .map_err(|e| anyhow!("install_plugin_bundle: write {}: {e}", path.display()))?;
    let local_path = path.to_string_lossy().to_string();

    // Deterministic id ⇒ re-install replaces rather than duplicates.
    let file_id = blake3::hash(format!("plugin-bundle:{group_id}:{plugin_id}").as_bytes())
        .to_hex()
        .to_string();
    let hash = blake3::hash(bytes).to_hex().to_string();
    let now = chrono::Utc::now().timestamp();

    {
        let conn = db().lock_safe();
        conn.execute(
            "INSERT OR REPLACE INTO objects
               (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, local_path, created_at, deleted)
             VALUES (?1, ?2, ?3, NULL, 'file', ?4, ?5, ?6, 'install', ?7, ?8, 0)",
            params![
                file_id, group_id, plugins_ws, file_name, hash, bytes.len() as i64, local_path, now
            ],
        )?;
    }
    // Unpack for the on-device MCP host (registry/autocomplete/binding/spawn).
    // Best-effort: a bundle that won't unpack still installs as a file row.
    let _ = ensure_bundle_unpacked(plugin_id);
    Ok(file_id)
}

// ═══════════════════════════════════════════════════════════════════════════
// FILE TRANSFERS (resumable download state)
// ═══════════════════════════════════════════════════════════════════════════

pub fn transfer_upsert(
    file_id: &str, file_name: &str, total_size: u64, hash: &str,
    bytes_received: u64, temp_path: &str, source_peer: &str, status: &str,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR REPLACE INTO file_transfers (file_id, file_name, total_size, hash, bytes_received, temp_path, source_peer, status, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
        params![file_id, file_name, total_size as i64, hash, bytes_received as i64, temp_path, source_peer, status, now],
    )?;
    Ok(())
}

pub fn transfer_update_progress(file_id: &str, bytes_received: u64) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "UPDATE file_transfers SET bytes_received=?1, updated_at=?2 WHERE file_id=?3",
        params![bytes_received as i64, now, file_id],
    )?;
    Ok(())
}

pub fn transfer_set_status(file_id: &str, status: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "UPDATE file_transfers SET status=?1, updated_at=?2 WHERE file_id=?3",
        params![status, now, file_id],
    )?;
    Ok(())
}

/// The interrupted-transfer row for `(file_id, hash)`, if one is still resumable:
/// returns `(bytes_received, temp_path)`. Complete transfers and hash mismatches
/// don't resume; a different hash means the file changed — start fresh.
pub fn transfer_get_partial(file_id: &str, hash: &str) -> Option<(u64, String)> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT bytes_received, temp_path FROM file_transfers \
         WHERE file_id=?1 AND hash=?2 AND status IN ('pending','in_progress','failed')",
        params![file_id, hash],
        |r| Ok((r.get::<_, i64>(0)? as u64, r.get(1)?)),
    )
    .optional()
    .ok()?
}

pub fn transfer_list_pending() -> Result<Vec<(String, String, String, u64)>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare("SELECT file_id, hash, source_peer, bytes_received FROM file_transfers WHERE status IN ('pending', 'in_progress')")?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get::<_, i64>(3)? as u64))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// WHITEBOARD ELEMENTS
// ═══════════════════════════════════════════════════════════════════════════

pub fn element_insert(e: &WhiteboardElementDTO) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO whiteboard_elements (id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![e.id, e.board_id, e.element_type, e.x, e.y, e.width, e.height, e.z_index, e.style_json, e.content_json, e.created_at, e.updated_at],
    )?;
    Ok(())
}

pub fn element_update(e: &WhiteboardElementDTO) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "UPDATE whiteboard_elements SET board_id=?2, element_type=?3, x=?4, y=?5, width=?6, height=?7, z_index=?8, style_json=?9, content_json=?10, updated_at=?11 WHERE id=?1",
        params![e.id, e.board_id, e.element_type, e.x, e.y, e.width, e.height, e.z_index, e.style_json, e.content_json, e.updated_at],
    )?;
    Ok(())
}

pub fn element_delete(id: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("DELETE FROM whiteboard_elements WHERE id=?1", params![id])?;
    Ok(())
}

pub fn element_clear_board(board_id: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("DELETE FROM whiteboard_elements WHERE board_id=?1", params![board_id])?;
    Ok(())
}

/// List elements by board IDs (for snapshot)
pub fn element_list_by_boards(board_ids: &[String]) -> Result<Vec<WhiteboardElementDTO>> {
    if board_ids.is_empty() { return Ok(vec![]); }

    let conn = db().lock_safe();
    let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at
         FROM whiteboard_elements WHERE board_id IN ({}) ORDER BY z_index",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> = board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

    let rows = stmt.query_map(params.as_slice(), |r| {
        Ok(WhiteboardElementDTO {
            id: r.get(0)?,
            board_id: r.get(1)?,
            element_type: r.get(2)?,
            x: r.get(3)?,
            y: r.get(4)?,
            width: r.get(5)?,
            height: r.get(6)?,
            z_index: r.get(7)?,
            style_json: r.get(8)?,
            content_json: r.get(9)?,
            created_at: r.get(10)?,
            updated_at: r.get(11)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// NOTEBOOK CELLS
// ═══════════════════════════════════════════════════════════════════════════

pub fn cell_insert(id: &str, board_id: &str, cell_type: &str, cell_order: i32, content: Option<&str>) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO notebook_cells (id, board_id, cell_type, cell_order, content, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![id, board_id, cell_type, cell_order, content, now, now],
    )?;
    Ok(())
}

pub fn cell_update(c: &NotebookCellDTO) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "UPDATE notebook_cells SET cell_type=?2, cell_order=?3, content=?4, output=?5, collapsed=?6, height=?7, metadata_json=?8, updated_at=?9 WHERE id=?1",
        params![c.id, c.cell_type, c.cell_order, c.content, c.output, c.collapsed as i32, c.height, c.metadata_json, now],
    )?;
    Ok(())
}

pub fn cell_delete(id: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute("DELETE FROM notebook_cells WHERE id=?1", params![id])?;
    Ok(())
}

pub fn cell_reorder(board_id: &str, cell_ids: &[String]) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    for (idx, cell_id) in cell_ids.iter().enumerate() {
        conn.execute(
            "UPDATE notebook_cells SET cell_order=?1, updated_at=?2 WHERE id=?3 AND board_id=?4",
            params![idx as i32, now, cell_id, board_id],
        )?;
    }
    Ok(())
}

/// List cells by board IDs (for snapshot)
pub fn cell_list_by_boards(board_ids: &[String]) -> Result<Vec<NotebookCellDTO>> {
    if board_ids.is_empty() { return Ok(vec![]); }

    let conn = db().lock_safe();
    let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json, created_at, updated_at
         FROM notebook_cells WHERE board_id IN ({}) ORDER BY cell_order",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> = board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

    let rows = stmt.query_map(params.as_slice(), |r| {
        Ok(NotebookCellDTO {
            id: r.get(0)?,
            board_id: r.get(1)?,
            cell_type: r.get(2)?,
            cell_order: r.get(3)?,
            content: r.get(4)?,
            output: r.get(5)?,
            collapsed: r.get::<_, i32>(6)? != 0,
            height: r.get(7)?,
            metadata_json: r.get(8)?,
            created_at: r.get(9)?,
            updated_at: r.get(10)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// BOARD METADATA
// ═══════════════════════════════════════════════════════════════════════════

pub fn board_meta_upsert(board_id: &str, labels: &[String], rating: i32, contains_model: Option<&str>, contains_skills: &[String]) -> Result<()> {
    let labels_json = serde_json::to_string(labels)?;
    let skills_json = serde_json::to_string(contains_skills)?;
    let conn = db().lock_safe();
    conn.execute(
        "INSERT INTO board_metadata (board_id, labels, rating, contains_model, contains_skills) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(board_id) DO UPDATE SET labels=?2, rating=?3, contains_model=?4, contains_skills=?5",
        params![board_id, labels_json, rating, contains_model, skills_json],
    )?;
    Ok(())
}

pub fn board_meta_update_labels(board_id: &str, labels: &[String]) -> Result<()> {
    let labels_json = serde_json::to_string(labels)?;
    let conn = db().lock_safe();
    conn.execute(
        "INSERT INTO board_metadata (board_id, labels) VALUES (?1, ?2) ON CONFLICT(board_id) DO UPDATE SET labels=?2",
        params![board_id, labels_json],
    )?;
    Ok(())
}

pub fn board_meta_update_rating(board_id: &str, rating: i32) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT INTO board_metadata (board_id, rating) VALUES (?1, ?2) ON CONFLICT(board_id) DO UPDATE SET rating=?2",
        params![board_id, rating],
    )?;
    Ok(())
}

/// List board metadata by board IDs (for snapshot)
pub fn board_metadata_list_by_boards(board_ids: &[String]) -> Result<Vec<BoardMetadataDTO>> {
    if board_ids.is_empty() { return Ok(vec![]); }

    let conn = db().lock_safe();
    let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT board_id, labels, rating, view_count, contains_model, contains_skills, board_type, last_accessed, COALESCE(is_pinned, 0), COALESCE(meta_updated_at, 0), COALESCE(pin_updated_at, 0)
         FROM board_metadata WHERE board_id IN ({})",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> = board_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

    let rows = stmt.query_map(params.as_slice(), |r| {
        let labels_json: String = r.get(1)?;
        let skills_json: String = r.get(5)?;
        Ok(BoardMetadataDTO {
            board_id: r.get(0)?,
            labels: serde_json::from_str(&labels_json).unwrap_or_default(),
            rating: r.get(2)?,
            view_count: r.get(3)?,
            contains_model: r.get(4)?,
            contains_skills: serde_json::from_str(&skills_json).unwrap_or_default(),
            board_type: r.get(6)?,
            last_accessed: r.get(7)?,
            is_pinned: r.get::<_, i32>(8)? != 0,
            meta_updated_at: r.get(9)?,
            pin_updated_at: r.get(10)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// USER PROFILES
// ═══════════════════════════════════════════════════════════════════════════

pub fn profile_upsert(node_id: &str, display_name: &str, avatar_hash: Option<&str>) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "INSERT INTO user_profiles (node_id, display_name, avatar_hash, status, updated_at) VALUES (?1, ?2, ?3, 'online', ?4) ON CONFLICT(node_id) DO UPDATE SET display_name=excluded.display_name, avatar_hash=COALESCE(excluded.avatar_hash, user_profiles.avatar_hash), status='online', updated_at=excluded.updated_at",
        params![node_id, display_name, avatar_hash, now],
    )?;
    Ok(())
}

/// Get profile by node_id: (display_name, avatar_hash)
pub fn profile_get(node_id: &str) -> Option<(String, Option<String>)> {
    let conn = db().lock_safe();
    conn.query_row(
        "SELECT display_name, avatar_hash FROM user_profiles WHERE node_id = ?1",
        params![node_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ).optional().ok()?
}

// ═══════════════════════════════════════════════════════════════════════════
// GROUP KNOWN PEERS — persisted NodeAddr store for topic re-seeding (MESH §2.3)
// ═══════════════════════════════════════════════════════════════════════════

/// Persist a resolvable peer `EndpointAddr` (serialized JSON) for a group, so the group's gossip
/// topic can be re-seeded on rejoin. Idempotent upsert keyed by (group_id, peer_id).
pub fn group_known_peer_upsert(group_id: &str, peer_id: &str, addr_json: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "INSERT INTO group_known_peers (group_id, peer_id, addr_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(group_id, peer_id) DO UPDATE SET addr_json=excluded.addr_json, updated_at=excluded.updated_at",
        params![group_id, peer_id, addr_json, now],
    )?;
    Ok(())
}

/// All persisted peer addresses for a group, as `(peer_id, addr_json)` — the re-seed source on
/// rejoin (MESH §2.3). Returns an empty vec if the group has no saved peers.
pub fn group_known_peers_list(group_id: &str) -> Vec<(String, String)> {
    let conn = db().lock_safe();
    let mut stmt = match conn.prepare(
        "SELECT peer_id, addr_json FROM group_known_peers WHERE group_id = ?1",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let rows = match stmt.query_map(params![group_id], |r| Ok((r.get(0)?, r.get(1)?))) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    rows.filter_map(|r| r.ok()).collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// GROUP MEMBERS — persistent presence roster (MESH §3)
// ═══════════════════════════════════════════════════════════════════════════

/// Record that `peer_id` was seen in `group_id` at `now` (gossip NeighborUp / gossip author /
/// chat author / profile). Sets `first_seen` once, advances `last_seen` on every contact. The row
/// is never deleted, so an offline peer stays in the roster (greyed, with its cached last-seen).
pub fn member_seen(group_id: &str, peer_id: &str, now: i64) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT INTO group_members (group_id, peer_id, first_seen, last_seen)
         VALUES (?1, ?2, ?3, ?3)
         ON CONFLICT(group_id, peer_id) DO UPDATE SET last_seen=excluded.last_seen",
        params![group_id, peer_id, now],
    )?;
    Ok(())
}

/// The persistent roster for a group: `(peer_id, name, avatar, last_seen)` ordered by first contact.
/// Name/avatar are resolved from `user_profiles` (None until a profile is seen). The caller overlays
/// the live `online` flag from `peers_per_group`. Tenant-scoped by `group_id`.
pub fn group_members_list(group_id: &str) -> Vec<(String, Option<String>, Option<String>, i64)> {
    let conn = db().lock_safe();
    let mut stmt = match conn.prepare(
        "SELECT m.peer_id, p.display_name, p.avatar_hash, m.last_seen
         FROM group_members m
         LEFT JOIN user_profiles p ON p.node_id = m.peer_id
         WHERE m.group_id = ?1
         ORDER BY m.first_seen ASC",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let rows = match stmt.query_map(params![group_id], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    }) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    rows.filter_map(|r| r.ok()).collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// GROUP SYNC STATE — "synced as of T" watermark (MESH_HARDENING §5/§11)
// ═══════════════════════════════════════════════════════════════════════════

/// Record that `group_id` is synced as of `synced_as_of` (unix seconds). Monotonic
/// (Last-Writer-Wins on the MAX): a later import / catch-up only advances the watermark,
/// never rewinds it, so a stale bundle can't reset a group that has already caught up.
/// New code: no `unwrap` on the lock (panics cross the FFI boundary).
pub fn group_sync_state_set(group_id: &str, synced_as_of: i64) -> Result<()> {
    let conn = db().lock().map_err(|_| anyhow!("db mutex poisoned"))?;
    conn.execute(
        "INSERT INTO group_sync_state (group_id, synced_as_of) VALUES (?1, ?2)
         ON CONFLICT(group_id) DO UPDATE SET synced_as_of = MAX(synced_as_of, excluded.synced_as_of)",
        params![group_id, synced_as_of],
    )?;
    Ok(())
}

/// The "synced as of T" watermark for `group_id`, or `None` if never recorded. This is the
/// `since` an incremental catch-up uses on first online contact after a bundle import.
pub fn group_sync_state_get(group_id: &str) -> Option<i64> {
    let conn = db().lock().ok()?;
    conn.query_row(
        "SELECT synced_as_of FROM group_sync_state WHERE group_id = ?1",
        params![group_id],
        |r| r.get(0),
    )
    .optional()
    .ok()
    .flatten()
}

// ═══════════════════════════════════════════════════════════════════════════
// MESH HOLD — durable, content-addressed offline-hold outbox (MESH_HARDENING §4)
// ═══════════════════════════════════════════════════════════════════════════

/// Persist one group broadcast into the durable hold store, content-addressed by
/// `hash` = blake3(payload). Idempotent: re-holding the same content is a no-op (the
/// `(group_id, hash)` primary key dedups), so a re-broadcast never bloats the store.
/// This is the seam the Lens super-peer consumes to hold/serve messages for offline peers.
pub fn hold_put(group_id: &str, hash: &str, payload: &[u8], created_at: i64) -> Result<()> {
    let conn = db().lock().map_err(|_| anyhow!("db mutex poisoned"))?;
    conn.execute(
        "INSERT OR IGNORE INTO mesh_hold (group_id, hash, payload, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![group_id, hash, payload, created_at],
    )?;
    Ok(())
}

/// Held broadcasts for `group_id` created strictly after `since` (unix seconds), oldest
/// first, as `(hash, payload, created_at)`. A super-peer serving an offline peer's
/// reconnect passes the peer's last-seen watermark as `since` and replays only what it missed.
pub fn hold_list_since(group_id: &str, since: i64) -> Vec<(String, Vec<u8>, i64)> {
    let Ok(conn) = db().lock() else {
        return Vec::new();
    };
    let mut stmt = match conn.prepare(
        "SELECT hash, payload, created_at FROM mesh_hold
         WHERE group_id = ?1 AND created_at > ?2 ORDER BY created_at ASC",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let rows = match stmt.query_map(params![group_id, since], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    }) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    rows.filter_map(|r| r.ok()).collect()
}

/// Count of held broadcasts for `group_id` (the offline-hold depth oracle for §4 tests).
pub fn hold_count(group_id: &str) -> i64 {
    let Ok(conn) = db().lock() else { return 0 };
    conn.query_row(
        "SELECT COUNT(*) FROM mesh_hold WHERE group_id = ?1",
        params![group_id],
        |r| r.get(0),
    )
    .unwrap_or(0)
}

// ═══════════════════════════════════════════════════════════════════════════
// DIRECT MESSAGES
// ═══════════════════════════════════════════════════════════════════════════

pub fn dm_insert(id: &str, peer_id: &str, message: &str, timestamp: i64, is_incoming: bool) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO direct_messages (id, peer_id, message, timestamp, is_incoming) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, peer_id, message, timestamp, is_incoming],
    )?;
    Ok(())
}

/// List DM history with a peer
pub fn dm_list_by_peer(peer_id: &str, limit: usize) -> Result<Vec<(String, String, i64, bool)>> {
    let conn = db().lock_safe();
    let mut stmt = conn.prepare(
        "SELECT id, message, timestamp, is_incoming FROM direct_messages
         WHERE peer_id = ?1 ORDER BY timestamp ASC LIMIT ?2"
    )?;
    let rows = stmt.query_map(params![peer_id, limit as i64], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get::<_, i32>(3)? != 0))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// INTEGRATION BINDINGS
// ═══════════════════════════════════════════════════════════════════════════

/// List integrations by group (scope_id matches group_id or any workspace in group)
pub fn integration_list_by_group(group_id: &str) -> Result<Vec<IntegrationBindingDTO>> {
    let conn = db().lock_safe();

    // Get workspace IDs for this group
    let workspace_ids: Vec<String> = {
        let mut stmt = conn.prepare("SELECT id FROM workspaces WHERE group_id = ?1")?;
        stmt.query_map(params![group_id], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect()
    };

    // Build IN clause for scope_ids (group_id + all workspace_ids)
    let mut scope_ids = vec![group_id.to_string()];
    scope_ids.extend(workspace_ids);

    let placeholders: String = scope_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, scope_type, scope_id, integration_type, config_json, created_at
         FROM integration_bindings WHERE scope_id IN ({}) ORDER BY created_at",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> = scope_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

    let rows = stmt.query_map(params.as_slice(), |r| {
        let config_str: String = r.get(4)?;
        let config = serde_json::from_str(&config_str).unwrap_or(serde_json::Value::Null);
        Ok(IntegrationBindingDTO {
            id: r.get(0)?,
            scope_type: r.get(1)?,
            scope_id: r.get(2)?,
            integration_type: r.get(3)?,
            config,
            created_at: r.get(5)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// SNAPSHOT BATCH OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════

pub fn snapshot_insert_structure(group: &Group, workspaces: &[Workspace], boards: &[WhiteboardDTO]) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute_batch("BEGIN TRANSACTION")?;

    conn.execute(
        "INSERT OR REPLACE INTO groups (id, name, icon, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![group.id, group.name, group.icon, group.color, group.created_at],
    )?;

    for ws in workspaces {
        conn.execute(
            "INSERT OR REPLACE INTO workspaces (id, group_id, name, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![ws.id, ws.group_id, ws.name, ws.created_at],
        )?;
    }

    for b in boards {
        conn.execute(
            "INSERT OR REPLACE INTO objects (id, workspace_id, type, name, created_at) VALUES (?1, ?2, 'whiteboard', ?3, ?4)",
            params![b.id, b.workspace_id, b.name, b.created_at],
        )?;
    }

    conn.execute_batch("COMMIT")?;
    Ok(())
}

pub fn snapshot_insert_content(elements: &[WhiteboardElementDTO], cells: &[NotebookCellDTO]) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute_batch("BEGIN TRANSACTION")?;

    for e in elements {
        conn.execute(
            "INSERT OR REPLACE INTO whiteboard_elements (id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![e.id, e.board_id, e.element_type, e.x, e.y, e.width, e.height, e.z_index, e.style_json, e.content_json, e.created_at, e.updated_at],
        )?;
    }

    for c in cells {
        conn.execute(
            "INSERT OR REPLACE INTO notebook_cells (id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![c.id, c.board_id, c.cell_type, c.cell_order, c.content, c.output, c.collapsed, c.height, c.metadata_json, c.created_at, c.updated_at],
        )?;
    }

    conn.execute_batch("COMMIT")?;
    Ok(())
}

/// Batch insert files (for snapshot)
pub fn snapshot_insert_files(files: &[FileDTO]) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute_batch("BEGIN TRANSACTION")?;

    for f in files {
        conn.execute(
            "INSERT OR REPLACE INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, local_path, created_at) VALUES (?1, ?2, ?3, ?4, 'file', ?5, ?6, ?7, ?8, ?9, ?10)",
            params![f.id, f.group_id, f.workspace_id, f.board_id, f.name, f.hash, f.size as i64, f.source_peer, f.local_path, f.created_at],
        )?;
    }

    conn.execute_batch("COMMIT")?;
    Ok(())
}

/// Batch insert chats (for snapshot)
pub fn snapshot_insert_chats(chats: &[ChatDTO]) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute_batch("BEGIN TRANSACTION")?;

    for c in chats {
        conn.execute(
            "INSERT OR REPLACE INTO objects (id, board_id, workspace_id, type, name, hash, data, created_at) VALUES (?1, ?2, ?3, 'chat', ?4, ?5, ?6, ?7)",
            params![c.id, c.board_id, c.workspace_id, c.message, c.author, c.parent_id.as_ref().map(|s| s.as_bytes()), c.timestamp],
        )?;
    }

    conn.execute_batch("COMMIT")?;
    Ok(())
}

/// Batch insert board metadata (for snapshot). Routes each row through the per-field LWW
/// merge ([`board_metadata_upsert`]) — never a whole-record replace (R11 §9/PATTERN).
pub fn snapshot_insert_metadata(metadata: &[BoardMetadataDTO]) -> Result<()> {
    for m in metadata {
        board_metadata_upsert(
            &m.board_id,
            &m.labels,
            m.rating,
            m.view_count,
            m.contains_model.as_deref(),
            &m.contains_skills,
            Some(&m.board_type),
            m.last_accessed,
            m.is_pinned,
            m.meta_updated_at,
            m.pin_updated_at,
        )?;
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// MIGRATIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Run schema migrations for existing databases
pub fn run_migrations(conn: &Connection) -> Result<()> {
    // Check and add board_id column
    if conn.prepare("SELECT board_id FROM objects LIMIT 1").is_err() {
        tracing::info!("Migration: adding board_id column");
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN board_id TEXT", []);
    }
    // Check and add source_peer column
    if conn.prepare("SELECT source_peer FROM objects LIMIT 1").is_err() {
        tracing::info!("Migration: adding source_peer column");
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN source_peer TEXT", []);
    }
    // Check and add local_path column
    if conn.prepare("SELECT local_path FROM objects LIMIT 1").is_err() {
        tracing::info!("Migration: adding local_path column");
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN local_path TEXT", []);
    }
    // Check and add cell_id column to whiteboard_elements
    if conn.prepare("SELECT cell_id FROM whiteboard_elements LIMIT 1").is_err() {
        tracing::info!("Migration: adding cell_id column to whiteboard_elements");
        let _ = conn.execute("ALTER TABLE whiteboard_elements ADD COLUMN cell_id TEXT", []);
    }
    // Check and add board_mode column to objects
    if conn.prepare("SELECT board_mode FROM objects LIMIT 1").is_err() {
        tracing::info!("Migration: adding board_mode column to objects");
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN board_mode TEXT DEFAULT 'canvas'", []);
    }

    // Migrate existing 'freeform' values to 'canvas'
    let _ = conn.execute("UPDATE objects SET board_mode = 'canvas' WHERE board_mode = 'freeform' OR board_mode IS NULL", []);

    // Check and create board_metadata table if not exists
    if conn.prepare("SELECT board_id FROM board_metadata LIMIT 1").is_err() {
        tracing::info!("Migration: creating board_metadata table");
        let _ = conn.execute("CREATE TABLE IF NOT EXISTS board_metadata (board_id TEXT PRIMARY KEY, labels TEXT DEFAULT '[]', rating INTEGER DEFAULT 0, view_count INTEGER DEFAULT 0, contains_model TEXT, contains_skills TEXT DEFAULT '[]', board_type TEXT DEFAULT 'canvas', last_accessed INTEGER DEFAULT 0, is_pinned INTEGER DEFAULT 0)", []);
        let _ = conn.execute("CREATE INDEX IF NOT EXISTS idx_board_rating ON board_metadata(rating DESC)", []);
        let _ = conn.execute("CREATE INDEX IF NOT EXISTS idx_board_pinned ON board_metadata(is_pinned DESC)", []);
    }

    // Migration: Add is_pinned column if missing
    if conn.prepare("SELECT is_pinned FROM board_metadata LIMIT 1").is_err() {
        tracing::info!("Migration: adding is_pinned column to board_metadata");
        let _ = conn.execute("ALTER TABLE board_metadata ADD COLUMN is_pinned INTEGER DEFAULT 0", []);
        let _ = conn.execute("CREATE INDEX IF NOT EXISTS idx_board_pinned ON board_metadata(is_pinned DESC)", []);
    }

    // Check and create user_profiles table if not exists
    if conn.prepare("SELECT node_id FROM user_profiles LIMIT 1").is_err() {
        tracing::info!("Migration: creating user_profiles table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS user_profiles (
                node_id TEXT PRIMARY KEY,
                display_name TEXT,
                avatar_hash TEXT,
                status TEXT DEFAULT 'offline',
                last_seen INTEGER,
                updated_at INTEGER
            )",
            [],
        );
    }

    // Check and create direct_messages table if not exists
    if conn.prepare("SELECT id FROM direct_messages LIMIT 1").is_err() {
        tracing::info!("Migration: creating direct_messages table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS direct_messages (
                id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                workspace_id TEXT,
                message TEXT NOT NULL,
                parent_id TEXT,
                is_incoming INTEGER DEFAULT 0,
                timestamp INTEGER NOT NULL
            )",
            [],
        );
        let _ = conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dm_peer ON direct_messages(peer_id, timestamp)",
            [],
        );
    }

    // Migration: file_transfers table for resumable downloads
    if conn.prepare("SELECT file_id FROM file_transfers LIMIT 1").is_err() {
        tracing::info!("Migration: creating file_transfers table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS file_transfers (
                file_id TEXT PRIMARY KEY,
                file_name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                hash TEXT NOT NULL,
                bytes_received INTEGER DEFAULT 0,
                temp_path TEXT NOT NULL,
                source_peer TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        );
        let _ = conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ft_status ON file_transfers(status)",
            [],
        );
    }

    // Migration: integration_bindings table
    if conn.prepare("SELECT id FROM integration_bindings LIMIT 1").is_err() {
        tracing::info!("Migration: creating integration_bindings table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS integration_bindings (
                id TEXT PRIMARY KEY,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                integration_type TEXT NOT NULL,
                config_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL
            )",
            [],
        );
        let _ = conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ib_scope ON integration_bindings(scope_id)",
            [],
        );
    }

    // Migration: group_known_peers — the per-group persisted NodeAddr store (MESH_HARDENING §2.3).
    // On rejoin we re-seed each group's gossip topic from these saved `EndpointAddr`s so the mesh
    // re-forms without depending on bootstrap/relay being reachable. Keyed by (group_id, peer_id);
    // the group_id IS the tenant boundary (a group belongs to one tenant), so this is tenant-scoped.
    if conn.prepare("SELECT group_id FROM group_known_peers LIMIT 1").is_err() {
        tracing::info!("Migration: creating group_known_peers table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS group_known_peers (
                group_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                addr_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (group_id, peer_id)
            )",
            [],
        );
    }

    // Migration: group_members — the persistent presence ROSTER (MESH_HARDENING §3). Anyone ever
    // seen in a group (gossip NeighborUp / gossip author) is recorded with first_seen/last_seen and
    // survives restart; name/avatar resolve from user_profiles, and `online` is overlaid at read
    // time from the live neighbor set (peers_per_group). Tenant-scoped by group_id like the roster.
    if conn.prepare("SELECT group_id FROM group_members LIMIT 1").is_err() {
        tracing::info!("Migration: creating group_members table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS group_members (
                group_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                PRIMARY KEY (group_id, peer_id)
            )",
            [],
        );
    }

    // Migration: group_sync_state — the "synced as of T" watermark per group (MESH_HARDENING
    // §5/§11). An imported Group Export bundle stamps the bundle's `synced_as_of` here; the
    // engine then drives an INCREMENTAL catch-up (`since = synced_as_of`) on first online
    // contact instead of a full re-snapshot. One row per group; LWW on the max watermark.
    if conn.prepare("SELECT group_id FROM group_sync_state LIMIT 1").is_err() {
        tracing::info!("Migration: creating group_sync_state table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS group_sync_state (
                group_id TEXT PRIMARY KEY,
                synced_as_of INTEGER NOT NULL
            )",
            [],
        );
    }

    // Migration: mesh_hold — the OFFLINE-HOLD durable outbox (MESH_HARDENING §4). Every group
    // broadcast is also persisted here, content-addressed by blake3(payload), so it is
    // deliverable on reconnect. This is the clean seam the Lens super-peer consumes to hold and
    // re-serve messages for peers that were offline. Append-only; dedup by (group_id, hash).
    if conn.prepare("SELECT group_id FROM mesh_hold LIMIT 1").is_err() {
        tracing::info!("Migration: creating mesh_hold table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS mesh_hold (
                group_id TEXT NOT NULL,
                hash TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (group_id, hash)
            )",
            [],
        );
        let _ = conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mesh_hold_created ON mesh_hold(group_id, created_at)",
            [],
        );
    }

    // Ensure is_pinned index exists (after column migration has run)
    let _ = conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_board_pinned ON board_metadata(is_pinned DESC)",
        [],
    );

    // In run_migrations() function:

    // Migration: Add owner_node_id to groups table
    if conn.prepare("SELECT owner_node_id FROM groups LIMIT 1").is_err() {
        tracing::info!("Migration: adding owner_node_id column to groups");
        let _ = conn.execute("ALTER TABLE groups ADD COLUMN owner_node_id TEXT", []);
    }

    // Migration: Add owner_node_id to workspaces table
    if conn.prepare("SELECT owner_node_id FROM workspaces LIMIT 1").is_err() {
        tracing::info!("Migration: adding owner_node_id column to workspaces");
        let _ = conn.execute("ALTER TABLE workspaces ADD COLUMN owner_node_id TEXT", []);
    }

    // Migration: Add owner_node_id to objects table
    if conn.prepare("SELECT owner_node_id FROM objects LIMIT 1").is_err() {
        tracing::info!("Migration: adding owner_node_id column to objects");
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN owner_node_id TEXT", []);
    }

    // ROUND8 §W3: workspaces carry a `is_system` flag — the per-group auto-seeded
    // "Plugins" workspace is system + non-deletable. Constant default keeps the ALTER
    // valid on existing rows; idempotent.
    if conn.prepare("SELECT is_system FROM workspaces LIMIT 1").is_err() {
        tracing::info!("Migration: adding is_system column to workspaces");
        let _ = conn.execute("ALTER TABLE workspaces ADD COLUMN is_system INTEGER NOT NULL DEFAULT 0", []);
    }

    // ROUND8 §W1: collapse the six legacy authoring cell kinds into the single
    // step primitive. Idempotent + data-loss-free; preserves the version column so
    // it stays invisible to the anti-entropy digest (each peer reaches the same
    // migrated state deterministically — no spurious repair churn).
    let _ = migrate_legacy_authoring_cells_conn(conn);

    // ROUND8 §W2: notes — a board-level authored LWW ledger with its OWN store (NOT
    // notebook cells). Created here for DBs that predate the table; idempotent.
    if conn.prepare("SELECT id FROM notes LIMIT 1").is_err() {
        tracing::info!("Migration: creating notes table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS notes (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                author_id TEXT NOT NULL,
                author_name TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                scope TEXT NOT NULL DEFAULT 'board',
                kind TEXT NOT NULL DEFAULT 'editor-note'
            )",
            [],
        );
        let _ = conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_board ON notes(board_id)", []);
    }
    // feat/notes-constitution: notes gain SCOPE (tenant/group/board — `board_id` is the
    // scope anchor) + KIND (constitution/preference/editor-note), ADDITIVE — legacy rows
    // default to 'board'/'editor-note', i.e. exactly the pre-scope behavior.
    if conn.prepare("SELECT scope FROM notes LIMIT 1").is_err() {
        tracing::info!("Migration: adding scope column to notes");
        let _ = conn
            .execute("ALTER TABLE notes ADD COLUMN scope TEXT NOT NULL DEFAULT 'board'", []);
    }
    if conn.prepare("SELECT kind FROM notes LIMIT 1").is_err() {
        tracing::info!("Migration: adding kind column to notes");
        let _ = conn.execute(
            "ALTER TABLE notes ADD COLUMN kind TEXT NOT NULL DEFAULT 'editor-note'",
            [],
        );
    }
    let _ = conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_notes_tenant_scope_kind
         ON notes(tenant_id, scope, board_id, kind)",
        [],
    );
    // CHAT C1/C7 (Anchored Lane, additive): chat rows (in `objects`) and notes gain an
    // optional step/board anchor; notes additionally gain provenance (`origin_ref`,
    // `chat:<message_id>` for promoted notes). Nullable columns — every pre-C1/C7 row
    // reads back as NULL ⇒ unanchored, exactly the prior behavior. Idempotent.
    if conn.prepare("SELECT anchor_kind FROM objects LIMIT 1").is_err() {
        tracing::info!("Migration: adding anchor_kind/anchor_id columns to objects (chat C1)");
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN anchor_kind TEXT", []);
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN anchor_id TEXT", []);
    }
    if conn.prepare("SELECT anchor_kind FROM notes LIMIT 1").is_err() {
        tracing::info!("Migration: adding anchor_kind/anchor_id/origin_ref columns to notes (chat C7)");
        let _ = conn.execute("ALTER TABLE notes ADD COLUMN anchor_kind TEXT", []);
        let _ = conn.execute("ALTER TABLE notes ADD COLUMN anchor_id TEXT", []);
        let _ = conn.execute("ALTER TABLE notes ADD COLUMN origin_ref TEXT", []);
    }

    // ROUND8 §W4: templates — a pre-written English workflow (steps + bound plugins)
    // cloned into a board. Own store; user templates are tenant-scoped (built-in seeds
    // live in code, never persisted). Created here for DBs that predate it; idempotent.
    if conn.prepare("SELECT id FROM templates LIMIT 1").is_err() {
        tracing::info!("Migration: creating templates table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS templates (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT 'user',
                steps_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )",
            [],
        );
        let _ = conn.execute("CREATE INDEX IF NOT EXISTS idx_templates_tenant ON templates(tenant_id)", []);
    }

    // ROUND8 §W4: pins — board-level pinned-workflow state (a pinned workflow surfaces
    // for fast cloning). Own store keyed by board_id; LWW on updated_at; rides the
    // existing anti-entropy digest + snapshot path like notes. Idempotent migration.
    if conn.prepare("SELECT board_id FROM pins LIMIT 1").is_err() {
        tracing::info!("Migration: creating pins table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS pins (
                board_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                pinned INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        );
    }

    // R12 D2/E1: per-board workflow lifecycle state — `deployed`/`dashboard_available` gate
    // the board face (editor vs running dashboard); `locked` (set on deploy) freezes edits and
    // an unlock requires an org-XaeroID grant (W17). Keyed by board_id; LWW on updated_at.
    if conn.prepare("SELECT board_id FROM board_workflow_state LIMIT 1").is_err() {
        tracing::info!("Migration: creating board_workflow_state table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS board_workflow_state (
                board_id            TEXT PRIMARY KEY,
                deployed            INTEGER NOT NULL DEFAULT 0,
                dashboard_available INTEGER NOT NULL DEFAULT 0,
                locked              INTEGER NOT NULL DEFAULT 0,
                updated_at          INTEGER NOT NULL DEFAULT 0
            )",
            [],
        );
    }

    // R10FB §F4: soft-delete/tombstone column for files (objects). A user-initiated
    // delete sets `deleted=1` so the deletion converges to peers; the engine never
    // hard-deletes a file. All file reads filter `deleted=0`. Idempotent migration.
    if conn.prepare("SELECT deleted FROM objects LIMIT 1").is_err() {
        tracing::info!("Migration: adding deleted column to objects (file tombstone)");
        let _ = conn.execute("ALTER TABLE objects ADD COLUMN deleted INTEGER DEFAULT 0", []);
    }

    // R12 A2: cold-chat first-open speed. `chat_list_by_board` ran a full scan of the big
    // multi-purpose `objects` table (chats + files + boards + whiteboard elements) for every
    // board's chat load. A partial, ordered index makes it an index range scan in
    // created_at order, so the first page returns without a table scan. Partial (`WHERE
    // type='chat'`) keeps it tiny and off the file/board write paths.
    let _ = conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_objects_chat_board
         ON objects(board_id, created_at) WHERE type = 'chat'",
        [],
    );

    // R10FB §N: per-reader unread ledger. One row per message_id, ever — the PRIMARY KEY
    // makes counting idempotent (a message counts once for this reader). `kind` is the
    // notification type seam ('chat' now; 'nudge'/'ask'/'decision' later, §N5). Each row
    // carries the board/workspace/group scope so counts roll up at all three levels.
    if conn.prepare("SELECT message_id FROM unread LIMIT 1").is_err() {
        tracing::info!("Migration: creating unread table");
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS unread (
                message_id   TEXT PRIMARY KEY,
                kind         TEXT NOT NULL,
                group_id     TEXT,
                workspace_id TEXT,
                board_id     TEXT,
                read         INTEGER NOT NULL DEFAULT 0,
                created_at   INTEGER NOT NULL
            )",
            [],
        );
        let _ = conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_unread_open ON unread(read)",
            [],
        );
    }

    // R11 §9/§9b: per-field LWW clocks for board_metadata (descriptive lane + pin lane), so
    // a snapshot merge converges per-field instead of whole-record clobbering. Idempotent.
    if conn.prepare("SELECT meta_updated_at FROM board_metadata LIMIT 1").is_err() {
        tracing::info!("Migration: adding per-field LWW clocks to board_metadata");
        let _ = conn.execute("ALTER TABLE board_metadata ADD COLUMN meta_updated_at INTEGER DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE board_metadata ADD COLUMN pin_updated_at INTEGER DEFAULT 0", []);
    }

    // R11 §1: re-key legacy workspace-scoped chats onto a board (idempotent — only touches
    // rows missing a board_id). Runs on the init connection (the global DB handle is not set
    // until init_db finishes), so it operates on `conn` directly.
    match migrate_chats_to_boards_conn(conn) {
        Ok(n) if n > 0 => tracing::info!("Migration: re-keyed {n} legacy chat rows to a board"),
        Ok(_) => {}
        Err(e) => tracing::warn!("Migration: chat re-key failed: {e}"),
    }

    // ChangeList store (CYAN_CHANGELIST_STORE_AND_REVIEW_LOOP §Part 1): the durable,
    // content-addressed per-asset change-list artifact the Frame.io review-&-conform
    // loop operates on. Creates change_entry / change_version / change_branch /
    // change_audit. Idempotent (CREATE TABLE IF NOT EXISTS); additive — no existing
    // table or behavior changes.
    if let Err(e) = crate::changelist::migrate(conn) {
        tracing::warn!("Migration: changelist tables failed: {e}");
    }

    // Review-loop state machine (CYAN_REVIEW_LOOP_TRANSITION_CONTRACT): the per
    // (tenant, asset, branch) review_state row (DRAFT..DELIVERED + round counter)
    // the editable-proposal review loop advances. Creates `review_state`.
    // Idempotent (CREATE TABLE IF NOT EXISTS); additive — no existing table or
    // behavior changes.
    if let Err(e) = crate::review_state::migrate(conn) {
        tracing::warn!("Migration: review_state table failed: {e}");
    }

    // Batch-confirm gate (feat/notes-constitution): per-editor trust tiers over
    // the changelist confirm surface. Creates `editor_trust`. Idempotent
    // (CREATE TABLE IF NOT EXISTS); additive — no existing table or behavior
    // changes.
    if let Err(e) = crate::batch_confirm::migrate(conn) {
        tracing::warn!("Migration: editor_trust table failed: {e}");
    }

    // Asset registry (CYAN_FORMAT_SPEC / CYAN_FORMAT_QA): one row per
    // content-addressed media asset — kind/fps/duration (frame math), derivation
    // edges (proxy/deliverable → {parent master, version}), and remote refs
    // (e.g. the Frame.io file id a proxy was published as). Creates `asset`.
    // Idempotent (CREATE TABLE IF NOT EXISTS); additive — no existing table or
    // behavior changes.
    if let Err(e) = crate::asset_registry::migrate(conn) {
        tracing::warn!("Migration: asset registry table failed: {e}");
    }

    // Review-loop controller (CYAN_CHANGELIST_STORE_AND_REVIEW_LOOP §Part 2,
    // engine delta #3): the per (board, asset) loop registration + the rounds-as-
    // sequential-runs stamp table. Creates `review_loop` / `review_loop_run`.
    // Idempotent (CREATE TABLE IF NOT EXISTS); additive — no existing table or
    // behavior changes.
    if let Err(e) = crate::review_loop::migrate(conn) {
        tracing::warn!("Migration: review_loop tables failed: {e}");
    }

    // STAGE 4 ingest (AUTHORING_FIXES_ROUND2 §STAGE 4): watched sources
    // (folder / s3 / frameio_c2c) + per-asset workflow runs. Creates
    // `ingest_source` / `workflow_run`. Idempotent (CREATE TABLE IF NOT
    // EXISTS); additive — no existing table or behavior changes.
    if let Err(e) = crate::ingest::migrate(conn) {
        tracing::warn!("Migration: ingest tables failed: {e}");
    }

    // Per-install / per-workflow plugin CONFIG (PLUGIN_CREDENTIAL_ONBOARDING
    // §A): non-secret plugin targets (account_id/folder_id/…) scoped board →
    // tenant, replacing the global env stopgap. Creates `plugin_config`.
    // Idempotent (CREATE TABLE IF NOT EXISTS); additive.
    if let Err(e) = crate::plugin_config::migrate(conn) {
        tracing::warn!("Migration: plugin_config table failed: {e}");
    }

    // B4 — per-step edit history (undo/redo stacks). Creates
    // `cell_edit_history`. Idempotent; additive.
    if let Err(e) = crate::step_history::migrate(conn) {
        tracing::warn!("Migration: cell_edit_history table failed: {e}");
    }

    Ok(())
}

/// Collapse legacy authorable cells (`markdown`/`mermaid`/`canvas`/`image`/`code`/
/// `model`) into the ROUND8 §W1 step model. Returns the number of rows migrated.
///
/// Text-bearing kinds (`markdown`, `code`) become **steps** (their text is the step
/// text). Non-text kinds (`mermaid`, `canvas`, `image`, `model`) are **archived**
/// (kept, never authorable; original kind stashed in `metadata_json.original_cell_type`
/// so nothing is dropped). `created_at`/`updated_at` are left untouched so the
/// migration does not perturb the convergence digest.
pub fn migrate_legacy_authoring_cells() -> Result<usize> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    migrate_legacy_authoring_cells_conn(&conn)
}

fn migrate_legacy_authoring_cells_conn(conn: &Connection) -> Result<usize> {
    // Text kinds become steps; the rest are archived (content + original kind kept).
    const TEXT_KINDS: &[&str] = &["markdown", "code"];

    let mut stmt = conn.prepare(
        "SELECT id, cell_type, metadata_json FROM notebook_cells \
         WHERE cell_type IN ('markdown','mermaid','canvas','image','code','model')",
    )?;
    let rows: Vec<(String, String, Option<String>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);

    let mut migrated = 0usize;
    for (id, old_kind, metadata_json) in rows {
        if TEXT_KINDS.contains(&old_kind.as_str()) {
            conn.execute(
                "UPDATE notebook_cells SET cell_type='step' WHERE id=?1",
                params![id],
            )?;
        } else {
            // Stash the original kind so the archive is reversible (no data loss).
            let mut meta: serde_json::Value = metadata_json
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            if !meta.is_object() {
                meta = serde_json::json!({});
            }
            meta["archived"] = serde_json::Value::Bool(true);
            meta["original_cell_type"] = serde_json::Value::String(old_kind.clone());
            conn.execute(
                "UPDATE notebook_cells SET cell_type='archived', metadata_json=?2 WHERE id=?1",
                params![id, meta.to_string()],
            )?;
        }
        migrated += 1;
    }

    if migrated > 0 {
        tracing::info!("Migration: collapsed {} legacy cells into the step model", migrated);
    }
    Ok(migrated)
}

// ═══════════════════════════════════════════════════════════════════════════
// SNAPSHOT HELPER FUNCTIONS (simple parameter versions)
// ═══════════════════════════════════════════════════════════════════════════

/// Insert a group by individual fields (for snapshot sync)
pub fn group_insert_simple(id: &str, name: &str, icon: &str, color: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO groups (id, name, icon, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, name, icon, color, now],
    )?;
    Ok(())
}

/// Insert a workspace by individual fields (for snapshot sync)
pub fn workspace_insert_simple(id: &str, group_id: &str, name: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO workspaces (id, group_id, name, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![id, group_id, name, now],
    )?;
    Ok(())
}

/// Insert a board by individual fields (for snapshot sync) - uses created_at from DTO
pub fn board_insert_simple(id: &str, workspace_id: &str, name: &str, created_at: i64) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, created_at) VALUES (?1, ?2, 'whiteboard', ?3, ?4)",
        params![id, workspace_id, name, created_at],
    )?;
    Ok(())
}

/// Insert an element by individual fields (for snapshot sync)
pub fn element_insert_simple(
    id: &str, board_id: &str, element_type: &str,
    x: f64, y: f64, width: f64, height: f64, z_index: i32,
    style_json: Option<&str>, content_json: Option<&str>,
    created_at: i64, updated_at: i64,
) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR REPLACE INTO whiteboard_elements (id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at],
    )?;
    Ok(())
}

/// Insert a cell by individual fields (for snapshot sync)
pub fn cell_insert_simple(
    id: &str, board_id: &str, cell_type: &str, cell_order: i32,
    content: Option<&str>, output: Option<&str>, collapsed: bool,
    height: Option<f64>, metadata_json: Option<&str>,
    created_at: i64, updated_at: i64,
) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR REPLACE INTO notebook_cells (id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![id, board_id, cell_type, cell_order, content, output, collapsed as i32, height, metadata_json, created_at, updated_at],
    )?;
    Ok(())
}

/// Insert a chat by individual fields (for snapshot sync). Carries `board_id` so a synced
/// chat lands on the right board thread (R11 §1). CHAT C1: anchors ride the snapshot too,
/// so a late-joining peer sees the same threads as everyone else.
#[allow(clippy::too_many_arguments)]
pub fn chat_insert_simple(
    id: &str, board_id: &str, workspace_id: &str, message: &str,
    author: &str, parent_id: Option<&str>, timestamp: i64,
    anchor_kind: Option<&str>, anchor_id: Option<&str>,
) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, board_id, workspace_id, type, name, hash, data, created_at, anchor_kind, anchor_id) VALUES (?1, ?2, ?3, 'chat', ?4, ?5, ?6, ?7, ?8, ?9)",
        params![id, board_id, workspace_id, message, author, parent_id.map(|s| s.as_bytes()), timestamp, anchor_kind, anchor_id],
    )?;
    Ok(())
}

/// Insert a file by individual fields (for snapshot sync)
pub fn file_insert_simple(
    id: &str, group_id: Option<&str>, workspace_id: Option<&str>, board_id: Option<&str>,
    name: &str, hash: &str, size: u64, source_peer: Option<&str>, created_at: i64,
) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, created_at) VALUES (?1, ?2, ?3, ?4, 'file', ?5, ?6, ?7, ?8, ?9)",
        params![id, group_id, workspace_id, board_id, name, hash, size as i64, source_peer, created_at],
    )?;
    Ok(())
}

/// Upsert board metadata as a **per-field convergent LWW merge** — never a whole-record
/// replace (R11 §9/§9b/PATTERN). Three independent lanes converge so concurrent edits from
/// different peers merge instead of clobbering:
///
/// - **descriptive** (labels/rating/contains_model/contains_skills/board_type) — applied only
///   when `meta_updated_at` is strictly newer (these move together via `UpdateBoardMetadata`);
/// - **pin** (`is_pinned`) — applied only when `pin_updated_at` is strictly newer, so a stale
///   snapshot row never un-pins a board another peer just pinned;
/// - **activity counters** (`view_count`, `last_accessed`) — merged with `MAX` (monotonic, so
///   they never decrease and need no clock).
///
/// Every lane is independent, so a snapshot re-applying the full board_metadata set is safe.
#[allow(clippy::too_many_arguments)]
pub fn board_metadata_upsert(
    board_id: &str,
    labels: &[String],
    rating: i32,
    view_count: i32,
    contains_model: Option<&str>,
    contains_skills: &[String],
    board_type: Option<&str>,
    last_accessed: i64,
    is_pinned: bool,
    meta_updated_at: i64,
    pin_updated_at: i64,
) -> Result<()> {
    let conn = db().lock_safe();
    let labels_json = serde_json::to_string(labels).unwrap_or_else(|_| "[]".to_string());
    let skills_json = serde_json::to_string(contains_skills).unwrap_or_else(|_| "[]".to_string());
    let board_type = board_type.unwrap_or("canvas");

    conn.execute(
        "INSERT INTO board_metadata
            (board_id, labels, rating, view_count, contains_model, contains_skills, board_type, last_accessed, is_pinned, meta_updated_at, pin_updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(board_id) DO UPDATE SET
            labels          = CASE WHEN excluded.meta_updated_at > board_metadata.meta_updated_at THEN excluded.labels          ELSE board_metadata.labels          END,
            rating          = CASE WHEN excluded.meta_updated_at > board_metadata.meta_updated_at THEN excluded.rating          ELSE board_metadata.rating          END,
            contains_model  = CASE WHEN excluded.meta_updated_at > board_metadata.meta_updated_at THEN excluded.contains_model  ELSE board_metadata.contains_model  END,
            contains_skills = CASE WHEN excluded.meta_updated_at > board_metadata.meta_updated_at THEN excluded.contains_skills ELSE board_metadata.contains_skills END,
            board_type      = CASE WHEN excluded.meta_updated_at > board_metadata.meta_updated_at THEN excluded.board_type      ELSE board_metadata.board_type      END,
            is_pinned       = CASE WHEN excluded.pin_updated_at  > board_metadata.pin_updated_at  THEN excluded.is_pinned       ELSE board_metadata.is_pinned       END,
            view_count      = MAX(excluded.view_count,    board_metadata.view_count),
            last_accessed   = MAX(excluded.last_accessed, board_metadata.last_accessed),
            meta_updated_at = MAX(excluded.meta_updated_at, board_metadata.meta_updated_at),
            pin_updated_at  = MAX(excluded.pin_updated_at,  board_metadata.pin_updated_at)",
        params![board_id, labels_json, rating, view_count, contains_model, skills_json, board_type, last_accessed, is_pinned as i32, meta_updated_at, pin_updated_at],
    )?;
    Ok(())
}

/// Insert an integration binding (for snapshot sync)
pub fn integration_insert(
    id: &str, scope_type: &str, scope_id: &str, integration_type: &str,
    config: &serde_json::Value, created_at: i64,
) -> Result<()> {
    let conn = db().lock_safe();
    let config_json = serde_json::to_string(config).unwrap_or_else(|_| "{}".to_string());
    conn.execute(
        "INSERT OR REPLACE INTO integration_bindings (id, scope_type, scope_id, integration_type, config_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, scope_type, scope_id, integration_type, config_json, created_at],
    )?;
    Ok(())
}

// ============================================================================
// Anonymous Sessions
// ============================================================================

pub fn anonymous_session_save(
    scope_id: &str,
    ephemeral_key: &str,
    ephemeral_secret: &str,
    commitment: &str,
    handle: &str,
) -> Result<()> {
    let conn = db().lock_safe();
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS anonymous_sessions (
            scope_id TEXT PRIMARY KEY,
            ephemeral_key TEXT NOT NULL,
            ephemeral_secret TEXT NOT NULL,
            commitment TEXT NOT NULL,
            handle TEXT NOT NULL,
            revealed INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO anonymous_sessions 
         (scope_id, ephemeral_key, ephemeral_secret, commitment, handle, revealed, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
        params![scope_id, ephemeral_key, ephemeral_secret, commitment, handle, now],
    )?;
    Ok(())
}

pub fn anonymous_session_get(scope_id: &str) -> Option<(String, String, String, String, bool)> {
    let conn = db().lock_safe();
    let _ = conn.execute(
        "CREATE TABLE IF NOT EXISTS anonymous_sessions (
            scope_id TEXT PRIMARY KEY,
            ephemeral_key TEXT NOT NULL,
            ephemeral_secret TEXT NOT NULL,
            commitment TEXT NOT NULL,
            handle TEXT NOT NULL,
            revealed INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        )",
        [],
    );
    conn.query_row(
        "SELECT ephemeral_key, ephemeral_secret, commitment, handle, revealed 
         FROM anonymous_sessions WHERE scope_id = ?1",
        params![scope_id],
        |row| Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i32>(4)? != 0,
        )),
    ).ok()
}

pub fn anonymous_session_reveal(scope_id: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "UPDATE anonymous_sessions SET revealed = 1 WHERE scope_id = ?1",
        params![scope_id],
    )?;
    Ok(())
}

pub fn anonymous_session_delete(scope_id: &str) -> Result<()> {
    let conn = db().lock_safe();
    conn.execute(
        "DELETE FROM anonymous_sessions WHERE scope_id = ?1",
        params![scope_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod open_db_tests {
    use super::*;

    /// Unique scratch dir under the OS temp dir; auto-removed on drop.
    fn scratch() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("cyan-open-db-")
            .tempdir()
            .expect("create temp dir")
    }

    #[test]
    fn open_db_creates_missing_parent_dir() {
        let root = scratch();
        // A nested path whose parent dirs do NOT exist yet — the run_multi repro.
        let db_path = root.path().join("nested").join("deeper").join("cyan.db");
        assert!(!db_path.parent().expect("has parent").exists());

        let conn = open_db(&db_path).expect("open should create parent and succeed");
        assert!(db_path.parent().expect("has parent").exists(), "parent dir created");
        // Connection is usable.
        conn.execute_batch("CREATE TABLE t (x INTEGER);")
            .expect("usable connection");
    }

    #[test]
    fn open_db_failure_returns_error_not_panic() {
        let root = scratch();
        // Make an ancestor a *file*, so create_dir_all of the parent must fail
        // (NotADirectory) — exercises the graceful error path with no panic.
        let blocker = root.path().join("iamafile");
        std::fs::write(&blocker, b"not a dir").expect("write blocker file");
        let db_path = blocker.join("sub").join("cyan.db");

        let err = open_db(&db_path);
        assert!(err.is_err(), "bad path must return Err, not panic");
    }

    #[test]
    fn same_datadir_reopens_same_db() {
        let root = scratch();
        let db_path = root.path().join("data").join("cyan.db");

        {
            let conn = open_db(&db_path).expect("first open");
            conn.execute_batch(
                "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT);
                 INSERT INTO kv (k, v) VALUES ('hello', 'world');",
            )
            .expect("write row");
        } // drop/close

        // Reopen the SAME resolved path — the row must still be there.
        let conn = open_db(&db_path).expect("reopen");
        let v: String = conn
            .query_row("SELECT v FROM kv WHERE k = 'hello'", [], |r| r.get(0))
            .expect("row persisted across reopen");
        assert_eq!(v, "world");
    }

    #[test]
    fn distinct_datadirs_are_isolated() {
        let a = scratch();
        let b = scratch();
        let db_a = a.path().join("cyan.db");
        let db_b = b.path().join("cyan.db");

        let conn_a = open_db(&db_a).expect("open a");
        conn_a
            .execute_batch("CREATE TABLE kv (k TEXT PRIMARY KEY); INSERT INTO kv VALUES ('a');")
            .expect("write a");

        let conn_b = open_db(&db_b).expect("open b");
        // b is a separate database file — table from a must not exist here.
        let count: i64 = conn_b
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='kv'",
                [],
                |r| r.get(0),
            )
            .expect("query b schema");
        assert_eq!(count, 0, "distinct data dirs share no tables");
    }

    #[test]
    fn resolve_db_path_honors_explicit_then_env() {
        // Explicit path wins verbatim (the FFI/app contract).
        assert_eq!(resolve_db_path("/tmp/x/cyan.db"), PathBuf::from("/tmp/x/cyan.db"));
        // Empty falls back to ./cyan.db when no env is set.
        // (We avoid mutating CYAN_DATA_DIR here to keep the test free of global env races.)
        let fallback = resolve_db_path("");
        assert_eq!(fallback.file_name().expect("has file name"), "cyan.db");
    }
}
