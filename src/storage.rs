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
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                tracing::error!(
                    "Failed to create data dir {}: {} (os error)",
                    parent.display(),
                    e
                );
                anyhow!("create data dir {}: {e}", parent.display())
            })?;
        }
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

// ═══════════════════════════════════════════════════════════════════════════
// GROUPS
// ═══════════════════════════════════════════════════════════════════════════

pub fn group_insert(g: &Group) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO groups (id, name, icon, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![g.id, g.name, g.icon, g.color, g.created_at],
    )?;
    Ok(())
}

pub fn group_rename(id: &str, name: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("UPDATE groups SET name=?1 WHERE id=?2", params![name, id])?;
    Ok(())
}

pub fn group_delete(id: &str) -> Result<bool> {
    let conn = db().lock().unwrap();

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
    let conn = db().lock().unwrap();
    let mut stmt = conn.prepare("SELECT id FROM groups").unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut out = HashSet::new();
    while let Some(r) = rows.next().unwrap() {
        out.insert(r.get::<_, String>(0).unwrap());
    }
    out
}

// ═══════════════════════════════════════════════════════════════════════════
// OWNERSHIP HELPERS
// ═══════════════════════════════════════════════════════════════════════════

/// Check if node owns this group
pub fn group_is_owner(group_id: &str, node_id: &str) -> bool {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT owner_node_id FROM groups WHERE id = ?1",
        params![group_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten().as_deref() == Some(node_id)
}

/// Check if node owns this workspace
pub fn workspace_is_owner(workspace_id: &str, node_id: &str) -> bool {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT owner_node_id FROM workspaces WHERE id = ?1",
        params![workspace_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten().as_deref() == Some(node_id)
}

/// Check if node owns this board
pub fn board_is_owner(board_id: &str, node_id: &str) -> bool {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT owner_node_id FROM objects WHERE id = ?1 AND type = 'whiteboard'",
        params![board_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten().as_deref() == Some(node_id)
}

/// Get owner_node_id of a group
pub fn group_get_owner(group_id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT owner_node_id FROM groups WHERE id = ?1",
        params![group_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten()
}

/// Get owner_node_id of a workspace
pub fn workspace_get_owner(workspace_id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT owner_node_id FROM workspaces WHERE id = ?1",
        params![workspace_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten()
}

/// Get owner_node_id of a board
pub fn board_get_owner(board_id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT owner_node_id FROM objects WHERE id = ?1 AND type = 'whiteboard'",
        params![board_id],
        |r| r.get::<_, Option<String>>(0)
    ).ok().flatten()
}

pub fn group_get(id: &str) -> Result<Option<Group>> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
        let conn = db().lock().unwrap();
        for ws in [&default, &plugins] {
            conn.execute(
                "INSERT OR IGNORE INTO workspaces (id, group_id, name, created_at, is_system, owner_node_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![ws.id, ws.group_id, ws.name, ws.created_at, ws.system as i32, owner_node_id],
            )?;
        }
    }
    Ok((default, plugins))
}

pub fn workspace_insert(ws: &Workspace) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO workspaces (id, group_id, name, created_at, is_system) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![ws.id, ws.group_id, ws.name, ws.created_at, ws.system as i32],
    )?;
    Ok(())
}

pub fn workspace_rename(id: &str, name: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("UPDATE workspaces SET name=?1 WHERE id=?2", params![name, id])?;
    Ok(())
}

/// ROUND8 §W3: is this a system (non-deletable) workspace — the per-group "Plugins"
/// workspace? Returns false for unknown ids.
pub fn workspace_is_system(id: &str) -> bool {
    let conn = db().lock().unwrap();
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

    let conn = db().lock().unwrap();

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
    let conn = db().lock().unwrap();
    let mut stmt = conn.prepare("SELECT group_id FROM workspaces WHERE id=?1 LIMIT 1").ok()?;
    stmt.query_row(params![workspace_id], |r| r.get(0)).optional().ok()?
}

/// Get group_id for a board (via its workspace)
pub fn board_get_group_id(board_id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    // Board -> workspace_id -> group_id
    let mut stmt = conn.prepare(
        "SELECT w.group_id FROM workspaces w
         INNER JOIN objects o ON o.workspace_id = w.id
         WHERE o.id = ?1 AND o.type = 'whiteboard' LIMIT 1"
    ).ok()?;
    stmt.query_row(params![board_id], |r| r.get(0)).optional().ok()?
}

pub fn workspace_list_by_group(group_id: &str) -> Result<Vec<Workspace>> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, created_at) VALUES (?1, ?2, 'whiteboard', ?3, ?4)",
        params![id, workspace_id, name, created_at],
    )?;
    Ok(())
}

pub fn board_rename(id: &str, name: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("UPDATE objects SET name=?1 WHERE id=?2 AND type='whiteboard'", params![name, id])?;
    Ok(())
}

pub fn board_delete(id: &str) -> Result<()> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute("UPDATE objects SET board_mode=?1 WHERE id=?2", params![mode, board_id])?;
    Ok(())
}

pub fn board_get_workspace_id(board_id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    let mut stmt = conn.prepare("SELECT workspace_id FROM objects WHERE id=?1 AND type='whiteboard' LIMIT 1").ok()?;
    stmt.query_row(params![board_id], |r| r.get(0)).optional().ok()?
}

pub fn board_list_by_workspaces(workspace_ids: &[String]) -> Result<Vec<WhiteboardDTO>> {
    if workspace_ids.is_empty() { return Ok(vec![]); }
    let conn = db().lock().unwrap();
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

pub fn chat_insert(id: &str, workspace_id: &str, message: &str, author: &str, parent_id: Option<&str>, timestamp: i64) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, hash, data, created_at) VALUES (?1, ?2, 'chat', ?3, ?4, ?5, ?6)",
        params![id, workspace_id, message, author, parent_id.map(|s| s.as_bytes()), timestamp],
    )?;
    Ok(())
}

pub fn chat_delete(id: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("DELETE FROM objects WHERE id=?1 AND type='chat'", params![id])?;
    Ok(())
}

pub fn chat_get_workspace_id(chat_id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    let mut stmt = conn.prepare("SELECT workspace_id FROM objects WHERE id=?1 AND type='chat' LIMIT 1").ok()?;
    stmt.query_row(params![chat_id], |r| r.get(0)).optional().ok()?
}

/// List chats by workspace IDs (for snapshot)
pub fn chat_list_by_workspaces(workspace_ids: &[String]) -> Result<Vec<ChatDTO>> {
    if workspace_ids.is_empty() { return Ok(vec![]); }

    let conn = db().lock().unwrap();
    let placeholders: String = workspace_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, workspace_id, name, hash, data, created_at
         FROM objects WHERE type = 'chat' AND workspace_id IN ({}) ORDER BY created_at",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> = workspace_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

    let rows = stmt.query_map(params.as_slice(), |r| {
        let parent_bytes: Option<Vec<u8>> = r.get(4)?;
        let parent_id = parent_bytes.and_then(|b| String::from_utf8(b).ok());
        Ok(ChatDTO {
            id: r.get(0)?,
            workspace_id: r.get(1)?,
            message: r.get(2)?,
            author: r.get(3)?,
            parent_id,
            timestamp: r.get(5)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Get chats for a single workspace
pub fn chat_list_by_workspace(workspace_id: &str) -> Result<Vec<ChatDTO>> {
    let conn = db().lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT id, workspace_id, name, hash, data, created_at
         FROM objects WHERE type = 'chat' AND workspace_id = ?1 ORDER BY created_at"
    )?;

    let rows = stmt.query_map(params![workspace_id], |r| {
        let parent_bytes: Option<Vec<u8>> = r.get(4)?;
        let parent_id = parent_bytes.and_then(|b| String::from_utf8(b).ok());
        Ok(ChatDTO {
            id: r.get(0)?,
            workspace_id: r.get(1)?,
            message: r.get(2)?,
            author: r.get(3)?,
            parent_id,
            timestamp: r.get(5)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
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
        "INSERT INTO notes (id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(id) DO UPDATE SET
            board_id    = excluded.board_id,
            tenant_id   = excluded.tenant_id,
            author_id   = excluded.author_id,
            author_name = excluded.author_name,
            text        = excluded.text,
            updated_at  = excluded.updated_at
         WHERE excluded.updated_at > notes.updated_at",
        params![
            n.id, n.board_id, n.tenant_id, n.author_id, n.author_name, n.text,
            n.created_at, n.updated_at
        ],
    )?;
    Ok(changed > 0)
}

/// List a board's notes, **tenant-scoped** — a note never crosses the tenant boundary
/// even when the board id is known. Ordered by creation time.
pub fn note_list_by_board(board_id: &str, tenant_id: &str) -> Result<Vec<NoteDTO>> {
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let mut stmt = conn.prepare(
        "SELECT id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at
         FROM notes WHERE board_id = ?1 AND tenant_id = ?2 ORDER BY created_at",
    )?;
    let rows = stmt.query_map(params![board_id, tenant_id], note_from_row)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// List all notes attached to the given boards (for the digest + snapshot serializer).
/// A group is a single tenant, so this is naturally tenant-scoped by the board set.
pub fn note_list_by_boards(board_ids: &[String]) -> Result<Vec<NoteDTO>> {
    if board_ids.is_empty() {
        return Ok(vec![]);
    }
    let conn = db().lock().map_err(|e| anyhow::anyhow!("DB lock: {}", e))?;
    let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at
         FROM notes WHERE board_id IN ({}) ORDER BY created_at",
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
        "SELECT id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at
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
// FILES
// ═══════════════════════════════════════════════════════════════════════════

pub fn file_insert(
    id: &str, group_id: Option<&str>, workspace_id: Option<&str>, board_id: Option<&str>,
    name: &str, hash: &str, size: u64, source_peer: &str, created_at: i64,
) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, created_at) VALUES (?1, ?2, ?3, ?4, 'file', ?5, ?6, ?7, ?8, ?9)",
        params![id, group_id, workspace_id, board_id, name, hash, size as i64, source_peer, created_at],
    )?;
    Ok(())
}

pub fn file_set_local_path(id: &str, local_path: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("UPDATE objects SET local_path=?1 WHERE id=?2 AND type='file'", params![local_path, id])?;
    Ok(())
}

pub fn file_get_for_transfer(id: &str, hash: &str) -> Option<(String, String, u64)> {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT name, local_path, size FROM objects WHERE id=?1 AND type='file' AND hash=?2",
        params![id, hash],
        |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? as u64)),
    ).optional().ok()?
}

pub fn file_get_local_path(id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    let mut stmt = conn.prepare("SELECT local_path FROM objects WHERE id=?1 AND type='file'").ok()?;
    stmt.query_row(params![id], |r| r.get(0)).optional().ok()?
}

/// Get the group_id for a file (for routing file downloads to correct TopicActor)
pub fn file_get_group_id(file_id: &str) -> Option<String> {
    let conn = db().lock().unwrap();
    conn.query_row(
        "SELECT group_id FROM objects WHERE id = ?1 AND type = 'file'",
        params![file_id],
        |r| r.get(0),
    ).optional().ok()?
}

/// List files by group (for snapshot)
pub fn file_list_by_group(group_id: &str) -> Result<Vec<FileDTO>> {
    let conn = db().lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at
         FROM objects WHERE type = 'file' AND group_id = ?1 ORDER BY name"
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
    let conn = db().lock().unwrap();
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

// ═══════════════════════════════════════════════════════════════════════════
// FILE TRANSFERS (resumable download state)
// ═══════════════════════════════════════════════════════════════════════════

pub fn transfer_upsert(
    file_id: &str, file_name: &str, total_size: u64, hash: &str,
    bytes_received: u64, temp_path: &str, source_peer: &str, status: &str,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO file_transfers (file_id, file_name, total_size, hash, bytes_received, temp_path, source_peer, status, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
        params![file_id, file_name, total_size as i64, hash, bytes_received as i64, temp_path, source_peer, status, now],
    )?;
    Ok(())
}

pub fn transfer_update_progress(file_id: &str, bytes_received: u64) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock().unwrap();
    conn.execute(
        "UPDATE file_transfers SET bytes_received=?1, updated_at=?2 WHERE file_id=?3",
        params![bytes_received as i64, now, file_id],
    )?;
    Ok(())
}

pub fn transfer_set_status(file_id: &str, status: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock().unwrap();
    conn.execute(
        "UPDATE file_transfers SET status=?1, updated_at=?2 WHERE file_id=?3",
        params![status, now, file_id],
    )?;
    Ok(())
}

pub fn transfer_list_pending() -> Result<Vec<(String, String, String, u64)>> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO whiteboard_elements (id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![e.id, e.board_id, e.element_type, e.x, e.y, e.width, e.height, e.z_index, e.style_json, e.content_json, e.created_at, e.updated_at],
    )?;
    Ok(())
}

pub fn element_update(e: &WhiteboardElementDTO) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "UPDATE whiteboard_elements SET board_id=?2, element_type=?3, x=?4, y=?5, width=?6, height=?7, z_index=?8, style_json=?9, content_json=?10, updated_at=?11 WHERE id=?1",
        params![e.id, e.board_id, e.element_type, e.x, e.y, e.width, e.height, e.z_index, e.style_json, e.content_json, e.updated_at],
    )?;
    Ok(())
}

pub fn element_delete(id: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("DELETE FROM whiteboard_elements WHERE id=?1", params![id])?;
    Ok(())
}

pub fn element_clear_board(board_id: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("DELETE FROM whiteboard_elements WHERE board_id=?1", params![board_id])?;
    Ok(())
}

/// List elements by board IDs (for snapshot)
pub fn element_list_by_boards(board_ids: &[String]) -> Result<Vec<WhiteboardElementDTO>> {
    if board_ids.is_empty() { return Ok(vec![]); }

    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO notebook_cells (id, board_id, cell_type, cell_order, content, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![id, board_id, cell_type, cell_order, content, now, now],
    )?;
    Ok(())
}

pub fn cell_update(c: &NotebookCellDTO) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock().unwrap();
    conn.execute(
        "UPDATE notebook_cells SET cell_type=?2, cell_order=?3, content=?4, output=?5, collapsed=?6, height=?7, metadata_json=?8, updated_at=?9 WHERE id=?1",
        params![c.id, c.cell_type, c.cell_order, c.content, c.output, c.collapsed as i32, c.height, c.metadata_json, now],
    )?;
    Ok(())
}

pub fn cell_delete(id: &str) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute("DELETE FROM notebook_cells WHERE id=?1", params![id])?;
    Ok(())
}

pub fn cell_reorder(board_id: &str, cell_ids: &[String]) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock().unwrap();
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

    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT INTO board_metadata (board_id, labels, rating, contains_model, contains_skills) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(board_id) DO UPDATE SET labels=?2, rating=?3, contains_model=?4, contains_skills=?5",
        params![board_id, labels_json, rating, contains_model, skills_json],
    )?;
    Ok(())
}

pub fn board_meta_update_labels(board_id: &str, labels: &[String]) -> Result<()> {
    let labels_json = serde_json::to_string(labels)?;
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT INTO board_metadata (board_id, labels) VALUES (?1, ?2) ON CONFLICT(board_id) DO UPDATE SET labels=?2",
        params![board_id, labels_json],
    )?;
    Ok(())
}

pub fn board_meta_update_rating(board_id: &str, rating: i32) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT INTO board_metadata (board_id, rating) VALUES (?1, ?2) ON CONFLICT(board_id) DO UPDATE SET rating=?2",
        params![board_id, rating],
    )?;
    Ok(())
}

/// List board metadata by board IDs (for snapshot)
pub fn board_metadata_list_by_boards(board_ids: &[String]) -> Result<Vec<BoardMetadataDTO>> {
    if board_ids.is_empty() { return Ok(vec![]); }

    let conn = db().lock().unwrap();
    let placeholders: String = board_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT board_id, labels, rating, view_count, contains_model, contains_skills, board_type, last_accessed, COALESCE(is_pinned, 0)
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
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ═══════════════════════════════════════════════════════════════════════════
// USER PROFILES
// ═══════════════════════════════════════════════════════════════════════════

pub fn profile_upsert(node_id: &str, display_name: &str, avatar_hash: Option<&str>) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT INTO user_profiles (node_id, display_name, avatar_hash, status, updated_at) VALUES (?1, ?2, ?3, 'online', ?4) ON CONFLICT(node_id) DO UPDATE SET display_name=excluded.display_name, avatar_hash=COALESCE(excluded.avatar_hash, user_profiles.avatar_hash), status='online', updated_at=excluded.updated_at",
        params![node_id, display_name, avatar_hash, now],
    )?;
    Ok(())
}

/// Get profile by node_id: (display_name, avatar_hash)
pub fn profile_get(node_id: &str) -> Option<(String, Option<String>)> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
// DIRECT MESSAGES
// ═══════════════════════════════════════════════════════════════════════════

pub fn dm_insert(id: &str, peer_id: &str, message: &str, timestamp: i64, is_incoming: bool) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO direct_messages (id, peer_id, message, timestamp, is_incoming) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, peer_id, message, timestamp, is_incoming],
    )?;
    Ok(())
}

/// List DM history with a peer
pub fn dm_list_by_peer(peer_id: &str, limit: usize) -> Result<Vec<(String, String, i64, bool)>> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();

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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute_batch("BEGIN TRANSACTION")?;

    for c in chats {
        conn.execute(
            "INSERT OR REPLACE INTO objects (id, workspace_id, type, name, hash, data, created_at) VALUES (?1, ?2, 'chat', ?3, ?4, ?5, ?6)",
            params![c.id, c.workspace_id, c.message, c.author, c.parent_id.as_ref().map(|s| s.as_bytes()), c.timestamp],
        )?;
    }

    conn.execute_batch("COMMIT")?;
    Ok(())
}

/// Batch insert board metadata (for snapshot)
pub fn snapshot_insert_metadata(metadata: &[BoardMetadataDTO]) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute_batch("BEGIN TRANSACTION")?;

    for m in metadata {
        let labels_json = serde_json::to_string(&m.labels)?;
        let skills_json = serde_json::to_string(&m.contains_skills)?;
        conn.execute(
            "INSERT OR REPLACE INTO board_metadata (board_id, labels, rating, view_count, contains_model, contains_skills, board_type, last_accessed, is_pinned) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![m.board_id, labels_json, m.rating, m.view_count, m.contains_model, skills_json, m.board_type, m.last_accessed, m.is_pinned as i32],
        )?;
    }

    conn.execute_batch("COMMIT")?;
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
                updated_at INTEGER NOT NULL
            )",
            [],
        );
        let _ = conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_board ON notes(board_id)", []);
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
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO groups (id, name, icon, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, name, icon, color, now],
    )?;
    Ok(())
}

/// Insert a workspace by individual fields (for snapshot sync)
pub fn workspace_insert_simple(id: &str, group_id: &str, name: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO workspaces (id, group_id, name, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![id, group_id, name, now],
    )?;
    Ok(())
}

/// Insert a board by individual fields (for snapshot sync) - uses created_at from DTO
pub fn board_insert_simple(id: &str, workspace_id: &str, name: &str, created_at: i64) -> Result<()> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO notebook_cells (id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![id, board_id, cell_type, cell_order, content, output, collapsed as i32, height, metadata_json, created_at, updated_at],
    )?;
    Ok(())
}

/// Insert a chat by individual fields (for snapshot sync)
pub fn chat_insert_simple(
    id: &str, workspace_id: &str, message: &str,
    author: &str, parent_id: Option<&str>, timestamp: i64,
) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, workspace_id, type, name, hash, data, created_at) VALUES (?1, ?2, 'chat', ?3, ?4, ?5, ?6)",
        params![id, workspace_id, message, author, parent_id.map(|s| s.as_bytes()), timestamp],
    )?;
    Ok(())
}

/// Insert a file by individual fields (for snapshot sync)
pub fn file_insert_simple(
    id: &str, group_id: Option<&str>, workspace_id: Option<&str>, board_id: Option<&str>,
    name: &str, hash: &str, size: u64, source_peer: Option<&str>, created_at: i64,
) -> Result<()> {
    let conn = db().lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, created_at) VALUES (?1, ?2, ?3, ?4, 'file', ?5, ?6, ?7, ?8, ?9)",
        params![id, group_id, workspace_id, board_id, name, hash, size as i64, source_peer, created_at],
    )?;
    Ok(())
}

/// Upsert board metadata (for snapshot sync and updates)
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
) -> Result<()> {
    let conn = db().lock().unwrap();
    let labels_json = serde_json::to_string(labels).unwrap_or_else(|_| "[]".to_string());
    let skills_json = serde_json::to_string(contains_skills).unwrap_or_else(|_| "[]".to_string());
    let board_type = board_type.unwrap_or("canvas");

    conn.execute(
        "INSERT INTO board_metadata (board_id, labels, rating, view_count, contains_model, contains_skills, board_type, last_accessed, is_pinned)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(board_id) DO UPDATE SET
            labels = ?2, rating = ?3, view_count = ?4, contains_model = ?5,
            contains_skills = ?6, board_type = ?7, last_accessed = ?8, is_pinned = ?9",
        params![board_id, labels_json, rating, view_count, contains_model, skills_json, board_type, last_accessed, is_pinned as i32],
    )?;
    Ok(())
}

/// Insert an integration binding (for snapshot sync)
pub fn integration_insert(
    id: &str, scope_type: &str, scope_id: &str, integration_type: &str,
    config: &serde_json::Value, created_at: i64,
) -> Result<()> {
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
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
    let conn = db().lock().unwrap();
    conn.execute(
        "UPDATE anonymous_sessions SET revealed = 1 WHERE scope_id = ?1",
        params![scope_id],
    )?;
    Ok(())
}

pub fn anonymous_session_delete(scope_id: &str) -> Result<()> {
    let conn = db().lock().unwrap();
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
