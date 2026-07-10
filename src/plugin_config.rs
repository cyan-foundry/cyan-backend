//! Per-install / per-workflow plugin CONFIG (PLUGIN_CREDENTIAL_ONBOARDING §A/§B).
//!
//! Non-secret plugin targets — `account_id`, `folder_id`, a C2C project — must be
//! configured per **workflow (board)** or per **tenant**, never as one global env
//! var: one operator serves many producers, each with their own Frame.io account
//! and destination. This table is the store the install/settings UX writes and
//! the deterministic bind reads (most-specific wins: board → tenant → the demo
//! env fallback, which stays only for the transition).
//!
//! SECRETS DO NOT BELONG HERE. Credentials go to the vault
//! (`device_vault::plugin_cred_vault`) and are injected as spawn env — see
//! `mcp_host::bundle_spawn_config`. The write API refuses secret-looking keys.

use anyhow::{Result, anyhow};
use rusqlite::Connection;

/// Board value meaning "tenant-wide default" (SQLite PKs can't hold NULL).
const TENANT_WIDE: &str = "";

/// Create the `plugin_config` table. Idempotent; additive.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS plugin_config (
            tenant_id  TEXT NOT NULL,
            board_id   TEXT NOT NULL DEFAULT '',
            plugin_id  TEXT NOT NULL,
            key        TEXT NOT NULL,
            value      TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, board_id, plugin_id, key)
        );",
    )?;
    Ok(())
}

/// Does `key` look like secret material? Refused here — secrets go to the vault.
fn looks_secret(key: &str) -> bool {
    let k = key.to_lowercase();
    ["token", "secret", "password", "passwd", "api_key", "apikey", "private"]
        .iter()
        .any(|s| k.contains(s))
}

/// Upsert one config value, scoped to a workflow (`Some(board)`) or the tenant
/// (`None`). Secret-looking keys are refused with a clear error.
pub fn set(
    conn: &Connection,
    tenant_id: &str,
    board_id: Option<&str>,
    plugin_id: &str,
    key: &str,
    value: &str,
    now_unix: i64,
) -> Result<()> {
    if looks_secret(key) {
        return Err(anyhow!(
            "'{key}' looks like a secret — plugin_config stores non-secret targets only; \
             credentials go to the vault (see PLUGIN_CREDENTIAL_ONBOARDING.md)"
        ));
    }
    conn.execute(
        "INSERT OR REPLACE INTO plugin_config
           (tenant_id, board_id, plugin_id, key, value, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            tenant_id,
            board_id.unwrap_or(TENANT_WIDE),
            plugin_id,
            key,
            value,
            now_unix
        ],
    )?;
    Ok(())
}

/// Resolve one config value: the WORKFLOW row wins, else the tenant-wide row.
pub fn get(
    conn: &Connection,
    tenant_id: &str,
    board_id: Option<&str>,
    plugin_id: &str,
    key: &str,
) -> Result<Option<String>> {
    let lookup = |board: &str| -> Result<Option<String>> {
        Ok(conn
            .query_row(
                "SELECT value FROM plugin_config
                 WHERE tenant_id=?1 AND board_id=?2 AND plugin_id=?3 AND key=?4",
                rusqlite::params![tenant_id, board, plugin_id, key],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .filter(|v| !v.trim().is_empty()))
    };
    if let Some(board) = board_id.filter(|b| !b.is_empty())
        && let Some(v) = lookup(board)?
    {
        return Ok(Some(v));
    }
    lookup(TENANT_WIDE)
}

/// All rows for a (tenant, board?, plugin) — what a settings sheet lists.
/// Workflow rows shadow tenant rows of the same key.
pub fn list(
    conn: &Connection,
    tenant_id: &str,
    board_id: Option<&str>,
    plugin_id: &str,
) -> Result<Vec<(String, String)>> {
    let mut out: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT board_id, key, value FROM plugin_config
         WHERE tenant_id=?1 AND plugin_id=?2 AND board_id IN (?3, '')
         ORDER BY board_id ASC", // '' first, so the board row overwrites it
    )?;
    let rows = stmt.query_map(
        rusqlite::params![tenant_id, plugin_id, board_id.unwrap_or(TENANT_WIDE)],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        },
    )?;
    for row in rows.flatten() {
        let (_board, key, value) = row;
        out.insert(key, value);
    }
    Ok(out.into_iter().collect())
}

/// The bind-time context resolver (PLUGIN_CREDENTIAL_ONBOARDING §B): a required
/// tool prop the author didn't inline resolves WORKFLOW config → TENANT config →
/// the plugin's ambient `<PLUGIN>_<PROP>` env (the demo stopgap, last). Takes the
/// already-held connection so callers under the storage lock can't self-deadlock.
pub fn config_context_value(
    conn: &Connection,
    tenant_id: &str,
    board_id: Option<&str>,
    plugin_id: &str,
    prop: &str,
) -> Option<String> {
    if let Ok(Some(v)) = get(conn, tenant_id, board_id, plugin_id, prop) {
        return Some(v);
    }
    crate::workflow_bind::env_context_value(plugin_id, prop)
}

/// [`config_context_value`] for call-sites that do NOT hold the storage lock
/// (the deterministic bind): takes the global lock briefly for the store read,
/// then falls back to the ambient env.
pub fn context_value(
    tenant_id: &str,
    board_id: Option<&str>,
    plugin_id: &str,
    prop: &str,
) -> Option<String> {
    // try_db: pre-init (or DB-free unit tests) degrades to the env fallback —
    // a config lookup must never panic across the FFI boundary.
    if let Some(db) = crate::storage::try_db()
        && let Ok(conn) = db.lock()
        && let Ok(Some(v)) = get(&conn, tenant_id, board_id, plugin_id, prop)
    {
        return Some(v);
    }
    crate::workflow_bind::env_context_value(plugin_id, prop)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        let c = Connection::open_in_memory().expect("in-memory db");
        migrate(&c).expect("migrate");
        c
    }

    #[test]
    fn workflow_row_wins_over_tenant_row_and_env_is_last() {
        let c = conn();
        set(&c, "acme", None, "frameio", "folder_id", "tenant-folder", 1).unwrap();
        set(&c, "acme", Some("board-1"), "frameio", "folder_id", "board-folder", 2).unwrap();

        // Workflow-scoped read: the board row shadows the tenant row.
        assert_eq!(
            get(&c, "acme", Some("board-1"), "frameio", "folder_id").unwrap().as_deref(),
            Some("board-folder")
        );
        // Another workflow (no row) falls back to the tenant default.
        assert_eq!(
            get(&c, "acme", Some("board-2"), "frameio", "folder_id").unwrap().as_deref(),
            Some("tenant-folder")
        );
        // Another tenant sees nothing — per-tenant isolation.
        assert_eq!(get(&c, "globex", Some("board-1"), "frameio", "folder_id").unwrap(), None);

        // config_context_value prefers store rows over the ambient env.
        unsafe { std::env::set_var("FRAMEIO_FOLDER_ID", "env-folder") };
        assert_eq!(
            config_context_value(&c, "acme", Some("board-1"), "frameio", "folder_id").as_deref(),
            Some("board-folder"),
            "a configured workflow never reads the global env stopgap"
        );
        assert_eq!(
            config_context_value(&c, "globex", None, "frameio", "folder_id").as_deref(),
            Some("env-folder"),
            "an unconfigured tenant keeps the demo env fallback during the transition"
        );
        unsafe { std::env::remove_var("FRAMEIO_FOLDER_ID") };
    }

    #[test]
    fn secret_looking_keys_are_refused() {
        let c = conn();
        for k in ["ims_token", "API_KEY", "client_secret", "password"] {
            let err = set(&c, "acme", None, "frameio", k, "x", 1).unwrap_err();
            assert!(err.to_string().contains("vault"), "{k}: {err}");
        }
        // The non-secret targets all store fine.
        for k in ["account_id", "folder_id", "project_id", "workspace_id"] {
            set(&c, "acme", None, "frameio", k, "v", 1).unwrap();
        }
    }

    #[test]
    fn list_merges_tenant_defaults_under_workflow_overrides() {
        let c = conn();
        set(&c, "acme", None, "frameio", "account_id", "acct-1", 1).unwrap();
        set(&c, "acme", None, "frameio", "folder_id", "tenant-folder", 1).unwrap();
        set(&c, "acme", Some("b1"), "frameio", "folder_id", "board-folder", 2).unwrap();

        let rows = list(&c, "acme", Some("b1"), "frameio").unwrap();
        assert_eq!(rows, vec![
            ("account_id".to_string(), "acct-1".to_string()),
            ("folder_id".to_string(), "board-folder".to_string()),
        ]);
    }
}
