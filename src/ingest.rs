//! STAGE 4 — ingest sources + per-asset pipeline materialization
//! (AUTHORING_FIXES_ROUND2 §STAGE 4).
//!
//! **WORKFLOW = ASSET CLASS.** The workflow authored on a board stays a
//! TEMPLATE; it is never welded to one `#file`. Each incoming asset
//! **materializes its own run** (a `workflow_run` row keyed by `run_id`, not by
//! board), and the run's asset binding is REAL: the ingested file lands as an
//! `objects` row on the source's board, and the run binds it EXPLICITLY through
//! `workflow_bind::bind_step_for_asset` — never "whichever file the board has".
//!
//! **INGEST SOURCES (source sensors).** "Point at a source" replaces "attach
//! 1–10 files": an `ingest_source` row names a `folder` / `s3` / `frameio_c2c`
//! location to watch. The `folder` transport is FULLY LIVE in v1 (non-recursive
//! directory scan, Blake3 content hash, dedup, register + materialize). The
//! `s3` / `frameio_c2c` kinds register and validate today but their `scan`
//! honestly returns [`NotSupportedYet`] — the connector seam exists, the
//! transports are follow-ups.
//!
//! **CADENCE — polling only, by design.** v1 cadence is manual "scan now" plus
//! scheduled polling: [`due_sources`] / [`scan_due`] are pure functions the
//! app/engine tick calls (the iOS Schedule button stores `schedule_secs` via
//! `source_add`; an app timer calls the `scan_due` FFI verb). There is NO
//! background thread in the engine — the app drives cadence, which keeps the
//! engine simple, testable, and battery-honest. fs-watch/event/webhook delivery
//! is the follow-up seam (deliberately skipped: it would pull a new heavy dep
//! and a thread the app can't pace).
//!
//! **DEDUP is content identity.** A file is NEW iff its Blake3 hash is unknown
//! to BOTH the asset registry (prior ingests) and the board's `objects` rows
//! (prior attachments) — the same rail `storage::file_insert` rides. Re-scans
//! are no-ops; a content EDIT is a new hash ⇒ a new asset ⇒ a new run.
//!
//! Design seam mirrors `changelist` / `asset_registry`: every op takes an
//! explicit `&Connection` so tests run on isolated DBs, while the FFI
//! (`cyan_ingest_command`) drives the process-global `storage::db()` through
//! the JSON dispatch in [`command`].

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

// ============================================================================
// Closed vocabs.
// ============================================================================

/// Source kinds. `folder` is the live v1 transport; `s3` / `frameio_c2c`
/// register but scanning returns [`NotSupportedYet`].
pub const INGEST_KIND_VOCAB: [&str; 3] = ["folder", "s3", "frameio_c2c"];

/// Media extensions the folder scan ingests (v1, lowercase-compared).
pub const MEDIA_EXTENSIONS: [&str; 5] = ["mp4", "mov", "mxf", "wav", "aif"];

/// Run lifecycle states stamped on `workflow_run.status`. v1 mints
/// `materialized`; the executor advances the rest (follow-up wiring).
pub const RUN_STATUS_VOCAB: [&str; 4] = ["materialized", "running", "done", "failed"];

/// The typed "this transport is a seam, not a lie" error: scanning an `s3` /
/// `frameio_c2c` source returns this until the transports land. Callers can
/// `err.downcast_ref::<NotSupportedYet>()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotSupportedYet {
    pub kind: String,
}

impl std::fmt::Display for NotSupportedYet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ingest kind '{}' is registered but scanning it is not supported yet (folder is the live v1 transport)",
            self.kind
        )
    }
}

impl std::error::Error for NotSupportedYet {}

// ============================================================================
// Rows.
// ============================================================================

/// One watched source: a board's sensor pointed at a folder / bucket / C2C
/// project. `schedule_secs = None` means manual-only ("scan now").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestSource {
    pub id: String,
    /// Tenant boundary (the group id) — every query carries it.
    pub tenant_id: String,
    /// The board whose workflow TEMPLATE each ingested asset materializes.
    pub board_id: String,
    /// folder | s3 | frameio_c2c ([`INGEST_KIND_VOCAB`]).
    pub kind: String,
    /// The watched location: a directory path or `file://` URI for `folder`;
    /// `s3://bucket/prefix` or a C2C project ref for the seam kinds.
    pub uri: String,
    /// Poll cadence in seconds (the Schedule button); `None` = manual only.
    #[serde(default)]
    pub schedule_secs: Option<i64>,
    /// Unix seconds of the last SUCCESSFUL scan; `None` = never scanned.
    #[serde(default)]
    pub last_scan_at: Option<i64>,
    pub created_at: i64,
}

/// One materialized per-asset run of a board's workflow template.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaterializedRun {
    pub run_id: String,
    pub board_id: String,
    /// The SPECIFIC asset this run processes — the explicit bind target
    /// (`workflow_bind::bind_step_for_asset`), never "the board's file".
    pub asset_hash: String,
    /// materialized | running | done | failed ([`RUN_STATUS_VOCAB`]).
    pub status: String,
    pub created_at: i64,
}

/// What one scan did. `discovered` counts candidate media files seen;
/// `ingested` the NEW ones (asset registered + objects row + run materialized);
/// `deduped` the already-known content skipped.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ScanReport {
    pub discovered: usize,
    pub ingested: usize,
    pub deduped: usize,
}

/// One source's outcome from a [`scan_due`] sweep — errors are carried, not
/// thrown, so one bad source never blocks the rest of the tick.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanDueOutcome {
    pub source_id: String,
    #[serde(default)]
    pub report: Option<ScanReport>,
    #[serde(default)]
    pub error: Option<String>,
}

// ============================================================================
// Migration.
// ============================================================================

/// Create `ingest_source` + `workflow_run`. Idempotent; called from
/// `storage::run_migrations` (alongside `changelist::migrate`) and directly
/// from tests.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS ingest_source (
            id            TEXT PRIMARY KEY,
            tenant_id     TEXT NOT NULL,
            board_id      TEXT NOT NULL,
            kind          TEXT NOT NULL,
            uri           TEXT NOT NULL,
            schedule_secs INTEGER,
            last_scan_at  INTEGER,
            created_at    INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ingest_source_tenant
            ON ingest_source(tenant_id);
        CREATE TABLE IF NOT EXISTS workflow_run (
            run_id     TEXT PRIMARY KEY,
            tenant_id  TEXT NOT NULL,
            board_id   TEXT NOT NULL,
            asset_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            status     TEXT NOT NULL DEFAULT 'materialized'
        );
        CREATE INDEX IF NOT EXISTS idx_workflow_run_board
            ON workflow_run(board_id, created_at);
        "#,
    )?;
    Ok(())
}

// ============================================================================
// Per-asset pipeline materialization.
// ============================================================================

/// Mint a run row for `(board, asset)`: the board's workflow template stays
/// untouched; this asset gets ITS OWN pipeline instance. Dedup lives in the
/// scan (content identity) — calling this twice deliberately mints two runs
/// (a re-review round is a legitimate second run of the same asset).
pub fn materialize_run(
    conn: &Connection,
    board_id: &str,
    asset_hash: &str,
) -> Result<MaterializedRun> {
    if board_id.trim().is_empty() {
        return Err(anyhow!("board_id required"));
    }
    if asset_hash.trim().is_empty() {
        return Err(anyhow!("asset_hash required"));
    }
    let tenant_id = crate::storage::board_get_group_id_with(conn, board_id)
        .filter(|g| !g.is_empty())
        .unwrap_or_else(|| "device".to_string());
    let run = MaterializedRun {
        run_id: uuid::Uuid::new_v4().to_string(),
        board_id: board_id.to_string(),
        asset_hash: asset_hash.to_string(),
        status: "materialized".to_string(),
        created_at: now(),
    };
    conn.execute(
        "INSERT INTO workflow_run (run_id, tenant_id, board_id, asset_hash, created_at, status) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        params![run.run_id, tenant_id, run.board_id, run.asset_hash, run.created_at, run.status],
    )?;
    Ok(run)
}

/// Every run materialized on a board, oldest first (rowid breaks same-second
/// ties in insertion order).
pub fn runs_for_board(conn: &Connection, board_id: &str) -> Result<Vec<MaterializedRun>> {
    let mut stmt = conn.prepare(
        "SELECT run_id, board_id, asset_hash, status, created_at FROM workflow_run \
         WHERE board_id=?1 ORDER BY created_at ASC, rowid ASC",
    )?;
    let rows = stmt
        .query_map(params![board_id], |r| {
            Ok(MaterializedRun {
                run_id: r.get(0)?,
                board_id: r.get(1)?,
                asset_hash: r.get(2)?,
                status: r.get(3)?,
                created_at: r.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
