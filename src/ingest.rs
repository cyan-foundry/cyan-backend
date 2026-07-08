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

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
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
// Sources.
// ============================================================================

/// Register a watched source on a board. `kind` is closed vocab
/// ([`INGEST_KIND_VOCAB`]); `schedule_secs` is the poll cadence (the Schedule
/// button) or `None` for manual-only.
pub fn source_add(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
    kind: &str,
    uri: &str,
    schedule_secs: Option<i64>,
) -> Result<IngestSource> {
    if tenant_id.trim().is_empty() {
        return Err(anyhow!("tenant_id required"));
    }
    if board_id.trim().is_empty() {
        return Err(anyhow!("board_id required"));
    }
    if !INGEST_KIND_VOCAB.contains(&kind) {
        return Err(anyhow!(
            "ingest kind '{}' not in closed vocab {:?}",
            kind,
            INGEST_KIND_VOCAB
        ));
    }
    if uri.trim().is_empty() {
        return Err(anyhow!("uri required"));
    }
    if let Some(s) = schedule_secs
        && s <= 0
    {
        return Err(anyhow!("schedule_secs must be positive (or absent for manual-only)"));
    }
    let source = IngestSource {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant_id.to_string(),
        board_id: board_id.to_string(),
        kind: kind.to_string(),
        uri: uri.to_string(),
        schedule_secs,
        last_scan_at: None,
        created_at: now(),
    };
    conn.execute(
        "INSERT INTO ingest_source (id, tenant_id, board_id, kind, uri, schedule_secs, last_scan_at, created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,NULL,?7)",
        params![
            source.id,
            source.tenant_id,
            source.board_id,
            source.kind,
            source.uri,
            source.schedule_secs,
            source.created_at
        ],
    )?;
    Ok(source)
}

fn row_to_source(row: &rusqlite::Row) -> rusqlite::Result<IngestSource> {
    Ok(IngestSource {
        id: row.get("id")?,
        tenant_id: row.get("tenant_id")?,
        board_id: row.get("board_id")?,
        kind: row.get("kind")?,
        uri: row.get("uri")?,
        schedule_secs: row.get("schedule_secs")?,
        last_scan_at: row.get("last_scan_at")?,
        created_at: row.get("created_at")?,
    })
}

/// Every source a tenant holds, oldest first.
pub fn source_list(conn: &Connection, tenant_id: &str) -> Result<Vec<IngestSource>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM ingest_source WHERE tenant_id=?1 ORDER BY created_at ASC, rowid ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_source)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// One source by id (the scan entrypoint's lookup). Errors if unknown.
pub fn source_get(conn: &Connection, source_id: &str) -> Result<IngestSource> {
    conn.query_row(
        "SELECT * FROM ingest_source WHERE id=?1",
        params![source_id],
        row_to_source,
    )
    .map_err(|e| anyhow!("ingest source {} not found: {}", source_id, e))
}

/// Remove a source (tenant-scoped). Already-ingested assets and their runs are
/// untouched — removing a sensor never deletes what it ingested.
pub fn source_remove(conn: &Connection, tenant_id: &str, source_id: &str) -> Result<()> {
    let n = conn.execute(
        "DELETE FROM ingest_source WHERE tenant_id=?1 AND id=?2",
        params![tenant_id, source_id],
    )?;
    if n == 0 {
        return Err(anyhow!("ingest source {} not found", source_id));
    }
    Ok(())
}

// ============================================================================
// Scan.
// ============================================================================

/// Scan one source now. `folder` is fully live; `s3` / `frameio_c2c` return
/// [`NotSupportedYet`]. On success `last_scan_at` advances (the scheduling
/// clock); a failed scan leaves it untouched so the next tick retries.
pub fn scan(conn: &Connection, source_id: &str) -> Result<ScanReport> {
    let source = source_get(conn, source_id)?;
    let report = match source.kind.as_str() {
        "folder" => scan_folder(conn, &source)?,
        kind @ ("s3" | "frameio_c2c") => {
            return Err(NotSupportedYet { kind: kind.to_string() }.into());
        }
        other => return Err(anyhow!("ingest kind '{}' not in closed vocab", other)),
    };
    conn.execute(
        "UPDATE ingest_source SET last_scan_at=?1 WHERE id=?2",
        params![now(), source.id],
    )?;
    Ok(report)
}

/// The live v1 transport: a non-recursive directory walk over the media
/// extensions, content-hashed with Blake3. A file is NEW iff its hash is in
/// neither the asset registry nor the board's objects rows; each NEW file is
/// registered (kind=master, class=clip, location=file://…), attached to the
/// board (objects row with its real local path — the bind seam), and
/// materialized as its own run.
fn scan_folder(conn: &Connection, source: &IngestSource) -> Result<ScanReport> {
    let dir = PathBuf::from(source.uri.strip_prefix("file://").unwrap_or(&source.uri));
    let entries = std::fs::read_dir(&dir)
        .map_err(|e| anyhow!("cannot read watched folder {}: {}", dir.display(), e))?;

    // Deterministic walk order (names), media extensions only, non-recursive.
    let mut media: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_file() && is_media(p))
        .collect();
    media.sort();

    // The board's workspace, so ingested rows carry the same linkage a
    // hand-attached file would.
    let workspace_id: Option<String> = conn
        .query_row(
            "SELECT workspace_id FROM objects WHERE id=?1 AND type='whiteboard'",
            params![source.board_id],
            |r| r.get(0),
        )
        .optional()?
        .flatten();

    let mut report = ScanReport { discovered: media.len(), ..Default::default() };
    for path in &media {
        let hash = blake3_file(path)?;

        // Content-identity dedup against BOTH prior ingests (asset registry)
        // and prior attachments (the objects hash seam).
        let known_asset =
            crate::asset_registry::get(conn, &source.tenant_id, &hash).is_ok();
        let known_object: bool = conn
            .query_row(
                "SELECT 1 FROM objects WHERE type='file' AND hash=?1 \
                 AND COALESCE(deleted,0)=0 AND COALESCE(board_id,'')=?2 LIMIT 1",
                params![hash, source.board_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if known_asset || known_object {
            report.deduped += 1;
            continue;
        }

        let abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.clone());
        let name = abs
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("ingested-file")
            .to_string();
        let size = std::fs::metadata(&abs).map(|m| m.len()).unwrap_or(0);

        // 1 — register the MASTER: Cyan references it (location), needn't hold it.
        crate::asset_registry::upsert(
            conn,
            &crate::asset_registry::Asset {
                hash: hash.clone(),
                tenant_id: source.tenant_id.clone(),
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
        )?;
        crate::asset_registry::set_class_location(
            conn,
            &source.tenant_id,
            &hash,
            Some("clip"),
            Some(&format!("file://{}", abs.display())),
        )?;

        // 2 — land the objects row on the source's board (the existing
        // insert/dedup seam), with the real local path so the run's explicit
        // bind resolves it.
        crate::storage::file_insert_conn(
            conn,
            &uuid::Uuid::new_v4().to_string(),
            Some(&source.tenant_id),
            workspace_id.as_deref(),
            Some(&source.board_id),
            &name,
            &hash,
            size,
            "ingest",
            abs.to_str(),
            now(),
        )?;

        // 3 — this asset gets ITS OWN pipeline run.
        materialize_run(conn, &source.board_id, &hash)?;
        report.ingested += 1;
    }
    Ok(report)
}

fn is_media(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| MEDIA_EXTENSIONS.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Blake3 over the file's bytes — content identity, never a filename.
fn blake3_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| anyhow!("cannot open {}: {}", path.display(), e))?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut file, &mut hasher)
        .map_err(|e| anyhow!("cannot hash {}: {}", path.display(), e))?;
    Ok(hasher.finalize().to_hex().to_string())
}

// ============================================================================
// Scheduling — polling, app-driven (no engine thread; see module docs).
// ============================================================================

/// Sources whose poll cadence has elapsed at `now`: `schedule_secs` set and
/// either never scanned or `now - last_scan_at >= schedule_secs`. Manual-only
/// sources (`schedule_secs = None`) are never due.
pub fn due_sources(conn: &Connection, now: i64) -> Result<Vec<IngestSource>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM ingest_source \
         WHERE schedule_secs IS NOT NULL \
           AND (last_scan_at IS NULL OR ?1 - last_scan_at >= schedule_secs) \
         ORDER BY created_at ASC, rowid ASC",
    )?;
    let rows = stmt
        .query_map(params![now], row_to_source)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Scan every due source (the app tick's one call). Per-source failures are
/// carried in the outcome, never thrown — one bad source must not stall the
/// sweep.
pub fn scan_due(conn: &Connection, now: i64) -> Result<Vec<ScanDueOutcome>> {
    let mut out = Vec::new();
    for source in due_sources(conn, now)? {
        match scan(conn, &source.id) {
            Ok(report) => out.push(ScanDueOutcome {
                source_id: source.id,
                report: Some(report),
                error: None,
            }),
            Err(e) => out.push(ScanDueOutcome {
                source_id: source.id,
                report: None,
                error: Some(e.to_string()),
            }),
        }
    }
    Ok(out)
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

// ============================================================================
// FFI command/event JSON dispatch — the `cyan_changelist_command` pattern.
// ============================================================================

/// Run an ingest command against the process-global DB. JSON in, JSON out.
/// Errors surface as `{ "error": "<msg>" }` — never a panic across the boundary.
///
/// Ops: `source_add`, `source_list`, `source_remove`, `scan_now`, `scan_due`,
/// `runs_for_board`, `produce_master_plan`.
pub fn command(json_str: &str) -> String {
    match dispatch(json_str) {
        Ok(v) => v.to_string(),
        Err(e) => serde_json::json!({ "error": e.to_string() }).to_string(),
    }
}

fn dispatch(json_str: &str) -> Result<serde_json::Value> {
    let cmd: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| anyhow!("bad command JSON: {}", e))?;
    let op = cmd
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'op'"))?;

    let lock = crate::storage::try_db()
        .ok_or_else(|| anyhow!("DB not initialized"))?
        .lock()
        .map_err(|e| anyhow!("DB lock: {}", e))?;
    let conn: &Connection = &lock;

    let s = |k: &str| -> Result<String> {
        cmd.get(k)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("missing '{}'", k))
    };

    match op {
        "source_add" => {
            let schedule_secs = cmd.get("schedule_secs").and_then(|v| v.as_i64());
            let out = source_add(
                conn,
                &s("tenant_id")?,
                &s("board_id")?,
                &s("kind")?,
                &s("uri")?,
                schedule_secs,
            )?;
            Ok(serde_json::to_value(out)?)
        }
        "source_list" => {
            let out = source_list(conn, &s("tenant_id")?)?;
            Ok(serde_json::to_value(out)?)
        }
        "source_remove" => {
            source_remove(conn, &s("tenant_id")?, &s("id")?)?;
            Ok(serde_json::json!({ "removed": true }))
        }
        "scan_now" => {
            let out = scan(conn, &s("source_id")?)?;
            Ok(serde_json::to_value(out)?)
        }
        "scan_due" => {
            let at = cmd.get("now").and_then(|v| v.as_i64()).unwrap_or_else(now);
            let out = scan_due(conn, at)?;
            Ok(serde_json::to_value(out)?)
        }
        "runs_for_board" => {
            let out = runs_for_board(conn, &s("board_id")?)?;
            Ok(serde_json::to_value(out)?)
        }
        // "Produce master" leg 1: the SELECTIVE retrieve list for a frozen
        // version — (asset, location) for exactly the masters the cut uses.
        "produce_master_plan" => {
            let masters = crate::asset_registry::resolve_final_cut_masters(
                conn,
                &s("tenant_id")?,
                &s("version_id")?,
            )?;
            let out: Vec<serde_json::Value> = masters
                .into_iter()
                .map(|(asset, location)| serde_json::json!({ "asset": asset, "location": location }))
                .collect();
            Ok(serde_json::json!({ "masters": out }))
        }
        other => Err(anyhow!("unknown op '{}'", other)),
    }
}
