// cyan-backend/src/asset_registry.rs
//
// Asset registry — one row per content-addressed media asset (the "NEW to build"
// table in CYAN_FORMAT_QA / CYAN_FORMAT_SPEC). The ledger (`changelist`) anchors
// every entry to an `asset_hash`; this table is where that hash resolves to what
// the asset IS:
//
//   * frame math      — kind / fps / duration_ms (timecode is frames; fps lives here)
//   * derivation edge — the asset this one was MADE FROM (proxy/deliverable →
//                       {parent master, ledger version}); a NEW master (reshoot,
//                       recut base) is a new asset_hash = a new ledger, linked here
//                       by derivation — never a branch.
//   * remote refs     — external ids keyed by system, e.g. {"frameio": "file_123"}:
//                       the forward breadcrumb that lets a Frame.io comment walk
//                       backward (file id → proxy → version → master coordinates).
//
// Design seam mirrors `changelist`: every op takes an explicit `&Connection`, so
// unit tests run on isolated in-memory DBs while the FFI drives the process-global
// `storage::db()` through the `cyan_changelist_command` JSON dispatch
// (`asset_upsert` / `asset_get` / `asset_set_remote_ref`). Identity is content
// (Blake3 of the essence), never a filename.

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn default_json_object() -> serde_json::Value {
    serde_json::json!({})
}

/// STAGE 4 — the closed ASSET-CLASS vocab. A **clip** (daily) is the per-asset
/// review unit; a **sequence** (timeline) is the edit referencing many clips
/// (where lock + conform live). Stored in the nullable `asset.class` column via
/// [`set_class_location`]; anything else is rejected, never guessed.
pub const ASSET_CLASS_VOCAB: [&str; 2] = ["clip", "sequence"];

/// One registered asset. `hash` is the Blake3 of the essence — the primary key and
/// the spine every ChangeEntry anchors to.
///
/// Two STAGE-4 columns ride on the row but NOT on this struct (the struct is
/// constructed exhaustively across shipping call sites; the columns are additive):
///   * `class`    — clip | sequence ([`ASSET_CLASS_VOCAB`]).
///   * `location` — the MASTER's canonical location (`s3://…`, `file://…`, a NAS
///     path, an LTO/MAM ref). Cyan REFERENCES masters — it need not hold the
///     bytes; "produce master" resolves locations and retrieves selectively.
///
/// Read/write them via [`class_location`] / [`set_class_location`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Asset {
    pub hash: String,
    /// Tenant boundary (the group id) — every query carries it.
    pub tenant_id: String,
    /// master | proxy | deliverable | audio | ... (advisory, not a closed vocab yet).
    #[serde(default)]
    pub kind: Option<String>,
    /// Frames per second — the timecode denominator for this asset's entries.
    #[serde(default)]
    pub fps: Option<f64>,
    #[serde(default)]
    pub duration_ms: Option<i64>,
    /// Derivation edge: the asset hash this one was rendered/derived FROM.
    #[serde(default)]
    pub derived_from_asset: Option<String>,
    /// ... and the ledger version (change_version.version_id) whose conform plan
    /// produced it.
    #[serde(default)]
    pub derived_from_version: Option<String>,
    /// External ids keyed by system, e.g. {"frameio": "file_123"}. JSON object.
    #[serde(default = "default_json_object")]
    pub remote_refs: serde_json::Value,
    /// Free-form technical profile (codec, resolution, LUFS, ...). JSON object.
    #[serde(default = "default_json_object")]
    pub profile_json: serde_json::Value,
    /// The render profile a derived asset was produced with (e.g. "proxy-540p").
    #[serde(default)]
    pub render_profile: Option<String>,
    /// Set once on first registration; preserved by later upserts.
    #[serde(default)]
    pub created_at: i64,
}

/// Create the `asset` table. Idempotent; called from `storage::run_migrations`
/// (alongside `changelist::migrate`) and directly from tests.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS asset (
            hash                 TEXT PRIMARY KEY,
            tenant_id            TEXT NOT NULL,
            kind                 TEXT,
            fps                  REAL,
            duration_ms          INTEGER,
            derived_from_asset   TEXT,
            derived_from_version TEXT,
            remote_refs          TEXT DEFAULT '{}',
            profile_json         TEXT DEFAULT '{}',
            render_profile       TEXT,
            created_at           INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_asset_tenant
            ON asset(tenant_id);
        CREATE INDEX IF NOT EXISTS idx_asset_derived
            ON asset(tenant_id, derived_from_asset);
        "#,
    )?;
    // STAGE 4 additive columns (class / location) — idempotent ALTERs so a DB
    // created before this migration upgrades in place, and a fresh one is a no-op
    // on the second boot.
    add_column_if_missing(conn, "asset", "class", "TEXT")?;
    add_column_if_missing(conn, "asset", "location", "TEXT")?;
    Ok(())
}

/// Idempotent `ALTER TABLE … ADD COLUMN` — checks `PRAGMA table_info` first so
/// re-running the migration on an already-upgraded DB is a clean no-op.
fn add_column_if_missing(conn: &Connection, table: &str, column: &str, decl: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let existing = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !existing.iter().any(|c| c == column) {
        conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"), [])?;
    }
    Ok(())
}

fn row_to_asset(row: &rusqlite::Row) -> rusqlite::Result<Asset> {
    let remote_refs_str: Option<String> = row.get("remote_refs")?;
    let profile_str: Option<String> = row.get("profile_json")?;
    let parse = |s: Option<String>| -> serde_json::Value {
        s.and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(default_json_object)
    };
    Ok(Asset {
        hash: row.get("hash")?,
        tenant_id: row.get("tenant_id")?,
        kind: row.get("kind")?,
        fps: row.get("fps")?,
        duration_ms: row.get("duration_ms")?,
        derived_from_asset: row.get("derived_from_asset")?,
        derived_from_version: row.get("derived_from_version")?,
        remote_refs: parse(remote_refs_str),
        profile_json: parse(profile_str),
        render_profile: row.get("render_profile")?,
        created_at: row.get::<_, Option<i64>>("created_at")?.unwrap_or_default(),
    })
}

/// upsert(asset) → the canonical row after the write.
///
/// Inserts a new asset, or refreshes the descriptive fields (kind/fps/duration/
/// profile/render_profile) of an existing one. `remote_refs` and the derivation
/// edge are NOT touched on conflict — they accrete via `set_remote_ref` /
/// `set_derivation` and must not be clobbered by a metadata refresh. `created_at`
/// is set once. Tenant-guarded: a conflicting row owned by another tenant is left
/// untouched (and the follow-up `get` errors instead of leaking it).
pub fn upsert(conn: &Connection, asset: &Asset) -> Result<Asset> {
    if asset.hash.trim().is_empty() {
        return Err(anyhow!("asset hash required"));
    }
    if asset.tenant_id.trim().is_empty() {
        return Err(anyhow!("tenant_id required"));
    }
    let created_at = if asset.created_at == 0 {
        now()
    } else {
        asset.created_at
    };
    conn.execute(
        "INSERT INTO asset (\
            hash, tenant_id, kind, fps, duration_ms, derived_from_asset, \
            derived_from_version, remote_refs, profile_json, render_profile, created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) \
         ON CONFLICT(hash) DO UPDATE SET \
            kind=excluded.kind, \
            fps=excluded.fps, \
            duration_ms=excluded.duration_ms, \
            profile_json=excluded.profile_json, \
            render_profile=excluded.render_profile \
         WHERE asset.tenant_id = excluded.tenant_id",
        params![
            asset.hash,
            asset.tenant_id,
            asset.kind,
            asset.fps,
            asset.duration_ms,
            asset.derived_from_asset,
            asset.derived_from_version,
            asset.remote_refs.to_string(),
            asset.profile_json.to_string(),
            asset.render_profile,
            created_at,
        ],
    )?;
    get(conn, &asset.tenant_id, &asset.hash)
}

/// get(tenant, hash) → the asset row, tenant-scoped. Errors if not found (or owned
/// by another tenant — non-enumerable either way).
pub fn get(conn: &Connection, tenant_id: &str, hash: &str) -> Result<Asset> {
    conn.query_row(
        "SELECT * FROM asset WHERE tenant_id=?1 AND hash=?2",
        params![tenant_id, hash],
        row_to_asset,
    )
    .map_err(|e| anyhow!("asset {} not found: {}", hash, e))
}

/// set_remote_ref(hash, key, value) → merge one external id into `remote_refs`
/// (e.g. key "frameio", value the Frame.io file id a proxy was published as).
/// Other keys are preserved; the same key overwrites. Errors if the asset is not
/// registered — a remote ref without a registered asset is a dangling breadcrumb.
pub fn set_remote_ref(
    conn: &Connection,
    tenant_id: &str,
    hash: &str,
    key: &str,
    value: &str,
) -> Result<Asset> {
    if key.trim().is_empty() {
        return Err(anyhow!("remote ref key required"));
    }
    let mut asset = get(conn, tenant_id, hash)?;
    if !asset.remote_refs.is_object() {
        asset.remote_refs = default_json_object();
    }
    if let Some(map) = asset.remote_refs.as_object_mut() {
        map.insert(key.to_string(), serde_json::Value::String(value.to_string()));
    }
    conn.execute(
        "UPDATE asset SET remote_refs=?1 WHERE tenant_id=?2 AND hash=?3",
        params![asset.remote_refs.to_string(), tenant_id, hash],
    )?;
    Ok(asset)
}

/// find_by_remote_ref(key, value) → the asset carrying that external id, e.g.
/// key "frameio" + a file id → the proxy the PUBLISH actuator registered. This
/// is the sensor leg's backward walk (file id → proxy → version → master).
/// Tenant-scoped; `None` if no asset carries the ref. Filtered in Rust (the
/// asset table is small; no JSON1 dependency).
pub fn find_by_remote_ref(
    conn: &Connection,
    tenant_id: &str,
    key: &str,
    value: &str,
) -> Result<Option<Asset>> {
    let mut stmt = conn.prepare("SELECT * FROM asset WHERE tenant_id=?1")?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_asset)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .find(|a| a.remote_refs.get(key).and_then(|v| v.as_str()) == Some(value)))
}

/// latest_published_proxy(master) → the Frame.io file id of the newest proxy asset
/// DERIVED FROM `master` that carries a `frameio` remote ref, i.e. the CURRENT round's
/// published proxy (the one the SENSE step listed comments for). A freshly-conformed
/// proxy has NOT been published yet (no frameio ref until the human-gated upload), so
/// it is correctly skipped — this returns the last proxy actually sent for review.
/// `None` if the master has no published proxy. Tenant-scoped; filtered in Rust.
pub fn latest_published_proxy(
    conn: &Connection,
    tenant_id: &str,
    master_hash: &str,
) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT * FROM asset WHERE tenant_id=?1")?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_asset)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter(|a| a.derived_from_asset.as_deref() == Some(master_hash))
        .filter_map(|a| {
            let created = a.created_at;
            a.remote_refs
                .get("frameio")
                .and_then(|v| v.as_str())
                .map(|r| (created, r.to_string()))
        })
        .max_by_key(|(created, _)| *created)
        .map(|(_, r)| r))
}

/// set_derivation(hash, derived_from_asset, derived_from_version) → record the
/// derivation edge: this asset was rendered FROM `derived_from_asset` at ledger
/// version `derived_from_version` (proxy → {master, version}; deliverable →
/// {master, v_final}). Errors if the asset is not registered.
pub fn set_derivation(
    conn: &Connection,
    tenant_id: &str,
    hash: &str,
    derived_from_asset: &str,
    derived_from_version: &str,
) -> Result<Asset> {
    let n = conn.execute(
        "UPDATE asset SET derived_from_asset=?1, derived_from_version=?2 \
         WHERE tenant_id=?3 AND hash=?4",
        params![derived_from_asset, derived_from_version, tenant_id, hash],
    )?;
    if n == 0 {
        return Err(anyhow!("asset {} not found", hash));
    }
    get(conn, tenant_id, hash)
}

/// set_class_location(hash, class, location) → stamp the STAGE-4 columns.
/// `class` is validated against [`ASSET_CLASS_VOCAB`]; a `None` for either field
/// keeps the stored value (accrete, never clobber — the `set_remote_ref` rule).
/// Errors if the asset is not registered for this tenant.
pub fn set_class_location(
    conn: &Connection,
    tenant_id: &str,
    hash: &str,
    class: Option<&str>,
    location: Option<&str>,
) -> Result<()> {
    if let Some(c) = class
        && !ASSET_CLASS_VOCAB.contains(&c)
    {
        return Err(anyhow!(
            "asset class '{}' not in closed vocab {:?}",
            c,
            ASSET_CLASS_VOCAB
        ));
    }
    let n = conn.execute(
        "UPDATE asset SET class=COALESCE(?1, class), location=COALESCE(?2, location) \
         WHERE tenant_id=?3 AND hash=?4",
        params![class, location, tenant_id, hash],
    )?;
    if n == 0 {
        return Err(anyhow!("asset {} not found", hash));
    }
    Ok(())
}

/// class_location(hash) → `(class, location)`, tenant-scoped. Errors if the
/// asset is not registered (non-enumerable, like [`get`]).
pub fn class_location(
    conn: &Connection,
    tenant_id: &str,
    hash: &str,
) -> Result<(Option<String>, Option<String>)> {
    conn.query_row(
        "SELECT class, location FROM asset WHERE tenant_id=?1 AND hash=?2",
        params![tenant_id, hash],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .map_err(|e| anyhow!("asset {} not found: {}", hash, e))
}

/// resolve_final_cut_masters(version) → the SELECTIVE "produce master" retrieve
/// list: `(asset, location)` for every master the final cut actually USES —
/// the version's anchor asset plus any `insert`/`swap` media its conform plan
/// references (`params.asset_hash` / `params.new_asset_hash`). Unused dailies
/// stay archived (they are simply not in the list). Errors clearly when a used
/// master is unregistered or has no `location` — the retrieve leg must never
/// guess where a master lives.
pub fn resolve_final_cut_masters(
    conn: &Connection,
    tenant_id: &str,
    version_id: &str,
) -> Result<Vec<(Asset, String)>> {
    // The anchor: the asset the version snapshots (the cut's base master).
    let anchor: String = conn
        .query_row(
            "SELECT asset_hash FROM change_version WHERE tenant_id=?1 AND version_id=?2",
            params![tenant_id, version_id],
            |r| r.get(0),
        )
        .map_err(|e| anyhow!("version {} not found: {}", version_id, e))?;

    // Ordered, deduped hash walk: anchor first, then op order.
    let mut hashes: Vec<String> = vec![anchor];
    let mut seen: std::collections::HashSet<String> = hashes.iter().cloned().collect();
    for op in crate::changelist::conform_plan(conn, tenant_id, version_id)? {
        for key in ["asset_hash", "new_asset_hash"] {
            if let Some(h) = op.params.get(key).and_then(|v| v.as_str())
                && !h.is_empty()
                && seen.insert(h.to_string())
            {
                hashes.push(h.to_string());
            }
        }
    }

    let mut out = Vec::with_capacity(hashes.len());
    for h in &hashes {
        let asset = get(conn, tenant_id, h)
            .map_err(|_| anyhow!("used master {} is not registered in the asset registry", h))?;
        let (_, location) = class_location(conn, tenant_id, h)?;
        let location = location.filter(|l| !l.trim().is_empty()).ok_or_else(|| {
            anyhow!(
                "used master {} has no location — set asset.location (s3://…, file://…, NAS/MAM ref) before producing the master",
                h
            )
        })?;
        out.push((asset, location));
    }
    Ok(out)
}
