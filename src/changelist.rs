// cyan-backend/src/changelist.rs
//
// ChangeList store — the durable, offline-first, P2P-synced per-asset artifact that
// the Frame.io review-&-conform loop (and Cyan Lens) operate on. See
// CYAN_CHANGEOP_SPEC.md (the `ChangeEntry` record) and
// CYAN_CHANGELIST_STORE_AND_REVIEW_LOOP.md (Part 1 — this store).
//
// One record, three roles: a `ChangeEntry` is simultaneously an annotation
// (timecoded note), a change-op (when actionable + approved), and a labeled example
// (its `outcome` feeds the taste learner). Differing only by `kind`, `state`, and
// whether `op` is set.
//
// Content fields are immutable and content-addressed via Blake3 (`entry_hash`);
// lifecycle columns (`state`, `active`, approval, supersession, `outcome`) are
// mutable with an append-only `change_audit` trail. A version is an immutable
// snapshot: `master@asset_hash + list_hash`, where
// `list_hash = Blake3(asset_hash + ordered active entry_hashes)` — reproducible and
// diffable.
//
// Design seam: every store op takes an explicit `&Connection`, so unit tests run
// against isolated in-memory DBs while the FFI wrappers (`ffi/core.rs`) drive the
// process-global `storage::db()`. P2P sync is merge-by-union on the
// content-addressed entries (dedup by `entry_hash`); lifecycle transitions are
// last-writer-wins keyed by `(entry, transition, ts)` with the audit trail as the
// record — a proper CRDT is a later decision, flagged not over-built.

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

// ============================================================================
// Closed vocab — `op` (~11; closed list beats complete list). Anything not here
// is a creative `note`, escalated as a choice, never guessed.
// ============================================================================

/// The closed set of mechanical/structural ops that are auto-appliable at conform.
/// `marker` is the pure-annotation op (paired with `kind = "marker"`).
pub const OP_VOCAB: &[&str] = &[
    "trim",    // {edge: head|tail, frames}   shorten/extend an edge
    "lift",    // {}                           remove range, leave gap
    "delete",  // {}                           ripple-remove range
    "insert",  // {asset_hash, at}             insert media
    "swap",    // {new_asset_hash}             replace clip/media
    "slip",    // {frames}                     shift content within range
    "level",   // {target_lufs | gain_db}      audio loudness/gain
    "mute",    // {}                           silence range
    "fade",    // {dir: in|out, frames}        audio/video fade
    "reframe", // {aspect, crop|pan}           16:9 -> 9:16 etc.
    "speed",   // {ratio}                      retime
    "color",   // {preset}                     LUT/preset (not a full grade)
    "marker",  // {}                           kind=marker — pure annotation at tc
];

/// `kind` vocab.
pub const KIND_VOCAB: &[&str] = &["note", "op", "marker"];

/// `state` lifecycle vocab.
pub const STATE_VOCAB: &[&str] = &["proposed", "approved", "rejected", "applied", "superseded"];

/// Per-version `outcome` label (the taste-learning label).
pub const OUTCOME_VOCAB: &[&str] = &["pending", "shipped", "rejected"];

// ============================================================================
// ChangeEntry — the single atomic record.
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeEntry {
    // ── identity & anchor ──────────────────────────────────────────────
    pub id: String,
    /// Blake3 of the canonical content fields — makes the change-list hashable.
    /// Filled in by `append`; an empty value on input is recomputed.
    #[serde(default)]
    pub entry_hash: String,
    /// Blake3 of the SOURCE asset this anchors to — the spine.
    pub asset_hash: String,
    /// Tenant boundary (the group id). Every row/query carries it.
    pub tenant_id: String,
    /// Optional "V1"/"A1"… track (multi-track; needed for OTIO/AAF).
    #[serde(default)]
    pub track: Option<String>,
    /// Timecode in — frames (the asset carries fps). Frame-accurate.
    pub tc_in: i64,
    /// Timecode out — frames. `None`/== tc_in for a point marker.
    #[serde(default)]
    pub tc_out: Option<i64>,

    // ── kind & operation ───────────────────────────────────────────────
    pub kind: String, // note | op | marker
    #[serde(default)]
    pub op: Option<String>, // closed vocab, only when kind=op
    #[serde(default)]
    pub params: serde_json::Value, // typed payload per op

    // ── meaning & source ───────────────────────────────────────────────
    #[serde(default)]
    pub intent: String, // human text ("open feels rushed")
    #[serde(default)]
    pub source: Option<String>, // frameio | cyan | resolve | avid | agent
    #[serde(default)]
    pub source_ref: Option<String>, // id of originating comment/marker

    // ── provenance ─────────────────────────────────────────────────────
    #[serde(default)]
    pub author: Option<String>, // XaeroID / user id
    #[serde(default)]
    pub role: Option<String>, // producer | editor | reviewer | agent
    #[serde(default)]
    pub proposed_by: Option<String>, // human | agent
    pub created_at: i64,

    // ── lifecycle / reversibility ──────────────────────────────────────
    pub state: String, // proposed | approved | rejected | applied | superseded
    pub active: bool,  // in the current conform? (toggle = NON-DESTRUCTIVE reverse)
    #[serde(default)]
    pub approved_by: Option<String>,
    #[serde(default)]
    pub approved_at: Option<i64>,
    #[serde(default)]
    pub supersedes: Option<String>, // entry id this replaces (the redo chain)
    #[serde(default)]
    pub superseded_by: Option<String>, // entry id that replaced this

    // ── ordering ───────────────────────────────────────────────────────
    pub seq: i64, // position in the change-list (apply order)
    #[serde(default)]
    pub depends_on: Option<String>, // optional entry id(s)

    // ── outcome (the taste-learning label) ─────────────────────────────
    #[serde(default)]
    pub version_ref: Option<String>, // the version this entry first appeared in
    #[serde(default)]
    pub branch: Option<String>, // the branch this entry lives on
    #[serde(default)]
    pub outcome: Option<String>, // pending | shipped | rejected
}

/// An immutable version snapshot: `master@asset_hash + list_hash`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeVersion {
    pub version_id: String,
    pub asset_hash: String,
    pub tenant_id: String,
    pub branch: String,
    pub version_no: i64,
    pub list_hash: String,
    /// Ordered active entry hashes captured at snapshot time.
    pub entry_hashes: Vec<String>,
    pub created_at: i64,
    pub outcome: String, // pending | shipped | rejected
}

/// The reviewable diff between two versions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct VersionDiff {
    pub added: Vec<String>,      // entry_hashes present in B but not A
    pub removed: Vec<String>,    // entry_hashes present in A but not B
    pub superseded: Vec<String>, // entry ids in A whose superseded_by appears in B
}

/// One ordered op for the conform tool (a projection of an active entry).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConformOp {
    pub entry_id: String,
    pub seq: i64,
    pub track: Option<String>,
    pub tc_in: i64,
    pub tc_out: Option<i64>,
    pub op: String,
    pub params: serde_json::Value,
}

// ============================================================================
// Content addressing — Blake3 over canonical fields.
// ============================================================================

/// Canonical Blake3 hash of the immutable content of an entry. Lifecycle fields
/// (state/active/approval/supersession/outcome/version_ref) are deliberately
/// excluded so a transition never changes the entry's identity.
pub fn compute_entry_hash(e: &ChangeEntry) -> String {
    // Build a stable canonical string of immutable fields only. Order is fixed.
    // `branch` is part of identity: the same edit on two branches is two distinct
    // entries (a fork), while identical content on the SAME branch arriving from two
    // peers dedups to one row (the P2P union-merge key).
    let canonical = serde_json::json!({
        "asset_hash": e.asset_hash,
        "tenant_id": e.tenant_id,
        "branch": e.branch,
        "track": e.track,
        "tc_in": e.tc_in,
        "tc_out": e.tc_out,
        "kind": e.kind,
        "op": e.op,
        "params": e.params,
        "intent": e.intent,
        "source": e.source,
        "source_ref": e.source_ref,
        "author": e.author,
        "role": e.role,
        "proposed_by": e.proposed_by,
        "created_at": e.created_at,
        "seq": e.seq,
        "depends_on": e.depends_on,
    });
    // serde_json::Value serializes object keys in a stable (sorted) order, so this
    // canonical form is reproducible across processes/peers.
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    blake3::hash(&bytes).to_hex().to_string()
}

/// `list_hash = Blake3(asset_hash + ordered active entry_hashes)`.
pub fn compute_list_hash(asset_hash: &str, ordered_entry_hashes: &[String]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(asset_hash.as_bytes());
    for h in ordered_entry_hashes {
        hasher.update(b"\n");
        hasher.update(h.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

// ============================================================================
// Schema migration.
// ============================================================================

/// Create the four changelist tables. Idempotent; called from `storage::run_migrations`
/// and directly from tests.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS change_entry (
            id            TEXT PRIMARY KEY,
            entry_hash    TEXT NOT NULL,
            asset_hash    TEXT NOT NULL,
            tenant_id     TEXT NOT NULL,
            branch        TEXT NOT NULL DEFAULT 'main',
            track         TEXT,
            tc_in         INTEGER NOT NULL,
            tc_out        INTEGER,
            kind          TEXT NOT NULL,
            op            TEXT,
            params        TEXT NOT NULL DEFAULT '{}',
            intent        TEXT NOT NULL DEFAULT '',
            source        TEXT,
            source_ref    TEXT,
            author        TEXT,
            role          TEXT,
            proposed_by   TEXT,
            created_at    INTEGER NOT NULL,
            state         TEXT NOT NULL DEFAULT 'proposed',
            active        INTEGER NOT NULL DEFAULT 1,
            approved_by   TEXT,
            approved_at   INTEGER,
            supersedes    TEXT,
            superseded_by TEXT,
            seq           INTEGER NOT NULL DEFAULT 0,
            depends_on    TEXT,
            version_ref   TEXT,
            outcome       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ce_asset
            ON change_entry(tenant_id, asset_hash, branch);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ce_hash
            ON change_entry(tenant_id, entry_hash);

        CREATE TABLE IF NOT EXISTS change_version (
            version_id    TEXT PRIMARY KEY,
            asset_hash    TEXT NOT NULL,
            tenant_id     TEXT NOT NULL,
            branch        TEXT NOT NULL,
            version_no    INTEGER NOT NULL,
            list_hash     TEXT NOT NULL,
            entry_hashes  TEXT NOT NULL DEFAULT '[]',
            created_at    INTEGER NOT NULL,
            outcome       TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE INDEX IF NOT EXISTS idx_cv_asset
            ON change_version(tenant_id, asset_hash, branch, version_no);

        CREATE TABLE IF NOT EXISTS change_branch (
            tenant_id     TEXT NOT NULL,
            asset_hash    TEXT NOT NULL,
            branch        TEXT NOT NULL,
            head_version  TEXT,
            created_at    INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, asset_hash, branch)
        );

        CREATE TABLE IF NOT EXISTS change_audit (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id      TEXT NOT NULL,
            tenant_id     TEXT NOT NULL,
            transition    TEXT NOT NULL,
            actor         TEXT,
            ts            INTEGER NOT NULL,
            detail        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ca_entry
            ON change_audit(tenant_id, entry_id, ts);
        "#,
    )?;
    Ok(())
}

// ============================================================================
// Internal helpers.
// ============================================================================

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn validate_entry(e: &ChangeEntry) -> Result<()> {
    if e.asset_hash.trim().is_empty() {
        return Err(anyhow!("asset_hash required"));
    }
    if e.tenant_id.trim().is_empty() {
        return Err(anyhow!("tenant_id required"));
    }
    if !KIND_VOCAB.contains(&e.kind.as_str()) {
        return Err(anyhow!("invalid kind '{}'", e.kind));
    }
    if e.kind == "op" {
        match &e.op {
            Some(op) if OP_VOCAB.contains(&op.as_str()) => {}
            Some(op) => return Err(anyhow!("op '{}' not in closed vocab", op)),
            None => return Err(anyhow!("kind=op requires an op")),
        }
    }
    if let Some(op) = &e.op
        && !OP_VOCAB.contains(&op.as_str())
    {
        return Err(anyhow!("op '{}' not in closed vocab", op));
    }
    Ok(())
}

fn row_to_entry(row: &rusqlite::Row) -> rusqlite::Result<ChangeEntry> {
    let params_str: String = row.get("params")?;
    let params: serde_json::Value =
        serde_json::from_str(&params_str).unwrap_or(serde_json::Value::Null);
    Ok(ChangeEntry {
        id: row.get("id")?,
        entry_hash: row.get("entry_hash")?,
        asset_hash: row.get("asset_hash")?,
        tenant_id: row.get("tenant_id")?,
        branch: row.get("branch")?,
        track: row.get("track")?,
        tc_in: row.get("tc_in")?,
        tc_out: row.get("tc_out")?,
        kind: row.get("kind")?,
        op: row.get("op")?,
        params,
        intent: row.get("intent")?,
        source: row.get("source")?,
        source_ref: row.get("source_ref")?,
        author: row.get("author")?,
        role: row.get("role")?,
        proposed_by: row.get("proposed_by")?,
        created_at: row.get("created_at")?,
        state: row.get("state")?,
        active: row.get::<_, i64>("active")? != 0,
        approved_by: row.get("approved_by")?,
        approved_at: row.get("approved_at")?,
        supersedes: row.get("supersedes")?,
        superseded_by: row.get("superseded_by")?,
        seq: row.get("seq")?,
        depends_on: row.get("depends_on")?,
        version_ref: row.get("version_ref")?,
        outcome: row.get("outcome")?,
    })
}

fn audit(
    conn: &Connection,
    entry_id: &str,
    tenant_id: &str,
    transition: &str,
    actor: Option<&str>,
    detail: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO change_audit (entry_id, tenant_id, transition, actor, ts, detail) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![entry_id, tenant_id, transition, actor, now(), detail],
    )?;
    Ok(())
}

fn get_entry_row(conn: &Connection, tenant_id: &str, entry_id: &str) -> Result<ChangeEntry> {
    conn.query_row(
        "SELECT * FROM change_entry WHERE tenant_id=?1 AND id=?2",
        params![tenant_id, entry_id],
        row_to_entry,
    )
    .map_err(|e| anyhow!("entry {} not found: {}", entry_id, e))
}

// ============================================================================
// Store operations (the `cyan_changelist_*` FFI surface).
// ============================================================================

/// append(asset, branch, entry) → ChangeEntry (proposed).
///
/// Content-addresses the entry, assigns the next `seq` within (asset, branch) when
/// the caller left `seq == 0`, and inserts as `proposed` + `active`. Idempotent by
/// `entry_hash` (the union-merge key for P2P): a re-append of identical content
/// returns the existing row instead of duplicating.
pub fn append(
    conn: &Connection,
    asset_hash: &str,
    branch: &str,
    mut entry: ChangeEntry,
) -> Result<ChangeEntry> {
    entry.asset_hash = asset_hash.to_string();
    entry.branch = Some(branch.to_string());
    if entry.id.trim().is_empty() {
        entry.id = uuid::Uuid::new_v4().to_string();
    }
    if entry.created_at == 0 {
        entry.created_at = now();
    }
    if entry.kind.trim().is_empty() {
        entry.kind = "note".to_string();
    }
    if entry.state.trim().is_empty() {
        entry.state = "proposed".to_string();
    }
    validate_entry(&entry)?;

    // Assign apply-order seq if unset (0): next after the current max in this list.
    if entry.seq == 0 {
        let max_seq: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM change_entry \
                 WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3",
                params![entry.tenant_id, asset_hash, branch],
                |r| r.get(0),
            )
            .unwrap_or(0);
        entry.seq = max_seq + 1;
    }

    entry.entry_hash = compute_entry_hash(&entry);

    // Union-merge dedup: if this exact content already exists for the tenant, return it.
    if let Some(existing) = conn
        .query_row(
            "SELECT * FROM change_entry WHERE tenant_id=?1 AND entry_hash=?2",
            params![entry.tenant_id, entry.entry_hash],
            row_to_entry,
        )
        .optional()?
    {
        return Ok(existing);
    }

    conn.execute(
        "INSERT INTO change_entry (\
            id, entry_hash, asset_hash, tenant_id, branch, track, tc_in, tc_out, \
            kind, op, params, intent, source, source_ref, author, role, proposed_by, \
            created_at, state, active, approved_by, approved_at, supersedes, \
            superseded_by, seq, depends_on, version_ref, outcome) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,\
                 ?20,?21,?22,?23,?24,?25,?26,?27,?28)",
        params![
            entry.id,
            entry.entry_hash,
            entry.asset_hash,
            entry.tenant_id,
            branch,
            entry.track,
            entry.tc_in,
            entry.tc_out,
            entry.kind,
            entry.op,
            entry.params.to_string(),
            entry.intent,
            entry.source,
            entry.source_ref,
            entry.author,
            entry.role,
            entry.proposed_by,
            entry.created_at,
            entry.state,
            entry.active as i64,
            entry.approved_by,
            entry.approved_at,
            entry.supersedes,
            entry.superseded_by,
            entry.seq,
            entry.depends_on,
            entry.version_ref,
            entry.outcome,
        ],
    )?;
    audit(
        conn,
        &entry.id,
        &entry.tenant_id,
        "append",
        entry.proposed_by.as_deref(),
        None,
    )?;
    Ok(entry)
}

/// set_state(entry, approved|rejected, by) → transition + audit.
pub fn set_state(
    conn: &Connection,
    tenant_id: &str,
    entry_id: &str,
    new_state: &str,
    by: Option<&str>,
) -> Result<ChangeEntry> {
    if !STATE_VOCAB.contains(&new_state) {
        return Err(anyhow!("invalid state '{}'", new_state));
    }
    let mut e = get_entry_row(conn, tenant_id, entry_id)?;
    let approved_at = if new_state == "approved" {
        Some(now())
    } else {
        None
    };
    conn.execute(
        "UPDATE change_entry SET state=?1, approved_by=?2, approved_at=?3 \
         WHERE tenant_id=?4 AND id=?5",
        params![new_state, by, approved_at, tenant_id, entry_id],
    )?;
    audit(
        conn,
        entry_id,
        tenant_id,
        &format!("state:{}", new_state),
        by,
        None,
    )?;
    e.state = new_state.to_string();
    e.approved_by = by.map(|s| s.to_string());
    e.approved_at = approved_at;
    Ok(e)
}

/// set_active(entry, bool) → NON-DESTRUCTIVE reverse / re-enable.
pub fn set_active(
    conn: &Connection,
    tenant_id: &str,
    entry_id: &str,
    active: bool,
    by: Option<&str>,
) -> Result<ChangeEntry> {
    let mut e = get_entry_row(conn, tenant_id, entry_id)?;
    conn.execute(
        "UPDATE change_entry SET active=?1 WHERE tenant_id=?2 AND id=?3",
        params![active as i64, tenant_id, entry_id],
    )?;
    audit(
        conn,
        entry_id,
        tenant_id,
        if active { "active:true" } else { "active:false" },
        by,
        None,
    )?;
    e.active = active;
    Ok(e)
}

/// supersede(old_entry, new_entry) → the redo chain (op4 → op4').
///
/// Appends `new_entry`, links it to `old_entry` (`supersedes`/`superseded_by`),
/// marks the old one `superseded` + inactive, and the new one active. Returns the
/// newly-created entry.
pub fn supersede(
    conn: &Connection,
    old_entry_id: &str,
    new_entry: ChangeEntry,
) -> Result<ChangeEntry> {
    let tenant_id = new_entry.tenant_id.clone();
    let old = get_entry_row(conn, &tenant_id, old_entry_id)?;
    let branch = old.branch.clone().unwrap_or_else(|| "main".to_string());

    let mut new = new_entry;
    new.supersedes = Some(old_entry_id.to_string());
    new.active = true;
    if new.state.trim().is_empty() {
        new.state = "proposed".to_string();
    }
    let appended = append(conn, &old.asset_hash, &branch, new)?;

    conn.execute(
        "UPDATE change_entry SET state='superseded', active=0, superseded_by=?1 \
         WHERE tenant_id=?2 AND id=?3",
        params![appended.id, tenant_id, old_entry_id],
    )?;
    audit(
        conn,
        old_entry_id,
        &tenant_id,
        "superseded_by",
        appended.proposed_by.as_deref(),
        Some(&appended.id),
    )?;
    audit(
        conn,
        &appended.id,
        &tenant_id,
        "supersedes",
        appended.proposed_by.as_deref(),
        Some(old_entry_id),
    )?;
    Ok(appended)
}

/// The ordered active entries for (asset, branch): active=1, not rejected/superseded,
/// ordered by `seq` then `created_at`.
fn active_entries(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
) -> Result<Vec<ChangeEntry>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM change_entry \
         WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3 \
           AND active=1 AND state NOT IN ('rejected','superseded') \
         ORDER BY seq ASC, created_at ASC, id ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id, asset_hash, branch], row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// snapshot(asset, branch) → freeze active set → change_version (+ list_hash).
///
/// Immutable: each call mints a NEW version_no (monotonic per asset+branch). Stamps
/// `version_ref` on entries that first appear in this version and advances the
/// branch head.
pub fn snapshot(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
) -> Result<ChangeVersion> {
    let entries = active_entries(conn, tenant_id, asset_hash, branch)?;
    let entry_hashes: Vec<String> = entries.iter().map(|e| e.entry_hash.clone()).collect();
    let list_hash = compute_list_hash(asset_hash, &entry_hashes);

    let version_no: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(version_no), 0) + 1 FROM change_version \
             WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3",
            params![tenant_id, asset_hash, branch],
            |r| r.get(0),
        )
        .unwrap_or(1);

    let version = ChangeVersion {
        version_id: uuid::Uuid::new_v4().to_string(),
        asset_hash: asset_hash.to_string(),
        tenant_id: tenant_id.to_string(),
        branch: branch.to_string(),
        version_no,
        list_hash: list_hash.clone(),
        entry_hashes: entry_hashes.clone(),
        created_at: now(),
        outcome: "pending".to_string(),
    };

    conn.execute(
        "INSERT INTO change_version (\
            version_id, asset_hash, tenant_id, branch, version_no, list_hash, \
            entry_hashes, created_at, outcome) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            version.version_id,
            version.asset_hash,
            version.tenant_id,
            version.branch,
            version.version_no,
            version.list_hash,
            serde_json::to_string(&version.entry_hashes)?,
            version.created_at,
            version.outcome,
        ],
    )?;

    // Stamp version_ref on entries that didn't have one (first appearance).
    for e in &entries {
        if e.version_ref.is_none() {
            conn.execute(
                "UPDATE change_entry SET version_ref=?1 \
                 WHERE tenant_id=?2 AND id=?3 AND version_ref IS NULL",
                params![version.version_id, tenant_id, e.id],
            )?;
        }
    }

    // Advance the branch head.
    conn.execute(
        "INSERT INTO change_branch (tenant_id, asset_hash, branch, head_version, created_at) \
         VALUES (?1,?2,?3,?4,?5) \
         ON CONFLICT(tenant_id, asset_hash, branch) \
         DO UPDATE SET head_version=excluded.head_version",
        params![tenant_id, asset_hash, branch, version.version_id, now()],
    )?;

    Ok(version)
}

/// branch(asset, from_branch, new_branch) → fork the active set (today/tomorrow/day-after).
///
/// Copies the active entries of `from_branch` onto `new_branch` as fresh proposed
/// entries (new ids; content re-hashed under the new branch is identical since
/// branch is not part of the content hash — so the active SET forks but entries
/// stay content-addressed and dedup across branches). Registers the new branch.
pub fn branch(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    from_branch: &str,
    new_branch: &str,
) -> Result<Vec<ChangeEntry>> {
    if new_branch.trim().is_empty() {
        return Err(anyhow!("new_branch required"));
    }
    let src = active_entries(conn, tenant_id, asset_hash, from_branch)?;
    let mut forked = Vec::with_capacity(src.len());
    for e in src {
        let mut clone = e.clone();
        clone.id = uuid::Uuid::new_v4().to_string();
        clone.entry_hash = String::new(); // recompute under append
        clone.branch = Some(new_branch.to_string());
        clone.version_ref = None;
        clone.superseded_by = None;
        clone.outcome = None;
        // Keep state/active so the fork starts with the same conform set.
        forked.push(append(conn, asset_hash, new_branch, clone)?);
    }
    conn.execute(
        "INSERT OR IGNORE INTO change_branch (tenant_id, asset_hash, branch, head_version, created_at) \
         VALUES (?1,?2,?3,NULL,?4)",
        params![tenant_id, asset_hash, new_branch, now()],
    )?;
    Ok(forked)
}

/// diff(version_a, version_b) → {added, removed, superseded} — the reviewable diff.
pub fn diff(conn: &Connection, tenant_id: &str, version_a: &str, version_b: &str) -> Result<VersionDiff> {
    let load = |vid: &str| -> Result<Vec<String>> {
        let s: String = conn
            .query_row(
                "SELECT entry_hashes FROM change_version WHERE tenant_id=?1 AND version_id=?2",
                params![tenant_id, vid],
                |r| r.get(0),
            )
            .map_err(|e| anyhow!("version {} not found: {}", vid, e))?;
        Ok(serde_json::from_str(&s).unwrap_or_default())
    };
    let a = load(version_a)?;
    let b = load(version_b)?;
    let a_set: std::collections::HashSet<String> = a.iter().cloned().collect();
    let b_set: std::collections::HashSet<String> = b.iter().cloned().collect();

    let added: Vec<String> = b.iter().filter(|h| !a_set.contains(*h)).cloned().collect();
    let removed: Vec<String> = a.iter().filter(|h| !b_set.contains(*h)).cloned().collect();

    // superseded: entries in A (by hash) whose row has a superseded_by that resolves
    // to an entry whose hash is present in B.
    let mut superseded = Vec::new();
    for h in &removed {
        if let Some(e) = conn
            .query_row(
                "SELECT * FROM change_entry WHERE tenant_id=?1 AND entry_hash=?2",
                params![tenant_id, h],
                row_to_entry,
            )
            .optional()?
            && let Some(by_id) = &e.superseded_by
            && let Some(by) = conn
                .query_row(
                    "SELECT * FROM change_entry WHERE tenant_id=?1 AND id=?2",
                    params![tenant_id, by_id],
                    row_to_entry,
                )
                .optional()?
            && b_set.contains(&by.entry_hash)
        {
            superseded.push(e.id);
        }
    }

    Ok(VersionDiff {
        added,
        removed,
        superseded,
    })
}

/// conform_plan(version) → ordered active ops → to the conform tool.
///
/// Resolves the version's frozen entry_hashes back to rows and projects the
/// actionable ops (kind=op) in apply order. Pure annotations (note/marker) are
/// excluded from the plan (they ride as OTIO markers, not edits).
pub fn conform_plan(conn: &Connection, tenant_id: &str, version_id: &str) -> Result<Vec<ConformOp>> {
    let hashes_str: String = conn
        .query_row(
            "SELECT entry_hashes FROM change_version WHERE tenant_id=?1 AND version_id=?2",
            params![tenant_id, version_id],
            |r| r.get(0),
        )
        .map_err(|e| anyhow!("version {} not found: {}", version_id, e))?;
    let hashes: Vec<String> = serde_json::from_str(&hashes_str).unwrap_or_default();

    let mut ops = Vec::new();
    for h in &hashes {
        if let Some(e) = conn
            .query_row(
                "SELECT * FROM change_entry WHERE tenant_id=?1 AND entry_hash=?2",
                params![tenant_id, h],
                row_to_entry,
            )
            .optional()?
            && e.kind == "op"
            && let Some(op) = e.op.clone()
        {
            ops.push(ConformOp {
                entry_id: e.id,
                seq: e.seq,
                track: e.track,
                tc_in: e.tc_in,
                tc_out: e.tc_out,
                op,
                params: e.params,
            });
        }
    }
    ops.sort_by(|a, b| a.seq.cmp(&b.seq).then(a.entry_id.cmp(&b.entry_id)));
    Ok(ops)
}

/// get(asset, branch) → entries + head version (the review rail).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeListView {
    pub asset_hash: String,
    pub branch: String,
    pub entries: Vec<ChangeEntry>,
    pub head_version: Option<ChangeVersion>,
}

pub fn get(conn: &Connection, tenant_id: &str, asset_hash: &str, branch: &str) -> Result<ChangeListView> {
    let mut stmt = conn.prepare(
        "SELECT * FROM change_entry \
         WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3 \
         ORDER BY seq ASC, created_at ASC, id ASC",
    )?;
    let entries = stmt
        .query_map(params![tenant_id, asset_hash, branch], row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let head_version_id: Option<String> = conn
        .query_row(
            "SELECT head_version FROM change_branch \
             WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3",
            params![tenant_id, asset_hash, branch],
            |r| r.get(0),
        )
        .optional()?
        .flatten();

    let head_version = match head_version_id {
        Some(vid) => get_version(conn, tenant_id, &vid).ok(),
        None => None,
    };

    Ok(ChangeListView {
        asset_hash: asset_hash.to_string(),
        branch: branch.to_string(),
        entries,
        head_version,
    })
}

/// Load a single version by id.
pub fn get_version(conn: &Connection, tenant_id: &str, version_id: &str) -> Result<ChangeVersion> {
    conn.query_row(
        "SELECT version_id, asset_hash, tenant_id, branch, version_no, list_hash, \
                entry_hashes, created_at, outcome \
         FROM change_version WHERE tenant_id=?1 AND version_id=?2",
        params![tenant_id, version_id],
        |row| {
            let hashes_str: String = row.get("entry_hashes")?;
            Ok(ChangeVersion {
                version_id: row.get("version_id")?,
                asset_hash: row.get("asset_hash")?,
                tenant_id: row.get("tenant_id")?,
                branch: row.get("branch")?,
                version_no: row.get("version_no")?,
                list_hash: row.get("list_hash")?,
                entry_hashes: serde_json::from_str(&hashes_str).unwrap_or_default(),
                created_at: row.get("created_at")?,
                outcome: row.get("outcome")?,
            })
        },
    )
    .map_err(|e| anyhow!("version {} not found: {}", version_id, e))
}

/// set_outcome(version, shipped|rejected) → the taste-learning label.
///
/// Sets the version's outcome and propagates it to the entries that first appeared
/// in that version (those whose `version_ref` points here and whose `outcome` was
/// still pending).
pub fn set_outcome(conn: &Connection, tenant_id: &str, version_id: &str, outcome: &str) -> Result<()> {
    if !OUTCOME_VOCAB.contains(&outcome) {
        return Err(anyhow!("invalid outcome '{}'", outcome));
    }
    let n = conn.execute(
        "UPDATE change_version SET outcome=?1 WHERE tenant_id=?2 AND version_id=?3",
        params![outcome, tenant_id, version_id],
    )?;
    if n == 0 {
        return Err(anyhow!("version {} not found", version_id));
    }
    conn.execute(
        "UPDATE change_entry SET outcome=?1 \
         WHERE tenant_id=?2 AND version_ref=?3 AND (outcome IS NULL OR outcome='pending')",
        params![outcome, tenant_id, version_id],
    )?;
    Ok(())
}

// ============================================================================
// FFI command/event JSON dispatch.
// ============================================================================
//
// One additive entrypoint takes a JSON command `{ "op": <name>, ... }` and returns a
// JSON result string. This keeps the C ABI surface to two functions
// (`cyan_changelist_command` + the existing `cyan_free_string`) and mirrors the
// existing JSON-shaped FFI style. The op names match the store operations 1:1.

/// Run a changelist command against the process-global DB. JSON in, JSON out.
/// Errors surface as `{ "error": "<msg>" }` — never a panic across the boundary.
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

    let lock = crate::storage::db()
        .lock()
        .map_err(|e| anyhow!("DB lock: {}", e))?;
    let conn: &Connection = &lock;

    let tenant = |c: &serde_json::Value| -> Result<String> {
        c.get("tenant_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("missing 'tenant_id'"))
    };
    let s = |c: &serde_json::Value, k: &str| -> Result<String> {
        c.get(k)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("missing '{}'", k))
    };

    match op {
        "append" => {
            let asset = s(&cmd, "asset_hash")?;
            let branch = cmd
                .get("branch")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            let entry: ChangeEntry = serde_json::from_value(
                cmd.get("entry").cloned().ok_or_else(|| anyhow!("missing 'entry'"))?,
            )
            .map_err(|e| anyhow!("bad entry: {}", e))?;
            let out = append(conn, &asset, &branch, entry)?;
            Ok(serde_json::to_value(out)?)
        }
        "set_state" => {
            let out = set_state(
                conn,
                &tenant(&cmd)?,
                &s(&cmd, "entry_id")?,
                &s(&cmd, "state")?,
                cmd.get("by").and_then(|v| v.as_str()),
            )?;
            Ok(serde_json::to_value(out)?)
        }
        "set_active" => {
            let active = cmd.get("active").and_then(|v| v.as_bool()).unwrap_or(true);
            let out = set_active(
                conn,
                &tenant(&cmd)?,
                &s(&cmd, "entry_id")?,
                active,
                cmd.get("by").and_then(|v| v.as_str()),
            )?;
            Ok(serde_json::to_value(out)?)
        }
        "supersede" => {
            let new_entry: ChangeEntry = serde_json::from_value(
                cmd.get("entry").cloned().ok_or_else(|| anyhow!("missing 'entry'"))?,
            )
            .map_err(|e| anyhow!("bad entry: {}", e))?;
            let out = supersede(conn, &s(&cmd, "old_entry_id")?, new_entry)?;
            Ok(serde_json::to_value(out)?)
        }
        "snapshot" => {
            let branch = cmd
                .get("branch")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            let out = snapshot(conn, &tenant(&cmd)?, &s(&cmd, "asset_hash")?, &branch)?;
            Ok(serde_json::to_value(out)?)
        }
        "branch" => {
            let out = branch(
                conn,
                &tenant(&cmd)?,
                &s(&cmd, "asset_hash")?,
                &s(&cmd, "from_branch")?,
                &s(&cmd, "new_branch")?,
            )?;
            Ok(serde_json::to_value(out)?)
        }
        "diff" => {
            let out = diff(
                conn,
                &tenant(&cmd)?,
                &s(&cmd, "version_a")?,
                &s(&cmd, "version_b")?,
            )?;
            Ok(serde_json::to_value(out)?)
        }
        "conform_plan" => {
            let out = conform_plan(conn, &tenant(&cmd)?, &s(&cmd, "version_id")?)?;
            Ok(serde_json::to_value(out)?)
        }
        "get" => {
            let branch = cmd
                .get("branch")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            let out = get(conn, &tenant(&cmd)?, &s(&cmd, "asset_hash")?, &branch)?;
            Ok(serde_json::to_value(out)?)
        }
        "get_version" => {
            let out = get_version(conn, &tenant(&cmd)?, &s(&cmd, "version_id")?)?;
            Ok(serde_json::to_value(out)?)
        }
        "set_outcome" => {
            set_outcome(
                conn,
                &tenant(&cmd)?,
                &s(&cmd, "version_id")?,
                &s(&cmd, "outcome")?,
            )?;
            Ok(serde_json::json!({ "ok": true }))
        }
        other => Err(anyhow!("unknown op '{}'", other)),
    }
}
