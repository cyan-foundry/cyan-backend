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

    // ── lifecycle LWW clock (CYAN_FORMAT_SPEC §6.1) ────────────────────
    /// Versions ONLY the mutable lifecycle columns (content is immutable). Bumped by
    /// `set_state`/`set_active`/`supersede`/`set_outcome` and the `snapshot()`
    /// version_ref stamp; the P2P merge key for the ONE lifecycle LWW lane per entry.
    /// `#[serde(default)]` keeps pre-sync serializations parsing (clock 0 = never moved).
    #[serde(default)]
    pub updated_at: i64,
    /// The actor id of the last lifecycle write — the deterministic LWW tie-break
    /// (§6.3: equal clocks ⇒ higher actor id wins).
    #[serde(default)]
    pub updated_by: Option<String>,
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
    /// `cut_hash = Blake3(asset_hash + ordered active OP entry hashes)` — the PICTURE
    /// identity (list_hash/cut_hash split, CYAN_FORMAT_QA). Renders derive from it;
    /// an unchanged cut_hash means nothing to re-render. Notes/markers change
    /// `list_hash` but never `cut_hash`. `#[serde(default)]` so versions serialized
    /// before this field existed still parse.
    #[serde(default)]
    pub cut_hash: String,
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

/// `cut_hash = Blake3(asset_hash + ordered active OP entry hashes)` — same
/// construction as `compute_list_hash`, but the caller passes ONLY the hashes of
/// `kind == "op"` entries (see `snapshot`). This is the picture identity: notes and
/// markers annotate the cut without changing it, so two versions that differ only
/// by comments share a cut_hash (same picture ⇒ the previous render is reusable).
/// By construction, a version whose active set is pure ops has cut_hash == list_hash.
pub fn compute_cut_hash(asset_hash: &str, ordered_op_entry_hashes: &[String]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(asset_hash.as_bytes());
    for h in ordered_op_entry_hashes {
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
            outcome       TEXT,
            updated_at    INTEGER NOT NULL DEFAULT 0,
            updated_by    TEXT
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
            cut_hash      TEXT,
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
            updated_at    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (tenant_id, asset_hash, branch)
        );

        CREATE TABLE IF NOT EXISTS change_audit (
            id            TEXT PRIMARY KEY,
            entry_id      TEXT NOT NULL,
            tenant_id     TEXT NOT NULL,
            transition    TEXT NOT NULL,
            actor         TEXT,
            ts            INTEGER NOT NULL,
            detail        TEXT,
            audit_hash    TEXT
        );

        CREATE TABLE IF NOT EXISTS own_refs (
            tenant_id     TEXT NOT NULL,
            source        TEXT NOT NULL,
            source_ref    TEXT NOT NULL,
            created_at    INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, source, source_ref)
        );
        "#,
    )?;

    // ── Defensive upgrades for DBs created by earlier feat/frameio builds. ──────
    // The CREATE TABLE statements above carry the current shape; a device holding
    // the previous build's tables is brought to the same shape via PRAGMA-guarded
    // ALTERs (SQLite has no `ADD COLUMN IF NOT EXISTS`). CYAN_FORMAT_SPEC §6.1.
    if !has_column(conn, "change_version", "cut_hash")? {
        conn.execute("ALTER TABLE change_version ADD COLUMN cut_hash TEXT", [])?;
    }
    if !has_column(conn, "change_entry", "updated_at")? {
        conn.execute(
            "ALTER TABLE change_entry ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !has_column(conn, "change_branch", "updated_at")? {
        conn.execute(
            "ALTER TABLE change_branch ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !has_column(conn, "change_entry", "updated_by")? {
        conn.execute("ALTER TABLE change_entry ADD COLUMN updated_by TEXT", [])?;
    }

    // change_audit: the earlier build used `id INTEGER PRIMARY KEY AUTOINCREMENT` —
    // local-only ids that cannot union across peers (CYAN_FORMAT_SPEC §6.1 delta 2).
    // Rebuild to the content-addressed shape (TEXT uuid id + audit_hash). Legacy rows
    // keep their stringified ids with a NULL audit_hash (they predate content
    // addressing; NULLs are distinct under the unique index, so nothing is lost or
    // falsely deduped).
    if audit_id_is_integer(conn)? {
        conn.execute_batch(
            r#"
            ALTER TABLE change_audit RENAME TO change_audit_legacy;
            CREATE TABLE change_audit (
                id            TEXT PRIMARY KEY,
                entry_id      TEXT NOT NULL,
                tenant_id     TEXT NOT NULL,
                transition    TEXT NOT NULL,
                actor         TEXT,
                ts            INTEGER NOT NULL,
                detail        TEXT,
                audit_hash    TEXT
            );
            INSERT INTO change_audit (id, entry_id, tenant_id, transition, actor, ts, detail, audit_hash)
                SELECT CAST(id AS TEXT), entry_id, tenant_id, transition, actor, ts, detail, NULL
                FROM change_audit_legacy;
            DROP TABLE change_audit_legacy;
            "#,
        )?;
    }
    if !has_column(conn, "change_audit", "audit_hash")? {
        conn.execute("ALTER TABLE change_audit ADD COLUMN audit_hash TEXT", [])?;
    }

    // (Re)create the audit indexes once the shape is settled — the unique hash index
    // is what makes provenance union-merge (INSERT OR IGNORE) a no-op on replay.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_ca_entry
             ON change_audit(tenant_id, entry_id, ts);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_ca_hash
             ON change_audit(tenant_id, audit_hash);",
    )?;
    Ok(())
}

/// PRAGMA-guarded column existence check (SQLite has no `ADD COLUMN IF NOT EXISTS`).
fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// True when `change_audit.id` still has the legacy INTEGER AUTOINCREMENT shape.
fn audit_id_is_integer(conn: &Connection) -> Result<bool> {
    let mut stmt = conn.prepare("PRAGMA table_info(change_audit)")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == "id" {
            let declared_type: String = row.get(2)?;
            return Ok(declared_type.to_ascii_uppercase().contains("INT"));
        }
    }
    Ok(false)
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
        updated_at: row.get("updated_at")?,
        updated_by: row.get("updated_by")?,
    })
}

/// Canonical Blake3 hash of an audit row's content:
/// `Blake3(entry_id ‖ transition ‖ actor ‖ ts ‖ detail)` (CYAN_FORMAT_SPEC §6.1).
/// The local `id` (a uuid) is deliberately excluded — two peers observing the same
/// transition produce the same audit_hash, so provenance merges by union under the
/// unique `(tenant_id, audit_hash)` index.
pub fn compute_audit_hash(
    entry_id: &str,
    transition: &str,
    actor: Option<&str>,
    ts: i64,
    detail: Option<&str>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(entry_id.as_bytes());
    hasher.update(b"\n");
    hasher.update(transition.as_bytes());
    hasher.update(b"\n");
    hasher.update(actor.unwrap_or("").as_bytes());
    hasher.update(b"\n");
    hasher.update(ts.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(detail.unwrap_or("").as_bytes());
    hasher.finalize().to_hex().to_string()
}

fn audit(
    conn: &Connection,
    entry_id: &str,
    tenant_id: &str,
    transition: &str,
    actor: Option<&str>,
    detail: Option<&str>,
) -> Result<()> {
    let ts = now();
    let id = uuid::Uuid::new_v4().to_string();
    let audit_hash = compute_audit_hash(entry_id, transition, actor, ts, detail);
    // INSERT OR IGNORE + unique (tenant_id, audit_hash): an identical transition
    // observed twice (local replay or a peer's copy arriving over sync) unions to
    // one row instead of duplicating.
    conn.execute(
        "INSERT OR IGNORE INTO change_audit \
            (id, entry_id, tenant_id, transition, actor, ts, detail, audit_hash) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![id, entry_id, tenant_id, transition, actor, ts, detail, audit_hash],
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

/// get_entry(tenant, entry_id) → the single entry, tenant-scoped. Public accessor
/// over the internal row loader so callers (e.g. the review-loop state machine)
/// can read one entry without re-implementing the query. Errors if not found.
pub fn get_entry(conn: &Connection, tenant_id: &str, entry_id: &str) -> Result<ChangeEntry> {
    get_entry_row(conn, tenant_id, entry_id)
}

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
        "UPDATE change_entry SET state=?1, approved_by=?2, approved_at=?3, updated_at=?4, updated_by=?5 \
         WHERE tenant_id=?6 AND id=?7",
        params![new_state, by, approved_at, now(), by, tenant_id, entry_id],
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
        "UPDATE change_entry SET active=?1, updated_at=?2, updated_by=?3 WHERE tenant_id=?4 AND id=?5",
        params![active as i64, now(), by, tenant_id, entry_id],
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
        "UPDATE change_entry SET state='superseded', active=0, superseded_by=?1, updated_at=?2, updated_by=?3 \
         WHERE tenant_id=?4 AND id=?5",
        params![appended.id, now(), appended.proposed_by, tenant_id, old_entry_id],
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

/// snapshot(asset, branch) → freeze active set → change_version (+ list_hash and
/// cut_hash — the full frozen-list identity and the ops-only picture identity).
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
    // The picture identity: only the actionable ops (kind=="op"), in the same order.
    let op_hashes: Vec<String> = entries
        .iter()
        .filter(|e| e.kind == "op")
        .map(|e| e.entry_hash.clone())
        .collect();
    let cut_hash = compute_cut_hash(asset_hash, &op_hashes);

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
        cut_hash: cut_hash.clone(),
        entry_hashes: entry_hashes.clone(),
        created_at: now(),
        outcome: "pending".to_string(),
    };

    conn.execute(
        "INSERT INTO change_version (\
            version_id, asset_hash, tenant_id, branch, version_no, list_hash, \
            cut_hash, entry_hashes, created_at, outcome) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            version.version_id,
            version.asset_hash,
            version.tenant_id,
            version.branch,
            version.version_no,
            version.list_hash,
            version.cut_hash,
            serde_json::to_string(&version.entry_hashes)?,
            version.created_at,
            version.outcome,
        ],
    )?;

    // Stamp version_ref on entries that didn't have one (first appearance). Bumps the
    // lifecycle clock so the stamp is visible to the anti-entropy `ce` lane and rides
    // the next sweep to peers (version_ref is lifecycle ABOUT the entry, never content).
    for e in &entries {
        if e.version_ref.is_none() {
            conn.execute(
                "UPDATE change_entry SET version_ref=?1, updated_at=?2 \
                 WHERE tenant_id=?3 AND id=?4 AND version_ref IS NULL",
                params![version.version_id, now(), tenant_id, e.id],
            )?;
        }
    }

    // Advance the branch head (bumping the branch's lifecycle clock — the head
    // moving IS the branch-level LWW event, CYAN_FORMAT_SPEC §6.1).
    conn.execute(
        "INSERT INTO change_branch (tenant_id, asset_hash, branch, head_version, created_at, updated_at) \
         VALUES (?1,?2,?3,?4,?5,?5) \
         ON CONFLICT(tenant_id, asset_hash, branch) \
         DO UPDATE SET head_version=excluded.head_version, updated_at=excluded.updated_at",
        params![tenant_id, asset_hash, branch, version.version_id, now()],
    )?;

    Ok(version)
}

/// branch(asset, from_branch, new_branch) → fork the active set (today/tomorrow/day-after).
///
/// Copies the active entries of `from_branch` onto `new_branch` as fresh entries
/// (new ids, keeping state/active so the fork starts with the same conform set).
/// `branch` IS part of the content hash (see `compute_entry_hash`), so each copy
/// re-hashes under the new branch: the same edit on two branches is two distinct
/// entries — a lineage fork. Only identical content on the SAME branch dedups
/// (the P2P union-merge key). Registers the new branch.
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
        "INSERT OR IGNORE INTO change_branch (tenant_id, asset_hash, branch, head_version, created_at, updated_at) \
         VALUES (?1,?2,?3,NULL,?4,?4)",
        params![tenant_id, asset_hash, new_branch, now()],
    )?;
    Ok(forked)
}

/// branch_from_version(asset, from_version_id, new_branch) → fork the FROZEN entry
/// set of a specific version — not the current head — as the new branch's starting
/// active set (CYAN_FORMAT_QA: "branch(from_version) additive param: build").
///
/// Resolves the version's immutable `entry_hashes` back to rows in frozen order,
/// then runs the same clone/append flow as `branch()`: new ids, content re-hashed
/// under the new branch (branch IS identity). Because lifecycle may have moved on
/// since the snapshot (an entry superseded in a later version), each clone is
/// restored as `active`; a clone whose current state is `superseded`/`rejected`
/// restarts as `proposed` on the fork (approval is re-earned per branch — the
/// frozen set never recorded which approval it carried at freeze time).
pub fn branch_from_version(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    from_version_id: &str,
    new_branch: &str,
) -> Result<Vec<ChangeEntry>> {
    if new_branch.trim().is_empty() {
        return Err(anyhow!("new_branch required"));
    }
    let version = get_version(conn, tenant_id, from_version_id)?;
    if version.asset_hash != asset_hash {
        return Err(anyhow!(
            "version {} belongs to asset {}, not {}",
            from_version_id,
            version.asset_hash,
            asset_hash
        ));
    }
    let mut forked = Vec::with_capacity(version.entry_hashes.len());
    for h in &version.entry_hashes {
        let e = conn
            .query_row(
                "SELECT * FROM change_entry WHERE tenant_id=?1 AND entry_hash=?2",
                params![tenant_id, h],
                row_to_entry,
            )
            .map_err(|err| anyhow!("entry for frozen hash {} not found: {}", h, err))?;
        let mut clone = e;
        clone.id = uuid::Uuid::new_v4().to_string();
        clone.entry_hash = String::new(); // recompute under append (branch IS identity)
        clone.branch = Some(new_branch.to_string());
        clone.version_ref = None;
        clone.superseded_by = None;
        clone.outcome = None;
        clone.active = true; // the frozen set IS the fork's starting active set
        if clone.state == "superseded" || clone.state == "rejected" {
            clone.state = "proposed".to_string();
            clone.approved_by = None;
            clone.approved_at = None;
        }
        forked.push(append(conn, asset_hash, new_branch, clone)?);
    }
    conn.execute(
        "INSERT OR IGNORE INTO change_branch (tenant_id, asset_hash, branch, head_version, created_at, updated_at) \
         VALUES (?1,?2,?3,NULL,?4,?4)",
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
            // WOW verification finding (2026-07-08): the frozen plan feeds the
            // proxy⇄master conform map — the coordinates the player pins markers
            // with and the sensor remaps comments through. Only HUMAN-APPROVED
            // (or applied) ops may shape it: a proposed-but-never-approved op in
            // the plan describes a cut that was never approved or rendered, and
            // a post-snapshot rejection must fall out too. The render path
            // (approved_ops) already enforces this; the plan now matches it.
            && matches!(e.state.as_str(), "approved" | "applied")
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

/// approved_ops(asset, branch) → the CONFIRMED mechanical edits, in apply order.
///
/// The confirm→conform leg's op source (FORMAT_SUPERSET Part 7a — "Cyan applies
/// MECHANICAL edits itself"): the currently `active`, `approved`, `kind=op` entries
/// for `(asset, branch)`, ordered by `seq` then `id`. Unlike `conform_plan` (which
/// projects a FROZEN version's ops) this reads the LIVE active set, so it reflects
/// exactly the ops a human confirmed via `confirm_op` this round. Notes/markers
/// (`kind != "op"`) and unconfirmed proposals (`state != "approved"`) and reversed
/// entries (`active = 0`) are excluded — creative taste is NEVER conformed.
pub fn approved_ops(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
) -> Result<Vec<ConformOp>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM change_entry \
         WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3 \
           AND kind='op' AND active=1 AND state='approved' \
         ORDER BY seq ASC, id ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id, asset_hash, branch], row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut ops = Vec::with_capacity(rows.len());
    for e in rows {
        if let Some(op) = e.op.clone() {
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
                cut_hash, entry_hashes, created_at, outcome \
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
                // NULL for versions snapshotted before the cut_hash column existed.
                cut_hash: row
                    .get::<_, Option<String>>("cut_hash")?
                    .unwrap_or_default(),
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
        "UPDATE change_entry SET outcome=?1, updated_at=?4 \
         WHERE tenant_id=?2 AND version_ref=?3 AND (outcome IS NULL OR outcome='pending')",
        params![outcome, tenant_id, version_id, now()],
    )?;
    Ok(())
}

// ============================================================================
// Echo suppression (CYAN_FORMAT_QA gap 3) — the own_refs breadcrumb table.
// ============================================================================
//
// When an ACTUATOR writes back to an external system (posts a Frame.io comment,
// stamps a status), it records the remote id it just created here. The SENSOR leg
// checks `is_own_source_ref` before ingesting a remote item — our own write-backs
// never re-enter the ledger as new entries (the echo loop at the boundary).

/// Record one actuator write-back breadcrumb: we created `(source, source_ref)`
/// remotely. Idempotent — recording the same write-back twice is a no-op.
pub fn record_own_ref(
    conn: &Connection,
    tenant_id: &str,
    source: &str,
    source_ref: &str,
) -> Result<()> {
    if tenant_id.trim().is_empty() || source.trim().is_empty() || source_ref.trim().is_empty() {
        return Err(anyhow!("record_own_ref: tenant_id, source and source_ref required"));
    }
    conn.execute(
        "INSERT OR IGNORE INTO own_refs (tenant_id, source, source_ref, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![tenant_id, source, source_ref, now()],
    )?;
    Ok(())
}

/// True iff `(source, source_ref)` was written back by one of OUR actuators —
/// the sensor leg's echo check. Tenant-scoped.
pub fn is_own_source_ref(
    conn: &Connection,
    tenant_id: &str,
    source: &str,
    source_ref: &str,
) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM own_refs WHERE tenant_id=?1 AND source=?2 AND source_ref=?3",
        params![tenant_id, source, source_ref],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

// ============================================================================
// P2P sync (CYAN_FORMAT_SPEC §6) — wire rows, apply paths, list reads.
// ============================================================================
//
// The ledger replicates over the SAME three legs the rest of group state uses:
// live gossip deltas (`NetworkEvent::Change*`), the join-time snapshot (the five
// review tables ride the `Metadata` frame), and the anti-entropy digest sweep
// (`ce`/`cv`/`cb`/`ca`/`rs` lanes in `anti_entropy::group_digest`). Everything
// below is idempotent by construction — content unions by `entry_hash`, versions
// union by `version_id`, audits union by `audit_hash`, lifecycle/branch/review
// rows are one LWW lane keyed `updated_at` (ties: higher actor id, §6.3) — so a
// delta applied twice, or a delta racing a snapshot merge, converges identically.

/// One branch head-pointer row (`change_branch`) — replicated LWW on `updated_at`
/// (the head moving IS the branch-level event).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeBranch {
    pub tenant_id: String,
    pub asset_hash: String,
    pub branch: String,
    #[serde(default)]
    pub head_version: Option<String>,
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

/// One content-addressed audit row (`change_audit`). Provenance is global, not
/// local observability (§6.1): rows union across peers by `audit_hash`, so the
/// trail preserves BOTH histories even when lifecycle LWW discards a transition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeAudit {
    pub id: String,
    pub entry_id: String,
    pub tenant_id: String,
    pub transition: String,
    #[serde(default)]
    pub actor: Option<String>,
    pub ts: i64,
    #[serde(default)]
    pub detail: Option<String>,
    /// `None` only for legacy rows that predate content addressing.
    #[serde(default)]
    pub audit_hash: Option<String>,
}

/// The full lifecycle projection of one entry — the `ChangeEntryLifecycle` wire
/// payload (§6.2). Carries `entry_hash` alongside `entry_id` so a receiver whose
/// union-dedup kept a different row id for the same content still resolves it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LifecycleDelta {
    pub entry_id: String,
    #[serde(default)]
    pub entry_hash: String,
    pub state: String,
    pub active: bool,
    #[serde(default)]
    pub approved_by: Option<String>,
    #[serde(default)]
    pub approved_at: Option<i64>,
    #[serde(default)]
    pub superseded_by: Option<String>,
    #[serde(default)]
    pub version_ref: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    pub updated_at: i64,
    #[serde(default)]
    pub updated_by: Option<String>,
    /// The audit row the sender minted for this transition — unioned on apply even
    /// when the LWW discards the lifecycle change itself (nothing is lost, §6.3).
    #[serde(default)]
    pub audit: Option<ChangeAudit>,
}

/// The ONE lifecycle LWW comparison (§6.3): a write wins iff its clock is strictly
/// newer, or — equal clocks from concurrent writers — its actor id sorts higher
/// (deterministic on every peer). A zero clock (never moved) never wins a tie.
fn lww_wins(in_at: i64, in_by: Option<&str>, cur_at: i64, cur_by: Option<&str>) -> bool {
    in_at > cur_at || (in_at == cur_at && in_at > 0 && in_by.unwrap_or("") > cur_by.unwrap_or(""))
}

fn row_to_branch(row: &rusqlite::Row) -> rusqlite::Result<ChangeBranch> {
    Ok(ChangeBranch {
        tenant_id: row.get("tenant_id")?,
        asset_hash: row.get("asset_hash")?,
        branch: row.get("branch")?,
        head_version: row.get("head_version")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

fn row_to_audit(row: &rusqlite::Row) -> rusqlite::Result<ChangeAudit> {
    Ok(ChangeAudit {
        id: row.get("id")?,
        entry_id: row.get("entry_id")?,
        tenant_id: row.get("tenant_id")?,
        transition: row.get("transition")?,
        actor: row.get("actor")?,
        ts: row.get("ts")?,
        detail: row.get("detail")?,
        audit_hash: row.get("audit_hash")?,
    })
}

/// Load one branch head-pointer row.
pub fn get_branch(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
) -> Result<Option<ChangeBranch>> {
    conn.query_row(
        "SELECT * FROM change_branch WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3",
        params![tenant_id, asset_hash, branch],
        row_to_branch,
    )
    .optional()
    .map_err(Into::into)
}

/// The full lifecycle projection of `entry_id` plus its newest audit row — what a
/// sender puts on the wire after a local transition.
pub fn lifecycle_delta_for(
    conn: &Connection,
    tenant_id: &str,
    entry_id: &str,
) -> Result<LifecycleDelta> {
    let e = get_entry_row(conn, tenant_id, entry_id)?;
    let audit = conn
        .query_row(
            "SELECT * FROM change_audit WHERE tenant_id=?1 AND entry_id=?2 \
             ORDER BY ts DESC, id DESC LIMIT 1",
            params![tenant_id, entry_id],
            row_to_audit,
        )
        .optional()?;
    Ok(LifecycleDelta {
        entry_id: e.id,
        entry_hash: e.entry_hash,
        state: e.state,
        active: e.active,
        approved_by: e.approved_by,
        approved_at: e.approved_at,
        superseded_by: e.superseded_by,
        version_ref: e.version_ref,
        outcome: e.outcome,
        updated_at: e.updated_at,
        updated_by: e.updated_by,
        audit,
    })
}

/// Apply a peer's `ChangeEntryAppended` (or one snapshot-frame entry row): the
/// content lane unions under the unique `(tenant, entry_hash)` index — the same
/// dedup key `append` enforces — then the incoming lifecycle lane lands iff its
/// clock wins the LWW. Returns the local row.
///
/// Deliberately does NOT route through `append`: `append` mints a local "append"
/// audit row, and a REMOTE apply must not re-author provenance (N peers would each
/// mint one per entry). The sender's own append audit replicates on the `ca` lane
/// (lifecycle deltas + snapshot/sweep) instead. The closed op vocab is still
/// enforced on the inbound row, and the hash is recomputed — never trusted from
/// the wire.
pub fn apply_entry(conn: &Connection, entry: &ChangeEntry) -> Result<ChangeEntry> {
    let mut e = entry.clone();
    if e.branch.as_deref().unwrap_or("").is_empty() {
        e.branch = Some("main".to_string());
    }
    if e.id.trim().is_empty() {
        e.id = uuid::Uuid::new_v4().to_string();
    }
    if e.created_at == 0 {
        e.created_at = now();
    }
    if e.kind.trim().is_empty() {
        e.kind = "note".to_string();
    }
    if e.state.trim().is_empty() {
        e.state = "proposed".to_string();
    }
    validate_entry(&e)?;
    e.entry_hash = compute_entry_hash(&e);

    // Content union: OR IGNORE on both the id PK and the (tenant, entry_hash)
    // unique index — a replayed delta or a concurrent identical append is a no-op.
    conn.execute(
        "INSERT OR IGNORE INTO change_entry (\
            id, entry_hash, asset_hash, tenant_id, branch, track, tc_in, tc_out, \
            kind, op, params, intent, source, source_ref, author, role, proposed_by, \
            created_at, state, active, approved_by, approved_at, supersedes, \
            superseded_by, seq, depends_on, version_ref, outcome, updated_at, updated_by) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,\
                 ?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30)",
        params![
            e.id,
            e.entry_hash,
            e.asset_hash,
            e.tenant_id,
            e.branch,
            e.track,
            e.tc_in,
            e.tc_out,
            e.kind,
            e.op,
            e.params.to_string(),
            e.intent,
            e.source,
            e.source_ref,
            e.author,
            e.role,
            e.proposed_by,
            e.created_at,
            e.state,
            e.active as i64,
            e.approved_by,
            e.approved_at,
            e.supersedes,
            e.superseded_by,
            e.seq,
            e.depends_on,
            e.version_ref,
            e.outcome,
            e.updated_at,
            e.updated_by,
        ],
    )?;

    // The row that actually holds this content locally (ours on a union hit).
    let cur = conn
        .query_row(
            "SELECT * FROM change_entry WHERE tenant_id=?1 AND entry_hash=?2",
            params![e.tenant_id, e.entry_hash],
            row_to_entry,
        )
        .map_err(|err| anyhow!("apply_entry: row for hash {} not found: {}", e.entry_hash, err))?;

    // Lifecycle LWW (one lane per entry, §6.3).
    if lww_wins(
        e.updated_at,
        e.updated_by.as_deref(),
        cur.updated_at,
        cur.updated_by.as_deref(),
    ) {
        conn.execute(
            "UPDATE change_entry SET state=?1, active=?2, approved_by=?3, approved_at=?4, \
                superseded_by=?5, version_ref=COALESCE(?6, version_ref), outcome=?7, \
                updated_at=?8, updated_by=?9 \
             WHERE tenant_id=?10 AND id=?11",
            params![
                e.state,
                e.active as i64,
                e.approved_by,
                e.approved_at,
                e.superseded_by,
                e.version_ref,
                e.outcome,
                e.updated_at,
                e.updated_by,
                e.tenant_id,
                cur.id,
            ],
        )?;
    }
    get_entry_row(conn, &e.tenant_id, &cur.id)
}

/// Apply a peer's `ChangeEntryLifecycle`: union the audit row FIRST (provenance is
/// never lost), then apply the lifecycle projection iff `updated_at` wins the LWW
/// (§6.3). Returns whether the lifecycle landed. An entry we don't hold yet is a
/// no-op (`false`) — the content row arrives via its own lane / the next sweep.
pub fn apply_lifecycle(conn: &Connection, tenant_id: &str, d: &LifecycleDelta) -> Result<bool> {
    if let Some(a) = &d.audit {
        apply_audit(conn, a)?;
    }
    // Resolve by id first; fall back to entry_hash (concurrent identical-content
    // appends dedup to one row whose id may differ from the sender's).
    let cur = conn
        .query_row(
            "SELECT * FROM change_entry WHERE tenant_id=?1 AND id=?2",
            params![tenant_id, d.entry_id],
            row_to_entry,
        )
        .optional()?;
    let cur = match cur {
        Some(c) => Some(c),
        None if !d.entry_hash.is_empty() => conn
            .query_row(
                "SELECT * FROM change_entry WHERE tenant_id=?1 AND entry_hash=?2",
                params![tenant_id, d.entry_hash],
                row_to_entry,
            )
            .optional()?,
        None => None,
    };
    let Some(cur) = cur else {
        return Ok(false);
    };
    if !lww_wins(
        d.updated_at,
        d.updated_by.as_deref(),
        cur.updated_at,
        cur.updated_by.as_deref(),
    ) {
        return Ok(false);
    }
    conn.execute(
        "UPDATE change_entry SET state=?1, active=?2, approved_by=?3, approved_at=?4, \
            superseded_by=?5, version_ref=COALESCE(?6, version_ref), outcome=?7, \
            updated_at=?8, updated_by=?9 \
         WHERE tenant_id=?10 AND id=?11",
        params![
            d.state,
            d.active as i64,
            d.approved_by,
            d.approved_at,
            d.superseded_by,
            d.version_ref,
            d.outcome,
            d.updated_at,
            d.updated_by,
            tenant_id,
            cur.id,
        ],
    )?;
    Ok(true)
}

/// Apply a peer's `ChangeVersionCreated` (or one snapshot-frame version row):
/// immutable union by `version_id` (§6.3 — two peers snapshotting concurrently keep
/// BOTH versions). The set-once `outcome` label still unions onto an existing row
/// (`pending` → shipped/rejected is monotone; never overwritten once set).
pub fn apply_version(conn: &Connection, v: &ChangeVersion) -> Result<bool> {
    let n = conn.execute(
        "INSERT OR IGNORE INTO change_version (\
            version_id, asset_hash, tenant_id, branch, version_no, list_hash, \
            cut_hash, entry_hashes, created_at, outcome) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            v.version_id,
            v.asset_hash,
            v.tenant_id,
            v.branch,
            v.version_no,
            v.list_hash,
            v.cut_hash,
            serde_json::to_string(&v.entry_hashes)?,
            v.created_at,
            v.outcome,
        ],
    )?;
    if n == 0 && v.outcome != "pending" {
        conn.execute(
            "UPDATE change_version SET outcome=?1 \
             WHERE tenant_id=?2 AND version_id=?3 AND outcome='pending'",
            params![v.outcome, v.tenant_id, v.version_id],
        )?;
    }
    Ok(n > 0)
}

/// Apply a peer's `ChangeBranchHead` (or one snapshot-frame branch row): LWW upsert
/// on `updated_at` — a stale head move never clobbers a newer one.
pub fn apply_branch_head(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    head_version: Option<&str>,
    updated_at: i64,
) -> Result<bool> {
    let n = conn.execute(
        "INSERT INTO change_branch (tenant_id, asset_hash, branch, head_version, created_at, updated_at) \
         VALUES (?1,?2,?3,?4,?5,?5) \
         ON CONFLICT(tenant_id, asset_hash, branch) DO UPDATE SET \
            head_version=excluded.head_version, updated_at=excluded.updated_at \
         WHERE excluded.updated_at > change_branch.updated_at",
        params![tenant_id, asset_hash, branch, head_version, updated_at],
    )?;
    Ok(n > 0)
}

/// Apply a peer's audit row: union by the unique `(tenant_id, audit_hash)` index
/// (identical transitions observed on two peers collapse to one row); the `id` PK
/// catches legacy rows with no hash. Preserves the sender's id/ts — provenance is
/// content, never re-stamped.
pub fn apply_audit(conn: &Connection, a: &ChangeAudit) -> Result<bool> {
    let n = conn.execute(
        "INSERT OR IGNORE INTO change_audit \
            (id, entry_id, tenant_id, transition, actor, ts, detail, audit_hash) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![a.id, a.entry_id, a.tenant_id, a.transition, a.actor, a.ts, a.detail, a.audit_hash],
    )?;
    Ok(n > 0)
}

/// Every entry a tenant holds, in canonical order — the snapshot-frame read.
pub fn list_entries_by_tenant(conn: &Connection, tenant_id: &str) -> Result<Vec<ChangeEntry>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM change_entry WHERE tenant_id=?1 ORDER BY seq ASC, created_at ASC, id ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Every version a tenant holds.
pub fn list_versions_by_tenant(conn: &Connection, tenant_id: &str) -> Result<Vec<ChangeVersion>> {
    let mut stmt = conn.prepare(
        "SELECT version_id, asset_hash, tenant_id, branch, version_no, list_hash, \
                cut_hash, entry_hashes, created_at, outcome \
         FROM change_version WHERE tenant_id=?1 ORDER BY created_at ASC, version_id ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id], |row| {
            let hashes_str: String = row.get("entry_hashes")?;
            Ok(ChangeVersion {
                version_id: row.get("version_id")?,
                asset_hash: row.get("asset_hash")?,
                tenant_id: row.get("tenant_id")?,
                branch: row.get("branch")?,
                version_no: row.get("version_no")?,
                list_hash: row.get("list_hash")?,
                cut_hash: row
                    .get::<_, Option<String>>("cut_hash")?
                    .unwrap_or_default(),
                entry_hashes: serde_json::from_str(&hashes_str).unwrap_or_default(),
                created_at: row.get("created_at")?,
                outcome: row.get("outcome")?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Every branch head-pointer a tenant holds.
pub fn list_branches_by_tenant(conn: &Connection, tenant_id: &str) -> Result<Vec<ChangeBranch>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM change_branch WHERE tenant_id=?1 ORDER BY asset_hash ASC, branch ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_branch)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Every audit row a tenant holds.
pub fn list_audits_by_tenant(conn: &Connection, tenant_id: &str) -> Result<Vec<ChangeAudit>> {
    let mut stmt =
        conn.prepare("SELECT * FROM change_audit WHERE tenant_id=?1 ORDER BY ts ASC, id ASC")?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_audit)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
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

    let lock = crate::storage::try_db()
        .ok_or_else(|| anyhow!("DB not initialized"))?
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

    // Ledger-sync broadcast bridge (CYAN_FORMAT_SPEC §6.2): after a successful LOCAL
    // mutation, queue the matching delta for the group topic (tenant == group id) via
    // the engine command loop. `queue_command` is a no-op without a running system,
    // and the topic simply isn't joined for non-group tenants — a local store op
    // never fails or blocks on sync. Receivers apply through the same idempotent
    // fns, so echoes/replays are no-ops (no rebroadcast loop: applies don't queue).
    let queue_entry = |entry: &ChangeEntry| {
        crate::queue_command(crate::models::commands::CommandMsg::ChangeEntryAppended {
            tenant_id: entry.tenant_id.clone(),
            entry: Box::new(entry.clone()),
        });
    };
    let queue_lifecycle = |conn: &Connection, tenant_id: &str, entry_id: &str| {
        if let Ok(delta) = lifecycle_delta_for(conn, tenant_id, entry_id) {
            crate::queue_command(crate::models::commands::CommandMsg::ChangeEntryLifecycle {
                tenant_id: tenant_id.to_string(),
                delta: Box::new(delta),
            });
        }
    };
    let queue_version = |version: &ChangeVersion| {
        crate::queue_command(crate::models::commands::CommandMsg::ChangeVersionCreated {
            tenant_id: version.tenant_id.clone(),
            version: Box::new(version.clone()),
        });
    };
    let queue_branch_head = |conn: &Connection, tenant_id: &str, asset: &str, br: &str| {
        if let Ok(Some(b)) = get_branch(conn, tenant_id, asset, br) {
            crate::queue_command(crate::models::commands::CommandMsg::ChangeBranchHead {
                tenant_id: b.tenant_id,
                asset_hash: b.asset_hash,
                branch: b.branch,
                head_version: b.head_version,
                updated_at: b.updated_at,
            });
        }
    };

    match op {
        // ── BOARD-keyed app dialect (the macOS review player): the full envelope
        // {asset_hash, branch, version, review_state, entries}. Additive.
        "list" => {
            let board = s(&cmd, "board_id")?;
            let (tenant_r, asset, br) = crate::review_loop::resolve_board_review(
                conn,
                &board,
                cmd.get("asset_hash").and_then(|v| v.as_str()),
            )?;
            Ok(crate::review_loop::board_envelope(conn, &board, &tenant_r, &asset, &br)?)
        }
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
            queue_entry(&out);
            Ok(serde_json::to_value(out)?)
        }
        "set_state" => {
            let tenant_id = tenant(&cmd)?;
            let entry_id = s(&cmd, "entry_id")?;
            let out = set_state(
                conn,
                &tenant_id,
                &entry_id,
                &s(&cmd, "state")?,
                cmd.get("by").and_then(|v| v.as_str()),
            )?;
            queue_lifecycle(conn, &tenant_id, &entry_id);
            Ok(serde_json::to_value(out)?)
        }
        "set_active" => {
            let tenant_id = tenant(&cmd)?;
            let entry_id = s(&cmd, "entry_id")?;
            let active = cmd.get("active").and_then(|v| v.as_bool()).unwrap_or(true);
            let out = set_active(
                conn,
                &tenant_id,
                &entry_id,
                active,
                cmd.get("by").and_then(|v| v.as_str()),
            )?;
            queue_lifecycle(conn, &tenant_id, &entry_id);
            Ok(serde_json::to_value(out)?)
        }
        "supersede" => {
            let new_entry: ChangeEntry = serde_json::from_value(
                cmd.get("entry").cloned().ok_or_else(|| anyhow!("missing 'entry'"))?,
            )
            .map_err(|e| anyhow!("bad entry: {}", e))?;
            let old_entry_id = s(&cmd, "old_entry_id")?;
            let out = supersede(conn, &old_entry_id, new_entry)?;
            // Two deltas: the new content row + the old entry's superseded lifecycle.
            queue_entry(&out);
            queue_lifecycle(conn, &out.tenant_id, &old_entry_id);
            Ok(serde_json::to_value(out)?)
        }
        "snapshot" => {
            let branch = cmd
                .get("branch")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            let tenant_id = tenant(&cmd)?;
            let asset = s(&cmd, "asset_hash")?;
            let out = snapshot(conn, &tenant_id, &asset, &branch)?;
            // Two deltas: the immutable version + the branch head move (LWW). The
            // version_ref stamps on member entries ride the `ce` digest lane.
            queue_version(&out);
            queue_branch_head(conn, &tenant_id, &asset, &branch);
            Ok(serde_json::to_value(out)?)
        }
        "branch" => {
            let tenant_id = tenant(&cmd)?;
            let asset = s(&cmd, "asset_hash")?;
            let new_branch = s(&cmd, "new_branch")?;
            let out = branch(conn, &tenant_id, &asset, &s(&cmd, "from_branch")?, &new_branch)?;
            for e in &out {
                queue_entry(e);
            }
            queue_branch_head(conn, &tenant_id, &asset, &new_branch);
            Ok(serde_json::to_value(out)?)
        }
        "branch_from_version" => {
            let tenant_id = tenant(&cmd)?;
            let asset = s(&cmd, "asset_hash")?;
            let new_branch = s(&cmd, "new_branch")?;
            let out = branch_from_version(
                conn,
                &tenant_id,
                &asset,
                &s(&cmd, "from_version_id")?,
                &new_branch,
            )?;
            for e in &out {
                queue_entry(e);
            }
            queue_branch_head(conn, &tenant_id, &asset, &new_branch);
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
        // The proxy ⇄ master frame map for a version (src/conform_map.rs) — the
        // sensor leg's remap entry point (CYAN_FORMAT_QA gap 1). Additively also
        // BOARD-keyed (WOW-2, the macOS review player): `board_id` resolves the
        // board's review context → its branch head → that version's map, so the
        // player can pin a master-anchored comment onto the conformed proxy
        // (master 30 IS proxy 18 after a 12-frame head trim). No head version
        // yet ⇒ the identity map (v1 — nothing moved).
        "conform_map" => {
            if let Some(board) = cmd.get("board_id").and_then(|v| v.as_str()) {
                let (tenant_r, asset, br) = crate::review_loop::resolve_board_review(
                    conn,
                    board,
                    cmd.get("asset_hash").and_then(|v| v.as_str()),
                )?;
                let head = get_branch(conn, &tenant_r, &asset, &br)?.and_then(|b| b.head_version);
                let out = match head {
                    Some(v) => crate::conform_map::for_version(conn, &tenant_r, &v)?,
                    None => crate::conform_map::build(&[]),
                };
                return Ok(serde_json::to_value(out)?);
            }
            let out = crate::conform_map::for_version(conn, &tenant(&cmd)?, &s(&cmd, "version_id")?)?;
            Ok(serde_json::to_value(out)?)
        }
        // Echo suppression (CYAN_FORMAT_QA gap 3): actuator write-back breadcrumbs.
        "record_own_ref" => {
            record_own_ref(conn, &tenant(&cmd)?, &s(&cmd, "source")?, &s(&cmd, "source_ref")?)?;
            Ok(serde_json::json!({ "ok": true }))
        }
        "is_own_source_ref" => {
            let own = is_own_source_ref(conn, &tenant(&cmd)?, &s(&cmd, "source")?, &s(&cmd, "source_ref")?)?;
            Ok(serde_json::json!({ "own": own }))
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
            let tenant_id = tenant(&cmd)?;
            let version_id = s(&cmd, "version_id")?;
            set_outcome(conn, &tenant_id, &version_id, &s(&cmd, "outcome")?)?;
            // Re-broadcast the version row: `apply_version` unions the set-once
            // outcome onto a peer's existing row (pending → shipped/rejected).
            // Member-entry outcome stamps bumped their clocks ⇒ the `ce` lane heals them.
            if let Ok(v) = get_version(conn, &tenant_id, &version_id) {
                queue_version(&v);
            }
            Ok(serde_json::json!({ "ok": true }))
        }
        // ── asset registry (src/asset_registry.rs) — additive ops on the same JSON
        // seam; where an asset_hash resolves to what the asset IS (kind/fps/duration,
        // derivation edges, remote refs like the Frame.io file id).
        "asset_upsert" => {
            let asset: crate::asset_registry::Asset = serde_json::from_value(
                cmd.get("asset")
                    .cloned()
                    .ok_or_else(|| anyhow!("missing 'asset'"))?,
            )
            .map_err(|e| anyhow!("bad asset: {}", e))?;
            let out = crate::asset_registry::upsert(conn, &asset)?;
            Ok(serde_json::to_value(out)?)
        }
        "asset_get" => {
            let out = crate::asset_registry::get(conn, &tenant(&cmd)?, &s(&cmd, "hash")?)?;
            Ok(serde_json::to_value(out)?)
        }
        "asset_set_remote_ref" => {
            let out = crate::asset_registry::set_remote_ref(
                conn,
                &tenant(&cmd)?,
                &s(&cmd, "hash")?,
                &s(&cmd, "key")?,
                &s(&cmd, "value")?,
            )?;
            Ok(serde_json::to_value(out)?)
        }
        other => Err(anyhow!("unknown op '{}'", other)),
    }
}
