// cyan-backend/src/review_loop.rs
//
// The review LOOP as engine machinery — the piece that turns the green parts
// (changelist store, review_state machine, conform_map remap, asset registry)
// into the thing a tenant actually authors and runs
// (CYAN_CHANGELIST_STORE_AND_REVIEW_LOOP.md Part 2, engine delta #3;
// CYAN_REVIEW_LOOP_TRANSITION_CONTRACT.md).
//
// Two halves:
//
//   1. **SENSE → ledger glue** (`ingest_sense_result`): when a workflow's SENSE
//      step completes with a `frameio.list_comments` plugin tool result, each
//      comment walks the forward breadcrumb BACKWARD — proxy file id →
//      `asset_registry` remote_refs → the proxy's `derived_from_version` →
//      `conform_map` → MASTER coordinates — and lands in the change-list as a
//      `kind=note, source=frameio` entry with `params.observed` provenance.
//      Own write-backs are dropped (`is_own_source_ref` echo suppression) and
//      re-ingest dedups on `entry_hash` (append is idempotent). The first NEW
//      note advances the machine IN_REVIEW → NOTES_IN (AUTO, per contract).
//
//   2. **The loop controller** (`register` / `tick` / `record_round_run`): a
//      review-loop workflow pauses at PUBLISH-done (run parks, review_state
//      IN_REVIEW), resumes when SENSE brings new notes (NOTES_IN → the
//      INTERPRET/CONFIRM machinery), and exits on external approval (APPROVED
//      → outcome=shipped) or the max_rounds cap — which forces a HUMAN
//      escalation as a durable ask on the ledger, never a silent stop.
//      Rounds are SEQUENTIAL RUNS (each round = a run, per the dashboard
//      model); the loop identity is the (board, asset) pair, and each run is
//      stamped with the `review_state.round` current when it starts.
//
// Design seam mirrors `changelist`/`review_state`: every op takes an explicit
// `&Connection` so tests run on isolated in-memory DBs; the FFI drives the
// process-global DB through additive ops on the `cyan_review_command` JSON
// surface (see `review_state::dispatch`). Nothing here panics.

use crate::{asset_registry, changelist, conform_map, review_state};
use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Loop status vocab: `active` (looping), `escalated` (round cap hit — parked on
/// a human ask), `exited` (approval or terminal state reached).
pub const LOOP_STATUS_VOCAB: &[&str] = &["active", "escalated", "exited"];

// ============================================================================
// Migration — additive tables, idempotent.
// ============================================================================

/// Create the `review_loop` + `review_loop_run` tables. Idempotent; called from
/// `storage::run_migrations` and directly from tests. Never touches an existing
/// table.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS review_loop (
            tenant_id   TEXT NOT NULL,
            board_id    TEXT NOT NULL,
            asset_hash  TEXT NOT NULL,
            branch      TEXT NOT NULL DEFAULT 'main',
            max_rounds  INTEGER NOT NULL DEFAULT 10,
            status      TEXT NOT NULL DEFAULT 'active',
            outcome     TEXT,
            created_at  INTEGER NOT NULL,
            updated_at  INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, board_id, asset_hash)
        );
        CREATE TABLE IF NOT EXISTS review_loop_run (
            tenant_id   TEXT NOT NULL,
            board_id    TEXT NOT NULL,
            asset_hash  TEXT NOT NULL,
            run_id      TEXT NOT NULL,
            round       INTEGER NOT NULL,
            started_at  INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, board_id, asset_hash, run_id)
        );
        "#,
    )?;
    Ok(())
}

// ============================================================================
// The loop record.
// ============================================================================

/// One registered review loop: a workflow board driving the review of one asset
/// branch. The loop identity is `(board_id, asset_hash)` within the tenant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewLoop {
    pub tenant_id: String,
    pub board_id: String,
    pub asset_hash: String,
    pub branch: String,
    pub max_rounds: i64,
    pub status: String,
    pub outcome: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// One round-run of a loop: rounds are SEQUENTIAL RUNS (never one run id reused
/// across rounds); `round` is `review_state.round` when the run started.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoopRun {
    pub run_id: String,
    pub round: i64,
    pub started_at: i64,
}

fn row_to_loop(row: &rusqlite::Row) -> rusqlite::Result<ReviewLoop> {
    Ok(ReviewLoop {
        tenant_id: row.get("tenant_id")?,
        board_id: row.get("board_id")?,
        asset_hash: row.get("asset_hash")?,
        branch: row.get("branch")?,
        max_rounds: row.get("max_rounds")?,
        status: row.get("status")?,
        outcome: row.get("outcome")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Register (or fetch) the loop for `(board, asset)`. Idempotent: re-registering
/// returns the existing loop unchanged (max_rounds is set once, at registration).
/// Also bootstraps the asset's `review_state` row (DRAFT) if none exists yet.
pub fn register(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
    asset_hash: &str,
    branch: &str,
    max_rounds: i64,
) -> Result<ReviewLoop> {
    if tenant_id.trim().is_empty() || board_id.trim().is_empty() || asset_hash.trim().is_empty() {
        return Err(anyhow!("tenant_id, board_id and asset_hash are required"));
    }
    if max_rounds < 1 {
        return Err(anyhow!("max_rounds must be >= 1"));
    }
    if let Some(existing) = get_loop(conn, tenant_id, board_id, asset_hash)? {
        return Ok(existing);
    }
    let ts = now();
    conn.execute(
        "INSERT INTO review_loop \
            (tenant_id, board_id, asset_hash, branch, max_rounds, status, outcome, created_at, updated_at) \
         VALUES (?1,?2,?3,?4,?5,'active',NULL,?6,?6)",
        params![tenant_id, board_id, asset_hash, branch, max_rounds, ts],
    )?;
    // The loop drives this asset's review machine; stand it up if absent (idempotent).
    review_state::start_draft(conn, tenant_id, asset_hash, branch)
        .map_err(|e| anyhow!("start_draft for loop: {e}"))?;
    get_loop(conn, tenant_id, board_id, asset_hash)?
        .ok_or_else(|| anyhow!("loop row vanished after insert"))
}

/// Fetch the loop for `(board, asset)`, if registered.
pub fn get_loop(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
    asset_hash: &str,
) -> Result<Option<ReviewLoop>> {
    conn.query_row(
        "SELECT * FROM review_loop WHERE tenant_id=?1 AND board_id=?2 AND asset_hash=?3",
        params![tenant_id, board_id, asset_hash],
        row_to_loop,
    )
    .optional()
    .map_err(Into::into)
}

/// Resolve the CURRENT round's proxy Frame.io ref for a workflow board — the input
/// the conform step needs. Walks board → its active review loop(s) → the master asset,
/// then the newest published proxy derived from that master
/// (`asset_registry::latest_published_proxy`). Returns the first board loop that has a
/// published proxy; `None` if the board drives no loop yet or nothing is published.
/// This is the board-state fallback the run-loop conform wire uses when the step does
/// not already carry an explicit `proxy_ref`.
pub fn current_proxy_ref(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
) -> Result<Option<String>> {
    let mut stmt = conn.prepare(
        "SELECT asset_hash FROM review_loop \
         WHERE tenant_id=?1 AND board_id=?2 AND status='active' ORDER BY updated_at DESC",
    )?;
    let masters = stmt
        .query_map(params![tenant_id, board_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for master in masters {
        if let Some(proxy_ref) = asset_registry::latest_published_proxy(conn, tenant_id, &master)? {
            return Ok(Some(proxy_ref));
        }
    }
    Ok(None)
}

// ============================================================================
// SENSE → ledger glue.
// ============================================================================

/// One comment parsed out of a SENSE step's `frameio.list_comments` tool result.
#[derive(Debug, Clone, PartialEq)]
pub struct SenseComment {
    /// The Frame.io comment id — becomes the entry's `source_ref`.
    pub id: String,
    /// The comment text — becomes the entry's `intent`.
    pub text: String,
    /// Comment anchor in PROXY frames (`frame`, falling back to `timestamp`).
    pub frame: i64,
    /// Optional range end in PROXY frames (`frame_out`).
    pub frame_out: Option<i64>,
    /// Comment author display name (`owner.name`, falling back to `author`).
    pub author: Option<String>,
}

/// Frame.io V4 `Comment.timestamp` is a **oneOf** (verified against the live
/// V4 openapi.json, 2026-07-03): an integer FRAME NUMBER *or* a non-drop
/// timecode string `"HH:MM:SS:FF"`. An integer passes through as frames. A
/// timecode string needs the proxy's fps to become a frame index
/// (`(h*3600+m*60+s)*round(fps) + ff` — nominal SMPTE NDF math; 23.976
/// rounds to base 24). Returns `None` for a timecode string when fps is
/// unknown, for drop-frame (`;`-separated) forms, and for anything
/// unparseable — the caller counts those malformed rather than silently
/// pinning the note to frame 0 (the pre-2026-07-03 bug).
fn timestamp_frames(v: &serde_json::Value, fps: Option<f64>) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    let s = v.as_str()?;
    if s.contains(';') {
        return None; // drop-frame timecode: not supported — surface, don't guess
    }
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 4 {
        return None;
    }
    let nums: Vec<i64> = parts
        .iter()
        .map(|p| p.parse::<i64>().ok())
        .collect::<Option<Vec<_>>>()?;
    let (h, m, sec, ff) = (nums[0], nums[1], nums[2], nums[3]);
    let base = fps?.round() as i64;
    if base <= 0 || h < 0 || !(0..60).contains(&m) || !(0..60).contains(&sec) || !(0..base).contains(&ff) {
        return None;
    }
    Some((h * 3600 + m * 60 + sec) * base + ff)
}

/// Parse the comments out of a SENSE step's plugin tool result. The step-result
/// contract (what `execute_local_mcp_tool_step` / the lens path persist as the
/// step's output): the tool's JSON — either the Frame.io V4 envelope
/// `{"data": [ ...comments ]}` or a bare comment array. Per comment:
/// `id` (string, required), `text` (string, required), the proxy-frame anchor
/// from `frame` else `timestamp` (int frames OR `"HH:MM:SS:FF"` timecode —
/// the V4 oneOf; timecode needs `fps`, see [`timestamp_frames`]). A comment
/// with NO anchor field at all is a general file comment → frame 0. A comment
/// whose anchor is PRESENT but unresolvable (timecode without fps, drop-frame,
/// garbage) is malformed and skipped — counted, never guessed onto frame 0.
/// Range end from `frame_out`, else Frame.io's `duration` (int FRAMES per the
/// V4 spec) added to the anchor. Author from `owner.name` else `author`.
/// Comments missing `id`/`text` are malformed and skipped (counted by the
/// caller's report — no silent truncation).
pub fn parse_sense_comments(
    result: &serde_json::Value,
    fps: Option<f64>,
) -> (Vec<SenseComment>, usize) {
    let items = result
        .get("data")
        .and_then(|d| d.as_array())
        .or_else(|| result.as_array());
    let Some(items) = items else {
        return (Vec::new(), 0);
    };
    let mut out = Vec::new();
    let mut malformed = 0usize;
    for item in items {
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let text = item.get("text").and_then(|v| v.as_str()).unwrap_or("");
        if id.is_empty() || text.is_empty() {
            malformed += 1;
            continue;
        }
        let anchor_value = item.get("frame").or_else(|| item.get("timestamp"));
        let frame = match anchor_value {
            None => 0, // no anchor = general file comment → start of media
            Some(v) if v.is_null() => 0,
            Some(v) => match timestamp_frames(v, fps) {
                Some(f) => f,
                None => {
                    // Anchor present but unresolvable — surfacing beats guessing.
                    tracing::warn!(
                        "sense parse: comment {} anchor {:?} unresolvable (fps={:?}) — counted malformed",
                        id, v, fps
                    );
                    malformed += 1;
                    continue;
                }
            },
        };
        let frame_out = item
            .get("frame_out")
            .and_then(|v| v.as_i64())
            .or_else(|| {
                // Frame.io V4: `duration` is an int32 in FRAMES (requires timestamp).
                item.get("duration").and_then(|v| v.as_i64()).map(|d| frame + d)
            });
        let author = item
            .get("owner")
            .and_then(|o| o.get("name"))
            .and_then(|v| v.as_str())
            .or_else(|| item.get("author").and_then(|v| v.as_str()))
            .map(str::to_string);
        out.push(SenseComment {
            id: id.to_string(),
            text: text.to_string(),
            frame,
            frame_out,
            author,
        });
    }
    (out, malformed)
}

/// What one SENSE ingest did — every dropped comment is COUNTED, never silent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SenseIngest {
    /// Entries newly appended this ingest (kind=note, source=frameio).
    pub appended: Vec<changelist::ChangeEntry>,
    /// Comments whose identical content already existed (`entry_hash` dedup).
    pub deduped: usize,
    /// Our own write-backs, dropped by `is_own_source_ref` echo suppression.
    pub own_refs_skipped: usize,
    /// Comments anchored on inserted foreign media — no master coordinates
    /// exist, so they are surfaced here rather than guessed onto the master.
    pub unmappable: usize,
    /// Comments missing `id`/`text` in the tool result.
    pub malformed: usize,
    /// The review state after ingest (advanced IN_REVIEW → NOTES_IN when the
    /// first new note landed).
    pub state: Option<review_state::ReviewState>,
}

/// Ingest a SENSE step's `list_comments` tool result into the change-list.
///
/// `proxy_ref` is the Frame.io file id the SENSE step listed comments for (the
/// same id the PUBLISH actuator recorded via `asset_set_remote_ref`). The walk:
/// proxy_ref → `asset_registry::find_by_remote_ref` → the proxy asset →
/// `derived_from_asset` (the MASTER the ledger anchors to) + `derived_from_version`
/// (the frozen conform plan) → `conform_map::for_version` → remap each comment's
/// proxy frames to MASTER coordinates with `params.observed` provenance →
/// `changelist::append` as `kind=note, source=frameio, source_ref=<comment id>`.
///
/// Echo suppression: a comment whose id we recorded as our own write-back
/// (`record_own_ref`) is skipped. Dedup rides `entry_hash` (append is
/// idempotent). If at least one NEW note landed and the machine is IN_REVIEW,
/// it advances to NOTES_IN (AUTO — contract row "new Frame.io comments").
pub fn ingest_sense_result(
    conn: &Connection,
    tenant_id: &str,
    proxy_ref: &str,
    result: &serde_json::Value,
) -> Result<SenseIngest> {
    let proxy = asset_registry::find_by_remote_ref(conn, tenant_id, "frameio", proxy_ref)?
        .ok_or_else(|| anyhow!("no registered asset carries frameio ref '{proxy_ref}'"))?;
    let master = proxy
        .derived_from_asset
        .clone()
        .ok_or_else(|| anyhow!("proxy asset {} has no derived_from_asset (master)", proxy.hash))?;
    let version_id = proxy.derived_from_version.clone().ok_or_else(|| {
        anyhow!("proxy asset {} has no derived_from_version (conform plan)", proxy.hash)
    })?;
    let version = changelist::get_version(conn, tenant_id, &version_id)?;
    let map = conform_map::for_version(conn, tenant_id, &version_id)?;

    let (comments, malformed) = parse_sense_comments(result, proxy.fps);
    let mut ingest = SenseIngest {
        appended: Vec::new(),
        deduped: 0,
        own_refs_skipped: 0,
        unmappable: 0,
        malformed,
        state: None,
    };

    for c in comments {
        // Echo suppression: our own PUBLISH write-backs never re-enter the ledger.
        if changelist::is_own_source_ref(conn, tenant_id, "frameio", &c.id)? {
            ingest.own_refs_skipped += 1;
            continue;
        }
        // PROXY → MASTER coordinates + `params.observed` provenance. A frame on
        // inserted foreign media has no master coordinates — surfaced, not guessed.
        let (tc_in, tc_out, params) = match conform_map::remap_observed(
            &map,
            proxy_ref,
            c.frame,
            c.frame_out,
            serde_json::json!({}),
        ) {
            Ok(mapped) => mapped,
            Err(e) => {
                tracing::warn!("sense ingest: comment {} unmappable: {e}", c.id);
                ingest.unmappable += 1;
                continue;
            }
        };
        let minted_id = uuid::Uuid::new_v4().to_string();
        let entry = changelist::ChangeEntry {
            id: minted_id.clone(),
            entry_hash: String::new(),
            asset_hash: master.clone(),
            tenant_id: tenant_id.to_string(),
            branch: None, // append stamps the branch
            track: None,
            tc_in,
            tc_out,
            kind: "note".to_string(),
            op: None,
            params,
            intent: c.text.clone(),
            source: Some("frameio".to_string()),
            source_ref: Some(c.id.clone()),
            author: c.author.clone(),
            role: Some("reviewer".to_string()),
            proposed_by: Some("human".to_string()),
            created_at: 0,
            state: String::new(),
            active: true,
            approved_by: None,
            approved_at: None,
            supersedes: None,
            superseded_by: None,
            seq: 0,
            depends_on: None,
            version_ref: None,
            outcome: None,
            updated_at: 0,
            updated_by: None,
        };
        let stored = changelist::append(conn, &master, &version.branch, entry)?;
        if stored.id == minted_id {
            ingest.appended.push(stored);
        } else {
            ingest.deduped += 1; // identical content existed — union-merge dedup
        }
    }

    // First NEW note advances the machine: IN_REVIEW → NOTES_IN (AUTO, sensor).
    if !ingest.appended.is_empty() {
        let cur = review_state::get(conn, tenant_id, &master, &version.branch)
            .map_err(|e| anyhow!("review_state get: {e}"))?;
        if cur.as_ref().map(|s| s.state.as_str()) == Some("IN_REVIEW") {
            review_state::notes_arrived(
                conn,
                tenant_id,
                &master,
                &version.branch,
                review_state::Actor::Auto,
            )
            .map_err(|e| anyhow!("notes_arrived: {e}"))?;
        }
    }
    ingest.state = review_state::get(conn, tenant_id, &master, &version.branch)
        .map_err(|e| anyhow!("review_state get: {e}"))?;
    Ok(ingest)
}

// ============================================================================
// Confirm → CONFORM → register → round++  (FORMAT_SUPERSET Part 7a + 8b).
//
// The auto-technical-edit loop closes HERE: when a review-loop workflow reaches
// its "apply confirmed mechanical edits and conform proxy" step, Cyan APPLIES the
// approved mechanical ops itself — via the cyan-media `conform` tool — to render a
// NEW proxy, registers it as a derived asset, freezes a new ledger Version over the
// now-applied ops, surfaces any op the engine couldn't apply (`needs_manual`), and
// advances the review round. NO Avid, no editor: the conform engine (a typed op →
// deterministic ffmpeg, Part 8b) turns the ledger into the next review cut.
//
// The cyan-media `conform` tool (branch feat/conform-tool) is `side_effects: none`,
// so it runs un-gated through the SAME McpDispatch path `pipeline_executor` uses;
// the PUBLISH/upload of the new proxy that FOLLOWS stays `external_send`-gated
// (publish_proxy is HUMAN-fired). This module owns steps (a)–(e); it does NOT
// publish.
// ============================================================================

/// The dispatch seam for the cyan-media `conform` tool — the ONE external thing in
/// this leg, behind a trait so unit tests run against a fake (no ffmpeg, no plugin
/// process). Prod wires it to the supervised cyan-mcp host (see
/// `pipeline_executor::execute_conform_step`); tests pass a scripted fake.
///
/// The `args` are exactly the cyan-media `conform.in.json` shape
/// (`{ input, fps, ops:[{op, tc_in, tc_out, params}] }`); the returned value is the
/// `conform.out.json` shape (`{ output_path, applied:[…], needs_manual:[…],
/// size_bytes? }`). Both are agreed with cyan-media — a schema mismatch is a bug in
/// one of the two repos, flagged in VERIFY_CONFORM_IN_LOOP.md.
pub trait ConformDispatch {
    /// Run the `conform` tool with `args`, returning its JSON result.
    fn conform(&self, args: serde_json::Value) -> Result<serde_json::Value>;
}

/// One op the conform tool couldn't mechanically apply — surfaced, never dropped
/// (FORMAT_SUPERSET Part 7a: "Creative or unresolvable ops are reported as
/// needs_manual, never guessed").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NeedsManual {
    pub op: String,
    pub reason: String,
}

/// What one `conform_proxy` run produced — every op is accounted for.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConformOutcome {
    /// The approved ops that were sent to the conform tool, in seq order — exactly
    /// the confirmed mechanical edits (never the creative notes).
    pub sent_ops: Vec<changelist::ConformOp>,
    /// The new proxy's content-addressed asset hash (derived from the tool's
    /// `output_path`), registered as a derived asset.
    pub new_proxy_hash: String,
    /// The tool's returned proxy path (`output_path`).
    pub output_path: String,
    /// The new ledger Version frozen over the now-applied ops (asset_hash +
    /// new list_hash).
    pub new_version_id: String,
    /// Ops the engine escalated as needs_manual — surfaced as a ledger ask, never
    /// silently dropped (see `escalated_asks`).
    pub needs_manual: Vec<NeedsManual>,
    /// The durable `kind=note, source=cyan` ask entry ids minted for each
    /// needs_manual op (one per op, content-addressed → dedups on re-run).
    pub escalated_asks: Vec<String>,
    /// The review state after the round advanced (CONFORMING).
    pub state: Option<review_state::ReviewState>,
}

/// Derive the new proxy asset's content hash from the conform tool's `output_path`.
/// cyan-media already content-addresses the output over the full op list + fps
/// (`derived_path`), so the path IS a stable identity; we Blake3 it into a hash the
/// asset registry can key on. (When cyan-media later returns a real essence hash we
/// switch to that — flagged in VERIFY_CONFORM_IN_LOOP.md. Until then this keeps the
/// derivation edge deterministic and re-runnable, which is what the ledger needs.)
fn proxy_hash_from_output(output_path: &str) -> String {
    blake3::hash(output_path.as_bytes()).to_hex().to_string()
}

/// **conform_proxy** — steps (a)–(e) of the auto-technical-edit loop.
///
/// * (a) resolve the current proxy (by its `frameio` remote_ref) → its source
///   MASTER (`derived_from_asset`) + the frozen conform plan version
///   (`derived_from_version`) + the master asset's `fps`.
/// * (b) gather the ACTIVE + APPROVED `kind=op` entries for the master/branch, in
///   seq order — the confirmed mechanical edits (`changelist::approved_ops`).
///   Notes/creative (`kind=note`) are NEVER conformed.
/// * (c) build the cyan-media `conform` args and dispatch through `ConformDispatch`
///   (side_effects:none → runs; the follow-up upload stays human-gated).
/// * (d) register the returned proxy as a DERIVED asset (derived_from = master, at
///   the NEW version), freeze a new ledger Version over the applied ops, and
///   surface every `needs_manual` op as a durable `source=cyan` ledger ask.
/// * (e) advance the review round (`conform_run`, AUTO) so the next SENSE ingest on
///   the new proxy remaps through `conform_map::for_version(new_version)`.
///
/// The machine must be in `CONFORMING` (the human already fired `confirm_notes`);
/// `conform_run` is the AUTO advance this fires once the render lands. The new proxy
/// is registered but NOT published — `publish_proxy` (external_send) stays the
/// human's move.
pub fn conform_proxy(
    conn: &Connection,
    tenant_id: &str,
    proxy_ref: &str,
    new_proxy_frameio_ref: Option<&str>,
    dispatch: &dyn ConformDispatch,
) -> Result<ConformOutcome> {
    // (a) Backward walk: proxy remote_ref → proxy asset → master + frozen version.
    let proxy = asset_registry::find_by_remote_ref(conn, tenant_id, "frameio", proxy_ref)?
        .ok_or_else(|| anyhow!("no registered asset carries frameio ref '{proxy_ref}'"))?;
    let master = proxy
        .derived_from_asset
        .clone()
        .ok_or_else(|| anyhow!("proxy asset {} has no derived_from_asset (master)", proxy.hash))?;
    let version_id = proxy.derived_from_version.clone().ok_or_else(|| {
        anyhow!("proxy asset {} has no derived_from_version (conform plan)", proxy.hash)
    })?;
    let version = changelist::get_version(conn, tenant_id, &version_id)?;
    let branch = version.branch.clone();

    // GUARD: the machine must be CONFORMING (the human already fired `confirm_notes`).
    // Check BEFORE dispatching so an un-confirmed round never triggers a (potentially
    // expensive) render — the round only conforms what a human confirmed. The AUTO
    // `conform_run` advance at the end re-validates this, but failing early avoids the
    // rogue tool call.
    let cur = review_state::get(conn, tenant_id, &master, &branch)
        .map_err(|e| anyhow!("review_state get: {e}"))?
        .ok_or_else(|| anyhow!("no review_state row for master {master}"))?;
    if cur.state != "CONFORMING" {
        return Err(anyhow!(
            "conform_run rejected: master {master} is '{}', not CONFORMING (confirm the notes first)",
            cur.state
        ));
    }

    // fps: the master's frame denominator (the ops are anchored in master frames).
    // cyan-media's schema defaults fps to 25.0; we always send the real one when the
    // asset carries it, so timecode → frame math never silently uses the wrong base.
    let master_asset = asset_registry::get(conn, tenant_id, &master)?;
    let fps = master_asset.fps.or(proxy.fps);

    // (b) The confirmed mechanical edits — active + approved + kind=op, seq order.
    let sent_ops = changelist::approved_ops(conn, tenant_id, &master, &branch)?;

    // (c) Build the cyan-media `conform` args and dispatch (side_effects:none → runs).
    //     `input` is the proxy the ops apply to; the plugin path-resolves it on the
    //     executing node. We send the CONFIRMED ops only — never the creative notes.
    let op_args: Vec<serde_json::Value> = sent_ops
        .iter()
        .map(|o| {
            let mut m = serde_json::json!({ "op": o.op, "tc_in": o.tc_in });
            if let Some(out) = o.tc_out {
                m["tc_out"] = serde_json::json!(out);
            }
            if o.params.is_object() {
                m["params"] = o.params.clone();
            }
            m
        })
        .collect();
    let mut args = serde_json::json!({ "ops": op_args });
    // The proxy path the ops apply to: prod injects the resolved container path (like
    // `resolve_media_args`); here we carry the proxy asset's remote_ref so the arg is
    // never empty. cyan-media requires a non-empty `input`.
    args["input"] = serde_json::json!(proxy_ref);
    if let Some(f) = fps {
        args["fps"] = serde_json::json!(f);
    }

    let result = dispatch
        .conform(args)
        .map_err(|e| anyhow!("conform dispatch failed: {e}"))?;

    let output_path = result
        .get("output_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("conform result missing 'output_path'"))?
        .to_string();
    let needs_manual: Vec<NeedsManual> = result
        .get("needs_manual")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let op = v.get("op").and_then(|o| o.as_str())?.to_string();
                    let reason = v
                        .get("reason")
                        .and_then(|r| r.as_str())
                        .unwrap_or("unspecified")
                        .to_string();
                    Some(NeedsManual { op, reason })
                })
                .collect()
        })
        .unwrap_or_default();

    // (e-first) Advance the round: conform_run (AUTO) snapshots the active set as the
    // next version. Doing this BEFORE the derivation stamp gives us the new version id
    // the proxy is derived_from — the version whose conform_map the NEXT SENSE remaps
    // through. The machine must be CONFORMING (confirm_notes already fired).
    let state = review_state::conform_run(conn, tenant_id, &master, &branch, review_state::Actor::Auto)
        .map_err(|e| anyhow!("conform_run: {e}"))?;
    let new_version = changelist::get(conn, tenant_id, &master, &branch)?
        .head_version
        .ok_or_else(|| anyhow!("conform_run left no head version"))?;

    // (d) Register the rendered proxy as a DERIVED asset: derived_from = master, at
    //     the NEW version. Idempotent by content hash (re-running the same conform
    //     yields the same output_path → same hash → same row).
    let new_proxy_hash = proxy_hash_from_output(&output_path);
    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: new_proxy_hash.clone(),
            tenant_id: tenant_id.to_string(),
            kind: Some("proxy".to_string()),
            fps,
            duration_ms: None,
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: serde_json::json!({}),
            profile_json: serde_json::json!({ "output_path": output_path }),
            render_profile: proxy.render_profile.clone(),
            created_at: 0,
        },
    )?;
    asset_registry::set_derivation(conn, tenant_id, &new_proxy_hash, &master, &new_version.version_id)?;
    // If the actuator already knows the new proxy's Frame.io id (it published in the
    // same run), stamp the forward breadcrumb now so the NEXT SENSE walk resolves it.
    if let Some(fio) = new_proxy_frameio_ref {
        asset_registry::set_remote_ref(conn, tenant_id, &new_proxy_hash, "frameio", fio)?;
    }

    // Surface needs_manual: a durable `kind=note, source=cyan` ask per escalated op,
    // exactly as the loop escalates a creative note — never a silent drop.
    let mut escalated_asks = Vec::with_capacity(needs_manual.len());
    for nm in &needs_manual {
        let ask = conform_needs_manual_ask(conn, tenant_id, &master, &branch, nm)?;
        escalated_asks.push(ask.id);
    }

    Ok(ConformOutcome {
        sent_ops,
        new_proxy_hash,
        output_path,
        new_version_id: new_version.version_id,
        needs_manual,
        escalated_asks,
        state: Some(state),
    })
}

/// The durable ask for one op the conform engine couldn't apply: a `kind=note,
/// source=cyan` ledger entry (visible on the review rail, replicated like every
/// entry). Content-addressed (op + reason in params) so re-running the same conform
/// returns the SAME ask — no spam.
fn conform_needs_manual_ask(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    nm: &NeedsManual,
) -> Result<changelist::ChangeEntry> {
    let entry = changelist::ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: asset_hash.to_string(),
        tenant_id: tenant_id.to_string(),
        branch: None,
        track: None,
        tc_in: 0,
        tc_out: None,
        kind: "note".to_string(),
        op: None,
        params: serde_json::json!({
            "ask": "conform_needs_manual",
            "op": nm.op,
            "reason": nm.reason,
        }),
        intent: format!(
            "Conform could not apply '{}' automatically ({}). A human must apply it manually or in a DCC.",
            nm.op, nm.reason
        ),
        source: Some("cyan".to_string()),
        source_ref: None,
        author: None,
        role: Some("agent".to_string()),
        proposed_by: Some("agent".to_string()),
        created_at: 0,
        state: String::new(),
        active: true,
        approved_by: None,
        approved_at: None,
        supersedes: None,
        superseded_by: None,
        seq: 0,
        depends_on: None,
        version_ref: None,
        outcome: None,
        updated_at: 0,
        updated_by: None,
    };
    changelist::append(conn, asset_hash, branch, entry)
}

// ============================================================================
// The loop controller — pause / resume / exit / escalate.
// ============================================================================

/// What the loop should do next, decided from `review_state` + the loop row.
/// Serialized (tagged) onto the FFI JSON surface.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum LoopDecision {
    /// Round published (IN_REVIEW): the run parks; SENSE wakes it.
    Park { round: i64 },
    /// New notes (NOTES_IN): start the next round's run (INTERPRET/CONFIRM…).
    /// `round` is the `review_state.round` stamp for that run.
    Resume { round: i64 },
    /// Machinery mid-flight (DRAFT authoring, CONFORMING render) — nothing for
    /// the loop controller to do.
    Working { state: String },
    /// External approval (or a later terminal state): the loop is done.
    Exit { outcome: String },
    /// Round cap reached with notes still arriving: forced HUMAN escalation.
    /// `ask_entry_id` is the durable ask on the ledger — never a silent stop.
    Escalate { round: i64, cap: i64, ask_entry_id: String },
}

/// Advance the loop controller one tick: read `review_state`, decide.
///
/// * `IN_REVIEW`  → `Park` (round published; wait for SENSE).
/// * `NOTES_IN`   → `Resume` — unless resuming would exceed `max_rounds`
///   (publishing round N+1 past the cap), which forces `Escalate`: a durable
///   `kind=note, source=cyan` ask lands on the ledger (dedup by content) and
///   the loop parks `escalated` until a human acts.
/// * `APPROVED` / `FINISHING` / `DELIVERED` → `Exit`; on first arrival the head
///   version's outcome is stamped `shipped` (external approval ⇒ that cut ships)
///   and the loop closes with the same outcome.
/// * `DRAFT` / `CONFORMING` → `Working` (the machinery owns the next move).
pub fn tick(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
    asset_hash: &str,
) -> Result<LoopDecision> {
    let lp = get_loop(conn, tenant_id, board_id, asset_hash)?
        .ok_or_else(|| anyhow!("no review loop registered for board {board_id} asset {asset_hash}"))?;
    if lp.status == "exited" {
        return Ok(LoopDecision::Exit {
            outcome: lp.outcome.unwrap_or_else(|| "shipped".to_string()),
        });
    }
    let st = review_state::get(conn, tenant_id, asset_hash, &lp.branch)
        .map_err(|e| anyhow!("review_state get: {e}"))?
        .ok_or_else(|| anyhow!("loop registered but no review_state row"))?;

    match st.state.as_str() {
        "APPROVED" | "FINISHING" | "DELIVERED" => {
            // External approval detected: stamp the delivered cut `shipped` once
            // (idempotent — only a still-pending head version is stamped).
            if let Some(head) = changelist::get(conn, tenant_id, asset_hash, &lp.branch)?.head_version
                && head.outcome == "pending"
            {
                changelist::set_outcome(conn, tenant_id, &head.version_id, "shipped")?;
            }
            set_loop_status(conn, &lp, "exited", Some("shipped"))?;
            Ok(LoopDecision::Exit { outcome: "shipped".to_string() })
        }
        "IN_REVIEW" => Ok(LoopDecision::Park { round: st.round }),
        "NOTES_IN" => {
            // Resuming this round ends in a publish of round N+1; past the cap
            // that publish is unreachable — escalate NOW, as a durable ask.
            if st.round + 1 > lp.max_rounds {
                let ask = escalate_ask(conn, tenant_id, asset_hash, &lp.branch, st.round, lp.max_rounds)?;
                if lp.status != "escalated" {
                    set_loop_status(conn, &lp, "escalated", None)?;
                }
                Ok(LoopDecision::Escalate {
                    round: st.round,
                    cap: lp.max_rounds,
                    ask_entry_id: ask.id,
                })
            } else {
                Ok(LoopDecision::Resume { round: st.round })
            }
        }
        other => Ok(LoopDecision::Working { state: other.to_string() }),
    }
}

/// The forced-human-escalation ask: a durable `kind=note, source=cyan` ledger
/// entry (visible on the review rail, replicated like every entry). Content-
/// addressed, so re-ticking an escalated loop returns the SAME ask (no spam).
fn escalate_ask(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    round: i64,
    cap: i64,
) -> Result<changelist::ChangeEntry> {
    let entry = changelist::ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: asset_hash.to_string(),
        tenant_id: tenant_id.to_string(),
        branch: None,
        track: None,
        tc_in: 0,
        tc_out: None,
        kind: "note".to_string(),
        op: None,
        params: serde_json::json!({ "ask": "max_rounds_reached", "round": round, "cap": cap }),
        intent: format!(
            "Review loop hit its round cap ({cap}): a human must decide — approve the cut, extend max_rounds, or close the loop."
        ),
        source: Some("cyan".to_string()),
        source_ref: None,
        author: None,
        role: Some("agent".to_string()),
        proposed_by: Some("agent".to_string()),
        created_at: 0,
        state: String::new(),
        active: true,
        approved_by: None,
        approved_at: None,
        supersedes: None,
        superseded_by: None,
        seq: 0,
        depends_on: None,
        version_ref: None,
        outcome: None,
        updated_at: 0,
        updated_by: None,
    };
    changelist::append(conn, asset_hash, branch, entry)
}

fn set_loop_status(
    conn: &Connection,
    lp: &ReviewLoop,
    status: &str,
    outcome: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE review_loop SET status=?1, outcome=COALESCE(?2, outcome), updated_at=?3 \
         WHERE tenant_id=?4 AND board_id=?5 AND asset_hash=?6",
        params![status, outcome, now(), lp.tenant_id, lp.board_id, lp.asset_hash],
    )?;
    Ok(())
}

// ============================================================================
// Rounds as sequential runs.
// ============================================================================

/// Record that a run started for this loop, stamped with the CURRENT
/// `review_state.round` (each round = a run per the dashboard model; the run id
/// is minted by the pipeline executor and never reused across rounds).
/// Idempotent per (loop, run_id).
pub fn record_round_run(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
    asset_hash: &str,
    run_id: &str,
) -> Result<LoopRun> {
    let lp = get_loop(conn, tenant_id, board_id, asset_hash)?
        .ok_or_else(|| anyhow!("no review loop registered for board {board_id} asset {asset_hash}"))?;
    let st = review_state::get(conn, tenant_id, asset_hash, &lp.branch)
        .map_err(|e| anyhow!("review_state get: {e}"))?
        .ok_or_else(|| anyhow!("loop registered but no review_state row"))?;
    let ts = now();
    conn.execute(
        "INSERT OR IGNORE INTO review_loop_run \
            (tenant_id, board_id, asset_hash, run_id, round, started_at) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        params![tenant_id, board_id, asset_hash, run_id, st.round, ts],
    )?;
    conn.query_row(
        "SELECT run_id, round, started_at FROM review_loop_run \
         WHERE tenant_id=?1 AND board_id=?2 AND asset_hash=?3 AND run_id=?4",
        params![tenant_id, board_id, asset_hash, run_id],
        |row| {
            Ok(LoopRun {
                run_id: row.get(0)?,
                round: row.get(1)?,
                started_at: row.get(2)?,
            })
        },
    )
    .map_err(Into::into)
}

/// The loop's runs in start order — the "rounds as sequential runs" rail the
/// dashboard reads.
pub fn runs_for(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
    asset_hash: &str,
) -> Result<Vec<LoopRun>> {
    let mut stmt = conn.prepare(
        "SELECT run_id, round, started_at FROM review_loop_run \
         WHERE tenant_id=?1 AND board_id=?2 AND asset_hash=?3 \
         ORDER BY started_at, round",
    )?;
    let rows = stmt
        .query_map(params![tenant_id, board_id, asset_hash], |row| {
            Ok(LoopRun {
                run_id: row.get(0)?,
                round: row.get(1)?,
                started_at: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
