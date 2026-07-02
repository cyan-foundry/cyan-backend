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

/// Parse the comments out of a SENSE step's plugin tool result. The step-result
/// contract (what `execute_local_mcp_tool_step` / the lens path persist as the
/// step's output): the tool's JSON — either the Frame.io V4 envelope
/// `{"data": [ ...comments ]}` or a bare comment array. Per comment:
/// `id` (string, required), `text` (string, required), the proxy-frame anchor
/// from `frame` else `timestamp` (integer frames, default 0), optional
/// `frame_out`, author from `owner.name` else `author`. Comments missing
/// `id`/`text` are malformed and skipped (counted by the caller's report —
/// no silent truncation).
pub fn parse_sense_comments(result: &serde_json::Value) -> (Vec<SenseComment>, usize) {
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
        let frame = item
            .get("frame")
            .and_then(|v| v.as_i64())
            .or_else(|| item.get("timestamp").and_then(|v| v.as_i64()))
            .unwrap_or(0);
        let frame_out = item.get("frame_out").and_then(|v| v.as_i64());
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

    let (comments, malformed) = parse_sense_comments(result);
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
