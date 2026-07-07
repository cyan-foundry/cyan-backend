// cyan-backend/src/review_state.rs
//
// Review-loop STATE MACHINE + editable-proposal lifecycle. Implements
// CYAN_REVIEW_LOOP_TRANSITION_CONTRACT.md against the committed ChangeList store
// (`crate::changelist`). This is the layer that makes Cyan an *agentic
// application* rather than a stateless router: every item is an event that
// advances a goal-directed machine toward DELIVERED.
//
// One `review_state` row per `(tenant_id, asset_hash, branch)` carries the
// current state and a `round` counter. Transitions are functions that ENFORCE
// the three-actor authority model:
//
//   * AUTO   — deterministic, no human (sensor ingest, conform run, deliver).
//   * AGENT  — proposes ONLY: appends `proposed` entries. NEVER fires a gated or
//              side-effecting transition.
//   * HUMAN  — fires every gated / side-effecting transition (confirm, publish,
//              finish, deliver, branch, ratify).
//
// Hard rule: anything with `side_effect = external_send` (publish, finish) is
// HUMAN-gated, ALWAYS — a non-human caller is rejected with a typed error.
//
// Design seam mirrors `changelist`: every op takes an explicit `&Connection`, so
// unit tests run against isolated in-memory DBs while the FFI wrappers drive the
// process-global `storage::db()`. The table is additive; the migration is
// idempotent and wired into `storage::run_migrations` without touching any
// existing table.

use crate::changelist;
use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

// ============================================================================
// State + actor vocab (closed sets).
// ============================================================================

/// The review-loop states. `stale`/`nudge` are a derived overlay on any *waiting*
/// state, NOT their own state — see `nudges_for`.
pub const STATE_VOCAB: &[&str] = &[
    "DRAFT",
    "IN_REVIEW",
    "NOTES_IN",
    "CONFORMING",
    "APPROVED",
    "FINISHING",
    "DELIVERED",
];

/// The three actors that can fire a transition. Recorded on every transition.
pub const ACTOR_VOCAB: &[&str] = &["auto", "agent", "human"];

/// Default nudge thresholds (seconds). Tunable knobs (contract §Knobs).
pub const DEFAULT_IN_REVIEW_STALE_SECS: i64 = 48 * 3600; // 48h
pub const DEFAULT_NOTES_IN_STALE_SECS: i64 = 24 * 3600; // 24h

/// Default loop cap (contract §Knobs — max review rounds before forced human
/// escalation). A publish that would exceed this is rejected.
pub const DEFAULT_MAX_ROUNDS: i64 = 10;

// ============================================================================
// Actor — the caller identity the authority model gates on.
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Actor {
    Auto,
    Agent,
    Human,
}

impl Actor {
    pub fn as_str(&self) -> &'static str {
        match self {
            Actor::Auto => "auto",
            Actor::Agent => "agent",
            Actor::Human => "human",
        }
    }

    pub fn parse(s: &str) -> Result<Actor> {
        match s {
            "auto" => Ok(Actor::Auto),
            "agent" => Ok(Actor::Agent),
            "human" => Ok(Actor::Human),
            other => Err(anyhow!("invalid actor '{}'", other)),
        }
    }
}

// ============================================================================
// The state-machine record.
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewState {
    pub tenant_id: String,
    pub asset_hash: String,
    pub branch: String,
    pub state: String,
    pub round: i64,
    pub updated_at: i64,
}

/// A derived nudge item: a *waiting* state stuck past its threshold. NOT a stored
/// state — computed on read (contract §Non-op items · NUDGE).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Nudge {
    pub tenant_id: String,
    pub asset_hash: String,
    pub branch: String,
    pub state: String,
    pub round: i64,
    /// Seconds this state has been waiting.
    pub waiting_secs: i64,
    /// The threshold it exceeded.
    pub threshold_secs: i64,
}

// ============================================================================
// Errors — typed, so an invalid transition NEVER panics across FFI.
// ============================================================================

#[derive(Debug)]
pub enum ReviewError {
    /// The transition is not defined from the current state.
    InvalidTransition { from: String, event: String },
    /// The caller is not authorized to fire this transition (authority model).
    Unauthorized {
        event: String,
        actor: String,
        required: String,
    },
    /// A gated/side-effecting transition was attempted by a non-human caller.
    GatedNonHuman { event: String, actor: String },
    /// Loop cap reached (contract §Knobs — max review rounds).
    MaxRounds { rounds: i64, cap: i64 },
    /// No review_state row for this key yet.
    NotFound {
        tenant_id: String,
        asset_hash: String,
        branch: String,
    },
    /// Anything else (DB, vocab, etc.).
    Other(String),
}

impl std::fmt::Display for ReviewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReviewError::InvalidTransition { from, event } => {
                write!(f, "invalid transition '{}' from state '{}'", event, from)
            }
            ReviewError::Unauthorized {
                event,
                actor,
                required,
            } => write!(
                f,
                "actor '{}' may not fire '{}' (requires '{}')",
                actor, event, required
            ),
            ReviewError::GatedNonHuman { event, actor } => write!(
                f,
                "'{}' is human-gated (external_send / confirm); actor '{}' rejected",
                event, actor
            ),
            ReviewError::MaxRounds { rounds, cap } => {
                write!(f, "max review rounds reached: {} >= {}", rounds, cap)
            }
            ReviewError::NotFound {
                tenant_id,
                asset_hash,
                branch,
            } => write!(
                f,
                "no review_state for ({}, {}, {})",
                tenant_id, asset_hash, branch
            ),
            ReviewError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for ReviewError {}

impl From<rusqlite::Error> for ReviewError {
    fn from(e: rusqlite::Error) -> Self {
        ReviewError::Other(format!("db: {}", e))
    }
}

impl From<anyhow::Error> for ReviewError {
    fn from(e: anyhow::Error) -> Self {
        ReviewError::Other(e.to_string())
    }
}

/// The kind of a transition, for authority + gating enforcement.
enum Gate {
    /// AUTO fires it — deterministic, no human. (Non-auto callers rejected.)
    Auto,
    /// AGENT proposes; the actual state advance is HUMAN-gated (the "confirm"
    /// editable gate). Reject non-human.
    HumanConfirm,
    /// external_send — publish/finish. ALWAYS human-gated. Reject non-human.
    HumanExternalSend,
    /// HUMAN fires it (branch on reopen, ratify) — reject non-human.
    Human,
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

// ============================================================================
// Migration — additive table, idempotent.
// ============================================================================

/// Create the `review_state` table. Idempotent; called from
/// `storage::run_migrations` and directly from tests. Never touches an existing
/// table.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS review_state (
            tenant_id   TEXT NOT NULL,
            asset_hash  TEXT NOT NULL,
            branch      TEXT NOT NULL DEFAULT 'main',
            state       TEXT NOT NULL DEFAULT 'DRAFT',
            round       INTEGER NOT NULL DEFAULT 0,
            updated_at  INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, asset_hash, branch)
        );
        CREATE INDEX IF NOT EXISTS idx_rs_state
            ON review_state(tenant_id, state);
        "#,
    )?;
    Ok(())
}

// ============================================================================
// Row helpers.
// ============================================================================

fn row_to_state(row: &rusqlite::Row) -> rusqlite::Result<ReviewState> {
    Ok(ReviewState {
        tenant_id: row.get("tenant_id")?,
        asset_hash: row.get("asset_hash")?,
        branch: row.get("branch")?,
        state: row.get("state")?,
        round: row.get("round")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Read the current review_state, if any.
pub fn get(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
) -> Result<Option<ReviewState>, ReviewError> {
    conn.query_row(
        "SELECT tenant_id, asset_hash, branch, state, round, updated_at \
         FROM review_state WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3",
        params![tenant_id, asset_hash, branch],
        row_to_state,
    )
    .optional()
    .map_err(Into::into)
}

/// Every review-state row a tenant holds — the snapshot-frame / digest-lane read
/// (CYAN_FORMAT_SPEC §6.4, the `rs` lane).
pub fn list_by_tenant(conn: &Connection, tenant_id: &str) -> Result<Vec<ReviewState>, ReviewError> {
    let mut stmt = conn.prepare(
        "SELECT tenant_id, asset_hash, branch, state, round, updated_at \
         FROM review_state WHERE tenant_id=?1 ORDER BY asset_hash ASC, branch ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_state)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Apply a peer's review-state row (a snapshot frame / anti-entropy repair): LWW
/// upsert on `updated_at` — a replayed or stale row never clobbers a newer local
/// state, the same guard as `workflow_state_upsert`. Returns whether it landed.
/// This deliberately bypasses the transition gates: the AUTHORITY was enforced on
/// the peer that fired the transition; replication only converges the outcome.
pub fn apply_remote(conn: &Connection, rs: &ReviewState) -> Result<bool, ReviewError> {
    let n = conn.execute(
        "INSERT INTO review_state (tenant_id, asset_hash, branch, state, round, updated_at) \
         VALUES (?1,?2,?3,?4,?5,?6) \
         ON CONFLICT(tenant_id, asset_hash, branch) DO UPDATE SET \
            state=excluded.state, round=excluded.round, updated_at=excluded.updated_at \
         WHERE excluded.updated_at > review_state.updated_at",
        params![rs.tenant_id, rs.asset_hash, rs.branch, rs.state, rs.round, rs.updated_at],
    )?;
    Ok(n > 0)
}

fn require(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
) -> Result<ReviewState, ReviewError> {
    get(conn, tenant_id, asset_hash, branch)?.ok_or_else(|| ReviewError::NotFound {
        tenant_id: tenant_id.to_string(),
        asset_hash: asset_hash.to_string(),
        branch: branch.to_string(),
    })
}

/// Write the state row (upsert) with a fresh `updated_at`.
fn put_state(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    state: &str,
    round: i64,
) -> Result<ReviewState, ReviewError> {
    let ts = now();
    conn.execute(
        "INSERT INTO review_state (tenant_id, asset_hash, branch, state, round, updated_at) \
         VALUES (?1,?2,?3,?4,?5,?6) \
         ON CONFLICT(tenant_id, asset_hash, branch) DO UPDATE SET \
            state=excluded.state, round=excluded.round, updated_at=excluded.updated_at",
        params![tenant_id, asset_hash, branch, state, round, ts],
    )?;
    Ok(ReviewState {
        tenant_id: tenant_id.to_string(),
        asset_hash: asset_hash.to_string(),
        branch: branch.to_string(),
        state: state.to_string(),
        round,
        updated_at: ts,
    })
}

/// Enforce the gate for a transition. Returns Ok(()) iff `actor` may fire it.
fn enforce_gate(event: &str, gate: &Gate, actor: Actor) -> Result<(), ReviewError> {
    match gate {
        Gate::Auto => {
            if actor == Actor::Auto {
                Ok(())
            } else {
                Err(ReviewError::Unauthorized {
                    event: event.to_string(),
                    actor: actor.as_str().to_string(),
                    required: "auto".to_string(),
                })
            }
        }
        // Both HumanConfirm and HumanExternalSend require HUMAN. external_send is
        // the "always human-gated" rule; confirm is the editable gate. Agent/auto
        // callers are rejected — an AGENT may only PROPOSE, never fire the gate.
        Gate::HumanConfirm | Gate::HumanExternalSend | Gate::Human => {
            if actor == Actor::Human {
                Ok(())
            } else {
                Err(ReviewError::GatedNonHuman {
                    event: event.to_string(),
                    actor: actor.as_str().to_string(),
                })
            }
        }
    }
}

// ============================================================================
// Lifecycle bootstrap.
// ============================================================================

/// Start (or reset to) the DRAFT state for an asset/branch. AUTO — this is the
/// deterministic "asset ingested" entry point (round 0). Idempotent-ish: if a row
/// exists it is not clobbered (returns the existing one) so re-ingest never resets
/// an in-flight review. Use `reset` explicitly if a reset is intended.
pub fn start_draft(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
) -> Result<ReviewState, ReviewError> {
    if let Some(existing) = get(conn, tenant_id, asset_hash, branch)? {
        return Ok(existing);
    }
    put_state(conn, tenant_id, asset_hash, branch, "DRAFT", 0)
}

// ============================================================================
// Transitions — one function per contract row. Each takes the acting `Actor`,
// enforces the gate, validates the from-state, and advances (or stays).
// ============================================================================

/// DRAFT → IN_REVIEW: "conform & publish". external_send → HUMAN-gated. Snapshots
/// the active change-list as v1 (via `changelist::snapshot`).
pub fn publish_draft(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "publish";
    enforce_gate(event, &Gate::HumanExternalSend, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "DRAFT" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    // external work is committed: snapshot v1.
    changelist::snapshot(conn, tenant_id, asset_hash, branch)?;
    put_state(conn, tenant_id, asset_hash, branch, "IN_REVIEW", cur.round)
}

/// IN_REVIEW → NOTES_IN: new Frame.io comments arrive (sensor). AUTO. The comments
/// themselves are appended by the caller via `changelist::append` (source=frameio);
/// this transition just advances the machine.
pub fn notes_arrived(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "notes_arrived";
    enforce_gate(event, &Gate::Auto, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "IN_REVIEW" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    put_state(conn, tenant_id, asset_hash, branch, "NOTES_IN", cur.round)
}

/// IN_REVIEW → APPROVED: producer approves the version (sensor / external approval).
/// AUTO. Optionally the caller stamps the version outcome=shipped via changelist.
pub fn version_approved(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "version_approved";
    enforce_gate(event, &Gate::Auto, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "IN_REVIEW" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    put_state(conn, tenant_id, asset_hash, branch, "APPROVED", cur.round)
}

/// NOTES_IN → CONFORMING: the human has confirmed the proposed ops (the editable
/// gate). HUMAN-gated. Per-op confirm is done through `confirm_op`; this fires once
/// the batch is confirmed to advance the machine into the conform run. Advancing
/// also fires the CONFIRM-step interlock: the tenant's parked manual review-gate
/// workflow steps flip to human_approved (`pipeline::approve_review_gate_steps`).
pub fn confirm_notes(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "confirm_notes";
    enforce_gate(event, &Gate::HumanConfirm, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "NOTES_IN" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    let out = put_state(conn, tenant_id, asset_hash, branch, "CONFORMING", cur.round)?;
    // CONFIRM interlock (CYAN_FORMAT_QA gap 6): this NOTES_IN → CONFORMING advance
    // is the human decision a board's parked manual CONFIRM step was waiting on —
    // mark it human_approved so the workflow un-parks without a second approval
    // tap. Best-effort: the machine's advance stands even if the workflow-side
    // write fails (and a bare ledger-only DB simply has no boards).
    if let Err(e) = crate::pipeline::approve_review_gate_steps(conn, tenant_id) {
        tracing::warn!("confirm interlock (tenant {}): {}", tenant_id, e);
    }
    Ok(out)
}

/// CONFORMING → CONFORMING: a conform run (conform_plan → render proxy → snapshot
/// v(N+1)). AUTO — internal work is auto once ops are confirmed. Stays in
/// CONFORMING; snapshots the active set as the next version.
pub fn conform_run(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "conform_run";
    enforce_gate(event, &Gate::Auto, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "CONFORMING" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    changelist::snapshot(conn, tenant_id, asset_hash, branch)?;
    // stays CONFORMING
    put_state(conn, tenant_id, asset_hash, branch, "CONFORMING", cur.round)
}

/// CONFORMING → NOTES_IN: a render failure. AUTO — surfaces the failure item; the
/// active ops are unchanged. Bounces back to NOTES_IN so the human can adjust.
pub fn conform_failed(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "conform_failed";
    enforce_gate(event, &Gate::Auto, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "CONFORMING" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    put_state(conn, tenant_id, asset_hash, branch, "NOTES_IN", cur.round)
}

/// CONFORMING → IN_REVIEW (round++): proxy ready → publish to Frame.io.
/// external_send → HUMAN-gated. The `round` increments here (contract: "round
/// increments on each CONFORMING→IN_REVIEW publish"). `max_rounds` caps the loop.
pub fn publish_proxy(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
    max_rounds: i64,
) -> Result<ReviewState, ReviewError> {
    let event = "publish_proxy";
    enforce_gate(event, &Gate::HumanExternalSend, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "CONFORMING" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    let next_round = cur.round + 1;
    if next_round > max_rounds {
        return Err(ReviewError::MaxRounds {
            rounds: next_round,
            cap: max_rounds,
        });
    }
    changelist::snapshot(conn, tenant_id, asset_hash, branch)?;
    put_state(conn, tenant_id, asset_hash, branch, "IN_REVIEW", next_round)
}

/// APPROVED → FINISHING: "finish" — conform_plan(final) → OTIO → AAF/Resolve export
/// or final render. external_send → HUMAN-gated.
pub fn finish(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "finish";
    enforce_gate(event, &Gate::HumanExternalSend, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "APPROVED" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    put_state(conn, tenant_id, asset_hash, branch, "FINISHING", cur.round)
}

/// FINISHING → DELIVERED: export/render complete → record delivery. AUTO (on
/// success).
pub fn delivered(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "delivered";
    enforce_gate(event, &Gate::Auto, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "FINISHING" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    put_state(conn, tenant_id, asset_hash, branch, "DELIVERED", cur.round)
}

/// DELIVERED → NOTES_IN (new branch): producer reopens with new notes → branch off
/// the delivered version. HUMAN. Forks the active change-list onto `new_branch` and
/// stands up a NOTES_IN review_state for it (round preserved from the source).
pub fn reopen_branch(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    new_branch: &str,
    actor: Actor,
) -> Result<ReviewState, ReviewError> {
    let event = "reopen_branch";
    enforce_gate(event, &Gate::Human, actor)?;
    let cur = require(conn, tenant_id, asset_hash, branch)?;
    if cur.state != "DELIVERED" {
        return Err(ReviewError::InvalidTransition {
            from: cur.state,
            event: event.to_string(),
        });
    }
    changelist::branch(conn, tenant_id, asset_hash, branch, new_branch)?;
    put_state(conn, tenant_id, asset_hash, new_branch, "NOTES_IN", cur.round)
}

// ============================================================================
// Editable-proposal lifecycle (contract §Conformance-auto vs taste-escalate).
// ============================================================================

/// `propose_op` — AGENT proposes a mechanical op: append a `proposed`, `active`
/// entry via the ChangeList store. The AGENT may ONLY propose; it never activates
/// or confirms. Rejected if the caller is not the AGENT (a human uses the normal
/// append path; auto never proposes ops).
pub fn propose_op(
    conn: &Connection,
    asset_hash: &str,
    branch: &str,
    mut entry: changelist::ChangeEntry,
    actor: Actor,
) -> Result<changelist::ChangeEntry, ReviewError> {
    if actor != Actor::Agent {
        return Err(ReviewError::Unauthorized {
            event: "propose_op".to_string(),
            actor: actor.as_str().to_string(),
            required: "agent".to_string(),
        });
    }
    // Agent proposals are always `proposed` + tagged proposed_by=agent. It may not
    // pre-approve or pre-activate: force `proposed`.
    entry.state = "proposed".to_string();
    entry.proposed_by = Some("agent".to_string());
    let out = changelist::append(conn, asset_hash, branch, entry)?;
    Ok(out)
}

/// The confirm decision — the human's editable gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmDecision {
    Approve,
    Reject,
}

/// `confirm_op` — HUMAN confirms a proposed op (approve / reject), optionally with
/// EDITED params. This is the editable gate that drives NOTES_IN → CONFORMING.
///
///   * approve           → set_state(approved) + set_active(true).
///   * approve + edited  → supersede the original with an edited copy, then approve+activate the new entry (the redo chain).
///   * reject            → set_state(rejected) + set_active(false).
///
/// Only a HUMAN may confirm (the gate). Returns the resulting active/rejected entry.
pub fn confirm_op(
    conn: &Connection,
    tenant_id: &str,
    entry_id: &str,
    edited_params: Option<serde_json::Value>,
    decision: ConfirmDecision,
    actor: Actor,
) -> Result<changelist::ChangeEntry, ReviewError> {
    // The confirm gate is HUMAN-only (external editable gate).
    enforce_gate("confirm_op", &Gate::HumanConfirm, actor)?;

    let resolved = match decision {
        ConfirmDecision::Reject => {
            changelist::set_state(conn, tenant_id, entry_id, "rejected", Some("human"))?;
            // set_active re-reads the row, so the returned entry reflects both the
            // rejected state and active=false.
            let e = changelist::set_active(conn, tenant_id, entry_id, false, Some("human"))?;
            Ok(e)
        }
        ConfirmDecision::Approve => {
            match edited_params {
                // Approve as-is: approve + keep active.
                None => {
                    changelist::set_state(conn, tenant_id, entry_id, "approved", Some("human"))?;
                    let e =
                        changelist::set_active(conn, tenant_id, entry_id, true, Some("human"))?;
                    Ok(e)
                }
                // Edited: the human changed the params. Supersede the original with a
                // fresh entry carrying the edited params, then approve+activate it.
                // supersede() marks the old one superseded+inactive and the new one active.
                Some(new_params) => {
                    let orig = changelist::get_entry(conn, tenant_id, entry_id)?;
                    let mut edited = orig.clone();
                    edited.id = String::new();
                    edited.entry_hash = String::new();
                    edited.params = new_params;
                    edited.state = "proposed".to_string();
                    edited.active = true;
                    edited.superseded_by = None;
                    edited.supersedes = None;
                    edited.version_ref = None;
                    edited.proposed_by = Some("human".to_string());
                    let new_entry = changelist::supersede(conn, entry_id, edited)?;
                    // Approve + activate the newly-created (edited) entry.
                    changelist::set_state(
                        conn,
                        tenant_id,
                        &new_entry.id,
                        "approved",
                        Some("human"),
                    )?;
                    let active = changelist::set_active(
                        conn,
                        tenant_id,
                        &new_entry.id,
                        true,
                        Some("human"),
                    )?;
                    Ok::<changelist::ChangeEntry, ReviewError>(active)
                }
            }
        }
    }?;

    // CONFIRM interlock, per-op leg: resolving the LAST open proposal on this
    // asset/branch IS the human decision a board's parked manual CONFIRM step
    // was waiting on — release it without a second approval tap. (The batch
    // `confirm_notes` transition fires the same interlock; this leg covers the
    // op-by-op confirm path.) Best-effort: the confirm stands even if the
    // workflow-side write fails.
    let open_proposals: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM change_entry \
             WHERE tenant_id=?1 AND asset_hash=?2 AND branch=?3 \
               AND kind='op' AND state='proposed'",
            params![
                tenant_id,
                resolved.asset_hash,
                resolved.branch.as_deref().unwrap_or("main")
            ],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if open_proposals == 0
        && let Err(e) = crate::pipeline::approve_review_gate_steps(conn, tenant_id)
    {
        tracing::warn!("confirm interlock (per-op, tenant {}): {}", tenant_id, e);
    }
    Ok(resolved)
}

/// `escalate_note` — a creative `note` (kind=note) is escalated as a CHOICE: the
/// AGENT may NOT draft an op for it. The human decides: keep it as a note
/// (`promote=false`, a no-op that just records the escalation intent) or MANUALLY
/// promote it to an op (`promote=true` with an `op` + params). Never auto-converted.
///
/// `promote=true` requires a HUMAN caller (only a human may promote taste to an op).
/// The note entry itself is left intact; a NEW op entry is appended (superseding
/// nothing — the note stays as provenance).
pub fn escalate_note(
    conn: &Connection,
    tenant_id: &str,
    note_entry_id: &str,
    promote_to_op: Option<(String, serde_json::Value)>,
    actor: Actor,
) -> Result<Option<changelist::ChangeEntry>, ReviewError> {
    let note = changelist::get_entry(conn, tenant_id, note_entry_id)?;
    if note.kind != "note" {
        return Err(ReviewError::Other(format!(
            "escalate_note: entry {} is kind='{}', not a creative note",
            note_entry_id, note.kind
        )));
    }
    match promote_to_op {
        // Keep as a note — the human chose not to promote. No state change; the
        // choice is recorded by the audit trail on the note. Return None.
        None => Ok(None),
        // Promote — HUMAN only. Draft a real op entry from the note's anchor.
        Some((op, params)) => {
            enforce_gate("escalate_note.promote", &Gate::Human, actor)?;
            let mut op_entry = note.clone();
            op_entry.id = String::new();
            op_entry.entry_hash = String::new();
            op_entry.kind = "op".to_string();
            op_entry.op = Some(op);
            op_entry.params = params;
            op_entry.state = "proposed".to_string();
            op_entry.active = true;
            op_entry.supersedes = None;
            op_entry.superseded_by = None;
            op_entry.version_ref = None;
            op_entry.proposed_by = Some("human".to_string());
            let branch = note.branch.clone().unwrap_or_else(|| "main".to_string());
            let out = changelist::append(conn, &note.asset_hash, &branch, op_entry)?;
            Ok(Some(out))
        }
    }
}

// ============================================================================
// Derived signals — nudges (NOT states).
// ============================================================================

/// `nudges_for` — the stale/stuck items for an asset: any *waiting* state whose
/// `updated_at` is older than its threshold. Waiting states are IN_REVIEW (default
/// 48h) and NOTES_IN (default 24h). Returns one nudge per stale (branch) row.
/// Pass `None` for a threshold to use the default for that state.
pub fn nudges_for(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    in_review_threshold_secs: Option<i64>,
    notes_in_threshold_secs: Option<i64>,
) -> Result<Vec<Nudge>, ReviewError> {
    let in_review_t = in_review_threshold_secs.unwrap_or(DEFAULT_IN_REVIEW_STALE_SECS);
    let notes_in_t = notes_in_threshold_secs.unwrap_or(DEFAULT_NOTES_IN_STALE_SECS);
    let ts = now();

    let mut stmt = conn.prepare(
        "SELECT tenant_id, asset_hash, branch, state, round, updated_at \
         FROM review_state \
         WHERE tenant_id=?1 AND asset_hash=?2 AND state IN ('IN_REVIEW','NOTES_IN')",
    )?;
    let rows = stmt
        .query_map(params![tenant_id, asset_hash], row_to_state)?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut out = Vec::new();
    for rs in rows {
        let threshold = match rs.state.as_str() {
            "IN_REVIEW" => in_review_t,
            "NOTES_IN" => notes_in_t,
            _ => continue,
        };
        let waiting = ts - rs.updated_at;
        if waiting >= threshold {
            out.push(Nudge {
                tenant_id: rs.tenant_id,
                asset_hash: rs.asset_hash,
                branch: rs.branch,
                state: rs.state,
                round: rs.round,
                waiting_secs: waiting,
                threshold_secs: threshold,
            });
        }
    }
    Ok(out)
}

// ============================================================================
// FFI command/event JSON dispatch (the `cyan_review_*` surface).
// ============================================================================
//
// One additive entrypoint takes a JSON command `{ "op": <name>, ... }` and returns
// a JSON result string. Mirrors `changelist::command`. Errors surface as
// `{ "error": "<msg>" }` — never a panic across the boundary. `actor` is a required
// field on every transition op so the authority model is enforced end-to-end.

/// Run a review command against the process-global DB. JSON in, JSON out.
pub fn command(json_str: &str) -> String {
    match dispatch(json_str) {
        Ok(v) => v.to_string(),
        Err(e) => serde_json::json!({ "error": e.to_string() }).to_string(),
    }
}

fn dispatch(json_str: &str) -> Result<serde_json::Value, ReviewError> {
    let cmd: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| ReviewError::Other(format!("bad command JSON: {}", e)))?;
    let op = cmd
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ReviewError::Other("missing 'op'".to_string()))?
        .to_string();

    let lock = crate::storage::try_db()
        .ok_or_else(|| ReviewError::Other("DB not initialized".to_string()))?
        .lock()
        .map_err(|e| ReviewError::Other(format!("DB lock: {}", e)))?;
    let conn: &Connection = &lock;

    let s = |k: &str| -> Result<String, ReviewError> {
        cmd.get(k)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ReviewError::Other(format!("missing '{}'", k)))
    };
    let branch = || -> String {
        cmd.get("branch")
            .and_then(|v| v.as_str())
            .unwrap_or("main")
            .to_string()
    };
    let actor = || -> Result<Actor, ReviewError> {
        let a = cmd
            .get("actor")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ReviewError::Other("missing 'actor'".to_string()))?;
        Actor::parse(a).map_err(Into::into)
    };
    let max_rounds = || -> i64 {
        cmd.get("max_rounds")
            .and_then(|v| v.as_i64())
            .unwrap_or(DEFAULT_MAX_ROUNDS)
    };
    // The app-driven verbs accept `tenant_id` OR `board_id` (board → its group).
    let tenant_or_board = |c: &serde_json::Value| -> Result<String, ReviewError> {
        if let Some(t) = c.get("tenant_id").and_then(|v| v.as_str()) {
            if !t.is_empty() {
                return Ok(t.to_string());
            }
        }
        let board = c
            .get("board_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ReviewError::Other("missing 'tenant_id' (or 'board_id')".to_string()))?;
        Ok(crate::review_loop::board_tenant(conn, board))
    };

    let out: serde_json::Value = match op.as_str() {
        "get" => {
            let st = get(conn, &s("tenant_id")?, &s("asset_hash")?, &branch())?;
            serde_json::to_value(st).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "start_draft" => {
            let st = start_draft(conn, &s("tenant_id")?, &s("asset_hash")?, &branch())?;
            serde_json::to_value(st).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "publish" => {
            // Tenant-keyed (original shape) — unchanged.
            if cmd.get("tenant_id").is_some() {
                tv(publish_draft(
                    conn,
                    &s("tenant_id")?,
                    &s("asset_hash")?,
                    &branch(),
                    actor()?,
                )?)?
            } else {
                // BOARD-keyed (the app player dialect): resolve the board's review and
                // publish by state — DRAFT → publish_draft (v1), CONFORMING →
                // publish_proxy (round++). The app surface is the human.
                let board = s("board_id")?;
                let (tenant, asset, br) = crate::review_loop::resolve_board_review(
                    conn,
                    &board,
                    cmd.get("asset_hash").and_then(|v| v.as_str()),
                )
                .map_err(|e| ReviewError::Other(e.to_string()))?;
                let cur = get(conn, &tenant, &asset, &br)?
                    .ok_or_else(|| ReviewError::Other("no review state".to_string()))?;
                match cur.state.as_str() {
                    "DRAFT" => {
                        publish_draft(conn, &tenant, &asset, &br, Actor::Human)?;
                    }
                    "CONFORMING" => {
                        publish_proxy(conn, &tenant, &asset, &br, Actor::Human, max_rounds())?;
                    }
                    other => {
                        return Err(ReviewError::Other(format!(
                            "publish from state {other} is not a transition"
                        )))
                    }
                }
                crate::review_loop::board_envelope(conn, &tenant, &asset, &br)
                    .map_err(|e| ReviewError::Other(e.to_string()))?
            }
        }
        "notes_arrived" => tv(notes_arrived(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            actor()?,
        )?)?,
        "version_approved" => tv(version_approved(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            actor()?,
        )?)?,
        "confirm_notes" => tv(confirm_notes(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            actor()?,
        )?)?,
        "conform_run" => tv(conform_run(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            actor()?,
        )?)?,
        "conform_failed" => tv(conform_failed(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            actor()?,
        )?)?,
        "publish_proxy" => tv(publish_proxy(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            actor()?,
            max_rounds(),
        )?)?,
        "finish" => {
            if cmd.get("tenant_id").is_some() {
                tv(finish(
                    conn,
                    &s("tenant_id")?,
                    &s("asset_hash")?,
                    &branch(),
                    actor()?,
                )?)?
            } else {
                // BOARD-keyed (the app player dialect); the app surface is the human.
                let board = s("board_id")?;
                let (tenant, asset, br) = crate::review_loop::resolve_board_review(
                    conn,
                    &board,
                    cmd.get("asset_hash").and_then(|v| v.as_str()),
                )
                .map_err(|e| ReviewError::Other(e.to_string()))?;
                finish(conn, &tenant, &asset, &br, Actor::Human)?;
                crate::review_loop::board_envelope(conn, &tenant, &asset, &br)
                    .map_err(|e| ReviewError::Other(e.to_string()))?
            }
        }
        "delivered" => tv(delivered(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            actor()?,
        )?)?,
        "reopen_branch" => tv(reopen_branch(
            conn,
            &s("tenant_id")?,
            &s("asset_hash")?,
            &branch(),
            &s("new_branch")?,
            actor()?,
        )?)?,
        "propose_op" => {
            let entry: changelist::ChangeEntry = serde_json::from_value(
                cmd.get("entry")
                    .cloned()
                    .ok_or_else(|| ReviewError::Other("missing 'entry'".to_string()))?,
            )
            .map_err(|e| ReviewError::Other(format!("bad entry: {}", e)))?;
            let out = propose_op(conn, &s("asset_hash")?, &branch(), entry, actor()?)?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "confirm_op" => {
            let decision = match cmd.get("decision").and_then(|v| v.as_str()) {
                Some("approve") => ConfirmDecision::Approve,
                Some("reject") => ConfirmDecision::Reject,
                _ => {
                    return Err(ReviewError::Other(
                        "confirm_op needs decision=approve|reject".to_string(),
                    ))
                }
            };
            let edited = cmd.get("edited_params").cloned();
            let out = confirm_op(
                conn,
                &s("tenant_id")?,
                &s("entry_id")?,
                edited,
                decision,
                actor()?,
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "escalate_note" => {
            let promote = match (cmd.get("op_name").and_then(|v| v.as_str()), cmd.get("params")) {
                (Some(name), Some(p)) => Some((name.to_string(), p.clone())),
                _ => None,
            };
            let out = escalate_note(conn, &s("tenant_id")?, &s("entry_id")?, promote, actor()?)?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "nudges_for" => {
            let in_review_t = cmd.get("in_review_threshold_secs").and_then(|v| v.as_i64());
            let notes_in_t = cmd.get("notes_in_threshold_secs").and_then(|v| v.as_i64());
            let out = nudges_for(
                conn,
                &s("tenant_id")?,
                &s("asset_hash")?,
                in_review_t,
                notes_in_t,
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        // ── Review-loop controller (additive; see `crate::review_loop`) ─────
        "loop_register" => {
            let out = crate::review_loop::register(
                conn,
                &s("tenant_id")?,
                &s("board_id")?,
                &s("asset_hash")?,
                &branch(),
                max_rounds(),
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "loop_get" => {
            let out = crate::review_loop::get_loop(
                conn,
                &s("tenant_id")?,
                &s("board_id")?,
                &s("asset_hash")?,
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "loop_tick" => {
            let out = crate::review_loop::tick(
                conn,
                &s("tenant_id")?,
                &s("board_id")?,
                &s("asset_hash")?,
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "loop_record_run" => {
            let out = crate::review_loop::record_round_run(
                conn,
                &s("tenant_id")?,
                &s("board_id")?,
                &s("asset_hash")?,
                &s("run_id")?,
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "loop_runs" => {
            let out = crate::review_loop::runs_for(
                conn,
                &s("tenant_id")?,
                &s("board_id")?,
                &s("asset_hash")?,
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        // SENSE → ledger glue: ingest a `frameio.list_comments` step result.
        "sense_ingest" => {
            let result = cmd
                .get("result")
                .cloned()
                .ok_or_else(|| ReviewError::Other("missing 'result'".to_string()))?;
            let tenant = tenant_or_board(&cmd)?;
            let out = crate::review_loop::ingest_sense_result(
                conn,
                &tenant,
                &s("proxy_ref")?,
                &result,
            )?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        // ── APP-DRIVEN loop verbs (Frame.io integration) — ADDITIVE. The macOS app
        // drives the reverse loop over this same JSON surface; the loop-level logic
        // lives in `review_loop` (see its "APP-DRIVEN loop verbs" section). Each verb
        // accepts `tenant_id` OR `board_id` (the app's boards resolve to their group).
        "register_review_media" => {
            let tenant = tenant_or_board(&cmd)?;
            let out = crate::review_loop::register_review_media(
                conn,
                &tenant,
                &s("master_path")?,
                &s("proxy_ref")?,
                &branch(),
                actor()?,
            )
            .map_err(|e| ReviewError::Other(e.to_string()))?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "propose_from_note" => {
            let tenant = tenant_or_board(&cmd)?;
            let out = crate::review_loop::propose_from_note(
                conn,
                &tenant,
                &s("asset_hash")?,
                &branch(),
            )
            .map_err(|e| ReviewError::Other(e.to_string()))?;
            serde_json::to_value(out).map_err(|e| ReviewError::Other(e.to_string()))?
        }
        "conform_proxy" => {
            let tenant = tenant_or_board(&cmd)?;
            let (outcome, out_abs) =
                crate::review_loop::conform_via_env(conn, &tenant, &s("proxy_ref")?)
                    .map_err(|e| ReviewError::Other(e.to_string()))?;
            let mut v = serde_json::to_value(outcome)
                .map_err(|e| ReviewError::Other(e.to_string()))?;
            if let Some(obj) = v.as_object_mut() {
                obj.insert(
                    "output_abs".to_string(),
                    serde_json::Value::String(out_abs.display().to_string()),
                );
            }
            v
        }
        "review_media_info" => {
            let tenant = tenant_or_board(&cmd)?;
            crate::review_loop::review_media_info(conn, &tenant, &s("proxy_ref")?)
                .map_err(|e| ReviewError::Other(e.to_string()))?
        }
        // The app player's confirm gate (BOARD-keyed): approve / reject / edit one
        // proposed entry; when the LAST open agent proposal resolves and the machine
        // sits at NOTES_IN, the same human confirm advances NOTES_IN → CONFORMING
        // (`confirm_notes`) — the phase-3 semantic, one human action. Returns the
        // full envelope the player re-renders from.
        "confirm" => {
            let board = s("board_id")?;
            let (tenant, asset, br) = crate::review_loop::resolve_board_review(
                conn,
                &board,
                cmd.get("asset_hash").and_then(|v| v.as_str()),
            )
            .map_err(|e| ReviewError::Other(e.to_string()))?;
            let entry_id = s("entry_id")?;
            let decision = s("decision")?;
            let edited = cmd.get("params").cloned();
            match decision.as_str() {
                "approve" => {
                    confirm_op(conn, &tenant, &entry_id, None, ConfirmDecision::Approve, Actor::Human)?;
                }
                "edit" => {
                    confirm_op(conn, &tenant, &entry_id, edited, ConfirmDecision::Approve, Actor::Human)?;
                }
                "reject" => {
                    confirm_op(conn, &tenant, &entry_id, None, ConfirmDecision::Reject, Actor::Human)?;
                }
                other => {
                    return Err(ReviewError::Other(format!("unknown decision '{other}'")))
                }
            }
            // Same-human advance: no open proposals left + NOTES_IN ⇒ CONFORMING.
            let view = crate::changelist::get(conn, &tenant, &asset, &br)
                .map_err(|e| ReviewError::Other(e.to_string()))?;
            let open = view.entries.iter().any(|e| {
                e.kind == "op"
                    && e.proposed_by.as_deref() == Some("agent")
                    && e.active
                    && e.state == "proposed"
            });
            if !open {
                if let Some(cur) = get(conn, &tenant, &asset, &br)? {
                    if cur.state == "NOTES_IN" {
                        confirm_notes(conn, &tenant, &asset, &br, Actor::Human)?;
                    }
                }
            }
            crate::review_loop::board_envelope(conn, &tenant, &asset, &br)
                .map_err(|e| ReviewError::Other(e.to_string()))?
        }
        other => return Err(ReviewError::Other(format!("unknown op '{}'", other))),
    };
    Ok(out)
}

/// Serialize a ReviewState to a JSON value (helper to keep the dispatch arms terse).
fn tv(st: ReviewState) -> Result<serde_json::Value, ReviewError> {
    serde_json::to_value(st).map_err(|e| ReviewError::Other(e.to_string()))
}
