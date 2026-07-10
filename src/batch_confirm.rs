//! feat/notes-constitution — the BATCH-CONFIRM gate (TONIGHT_RUN Part 4).
//!
//! "Approve all mechanical" + per-editor TRUST TIERS over the changelist confirm
//! surface — the throughput mechanism: one editor clears many assets in one tap.
//!
//! The gate NEVER bypasses the authority model: every approval goes through
//! [`crate::review_state::confirm_op`] (human-only, audited, fires the confirm
//! interlock), so a batch is exactly N per-op confirms issued by one human
//! action. Tiers only decide WHICH open proposals qualify:
//!
//!   * `per-op` (default) — batch DENIED; the editor confirms one by one.
//!   * `mechanical` — closed-vocab op proposals that are confident: no
//!     `params.confidence` (deterministic proposer) or confidence ≥
//!     [`MIN_BATCH_CONFIDENCE`]. Low-confidence ops are SKIPPED for per-op review.
//!   * `senior` — all open closed-vocab op proposals.
//!
//! `ProposedOp.confidence` reaches the ledger as `params["confidence"]` (the
//! JOIN's ChangeEntry adapter places it there; absent = deterministic proposer).
//! Notes/markers are never batch candidates — creative taste stays human, per-op.

use rusqlite::{params, Connection, OptionalExtension};

use crate::changelist;
use crate::review_state::{confirm_op, Actor, ConfirmDecision, ReviewError};

/// The closed trust-tier vocabulary.
pub const TRUST_TIER_VOCAB: [&str; 3] = ["per-op", "mechanical", "senior"];

/// Confidence floor for the `mechanical` tier's batch (the contract's
/// "confidence drives the confirm gate / batch thresholds").
pub const MIN_BATCH_CONFIDENCE: f64 = 0.8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustTier {
    PerOp,
    Mechanical,
    Senior,
}

impl TrustTier {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "per-op" => Some(Self::PerOp),
            "mechanical" => Some(Self::Mechanical),
            "senior" => Some(Self::Senior),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PerOp => "per-op",
            Self::Mechanical => "mechanical",
            Self::Senior => "senior",
        }
    }
}

/// Create the `editor_trust` table. Idempotent; additive — called from
/// `storage::run_migrations` and directly from tests.
pub fn migrate(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS editor_trust (
            tenant_id   TEXT NOT NULL,
            editor_id   TEXT NOT NULL,
            tier        TEXT NOT NULL DEFAULT 'per-op',
            updated_at  INTEGER NOT NULL,
            updated_by  TEXT,
            PRIMARY KEY (tenant_id, editor_id)
        );
        "#,
    )?;
    Ok(())
}

/// Set an editor's trust tier (tenant-scoped upsert, latest write wins). A local
/// admin surface — not a synced lane.
pub fn set_trust(
    conn: &Connection,
    tenant_id: &str,
    editor_id: &str,
    tier: TrustTier,
    by: &str,
) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO editor_trust (tenant_id, editor_id, tier, updated_at, updated_by)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(tenant_id, editor_id) DO UPDATE SET
            tier = excluded.tier,
            updated_at = excluded.updated_at,
            updated_by = excluded.updated_by",
        params![tenant_id, editor_id, tier.as_str(), chrono::Utc::now().timestamp(), by],
    )?;
    tracing::info!(
        tenant_id = %tenant_id,
        "obs editor_trust_set editor={editor_id} tier={} by={by}",
        tier.as_str()
    );
    Ok(())
}

/// Read an editor's trust tier. Unknown editors get the SAFE default: `per-op`.
pub fn get_trust(conn: &Connection, tenant_id: &str, editor_id: &str) -> anyhow::Result<TrustTier> {
    let tier: Option<String> = conn
        .query_row(
            "SELECT tier FROM editor_trust WHERE tenant_id = ?1 AND editor_id = ?2",
            params![tenant_id, editor_id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(tier.as_deref().and_then(TrustTier::parse).unwrap_or(TrustTier::PerOp))
}

/// One batch's outcome: what approved, and what was skipped with WHY — the skip
/// list is the editor's per-op review queue, never silently hidden.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BatchOutcome {
    pub approved: Vec<String>,
    /// `(entry_id, reason)` — left `proposed` for per-op review.
    pub skipped: Vec<(String, String)>,
}

/// "Approve all mechanical" for one `(tenant, asset, branch)`. The actor must be
/// HUMAN (one human tap = N audited per-op confirms) and the editor's trust tier
/// must allow batching. Per-entry failures are reported as skips — the batch
/// never aborts halfway.
pub fn approve_all_mechanical(
    conn: &Connection,
    tenant_id: &str,
    asset_hash: &str,
    branch: &str,
    editor_id: &str,
    actor: Actor,
) -> Result<BatchOutcome, ReviewError> {
    // Human-only, checked up-front for a clean denial (confirm_op re-enforces it
    // per entry — defense in depth, same error either way).
    if actor != Actor::Human {
        return Err(ReviewError::GatedNonHuman {
            event: "batch_confirm".to_string(),
            actor: actor.as_str().to_string(),
        });
    }

    let tier = get_trust(conn, tenant_id, editor_id).map_err(ReviewError::from)?;
    if tier == TrustTier::PerOp {
        return Err(ReviewError::Unauthorized {
            event: "batch_confirm".to_string(),
            actor: editor_id.to_string(),
            required: "trust tier mechanical|senior".to_string(),
        });
    }

    // The open mechanical proposals, in apply order. Only kind='op' rows are
    // candidates — notes/markers are creative signal, never batchable.
    let mut stmt = conn
        .prepare(
            "SELECT id, op, params FROM change_entry
             WHERE tenant_id = ?1 AND asset_hash = ?2 AND branch = ?3
               AND kind = 'op' AND state = 'proposed' AND active = 1
             ORDER BY seq ASC, id ASC",
        )
        .map_err(ReviewError::from)?;
    let candidates: Vec<(String, Option<String>, String)> = stmt
        .query_map(params![tenant_id, asset_hash, branch], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, String>(2)?))
        })
        .map_err(ReviewError::from)?
        .collect::<rusqlite::Result<_>>()
        .map_err(ReviewError::from)?;

    let mut out = BatchOutcome::default();
    for (id, op, params_json) in candidates {
        // Belt-and-braces: the append path already validates the vocab.
        let mechanical = op.as_deref().map(|o| changelist::OP_VOCAB.contains(&o)).unwrap_or(false);
        if !mechanical {
            out.skipped.push((id, "op outside the closed mechanical vocab".to_string()));
            continue;
        }
        // Proposer confidence rides params["confidence"]; absent = deterministic.
        let confidence = serde_json::from_str::<serde_json::Value>(&params_json)
            .ok()
            .and_then(|v| v.get("confidence").and_then(|c| c.as_f64()));
        if tier == TrustTier::Mechanical
            && let Some(c) = confidence
            && c < MIN_BATCH_CONFIDENCE
        {
            out.skipped.push((
                id,
                format!("confidence {c:.2} below the {MIN_BATCH_CONFIDENCE} batch floor"),
            ));
            continue;
        }
        match confirm_op(conn, tenant_id, &id, None, ConfirmDecision::Approve, Actor::Human) {
            Ok(_) => out.approved.push(id),
            Err(e) => out.skipped.push((id, format!("confirm failed: {e}"))),
        }
    }

    tracing::info!(
        tenant_id = %tenant_id,
        "obs batch_confirm asset={asset_hash} branch={branch} editor={editor_id} tier={} approved={} skipped={}",
        tier.as_str(),
        out.approved.len(),
        out.skipped.len()
    );
    Ok(out)
}
