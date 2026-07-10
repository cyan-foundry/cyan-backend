//! feat/notes-constitution — the BATCH-CONFIRM gate (TONIGHT_RUN Part 4).
//!
//! "Approve all mechanical" + per-editor TRUST TIERS over the changelist confirm
//! surface — the throughput mechanism ("N assets, few editors"). Engine/seam
//! level only (Tier-1): explicit `&Connection`, in-memory SQLite, no GUI, no
//! live deps. The gate REUSES `review_state::confirm_op` per entry, so the
//! human-only authority model, the audit trail, and the confirm interlock hold
//! exactly as in the per-op path.
//!
//! Trust tiers (closed vocab):
//!   * `per-op` (default) — batch approval DENIED; the editor confirms one by one.
//!   * `mechanical` — may batch-approve closed-vocab op proposals that are
//!     confident (no `params.confidence`, i.e. deterministic, or confidence ≥ 0.8).
//!     Low-confidence ops are SKIPPED for per-op review, never silently approved.
//!   * `senior` — may batch-approve ALL open mechanical proposals.

use cyan_backend::batch_confirm::{self, TrustTier};
use cyan_backend::changelist::{self, ChangeEntry};
use cyan_backend::review_state::{self, Actor};
use rusqlite::Connection;
use serde_json::json;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("changelist migrate");
    review_state::migrate(&conn).expect("review_state migrate");
    batch_confirm::migrate(&conn).expect("batch_confirm migrate");
    // Idempotency, same discipline as every additive migration here.
    batch_confirm::migrate(&conn).expect("batch_confirm migrate twice");
    conn
}

/// A minimal op-kind entry (append fills id/hash/seq/state).
fn op_entry(tenant: &str, asset: &str, op: &str, tc_in: i64, params: serde_json::Value) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: asset.to_string(),
        tenant_id: tenant.to_string(),
        branch: None,
        track: Some("V1".to_string()),
        tc_in,
        tc_out: Some(tc_in + 24),
        kind: "op".to_string(),
        op: Some(op.to_string()),
        params,
        intent: format!("{op} at {tc_in}"),
        source: Some("frameio".to_string()),
        source_ref: None,
        author: Some("u-editor".to_string()),
        role: Some("editor".to_string()),
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
    }
}

fn note_entry(tenant: &str, asset: &str, text: &str) -> ChangeEntry {
    let mut e = op_entry(tenant, asset, "trim", 0, json!({}));
    e.kind = "note".to_string();
    e.op = None;
    e.intent = text.to_string();
    e
}

/// Agent proposes an op through the authority-checked path.
fn propose(conn: &Connection, asset: &str, entry: ChangeEntry) -> ChangeEntry {
    review_state::propose_op(conn, asset, "main", entry, Actor::Agent).expect("agent proposes")
}

fn state_of(conn: &Connection, tenant: &str, id: &str) -> (String, bool) {
    let e = changelist::get_entry(conn, tenant, id).expect("entry");
    (e.state, e.active)
}

// ════════════════════════════════════════════════════════════════════════════
// 1. Trust tiers: closed vocab, defaulting, and last-write-wins updates.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn trust_tier_vocab_and_default() {
    let conn = db();

    assert_eq!(TrustTier::parse("per-op"), Some(TrustTier::PerOp));
    assert_eq!(TrustTier::parse("mechanical"), Some(TrustTier::Mechanical));
    assert_eq!(TrustTier::parse("senior"), Some(TrustTier::Senior));
    assert_eq!(TrustTier::parse("root"), None, "unknown tiers rejected");

    // Unknown editor ⇒ the SAFE default: per-op.
    assert_eq!(
        batch_confirm::get_trust(&conn, "t1", "ed-unknown").expect("get"),
        TrustTier::PerOp
    );

    // Set + read back, tenant-scoped; latest write wins.
    batch_confirm::set_trust(&conn, "t1", "ed-1", TrustTier::Mechanical, "admin").expect("set");
    assert_eq!(batch_confirm::get_trust(&conn, "t1", "ed-1").expect("get"), TrustTier::Mechanical);
    batch_confirm::set_trust(&conn, "t1", "ed-1", TrustTier::Senior, "admin").expect("update");
    assert_eq!(batch_confirm::get_trust(&conn, "t1", "ed-1").expect("get"), TrustTier::Senior);

    // The SAME editor id under another tenant keeps its own (default) tier.
    assert_eq!(
        batch_confirm::get_trust(&conn, "t2", "ed-1").expect("get"),
        TrustTier::PerOp,
        "trust is tenant-scoped"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Default tier: batch approval is DENIED — nothing changes state.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn per_op_tier_cannot_batch_confirm() {
    let conn = db();
    let e = propose(&conn, "assetA", op_entry("t1", "assetA", "trim", 0, json!({"frames": 12})));

    let err = batch_confirm::approve_all_mechanical(&conn, "t1", "assetA", "main", "ed-new", Actor::Human)
        .expect_err("per-op tier must be denied");
    let msg = err.to_string();
    assert!(msg.contains("batch"), "the denial names the batch gate: {msg}");

    let (state, active) = state_of(&conn, "t1", &e.id);
    assert_eq!(state, "proposed", "nothing approved on denial");
    assert!(active);
}

// ════════════════════════════════════════════════════════════════════════════
// 3. Mechanical tier: confident closed-vocab ops approve in one tap; the
//    low-confidence one is SKIPPED (left for per-op review); notes untouched.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn mechanical_tier_approves_confident_ops_and_skips_low_confidence() {
    let conn = db();
    batch_confirm::set_trust(&conn, "t1", "ed-1", TrustTier::Mechanical, "admin").expect("trust");

    // Deterministic proposal (no confidence — the regex proposer's shape).
    let det = propose(&conn, "assetA", op_entry("t1", "assetA", "trim", 0, json!({"frames": 12})));
    // Confident LLM proposal (the JOIN's adapter carries confidence in params).
    let hi = propose(
        &conn,
        "assetA",
        op_entry("t1", "assetA", "level", 100, json!({"db": -14, "confidence": 0.93})),
    );
    // Low-confidence LLM proposal — must NOT ride the batch.
    let lo = propose(
        &conn,
        "assetA",
        op_entry("t1", "assetA", "fade", 200, json!({"frames": 12, "confidence": 0.41})),
    );
    // A creative note — never a batch candidate.
    let note = propose(&conn, "assetA", note_entry("t1", "assetA", "the open feels rushed"));

    let out = batch_confirm::approve_all_mechanical(&conn, "t1", "assetA", "main", "ed-1", Actor::Human)
        .expect("mechanical tier batch");

    assert_eq!(out.approved.len(), 2, "the two confident ops approve: {out:?}");
    assert!(out.approved.contains(&det.id));
    assert!(out.approved.contains(&hi.id));
    assert_eq!(out.skipped.len(), 1, "the low-confidence op is reported, not hidden");
    assert_eq!(out.skipped[0].0, lo.id);
    assert!(out.skipped[0].1.contains("confidence"), "skip reason names confidence");

    assert_eq!(state_of(&conn, "t1", &det.id), ("approved".to_string(), true));
    assert_eq!(state_of(&conn, "t1", &hi.id), ("approved".to_string(), true));
    assert_eq!(state_of(&conn, "t1", &lo.id), ("proposed".to_string(), true), "skipped stays open");
    assert_eq!(state_of(&conn, "t1", &note.id), ("proposed".to_string(), true), "notes untouched");

    // The approvals rode the REAL confirm surface: approved_by is stamped.
    let e = changelist::get_entry(&conn, "t1", &det.id).expect("entry");
    assert_eq!(e.approved_by.as_deref(), Some("human"));
    assert!(e.approved_at.is_some());

    // And they are exactly what conform will consume.
    let ops = changelist::approved_ops(&conn, "t1", "assetA", "main").expect("approved_ops");
    let ids: Vec<&str> = ops.iter().map(|o| o.entry_id.as_str()).collect();
    assert!(ids.contains(&det.id.as_str()) && ids.contains(&hi.id.as_str()));
    assert!(!ids.contains(&lo.id.as_str()));
}

// ════════════════════════════════════════════════════════════════════════════
// 4. Senior tier: low-confidence ops batch too.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn senior_tier_batches_low_confidence_ops_as_well() {
    let conn = db();
    batch_confirm::set_trust(&conn, "t1", "ed-sr", TrustTier::Senior, "admin").expect("trust");

    let lo = propose(
        &conn,
        "assetB",
        op_entry("t1", "assetB", "fade", 0, json!({"frames": 6, "confidence": 0.3})),
    );

    let out = batch_confirm::approve_all_mechanical(&conn, "t1", "assetB", "main", "ed-sr", Actor::Human)
        .expect("senior batch");
    assert_eq!(out.approved, vec![lo.id.clone()]);
    assert!(out.skipped.is_empty());
    assert_eq!(state_of(&conn, "t1", &lo.id), ("approved".to_string(), true));
}

// ════════════════════════════════════════════════════════════════════════════
// 5. The authority model holds: only a HUMAN can fire the batch, whatever the
//    tier says — an agent with a senior editor id is still rejected.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn non_human_actors_cannot_batch_confirm() {
    let conn = db();
    batch_confirm::set_trust(&conn, "t1", "ed-sr", TrustTier::Senior, "admin").expect("trust");
    let e = propose(&conn, "assetC", op_entry("t1", "assetC", "trim", 0, json!({"frames": 3})));

    for actor in [Actor::Agent, Actor::Auto] {
        let err =
            batch_confirm::approve_all_mechanical(&conn, "t1", "assetC", "main", "ed-sr", actor)
                .expect_err("non-human actors rejected");
        assert!(err.to_string().contains("human"), "denial names the human gate: {err}");
    }
    assert_eq!(state_of(&conn, "t1", &e.id), ("proposed".to_string(), true));
}

// ════════════════════════════════════════════════════════════════════════════
// 6. Tenant isolation: a batch for tenant A never touches tenant B's proposals,
//    even on identical asset/branch ids.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn batch_confirm_is_tenant_isolated() {
    let conn = db();
    batch_confirm::set_trust(&conn, "tA", "ed-1", TrustTier::Senior, "admin").expect("trust");

    let a = propose(&conn, "shared-asset", op_entry("tA", "shared-asset", "trim", 0, json!({"frames": 2})));
    let b = propose(&conn, "shared-asset", op_entry("tB", "shared-asset", "trim", 0, json!({"frames": 2})));

    let out = batch_confirm::approve_all_mechanical(&conn, "tA", "shared-asset", "main", "ed-1", Actor::Human)
        .expect("tenant A batch");
    assert_eq!(out.approved, vec![a.id.clone()]);

    assert_eq!(state_of(&conn, "tA", &a.id), ("approved".to_string(), true));
    assert_eq!(
        state_of(&conn, "tB", &b.id),
        ("proposed".to_string(), true),
        "tenant B's proposal must be untouched by tenant A's batch"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 7. Empty batch: no open proposals is a clean no-op outcome, not an error.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn empty_batch_is_a_clean_noop() {
    let conn = db();
    batch_confirm::set_trust(&conn, "t1", "ed-1", TrustTier::Mechanical, "admin").expect("trust");
    let out = batch_confirm::approve_all_mechanical(&conn, "t1", "asset-none", "main", "ed-1", Actor::Human)
        .expect("empty batch");
    assert!(out.approved.is_empty());
    assert!(out.skipped.is_empty());
}
