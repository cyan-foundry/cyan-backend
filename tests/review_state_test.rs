//! Review-loop state-machine + editable-proposal lifecycle tests
//! (CYAN_REVIEW_LOOP_TRANSITION_CONTRACT.md).
//!
//! Every op takes an explicit `&Connection`, so each test runs against its own
//! in-memory SQLite DB — isolated, deterministic, no process-global state, no live
//! deps. Both the `review_state` and `changelist` tables are migrated per-test.
//! Assertions are synchronous on the store's own rows (the oracle), never on log
//! lines. The transition contract, the three-actor authority model, and the gating
//! rules are exactly per the locked spec — no assertion is weakened.

use cyan_backend::changelist::{self, ChangeEntry};
use cyan_backend::review_state::{self as rv, Actor, ConfirmDecision, ReviewError};
use rusqlite::Connection;
use serde_json::json;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    conn
}

const T: &str = "tenantA";
const A: &str = "assetA";
const B: &str = "main";

fn op_entry(op: &str, tc_in: i64, params: serde_json::Value, proposed_by: &str) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: A.to_string(),
        tenant_id: T.to_string(),
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
        author: Some("u1".to_string()),
        role: Some("editor".to_string()),
        proposed_by: Some(proposed_by.to_string()),
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

fn note_entry(intent: &str) -> ChangeEntry {
    let mut e = op_entry("trim", 0, json!({}), "agent");
    e.kind = "note".to_string();
    e.op = None;
    e.intent = intent.to_string();
    e
}

// ── happy path: DRAFT → … → DELIVERED ─────────────────────────────────────────

#[test]
fn happy_path_full_lifecycle_to_delivered() {
    let conn = db();
    // Seed one active op so snapshots have content.
    changelist::append(&conn, A, B, op_entry("trim", 0, json!({"edge":"head","frames":5}), "human"))
        .expect("seed op");

    let s0 = rv::start_draft(&conn, T, A, B).expect("start_draft");
    assert_eq!(s0.state, "DRAFT");
    assert_eq!(s0.round, 0);

    // DRAFT → IN_REVIEW (publish, human/external_send)
    let s1 = rv::publish_draft(&conn, T, A, B, Actor::Human).expect("publish");
    assert_eq!(s1.state, "IN_REVIEW");
    assert_eq!(s1.round, 0);

    // IN_REVIEW → NOTES_IN (sensor, auto)
    let s2 = rv::notes_arrived(&conn, T, A, B, Actor::Auto).expect("notes_arrived");
    assert_eq!(s2.state, "NOTES_IN");

    // NOTES_IN → CONFORMING (human confirm)
    let s3 = rv::confirm_notes(&conn, T, A, B, Actor::Human).expect("confirm_notes");
    assert_eq!(s3.state, "CONFORMING");

    // CONFORMING → CONFORMING (auto conform run)
    let s4 = rv::conform_run(&conn, T, A, B, Actor::Auto).expect("conform_run");
    assert_eq!(s4.state, "CONFORMING");

    // CONFORMING → IN_REVIEW (publish_proxy, human, round++)
    let s5 = rv::publish_proxy(&conn, T, A, B, Actor::Human, rv::DEFAULT_MAX_ROUNDS)
        .expect("publish_proxy");
    assert_eq!(s5.state, "IN_REVIEW");
    assert_eq!(s5.round, 1, "round increments on CONFORMING→IN_REVIEW publish");

    // IN_REVIEW → APPROVED (producer approves, auto/external approval)
    let s6 = rv::version_approved(&conn, T, A, B, Actor::Auto).expect("version_approved");
    assert_eq!(s6.state, "APPROVED");

    // APPROVED → FINISHING (finish, human/external_send)
    let s7 = rv::finish(&conn, T, A, B, Actor::Human).expect("finish");
    assert_eq!(s7.state, "FINISHING");

    // FINISHING → DELIVERED (auto on success)
    let s8 = rv::delivered(&conn, T, A, B, Actor::Auto).expect("delivered");
    assert_eq!(s8.state, "DELIVERED");
}

// ── every gated / external_send transition rejects a non-human caller ─────────

#[test]
fn publish_draft_rejects_non_human() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    for actor in [Actor::Auto, Actor::Agent] {
        let err = rv::publish_draft(&conn, T, A, B, actor).unwrap_err();
        assert!(
            matches!(err, ReviewError::GatedNonHuman { .. }),
            "publish is external_send → human-gated; {:?} must be rejected, got {err}",
            actor
        );
    }
    // state unchanged
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "DRAFT");
}

#[test]
fn confirm_notes_rejects_non_human() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap();
    rv::notes_arrived(&conn, T, A, B, Actor::Auto).unwrap();
    for actor in [Actor::Auto, Actor::Agent] {
        let err = rv::confirm_notes(&conn, T, A, B, actor).unwrap_err();
        assert!(
            matches!(err, ReviewError::GatedNonHuman { .. }),
            "confirm is the editable gate → human-only; {:?} rejected",
            actor
        );
    }
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "NOTES_IN");
}

#[test]
fn publish_proxy_rejects_non_human() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap();
    rv::notes_arrived(&conn, T, A, B, Actor::Auto).unwrap();
    rv::confirm_notes(&conn, T, A, B, Actor::Human).unwrap();
    for actor in [Actor::Auto, Actor::Agent] {
        let err = rv::publish_proxy(&conn, T, A, B, actor, rv::DEFAULT_MAX_ROUNDS).unwrap_err();
        assert!(matches!(err, ReviewError::GatedNonHuman { .. }));
    }
    // round did not advance
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().round, 0);
}

#[test]
fn finish_rejects_non_human() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap();
    rv::version_approved(&conn, T, A, B, Actor::Auto).unwrap();
    for actor in [Actor::Auto, Actor::Agent] {
        let err = rv::finish(&conn, T, A, B, actor).unwrap_err();
        assert!(
            matches!(err, ReviewError::GatedNonHuman { .. }),
            "finish is external_send → always human-gated"
        );
    }
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "APPROVED");
}

// ── AUTO-only transitions reject a human/agent firing them ────────────────────

#[test]
fn auto_transitions_reject_non_auto() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap();
    // notes_arrived is AUTO (sensor): a human/agent cannot fire it.
    for actor in [Actor::Human, Actor::Agent] {
        let err = rv::notes_arrived(&conn, T, A, B, actor).unwrap_err();
        assert!(
            matches!(err, ReviewError::Unauthorized { .. }),
            "sensor transition is auto-only; {:?} rejected",
            actor
        );
    }
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "IN_REVIEW");
}

// ── agent-proposes-only is enforced ───────────────────────────────────────────

#[test]
fn agent_may_only_propose_not_confirm() {
    let conn = db();
    // Agent proposes an op — allowed, lands as proposed + proposed_by=agent.
    let proposed = rv::propose_op(&conn, A, B, op_entry("fade", 10, json!({"dir":"in","frames":6}), "agent"), Actor::Agent)
        .expect("agent proposes");
    assert_eq!(proposed.state, "proposed");
    assert_eq!(proposed.proposed_by.as_deref(), Some("agent"));

    // Agent may NOT confirm (the editable gate is human-only).
    let err = rv::confirm_op(&conn, T, &proposed.id, None, ConfirmDecision::Approve, Actor::Agent)
        .unwrap_err();
    assert!(matches!(err, ReviewError::GatedNonHuman { .. }));

    // A human/auto caller may NOT use propose_op (that path is agent-only).
    for actor in [Actor::Human, Actor::Auto] {
        let err = rv::propose_op(&conn, A, B, op_entry("mute", 5, json!({}), "human"), actor)
            .unwrap_err();
        assert!(matches!(err, ReviewError::Unauthorized { .. }));
    }
}

// ── invalid transition is a typed error, not a panic ──────────────────────────

#[test]
fn invalid_transition_is_typed_error() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    // From DRAFT you cannot finish() (finish is APPROVED→FINISHING).
    let err = rv::finish(&conn, T, A, B, Actor::Human).unwrap_err();
    assert!(
        matches!(err, ReviewError::InvalidTransition { .. }),
        "wrong from-state → InvalidTransition, got {err}"
    );
    // From DRAFT you cannot conform_run (that's CONFORMING→CONFORMING).
    let err = rv::conform_run(&conn, T, A, B, Actor::Auto).unwrap_err();
    assert!(matches!(err, ReviewError::InvalidTransition { .. }));
    // A transition on a missing row is NotFound, not a panic.
    let err = rv::publish_draft(&conn, T, "missing-asset", B, Actor::Human).unwrap_err();
    assert!(matches!(err, ReviewError::NotFound { .. }));
}

// ── confirm_op with edited params updates + activates; reject leaves inactive ──

#[test]
fn confirm_op_edited_params_updates_and_activates() {
    let conn = db();
    let proposed = rv::propose_op(&conn, A, B, op_entry("trim", 0, json!({"edge":"head","frames":10}), "agent"), Actor::Agent)
        .expect("propose");

    // Human approves with edited params — supersedes, then approves+activates new entry.
    let new = rv::confirm_op(
        &conn,
        T,
        &proposed.id,
        Some(json!({"edge":"head","frames":25})),
        ConfirmDecision::Approve,
        Actor::Human,
    )
    .expect("confirm edited");

    assert_ne!(new.id, proposed.id, "edited confirm creates a new (superseding) entry");
    assert_eq!(new.state, "approved");
    assert!(new.active, "the edited entry is active");
    assert_eq!(new.params, json!({"edge":"head","frames":25}), "edited params applied");
    assert_eq!(new.supersedes.as_deref(), Some(proposed.id.as_str()));

    // The original is now superseded + inactive.
    let orig = changelist::get_entry(&conn, T, &proposed.id).expect("orig");
    assert_eq!(orig.state, "superseded");
    assert!(!orig.active);
}

#[test]
fn confirm_op_approve_as_is_activates() {
    let conn = db();
    let proposed = rv::propose_op(&conn, A, B, op_entry("fade", 12, json!({"dir":"out","frames":8}), "agent"), Actor::Agent)
        .expect("propose");
    let out = rv::confirm_op(&conn, T, &proposed.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("approve");
    assert_eq!(out.id, proposed.id, "approve-as-is keeps the same entry");
    assert_eq!(out.state, "approved");
    assert!(out.active);
}

#[test]
fn confirm_op_reject_leaves_inactive() {
    let conn = db();
    let proposed = rv::propose_op(&conn, A, B, op_entry("speed", 30, json!({"ratio":0.5}), "agent"), Actor::Agent)
        .expect("propose");
    let out = rv::confirm_op(&conn, T, &proposed.id, None, ConfirmDecision::Reject, Actor::Human)
        .expect("reject");
    assert_eq!(out.state, "rejected");
    assert!(!out.active, "rejected op is inactive");
}

// ── creative note escalation: never auto-converted ────────────────────────────

#[test]
fn escalate_note_keep_vs_human_promote() {
    let conn = db();
    let note = changelist::append(&conn, A, B, note_entry("the open feels rushed"))
        .expect("append note");

    // Keep as a note (promote=None): no op created.
    let kept = rv::escalate_note(&conn, T, &note.id, None, Actor::Human).expect("keep");
    assert!(kept.is_none(), "keeping a note creates no op");

    // Human promotes it to an op — allowed, a new op entry appears.
    let promoted = rv::escalate_note(
        &conn,
        T,
        &note.id,
        Some(("trim".to_string(), json!({"edge":"head","frames":8}))),
        Actor::Human,
    )
    .expect("promote")
    .expect("op entry");
    assert_eq!(promoted.kind, "op");
    assert_eq!(promoted.op.as_deref(), Some("trim"));

    // Agent may NOT promote (only a human converts taste to an op).
    let err = rv::escalate_note(
        &conn,
        T,
        &note.id,
        Some(("fade".to_string(), json!({"dir":"in","frames":4}))),
        Actor::Agent,
    )
    .unwrap_err();
    assert!(matches!(err, ReviewError::GatedNonHuman { .. }));
}

// ── round increments + max_rounds caps ────────────────────────────────────────

#[test]
fn round_increments_and_max_rounds_caps() {
    let conn = db();
    changelist::append(&conn, A, B, op_entry("trim", 0, json!({"edge":"head","frames":3}), "human"))
        .expect("seed");
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap();

    let cap = 2;
    // Loop CONFORMING→IN_REVIEW `cap` times: rounds 1, 2.
    for expected_round in 1..=cap {
        rv::notes_arrived(&conn, T, A, B, Actor::Auto).unwrap();
        rv::confirm_notes(&conn, T, A, B, Actor::Human).unwrap();
        let s = rv::publish_proxy(&conn, T, A, B, Actor::Human, cap).expect("publish_proxy");
        assert_eq!(s.round, expected_round);
    }
    // One more round would be round 3 > cap → MaxRounds error.
    rv::notes_arrived(&conn, T, A, B, Actor::Auto).unwrap();
    rv::confirm_notes(&conn, T, A, B, Actor::Human).unwrap();
    let err = rv::publish_proxy(&conn, T, A, B, Actor::Human, cap).unwrap_err();
    assert!(
        matches!(err, ReviewError::MaxRounds { rounds: 3, cap: 2 }),
        "loop cap enforced, got {err}"
    );
    // Still CONFORMING — the cap did not advance state.
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "CONFORMING");
}

// ── nudge threshold query (derived, not a state) ──────────────────────────────

#[test]
fn nudge_threshold_query() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap(); // → IN_REVIEW, fresh

    // Fresh IN_REVIEW: with the default 48h threshold, no nudge.
    let none = rv::nudges_for(&conn, T, A, None, None).expect("nudges");
    assert!(none.is_empty(), "a fresh waiting state is not stale");

    // With a 0-second threshold, the same IN_REVIEW row IS stale → one nudge.
    let some = rv::nudges_for(&conn, T, A, Some(0), Some(0)).expect("nudges");
    assert_eq!(some.len(), 1);
    assert_eq!(some[0].state, "IN_REVIEW");
    assert!(some[0].waiting_secs >= some[0].threshold_secs);

    // A non-waiting state (DELIVERED) is never nudged, even at threshold 0.
    let conn2 = db();
    rv::start_draft(&conn2, T, A, B).unwrap();
    rv::publish_draft(&conn2, T, A, B, Actor::Human).unwrap();
    rv::version_approved(&conn2, T, A, B, Actor::Auto).unwrap();
    rv::finish(&conn2, T, A, B, Actor::Human).unwrap();
    rv::delivered(&conn2, T, A, B, Actor::Auto).unwrap();
    let d = rv::nudges_for(&conn2, T, A, Some(0), Some(0)).expect("nudges");
    assert!(d.is_empty(), "DELIVERED is not a waiting state");
}

// ── tenant / asset / branch isolation ─────────────────────────────────────────

#[test]
fn tenant_asset_branch_isolation() {
    let conn = db();
    // Same asset+branch, two tenants — independent state rows.
    rv::start_draft(&conn, "tA", A, B).unwrap();
    rv::start_draft(&conn, "tB", A, B).unwrap();
    rv::publish_draft(&conn, "tA", A, B, Actor::Human).unwrap();
    assert_eq!(rv::get(&conn, "tA", A, B).unwrap().unwrap().state, "IN_REVIEW");
    assert_eq!(rv::get(&conn, "tB", A, B).unwrap().unwrap().state, "DRAFT", "other tenant unaffected");

    // Same tenant+asset, two branches — independent.
    rv::start_draft(&conn, "tA", A, "cutdown").unwrap();
    assert_eq!(rv::get(&conn, "tA", A, "cutdown").unwrap().unwrap().state, "DRAFT");
    assert_eq!(rv::get(&conn, "tA", A, B).unwrap().unwrap().state, "IN_REVIEW", "other branch unaffected");

    // Same tenant+branch, two assets — independent.
    rv::start_draft(&conn, "tA", "assetZ", B).unwrap();
    assert_eq!(rv::get(&conn, "tA", "assetZ", B).unwrap().unwrap().state, "DRAFT");

    // nudges_for is asset-scoped: tenant tA / assetA only.
    let n = rv::nudges_for(&conn, "tA", A, Some(0), Some(0)).unwrap();
    assert!(n.iter().all(|x| x.asset_hash == A && x.tenant_id == "tA"));
}

// ── DELIVERED → NOTES_IN reopen forks a branch (human) ────────────────────────

#[test]
fn reopen_branches_off_delivered() {
    let conn = db();
    changelist::append(&conn, A, B, op_entry("level", 0, json!({"target_lufs":-14}), "human"))
        .expect("seed");
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap();
    rv::version_approved(&conn, T, A, B, Actor::Auto).unwrap();
    rv::finish(&conn, T, A, B, Actor::Human).unwrap();
    rv::delivered(&conn, T, A, B, Actor::Auto).unwrap();

    // Non-human cannot reopen.
    let err = rv::reopen_branch(&conn, T, A, B, "reopen-1", Actor::Agent).unwrap_err();
    assert!(matches!(err, ReviewError::GatedNonHuman { .. }));

    // Human reopens → new branch in NOTES_IN.
    let s = rv::reopen_branch(&conn, T, A, B, "reopen-1", Actor::Human).expect("reopen");
    assert_eq!(s.state, "NOTES_IN");
    assert_eq!(s.branch, "reopen-1");
    // The delivered branch is untouched.
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "DELIVERED");
    // The fork carried the active change-list onto the new branch.
    let forked = changelist::get(&conn, T, A, "reopen-1").expect("get forked");
    assert!(!forked.entries.is_empty(), "reopen forked the active ops");
}
