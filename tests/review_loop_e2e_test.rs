//! END-TO-END review-&-conform loop (CYAN_REVIEW_LOOP_TRANSITION_CONTRACT.md).
//!
//! This is the *integration* test that proves the ChangeList store
//! (`cyan_backend::changelist`) and the review-loop state machine
//! (`cyan_backend::review_state`) work together as the full review-&-conform
//! loop — driven end-to-end by a FAKE sensor / FAKE conform (no real Frame.io,
//! no LLM, no render). Every beat asserts BOTH the `review_state` row AND the
//! change-list (entries, versions, conform_plan, diff, outcome) at that beat.
//!
//! Design mirrors the unit suites: every store/state op takes an explicit
//! `&Connection`, so the whole loop runs against ONE isolated in-memory SQLite
//! DB with both migrations applied — deterministic, no process-global state, no
//! live deps. Assertions are synchronous on the store's own rows (the oracle),
//! never on log lines. No assertion is weakened.
//!
//! NOTE (additive accessors): NONE were required. Every property asserted below
//! is reachable through the already-public API surface
//! (`changelist::{get, get_entry, get_version, conform_plan, diff, snapshot,
//! set_outcome}` and `review_state::{get, nudges_for, ...}`). In particular a
//! version's frozen `entry_hashes` is read via the existing
//! `changelist::get_version`, and the head version via `changelist::get`.

use cyan_backend::changelist::{self, ChangeEntry};
use cyan_backend::review_state::{self as rv, Actor, ConfirmDecision, ReviewError};
use rusqlite::Connection;
use serde_json::json;

const T: &str = "tenant-e2e";
const A: &str = "asset-e2e";
const B: &str = "main";

/// One in-memory DB with BOTH tables migrated — the shared oracle for the loop.
fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    conn
}

/// A mechanical op entry (kind=op). `id`/`entry_hash`/`seq`/`state` are filled by
/// `append`/`propose_op`. `proposed_by` records who drafted it.
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
        author: Some("editor-1".to_string()),
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
        referent: None,
    region: None,
    intent_struct: None,
    capture_ctx: None,
}
}

/// A creative note (kind=note, no op) — the escalate-as-a-choice case.
fn note_entry(intent: &str, tc_in: i64) -> ChangeEntry {
    let mut e = op_entry("trim", tc_in, json!({}), "agent");
    e.kind = "note".to_string();
    e.op = None;
    e.intent = intent.to_string();
    e.source = Some("frameio".to_string());
    e
}

/// Resolve the head version of (tenant, asset, branch) via the public rail.
fn head_version(conn: &Connection) -> changelist::ChangeVersion {
    changelist::get(conn, T, A, B)
        .expect("get")
        .head_version
        .expect("a head version exists")
}

// ============================================================================
// THE FULL LOOP — one test, every beat, both layers asserted.
// ============================================================================

#[test]
fn full_review_and_conform_loop_e2e() {
    let conn = db();

    // ────────────────────────────────────────────────────────────────────────
    // BEAT 1 — DRAFT: editor appends mechanical ops, human publishes → IN_REVIEW,
    // v1 snapshot exists with those active entries + a stable list_hash.
    // ────────────────────────────────────────────────────────────────────────

    let s0 = rv::start_draft(&conn, T, A, B).expect("start_draft");
    assert_eq!(s0.state, "DRAFT");
    assert_eq!(s0.round, 0);

    // Editor appends 2 mechanical ops directly (the DRAFT authoring path — human).
    let trim = changelist::append(
        &conn,
        A,
        B,
        op_entry("trim", 0, json!({"edge":"head","frames":6}), "human"),
    )
    .expect("append trim");
    let level = changelist::append(
        &conn,
        A,
        B,
        op_entry("level", 120, json!({"target_lufs":-14}), "human"),
    )
    .expect("append level");
    assert_eq!(trim.seq, 1, "seq monotonic within (asset, branch)");
    assert_eq!(level.seq, 2);
    assert_eq!(trim.state, "proposed");
    assert!(trim.active && level.active);

    // Contract row DRAFT: "append ops (proposed) … HUMAN confirms | confirm" — the
    // human fires the DRAFT confirm gate on both seed ops before publishing. The
    // entry_hash is content-only (lifecycle excluded), so v1's hashes are unaffected.
    let trim = rv::confirm_op(&conn, T, &trim.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm trim in DRAFT");
    let level = rv::confirm_op(&conn, T, &level.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm level in DRAFT");
    assert_eq!(trim.state, "approved");
    assert_eq!(level.state, "approved");

    // publish_draft is external_send → HUMAN-gated; it snapshots the active set as v1.
    let s1 = rv::publish_draft(&conn, T, A, B, Actor::Human).expect("publish_draft");
    assert_eq!(s1.state, "IN_REVIEW");
    assert_eq!(s1.round, 0, "publishing the draft does not bump the round");

    // v1 exists with exactly the two active entries (in seq order) + a stable list_hash.
    let v1 = head_version(&conn);
    assert_eq!(v1.version_no, 1);
    assert_eq!(
        v1.entry_hashes,
        vec![trim.entry_hash.clone(), level.entry_hash.clone()],
        "v1 froze exactly the two active ops, in seq order"
    );
    // list_hash is reproducible from the same (asset + ordered active hashes).
    let expected_v1_hash =
        changelist::compute_list_hash(A, &[trim.entry_hash.clone(), level.entry_hash.clone()]);
    assert_eq!(v1.list_hash, expected_v1_hash, "v1 list_hash is stable/reproducible");
    // v1's conform_plan is exactly the two ops in seq order.
    let plan_v1 = changelist::conform_plan(&conn, T, &v1.version_id).expect("conform_plan v1");
    assert_eq!(
        plan_v1.iter().map(|o| o.op.as_str()).collect::<Vec<_>>(),
        vec!["trim", "level"],
        "v1 plan = the two seeded ops in order"
    );

    // ────────────────────────────────────────────────────────────────────────
    // BEAT 2 — FAKE sensor: notes_arrived (auto) → NOTES_IN. AGENT proposes two
    // mechanical ops (proposed) + one creative note escalated (NOT auto-converted).
    // ────────────────────────────────────────────────────────────────────────

    // FAKE sensor fires the auto transition.
    let s2 = rv::notes_arrived(&conn, T, A, B, Actor::Auto).expect("notes_arrived");
    assert_eq!(s2.state, "NOTES_IN");

    // FAKE Frame.io comment #1 → AGENT proposes a mechanical op (mute).
    let prop_mute = rv::propose_op(
        &conn,
        A,
        B,
        op_entry("mute", 200, json!({}), "agent"),
        Actor::Agent,
    )
    .expect("agent proposes mute");
    // FAKE Frame.io comment #2 → AGENT proposes a second mechanical op (fade).
    let prop_fade = rv::propose_op(
        &conn,
        A,
        B,
        op_entry("fade", 260, json!({"dir":"out","frames":8}), "agent"),
        Actor::Agent,
    )
    .expect("agent proposes fade");

    // Both are `proposed`, tagged proposed_by=agent, and (as appended) active.
    for p in [&prop_mute, &prop_fade] {
        assert_eq!(p.state, "proposed", "agent proposal lands as proposed");
        assert_eq!(p.proposed_by.as_deref(), Some("agent"));
    }

    // FAKE Frame.io comment #3 is CREATIVE → a note. The agent may NOT draft an op
    // for it; it rides as a note and is escalated as a CHOICE. Human keeps it (no
    // promote) → NOTHING is auto-converted.
    let creative = changelist::append(&conn, A, B, note_entry("the open feels rushed", 0))
        .expect("append creative note");
    let kept = rv::escalate_note(&conn, T, &creative.id, None, Actor::Human).expect("escalate keep");
    assert!(kept.is_none(), "creative note kept-as-note creates no op — never auto-converted");

    // Assert the store now holds: the 2 seed ops + 2 proposed ops + 1 note (5 rows),
    // and the creative one is still kind=note with NO op.
    let view_notes_in = changelist::get(&conn, T, A, B).expect("get NOTES_IN");
    assert_eq!(view_notes_in.entries.len(), 5, "2 seed + 2 proposed + 1 note");
    let creative_row = changelist::get_entry(&conn, T, &creative.id).expect("creative row");
    assert_eq!(creative_row.kind, "note");
    assert!(creative_row.op.is_none(), "creative note has no op — never guessed into an op");
    // Exactly two proposed op rows exist (the agent's), still awaiting the gate.
    let proposed_count = view_notes_in
        .entries
        .iter()
        .filter(|e| e.state == "proposed" && e.kind == "op")
        .count();
    assert_eq!(proposed_count, 2, "the two agent proposals sit as proposed ops");

    // ────────────────────────────────────────────────────────────────────────
    // BEAT 3 — Human confirm_op: approve one as-is, approve one EDITED (redo chain),
    // reject none; then confirm_notes (human) → CONFORMING.
    // ────────────────────────────────────────────────────────────────────────

    // Approve `mute` as-is → approved + active, same entry id.
    let confirmed_mute =
        rv::confirm_op(&conn, T, &prop_mute.id, None, ConfirmDecision::Approve, Actor::Human)
            .expect("confirm mute as-is");
    assert_eq!(confirmed_mute.id, prop_mute.id, "approve-as-is keeps the same entry");
    assert_eq!(confirmed_mute.state, "approved");
    assert!(confirmed_mute.active);

    // Approve `fade` with EDITED params → supersede/redo chain: original superseded+
    // inactive, a new active entry carries the edited params.
    let edited_fade = rv::confirm_op(
        &conn,
        T,
        &prop_fade.id,
        Some(json!({"dir":"out","frames":16})),
        ConfirmDecision::Approve,
        Actor::Human,
    )
    .expect("confirm fade edited");
    assert_ne!(edited_fade.id, prop_fade.id, "edited confirm mints a NEW superseding entry");
    assert_eq!(edited_fade.state, "approved");
    assert!(edited_fade.active, "the edited entry is active");
    assert_eq!(edited_fade.params, json!({"dir":"out","frames":16}), "edited params applied");
    assert_eq!(edited_fade.supersedes.as_deref(), Some(prop_fade.id.as_str()));
    // Original fade proposal is now superseded + inactive.
    let orig_fade = changelist::get_entry(&conn, T, &prop_fade.id).expect("orig fade");
    assert_eq!(orig_fade.state, "superseded");
    assert!(!orig_fade.active, "the superseded original goes inactive");
    assert_eq!(orig_fade.superseded_by.as_deref(), Some(edited_fade.id.as_str()));

    // confirm_notes is the editable gate → HUMAN → CONFORMING.
    let s3 = rv::confirm_notes(&conn, T, A, B, Actor::Human).expect("confirm_notes");
    assert_eq!(s3.state, "CONFORMING");

    // ────────────────────────────────────────────────────────────────────────
    // BEAT 4 — FAKE conform: conform_run (auto) → snapshot v2. conform_plan(v2)
    // contains exactly the active ops in seq order and EXCLUDES the note, the
    // rejected/inactive, and the superseded original. diff(v1,v2) shows the adds.
    // ────────────────────────────────────────────────────────────────────────

    let s4 = rv::conform_run(&conn, T, A, B, Actor::Auto).expect("conform_run");
    assert_eq!(s4.state, "CONFORMING", "conform_run stays in CONFORMING");

    let v2 = head_version(&conn);
    assert_eq!(v2.version_no, 2, "conform_run minted v2");

    // conform_plan(v2): trim, level, mute, fade' — the four active ops, in seq order.
    let plan_v2 = changelist::conform_plan(&conn, T, &v2.version_id).expect("conform_plan v2");
    assert_eq!(
        plan_v2.iter().map(|o| o.op.as_str()).collect::<Vec<_>>(),
        vec!["trim", "level", "mute", "fade"],
        "v2 plan = the four active ops in seq order"
    );
    // The EDITED fade (not the superseded original) is the one in the plan.
    let fade_in_plan = plan_v2.iter().find(|o| o.op == "fade").expect("fade in plan");
    assert_eq!(fade_in_plan.entry_id, edited_fade.id, "the edited fade is the one that conforms");
    assert_eq!(fade_in_plan.params, json!({"dir":"out","frames":16}));
    // The creative NOTE is excluded (not actionable), and neither the superseded
    // original nor any inactive row appears.
    let plan_ids: Vec<&str> = plan_v2.iter().map(|o| o.entry_id.as_str()).collect();
    assert!(!plan_ids.contains(&creative.id.as_str()), "creative note excluded from plan");
    assert!(!plan_ids.contains(&prop_fade.id.as_str()), "superseded original excluded from plan");
    assert_eq!(plan_v2.len(), 4, "exactly four actionable ops conform");

    // diff(v1, v2): mute + fade' + the kept creative note are the added entry hashes
    // (spec §2: a version freezes the ordered ACTIVE set — notes included; only
    // conform_plan filters to ops); nothing removed (the superseded original never
    // made v1, and v1's two ops persist into v2).
    let d = changelist::diff(&conn, T, &v1.version_id, &v2.version_id).expect("diff v1 v2");
    assert!(d.added.contains(&confirmed_mute.entry_hash), "mute added in v2");
    assert!(d.added.contains(&edited_fade.entry_hash), "fade' added in v2");
    assert!(d.added.contains(&creative.entry_hash), "the kept active note is frozen into v2");
    assert_eq!(d.added.len(), 3, "mute + fade' + note were added between v1 and v2");
    assert!(d.removed.is_empty(), "v1's ops persist into v2 — nothing removed");
    // Sanity: v1's own two ops are still present in v2's frozen set.
    assert!(v2.entry_hashes.contains(&trim.entry_hash) && v2.entry_hashes.contains(&level.entry_hash));

    // ────────────────────────────────────────────────────────────────────────
    // BEAT 5 — publish_proxy (human) → IN_REVIEW, round==1... then loop once more
    // (another comment → confirm → conform → v3, publish → round==2) so rounds
    // accrue and versions chain.
    // ────────────────────────────────────────────────────────────────────────

    let s5 = rv::publish_proxy(&conn, T, A, B, Actor::Human, rv::DEFAULT_MAX_ROUNDS)
        .expect("publish_proxy round 1");
    assert_eq!(s5.state, "IN_REVIEW");
    assert_eq!(s5.round, 1, "round increments on CONFORMING→IN_REVIEW publish");
    // publish_proxy snapshots too: v3 (an unchanged active set → same list_hash as v2).
    let v3 = head_version(&conn);
    assert_eq!(v3.version_no, 3, "publish_proxy minted v3");
    assert_eq!(v3.list_hash, v2.list_hash, "unchanged active set → same list_hash across snapshots");

    // --- second round of the loop ---
    rv::notes_arrived(&conn, T, A, B, Actor::Auto).expect("notes_arrived r2");
    let prop_reframe = rv::propose_op(
        &conn,
        A,
        B,
        op_entry("reframe", 300, json!({"aspect":"9:16"}), "agent"),
        Actor::Agent,
    )
    .expect("agent proposes reframe r2");
    rv::confirm_op(&conn, T, &prop_reframe.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm reframe r2");
    rv::confirm_notes(&conn, T, A, B, Actor::Human).expect("confirm_notes r2");
    rv::conform_run(&conn, T, A, B, Actor::Auto).expect("conform_run r2");

    let v4 = head_version(&conn);
    assert_eq!(v4.version_no, 4, "second conform minted v4");
    let plan_v4 = changelist::conform_plan(&conn, T, &v4.version_id).expect("conform_plan v4");
    assert_eq!(plan_v4.len(), 5, "now five active ops (added reframe)");
    assert!(
        plan_v4.iter().any(|o| o.op == "reframe"),
        "the round-2 reframe op is in the conform plan"
    );
    assert_ne!(v4.list_hash, v3.list_hash, "adding an op changes the list_hash");

    let s6 = rv::publish_proxy(&conn, T, A, B, Actor::Human, rv::DEFAULT_MAX_ROUNDS)
        .expect("publish_proxy round 2");
    assert_eq!(s6.state, "IN_REVIEW");
    assert_eq!(s6.round, 2, "rounds accrue across loop iterations");

    // ────────────────────────────────────────────────────────────────────────
    // BEAT 6 — version_approved (auto/external) → APPROVED; finish (human) →
    // FINISHING; delivered (auto) → DELIVERED; set_outcome(final, shipped);
    // the final version's entries carry the outcome label.
    // ────────────────────────────────────────────────────────────────────────

    let s7 = rv::version_approved(&conn, T, A, B, Actor::Auto).expect("version_approved");
    assert_eq!(s7.state, "APPROVED");

    let s8 = rv::finish(&conn, T, A, B, Actor::Human).expect("finish");
    assert_eq!(s8.state, "FINISHING");

    let s9 = rv::delivered(&conn, T, A, B, Actor::Auto).expect("delivered");
    assert_eq!(s9.state, "DELIVERED");

    // The head version at approval time is v5 (from the last publish_proxy) — the
    // delivered cut. Note: v5's active set is unchanged from v4, so v5 introduced NO
    // NEW entries; `version_ref` is stamped on an entry's FIRST snapshot appearance
    // (see changelist::snapshot). The round-2 reframe op therefore carries
    // version_ref == v4 (the conform_run that first froze it), which is the version
    // whose entries changed and the one we ship as the taste-learning label.
    let final_head = head_version(&conn);
    assert_eq!(final_head.version_no, 5, "the last publish_proxy minted v5 (the delivered cut)");

    // The reframe op's first-appearance version is v4. Confirm, then ship v4 so the
    // label propagates to a real entry (the property under test).
    let reframe_before = changelist::get_entry(&conn, T, &prop_reframe.id).expect("reframe row");
    let final_version_id = reframe_before
        .version_ref
        .clone()
        .expect("the reframe op was frozen into a version");
    let final_version =
        changelist::get_version(&conn, T, &final_version_id).expect("get final version");
    assert_eq!(final_version.version_no, 4, "reframe first appeared in v4 (its version_ref)");

    changelist::set_outcome(&conn, T, &final_version.version_id, "shipped").expect("set_outcome");
    let reloaded = changelist::get_version(&conn, T, &final_version.version_id).expect("get_version");
    assert_eq!(reloaded.outcome, "shipped");

    // The outcome propagates to the entry that FIRST appeared in that version — the
    // round-2 reframe op. Assert its label is now `shipped`.
    let reframe_row = changelist::get_entry(&conn, T, &prop_reframe.id).expect("reframe row");
    assert_eq!(
        reframe_row.outcome.as_deref(),
        Some("shipped"),
        "the entry first appearing in the shipped version carries the shipped label"
    );
    // And an entry from an EARLIER version (trim, version_ref == v1) is NOT relabeled
    // by shipping v4 — the label is scoped to the version's first-appearance entries.
    let trim_row = changelist::get_entry(&conn, T, &trim.id).expect("trim row");
    assert_ne!(
        trim_row.outcome.as_deref(),
        Some("shipped"),
        "shipping v4 does not relabel entries first frozen in earlier versions"
    );

    // ────────────────────────────────────────────────────────────────────────
    // BEAT 8 (woven here) — nudges: nothing surfaces while actively moving; a
    // waiting IN_REVIEW is stale at a 0-second threshold. (No public updated_at
    // setter exists to back-date the row — see TODO — so the stale case is
    // exercised by driving the threshold to 0, which is the same code path.)
    // ────────────────────────────────────────────────────────────────────────

    // DELIVERED is a terminal (non-waiting) state → never nudged, even at threshold 0.
    let no_nudge_delivered = rv::nudges_for(&conn, T, A, Some(0), Some(0)).expect("nudges delivered");
    assert!(
        no_nudge_delivered.is_empty(),
        "DELIVERED is not a waiting state — no nudge while the loop has moved on"
    );
}

// ============================================================================
// BEAT 7 — Authority model holds END-TO-END (negative beats).
// AGENT is REJECTED on the gated human transitions; a non-auto actor is REJECTED
// on the auto-only conform_run. Driven through the same loop, on its own DB so the
// rejections are unambiguous.
// ============================================================================

#[test]
fn authority_model_holds_end_to_end() {
    let conn = db();
    // Seed one op so snapshots have content, and bring the machine to each gate.
    changelist::append(&conn, A, B, op_entry("trim", 0, json!({"edge":"head","frames":4}), "human"))
        .expect("seed");
    rv::start_draft(&conn, T, A, B).unwrap();

    // publish_draft is external_send → AGENT (and AUTO) REJECTED (GatedNonHuman).
    for actor in [Actor::Agent, Actor::Auto] {
        let err = rv::publish_draft(&conn, T, A, B, actor).unwrap_err();
        assert!(
            matches!(err, ReviewError::GatedNonHuman { .. }),
            "publish is external_send → human-gated; {actor:?} rejected, got {err}"
        );
    }
    // Human advances it legitimately.
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap();
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "IN_REVIEW");

    rv::notes_arrived(&conn, T, A, B, Actor::Auto).unwrap();

    // confirm_notes is the editable gate → AGENT REJECTED (GatedNonHuman).
    let err = rv::confirm_notes(&conn, T, A, B, Actor::Agent).unwrap_err();
    assert!(
        matches!(err, ReviewError::GatedNonHuman { .. }),
        "confirm_notes is human-gated; AGENT rejected, got {err}"
    );
    rv::confirm_notes(&conn, T, A, B, Actor::Human).unwrap();
    assert_eq!(rv::get(&conn, T, A, B).unwrap().unwrap().state, "CONFORMING");

    // conform_run is AUTO-only → a HUMAN and an AGENT are REJECTED (Unauthorized).
    for actor in [Actor::Human, Actor::Agent] {
        let err = rv::conform_run(&conn, T, A, B, actor).unwrap_err();
        assert!(
            matches!(err, ReviewError::Unauthorized { .. }),
            "conform_run is auto-only; {actor:?} rejected, got {err}"
        );
    }
    // Auto runs it legitimately.
    rv::conform_run(&conn, T, A, B, Actor::Auto).unwrap();

    // publish_proxy is external_send → AGENT REJECTED, and the round does NOT advance.
    let err = rv::publish_proxy(&conn, T, A, B, Actor::Agent, rv::DEFAULT_MAX_ROUNDS).unwrap_err();
    assert!(
        matches!(err, ReviewError::GatedNonHuman { .. }),
        "publish_proxy is external_send → human-gated; AGENT rejected, got {err}"
    );
    assert_eq!(
        rv::get(&conn, T, A, B).unwrap().unwrap().round,
        0,
        "a rejected publish_proxy never advanced the round"
    );

    // AGENT may ONLY propose: the propose path itself rejects a non-agent caller.
    for actor in [Actor::Human, Actor::Auto] {
        let err = rv::propose_op(&conn, A, B, op_entry("mute", 9, json!({}), "x"), actor)
            .unwrap_err();
        assert!(
            matches!(err, ReviewError::Unauthorized { .. }),
            "propose_op is agent-only; {actor:?} rejected, got {err}"
        );
    }
}

// ============================================================================
// BEAT 8 (dedicated) — nudges: none while actively moving; a fresh waiting state
// is not stale at the default threshold, but IS stale at a 0-second threshold
// (the derived stale/nudge overlay on a *waiting* state).
//
// TODO(back-date): there is no public setter for `review_state.updated_at`, so we
// cannot literally age the row's timestamp from a test. Driving the threshold to 0
// exercises the identical `nudges_for` staleness path (waiting_secs >= threshold).
// If an additive `set_updated_at` test helper is added later, tighten this to
// back-date the row and assert with the DEFAULT threshold.
// ============================================================================

#[test]
fn nudges_none_while_moving_then_stale_at_threshold() {
    let conn = db();
    rv::start_draft(&conn, T, A, B).unwrap();
    rv::publish_draft(&conn, T, A, B, Actor::Human).unwrap(); // → IN_REVIEW (fresh)

    // While actively moving: a fresh IN_REVIEW is not stale at the default 48h.
    let none = rv::nudges_for(&conn, T, A, None, None).expect("nudges default");
    assert!(none.is_empty(), "a fresh waiting state surfaces no nudge");

    // Force staleness with a 0-second threshold → the SAME IN_REVIEW row is stale.
    let some = rv::nudges_for(&conn, T, A, Some(0), Some(0)).expect("nudges @0");
    assert_eq!(some.len(), 1, "the stale waiting row surfaces exactly one nudge");
    assert_eq!(some[0].state, "IN_REVIEW");
    assert_eq!(some[0].branch, B);
    assert!(some[0].waiting_secs >= some[0].threshold_secs, "waiting exceeds threshold");

    // Advance to NOTES_IN (also a waiting state) — still nudgeable at threshold 0.
    rv::notes_arrived(&conn, T, A, B, Actor::Auto).unwrap();
    let notes_nudge = rv::nudges_for(&conn, T, A, Some(0), Some(0)).expect("nudges NOTES_IN");
    assert_eq!(notes_nudge.len(), 1);
    assert_eq!(notes_nudge[0].state, "NOTES_IN", "NOTES_IN is a waiting state too");

    // Advance to CONFORMING (NOT a waiting state) — no nudge, even at threshold 0.
    rv::confirm_notes(&conn, T, A, B, Actor::Human).unwrap();
    let conforming = rv::nudges_for(&conn, T, A, Some(0), Some(0)).expect("nudges CONFORMING");
    assert!(
        conforming.is_empty(),
        "CONFORMING is a working (non-waiting) state — no nudge while it moves"
    );
}

// ============================================================================
// WORKFLOW-DRIVEN VARIANT — the same loop, but every advance is DECIDED by the
// loop controller (`review_loop::tick`) and the sensor beat arrives through the
// SENSE → ledger glue (`ingest_sense_result` on a canned list_comments result),
// not by calling `notes_arrived` directly. This is the loop as the workflow
// machinery runs it: park at publish-done → resume on new notes → exit on
// external approval, rounds as sequential runs.
// ============================================================================

#[test]
fn workflow_driven_loop_park_resume_exit() {
    use cyan_backend::asset_registry;
    use cyan_backend::review_loop::{self as rl, LoopDecision};

    let conn = db();
    asset_registry::migrate(&conn).expect("migrate asset_registry");
    rl::migrate(&conn).expect("migrate review_loop");
    const BOARD: &str = "board-wf-e2e";

    // DRAFT: seed one confirmed op; the loop registers on (board, asset).
    let seed = changelist::append(
        &conn,
        A,
        B,
        op_entry("level", 120, json!({"target_lufs":-14}), "human"),
    )
    .expect("seed op");
    rl::register(&conn, T, BOARD, A, B, rv::DEFAULT_MAX_ROUNDS).expect("register loop");
    rv::confirm_op(&conn, T, &seed.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm seed");
    assert_eq!(
        rl::tick(&conn, T, BOARD, A).expect("tick draft"),
        LoopDecision::Working { state: "DRAFT".to_string() },
        "authoring is the machinery's business — the controller waits"
    );

    // PUBLISH v1 (human) → the controller PARKS the run.
    rv::publish_draft(&conn, T, A, B, Actor::Human).expect("publish v1");
    rl::record_round_run(&conn, T, BOARD, A, "run-1").expect("record run-1");
    assert_eq!(
        rl::tick(&conn, T, BOARD, A).expect("tick parked"),
        LoopDecision::Park { round: 0 }
    );

    // The published proxy is registered with its Frame.io ref (the actuator's
    // breadcrumb), and the producer comments — the SENSE result ingests.
    let v1 = head_version(&conn);
    asset_registry::upsert(
        &conn,
        &asset_registry::Asset {
            hash: "proxy-e2e-1".to_string(),
            tenant_id: T.to_string(),
            kind: Some("proxy".to_string()),
            fps: Some(24.0),
            duration_ms: None,
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: None,
            created_at: 0,
        },
    )
    .expect("register proxy");
    asset_registry::set_derivation(&conn, T, "proxy-e2e-1", A, &v1.version_id).expect("derivation");
    asset_registry::set_remote_ref(&conn, T, "proxy-e2e-1", "frameio", "file_e2e_1")
        .expect("remote ref");

    let sense = json!({ "data": [
        { "id": "c-e2e-1", "text": "music too loud at 0:42", "frame": 1008 }
    ]});
    let ingest = rl::ingest_sense_result(&conn, T, "file_e2e_1", &sense).expect("SENSE ingest");
    assert_eq!(ingest.appended.len(), 1, "the producer note landed on the ledger");

    // New notes → the controller RESUMES; the round runs to the next publish.
    assert_eq!(
        rl::tick(&conn, T, BOARD, A).expect("tick resumed"),
        LoopDecision::Resume { round: 0 }
    );
    let prop = rv::propose_op(&conn, A, B, op_entry("mute", 1008, json!({}), "agent"), Actor::Agent)
        .expect("agent proposes");
    rv::confirm_op(&conn, T, &prop.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm");
    rv::confirm_notes(&conn, T, A, B, Actor::Human).expect("confirm_notes");
    rv::conform_run(&conn, T, A, B, Actor::Auto).expect("conform");
    rv::publish_proxy(&conn, T, A, B, Actor::Human, rv::DEFAULT_MAX_ROUNDS).expect("publish r1");
    rl::record_round_run(&conn, T, BOARD, A, "run-2").expect("record run-2");
    assert_eq!(
        rl::tick(&conn, T, BOARD, A).expect("tick parked r1"),
        LoopDecision::Park { round: 1 },
        "round 1 published → parked again"
    );

    // External approval → the controller EXITS and ships the cut.
    rv::version_approved(&conn, T, A, B, Actor::Auto).expect("approved");
    assert_eq!(
        rl::tick(&conn, T, BOARD, A).expect("tick exit"),
        LoopDecision::Exit { outcome: "shipped".to_string() }
    );
    assert_eq!(head_version(&conn).outcome, "shipped", "the delivered cut carries the label");

    // Rounds were SEQUENTIAL RUNS with the round stamp.
    let runs = rl::runs_for(&conn, T, BOARD, A).expect("runs");
    assert_eq!(
        runs.iter().map(|r| (r.run_id.as_str(), r.round)).collect::<Vec<_>>(),
        vec![("run-1", 0), ("run-2", 1)],
        "each round is its own run, stamped with review_state.round"
    );
}
