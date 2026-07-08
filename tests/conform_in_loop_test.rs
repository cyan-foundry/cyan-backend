//! The auto-technical-edit loop — CONFIRM → CONFORM → register → round++
//! (FORMAT_SUPERSET_AND_AVID_DESIGN.md Part 7a + 8b).
//!
//! Proves `review_loop::conform_proxy` closes the loop WITHOUT Avid: when the
//! workflow's "apply confirmed mechanical edits and conform proxy" step runs, Cyan
//! itself gathers the APPROVED mechanical ops (active + approved + kind=op, seq
//! order — NEVER the creative notes), dispatches them to the cyan-media `conform`
//! tool (through a FAKE `ConformDispatch` — no ffmpeg, no plugin process), registers
//! the returned proxy as a DERIVED asset (derived_from = master, at the new version),
//! freezes a new ledger Version over the applied ops, surfaces `needs_manual` ops as
//! durable ledger asks (never dropped), and advances the review round so the NEXT
//! SENSE ingest on the new proxy remaps through `conform_map`.
//!
//! Design mirrors the sibling suites: every store/state op takes an explicit
//! `&Connection` on an isolated in-memory DB with all four migrations; assertions
//! are synchronous on the store's own rows (the oracle), never on logs. The fake
//! conform dispatch CAPTURES its args so the test asserts the EXACT cyan-media
//! `conform.in.json` arg shape the engine emits. No existing assertion is weakened.

use cyan_backend::changelist::{self, ChangeEntry};
use cyan_backend::review_loop::{self as rl, ConformDispatch};
use cyan_backend::review_state::{self as rv, Actor, ConfirmDecision};
use cyan_backend::{asset_registry, conform_map};
use rusqlite::Connection;
use serde_json::json;
use std::cell::RefCell;

const T: &str = "tenant-conform";
const MASTER: &str = "master-conform-1";
const B: &str = "main";

/// One in-memory DB with ALL four migrations — the shared oracle.
fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    asset_registry::migrate(&conn).expect("migrate asset_registry");
    rl::migrate(&conn).expect("migrate review_loop");
    conn
}

/// A mechanical op entry on the master. `append`/`propose_op` fill id/hash/seq/state.
fn op_entry(op: &str, tc_in: i64, tc_out: Option<i64>, params: serde_json::Value) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: MASTER.to_string(),
        tenant_id: T.to_string(),
        branch: None,
        track: Some("V1".to_string()),
        tc_in,
        tc_out,
        kind: "op".to_string(),
        op: Some(op.to_string()),
        params,
        intent: format!("{op} at {tc_in}"),
        source: Some("frameio".to_string()),
        source_ref: None,
        author: Some("editor-1".to_string()),
        role: Some("editor".to_string()),
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
    }
}

/// A creative note (kind=note, no op) — must NEVER be conformed.
fn note_entry(intent: &str, tc_in: i64) -> ChangeEntry {
    let mut e = op_entry("trim", tc_in, Some(tc_in + 24), json!({}));
    e.kind = "note".to_string();
    e.op = None;
    e.intent = intent.to_string();
    e
}

/// A FAKE cyan-media `conform` dispatch: captures the args it was called with and
/// returns a scripted `conform.out.json` result. No ffmpeg, no plugin process.
struct FakeConform {
    /// The args the engine passed (the exact `conform.in.json` shape).
    captured: RefCell<Option<serde_json::Value>>,
    /// The output_path the "render" produced.
    output_path: String,
    /// needs_manual ops the engine "couldn't apply" (surfaced, never dropped).
    needs_manual: Vec<serde_json::Value>,
}

impl FakeConform {
    fn new(output_path: &str) -> Self {
        Self {
            captured: RefCell::new(None),
            output_path: output_path.to_string(),
            needs_manual: Vec::new(),
        }
    }
    fn with_needs_manual(output_path: &str, needs_manual: Vec<serde_json::Value>) -> Self {
        Self {
            captured: RefCell::new(None),
            output_path: output_path.to_string(),
            needs_manual,
        }
    }
    fn captured(&self) -> serde_json::Value {
        self.captured.borrow().clone().expect("conform was dispatched")
    }
}

impl ConformDispatch for FakeConform {
    fn conform(&self, args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        // Echo the sent ops back as `applied` (minus any needs_manual op names).
        let manual_names: Vec<String> = self
            .needs_manual
            .iter()
            .filter_map(|m| m.get("op").and_then(|o| o.as_str()).map(str::to_string))
            .collect();
        let applied: Vec<serde_json::Value> = args
            .get("ops")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter(|o| {
                        !manual_names.contains(
                            &o.get("op").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                        )
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        *self.captured.borrow_mut() = Some(args);
        Ok(json!({
            "output_path": self.output_path,
            "applied": applied,
            "needs_manual": self.needs_manual,
            "size_bytes": 123_456,
        }))
    }
}

/// Register the master, seed + confirm ops, publish v1 (IN_REVIEW), register the
/// round-1 proxy under `frameio ref`. Returns the v1 version id.
fn seed_published_round1(conn: &Connection, proxy_hash: &str, proxy_ref: &str, fps: f64) -> String {
    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: MASTER.to_string(),
            tenant_id: T.to_string(),
            kind: Some("master".to_string()),
            fps: Some(fps),
            duration_ms: Some(60_000),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: None,
            created_at: 0,
        },
    )
    .expect("register master");
    rv::start_draft(conn, T, MASTER, B).expect("start_draft");
    rv::publish_draft(conn, T, MASTER, B, Actor::Human).expect("publish v1");
    let v1 = changelist::get(conn, T, MASTER, B)
        .expect("get")
        .head_version
        .expect("v1 exists");
    register_proxy(conn, proxy_hash, &v1.version_id, proxy_ref, fps);
    v1.version_id
}

/// Register a proxy rendered from `version_id`, published as `frameio_ref`.
fn register_proxy(conn: &Connection, proxy_hash: &str, version_id: &str, frameio_ref: &str, fps: f64) {
    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: proxy_hash.to_string(),
            tenant_id: T.to_string(),
            kind: Some("proxy".to_string()),
            fps: Some(fps),
            duration_ms: None,
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: Some("proxy-540p".to_string()),
            created_at: 0,
        },
    )
    .expect("register proxy");
    asset_registry::set_derivation(conn, T, proxy_hash, MASTER, version_id).expect("derivation");
    asset_registry::set_remote_ref(conn, T, proxy_hash, "frameio", frameio_ref).expect("remote ref");
}

/// Bring the machine to CONFORMING with two APPROVED ops (a delete and a mute) plus a
/// kept creative NOTE, ready for `conform_proxy`. Returns (delete_id, mute_id, note_id).
/// (Was a lift; 2026-07-08 WOW verification aligned the map with the renderer —
/// lift blanks IN PLACE (identity map), delete is the op that ripples frames.)
fn arrive_notes_confirm_two_ops(conn: &Connection) -> (String, String, String) {
    rv::notes_arrived(conn, T, MASTER, B, Actor::Auto).expect("notes_arrived");

    // Agent proposes a structural op (delete) and an audio op (mute).
    let prop_lift = rv::propose_op(
        conn,
        MASTER,
        B,
        op_entry("delete", 48, Some(72), json!({})),
        Actor::Agent,
    )
    .expect("propose delete");
    let prop_mute = rv::propose_op(
        conn,
        MASTER,
        B,
        op_entry("mute", 200, Some(224), json!({})),
        Actor::Agent,
    )
    .expect("propose mute");

    // A creative note — the human keeps it (never promoted → never conformed).
    let note = changelist::append(conn, MASTER, B, note_entry("the open feels rushed", 0))
        .expect("append note");
    rv::escalate_note(conn, T, &note.id, None, Actor::Human).expect("keep note");

    // Human confirms BOTH ops, then batch-confirms → CONFORMING.
    let lift = rv::confirm_op(conn, T, &prop_lift.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm delete");
    let mute = rv::confirm_op(conn, T, &prop_mute.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm mute");
    rv::confirm_notes(conn, T, MASTER, B, Actor::Human).expect("confirm_notes");
    assert_eq!(
        rv::get(conn, T, MASTER, B).expect("get review_state").expect("review_state exists").state,
        "CONFORMING",
        "confirm_notes advanced to CONFORMING"
    );
    (lift.id, mute.id, note.id)
}

// ============================================================================
// 1. THE CLOSED LOOP: approved ops (seq order, no notes) → conform → register →
//    Version → round++. Asserts the EXACT cyan-media arg shape.
// ============================================================================

#[test]
fn conform_applies_approved_ops_registers_proxy_and_advances_round() {
    let conn = db();
    let v1 = seed_published_round1(&conn, "proxy-r1", "file_r1", 24.0);
    let (lift_id, mute_id, note_id) = arrive_notes_confirm_two_ops(&conn);

    let fake = FakeConform::new("/media/derived/conform-abc123.mp4");
    let outcome = rl::conform_proxy(&conn, T, "file_r1", None, &fake).expect("conform_proxy");

    // ── (b) the CONFIRMED mechanical edits, in seq order — NOT the creative note ──
    assert_eq!(
        outcome.sent_ops.iter().map(|o| o.op.as_str()).collect::<Vec<_>>(),
        vec!["delete", "mute"],
        "exactly the two approved ops, in seq order"
    );
    assert_eq!(outcome.sent_ops[0].entry_id, lift_id);
    assert_eq!(outcome.sent_ops[1].entry_id, mute_id);
    assert!(
        !outcome.sent_ops.iter().any(|o| o.entry_id == note_id),
        "the creative note is NEVER in the conform set"
    );

    // ── (c) the EXACT cyan-media `conform.in.json` arg shape the engine emitted ──
    let args = fake.captured();
    assert_eq!(args["input"], json!("file_r1"), "input carries the proxy the ops apply to");
    assert_eq!(args["fps"], json!(24.0), "fps = the master's frame denominator");
    let ops = args["ops"].as_array().expect("ops is an array");
    assert_eq!(ops.len(), 2, "only the two approved ops are sent — never the note");
    // Each op carries exactly {op, tc_in, tc_out, params} (the cyan-media item shape).
    assert_eq!(ops[0]["op"], json!("delete"));
    assert_eq!(ops[0]["tc_in"], json!(48));
    assert_eq!(ops[0]["tc_out"], json!(72));
    assert_eq!(ops[0]["params"], json!({}));
    assert_eq!(ops[1]["op"], json!("mute"));
    assert_eq!(ops[1]["tc_in"], json!(200));
    assert_eq!(ops[1]["tc_out"], json!(224));
    // The creative note's text never appears anywhere in the args.
    assert!(
        !args.to_string().contains("the open feels rushed"),
        "the creative note is not passed to the conform tool"
    );

    // ── (d) the new proxy registered as a DERIVED asset (derived_from = master) ──
    let new_proxy = asset_registry::get(&conn, T, &outcome.new_proxy_hash).expect("new proxy asset");
    assert_eq!(new_proxy.kind.as_deref(), Some("proxy"));
    assert_eq!(
        new_proxy.derived_from_asset.as_deref(),
        Some(MASTER),
        "the new proxy is derived FROM the master"
    );
    assert_eq!(
        new_proxy.derived_from_version.as_deref(),
        Some(outcome.new_version_id.as_str()),
        "derived at the NEW conform version"
    );
    assert_eq!(
        new_proxy.profile_json["output_path"], json!("/media/derived/conform-abc123.mp4"),
        "the tool's output_path is recorded on the derived asset"
    );

    // ── (d) a NEW ledger Version was recorded over the now-applied ops ──
    assert_ne!(outcome.new_version_id, v1, "conform froze a new version");
    let new_version = changelist::get_version(&conn, T, &outcome.new_version_id).expect("new version");
    assert_eq!(new_version.version_no, 2, "conform_run minted v2");
    // The new version's conform plan is exactly the two applied ops (in seq order).
    let plan = changelist::conform_plan(&conn, T, &new_version.version_id).expect("plan");
    assert_eq!(
        plan.iter().map(|o| o.op.as_str()).collect::<Vec<_>>(),
        vec!["delete", "mute"],
        "the new version freezes the applied ops"
    );

    // ── nothing escalated (both ops applied) ──
    assert!(outcome.needs_manual.is_empty(), "both ops applied — nothing escalated");
    assert!(outcome.escalated_asks.is_empty());

    // ── (e) the round advanced: the machine is CONFORMING (round unchanged until publish) ──
    assert_eq!(
        outcome.state.as_ref().expect("state").state,
        "CONFORMING",
        "conform_proxy fired the AUTO conform_run advance"
    );
}

// ============================================================================
// 2. needs_manual ops are SURFACED as durable ledger asks — never silently dropped.
// ============================================================================

#[test]
fn conform_surfaces_needs_manual_as_ledger_asks() {
    let conn = db();
    seed_published_round1(&conn, "proxy-r1", "file_r1", 24.0);
    arrive_notes_confirm_two_ops(&conn);

    // The engine reports it could NOT apply `mute` (say, a codec the pass can't touch).
    let fake = FakeConform::with_needs_manual(
        "/media/derived/conform-def456.mp4",
        vec![json!({ "op": "mute", "reason": "unsupported audio codec" })],
    );
    let outcome = rl::conform_proxy(&conn, T, "file_r1", None, &fake).expect("conform_proxy");

    // Both ops were still SENT (the engine decides what it can apply), and the mute
    // came back as needs_manual — surfaced, not dropped.
    assert_eq!(outcome.sent_ops.len(), 2, "both approved ops are sent to the engine");
    assert_eq!(outcome.needs_manual.len(), 1, "one op escalated");
    assert_eq!(outcome.needs_manual[0].op, "mute");
    assert_eq!(outcome.needs_manual[0].reason, "unsupported audio codec");

    // A DURABLE ledger ask exists for it (source=cyan, kind=note) — the human's cue.
    assert_eq!(outcome.escalated_asks.len(), 1, "one durable ask minted");
    let ask = changelist::get_entry(&conn, T, &outcome.escalated_asks[0]).expect("ask entry");
    assert_eq!(ask.kind, "note");
    assert_eq!(ask.source.as_deref(), Some("cyan"));
    assert_eq!(ask.params["ask"], json!("conform_needs_manual"));
    assert_eq!(ask.params["op"], json!("mute"));
    assert_eq!(ask.params["reason"], json!("unsupported audio codec"));
    assert!(ask.intent.contains("human"), "the ask asks for a human");

    // The ask dedups by content: a hypothetical re-surface of the same op+reason is
    // one row (content-addressed append). Assert exactly one such ask on the ledger.
    let asks = changelist::get(&conn, T, MASTER, B)
        .expect("get")
        .entries
        .iter()
        .filter(|e| e.params.get("ask") == Some(&json!("conform_needs_manual")))
        .count();
    assert_eq!(asks, 1, "exactly one needs_manual ask on the ledger — never dropped, never spammed");
}

// ============================================================================
// 3. ROUND-2 SENSE on the NEW proxy remaps through conform_map — the new version's
//    structural ops shift the tc (the whole point of advancing the round).
// ============================================================================

#[test]
fn round2_sense_on_new_proxy_remaps_through_conform_map() {
    let conn = db();
    // Round 1 identity map (no structural op yet); publish, register proxy r1.
    seed_published_round1(&conn, "proxy-r1", "file_r1", 24.0);
    // Confirm a STRUCTURAL delete ([48,72) — 24 frames gone) + a mute, then conform.
    arrive_notes_confirm_two_ops(&conn);

    let fake = FakeConform::new("/media/derived/conform-r2.mp4");
    let outcome = rl::conform_proxy(&conn, T, "file_r1", None, &fake).expect("conform_proxy");

    // The new proxy's derived_from_version is the version the NEXT SENSE remaps through.
    let new_version_id = outcome.new_version_id.clone();
    // Its conform_map is NOT identity (the delete is structural).
    let map = conform_map::for_version(&conn, T, &new_version_id).expect("map for new version");
    assert!(!map.is_identity(), "the new version has a structural op → a real remap");
    // A comment at PROXY frame 100 sits past the 24-frame cut ⇒ MASTER 124.
    assert_eq!(map.proxy_to_master(100), Some(124), "proxy 100 remaps past the cut to master 124");

    // Publish the new proxy (human) and stamp its frameio ref — the actuator's
    // breadcrumb the next SENSE walks. (In prod this is the FOLLOWING @frameio.upload
    // step; here we do it directly to exercise the round-2 SENSE ingest.)
    rv::publish_proxy(&conn, T, MASTER, B, Actor::Human, rv::DEFAULT_MAX_ROUNDS).expect("publish r2");
    asset_registry::set_remote_ref(&conn, T, &outcome.new_proxy_hash, "frameio", "file_r2")
        .expect("stamp new proxy ref");

    // A round-2 producer comment at proxy frame 100 → ingested against the NEW proxy.
    let r2 = json!({ "data": [ { "id": "c-r2-1", "text": "swap the sting here", "frame": 100 } ] });
    let ingest = rl::ingest_sense_result(&conn, T, "file_r2", &r2).expect("ingest r2");
    assert_eq!(ingest.appended.len(), 1, "the round-2 note landed");
    let note = &ingest.appended[0];
    assert_eq!(
        note.tc_in, 124,
        "round-2 SENSE remaps proxy 100 through the NEW version's conform_map to master 124"
    );
    assert_eq!(note.params["observed"]["proxy_ref"], json!("file_r2"));
    assert_eq!(note.params["observed"]["tc_in"], json!(100), "raw proxy observation preserved");
}

// ============================================================================
// 4. GUARD: the machine must be CONFORMING (confirm already fired). An un-confirmed
//    round (still IN_REVIEW / NOTES_IN) does NOT silently conform — conform_run
//    rejects the invalid transition, so no rogue proxy / version is produced.
// ============================================================================

#[test]
fn conform_requires_confirmed_state_no_rogue_render() {
    let conn = db();
    seed_published_round1(&conn, "proxy-r1", "file_r1", 24.0);
    // Notes arrived + one op proposed, but the human has NOT confirmed_notes yet:
    // the machine is NOTES_IN, not CONFORMING.
    rv::notes_arrived(&conn, T, MASTER, B, Actor::Auto).expect("notes_arrived");
    rv::propose_op(&conn, MASTER, B, op_entry("mute", 200, Some(224), json!({})), Actor::Agent)
        .expect("propose");

    let versions_before = changelist::list_versions_by_tenant(&conn, T).expect("versions").len();
    let fake = FakeConform::new("/media/derived/rogue.mp4");
    let err = rl::conform_proxy(&conn, T, "file_r1", None, &fake).unwrap_err();
    assert!(
        err.to_string().contains("conform_run"),
        "an un-confirmed round is rejected at conform_run, got: {err}"
    );
    let versions_after = changelist::list_versions_by_tenant(&conn, T).expect("versions").len();
    assert_eq!(versions_before, versions_after, "no rogue version was frozen");
    // And no derived proxy row was registered for the (never produced) output.
    let rogue_hash = cyan_backend::changelist::compute_list_hash("x", &[]); // any non-real hash
    assert!(
        asset_registry::get(&conn, T, &rogue_hash).is_err(),
        "no proxy registered from a rejected conform"
    );
}

// ============================================================================
// 5. THE RUN-LOOP WIRE: `current_proxy_ref(board)` resolves the CURRENT round's
//    published proxy from board state — the fallback the pipeline conform step uses
//    when no explicit proxy_ref is threaded in. It follows board → active loop →
//    master → newest published (frameio-ref'd) proxy, and skips an unpublished
//    (freshly-conformed) proxy so it returns the last cut actually sent for review.
// ============================================================================

#[test]
fn current_proxy_ref_resolves_board_to_published_proxy() {
    let conn = db();
    let board = "board-review-1";
    seed_published_round1(&conn, "proxy-r1", "file_r1", 24.0);
    // Link the board to the master (what the workflow does when the loop registers).
    rl::register(&conn, T, board, MASTER, B, 10).expect("register loop");

    // The board resolves to the round-1 published proxy's Frame.io ref.
    let got = rl::current_proxy_ref(&conn, T, board).expect("resolve");
    assert_eq!(got.as_deref(), Some("file_r1"));

    // An unknown board resolves to nothing (no accidental cross-board conform).
    assert_eq!(
        rl::current_proxy_ref(&conn, T, "no-such-board").expect("resolve"),
        None
    );

    // Register a NEWER published proxy (round 2) → it becomes the current one.
    // `upsert` stamps `now()` when created_at==0, so pin both explicitly to make the
    // ordering deterministic (r2 strictly newer than r1).
    register_proxy(&conn, "proxy-r2", "v-later", "file_r2", 24.0);
    conn.execute("UPDATE asset SET created_at=10 WHERE hash='proxy-r1'", [])
        .expect("pin r1");
    conn.execute("UPDATE asset SET created_at=20 WHERE hash='proxy-r2'", [])
        .expect("pin r2");
    assert_eq!(
        rl::current_proxy_ref(&conn, T, board).expect("resolve").as_deref(),
        Some("file_r2"),
        "the newest PUBLISHED proxy is the current round's proxy"
    );

    // A derived proxy with NO frameio ref (a conformed-but-unpublished cut) must NOT
    // shadow the last published one.
    asset_registry::upsert(
        &conn,
        &asset_registry::Asset {
            hash: "proxy-unpublished".to_string(),
            tenant_id: T.to_string(),
            kind: Some("proxy".to_string()),
            fps: Some(24.0),
            duration_ms: None,
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: Some("proxy-540p".to_string()),
            created_at: 999,
        },
    )
    .expect("register unpublished proxy");
    asset_registry::set_derivation(&conn, T, "proxy-unpublished", MASTER, "v-later")
        .expect("derivation");
    assert_eq!(
        rl::current_proxy_ref(&conn, T, board).expect("resolve").as_deref(),
        Some("file_r2"),
        "an unpublished (no frameio ref) proxy never shadows the last published one"
    );
}
