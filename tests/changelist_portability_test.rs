//! WOW-1 portability gate (FABLE_OVERNIGHT_PROMPT): the approved edit cyan-media
//! executes is a TOOL-AGNOSTIC `ChangeEntry`/`ConformOp` — the identical op a
//! future Avid / Resolve / Pro Tools actuator would emit and consume. These tests
//! pin that at the representation level, independent of the Frame.io source:
//!
//!   1. the conform projection is source-blind (frameio vs cyan vs resolve ⇒ the
//!      SAME op),
//!   2. `ConformOp` round-trips through JSON byte-stably (op in → op out — the
//!      wire a DCC actuator reads),
//!   3. a changelist entry replicated to a peer store projects the identical op
//!      (op in → op out across the P2P/superset path),
//!   4. an approved-CREATIVE edit (a human resolves a note into a concrete op and
//!      approves it) conforms exactly like a mechanical one — the gate is "a human
//!      approved a concrete edit", never "Lens proposed it",
//!   5. only approved+active ops ever project (the human gate IS the gate).
//!
//! No Frame.io, no network, no seeds: an in-memory SQLite per test, the changelist
//! store as the oracle.

use cyan_backend::changelist::{self, ChangeEntry, ConformOp};
use rusqlite::Connection;
use serde_json::json;

const T: &str = "tenant-portability";
const ASSET: &str = "blake3-of-master-clip";
const BRANCH: &str = "main";

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    conn
}

/// A ChangeEntry with every non-essential field defaulted; the test sets what it pins.
fn entry(kind: &str, op: Option<&str>, tc_in: i64, tc_out: Option<i64>, params: serde_json::Value) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: ASSET.to_string(),
        tenant_id: T.to_string(),
        branch: None,
        track: Some("V1".to_string()),
        tc_in,
        tc_out,
        kind: kind.to_string(),
        op: op.map(|s| s.to_string()),
        params,
        intent: String::new(),
        source: None,
        source_ref: None,
        author: Some("u1".to_string()),
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

/// Strip per-store identity (entry_id/seq) so two stores' projections compare on
/// the fields a conform/actuator actually executes.
fn op_payload(o: &ConformOp) -> (Option<String>, i64, Option<i64>, String, serde_json::Value) {
    (o.track.clone(), o.tc_in, o.tc_out, o.op.clone(), o.params.clone())
}

#[test]
fn projection_is_source_blind_frameio_vs_dcc() {
    // The SAME edit arriving from three different sensors (Frame.io comment,
    // native Cyan authoring, a Resolve actuator) must project the identical op.
    let mut payloads = Vec::new();
    for source in [Some("frameio"), Some("cyan"), Some("resolve")] {
        let conn = db();
        let mut e = entry("op", Some("trim"), 0, Some(96), json!({"edge":"head","frames":12}));
        e.source = source.map(|s| s.to_string());
        e.source_ref = source.map(|s| format!("{s}-comment-123"));
        let appended = changelist::append(&conn, ASSET, BRANCH, e).expect("append");
        changelist::set_state(&conn, T, &appended.id, "approved", Some("rick")).expect("approve");

        let ops = changelist::approved_ops(&conn, T, ASSET, BRANCH).expect("approved_ops");
        assert_eq!(ops.len(), 1, "exactly one approved op for source {source:?}");
        payloads.push(op_payload(&ops[0]));
    }
    assert_eq!(payloads[0], payloads[1], "frameio vs cyan must project identically");
    assert_eq!(payloads[1], payloads[2], "cyan vs resolve must project identically");
}

#[test]
fn conform_op_round_trips_through_json() {
    // ConformOp is the wire a DCC actuator reads — op in must equal op out.
    let op = ConformOp {
        entry_id: "e-1".to_string(),
        seq: 3,
        track: Some("V1".to_string()),
        tc_in: 24,
        tc_out: Some(36),
        op: "delete".to_string(),
        params: json!({"ripple": true}),
    };
    let wire = serde_json::to_string(&op).expect("serialize");
    let back: ConformOp = serde_json::from_str(&wire).expect("deserialize");
    assert_eq!(op, back, "ConformOp must round-trip byte-stably through JSON");
}

#[test]
fn replicated_entry_projects_identical_op_on_the_peer() {
    // op in → op out across stores: an approved entry replicated to a second
    // peer's DB (apply_entry — the P2P union-merge ingest) projects the SAME op.
    let a = db();
    let e = entry("op", Some("delete"), 24, Some(36), json!({}));
    let appended = changelist::append(&a, ASSET, BRANCH, e).expect("append");
    let approved = changelist::set_state(&a, T, &appended.id, "approved", Some("rick")).expect("approve");

    let b = db();
    changelist::apply_entry(&b, &approved).expect("replicate to peer");

    let ops_a = changelist::approved_ops(&a, T, ASSET, BRANCH).expect("ops A");
    let ops_b = changelist::approved_ops(&b, T, ASSET, BRANCH).expect("ops B");
    assert_eq!(ops_a.len(), 1);
    assert_eq!(ops_b.len(), 1, "the replicated approved op must project on the peer");
    assert_eq!(op_payload(&ops_a[0]), op_payload(&ops_b[0]),
               "identical op on both stores — the changelist is the portable artifact");
    // Content identity survives the hop too (the union-merge/dedup key).
    let ea = changelist::get_entry(&a, T, &ops_a[0].entry_id).expect("entry A");
    let eb = changelist::get_entry(&b, T, &ops_b[0].entry_id).expect("entry B");
    assert_eq!(ea.entry_hash, eb.entry_hash, "entry_hash is store-independent");
}

#[test]
fn approved_creative_note_conforms_exactly_like_a_mechanical_op() {
    let conn = db();

    // A CREATIVE reviewer note (no op — "the open feels rushed").
    let mut note = entry("note", None, 0, Some(48), json!({}));
    note.intent = "the open feels rushed — tighten it".to_string();
    note.source = Some("frameio".to_string());
    note.proposed_by = Some("human".to_string());
    let note = changelist::append(&conn, ASSET, BRANCH, note).expect("append note");

    // A MECHANICAL Lens-proposed op, approved by a human (the baseline lane).
    let mut mech = entry("op", Some("trim"), 0, Some(96), json!({"edge":"head","frames":12}));
    mech.proposed_by = Some("agent".to_string());
    let mech = changelist::append(&conn, ASSET, BRANCH, mech).expect("append mech");
    changelist::set_state(&conn, T, &mech.id, "approved", Some("rick")).expect("approve mech");

    // The human resolves the creative note into a CONCRETE op and approves it.
    let mut resolved = entry("op", Some("trim"), 0, Some(96), json!({"edge":"head","frames":12}));
    resolved.proposed_by = Some("human".to_string());
    resolved.depends_on = Some(note.id.clone());
    resolved.intent = "resolved from creative note: tighten the open".to_string();
    let resolved = changelist::append(&conn, ASSET, BRANCH, resolved).expect("append resolved");
    changelist::set_state(&conn, T, &resolved.id, "approved", Some("rick")).expect("approve resolved");

    let ops = changelist::approved_ops(&conn, T, ASSET, BRANCH).expect("approved_ops");
    // The note itself NEVER conforms; both approved ops do — and identically shaped.
    assert_eq!(ops.len(), 2, "two approved ops (mechanical + resolved-creative), never the note");
    assert!(ops.iter().all(|o| o.op == "trim"));
    assert_eq!(op_payload(&ops[0]).4, op_payload(&ops[1]).4,
               "the resolved-creative op carries the same executable params as the mechanical one");
    assert!(!ops.iter().any(|o| o.entry_id == note.id), "a kind=note entry must not project");
}

#[test]
fn only_approved_and_active_ops_project() {
    let conn = db();

    // proposed (never approved) — must NOT project.
    let proposed = entry("op", Some("mute"), 10, Some(20), json!({}));
    changelist::append(&conn, ASSET, BRANCH, proposed).expect("append proposed");

    // approved then deactivated (non-destructive reverse) — must NOT project.
    let toggled = entry("op", Some("fade"), 0, Some(12), json!({"dir":"in","frames":12}));
    let toggled = changelist::append(&conn, ASSET, BRANCH, toggled).expect("append toggled");
    changelist::set_state(&conn, T, &toggled.id, "approved", Some("rick")).expect("approve");
    changelist::set_active(&conn, T, &toggled.id, false, Some("rick")).expect("deactivate");

    // rejected — must NOT project.
    let rejected = entry("op", Some("level"), 5, Some(15), json!({"gain_db":-3.0}));
    let rejected = changelist::append(&conn, ASSET, BRANCH, rejected).expect("append rejected");
    changelist::set_state(&conn, T, &rejected.id, "rejected", Some("rick")).expect("reject");

    let ops = changelist::approved_ops(&conn, T, ASSET, BRANCH).expect("approved_ops");
    assert!(ops.is_empty(),
            "no proposed/inactive/rejected op may ever reach the conform: got {ops:?}");
}
