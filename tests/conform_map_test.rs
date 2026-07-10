//! Timecode remap tests (CYAN_FORMAT_QA gap 1 — the round-2 mis-pin bug).
//!
//! `conform_map` is pure over a version's ordered ops; the version-backed tests run
//! against an isolated in-memory SQLite DB (the store is the oracle). Non-structural
//! versions map identity; trim/delete/insert offset (lift keeps duration — the
//! renderer blanks in place); speed retimes piecewise;
//! and an observation made in proxy coordinates is stored in MASTER coordinates
//! with the raw `observed` object inside `params` — content-hashed with the entry.

use cyan_backend::changelist::{self, compute_entry_hash, ChangeEntry, ConformOp};
use cyan_backend::conform_map;
use rusqlite::Connection;
use serde_json::json;

const T: &str = "tenantA";
const A: &str = "assetA";
const B: &str = "main";

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    conn
}

fn entry(kind: &str, op: Option<&str>, tc_in: i64, tc_out: Option<i64>, params: serde_json::Value) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: A.to_string(),
        tenant_id: T.to_string(),
        branch: None,
        track: Some("V1".to_string()),
        tc_in,
        tc_out,
        kind: kind.to_string(),
        op: op.map(|s| s.to_string()),
        params,
        intent: format!("{} at {}", op.unwrap_or(kind), tc_in),
        source: Some("frameio".to_string()),
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

fn conform_op(seq: i64, op: &str, tc_in: i64, tc_out: Option<i64>, params: serde_json::Value) -> ConformOp {
    ConformOp {
        entry_id: format!("op-{seq}"),
        seq,
        track: Some("V1".to_string()),
        tc_in,
        tc_out,
        op: op.to_string(),
        params,
    }
}

// ── a version with only non-structural ops maps identity ──────────────────────

#[test]
fn identity_map_on_non_structural_version() {
    let conn = db();
    // level / mute / fade decorate frames in place; none moves a frame.
    for (op, tc_in, tc_out, params) in [
        ("level", 0, 240, json!({"gain_db": -3})),
        ("mute", 100, 140, json!({})),
        ("fade", 200, 240, json!({"dir":"out","frames":12})),
    ] {
        let e = changelist::append(&conn, A, B, entry("op", Some(op), tc_in, Some(tc_out), params))
            .expect("append");
        changelist::set_state(&conn, T, &e.id, "approved", Some("u1")).expect("approve");
    }
    let v = changelist::snapshot(&conn, T, A, B).expect("snapshot v1");

    let map = conform_map::for_version(&conn, T, &v.version_id).expect("map for version");
    assert!(map.is_identity(), "no structural ops ⇒ identity map");
    assert_eq!(map.proxy_to_master(0), Some(0));
    assert_eq!(map.proxy_to_master(150), Some(150));
    assert_eq!(map.master_to_proxy(150), Some(150));
    assert_eq!(map.master_to_proxy(99_999), Some(99_999), "identity is unbounded");
}

// ── the QA-doc removal example: 100 removed frames offset the pin by 100 ───────
//
// 2026-07-08 (WOW-2 verification): this example originally rode a `lift` op, but
// the RENDERER (cyan-media conform) blanks a lifted range IN PLACE — black +
// silence, duration KEPT (true NLE lift; only delete/extract ripples). A map
// that dropped lifted frames disagreed with the rendered pixels by exactly the
// lifted length — the round-2 mis-pin all over again, just one op further down.
// The rippling example is therefore pinned on DELETE (which the renderer really
// removes), and lift is pinned as IDENTITY below. Decision made overnight per
// the renderer's tested behavior; flagged in the run report for Rick.

#[test]
fn delete_offsets_map_correctly() {
    let conn = db();
    // delete master [100, 200) out of the proxy. A reviewer's comment at PROXY
    // frame 150 sits 50 frames past the cut ⇒ MASTER frame 250. Without the remap
    // it would pin to master 150 — inside the removed range: the round-2 mis-pin.
    let e = changelist::append(&conn, A, B, entry("op", Some("delete"), 100, Some(200), json!({})))
        .expect("append delete");
    changelist::set_state(&conn, T, &e.id, "approved", Some("u1")).expect("approve");
    let v = changelist::snapshot(&conn, T, A, B).expect("snapshot");
    let map = conform_map::for_version(&conn, T, &v.version_id).expect("map");

    assert!(!map.is_identity(), "a delete is structural");
    // Before the cut point: untouched.
    assert_eq!(map.proxy_to_master(99), Some(99));
    assert_eq!(map.master_to_proxy(99), Some(99));
    // The example: proxy 150 → master 250, and the inverse walks back.
    assert_eq!(map.proxy_to_master(150), Some(250), "proxy 150 = 50 past the cut ⇒ master 250");
    assert_eq!(map.master_to_proxy(250), Some(150));
    // The deleted master range has NO proxy frame — it is gone from the render.
    assert_eq!(map.master_to_proxy(150), None, "deleted master frames don't exist in the proxy");
    assert_eq!(map.master_to_proxy(100), None);
    assert_eq!(map.master_to_proxy(199), None);
    // Boundary: master 200 is the first surviving frame after the cut.
    assert_eq!(map.master_to_proxy(200), Some(100));
}

#[test]
fn lift_is_identity_because_the_renderer_keeps_duration() {
    let conn = db();
    // lift master [100, 200): the renderer blanks the range in place (black +
    // silence) and KEEPS duration, so frame coordinates do not move — an anchor
    // at master 150 still sits at proxy 150 (on a black frame, which is correct:
    // that IS what the reviewer's frame looks like in this version).
    let e = changelist::append(&conn, A, B, entry("op", Some("lift"), 100, Some(200), json!({})))
        .expect("append lift");
    changelist::set_state(&conn, T, &e.id, "approved", Some("u1")).expect("approve");
    let v = changelist::snapshot(&conn, T, A, B).expect("snapshot");
    let map = conform_map::for_version(&conn, T, &v.version_id).expect("map");

    assert!(map.is_identity(), "lift keeps duration ⇒ identity map (matches the renderer)");
    assert_eq!(map.master_to_proxy(150), Some(150));
    assert_eq!(map.proxy_to_master(150), Some(150));
}

// ── roundtrip master → proxy → master across offsets, inserts and a retime ─────

#[test]
fn roundtrip_master_to_proxy_to_master() {
    // trim 10 head frames off [0,240); delete [100,200); insert 50 foreign frames
    // at master 300; speed 2.0 over [400,500).
    let ops = vec![
        conform_op(1, "trim", 0, Some(240), json!({"edge":"head","frames":10})),
        conform_op(2, "delete", 100, Some(200), json!({})),
        conform_op(3, "insert", 300, None, json!({"asset_hash":"other-asset","at":300,"frames":50})),
        conform_op(4, "speed", 400, Some(500), json!({"ratio":2.0})),
        // Non-structural noise must not disturb the map.
        conform_op(5, "level", 0, Some(600), json!({"target_lufs":-14})),
    ];
    let map = conform_map::build(&ops);

    // Every surviving master frame walks master → proxy → master exactly. Sample
    // the offset-only ranges densely and the retimed range at ratio granularity.
    for m in (10..100).chain(200..300).chain(300..400).chain(500..700) {
        let p = map.master_to_proxy(m).unwrap_or_else(|| panic!("master {m} must survive"));
        assert_eq!(
            map.proxy_to_master(p),
            Some(m),
            "roundtrip master {m} → proxy {p} → master"
        );
    }
    for m in (400..500).step_by(2) {
        let p = map.master_to_proxy(m).unwrap_or_else(|| panic!("retimed master {m} must survive"));
        assert_eq!(map.proxy_to_master(p), Some(m), "retimed roundtrip master {m}");
    }

    // Removed master frames have no proxy coordinate.
    assert_eq!(map.master_to_proxy(5), None, "head-trimmed frame");
    assert_eq!(map.master_to_proxy(150), None, "deleted frame");

    // Spot-check the piecewise arithmetic (proxy layout: [0,90)=master 10..100,
    // [90,190)=master 200..300, [190,240)=insert, [240,340)=master 300..400,
    // [340,390)=master 400..500 at 2x, [390,∞)=master 500..).
    assert_eq!(map.master_to_proxy(250), Some(140));
    assert_eq!(map.proxy_to_master(140), Some(250));
    assert_eq!(map.master_to_proxy(450), Some(365), "2x retime halves proxy footage");
    assert_eq!(map.proxy_to_master(365), Some(450));
    assert_eq!(map.master_to_proxy(500), Some(390), "tail resumes at 1:1 after the retime");

    // Inserted FOREIGN frames have no master coordinates at all.
    assert_eq!(map.proxy_to_master(190), None, "inserted media has no master tc");
    assert_eq!(map.proxy_to_master(239), None);
    assert_eq!(map.proxy_to_master(240), Some(300), "first frame after the insert");
}

// ── sensor observations: master coords stored, `observed` rides in params ──────

#[test]
fn observed_preserved_and_hashed() {
    let conn = db();
    let e = changelist::append(&conn, A, B, entry("op", Some("delete"), 100, Some(200), json!({})))
        .expect("append delete");
    changelist::set_state(&conn, T, &e.id, "approved", Some("u1")).expect("approve");
    let v = changelist::snapshot(&conn, T, A, B).expect("snapshot");
    let map = conform_map::for_version(&conn, T, &v.version_id).expect("map");

    // A Frame.io comment observed at PROXY 150..160 remaps to MASTER 250..260 with
    // the raw observation preserved.
    let (tc_in, tc_out, params) =
        conform_map::remap_observed(&map, "frameio:file_123", 150, Some(160), json!({}))
            .expect("remap observed");
    assert_eq!(tc_in, 250);
    assert_eq!(tc_out, Some(260));
    assert_eq!(
        params["observed"],
        json!({"proxy_ref": "frameio:file_123", "tc_in": 150, "tc_out": 160}),
        "the raw proxy observation rides inside params"
    );

    // Append the entry the way the sensor leg would: MASTER coords + observed params.
    let mut e = entry("note", None, tc_in, tc_out, params.clone());
    e.source_ref = Some("comment-9".to_string());
    e.intent = "tighten this beat".to_string();
    let appended = changelist::append(&conn, A, B, e).expect("append observed note");
    let stored = changelist::get_entry(&conn, T, &appended.id).expect("stored row");
    assert_eq!(stored.tc_in, 250, "stored in MASTER coordinates");
    assert_eq!(stored.tc_out, Some(260));
    assert_eq!(
        stored.params["observed"]["proxy_ref"], json!("frameio:file_123"),
        "observed survives the store roundtrip"
    );
    assert_eq!(stored.params["observed"]["tc_in"], json!(150));

    // `observed` is CONTENT: params are inside entry_hash, so a different observed
    // tc is a different entry identity…
    let mut other = stored.clone();
    other.params["observed"]["tc_in"] = json!(151);
    assert_ne!(
        compute_entry_hash(&stored),
        compute_entry_hash(&other),
        "observed participates in the content hash"
    );
    // …and an identical re-append (a peer replaying the same observation) dedups
    // to the SAME row by that hash.
    let mut replay = entry("note", None, 250, Some(260), params);
    replay.source_ref = Some("comment-9".to_string());
    replay.intent = "tighten this beat".to_string();
    let deduped = changelist::append(&conn, A, B, replay).expect("replay append");
    assert_eq!(deduped.id, appended.id, "identical observation unions to one row");
}
