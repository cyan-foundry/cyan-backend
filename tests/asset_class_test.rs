//! STAGE 4 — asset classes (clip | sequence) + master LOCATION + the selective
//! "produce master" retrieve list (`resolve_final_cut_masters`).
//!
//! Every test runs against its own in-memory SQLite DB (the changelist +
//! asset-registry pattern): explicit `&Connection`, no process-global state.

use cyan_backend::asset_registry::{self, Asset, ASSET_CLASS_VOCAB};
use cyan_backend::changelist::{self, ChangeEntry};
use rusqlite::Connection;
use serde_json::json;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("changelist migrate");
    asset_registry::migrate(&conn).expect("asset registry migrate");
    conn
}

fn master(tenant: &str, hash: &str) -> Asset {
    Asset {
        hash: hash.to_string(),
        tenant_id: tenant.to_string(),
        kind: Some("master".to_string()),
        fps: Some(24.0),
        duration_ms: Some(60_000),
        derived_from_asset: None,
        derived_from_version: None,
        remote_refs: json!({}),
        profile_json: json!({}),
        render_profile: None,
        created_at: 0,
    }
}

/// A minimal op-kind entry (the changelist_test shape).
fn op_entry(tenant: &str, op: &str, tc_in: i64, params: serde_json::Value) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: String::new(),
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

// ── class vocab + location round-trip ─────────────────────────────────────────

#[test]
fn class_is_closed_vocab_and_location_round_trips() {
    let conn = db();
    assert_eq!(ASSET_CLASS_VOCAB, ["clip", "sequence"]);

    asset_registry::upsert(&conn, &master("t1", "m-a")).expect("upsert");

    // Both vocab members are accepted; location rides along.
    asset_registry::set_class_location(&conn, "t1", "m-a", Some("clip"), Some("s3://bucket/dailies/m-a.mxf"))
        .expect("clip + s3 location");
    let (class, location) = asset_registry::class_location(&conn, "t1", "m-a").expect("read back");
    assert_eq!(class.as_deref(), Some("clip"));
    assert_eq!(location.as_deref(), Some("s3://bucket/dailies/m-a.mxf"));

    asset_registry::set_class_location(&conn, "t1", "m-a", Some("sequence"), None)
        .expect("sequence keeps the prior location");
    let (class, location) = asset_registry::class_location(&conn, "t1", "m-a").expect("read back 2");
    assert_eq!(class.as_deref(), Some("sequence"));
    assert_eq!(
        location.as_deref(),
        Some("s3://bucket/dailies/m-a.mxf"),
        "a None location must not clobber the stored one"
    );

    // Outside the closed vocab ⇒ clear error, nothing written.
    let err = asset_registry::set_class_location(&conn, "t1", "m-a", Some("reel"), None)
        .expect_err("'reel' is not an asset class");
    assert!(
        err.to_string().contains("clip") && err.to_string().contains("sequence"),
        "the error must name the closed vocab; got: {err}"
    );

    // An unregistered asset ⇒ clear error (no dangling class/location rows).
    assert!(asset_registry::set_class_location(&conn, "t1", "ghost", Some("clip"), None).is_err());

    // Tenant-scoped: another tenant cannot read or write the row.
    assert!(asset_registry::class_location(&conn, "t2", "m-a").is_err());
}

#[test]
fn migrate_is_idempotent_and_upgrades_a_legacy_table() {
    let conn = db();
    asset_registry::upsert(&conn, &master("t1", "m-keep")).expect("upsert");
    asset_registry::set_class_location(&conn, "t1", "m-keep", Some("clip"), Some("file:///nas/m.mov"))
        .expect("set");

    // Re-running the migration (the every-boot path) must keep data intact.
    asset_registry::migrate(&conn).expect("re-migrate");
    let (class, location) = asset_registry::class_location(&conn, "t1", "m-keep").expect("survives");
    assert_eq!(class.as_deref(), Some("clip"));
    assert_eq!(location.as_deref(), Some("file:///nas/m.mov"));
}

// ── the SELECTIVE retrieve list ───────────────────────────────────────────────

#[test]
fn resolve_final_cut_masters_lists_only_used_masters_with_locations() {
    let conn = db();
    let t = "t1";

    // Four registered masters: the anchor, an inserted daily, a swapped-in
    // replacement — and one UNUSED daily that stays archived.
    for (hash, loc) in [
        ("m-anchor", "file:///nas/masters/anchor.mxf"),
        ("m-insert", "s3://bucket/dailies/insert.mxf"),
        ("m-swap", "file:///nas/masters/swap.mov"),
        ("m-unused", "s3://bucket/dailies/unused.mxf"),
    ] {
        asset_registry::upsert(&conn, &master(t, hash)).expect("upsert");
        asset_registry::set_class_location(&conn, t, hash, Some("clip"), Some(loc)).expect("loc");
    }

    // The cut: trim (no asset params) + insert(m-insert) + swap(m-swap) — all
    // APPROVED: retrieve-then-conform only ever pulls masters the HUMAN-approved
    // final cut uses (conform_plan carries only approved|applied ops).
    for (op, tc, params) in [
        ("trim", 0, json!({"edge": "head", "frames": 12})),
        ("insert", 24, json!({"asset_hash": "m-insert", "at": 24})),
        ("swap", 48, json!({"new_asset_hash": "m-swap"})),
    ] {
        let e = changelist::append(&conn, "m-anchor", "main", op_entry(t, op, tc, params))
            .expect("append");
        changelist::set_state(&conn, t, &e.id, "approved", Some("rick")).expect("approve");
    }
    let version = changelist::snapshot(&conn, t, "m-anchor", "main").expect("snapshot");

    let masters =
        asset_registry::resolve_final_cut_masters(&conn, t, &version.version_id).expect("resolve");
    let hashes: Vec<&str> = masters.iter().map(|(a, _)| a.hash.as_str()).collect();
    assert_eq!(
        hashes,
        vec!["m-anchor", "m-insert", "m-swap"],
        "anchor first, then op-order; the UNUSED registered master is NOT retrieved"
    );
    let locations: Vec<&str> = masters.iter().map(|(_, l)| l.as_str()).collect();
    assert_eq!(
        locations,
        vec![
            "file:///nas/masters/anchor.mxf",
            "s3://bucket/dailies/insert.mxf",
            "file:///nas/masters/swap.mov",
        ]
    );

    // An unknown version errors clearly.
    assert!(asset_registry::resolve_final_cut_masters(&conn, t, "no-such-version").is_err());
}

#[test]
fn resolve_final_cut_masters_errors_on_missing_location_or_registration() {
    let conn = db();
    let t = "t1";

    // A used master REGISTERED but with no location ⇒ clear error naming it.
    asset_registry::upsert(&conn, &master(t, "m-base")).expect("upsert base");
    asset_registry::set_class_location(&conn, t, "m-base", Some("clip"), Some("file:///nas/base.mxf"))
        .expect("base loc");
    asset_registry::upsert(&conn, &master(t, "m-noloc")).expect("upsert noloc");
    let e = changelist::append(&conn, "m-base", "main", op_entry(t, "insert", 10, json!({"asset_hash": "m-noloc", "at": 10})))
        .expect("insert");
    changelist::set_state(&conn, t, &e.id, "approved", Some("rick")).expect("approve");
    let v1 = changelist::snapshot(&conn, t, "m-base", "main").expect("v1");
    let err = asset_registry::resolve_final_cut_masters(&conn, t, &v1.version_id)
        .expect_err("a used master without a location must not silently resolve");
    assert!(
        err.to_string().contains("m-noloc") && err.to_string().contains("location"),
        "error must name the master and the missing location; got: {err}"
    );

    // A used master that is NOT registered at all ⇒ clear error naming it.
    asset_registry::upsert(&conn, &master(t, "m-base2")).expect("upsert base2");
    asset_registry::set_class_location(&conn, t, "m-base2", Some("clip"), Some("file:///nas/base2.mxf"))
        .expect("base2 loc");
    let e2 = changelist::append(&conn, "m-base2", "main", op_entry(t, "swap", 5, json!({"new_asset_hash": "m-ghost"})))
        .expect("swap");
    changelist::set_state(&conn, t, &e2.id, "approved", Some("rick")).expect("approve");
    let v2 = changelist::snapshot(&conn, t, "m-base2", "main").expect("v2");
    let err = asset_registry::resolve_final_cut_masters(&conn, t, &v2.version_id)
        .expect_err("an unregistered used master must not silently resolve");
    assert!(err.to_string().contains("m-ghost"), "error must name the ghost master; got: {err}");
}
