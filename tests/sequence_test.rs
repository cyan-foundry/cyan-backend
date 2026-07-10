//! C1 — the SEQUENCE asset class producer: a timeline referencing many clips,
//! content-addressed (same ordered clips ⇒ same sequence), clips-only (no
//! ghost references, no sequence-of-sequences).

use cyan_backend::asset_registry::{self, Asset};
use rusqlite::Connection;
use serde_json::json;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    asset_registry::migrate(&conn).expect("migrate asset_registry");
    conn
}

fn clip(tenant: &str, hash: &str) -> Asset {
    Asset {
        hash: hash.to_string(),
        tenant_id: tenant.to_string(),
        kind: Some("master".to_string()),
        fps: Some(24.0),
        duration_ms: None,
        derived_from_asset: None,
        derived_from_version: None,
        remote_refs: json!({}),
        profile_json: json!({}),
        render_profile: None,
        created_at: 0,
    }
}

#[test]
fn sequence_registers_content_addressed_and_validates_clips() {
    let conn = db();
    let t = "t1";
    for h in ["c-1", "c-2"] {
        asset_registry::upsert(&conn, &clip(t, h)).expect("clip");
        asset_registry::set_class_location(&conn, t, h, Some("clip"), None).expect("class");
    }

    let seq = asset_registry::register_sequence(
        &conn, t, "ep101 cut", &["c-1".to_string(), "c-2".to_string()],
    )
    .expect("register");
    assert_eq!(seq.kind.as_deref(), Some("sequence"));
    let (class, _) = asset_registry::class_location(&conn, t, &seq.hash).expect("cl");
    assert_eq!(class.as_deref(), Some("sequence"));
    assert_eq!(seq.profile_json["clips"], json!(["c-1", "c-2"]));
    assert_eq!(seq.profile_json["name"], json!("ep101 cut"));

    // Content identity: the same ordered clips register ONCE; a different
    // order is a DIFFERENT timeline.
    let again = asset_registry::register_sequence(
        &conn, t, "ep101 cut (retry)", &["c-1".to_string(), "c-2".to_string()],
    )
    .expect("re-register");
    assert_eq!(again.hash, seq.hash, "same ordered clips ⇒ same sequence");
    let reversed = asset_registry::register_sequence(
        &conn, t, "reversed", &["c-2".to_string(), "c-1".to_string()],
    )
    .expect("reversed");
    assert_ne!(reversed.hash, seq.hash, "order is part of the timeline's identity");

    // Ghost clips and sequence-of-sequences are rejected, clearly.
    let ghost = asset_registry::register_sequence(&conn, t, "x", &["nope".to_string()]);
    assert!(ghost.expect_err("ghost").to_string().contains("unregistered clip"));
    let nested = asset_registry::register_sequence(&conn, t, "x", std::slice::from_ref(&seq.hash));
    assert!(nested.expect_err("nested").to_string().contains("not other sequences"));
    let empty: Vec<String> = Vec::new();
    assert!(asset_registry::register_sequence(&conn, t, "x", &empty).is_err());
}
