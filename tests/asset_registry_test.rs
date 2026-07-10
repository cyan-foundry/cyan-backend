//! Asset registry tests (CYAN_FORMAT_SPEC / CYAN_FORMAT_QA — the `asset` table:
//! frame math, derivation edges, remote refs).
//!
//! Same seam as `changelist_test`: every op takes an explicit `&Connection`, so each
//! test runs against its own in-memory SQLite DB — isolated, deterministic, no
//! process-global state. Assertions are on the store's own rows, never on log lines.

use cyan_backend::asset_registry::{self, Asset};
use rusqlite::Connection;
use serde_json::json;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    asset_registry::migrate(&conn).expect("migrate");
    conn
}

fn asset(hash: &str, tenant: &str) -> Asset {
    Asset {
        hash: hash.to_string(),
        tenant_id: tenant.to_string(),
        kind: Some("master".to_string()),
        fps: Some(23.976),
        duration_ms: Some(90_000),
        derived_from_asset: None,
        derived_from_version: None,
        remote_refs: json!({}),
        profile_json: json!({"codec": "prores422"}),
        render_profile: None,
        created_at: 0,
    }
}

// ── upsert → get round-trip ────────────────────────────────────────────────────

#[test]
fn registry_upsert_get_roundtrip() {
    let conn = db();
    let a = asset_registry::upsert(&conn, &asset("h1", "t")).expect("upsert");
    assert!(a.created_at > 0, "created_at is stamped on first registration");

    let got = asset_registry::get(&conn, "t", "h1").expect("get");
    assert_eq!(got.hash, "h1");
    assert_eq!(got.kind.as_deref(), Some("master"));
    assert_eq!(got.fps, Some(23.976), "fps round-trips exactly (frame math depends on it)");
    assert_eq!(got.duration_ms, Some(90_000));
    assert_eq!(got.profile_json, json!({"codec": "prores422"}));

    // Re-upsert refreshes descriptive fields but keeps identity + created_at.
    let mut again = asset("h1", "t");
    again.kind = Some("proxy".to_string());
    again.duration_ms = Some(91_000);
    let a2 = asset_registry::upsert(&conn, &again).expect("re-upsert");
    assert_eq!(a2.kind.as_deref(), Some("proxy"));
    assert_eq!(a2.duration_ms, Some(91_000));
    assert_eq!(a2.created_at, a.created_at, "created_at is set once, never refreshed");

    // Tenant scoping: another tenant cannot read (or discover) the row.
    assert!(asset_registry::get(&conn, "other-tenant", "h1").is_err());
}

// ── remote refs: the forward breadcrumb (e.g. Frame.io file id) ───────────────

#[test]
fn remote_ref_set_and_lookup() {
    let conn = db();
    asset_registry::upsert(&conn, &asset("proxy1", "t")).expect("upsert");

    let a = asset_registry::set_remote_ref(&conn, "t", "proxy1", "frameio", "file_abc123").expect("set frameio ref");
    assert_eq!(a.remote_refs, json!({"frameio": "file_abc123"}));

    // A second system's key merges — it does not clobber the first.
    let a = asset_registry::set_remote_ref(&conn, "t", "proxy1", "resolve", "clip_9").expect("set resolve ref");
    assert_eq!(a.remote_refs.get("frameio").and_then(|v| v.as_str()), Some("file_abc123"));
    assert_eq!(a.remote_refs.get("resolve").and_then(|v| v.as_str()), Some("clip_9"));

    // Re-publishing under the same key overwrites that key only.
    let a = asset_registry::set_remote_ref(&conn, "t", "proxy1", "frameio", "file_def456").expect("overwrite frameio ref");
    assert_eq!(a.remote_refs.get("frameio").and_then(|v| v.as_str()), Some("file_def456"));
    assert_eq!(a.remote_refs.get("resolve").and_then(|v| v.as_str()), Some("clip_9"));

    // Persisted, not just returned: a fresh get sees the same map.
    let got = asset_registry::get(&conn, "t", "proxy1").expect("get");
    assert_eq!(got.remote_refs, json!({"frameio": "file_def456", "resolve": "clip_9"}));

    // A ref on an unregistered asset is an error — no dangling breadcrumbs.
    assert!(asset_registry::set_remote_ref(&conn, "t", "nope", "frameio", "x").is_err());
}

// ── derivation edge: proxy → {parent master, ledger version} ──────────────────

#[test]
fn derivation_edge_recorded() {
    let conn = db();
    asset_registry::upsert(&conn, &asset("master1", "t")).expect("master");
    let mut proxy = asset("proxy1", "t");
    proxy.kind = Some("proxy".to_string());
    asset_registry::upsert(&conn, &proxy).expect("proxy");

    let a = asset_registry::set_derivation(&conn, "t", "proxy1", "master1", "v-final-uuid").expect("set derivation");
    assert_eq!(a.derived_from_asset.as_deref(), Some("master1"));
    assert_eq!(a.derived_from_version.as_deref(), Some("v-final-uuid"));

    // Round-trips through get — the backward walk (comment → proxy → version →
    // master coordinates) reads exactly this edge.
    let got = asset_registry::get(&conn, "t", "proxy1").expect("get");
    assert_eq!(got.derived_from_asset.as_deref(), Some("master1"));
    assert_eq!(got.derived_from_version.as_deref(), Some("v-final-uuid"));

    // The parent master carries no edge (it IS the origin).
    let master = asset_registry::get(&conn, "t", "master1").expect("get master");
    assert_eq!(master.derived_from_asset, None);
    assert_eq!(master.derived_from_version, None);

    // Deriving an unregistered asset is an error.
    assert!(asset_registry::set_derivation(&conn, "t", "ghost", "master1", "v1").is_err());
}

// ── migration robustness: re-running migrate is a clean no-op ──────────────────

#[test]
fn migrate_is_idempotent_and_preserves_rows() {
    let conn = db(); // first migrate ran in db()
    let a = asset_registry::upsert(&conn, &asset("master1", "t")).expect("upsert");

    // A device re-opening its DB runs the migration again — must be a clean no-op.
    asset_registry::migrate(&conn).expect("second migrate");
    asset_registry::migrate(&conn).expect("third migrate");

    let got = asset_registry::get(&conn, "t", "master1").expect("row survives re-migration");
    assert_eq!(got, a);
}
