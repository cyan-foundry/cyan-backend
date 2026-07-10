//! C3 leg 2 — "Produce master": SELECTIVE retrieve-then-conform on a FROZEN
//! version. Leg 1 (`resolve_final_cut_masters`) is proven in asset_class_test;
//! this proves the leg-2 engine: only the USED masters retrieve (by canonical
//! LOCATION), the anchor conforms with the frozen op list (master frames), and
//! the delivery output registers derived-from anchor@version — all with a fake
//! retrieval + fake conform (no network, no ffmpeg).

use std::cell::RefCell;
use std::sync::Mutex;

use cyan_backend::changelist::{self, ChangeEntry};
use cyan_backend::review_loop as rl;
use cyan_backend::{asset_registry, review_state as rv};
use cyan_backend::asset_registry::Asset;
use rusqlite::Connection;
use serde_json::json;

static ENV_MUTEX: Mutex<()> = Mutex::new(());

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    asset_registry::migrate(&conn).expect("migrate asset_registry");
    conn
}

fn master(tenant: &str, hash: &str) -> Asset {
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

fn op_entry(tenant: &str, anchor: &str, op: &str, tc_in: i64, params: serde_json::Value) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: anchor.to_string(),
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

/// A fake conform: records args, writes a real output file under the media
/// root's derived tree, returns its relative output_path.
struct FakeConform {
    root: std::path::PathBuf,
    captured: RefCell<Option<serde_json::Value>>,
}

impl rl::ConformDispatch for FakeConform {
    fn conform(&self, args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        *self.captured.borrow_mut() = Some(args);
        let rel = ".cyan-derived/master/delivery-v1.mov";
        let abs = self.root.join(rel);
        std::fs::create_dir_all(abs.parent().expect("parent"))?;
        std::fs::write(&abs, b"DELIVERY-BYTES")?;
        Ok(json!({ "output_path": rel }))
    }
}

#[test]
fn produce_master_retrieves_used_masters_conforms_anchor_and_registers_delivery() {
    let _g = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let root = tempfile::tempdir().expect("media root");
    unsafe { std::env::set_var("CYAN_MEDIA_ROOT", root.path()) };

    let conn = db();
    let t = "t1";

    // Three masters: the anchor (held LOCALLY already — no retrieval), an
    // inserted daily on S3 (must retrieve), and an unused one (must NOT).
    let anchor_local = root.path().join("anchor.mxf");
    std::fs::write(&anchor_local, b"ANCHOR-BYTES").expect("anchor bytes");
    let mut anchor = master(t, "m-anchor");
    anchor.profile_json = json!({ "path": anchor_local.display().to_string() });
    asset_registry::upsert(&conn, &anchor).expect("anchor");
    asset_registry::set_class_location(
        &conn, t, "m-anchor", Some("clip"),
        Some(&format!("file://{}", anchor_local.display())),
    ).expect("anchor loc");
    for (hash, loc) in [
        ("m-insert", "s3://bucket/dailies/insert.mxf"),
        ("m-unused", "s3://bucket/dailies/unused.mxf"),
    ] {
        asset_registry::upsert(&conn, &master(t, hash)).expect("upsert");
        asset_registry::set_class_location(&conn, t, hash, Some("clip"), Some(loc)).expect("loc");
    }

    // The frozen cut: a trim + an insert(m-insert), both approved, snapshotted.
    for (op, tc, params) in [
        ("trim", 0, json!({"edge": "head", "frames": 12})),
        ("insert", 24, json!({"asset_hash": "m-insert", "at": 24})),
    ] {
        let e = changelist::append(&conn, "m-anchor", "main", op_entry(t, "m-anchor", op, tc, params))
            .expect("append");
        changelist::set_state(&conn, t, &e.id, "approved", Some("rick")).expect("approve");
    }
    let version = changelist::snapshot(&conn, t, "m-anchor", "main").expect("snapshot");

    // Fake retrieval: records locations, writes bytes at the suggested dest.
    let retrieved_locs: RefCell<Vec<String>> = RefCell::new(Vec::new());
    let retrieve = |location: &str, dest: &std::path::Path| -> anyhow::Result<std::path::PathBuf> {
        retrieved_locs.borrow_mut().push(location.to_string());
        std::fs::create_dir_all(dest.parent().expect("parent"))?;
        std::fs::write(dest, b"REMOTE-MASTER-BYTES")?;
        Ok(dest.to_path_buf())
    };
    let fake = FakeConform { root: root.path().to_path_buf(), captured: RefCell::new(None) };

    let out = rl::produce_master(&conn, t, &version.version_id, &retrieve, &fake)
        .expect("produce master");

    // SELECTIVE retrieval: only the S3 insert master was fetched (the anchor
    // was already held; the unused master never moves).
    assert_eq!(
        *retrieved_locs.borrow(),
        vec!["s3://bucket/dailies/insert.mxf".to_string()],
        "retrieve exactly the used, un-held masters"
    );
    let masters = out["masters"].as_array().expect("masters");
    assert_eq!(masters.len(), 2, "plan carries anchor + insert, got {masters:?}");

    // The conform ran on the anchor with the FROZEN two-op plan.
    let args = fake.captured.borrow().clone().expect("conform dispatched");
    assert_eq!(args["ops"].as_array().map(|a| a.len()), Some(2));
    assert_eq!(args["fps"], json!(24.0));
    assert_eq!(out["ops_applied"], json!(2));

    // The delivery output exists and registered derived-from anchor@version.
    let out_path = out["output_path"].as_str().expect("output path");
    assert!(std::path::Path::new(out_path).is_file());
    let delivery_hash = out["delivery_hash"].as_str().expect("delivery hash");
    let delivery = asset_registry::get(&conn, t, delivery_hash).expect("registered");
    assert_eq!(delivery.kind.as_deref(), Some("delivery"));
    assert_eq!(delivery.derived_from_asset.as_deref(), Some("m-anchor"));
    assert_eq!(delivery.derived_from_version.as_deref(), Some(version.version_id.as_str()));

    unsafe { std::env::remove_var("CYAN_MEDIA_ROOT") };
}
