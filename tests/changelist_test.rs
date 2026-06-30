//! ChangeList store tests (CYAN_CHANGEOP_SPEC + CYAN_CHANGELIST_STORE_AND_REVIEW_LOOP §Part 1).
//!
//! The store ops take an explicit `&Connection`, so every test runs against its own
//! in-memory SQLite DB — fully isolated, deterministic, no process-global state, no
//! live deps. Assertions are synchronous on the store's own rows (the oracle), never
//! on log lines. Vocabulary, hashing, and lifecycle are exactly per the locked spec;
//! no assertion is weakened.

use cyan_backend::changelist::{
    self, compute_entry_hash, compute_list_hash, ChangeEntry,
};
use rusqlite::Connection;
use serde_json::json;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate");
    conn
}

/// A minimal op-kind entry. `entry_hash`/`id`/`seq` are filled by `append`.
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
    }
}

// ── round-trip: append → snapshot → get ───────────────────────────────────────

#[test]
fn round_trip_append_snapshot_get() {
    let conn = db();
    let e1 = changelist::append(&conn, "assetA", "main", op_entry("t1", "assetA", "trim", 0, json!({"edge":"head","frames":10})))
        .expect("append e1");
    let e2 = changelist::append(&conn, "assetA", "main", op_entry("t1", "assetA", "fade", 100, json!({"dir":"in","frames":12})))
        .expect("append e2");

    assert_eq!(e1.state, "proposed");
    assert!(e1.active);
    assert!(!e1.entry_hash.is_empty(), "append must content-address the entry");
    assert_eq!(e1.seq, 1);
    assert_eq!(e2.seq, 2, "seq is monotonic within (asset, branch)");

    let v = changelist::snapshot(&conn, "t1", "assetA", "main").expect("snapshot");
    assert_eq!(v.version_no, 1);
    assert_eq!(v.entry_hashes, vec![e1.entry_hash.clone(), e2.entry_hash.clone()]);

    let view = changelist::get(&conn, "t1", "assetA", "main").expect("get");
    assert_eq!(view.entries.len(), 2);
    assert_eq!(view.head_version.as_ref().map(|h| h.version_no), Some(1));
    assert_eq!(
        view.head_version.as_ref().map(|h| h.list_hash.clone()),
        Some(v.list_hash.clone())
    );
}

// ── reverse: set_active(false) excludes the entry from conform_plan ────────────

#[test]
fn reverse_set_active_false_excluded_from_conform_plan() {
    let conn = db();
    let keep = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"tail","frames":5})))
        .expect("keep");
    let reverse = changelist::append(&conn, "a", "main", op_entry("t", "a", "mute", 50, json!({})))
        .expect("reverse");

    // Non-destructive reverse: flip active=false on the second op.
    changelist::set_active(&conn, "t", &reverse.id, false, Some("u-editor")).expect("set_active false");

    let v = changelist::snapshot(&conn, "t", "a", "main").expect("snapshot");
    let plan = changelist::conform_plan(&conn, "t", &v.version_id).expect("conform_plan");

    let ids: Vec<&str> = plan.iter().map(|o| o.entry_id.as_str()).collect();
    assert!(ids.contains(&keep.id.as_str()), "active entry stays in the plan");
    assert!(!ids.contains(&reverse.id.as_str()), "reversed (active=false) entry is excluded");
    assert_eq!(plan.len(), 1);

    // History is preserved — the row still exists via get().
    let view = changelist::get(&conn, "t", "a", "main").expect("get");
    assert_eq!(view.entries.len(), 2, "reverse is non-destructive: the row is kept");
}

// ── redo: supersede chains old → new ──────────────────────────────────────────

#[test]
fn redo_supersede_chain() {
    let conn = db();
    let old = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":10})))
        .expect("old");

    let new = changelist::supersede(
        &conn,
        &old.id,
        op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":20})),
    )
    .expect("supersede");

    assert_eq!(new.supersedes.as_deref(), Some(old.id.as_str()));
    assert!(new.active);

    let view = changelist::get(&conn, "t", "a", "main").expect("get");
    let old_row = view.entries.iter().find(|e| e.id == old.id).expect("old row");
    let new_row = view.entries.iter().find(|e| e.id == new.id).expect("new row");
    assert_eq!(old_row.state, "superseded");
    assert!(!old_row.active, "superseded entry goes inactive");
    assert_eq!(old_row.superseded_by.as_deref(), Some(new.id.as_str()));
    assert!(new_row.active);

    // The conform plan reflects only the new op.
    let v = changelist::snapshot(&conn, "t", "a", "main").expect("snapshot");
    let plan = changelist::conform_plan(&conn, "t", &v.version_id).expect("plan");
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].entry_id, new.id);
    assert_eq!(plan[0].params["frames"], json!(20));
}

// ── diff(vA, vB) → added / removed / superseded ───────────────────────────────

#[test]
fn diff_added_removed_superseded() {
    let conn = db();
    let a = changelist::append(&conn, "asset", "main", op_entry("t", "asset", "trim", 0, json!({"edge":"head","frames":10})))
        .expect("a");
    let b = changelist::append(&conn, "asset", "main", op_entry("t", "asset", "mute", 50, json!({})))
        .expect("b");
    let v1 = changelist::snapshot(&conn, "t", "asset", "main").expect("v1");

    // Reverse `b` (removed), supersede `a` (superseded), and add a fresh `c` (added).
    changelist::set_active(&conn, "t", &b.id, false, None).expect("reverse b");
    let a_prime = changelist::supersede(&conn, &a.id, op_entry("t", "asset", "trim", 0, json!({"edge":"head","frames":30})))
        .expect("supersede a");
    let c = changelist::append(&conn, "asset", "main", op_entry("t", "asset", "fade", 200, json!({"dir":"out","frames":8})))
        .expect("c");
    let v2 = changelist::snapshot(&conn, "t", "asset", "main").expect("v2");

    let d = changelist::diff(&conn, "t", &v1.version_id, &v2.version_id).expect("diff");

    // a_prime + c are new hashes in v2.
    assert!(d.added.contains(&a_prime.entry_hash), "a' is added");
    assert!(d.added.contains(&c.entry_hash), "c is added");
    // a (replaced) and b (reversed) are gone from v2.
    assert!(d.removed.contains(&a.entry_hash), "old a is removed");
    assert!(d.removed.contains(&b.entry_hash), "reversed b is removed");
    // a specifically was superseded (its replacement is present in v2).
    assert!(d.superseded.contains(&a.id), "a is reported as superseded, not merely removed");
    assert!(!d.superseded.contains(&b.id), "b was reversed, not superseded");
}

// ── list_hash is stable / reproducible ────────────────────────────────────────

#[test]
fn list_hash_stable_and_reproducible() {
    // Same asset + same ordered entry hashes ⇒ identical list_hash.
    let h1 = compute_list_hash("asset", &["x".into(), "y".into(), "z".into()]);
    let h2 = compute_list_hash("asset", &["x".into(), "y".into(), "z".into()]);
    assert_eq!(h1, h2, "list_hash is deterministic");

    // Order matters.
    let h3 = compute_list_hash("asset", &["y".into(), "x".into(), "z".into()]);
    assert_ne!(h1, h3, "list_hash depends on entry order");

    // Different asset ⇒ different hash.
    let h4 = compute_list_hash("other", &["x".into(), "y".into(), "z".into()]);
    assert_ne!(h1, h4, "list_hash binds the asset_hash spine");

    // Re-snapshotting an unchanged active set reproduces the same list_hash.
    let conn = db();
    changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":1}))).expect("e");
    let va = changelist::snapshot(&conn, "t", "a", "main").expect("va");
    let vb = changelist::snapshot(&conn, "t", "a", "main").expect("vb");
    assert_eq!(va.list_hash, vb.list_hash, "stable across snapshots of the same active set");
    assert_eq!(vb.version_no, va.version_no + 1, "but each snapshot is a new immutable version");
}

// ── entry_hash is content-addressed and excludes lifecycle ────────────────────

#[test]
fn entry_hash_is_content_only() {
    let conn = db();
    let e = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":7})))
        .expect("e");
    let before = e.entry_hash.clone();

    // A lifecycle transition (approve, reverse) must NOT change the entry's identity.
    changelist::set_state(&conn, "t", &e.id, "approved", Some("u")).expect("approve");
    changelist::set_active(&conn, "t", &e.id, false, None).expect("reverse");

    let view = changelist::get(&conn, "t", "a", "main").expect("get");
    let row = view.entries.iter().find(|x| x.id == e.id).expect("row");
    assert_eq!(row.entry_hash, before, "lifecycle changes never re-hash the entry");
    assert_eq!(row.entry_hash, compute_entry_hash(row), "hash recomputes deterministically from content");
}

// ── branch forks the active set ───────────────────────────────────────────────

#[test]
fn branch_forks_active_set() {
    let conn = db();
    changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":3}))).expect("e1");
    let reversed = changelist::append(&conn, "a", "main", op_entry("t", "a", "mute", 40, json!({}))).expect("e2");
    changelist::set_active(&conn, "t", &reversed.id, false, None).expect("reverse e2");
    // main active set is now just e1.

    let forked = changelist::branch(&conn, "t", "a", "main", "promo-30").expect("branch");
    assert_eq!(forked.len(), 1, "fork copies only the ACTIVE entries of main");

    let promo = changelist::get(&conn, "t", "a", "promo-30").expect("get promo");
    assert_eq!(promo.entries.len(), 1);
    let main = changelist::get(&conn, "t", "a", "main").expect("get main");
    assert_eq!(main.entries.len(), 2, "main is unchanged by the fork");

    // The two branches are independent: snapshot each separately.
    let vp = changelist::snapshot(&conn, "t", "a", "promo-30").expect("snap promo");
    let vm = changelist::snapshot(&conn, "t", "a", "main").expect("snap main");
    assert_eq!(vp.branch, "promo-30");
    assert_eq!(vm.branch, "main");
}

// ── tenant / asset isolation ──────────────────────────────────────────────────

#[test]
fn tenant_and_asset_isolation() {
    let conn = db();
    changelist::append(&conn, "asset1", "main", op_entry("tenantA", "asset1", "trim", 0, json!({"edge":"head","frames":1}))).expect("A1");
    changelist::append(&conn, "asset1", "main", op_entry("tenantB", "asset1", "mute", 0, json!({}))).expect("B1");
    changelist::append(&conn, "asset2", "main", op_entry("tenantA", "asset2", "fade", 0, json!({"dir":"in","frames":2}))).expect("A2");

    // tenantA never sees tenantB's entry on the same asset.
    let a = changelist::get(&conn, "tenantA", "asset1", "main").expect("get A");
    assert_eq!(a.entries.len(), 1);
    assert_eq!(a.entries[0].tenant_id, "tenantA");

    let b = changelist::get(&conn, "tenantB", "asset1", "main").expect("get B");
    assert_eq!(b.entries.len(), 1);
    assert_eq!(b.entries[0].tenant_id, "tenantB");

    // Different asset for the same tenant is a separate list.
    let a2 = changelist::get(&conn, "tenantA", "asset2", "main").expect("get A asset2");
    assert_eq!(a2.entries.len(), 1);
    assert_eq!(a2.entries[0].op.as_deref(), Some("fade"));
}

// ── set_outcome propagates the per-version label to that version's entries ─────

#[test]
fn set_outcome_labels_version_entries() {
    let conn = db();
    let e = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":4}))).expect("e");
    let v = changelist::snapshot(&conn, "t", "a", "main").expect("snapshot");

    changelist::set_outcome(&conn, "t", &v.version_id, "shipped").expect("set_outcome");

    let reloaded = changelist::get_version(&conn, "t", &v.version_id).expect("get_version");
    assert_eq!(reloaded.outcome, "shipped");

    let view = changelist::get(&conn, "t", "a", "main").expect("get");
    let row = view.entries.iter().find(|x| x.id == e.id).expect("row");
    assert_eq!(row.outcome.as_deref(), Some("shipped"), "outcome propagates to entries first appearing in that version");
}

// ── union-merge dedup + op vocab enforcement ──────────────────────────────────

#[test]
fn union_merge_dedups_and_vocab_is_enforced() {
    let conn = db();
    // Identical content twice ⇒ one row (content-addressed union merge — the P2P key).
    let e1 = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":9}))).expect("e1");
    let e2 = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":9}))).expect("e2 (dup)");
    assert_eq!(e1.id, e2.id, "re-appending identical content returns the existing entry, no duplicate");
    let view = changelist::get(&conn, "t", "a", "main").expect("get");
    assert_eq!(view.entries.len(), 1);

    // An op outside the closed vocab is rejected.
    let bad = op_entry("t", "a", "transmogrify", 0, json!({}));
    assert!(changelist::append(&conn, "a", "main", bad).is_err(), "unknown op is rejected, not guessed");
}

// ── notes/markers are excluded from conform_plan ──────────────────────────────

#[test]
fn notes_excluded_from_conform_plan() {
    let conn = db();
    let mut note = op_entry("t", "a", "trim", 0, json!({}));
    note.kind = "note".to_string();
    note.op = None;
    note.intent = "open feels rushed".to_string();
    changelist::append(&conn, "a", "main", note).expect("note");

    let op = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 10, json!({"edge":"head","frames":2}))).expect("op");

    let v = changelist::snapshot(&conn, "t", "a", "main").expect("snapshot");
    let plan = changelist::conform_plan(&conn, "t", &v.version_id).expect("plan");
    assert_eq!(plan.len(), 1, "creative notes are not actionable ops");
    assert_eq!(plan[0].entry_id, op.id);
}
