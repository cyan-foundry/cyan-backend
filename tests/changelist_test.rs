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
        updated_at: 0,
        updated_by: None,
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

// ── cut_hash: picture identity covers ONLY the active ops (list/cut split) ─────

#[test]
fn snapshot_computes_cut_hash_over_ops_only() {
    let conn = db();
    let op = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":6}))).expect("op");
    let mut note = op_entry("t", "a", "trim", 24, json!({}));
    note.kind = "note".to_string();
    note.op = None;
    note.intent = "first note".to_string();
    changelist::append(&conn, "a", "main", note).expect("note");

    let v1 = changelist::snapshot(&conn, "t", "a", "main").expect("v1");
    assert_eq!(
        v1.cut_hash,
        changelist::compute_cut_hash("a", std::slice::from_ref(&op.entry_hash)),
        "cut_hash covers only the ordered active OP hashes"
    );
    assert_ne!(v1.cut_hash, v1.list_hash, "with a note in the set, picture identity != full frozen-list identity");

    // A second note changes the list (new comments) but never the picture.
    let mut note2 = op_entry("t", "a", "trim", 48, json!({}));
    note2.kind = "note".to_string();
    note2.op = None;
    note2.intent = "second note".to_string();
    changelist::append(&conn, "a", "main", note2).expect("note2");

    let v2 = changelist::snapshot(&conn, "t", "a", "main").expect("v2");
    assert_eq!(v2.cut_hash, v1.cut_hash, "a new note leaves the cut untouched — the previous render is reusable");
    assert_ne!(v2.list_hash, v1.list_hash, "but the frozen list did change");

    // Round-trips through the store.
    let reloaded = changelist::get_version(&conn, "t", &v2.version_id).expect("get_version");
    assert_eq!(reloaded.cut_hash, v2.cut_hash);
}

// ── lifecycle transitions bump the LWW clocks (spec §6.1 delta 1) ──────────────

#[test]
fn lifecycle_transitions_bump_updated_at() {
    let conn = db();
    let updated_at = |id: &str| -> i64 {
        conn.query_row("SELECT updated_at FROM change_entry WHERE id=?1", [id], |r| r.get(0))
            .expect("updated_at")
    };
    let reset = |id: &str| {
        conn.execute("UPDATE change_entry SET updated_at=0 WHERE id=?1", [id])
            .expect("reset clock");
    };

    let e = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":6}))).expect("e");
    assert_eq!(updated_at(&e.id), 0, "append is content, not lifecycle — the clock starts at 0");

    changelist::set_state(&conn, "t", &e.id, "approved", Some("u-prod")).expect("set_state");
    assert!(updated_at(&e.id) > 0, "set_state bumps the lifecycle clock");

    reset(&e.id);
    changelist::set_active(&conn, "t", &e.id, false, None).expect("set_active");
    assert!(updated_at(&e.id) > 0, "set_active bumps the lifecycle clock");

    reset(&e.id);
    let e2 = changelist::supersede(&conn, &e.id, op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":9}))).expect("supersede");
    assert!(updated_at(&e.id) > 0, "supersede bumps the OLD entry's clock");

    // Outcome propagation bumps the entries first appearing in the version.
    let v = changelist::snapshot(&conn, "t", "a", "main").expect("snapshot");
    reset(&e2.id);
    changelist::set_outcome(&conn, "t", &v.version_id, "shipped").expect("set_outcome");
    assert!(updated_at(&e2.id) > 0, "outcome propagation bumps the labeled entries");

    // The head advancing (snapshot) bumps the branch clock.
    let branch_clock: i64 = conn
        .query_row(
            "SELECT updated_at FROM change_branch WHERE tenant_id='t' AND asset_hash='a' AND branch='main'",
            [],
            |r| r.get(0),
        )
        .expect("branch clock");
    assert!(branch_clock > 0, "advancing the branch head bumps change_branch.updated_at");
}

// ── audit rows are content-addressed and union-merge (spec §6.1 delta 2) ───────

#[test]
fn audit_rows_are_content_addressed_and_dedup() {
    let conn = db();
    let e = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"tail","frames":3}))).expect("e");

    // The append audit row carries a TEXT uuid id + an audit_hash that recomputes
    // exactly from its own fields — provenance is global, not a local counter.
    let (id, actor, ts, audit_hash): (String, Option<String>, i64, Option<String>) = conn
        .query_row(
            "SELECT id, actor, ts, audit_hash FROM change_audit \
             WHERE tenant_id='t' AND entry_id=?1 AND transition='append'",
            [&e.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("append audit row");
    assert_eq!(id.len(), 36, "audit id is a uuid, not an AUTOINCREMENT int");
    let expected = changelist::compute_audit_hash(&e.id, "append", actor.as_deref(), ts, None);
    assert_eq!(audit_hash.as_deref(), Some(expected.as_str()), "audit rows are content-addressed");

    // Union-merge: the same audit row arriving again (a peer's copy — same content
    // hash, different local uuid) is a no-op under the unique (tenant_id, audit_hash)
    // index + INSERT OR IGNORE.
    let before: i64 = conn
        .query_row("SELECT COUNT(*) FROM change_audit WHERE tenant_id='t'", [], |r| r.get(0))
        .expect("count before");
    conn.execute(
        "INSERT OR IGNORE INTO change_audit \
            (id, entry_id, tenant_id, transition, actor, ts, detail, audit_hash) \
         VALUES ('peer-replayed-uuid', ?1, 't', 'append', ?2, ?3, NULL, ?4)",
        rusqlite::params![e.id, actor, ts, expected],
    )
    .expect("replay insert");
    let after: i64 = conn
        .query_row("SELECT COUNT(*) FROM change_audit WHERE tenant_id='t'", [], |r| r.get(0))
        .expect("count after");
    assert_eq!(before, after, "identical provenance unions to one row");

    // A different transition is new content — a new row, no false dedup.
    changelist::set_state(&conn, "t", &e.id, "approved", Some("u-prod")).expect("approve");
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM change_audit WHERE tenant_id='t' AND entry_id=?1",
            [&e.id],
            |r| r.get(0),
        )
        .expect("n");
    assert_eq!(n, 2, "distinct transitions produce distinct audit rows");
}

// ── branch_from_version forks the FROZEN set, not the moved-on head ────────────

#[test]
fn branch_from_version_forks_frozen_set() {
    let conn = db();
    let e1 = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":5}))).expect("e1");
    let e2 = changelist::append(&conn, "a", "main", op_entry("t", "a", "lift", 100, json!({}))).expect("e2");
    let v1 = changelist::snapshot(&conn, "t", "a", "main").expect("v1");

    // The head moves on: e2 is superseded and a new version is cut.
    let e2b = changelist::supersede(&conn, &e2.id, op_entry("t", "a", "delete", 100, json!({}))).expect("e2'");
    let v2 = changelist::snapshot(&conn, "t", "a", "main").expect("v2");
    assert!(v2.entry_hashes.contains(&e2b.entry_hash), "sanity: v2 carries the replacement");
    assert!(!v2.entry_hashes.contains(&e2.entry_hash), "sanity: v2 dropped the superseded op");

    // Fork from v1 — the fork's starting active set is v1's frozen set.
    let forked = changelist::branch_from_version(&conn, "t", "a", &v1.version_id, "alt").expect("fork");
    assert_eq!(forked.len(), 2, "both frozen entries restored on the fork");

    let v_alt = changelist::snapshot(&conn, "t", "a", "alt").expect("v_alt");
    assert_eq!(v_alt.entry_hashes.len(), 2);

    let view = changelist::get(&conn, "t", "a", "alt").expect("get alt");
    let active_ops: Vec<(String, i64)> = view
        .entries
        .iter()
        .filter(|x| x.active)
        .map(|x| (x.op.clone().unwrap_or_default(), x.tc_in))
        .collect();
    assert!(active_ops.contains(&("trim".to_string(), 0)), "v1's e1 is on the fork");
    assert!(
        active_ops.contains(&("lift".to_string(), 100)),
        "the fork restores v1's e2 — the entry the head later superseded"
    );
    assert!(
        !active_ops.contains(&("delete".to_string(), 100)),
        "v2's superseding entry is NOT part of the v1 fork"
    );
    // Sanity vs e1 content: the same edit re-hashes under the new branch (branch IS
    // identity), so the fork's frozen hashes are all distinct from v1's.
    assert_eq!(e1.op.as_deref(), Some("trim"));
    for h in &v_alt.entry_hashes {
        assert!(!v1.entry_hashes.contains(h), "forked entries re-hash under the new branch");
    }
}

// ── migration robustness: idempotence + legacy audit-table rebuild ─────────────

#[test]
fn migrate_is_idempotent_and_preserves_data() {
    let conn = db(); // first migrate ran in db()
    let e = changelist::append(&conn, "a", "main", op_entry("t", "a", "trim", 0, json!({"edge":"head","frames":2})))
        .expect("append");
    let v = changelist::snapshot(&conn, "t", "a", "main").expect("snapshot");

    // A device re-opening its DB runs the migration again — must be a clean no-op.
    changelist::migrate(&conn).expect("second migrate");
    changelist::migrate(&conn).expect("third migrate");

    let reread = changelist::get_entry(&conn, "t", &e.id).expect("entry survives re-migration");
    assert_eq!(reread.entry_hash, e.entry_hash);
    let ver = changelist::get_version(&conn, "t", &v.version_id).expect("version survives");
    assert_eq!(ver.entry_hashes, v.entry_hashes);
    let audits: i64 = conn
        .query_row("SELECT COUNT(*) FROM change_audit WHERE tenant_id='t'", [], |r| r.get(0))
        .expect("audit count");
    assert!(audits >= 1, "audit rows survive re-migration");
}

#[test]
fn migrate_rebuilds_legacy_integer_audit_table() {
    // Pre-seed the PREVIOUS build's audit shape: INTEGER AUTOINCREMENT ids, no
    // audit_hash (CYAN_FORMAT_SPEC §6.1 delta 2 — local-only ids that cannot union).
    let conn = Connection::open_in_memory().expect("in-memory db");
    conn.execute_batch(
        r#"
        CREATE TABLE change_audit (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id    TEXT NOT NULL,
            tenant_id   TEXT NOT NULL,
            transition  TEXT NOT NULL,
            actor       TEXT,
            ts          INTEGER NOT NULL,
            detail      TEXT
        );
        INSERT INTO change_audit (entry_id, tenant_id, transition, actor, ts, detail)
            VALUES ('e-old-1', 't', 'append', 'u1', 100, NULL),
                   ('e-old-1', 't', 'set_state:approved', 'u2', 200, 'by u2');
        "#,
    )
    .expect("seed legacy table");

    changelist::migrate(&conn).expect("migrate rebuilds the legacy table");

    // Legacy rows survive with stringified ids + NULL audit_hash (they predate
    // content addressing; NULLs are distinct under the unique index).
    let rows: Vec<(String, String, Option<String>)> = conn
        .prepare("SELECT id, transition, audit_hash FROM change_audit WHERE tenant_id='t' ORDER BY ts")
        .expect("prepare")
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows");
    assert_eq!(rows.len(), 2, "both legacy audit rows survive the rebuild");
    assert_eq!(rows[0], ("1".to_string(), "append".to_string(), None));
    assert_eq!(rows[1].1, "set_state:approved");
    assert!(rows[1].2.is_none(), "legacy rows carry NULL audit_hash");

    // The rebuilt table accepts new content-addressed writes end-to-end.
    let e = changelist::append(&conn, "a", "main", op_entry("t", "a", "mute", 0, json!({})))
        .expect("append on rebuilt table");
    let new_hash: Option<String> = conn
        .query_row(
            "SELECT audit_hash FROM change_audit WHERE tenant_id='t' AND entry_id=?1 AND transition='append'",
            [&e.id],
            |r| r.get(0),
        )
        .expect("new audit row");
    assert!(new_hash.is_some(), "new audit rows are content-addressed after the rebuild");

    // Re-running the migration on the ALREADY-rebuilt table is a no-op (idempotent —
    // the INTEGER-id guard no longer fires; nothing is dropped or duplicated).
    let before: i64 = conn
        .query_row("SELECT COUNT(*) FROM change_audit", [], |r| r.get(0))
        .expect("count before");
    changelist::migrate(&conn).expect("re-migrate rebuilt table");
    let after: i64 = conn
        .query_row("SELECT COUNT(*) FROM change_audit", [], |r| r.get(0))
        .expect("count after");
    assert_eq!(before, after, "re-migration neither drops nor duplicates audit rows");
}

// ── FFI dispatch never panics — bad input surfaces as {"error": ...} JSON ──────

#[test]
fn command_dispatch_returns_clean_json_errors() {
    // These paths must NEVER panic (they sit behind cyan_changelist_command). The
    // process-global DB is deliberately untouched here: garbage JSON, a missing op,
    // an unknown op, and an op reaching for the uninitialized DB all come back as
    // parseable {"error": ...} strings.
    for bad in [
        "not json at all",
        r#"{"no_op_field": 1}"#,
        r#"{"op": "definitely_not_an_op"}"#,
        r#"{"op": "get", "tenant_id": "t", "asset_hash": "a"}"#,
    ] {
        let out = changelist::command(bad);
        let v: serde_json::Value =
            serde_json::from_str(&out).expect("dispatch output is always valid JSON");
        assert!(
            v.get("error").and_then(|e| e.as_str()).is_some(),
            "bad command {bad:?} surfaces a clean error, got: {out}"
        );
    }
}

// ── echo suppression: own write-back refs are flagged (own_refs table) ─────────

#[test]
fn own_writeback_ref_is_flagged() {
    let conn = db();

    // An actuator posted comment-123 to Frame.io and recorded the breadcrumb.
    changelist::record_own_ref(&conn, "t1", "frameio", "comment-123").expect("record own ref");

    // The sensor leg's echo check: OUR write-back is flagged…
    assert!(
        changelist::is_own_source_ref(&conn, "t1", "frameio", "comment-123").expect("check own"),
        "our own write-back must be flagged"
    );
    // …and everything else is not: another ref, another source, another tenant.
    assert!(!changelist::is_own_source_ref(&conn, "t1", "frameio", "comment-999").expect("other ref"));
    assert!(!changelist::is_own_source_ref(&conn, "t1", "resolve", "comment-123").expect("other source"));
    assert!(!changelist::is_own_source_ref(&conn, "t2", "frameio", "comment-123").expect("other tenant"));

    // Recording the same write-back twice is a no-op (idempotent breadcrumb).
    changelist::record_own_ref(&conn, "t1", "frameio", "comment-123").expect("idempotent record");
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM own_refs WHERE tenant_id='t1' AND source='frameio' AND source_ref='comment-123'",
            [],
            |r| r.get(0),
        )
        .expect("count");
    assert_eq!(n, 1, "one breadcrumb row, not two");

    // Blank pieces are rejected with a clean error, never a panic.
    assert!(changelist::record_own_ref(&conn, "t1", "", "x").is_err());
    assert!(changelist::record_own_ref(&conn, "t1", "frameio", " ").is_err());
}
