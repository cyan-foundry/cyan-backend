//! Substrate — review-ledger P2P sync (CYAN_FORMAT_SPEC §6.2–§6.5).
//!
//! The ledger replicates over the same three legs as the rest of group state:
//! live gossip deltas (`NetworkEvent::Change*`, §6.2), the join-time snapshot
//! (the five review tables ride the `Metadata` frame, §6.4), and the anti-entropy
//! digest sweep (the `ce`/`cv`/`cb`/`ca`/`rs` lanes, §6.4). These tests prove the
//! §6.5 properties with the honest in-process split this suite uses everywhere
//! (see `substrate_anti_entropy_lanes.rs`, the C3 precedent):
//!
//! * **Wire tests** (2-node MeshHarness, loopback, relay disabled): a delta put on
//!   the group topic is received and applied by the peer's persist worker through
//!   the idempotent `changelist::` fns — the dedup/LWW/union oracles are honest
//!   even under the shared process-global DB, because a NON-idempotent apply would
//!   duplicate rows / clobber lifecycle and the storage assertions would catch it.
//! * **Tier-1 tests** (DETECT / CARRY / MERGE): the digest flips on every ledger
//!   lane, the snapshot carries all five tables, and the apply fns are
//!   order-independent — the deterministic properties the cross-process sweep
//!   heal rests on. The e2e heal itself needs per-node DBs (multi-process /
//!   docker rig), exactly like notes/pins (`substrate_stress`).
//!
//! Assertions are on `storage::*` / `changelist::*` / `group_digest` / built
//! frames — never log lines. Bounded waits only. iroh 0.95.

mod support;

use std::time::Duration;

use cyan_backend::anti_entropy::group_digest;
use cyan_backend::changelist::{
    self, compute_audit_hash, ChangeAudit, ChangeEntry, ChangeVersion, LifecycleDelta,
};
use cyan_backend::models::events::NetworkEvent;
use cyan_backend::review_state::{self, ReviewState};
use cyan_backend::snapshot::{apply_snapshot_frames, build_snapshot_frames};
use cyan_backend::storage;
use rusqlite::Connection;
use serde_json::json;
use support::{
    ensure_db, serial, spawn_mesh, unique_group_id, wait_until, NodeCfg, SYNC_TIMEOUT,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn cfg() -> NodeCfg {
    NodeCfg::default() // relay Disabled + loopback — the offline-LAN substrate
}

/// A minimal op entry; `append`/`apply_entry` fill id/hash/seq.
fn op_entry(tenant: &str, asset: &str, op: &str, tc_in: i64) -> ChangeEntry {
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
        params: json!({"edge": "head", "frames": 10}),
        intent: format!("{op} at {tc_in}"),
        source: Some("frameio".to_string()),
        source_ref: None,
        author: Some("u-editor".to_string()),
        role: Some("editor".to_string()),
        proposed_by: Some("human".to_string()),
        created_at: 1000,
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

/// Run `f` against the shared process-global DB without holding the lock across awaits.
fn with_conn<T>(f: impl FnOnce(&Connection) -> T) -> T {
    let lock = storage::db().lock().expect("db lock");
    f(&lock)
}

/// Fresh isolated in-memory ledger DB (the Tier-1 MERGE oracle).
fn mem_db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("changelist migrate");
    review_state::migrate(&conn).expect("review_state migrate");
    conn
}

/// A wire-shaped lifecycle delta + its content-addressed audit row.
fn lifecycle_delta(
    tenant: &str,
    entry_id: &str,
    entry_hash: &str,
    state: &str,
    updated_at: i64,
    actor: &str,
) -> LifecycleDelta {
    let transition = format!("state:{state}");
    let audit_hash = compute_audit_hash(entry_id, &transition, Some(actor), updated_at, None);
    LifecycleDelta {
        entry_id: entry_id.to_string(),
        entry_hash: entry_hash.to_string(),
        state: state.to_string(),
        active: state != "rejected",
        approved_by: Some(actor.to_string()),
        approved_at: Some(updated_at),
        superseded_by: None,
        version_ref: None,
        outcome: None,
        updated_at,
        updated_by: Some(actor.to_string()),
        audit: Some(ChangeAudit {
            id: format!("audit-{entry_id}-{updated_at}-{actor}"),
            entry_id: entry_id.to_string(),
            tenant_id: tenant.to_string(),
            transition,
            actor: Some(actor.to_string()),
            ts: updated_at,
            detail: None,
            audit_hash: Some(audit_hash),
        }),
    }
}

/// Count the tenant's entries holding exactly this content hash.
fn rows_with_hash(tenant: &str, hash: &str) -> usize {
    storage::change_entry_list_by_tenant(tenant)
        .expect("list entries")
        .iter()
        .filter(|e| e.entry_hash == hash)
        .count()
}

// ── §6.5 changelist_append_gossips_and_dedups_by_entry_hash ──────────────────
//
// 2 nodes, same entry → 1 row. The author holds the row locally; the delta rides
// the group topic; the receiver's persist worker applies it through the union
// path — a replayed delta (gossip is at-least-once) must never mint a second row.
#[tokio::test]
async fn changelist_append_gossips_and_dedups_by_entry_hash() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    support::meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    // Author locally on node-0's engine (tenant == the group id).
    let asset = format!("{group}-asset");
    let entry = with_conn(|c| changelist::append(c, &asset, "main", op_entry(&group, &asset, "trim", 100)))
        .expect("local append");
    assert_eq!(rows_with_hash(&group, &entry.entry_hash), 1, "author holds one row");

    // Live delta → group topic → peer applies (persist worker runs BEFORE forwarding).
    let evt = NetworkEvent::ChangeEntryAppended {
        tenant_id: group.clone(),
        entry: Box::new(entry.clone()),
    };
    nodes[0].broadcast(&group, evt.clone());
    let want = entry.entry_hash.clone();
    nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::ChangeEntryAppended { entry, .. } if entry.entry_hash == want),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer receives the append delta");
    assert_eq!(
        rows_with_hash(&group, &entry.entry_hash),
        1,
        "the receiver's apply deduped by (tenant, entry_hash) — union, not duplicate"
    );

    // Replay: the gossip layer itself dedups byte-identical payloads, so a wire
    // replay can't be forced in-process — the at-least-once case that matters is a
    // REDELIVERED delta hitting the receiver's apply path again. Run exactly that
    // fn (the one the persist worker runs) and assert the union holds.
    storage::change_entry_apply(&entry).expect("replayed apply");
    assert_eq!(
        rows_with_hash(&group, &entry.entry_hash),
        1,
        "a replayed append delta is a no-op (idempotent by construction)"
    );
}

// ── §6.5 lifecycle_lww_converges_and_audit_unions ─────────────────────────────
//
// Concurrent approve vs reject: the newer clock wins on every peer REGARDLESS of
// arrival order, and the audit trail preserves BOTH transitions (union) even
// though LWW discarded one in effect. Ties break deterministically by actor id.
#[tokio::test]
async fn lifecycle_lww_converges_and_audit_unions() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    support::meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    let asset = format!("{group}-asset");
    let entry = with_conn(|c| changelist::append(c, &asset, "main", op_entry(&group, &asset, "lift", 200)))
        .expect("seed entry");

    // Two concurrent lifecycle writes; deliver NEWER first, STALE second (the
    // out-of-order arrival that would clobber without the LWW guard).
    let newer = lifecycle_delta(&group, &entry.id, &entry.entry_hash, "approved", 2000, "reviewer-b");
    let stale = lifecycle_delta(&group, &entry.id, &entry.entry_hash, "rejected", 1000, "reviewer-a");
    let newer_hash = newer.audit.as_ref().expect("audit").audit_hash.clone().expect("hash");
    let stale_hash = stale.audit.as_ref().expect("audit").audit_hash.clone().expect("hash");

    for delta in [newer, stale] {
        let state = delta.state.clone();
        nodes[0].broadcast(&group, NetworkEvent::ChangeEntryLifecycle {
            tenant_id: group.clone(),
            delta: Box::new(delta),
        });
        nodes[1]
            .wait_network(
                move |e| matches!(e, NetworkEvent::ChangeEntryLifecycle { delta, .. } if delta.state == state),
                SYNC_TIMEOUT,
            )
            .await
            .expect("peer receives the lifecycle delta");
    }

    let row = with_conn(|c| changelist::get_entry(c, &group, &entry.id)).expect("entry row");
    assert_eq!(row.state, "approved", "newer clock (2000) wins; stale reject@1000 never clobbers");
    assert_eq!(row.updated_at, 2000, "lifecycle clock is the winner's");

    let audit_hashes: Vec<Option<String>> = storage::change_audit_list_by_tenant(&group)
        .expect("audit list")
        .into_iter()
        .map(|a| a.audit_hash)
        .collect();
    assert!(
        audit_hashes.contains(&Some(newer_hash)) && audit_hashes.contains(&Some(stale_hash)),
        "the audit trail unions BOTH transitions — LWW discards in effect, never in provenance"
    );

    // Tie-break (§6.3): equal clocks ⇒ higher actor id wins, in EITHER apply order.
    for reversed in [false, true] {
        let conn = mem_db();
        let e = changelist::apply_entry(&conn, &op_entry("t-tie", "asset-tie", "mute", 0))
            .expect("seed tie entry");
        let low = lifecycle_delta("t-tie", &e.id, &e.entry_hash, "applied", 3000, "aaa-actor");
        let high = lifecycle_delta("t-tie", &e.id, &e.entry_hash, "approved", 3000, "zzz-actor");
        let (first, second) = if reversed { (high.clone(), low.clone()) } else { (low, high) };
        changelist::apply_lifecycle(&conn, "t-tie", &first).expect("apply first");
        changelist::apply_lifecycle(&conn, "t-tie", &second).expect("apply second");
        let row = changelist::get_entry(&conn, "t-tie", &e.id).expect("row");
        assert_eq!(
            (row.state.as_str(), row.updated_by.as_deref()),
            ("approved", Some("zzz-actor")),
            "equal clocks: the higher actor id wins deterministically (reversed={reversed})"
        );
    }
}

// ── §6.5 cold_joiner_snapshot_carries_full_ledger ─────────────────────────────
//
// CARRY + MERGE (the C3 split): the snapshot a cold joiner pulls carries ALL five
// review tables, and applying those frames is an idempotent union — so the joiner
// ends holding the host's exact ledger and a replayed frame changes nothing. The
// QUIC transfer itself is the multi-process rig's job (the in-process cold join is
// structurally dishonest under the shared DB — see substrate_sync.rs's G3 note).
#[tokio::test]
async fn cold_joiner_snapshot_carries_full_ledger() {
    ensure_db();
    let gid = unique_group_id();
    storage::group_insert_simple(&gid, "Ledger fixture", "folder.fill", "#00AEEF").expect("group");

    // A real little ledger: two entries, an approval, a version, a review row.
    let asset = format!("{gid}-asset");
    let (e1, e2, v1) = with_conn(|c| {
        let e1 = changelist::append(c, &asset, "main", op_entry(&gid, &asset, "trim", 0)).expect("e1");
        let e2 = changelist::append(c, &asset, "main", op_entry(&gid, &asset, "lift", 48)).expect("e2");
        changelist::set_state(c, &gid, &e1.id, "approved", Some("producer")).expect("approve e1");
        let v1 = changelist::snapshot(c, &gid, &asset, "main").expect("v1");
        review_state::start_draft(c, &gid, &asset, "main").expect("draft");
        (e1, e2, v1)
    });

    // CARRY — the Metadata frame holds all five tables for this tenant.
    let frames = build_snapshot_frames(&gid, None).expect("build frames");
    let meta = frames
        .iter()
        .find_map(|f| match f {
            cyan_backend::models::protocol::SnapshotFrame::Metadata {
                change_entries,
                change_versions,
                change_branches,
                change_audits,
                review_states,
                ..
            } => Some((change_entries, change_versions, change_branches, change_audits, review_states)),
            _ => None,
        })
        .expect("snapshot has a Metadata frame");
    let (entries, versions, branches, audits, reviews) = meta;
    assert!(
        entries.iter().any(|e| e.entry_hash == e1.entry_hash)
            && entries.iter().any(|e| e.entry_hash == e2.entry_hash),
        "frame carries both entries"
    );
    assert!(
        entries.iter().any(|e| e.entry_hash == e1.entry_hash && e.state == "approved"),
        "frame entries carry lifecycle (the approval travels with the row)"
    );
    assert!(
        versions.iter().any(|v| v.version_id == v1.version_id),
        "frame carries the version"
    );
    assert!(
        branches.iter().any(|b| b.branch == "main" && b.head_version.as_deref() == Some(v1.version_id.as_str())),
        "frame carries the branch head"
    );
    assert!(
        audits.iter().any(|a| a.transition == "state:approved"),
        "frame carries the audit trail"
    );
    assert!(
        reviews.iter().any(|r| r.asset_hash == asset && r.state == "DRAFT"),
        "frame carries the review_state row"
    );

    // MERGE — re-applying the full ledger frames is a no-op union: identical
    // digest, identical row counts (a cold joiner applying twice, or a repair
    // racing the join, never duplicates or regresses).
    let digest_before = group_digest(&gid);
    let count_before = (
        storage::change_entry_list_by_tenant(&gid).expect("entries").len(),
        storage::change_version_list_by_tenant(&gid).expect("versions").len(),
        storage::change_branch_list_by_tenant(&gid).expect("branches").len(),
        storage::change_audit_list_by_tenant(&gid).expect("audits").len(),
        storage::review_state_list_by_tenant(&gid).expect("reviews").len(),
    );
    apply_snapshot_frames(&frames).expect("re-apply full snapshot");
    let count_after = (
        storage::change_entry_list_by_tenant(&gid).expect("entries").len(),
        storage::change_version_list_by_tenant(&gid).expect("versions").len(),
        storage::change_branch_list_by_tenant(&gid).expect("branches").len(),
        storage::change_audit_list_by_tenant(&gid).expect("audits").len(),
        storage::review_state_list_by_tenant(&gid).expect("reviews").len(),
    );
    assert_eq!(count_after, count_before, "replayed snapshot frames union to a no-op");
    assert_eq!(group_digest(&gid), digest_before, "digest unchanged by the replay");
}

// ── §6.5 divergence_while_apart_heals_on_sweep ────────────────────────────────
//
// Tier-1 proof of the heal, per the C3 precedent (substrate_anti_entropy_lanes):
// DETECT — every ledger lane flips the group digest, so a delta missed while
// apart is detectable; MERGE — the apply fns are order-independent, so the two
// sides' sweep pulls converge to the identical union no matter who pulls first.
// (In-process nodes share ONE process-global DB and cannot actually diverge; the
// cross-process sweep e2e lives with the notes/pins ones in the MP/docker rig.)
#[tokio::test]
async fn divergence_while_apart_heals_on_sweep() {
    ensure_db();
    let gid = unique_group_id();
    storage::group_insert_simple(&gid, "Divergence fixture", "folder.fill", "#00AEEF").expect("group");
    let asset = format!("{gid}-asset");

    // DETECT — each lane flips the digest (a dropped delta cannot hide).
    let base = group_digest(&gid);
    assert_eq!(group_digest(&gid), base, "digest deterministic for identical state");

    let e = with_conn(|c| changelist::append(c, &asset, "main", op_entry(&gid, &asset, "trim", 0)))
        .expect("append");
    let after_append = group_digest(&gid);
    assert_ne!(after_append.1, base.1, "ce lane: an append flips the digest");

    with_conn(|c| changelist::set_state(c, &gid, &e.id, "approved", Some("producer"))).expect("approve");
    let after_lifecycle = group_digest(&gid);
    assert_ne!(after_lifecycle.1, after_append.1, "ce lane: a lifecycle move flips the digest");

    with_conn(|c| changelist::snapshot(c, &gid, &asset, "main")).expect("version");
    let after_version = group_digest(&gid);
    assert_ne!(after_version.1, after_lifecycle.1, "cv+cb lanes: a snapshot flips the digest");

    with_conn(|c| review_state::start_draft(c, &gid, &asset, "main")).expect("draft");
    let after_review = group_digest(&gid);
    assert_ne!(after_review.1, after_version.1, "rs lane: a review-state move flips the digest");
    assert_eq!(group_digest(&gid), after_review, "digest stable once state stops moving");

    // MERGE — partition simulation: both sides share a base entry, then edit apart.
    // Side A approves @1000 and snapshots VA; side B rejects @2000 and snapshots VB.
    // Healing = each side applying the other's rows; both orders must converge to
    // the identical union (entry rejected@2000, BOTH versions kept, head = newer).
    let tenant = "t-diverge";
    // The shared base entry both sides hold BEFORE the partition: its id rides the
    // wire with the row (as it does in the real delta), so both sides key it alike.
    let mut base_entry = op_entry(tenant, "asset-d", "trim", 0);
    base_entry.id = "e-shared-base".to_string();
    let make_version = |id: &str, at: i64| ChangeVersion {
        version_id: id.to_string(),
        asset_hash: "asset-d".to_string(),
        tenant_id: tenant.to_string(),
        branch: "main".to_string(),
        version_no: 1,
        list_hash: format!("list-{id}"),
        cut_hash: format!("cut-{id}"),
        entry_hashes: vec![],
        created_at: at,
        outcome: "pending".to_string(),
    };

    let mut finals = Vec::new();
    for reversed in [false, true] {
        let conn = mem_db();
        let seeded = changelist::apply_entry(&conn, &base_entry).expect("seed shared entry");
        let side_a = |c: &Connection| {
            let d = lifecycle_delta(tenant, &seeded.id, &seeded.entry_hash, "approved", 1000, "peer-a");
            changelist::apply_lifecycle(c, tenant, &d).expect("A lifecycle");
            changelist::apply_version(c, &make_version("v-side-a", 1000)).expect("A version");
            changelist::apply_branch_head(c, tenant, "asset-d", "main", Some("v-side-a"), 1000)
                .expect("A head");
        };
        let side_b = |c: &Connection| {
            let d = lifecycle_delta(tenant, &seeded.id, &seeded.entry_hash, "rejected", 2000, "peer-b");
            changelist::apply_lifecycle(c, tenant, &d).expect("B lifecycle");
            changelist::apply_version(c, &make_version("v-side-b", 2000)).expect("B version");
            changelist::apply_branch_head(c, tenant, "asset-d", "main", Some("v-side-b"), 2000)
                .expect("B head");
        };
        if reversed {
            side_b(&conn);
            side_a(&conn);
        } else {
            side_a(&conn);
            side_b(&conn);
        }
        let projection = (
            changelist::list_entries_by_tenant(&conn, tenant).expect("entries"),
            changelist::list_versions_by_tenant(&conn, tenant).expect("versions"),
            changelist::list_branches_by_tenant(&conn, tenant)
                .expect("branches")
                .into_iter()
                .map(|b| (b.asset_hash, b.branch, b.head_version, b.updated_at))
                .collect::<Vec<_>>(),
        );
        finals.push(projection);
    }
    assert_eq!(finals[0], finals[1], "both apply orders converge to the identical union");
    let (entries, versions, branches) = &finals[0];
    assert_eq!(entries[0].state, "rejected", "newer lifecycle (B@2000) wins on both sides");
    assert_eq!(versions.len(), 2, "both concurrent versions survive (immutable union)");
    assert_eq!(
        branches[0].2.as_deref(),
        Some("v-side-b"),
        "branch head LWW picks the newer move"
    );
}

// ── §6.5 version_snapshot_immutable_union ─────────────────────────────────────
//
// Two peers snapshotting concurrently ⇒ two versions, both kept; head LWW picks;
// the set-once outcome label unions onto an existing row and never regresses.
#[tokio::test]
async fn version_snapshot_immutable_union() {
    let conn = mem_db();
    let tenant = "t-versions";
    let e = changelist::apply_entry(&conn, &op_entry(tenant, "asset-v", "trim", 0)).expect("entry");

    let local = ChangeVersion {
        version_id: "v-local".to_string(),
        asset_hash: "asset-v".to_string(),
        tenant_id: tenant.to_string(),
        branch: "main".to_string(),
        version_no: 1,
        list_hash: "list-local".to_string(),
        cut_hash: "cut-local".to_string(),
        entry_hashes: vec![e.entry_hash.clone()],
        created_at: 1000,
        outcome: "pending".to_string(),
    };
    let remote = ChangeVersion {
        version_id: "v-remote".to_string(),
        list_hash: "list-remote".to_string(),
        cut_hash: "cut-remote".to_string(),
        created_at: 1001,
        ..local.clone()
    };

    assert!(changelist::apply_version(&conn, &local).expect("local lands"));
    assert!(changelist::apply_version(&conn, &remote).expect("remote lands"));
    assert!(
        !changelist::apply_version(&conn, &remote).expect("replay is a no-op"),
        "replaying a version delta must not report an insert"
    );
    let versions = changelist::list_versions_by_tenant(&conn, tenant).expect("list");
    assert_eq!(versions.len(), 2, "concurrent versions BOTH survive — union, silent-drop never");

    // Head LWW: newer move wins; a stale move never clobbers back.
    assert!(changelist::apply_branch_head(&conn, tenant, "asset-v", "main", Some("v-local"), 1000).expect("head@1000"));
    assert!(changelist::apply_branch_head(&conn, tenant, "asset-v", "main", Some("v-remote"), 2000).expect("head@2000"));
    assert!(!changelist::apply_branch_head(&conn, tenant, "asset-v", "main", Some("v-local"), 1500).expect("stale head"));
    let head = changelist::get_branch(&conn, tenant, "asset-v", "main")
        .expect("branch row")
        .expect("exists");
    assert_eq!(head.head_version.as_deref(), Some("v-remote"), "head LWW picked the newer move");

    // Outcome is set-once and unions onto the existing immutable row.
    let mut shipped = remote.clone();
    shipped.outcome = "shipped".to_string();
    changelist::apply_version(&conn, &shipped).expect("outcome union");
    let after = changelist::get_version(&conn, tenant, "v-remote").expect("v-remote");
    assert_eq!(after.outcome, "shipped", "pending → shipped unions onto the existing row");
    let mut regress = remote.clone();
    regress.outcome = "rejected".to_string();
    changelist::apply_version(&conn, &regress).expect("regress attempt");
    let after = changelist::get_version(&conn, tenant, "v-remote").expect("v-remote");
    assert_eq!(after.outcome, "shipped", "a set outcome is never overwritten (set-once)");
}

// ── §6.5 review_state_lane_converges ──────────────────────────────────────────
//
// The `rs` lane end to end at Tier-1: DETECT (digest flips on a review-state
// move), CARRY (the snapshot frame holds the row), MERGE (LWW upsert is
// order-independent; stale/equal clocks are no-ops).
#[tokio::test]
async fn review_state_lane_converges() {
    ensure_db();
    let gid = unique_group_id();
    storage::group_insert_simple(&gid, "RS fixture", "folder.fill", "#00AEEF").expect("group");
    let asset = format!("{gid}-asset");

    // DETECT — start_draft touches ONLY review_state, so the flip is attributable.
    let base = group_digest(&gid);
    with_conn(|c| review_state::start_draft(c, &gid, &asset, "main")).expect("draft");
    let after = group_digest(&gid);
    assert_ne!(after.1, base.1, "rs lane: a review-state row flips the digest");

    // CARRY — the snapshot frame holds the row.
    let frames = build_snapshot_frames(&gid, None).expect("frames");
    let carried = frames.iter().any(|f| match f {
        cyan_backend::models::protocol::SnapshotFrame::Metadata { review_states, .. } => {
            review_states.iter().any(|r| r.asset_hash == asset && r.state == "DRAFT")
        }
        _ => false,
    });
    assert!(carried, "Metadata frame carries the review_state row (repairable)");

    // MERGE — LWW upsert on updated_at, order-independent.
    let conn = mem_db();
    let rs = |state: &str, at: i64| ReviewState {
        tenant_id: "t-rs".to_string(),
        asset_hash: "asset-rs".to_string(),
        branch: "main".to_string(),
        state: state.to_string(),
        round: 1,
        updated_at: at,
    };
    assert!(review_state::apply_remote(&conn, &rs("IN_REVIEW", 1000)).expect("in_review@1000"));
    assert!(review_state::apply_remote(&conn, &rs("NOTES_IN", 2000)).expect("notes_in@2000"));
    assert!(
        !review_state::apply_remote(&conn, &rs("IN_REVIEW", 1500)).expect("stale"),
        "a stale review-state row must not clobber a newer one"
    );
    assert!(
        !review_state::apply_remote(&conn, &rs("NOTES_IN", 2000)).expect("equal"),
        "an equal-clock replay is an idempotent no-op"
    );
    let row = review_state::get(&conn, "t-rs", "asset-rs", "main")
        .expect("get")
        .expect("row");
    assert_eq!((row.state.as_str(), row.updated_at), ("NOTES_IN", 2000), "newest write holds");
}

// ── §6.5 ledger_functional_with_lens_unreachable ──────────────────────────────
//
// The offline invariant: with relay DISABLED (pure loopback mesh) and Lens (an
// HTTP client leg, not a mesh peer) pointing at a dead port, the ledger works
// end to end — author, gossip, apply, version — and the digest stays computable.
#[tokio::test]
async fn ledger_functional_with_lens_unreachable() {
    let _serial = serial().await;

    // Lens → dead port; verify it is REALLY unreachable, then never touch it again.
    // SAFETY: single-threaded test setup; no other test in this binary reads these.
    unsafe {
        std::env::set_var("CYAN_LENS_URL", "http://127.0.0.1:1");
        std::env::set_var("CYAN_LENS_TIMEOUT", "1");
    }
    let lens = cyan_backend::cyan_lens_client::CyanLensClient::new(
        cyan_backend::cyan_lens_client::CyanLensConfig::from_env(),
    );
    assert!(!lens.is_available().await, "Lens must be unreachable in this scenario");

    let nodes = spawn_mesh(2, cfg()).await.expect("offline mesh spawns (relay disabled)");
    let group = unique_group_id();
    support::meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet over loopback");

    // Author → gossip → peer applies: the full content + version legs, Lens down.
    let asset = format!("{group}-asset");
    let entry = with_conn(|c| changelist::append(c, &asset, "main", op_entry(&group, &asset, "fade", 0)))
        .expect("append with Lens down");
    let version = with_conn(|c| changelist::snapshot(c, &group, &asset, "main")).expect("snapshot");

    nodes[0].broadcast(&group, NetworkEvent::ChangeEntryAppended {
        tenant_id: group.clone(),
        entry: Box::new(entry.clone()),
    });
    nodes[0].broadcast(&group, NetworkEvent::ChangeVersionCreated {
        tenant_id: group.clone(),
        version: Box::new(version.clone()),
    });

    let want = version.version_id.clone();
    nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::ChangeVersionCreated { version, .. } if version.version_id == want),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer receives the version delta with Lens unreachable");

    assert_eq!(rows_with_hash(&group, &entry.entry_hash), 1, "entry applied exactly once");
    let versions = storage::change_version_list_by_tenant(&group).expect("versions");
    assert!(
        versions.iter().any(|v| v.version_id == version.version_id),
        "version applied on the receiver path"
    );

    // The sweep detector stays alive offline: deterministic, non-empty digest.
    let (count, hash) = group_digest(&group);
    assert!(count > 0 && !hash.is_empty(), "digest computable with Lens down");
    wait_until(|| group_digest(&group) == (count, hash.clone()), Duration::from_secs(5), "digest stable")
        .await
        .expect("digest deterministic offline");
}

// ── Relay / WebSocket rungs — NOT in-process (SUBSTRATE_TEST_SPEC discipline) ──

/// Ledger deltas across a real relay (the G2 ladder) need actual network
/// isolation + a relay container — the docker rig in `cyan-local-harness/`.
#[ignore = "relay rung needs the docker rig (cyan-local-harness); not honestly testable in-process"]
#[tokio::test]
async fn ledger_sync_over_relay() {
    unimplemented!("docker rig: two isolated nodes + relay container, then the append/lifecycle/version legs");
}

/// Ledger sync with a WebSocket-bridged peer (G11) — same rig constraint.
#[ignore = "websocket rung needs the docker rig (cyan-local-harness); not honestly testable in-process"]
#[tokio::test]
async fn ledger_sync_over_websocket_bridge() {
    unimplemented!("docker rig: WS bridge leg, then the append/lifecycle/version legs");
}
