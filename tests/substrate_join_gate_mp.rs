//! Substrate (multi-process) — §6 ENTITLEMENT-GATED JOIN (the joiner-side self-gate).
//!
//! SUPER_PEER_COMPLETION_SPEC §6: "a peer may join/subscribe ONLY groups in its grant; reject a
//! join for a non-granted group." The existing `substrate_multiuser_mp` tests cover the HOLDER-side
//! refusal (an enforced host won't SERVE a snapshot to a grant-less joiner). This file covers the
//! NEW, complementary JOINER-side gate: a node that has opted into entitlement enforcement refuses
//! to even SUBSCRIBE to a group it holds no valid grant for — so it can neither receive its gossip
//! nor enumerate it.
//!
//! To isolate the joiner's OWN gate, the HOST does NOT enforce (it would happily serve). The only
//! thing that can block the join is the joiner's `authorize_join`. Oracle: a refused join never
//! reaches `SyncComplete` and leaves ZERO trace of the group in the joiner's own DB; the positive
//! control (a valid grant for the joined group) DOES sync — proving the host was willing all along.
//!
//! DO NOT weaken assertions. Bounded waits (the child `wait_sync` + per-request timeouts). iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::Duration;

use multiprocess::{wire_pair, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// Bounds the "it never synced" assertion for a refused join (the refusal is local + fast).
const REJECT_WAIT: Duration = Duration::from_secs(10);
/// Bounds the positive-control sync.
const SYNC_WAIT: Duration = Duration::from_secs(60);

/// A joiner that enforces entitlement refuses to join a group it isn't granted: no grant and a
/// wrong-group grant are both rejected (it never subscribes), while a valid grant for the joined
/// group is accepted (it syncs). The host never enforces — so every refusal is the JOINER's gate.
#[tokio::test]
async fn join_non_granted_group_rejected() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let granted = unique_group_id(); // G — the group the joiner IS entitled to
    let other = unique_group_id(); // H — a group the joiner holds a grant for, but isn't joining

    // Host holds BOTH groups and does NOT enforce either — it would serve a snapshot to anyone.
    let mut host = MpNode::spawn("host", &key, None, Some(&granted))
        .await
        .expect("host spawns + seeds G at startup");
    host.seed_fixture(&other).await.expect("host seeds H too");

    // Joiner opts INTO entitlement for both groups (this also registers it as their Owner-admin, so
    // a grant it self-issues verifies against its own roster — the honest local entitlement source).
    let mut joiner = MpNode::spawn("joiner", &key, Some(&host.node_id), None)
        .await
        .expect("joiner spawns clean");
    joiner.enforce_group(&granted).await.expect("joiner enforces G");
    joiner.enforce_group(&other).await.expect("joiner enforces H");
    wire_pair(&mut host, &mut joiner).await.expect("exchange loopback addrs");

    // ── Case 1: NO grant → refused. The joiner never subscribes to G. ──
    joiner
        .join_group_with_grant(&granted, Some(&host.node_id), None)
        .await
        .expect("attempt join G with no grant");
    let synced = joiner.wait_sync(&granted, REJECT_WAIT).await.expect("wait_sync control");
    assert!(!synced, "a grant-less join of an enforced group must NOT sync (joiner-side gate)");
    assert_eq!(
        joiner.count("groups", &granted).await.expect("count groups G"),
        0,
        "refused join must leave NO trace of the group in the joiner's DB"
    );

    // ── Case 2: WRONG-group grant → refused. A grant for H does not authorize joining G. ──
    let (_n, qr_h) = joiner.issue_grant(&other, "member", 3600).await.expect("issue grant for H");
    joiner
        .join_group_with_grant(&granted, Some(&host.node_id), Some(&qr_h))
        .await
        .expect("attempt join G with an H grant");
    let synced = joiner.wait_sync(&granted, REJECT_WAIT).await.expect("wait_sync control");
    assert!(!synced, "a grant for a DIFFERENT group must NOT authorize joining G (WrongGroup)");
    assert_eq!(
        joiner.count("groups", &granted).await.expect("count groups G after wrong grant"),
        0,
        "wrong-group join must still leave NO trace of G"
    );

    // ── Case 3 (positive control): a VALID grant for G → accepted, syncs in full. ──
    let (_n2, qr_g) = joiner.issue_grant(&granted, "member", 3600).await.expect("issue grant for G");
    joiner
        .join_group_with_grant(&granted, Some(&host.node_id), Some(&qr_g))
        .await
        .expect("join G with a valid G grant");
    let synced = joiner.wait_sync(&granted, SYNC_WAIT).await.expect("wait_sync control");
    assert!(synced, "a valid grant for the joined group IS accepted (host was willing all along)");
    assert_eq!(
        joiner.count("elements", &granted).await.expect("count elements G"),
        5,
        "the entitled join pulled G's full fixture"
    );

    // The host genuinely held G (so the 0s above mean "refused", not "empty source").
    assert_eq!(
        host.count("elements", &granted).await.expect("host count elements G"),
        5,
        "host's own DB holds G — the refusals were the joiner's gate, not a missing source"
    );

    joiner.shutdown().await;
    host.shutdown().await;
}
