//! Substrate (multi-process) — OFFLINE / no-internet multi-user: the signed-grant join and its
//! RBAC gate work with NO relay and NO cloud (Lens) reachable. Every `cyan_node` here runs with
//! `RELAY=disabled` and dials only over loopback (the offline / LAN substrate), and nothing in the
//! join path touches Lens — the mesh authenticates and authorizes purely by verifying the
//! XaeroID-signed grant locally on the holder. Assertions are on the receiver's OWN storage.
//!
//! These are the offline analogues of `substrate_multiuser_mp`: same engine path, but they pin the
//! property that group join + mesh RBAC need no server. Bounded waits only. iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::Duration;

use multiprocess::{wire_pair, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// Offline, signed-grant join: with relay disabled and no cloud, a joiner presenting a valid grant
/// joins group G over the LAN and snapshots G locally — authorization is the locally-verified grant.
#[tokio::test]
async fn group_join_via_qr_works_offline() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    let mut host = MpNode::spawn("host-offline", &key, None, Some(&group))
        .await
        .expect("host spawns offline (relay disabled)");
    host.enforce_group(&group).await.expect("host enforces G");

    let mut joiner = MpNode::spawn("joiner-offline", &key, Some(&host.node_id), None)
        .await
        .expect("joiner spawns offline");
    wire_pair(&mut host, &mut joiner).await.expect("exchange loopback addrs");

    let (_nonce, qr) = host.issue_grant(&group, "member", 3600).await.expect("issue grant");
    joiner
        .join_group_with_grant(&group, Some(&host.node_id), Some(&qr))
        .await
        .expect("joiner joins G with grant, offline");

    let synced = joiner
        .wait_sync(&group, Duration::from_secs(60))
        .await
        .expect("wait_sync control call");
    assert!(synced, "offline signed-grant join did not reach SyncComplete");
    assert_eq!(joiner.count("workspaces", &group).await.expect("count ws"), 1,
        "offline joiner with a valid grant should have G's workspace");
    assert_eq!(joiner.count("elements", &group).await.expect("count el"), 5,
        "offline joiner with a valid grant should have G's elements");

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}

/// Offline mesh RBAC: with relay disabled and no cloud, an enforced group still refuses a joiner
/// that presents NO grant — the holder enforces purely by local grant verification, no server.
#[tokio::test]
async fn unauthorized_peer_rejected_offline() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    let mut host = MpNode::spawn("host-offline", &key, None, Some(&group))
        .await
        .expect("host spawns offline");
    host.enforce_group(&group).await.expect("host enforces G");

    let mut joiner = MpNode::spawn("joiner-offline", &key, Some(&host.node_id), None)
        .await
        .expect("joiner spawns offline");
    wire_pair(&mut host, &mut joiner).await.expect("exchange loopback addrs");

    joiner
        .join_group_with_grant(&group, Some(&host.node_id), None)
        .await
        .expect("joiner attempts join with no grant, offline");

    let synced = joiner
        .wait_sync(&group, Duration::from_secs(10))
        .await
        .expect("wait_sync control call");
    assert!(!synced, "offline enforced group must refuse a grant-less joiner");
    assert_eq!(joiner.count("workspaces", &group).await.expect("count ws"), 0,
        "offline grant-less joiner received no rows");

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}
