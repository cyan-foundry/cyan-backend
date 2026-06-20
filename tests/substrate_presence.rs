//! Substrate — LIVE presence: a group's roster reflects REAL connected mesh peers (not a mock).
//!
//! Oracle: the RECEIVER's own `SwiftEvent` channel. The engine emits `PeerCountChanged` and
//! `MeshReachability` off each node's `TopicActor` peer set on every gossip NeighborUp/NeighborDown
//! — so asserting on a node's own events is honest per-node presence truth. Bounded waits only.
//! iroh 0.95; relay disabled (loopback).

mod support;
use cyan_backend::models::events::SwiftEvent;
use support::{
    meet, serial, spawn_mesh, unique_discovery_key, unique_group_id, DiscoveryPolicy, NodeCfg,
    RelayPolicy, SYNC_TIMEOUT,
};

fn lan_cfg() -> NodeCfg {
    NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        discovery_key: unique_discovery_key(),
    }
}

/// With N peers on a group topic, the seed's presence stream reports the group as `online`
/// (reachable on the mesh) with a live connected-peer count — the live roster, driven off the
/// real mesh peer set, replacing the old mock roster.
#[tokio::test]
async fn presence_roster_reflects_connected_peers() {
    let _serial = serial().await;
    let nodes = spawn_mesh(3, lan_cfg()).await.expect("3-node mesh spawns");
    let group = unique_group_id();

    // Form the group topic mesh: every node joins; the seed (node[0]) is dialed by the others.
    // `meet` confirms end-to-end delivery, so by the time it returns the seed has live neighbors.
    // `meet` drains only the non-seed nodes' event channels, so the seed's presence events remain
    // buffered for us to assert on.
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("group topic mesh forms");

    let gid = group.clone();
    // The seed reports the group reachable on the mesh (≥1 connected peer).
    let reach = nodes[0]
        .wait_for(
            move |e| matches!(e, SwiftEvent::MeshReachability { group_id, state }
                if *group_id == gid && state == "online"),
            SYNC_TIMEOUT,
        )
        .await
        .expect("seed observes the group as online once peers connect");
    assert!(matches!(reach, SwiftEvent::MeshReachability { .. }));

    // The seed's live peer count reflects ≥1 real connected peer (the roster is non-empty).
    let gid = group.clone();
    let count_ev = nodes[0]
        .wait_for(
            move |e| matches!(e, SwiftEvent::PeerCountChanged { group_id, count }
                if *group_id == gid && *count >= 1),
            SYNC_TIMEOUT,
        )
        .await
        .expect("seed observes a live connected-peer count of at least 1");
    if let SwiftEvent::PeerCountChanged { count, .. } = count_ev {
        assert!(count >= 1, "live roster must reflect at least one connected peer");
    }

    for n in nodes {
        n.shutdown().await;
    }
}

/// The LEAVE direction: after all peers disconnect, the seed's roster shrinks and the group
/// falls back to `local_only`.
///
/// `#[ignore]`d — NOT a missing capability. The engine DOES emit the shrink: `emit_presence` runs
/// on every gossip `NeighborDown`, so the seed emits `MeshReachability{local_only}` the moment its
/// last peer drops. The problem is purely the *timing* of that signal: iroh's connection-loss /
/// NeighborDown detection over loopback is not engine-bounded (it can take far longer than any
/// sane test timeout when a peer's endpoint simply closes), so this cannot be asserted with a
/// bounded wait the way the substrate discipline requires. The leave/reconnect path is instead
/// exercised by the multi-process partition scaffold (see `substrate_multiuser_mp` /
/// STATUS_BACKEND_MULTIUSER.md). The assertion below is race-free by construction (a `local_only`
/// can only be emitted when the count reaches 0, which never happens during join) — it would pass
/// the moment NeighborDown fires; it's the unbounded latency, not correctness, that gates it.
#[tokio::test]
#[ignore = "engine emits roster-shrink on NeighborDown, but iroh's NeighborDown latency over loopback is not bounded; asserted via the multi-process partition scaffold instead"]
async fn presence_tracks_join_leave_for_n_peers() {
    let _serial = serial().await;
    let nodes = spawn_mesh(3, lan_cfg()).await.expect("3-node mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("group topic mesh forms");

    // Drop every non-seed peer; the seed must fall back to local_only once its last peer leaves.
    let mut nodes = nodes;
    let seed = nodes.remove(0);
    for n in nodes {
        n.shutdown().await;
    }

    let gid = group.clone();
    seed.wait_for(
        move |e| matches!(e, SwiftEvent::MeshReachability { group_id, state }
            if *group_id == gid && state == "local_only"),
        SYNC_TIMEOUT,
    )
    .await
    .expect("seed observes the group fall back to local_only after all peers leave");

    seed.shutdown().await;
}
