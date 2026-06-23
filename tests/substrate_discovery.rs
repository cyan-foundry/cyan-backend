//! Substrate G1/G2-LAN — discovery & connection (SUBSTRATE_TEST_SPEC §3).
//! First file after the harness; its first test doubles as the harness smoke test.
//! DO NOT weaken assertions. Bounded waits only. iroh 0.95.
//!
//! Oracle: the **per-node** `PeerJoined` event for the shared group's topic (emitted
//! by the receiver's TopicActor on gossip NeighborUp) — proving each node actually
//! connected to the other. `peers_per_group` (per-node, populated by the discovery
//! `groups_exchange` path) is checked as a secondary signal where it applies.

mod support;
use support::{
    meet, serial, spawn_mesh, spawn_node, unique_discovery_key, unique_group_id, DiscoveryPolicy,
    NodeCfg, RelayPolicy, SYNC_TIMEOUT,
};

/// Harness smoke + G1/G2-LAN: two mDNS nodes on the same (loopback) LAN discover each
/// other with relay disabled. Proves spawn_node/spawn_mesh and the meet path, and that
/// the seed's address resolves over mDNS (no relay, no DNS) so they can connect.
#[tokio::test]
async fn two_nodes_meet_via_mdns_on_lan() {
    let _serial = serial().await;
    let cfg = NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        discovery_key: unique_discovery_key(),
    };
    let nodes = spawn_mesh(2, cfg).await.expect("mesh spawns");
    let group = unique_group_id();

    // Mutual discovery: every node must observe a PeerJoined for the shared group
    // within the bounded timeout, i.e. the group topic mesh actually formed.
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("both mDNS nodes discover each other on the LAN with relay disabled");
}

/// G1 via an explicit bootstrap node instead of mDNS auto-seeding. A dedicated `boot`
/// node is the rendezvous; `peer` is configured with `Bootstrap(boot.node_id)` so its
/// discovery topic dials `boot` directly. Both then meet on the shared group topic.
#[tokio::test]
async fn two_nodes_meet_via_bootstrap() {
    let _serial = serial().await;
    // One discovery key shared by this scenario's two nodes (isolates it from others).
    let key = unique_discovery_key();
    let boot = spawn_node(
        "boot",
        NodeCfg {
            discovery_key: key.clone(),
            ..NodeCfg::default()
        },
    )
    .await
    .expect("bootstrap node spawns");
    let cfg = NodeCfg {
        discovery: DiscoveryPolicy::Bootstrap(boot.node_id.clone()),
        discovery_key: key,
        ..NodeCfg::default()
    };
    let peer = spawn_node("peer", cfg).await.expect("peer spawns");

    let group = unique_group_id();
    // peer dials boot's group topic; boot accepts. Assert both observe the join.
    meet(&[boot, peer], &group, SYNC_TIMEOUT)
        .await
        .expect("peer discovers boot (and vice versa) via the bootstrap node");
}
