//! Substrate G1/G2-LAN — discovery & connection (SUBSTRATE_TEST_SPEC §3).
//! First file after the harness; its first test doubles as the harness smoke test.
//! DO NOT weaken assertions. Bounded waits only. iroh 0.95.

mod support;
use std::time::Duration;
use support::{spawn_mesh, spawn_node, DiscoveryPolicy, NodeCfg, RelayPolicy, SYNC_TIMEOUT};

/// Harness smoke + G1: two mDNS nodes on the same (loopback) LAN discover each
/// other with relay disabled. Proves spawn_node/spawn_mesh and the meet path.
#[tokio::test]
async fn two_nodes_meet_via_mdns_on_lan() {
    let cfg = NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        ..NodeCfg::default()
    };
    let nodes = spawn_mesh(2, cfg).await.expect("mesh spawns");
    // TODO(agent): assert each node sees the other as a peer within a timeout
    // (e.g. wait_for PeerJoined, or poll peers_per_group). Keep the wait bounded.
    let _ = (&nodes, SYNC_TIMEOUT);
    todo!("assert mutual discovery within SYNC_TIMEOUT");
}

/// G1 via an explicit bootstrap node instead of mDNS.
#[tokio::test]
async fn two_nodes_meet_via_bootstrap() {
    let boot = spawn_node("boot", NodeCfg::default()).await.expect("bootstrap");
    let cfg = NodeCfg {
        discovery: DiscoveryPolicy::Bootstrap(boot.node_id.clone()),
        ..NodeCfg::default()
    };
    let _peer = spawn_node("peer", cfg).await.expect("peer");
    let _ = Duration::from_secs(0);
    todo!("assert peer discovers boot (and vice versa) within SYNC_TIMEOUT");
}
