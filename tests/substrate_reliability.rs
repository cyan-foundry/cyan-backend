//! Substrate reliability — prove the green substrate behaviour is not flaky and
//! survives repetition, concurrency, and a larger mesh (OVERNIGHT_RUN PHASE 1).
//!
//! These tests do not add a new guarantee; they stress the *existing* G1/G2-LAN
//! discovery path (the foundation everything else builds on) under load. They reuse
//! the `support::` harness verbatim, hold the cross-process `serial()` guard, use
//! only bounded `meet()` waits, and assert on the per-node `meet`/`peers_per_group`
//! oracles — never on the process-global shared storage (see `support` module docs).
//!
//! DO NOT weaken assertions. Bounded waits only. iroh 0.95.

mod support;
use support::{
    meet, serial, spawn_mesh, unique_discovery_key, unique_group_id, DiscoveryPolicy, NodeCfg,
    RelayPolicy, SYNC_TIMEOUT,
};

/// Form and converge a fresh 2-node mesh many times in a row. Each iteration uses a
/// unique discovery key + group id and a freshly-spawned mesh (dropped at the end of
/// the iteration), so a pass means discovery is repeatably reliable, not a one-shot.
#[tokio::test]
async fn repeat_discovery_is_stable() {
    let _serial = serial().await;
    const ITERATIONS: usize = 15;

    for i in 0..ITERATIONS {
        let cfg = NodeCfg {
            relay: RelayPolicy::Disabled,
            discovery: DiscoveryPolicy::MdnsOnly,
            discovery_key: unique_discovery_key(),
        };
        let nodes = spawn_mesh(2, cfg)
            .await
            .unwrap_or_else(|e| panic!("iteration {i}: mesh spawns: {e}"));
        let group = unique_group_id();

        meet(&nodes, &group, SYNC_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("iteration {i}/{ITERATIONS}: 2 nodes failed to meet: {e}"));
        // `nodes` drops here, freeing the iteration's endpoints before the next round.
    }
}

/// Several independent 2-node meshes, each on its own discovery key + group id, formed
/// **concurrently**. They must not interfere: every mesh converges within the timeout.
/// This guards the isolation the suite relies on (unique keys ⇒ disjoint gossip topics)
/// when multiple scenarios are in flight at once.
#[tokio::test]
async fn concurrent_meshes_do_not_interfere() {
    let _serial = serial().await;
    const MESHES: usize = 3;

    // Spawn every mesh first, then drive all the `meet`s concurrently.
    let mut meshes = Vec::with_capacity(MESHES);
    for m in 0..MESHES {
        let cfg = NodeCfg {
            relay: RelayPolicy::Disabled,
            discovery: DiscoveryPolicy::MdnsOnly,
            discovery_key: unique_discovery_key(),
        };
        let nodes = spawn_mesh(2, cfg)
            .await
            .unwrap_or_else(|e| panic!("mesh {m}: spawns: {e}"));
        let group = unique_group_id();
        meshes.push((nodes, group));
    }

    let futures = meshes
        .iter()
        .enumerate()
        .map(|(m, (nodes, group))| async move {
            meet(nodes, group, SYNC_TIMEOUT)
                .await
                .unwrap_or_else(|e| panic!("concurrent mesh {m} failed to converge: {e}"));
        });
    futures::future::join_all(futures).await;
}

/// A single larger mesh (5 nodes sharing one discovery key) must fully converge: every
/// node joins the group topic, and the seed's group broadcast reaches **all** of them
/// (that is exactly what `meet` proves — gossip delivery seed→every node, which requires
/// the mesh to actually be connected). We additionally assert the per-node, shared-DB-safe
/// signal that each node holds the group's topic.
#[tokio::test]
async fn larger_mesh_converges() {
    let _serial = serial().await;
    const NODES: usize = 5;

    let cfg = NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        discovery_key: unique_discovery_key(),
    };
    let nodes = spawn_mesh(NODES, cfg).await.expect("5-node mesh spawns");
    let group = unique_group_id();

    // meet() asserts the seed's broadcast is delivered to every other node — full
    // mesh convergence, not just pairwise-with-seed.
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("all 5 nodes converge on the shared group");

    // Per-node oracle (no shared-storage reliance): every node has the group's topic.
    for node in &nodes {
        assert!(
            node.has_group(&group),
            "{} should hold the group topic after converging",
            node.name
        );
    }
}
