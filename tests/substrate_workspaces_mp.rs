//! W3 — Auto-seeded workspaces replicate to a joiner (ROUND8 §W3).
//!
//! A group is born with two workspaces — a **default** (landing) workspace and a system
//! **"Plugins"** workspace — and they must reach a cold joiner over the EXISTING
//! snapshot/digest path, with no new transfer. Done HONESTLY (mirrors the stress
//! fabric): two `cyan_node` OS processes, each with its OWN SQLite DB. The host
//! provisions the group (group record + the two seeded workspaces) BEFORE its actor
//! starts; the joiner cold-joins and snapshots. Assertions are on the joiner's own
//! `count workspaces` / `count system_workspaces` (storage), never on logs. Bounded
//! waits only. iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use multiprocess::{wire_mesh, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

const SYNC_WAIT: Duration = Duration::from_secs(60);
const CONVERGE_WAIT: Duration = Duration::from_secs(90);

/// Poll a peer's `count <kind> <group>` until it equals `expected`, or fail at the bound.
async fn converge_count(node: &mut MpNode, kind: &str, group: &str, expected: usize) -> Result<()> {
    let deadline = Instant::now() + CONVERGE_WAIT;
    loop {
        if node.count(kind, group).await? == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let got = node.count(kind, group).await?;
            return Err(anyhow!(
                "{}: {kind} did not converge to {expected} within {CONVERGE_WAIT:?} (got {got})",
                node.name
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
async fn seeded_workspaces_sync_to_joiner() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    // Host provisions the group the way the create path does: a group record + the two
    // auto-seeded workspaces (default + system Plugins). It seeds BEFORE its actor
    // starts, so the engine auto-hosts the group topic.
    let host = MpNode::spawn_with_env("host", &key, None, None, &[("PROVISION_GROUP", &group)])
        .await
        .expect("host spawns + provisions the group");
    let host_id = host.node_id.clone();
    let joiner = MpNode::spawn("joiner", &key, Some(&host_id), None)
        .await
        .expect("joiner spawns clean");

    let mut nodes = vec![host, joiner];
    wire_mesh(&mut nodes).await.expect("exchange loopback addrs");

    // Sanity: the host really did seed two workspaces, one of them the system Plugins ws.
    assert_eq!(
        nodes[0].count("workspaces", &group).await.expect("host ws count"),
        2,
        "host provisioned exactly two workspaces (default + Plugins)"
    );
    assert_eq!(
        nodes[0].count("system_workspaces", &group).await.expect("host system ws count"),
        1,
        "host provisioned exactly one system (Plugins) workspace"
    );

    nodes[1]
        .join_group(&group, Some(&host_id))
        .await
        .expect("joiner joins group");
    assert!(
        nodes[1].wait_sync(&group, SYNC_WAIT).await.expect("wait_sync"),
        "joiner completes the initial snapshot sync"
    );

    // The joiner converges to BOTH seeded workspaces — and the system flag rides along,
    // so the Plugins workspace is recognizable as system on the joiner too. No new
    // transfer was added: this is the existing snapshot/digest path.
    converge_count(&mut nodes[1], "workspaces", &group, 2)
        .await
        .expect("joiner receives both seeded workspaces via snapshot");
    converge_count(&mut nodes[1], "system_workspaces", &group, 1)
        .await
        .expect("the system (Plugins) flag replicated to the joiner");

    for n in nodes {
        n.shutdown().await;
    }
}
