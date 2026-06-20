//! W2 — Notes converge across peers under the anti-entropy digest (ROUND8 §W2).
//!
//! Done HONESTLY (mirrors the stress fabric): two `cyan_node` OS processes, each with
//! its OWN SQLite DB. Each peer writes notes LOCALLY ONLY (no live broadcast) — the
//! deterministic stand-in for "the live delta never reached the other peer". With
//! notes in the `group_digest` and the snapshot Metadata frame, the bounded
//! anti-entropy sweep detects the divergence and pulls a merge snapshot, so BOTH
//! peers must converge to the EXACT union of notes. Assertions are on each receiver's
//! own `count notes` (storage), never on logs. Bounded waits only. iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use multiprocess::{wire_mesh, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// Fast anti-entropy cadence so a missed delta is repaired within a bounded timeout
/// (production defaults are far slower; this changes cadence only, never behavior).
const AE_ENV: &[(&str, &str)] = &[("CYAN_AE_SWEEP_MS", "400"), ("CYAN_AE_PICK_MS", "120")];
const SYNC_WAIT: Duration = Duration::from_secs(60);
const CONVERGE_WAIT: Duration = Duration::from_secs(90);

/// Poll every peer's `count notes <group>` until ALL equal `expected`, or fail with a
/// per-peer report at the bound. Convergence to the EXACT count is the no-dupes /
/// no-loss oracle.
async fn converge_notes(nodes: &mut [MpNode], group: &str, expected: usize) -> Result<()> {
    let deadline = Instant::now() + CONVERGE_WAIT;
    loop {
        let mut all = true;
        for node in nodes.iter_mut() {
            if node.count("notes", group).await? != expected {
                all = false;
                break;
            }
        }
        if all {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let mut report = String::new();
            for node in nodes.iter_mut() {
                let c = node.count("notes", group).await?;
                report.push_str(&format!("{}={} ", node.name, c));
            }
            return Err(anyhow!(
                "notes did not converge to {expected} within {CONVERGE_WAIT:?}: [{report}]"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
async fn two_peers_converge_on_notes() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    // Host seeds the fixture (group → workspace → board) at boot; joiner cold-joins and
    // snapshots it, so both processes hold the fixture board the notes attach to.
    let host = MpNode::spawn_with_env("host", &key, None, Some(&group), AE_ENV)
        .await
        .expect("host spawns + seeds fixture");
    let host_id = host.node_id.clone();
    let joiner = MpNode::spawn_with_env("joiner", &key, Some(&host_id), None, AE_ENV)
        .await
        .expect("joiner spawns clean");

    let mut nodes = vec![host, joiner];
    wire_mesh(&mut nodes).await.expect("exchange loopback addrs");

    nodes[1]
        .join_group(&group, Some(&host_id))
        .await
        .expect("joiner joins group");
    assert!(
        nodes[1].wait_sync(&group, SYNC_WAIT).await.expect("wait_sync"),
        "joiner syncs the fixture before posting notes"
    );

    // Each peer authors NOTES_PER_PEER notes into its OWN DB only (no broadcast). Ids
    // are node-namespaced so the two sets are disjoint — the union is exactly 2×N.
    const NOTES_PER_PEER: usize = 4;
    nodes[0].post_notes(&group, NOTES_PER_PEER).await.expect("host posts notes");
    nodes[1].post_notes(&group, NOTES_PER_PEER).await.expect("joiner posts notes");

    // ONLY the anti-entropy digest+snapshot path can reconcile them. Both must reach
    // the exact union: no note lost, none duplicated.
    let expected = 2 * NOTES_PER_PEER;
    converge_notes(&mut nodes, &group, expected)
        .await
        .expect("both peers converge on the full set of notes via the digest");

    for n in nodes {
        n.shutdown().await;
    }
}
