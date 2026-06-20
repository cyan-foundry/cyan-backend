//! W4 — Pin state converges across peers under the anti-entropy digest (ROUND8 §W4).
//!
//! Done HONESTLY (mirrors the W2 notes proof): two `cyan_node` OS processes, each with
//! its OWN SQLite DB. The host pins the shared fixture board into its storage LOCALLY
//! ONLY (no live broadcast) — the deterministic stand-in for "the live PinSet never
//! reached the other peer". With pin state in the `group_digest` and the snapshot
//! Metadata frame, the bounded anti-entropy sweep detects the divergence and pulls a
//! merge snapshot, so the joiner MUST converge to the same pinned state. Assertions are
//! on each receiver's own `count pins` (storage), never on logs. Bounded waits only.
//! iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use multiprocess::{wire_mesh, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// Fast anti-entropy cadence so a missed pin is repaired within a bounded timeout
/// (production defaults are far slower; this changes cadence only, never behavior).
const AE_ENV: &[(&str, &str)] = &[("CYAN_AE_SWEEP_MS", "400"), ("CYAN_AE_PICK_MS", "120")];
const SYNC_WAIT: Duration = Duration::from_secs(60);
const CONVERGE_WAIT: Duration = Duration::from_secs(90);

/// Poll every peer's `count pins <group>` until ALL equal `expected`, or fail with a
/// per-peer report at the bound. Convergence to the EXACT count is the oracle.
async fn converge_pins(nodes: &mut [MpNode], group: &str, expected: usize) -> Result<()> {
    let deadline = Instant::now() + CONVERGE_WAIT;
    loop {
        let mut all = true;
        for node in nodes.iter_mut() {
            if node.count("pins", group).await? != expected {
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
                let c = node.count("pins", group).await?;
                report.push_str(&format!("{}={} ", node.name, c));
            }
            return Err(anyhow!(
                "pins did not converge to {expected} within {CONVERGE_WAIT:?}: [{report}]"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
async fn pin_state_syncs_across_peers() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    // Host seeds the fixture (group → workspace → board) at boot; joiner cold-joins and
    // snapshots it, so both processes hold the fixture board the pin attaches to.
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
        "joiner syncs the fixture before the host pins"
    );

    // Neither peer has pinned anything yet.
    assert_eq!(nodes[0].count("pins", &group).await.expect("host pins"), 0);
    assert_eq!(nodes[1].count("pins", &group).await.expect("joiner pins"), 0);

    // Host pins the fixture board into its OWN DB only (no broadcast) — ONLY the
    // anti-entropy digest+snapshot path can carry it to the joiner.
    nodes[0].set_pin(&group, true).await.expect("host pins the fixture board");

    // Both peers must converge to the pinned state: the pin is neither lost nor
    // duplicated, exactly like a note.
    converge_pins(&mut nodes, &group, 1)
        .await
        .expect("both peers converge on the pinned workflow via the digest");

    for n in nodes {
        n.shutdown().await;
    }
}
