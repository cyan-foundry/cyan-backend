//! Substrate G3 (multi-process) — `late_joiner_gets_full_snapshot`, done HONESTLY.
//!
//! The in-process harness shares ONE process-global SQLite DB across all nodes, so a
//! "late joiner" already sees the host's rows — a fake pass. This file runs the host
//! and the joiner as SEPARATE `cyan_node` OS PROCESSES, each with its OWN database, and
//! asserts on the **joiner's own** storage counts after sync. That is real per-node
//! snapshot truth: the rows can only be in the joiner's DB because they arrived over the
//! mesh. Relay is disabled (offline/LAN); the two peers dial directly over loopback.
//!
//! DO NOT weaken assertions. Bounded waits only (the child's `wait_sync` + per-request
//! timeouts). iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::Duration;

use multiprocess::{wire_pair, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// Host seeds a full group (workspace + board + 5 elements + 3 cells + 3 chats + 1
/// file-meta) in its own process/DB. A late joiner in a SEPARATE process joins and pulls
/// the snapshot; after `SyncComplete`, the joiner's OWN database must contain every row.
#[tokio::test]
async fn late_joiner_gets_full_snapshot() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    // Host process: seed the full fixture into its OWN db BEFORE the actor starts, so the
    // engine's startup auto-spawns and hosts the group topic (it then waits for a peer).
    let mut host = MpNode::spawn("host", &key, None, Some(&group))
        .await
        .expect("host process spawns + seeds fixture at startup");

    // Joiner process: a FRESH, EMPTY db (no group at startup) so its command loop is
    // reachable to process JoinGroup; bootstrapped off the host.
    let mut joiner = MpNode::spawn("joiner", &key, Some(&host.node_id), None)
        .await
        .expect("joiner process spawns clean");

    // Relay disabled ⇒ no public infra: exchange direct loopback addresses so the two
    // processes can dial each other (the cross-process analogue of the in-process
    // StaticProvider wiring).
    wire_pair(&mut host, &mut joiner)
        .await
        .expect("host/joiner exchange loopback addresses");

    // The joiner dials the host on the group topic (host is in its bootstrap-peer list and
    // its address is now known), triggering the snapshot pull.
    joiner
        .join_group(&group, Some(&host.node_id))
        .await
        .expect("joiner joins group");

    // The joiner requests and receives the snapshot; wait for completion in its process.
    let synced = joiner
        .wait_sync(&group, Duration::from_secs(60))
        .await
        .expect("wait_sync control call");
    assert!(
        synced,
        "joiner did not reach SyncComplete within the timeout — snapshot did not arrive"
    );

    // ── Honest per-node storage truth: count rows in the JOINER's OWN database. ──
    assert_eq!(
        joiner.count("workspaces", &group).await.expect("count workspaces"),
        1,
        "joiner should have the host's 1 workspace after snapshot"
    );
    assert_eq!(
        joiner.count("boards", &group).await.expect("count boards"),
        1,
        "joiner should have the host's 1 board after snapshot"
    );
    assert_eq!(
        joiner.count("elements", &group).await.expect("count elements"),
        5,
        "joiner should have all 5 board elements after snapshot"
    );
    assert_eq!(
        joiner.count("cells", &group).await.expect("count cells"),
        3,
        "joiner should have all 3 notebook cells after snapshot"
    );
    assert_eq!(
        joiner.count("chats", &group).await.expect("count chats"),
        3,
        "joiner should have all 3 chat messages after snapshot"
    );
    assert_eq!(
        joiner.count("files", &group).await.expect("count files"),
        1,
        "joiner should have the 1 file-meta record after snapshot"
    );

    // Sanity check the oracle itself: the host's own DB holds the source rows, so the
    // joiner's counts are being compared against a real, populated source (not 0 == 0).
    assert_eq!(
        host.count("elements", &group).await.expect("host count elements"),
        5,
        "host's own DB should hold the 5 seeded elements"
    );

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}
