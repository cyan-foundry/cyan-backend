//! Substrate G3/G4 — snapshot completeness + live deltas (SUBSTRATE_TEST_SPEC §3).
//!
//! Oracle: with the engine's process-global storage there is no per-node DB to assert
//! against, so we assert on the **receiver's per-node event channel** — the same
//! `SwiftEvent` stream the iOS app consumes (NOT log lines). For a live delta the
//! receiver surfaces `SwiftEvent::Network(<the event>)`; for a snapshot it surfaces the
//! `Sync*` progress events carrying the structure/content counts it received over the
//! wire. This proves "the receiver got X over the mesh" without a shared-DB false pass.
//!
//! Bounded waits only. iroh 0.95. Relay disabled + mDNS by default (offline path).

mod support;

use cyan_backend::models::events::{NetworkEvent, SwiftEvent};
use support::{
    meet, seed_group_fixture, serial, spawn_mesh, unique_discovery_key, unique_group_id,
    wait_until, wire_addrs, NodeCfg, SYNC_TIMEOUT,
};

fn cfg() -> NodeCfg {
    NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    }
}

fn element(id: &str, board: &str) -> NetworkEvent {
    NetworkEvent::WhiteboardElementAdded {
        id: id.to_string(),
        board_id: board.to_string(),
        element_type: "rectangle".to_string(),
        x: 10.0,
        y: 20.0,
        width: 100.0,
        height: 50.0,
        z_index: 1,
        style_json: Some("{\"fill\":\"#00AEEF\"}".to_string()),
        content_json: Some("{\"text\":\"hi\"}".to_string()),
        created_at: 1,
        updated_at: 1,
    }
}

/// G3: a late joiner receives the host's full current state, asserted via the joiner's
/// per-node `Sync*` progress events (structure counts, board element count, SyncComplete).
///
/// `#[ignore]` — real engine-capability finding, NOT a flaky test: the snapshot guarantee
/// is not honestly testable with N nodes in ONE process, for two structural reasons:
///   1. `storage` is a process-global singleton (`static DB: OnceLock`), so the "host" and
///      "joiner" share one DB — a late joiner trivially already has the state, and the
///      snapshot can neither be set up nor verified per-node. (The multi-process bins
///      `network_test`/`snapshot_test` exist precisely because of this.)
///   2. The host's group `TopicActor` is created via `gossip.subscribe_and_join`, which
///      blocks until a peer dials its group topic; combined with start-up group loading
///      this makes a "host seeds, then joiner syncs" ordering dead-lock in one process.
/// The snapshot QUIC protocol itself is exercised by the multi-process rig. Body kept as a
/// runnable template for that rig (per-node DBs); the live-delta path above covers G4 here.
#[ignore = "snapshot needs per-node storage; engine DB is a process-global singleton (see doc)"]
#[tokio::test]
async fn late_joiner_gets_full_snapshot() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    wire_addrs(&nodes, SYNC_TIMEOUT).await.expect("addresses wired");

    // Host joins first and creates its group topic. We wait until the topic actually
    // exists (the engine inserts the peers_per_group key) so the host is provably past
    // its start-up group-load before we seed — otherwise a seeded group would be loaded
    // at start and dead-lock on the snapshot join. THEN seed, THEN the joiner joins.
    nodes[0].join_group(&group, None);
    wait_until(
        || nodes[0].has_group(&group),
        SYNC_TIMEOUT,
        "host to create its group topic",
    )
    .await
    .expect("host topic up");

    seed_group_fixture(&group, 5, 3);

    nodes[1].join_group(&group, Some(nodes[0].node_id.clone()));

    let joiner = &nodes[1];
    let gid = group.clone();
    joiner
        .wait_for(
            move |e| matches!(e, SwiftEvent::SyncStructureReceived { group_id, workspace_count, board_count }
                if *group_id == gid && *workspace_count == 1 && *board_count == 1),
            SYNC_TIMEOUT,
        )
        .await
        .expect("joiner receives structure: 1 workspace, 1 board");
    joiner
        .wait_for(
            |e| matches!(e, SwiftEvent::SyncBoardReady { element_count, .. } if *element_count == 5),
            SYNC_TIMEOUT,
        )
        .await
        .expect("joiner receives the board with 5 elements");
    let gid = group.clone();
    joiner
        .wait_for(
            move |e| matches!(e, SwiftEvent::SyncComplete { group_id } if *group_id == gid),
            SYNC_TIMEOUT,
        )
        .await
        .expect("joiner reaches SyncComplete");
}

/// G4: a board element added on A after join appears (is received) on B.
#[tokio::test]
async fn delta_board_element_propagates() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    nodes[0].broadcast(&group, element("elem-delta-1", "board-1"));

    nodes[1]
        .wait_network(
            |e| matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if id == "elem-delta-1"),
            SYNC_TIMEOUT,
        )
        .await
        .expect("B receives the board element delta from A");
}

/// G4: a notebook cell created on A appears on B.
#[tokio::test]
async fn delta_notebook_cell_propagates() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    nodes[0].broadcast(
        &group,
        NetworkEvent::NotebookCellAdded {
            id: "cell-delta-1".to_string(),
            board_id: "board-1".to_string(),
            cell_type: "code".to_string(),
            cell_order: 0,
            content: Some("print('hi')".to_string()),
        },
    );

    nodes[1]
        .wait_network(
            |e| matches!(e, NetworkEvent::NotebookCellAdded { id, .. } if id == "cell-delta-1"),
            SYNC_TIMEOUT,
        )
        .await
        .expect("B receives the notebook cell delta from A");
}

/// G4: new workspace/board structure on A appears on B (BoardCreated is a structure delta).
#[tokio::test]
async fn delta_workspace_structure_propagates() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    nodes[0].broadcast(
        &group,
        NetworkEvent::BoardCreated {
            id: "board-new-1".to_string(),
            workspace_id: "ws-1".to_string(),
            name: "New Board".to_string(),
            created_at: 1,
        },
    );

    nodes[1]
        .wait_network(
            |e| matches!(e, NetworkEvent::BoardCreated { id, .. } if id == "board-new-1"),
            SYNC_TIMEOUT,
        )
        .await
        .expect("B receives the board-created structure delta from A");
}

/// G4: a change on A reaches BOTH B and C (multi-peer convergence).
#[tokio::test]
async fn three_node_convergence() {
    let _serial = serial().await;
    let nodes = spawn_mesh(3, cfg()).await.expect("3-node mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    nodes[0].broadcast(&group, element("elem-converge-1", "board-1"));

    for receiver in &nodes[1..] {
        receiver
            .wait_network(
                |e| matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if id == "elem-converge-1"),
                SYNC_TIMEOUT,
            )
            .await
            .unwrap_or_else(|e| panic!("{} never received A's delta: {e}", receiver.name));
    }
}
