//! Substrate — MESH_HARDENING §5 incremental catch-up OVER THE WIRE.
//!
//! Oracle: the engine's storage is a process-global singleton, so we cannot assert "B was
//! behind, then converged" via per-node DB content (both nodes share one DB). Instead we assert
//! on the **holder's served-snapshot metrics** — process-global counters reset at the start of
//! each (serialized) scenario — which honestly record what each holder actually put on the wire:
//! an INCREMENTAL (`since`-bounded) serve vs a FULL re-snapshot, and the row count of the last
//! serve. This proves a returning/partitioned peer pulled only the delta over real loopback
//! QUIC, not a full re-snapshot.
//!
//! A real netem partition / relay rung is the Docker rig's job (see `tests/support` docs); what
//! is honest in-process is that the catch-up MECHANISM serves the right deltas to the right peer.
//!
//! Bounded waits only. iroh 0.95. Relay disabled + mDNS (offline path).

mod support;

use cyan_backend::metrics;
use cyan_backend::models::commands::NetworkCommand;
use cyan_backend::models::core::Workspace;
use cyan_backend::storage;
use support::{meet, serial, spawn_mesh, unique_discovery_key, unique_group_id, wait_until, NodeCfg, SYNC_TIMEOUT};

fn cfg() -> NodeCfg {
    NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    }
}

/// Seed a controlled fixture (created_at=1) so a later `since` filter is deterministic; returns
/// the board id. `n` elements are stamped at `version`.
fn seed(group: &str, board_suffix: &str, version: i64, ids: &[&str]) -> String {
    let ws = format!("{group}-ws");
    let board = format!("{group}-board-{board_suffix}");
    let _ = storage::group_insert_simple(group, "Catchup", "folder.fill", "#00AEEF");
    let _ = storage::workspace_insert(&Workspace {
        id: ws.clone(),
        group_id: group.to_string(),
        name: "Main".to_string(),
        created_at: 1,
        system: false,
    });
    let _ = storage::board_insert_simple(&board, &ws, "Canvas", 1);
    for id in ids {
        let _ = storage::element_insert_simple(
            id, &board, "rectangle", 0.0, 0.0, 1.0, 1.0, 0, None, None, version, version,
        );
    }
    board
}

/// A peer returning after offline pulls ONLY the missing range over the wire: the holder serves
/// an INCREMENTAL snapshot (since-bounded) carrying exactly the new rows, NOT a full re-snapshot.
#[tokio::test]
async fn catchup_serves_incremental_over_the_wire() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();

    // Baseline that both nodes share, then form the live mesh (joiner does its full join-sync now).
    seed(&group, "base", 1, &["e-old-1", "e-old-2", "e-old-3"]);
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    // Isolate the catch-up: from here, only the explicit CatchUp should serve a snapshot.
    metrics::reset();

    // Two NEW elements authored after the (would-be) offline window, stamped at a later version.
    seed(&group, "base", 200, &["e-new-1", "e-new-2"]);

    // The returning peer (node 1) catches up from the holder (node 0) since a watermark between
    // the old (v1) and new (v200) rows — it must receive only the 2 new rows.
    nodes[1].cmd(NetworkCommand::CatchUp {
        group_id: group.clone(),
        source_peer: nodes[0].node_id.clone(),
        since: Some(100),
    });

    wait_until(
        || metrics::incremental_served() >= 1,
        SYNC_TIMEOUT,
        "holder to serve an incremental catch-up",
    )
    .await
    .expect("incremental catch-up served");

    assert_eq!(
        metrics::full_served(),
        0,
        "catch-up must NOT fall back to a full re-snapshot when a common base exists"
    );
    assert_eq!(
        metrics::rows_served_last(),
        2,
        "the holder served exactly the 2 missing rows, not the whole group"
    );
}

/// After a partition both sides reconcile bidirectionally: each peer pulls the other's missing
/// range as an incremental delta (both directions serve INCREMENTAL, neither a full snapshot),
/// so the mesh converges from the watermark instead of a full re-snapshot on either side.
#[tokio::test]
async fn partition_heals_bidirectional_converge() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();

    seed(&group, "base", 1, &["e-shared-1", "e-shared-2"]);
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");
    metrics::reset();

    // "During the partition" each side gained a row the other lacked (both land in the shared DB,
    // but the catch-up still pulls them as a since-bounded delta from each holder in turn).
    seed(&group, "base", 200, &["e-from-a", "e-from-b"]);

    // Heal: each side catches up from the other (bidirectional reconcile).
    nodes[1].cmd(NetworkCommand::CatchUp {
        group_id: group.clone(),
        source_peer: nodes[0].node_id.clone(),
        since: Some(100),
    });
    nodes[0].cmd(NetworkCommand::CatchUp {
        group_id: group.clone(),
        source_peer: nodes[1].node_id.clone(),
        since: Some(100),
    });

    // Both holders served an incremental delta over the wire → bidirectional convergence.
    wait_until(
        || metrics::incremental_served() >= 2,
        SYNC_TIMEOUT,
        "both sides to serve an incremental catch-up",
    )
    .await
    .expect("bidirectional incremental catch-up served");

    assert_eq!(
        metrics::full_served(),
        0,
        "neither side fell back to a full re-snapshot — both converged from the watermark"
    );
}

/// Wiring sanity: a CatchUp with no explicit `since` falls back to the persisted "synced as of T"
/// watermark (set by a §11 bundle import), proving the import → catch-up reconcile seam is live.
#[tokio::test]
async fn catchup_uses_import_watermark_when_since_absent() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();

    seed(&group, "base", 1, &["e-base-1"]);
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    // Simulate a prior bundle import: stamp the watermark BETWEEN the old and new rows.
    storage::group_sync_state_set(&group, 100).expect("stamp watermark");
    metrics::reset();
    seed(&group, "base", 200, &["e-after-import-1", "e-after-import-2"]);

    // CatchUp with `since: None` → engine uses the persisted watermark (100), serving only the
    // 2 rows newer than the import point.
    nodes[1].cmd(NetworkCommand::CatchUp {
        group_id: group.clone(),
        source_peer: nodes[0].node_id.clone(),
        since: None,
    });

    wait_until(
        || metrics::incremental_served() >= 1,
        SYNC_TIMEOUT,
        "holder to serve an incremental catch-up off the import watermark",
    )
    .await
    .expect("incremental catch-up served");
    assert_eq!(metrics::full_served(), 0, "watermark drove an incremental, not a full, pull");
    assert_eq!(
        metrics::rows_served_last(),
        2,
        "served exactly the rows authored after the import watermark"
    );
}
