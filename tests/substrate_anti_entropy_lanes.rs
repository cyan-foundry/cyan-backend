//! Substrate — R12 C3: anti-entropy coverage of the convergent board-metadata lanes.
//!
//! C1/C2 made the BOARD-PIN (`board_metadata.is_pinned` / `pin_updated_at`) a convergent LWW
//! delta, and D2/E1 added the per-board WORKFLOW-STATE (`board_workflow_state`). Both ride the
//! snapshot serve/apply path already — but until C3 NEITHER was in the anti-entropy `group_digest`,
//! so a *dropped* `BoardPinned` / a deploy a peer missed could never be detected and therefore
//! never repaired: the digest hash never flipped, the sweep never pulled. These Tier-1 tests are
//! the deterministic, process-local proof of the three properties the end-to-end heal rests on —
//! DETECT (`group_digest` now flips when either lane changes — the gap C3 closes), CARRY
//! (`build_snapshot_frames` carries both lanes — the repair transport), and MERGE (the LWW upserts
//! are order-independent: a stale clock never clobbers a newer row).
//!
//! The cross-process heal itself (divergent storage → AE sweep → converge) is Tier-2
//! (`substrate_stress::dropped_board_pin_and_workflow_state_repaired_by_sweep`) — in-process nodes
//! share one process-global DB, so they can't diverge. Assertions here are on `storage::*` /
//! `group_digest` / the built frame, never log lines; deterministic seeds + explicit clocks, no RNG.

mod support;

use cyan_backend::anti_entropy::group_digest;
use cyan_backend::models::dto::WorkflowStateDTO;
use cyan_backend::models::protocol::SnapshotFrame;
use cyan_backend::snapshot::build_snapshot_frames;
use cyan_backend::storage;
use support::{ensure_db, unique_group_id};

/// Seed a minimal group → workspace → board so the digest/snapshot have a board to carry
/// metadata for. Returns `(group_id, board_id)`.
fn seed_group_board() -> (String, String) {
    ensure_db();
    let gid = unique_group_id();
    let ws = format!("{gid}-ws");
    let board = format!("{gid}-board");
    storage::group_insert_simple(&gid, "AE-lane fixture", "folder.fill", "#00AEEF").expect("storage op");
    storage::workspace_insert_simple(&ws, &gid, "Main").expect("storage op");
    storage::board_insert_simple(&board, &ws, "Canvas", 1).expect("storage op");
    (gid, board)
}

/// DETECT — the digest flips on a board-pin change and on a workflow-state change, and is
/// deterministic (identical state ⇒ identical digest). Before C3 the pin/workflow lanes were
/// invisible to the digest, so a dropped delta in either lane was undetectable → unrepairable.
#[test]
fn digest_detects_board_pin_and_workflow_state_changes() {
    let (gid, board) = seed_group_board();

    let base = group_digest(&gid);
    assert_eq!(group_digest(&gid), base, "digest is deterministic for identical state");

    // Board-pin lane: pinning must flip the hash (it didn't before C3).
    storage::board_meta_set_pinned(&board, true, 1000).expect("storage op");
    let pinned = group_digest(&gid);
    assert_ne!(pinned.1, base.1, "pinning a board must flip the digest (board-pin lane detected)");
    assert_eq!(group_digest(&gid), pinned, "digest stable after the pin");

    // A higher-clock unpin is a *different* state ⇒ a different digest (the LWW clock is versioned).
    storage::board_meta_set_pinned(&board, false, 2000).expect("storage op");
    let unpinned = group_digest(&gid);
    assert_ne!(unpinned.1, pinned.1, "unpin at a newer clock flips the digest again");

    // Workflow-state lane: deploying must flip the hash too (D2/E1 lane detected).
    let before_wf = group_digest(&gid);
    storage::workflow_state_set_deployed(&board, true, 1000).expect("storage op");
    let deployed = group_digest(&gid);
    assert_ne!(deployed.1, before_wf.1, "deploying a workflow must flip the digest (workflow-state lane)");
    assert_eq!(group_digest(&gid), deployed, "digest stable after the deploy");
}

/// CARRY — the snapshot the AE repair pulls actually carries both lanes, so once the digest
/// detects divergence the pull can heal it. (The detector must never see further than the repair.)
#[test]
fn snapshot_frame_carries_board_pin_and_workflow_state() {
    let (gid, board) = seed_group_board();
    storage::board_meta_set_pinned(&board, true, 1000).expect("storage op");
    storage::workflow_state_set_deployed(&board, /*dashboard*/ true, 1000).expect("storage op");

    let frames = build_snapshot_frames(&gid, None).expect("build full snapshot");
    let meta = frames
        .iter()
        .find_map(|f| match f {
            SnapshotFrame::Metadata { board_metadata, workflow_states, .. } => {
                Some((board_metadata, workflow_states))
            }
            _ => None,
        })
        .expect("snapshot has a Metadata frame");

    let (board_metadata, workflow_states) = meta;
    assert!(
        board_metadata.iter().any(|m| m.board_id == board && m.is_pinned && m.pin_updated_at == 1000),
        "Metadata frame carries the pinned board-metadata row (pin lane is repairable)"
    );
    assert!(
        workflow_states.iter().any(|s| s.board_id == board && s.deployed && s.updated_at == 1000),
        "Metadata frame carries the deployed workflow-state row (workflow lane is repairable)"
    );
}

/// MERGE — the board-pin LWW is order-independent: a stale-clock write never clobbers a newer
/// pin, a newer write always wins. This is the apply where an AE repair pull lands, so a repair
/// that races an old frame cannot regress a peer.
#[test]
fn board_pin_lww_is_order_independent() {
    let (_gid, board) = seed_group_board();

    storage::board_meta_set_pinned(&board, true, 1000).expect("storage op");
    assert!(storage::board_is_pinned(&board), "pinned at clock 1000");

    // Stale unpin (older clock) — must NOT clobber the newer pin.
    storage::board_meta_set_pinned(&board, false, 500).expect("storage op");
    assert!(storage::board_is_pinned(&board), "stale unpin@500 must not clobber the pin@1000");

    // Newer unpin — wins.
    storage::board_meta_set_pinned(&board, false, 2000).expect("storage op");
    assert!(!storage::board_is_pinned(&board), "newer unpin@2000 wins");

    // Re-applying the same/older state (a replayed/debounced repair) is an idempotent no-op.
    storage::board_meta_set_pinned(&board, true, 1500).expect("storage op");
    assert!(!storage::board_is_pinned(&board), "stale re-pin@1500 < 2000 is a no-op");
}

/// MERGE — the workflow-state LWW upsert (the path the snapshot/AE repair applies through) is
/// order-independent the same way: stale loses, newer wins, equal is a no-op.
#[test]
fn workflow_state_lww_is_order_independent() {
    let (_gid, board) = seed_group_board();

    storage::workflow_state_set_deployed(&board, /*dashboard*/ true, 1000).expect("storage op");
    let s = storage::workflow_state_get(&board);
    assert!(s.deployed && s.locked && s.updated_at == 1000, "deployed+locked at clock 1000");

    // Stale full-record upsert (older clock) — no change.
    let stale = WorkflowStateDTO {
        board_id: board.clone(),
        deployed: false,
        dashboard_available: false,
        locked: false,
        updated_at: 500,
    };
    assert!(!storage::workflow_state_upsert(&stale).expect("storage op"), "stale@500 upsert reports no change");
    assert!(storage::workflow_state_get(&board).deployed, "stale@500 must not clobber deployed@1000");

    // Newer full-record upsert — wins (e.g. an unlock that propagated).
    let newer = WorkflowStateDTO {
        board_id: board.clone(),
        deployed: true,
        dashboard_available: true,
        locked: false,
        updated_at: 2000,
    };
    assert!(storage::workflow_state_upsert(&newer).expect("storage op"), "newer@2000 upsert applies");
    let after = storage::workflow_state_get(&board);
    assert!(after.deployed && !after.locked && after.updated_at == 2000, "newer@2000 wins (unlocked)");

    // Equal clock — idempotent no-op (a replayed repair frame).
    assert!(!storage::workflow_state_upsert(&newer).expect("storage op"), "equal-clock re-apply is a no-op");
}
