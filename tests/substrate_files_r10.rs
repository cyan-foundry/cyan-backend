//! Substrate — files: board-scoping, unique-names/dedupe, handle resolver, delete
//! (ROUND10_FEEDBACK §F).
//!
//! Storage-level properties (F1/F2/F3) assert directly on `storage::*` (the engine DB is
//! process-global). The delete-syncs property (F4) is a two-node mesh test: the tombstone
//! must converge to the peer, asserted on the receiver's event + the (shared) tombstone.

mod support;

use cyan_backend::models::events::NetworkEvent;
use cyan_backend::storage;
use support::{meet, serial, spawn_mesh, stage_file, unique_discovery_key, unique_group_id, NodeCfg, SYNC_TIMEOUT};

fn cfg() -> NodeCfg {
    NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    }
}

/// F1: shared files persist at board level — a board-scoped file is found by its board.
#[test]
fn files_are_board_scoped() {
    support::ensure_db();
    let g = unique_group_id();
    let w = unique_group_id();
    let b = unique_group_id();
    let id = format!("{b}-f1");

    storage::file_insert_simple(&id, Some(&g), Some(&w), Some(&b), "deck.bin", "h1", 10, Some("peer"), 1)
        .expect("insert file");

    let files = storage::file_list_by_board(&b).expect("list by board");
    let got = files.iter().find(|f| f.id == id).expect("file present at its board");
    assert_eq!(got.board_id.as_deref(), Some(b.as_str()), "scoped to the board");
}

/// F2: unique names per level + dedupe. Identical content re-share dedupes to one row;
/// a same-name/different-content share is renamed so names stay unique within the level.
#[test]
fn duplicate_name_rejected_or_deduped() {
    support::ensure_db();
    let g = unique_group_id();
    let w = unique_group_id();
    let b = unique_group_id();

    let (_id1, name1) = storage::file_insert_dedup(
        &format!("{b}-a"), Some(&g), Some(&w), Some(&b), "dup.bin", "hashA", 10, "peer", 1,
    ).expect("insert 1");
    assert_eq!(name1, "dup.bin");

    // Same name, SAME content → dedupe: no new distinct file.
    let (_id2, name2) = storage::file_insert_dedup(
        &format!("{b}-a2"), Some(&g), Some(&w), Some(&b), "dup.bin", "hashA", 10, "peer", 1,
    ).expect("insert 2 (dedupe)");
    assert_eq!(name2, "dup.bin", "deduped to the existing file");
    let after_dedupe = storage::file_list_by_board(&b).expect("list");
    assert_eq!(
        after_dedupe.iter().filter(|f| f.name == "dup.bin").count(),
        1,
        "dedupe leaves a single dup.bin"
    );

    // Same name, DIFFERENT content → rename so the level stays name-unique.
    let (_id3, name3) = storage::file_insert_dedup(
        &format!("{b}-b"), Some(&g), Some(&w), Some(&b), "dup.bin", "hashB", 10, "peer", 1,
    ).expect("insert 3 (rename)");
    assert_ne!(name3, "dup.bin", "different content is renamed, not collided");

    let files = storage::file_list_by_board(&b).expect("list");
    let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
    let unique: std::collections::HashSet<&str> = names.iter().copied().collect();
    assert_eq!(unique.len(), names.len(), "all names unique within the level");
    assert_eq!(names.len(), 2, "two distinct files (one deduped, one renamed)");
}

/// R12 B3 (P1): applying an inbound file delta is idempotent at the engine — a repeat
/// collapses to ONE row. Covers both replay shapes: the SAME delta (same id) applied twice,
/// and the SAME content re-announced under a DIFFERENT id within the same board scope (the
/// dogfood "file followed by a message rendered the file twice" bug).
#[test]
fn file_delta_applied_twice_dedups_to_one_row() {
    support::ensure_db();
    let g = unique_group_id();
    let w = unique_group_id();
    let b = unique_group_id();

    // Same delta (same id) applied twice — id-PK idempotency.
    storage::file_insert(
        &format!("{b}-f"), Some(&g), Some(&w), Some(&b), "deck.bin", "hashX", 12, "peer", 1,
    )
    .expect("first apply");
    storage::file_insert(
        &format!("{b}-f"), Some(&g), Some(&w), Some(&b), "deck.bin", "hashX", 12, "peer", 1,
    )
    .expect("replay same delta");

    // Same content (hashX) re-announced under a DIFFERENT id in the same board — content-hash
    // idempotency collapses the duplicate render.
    storage::file_insert(
        &format!("{b}-f-dup"), Some(&g), Some(&w), Some(&b), "deck.bin", "hashX", 12, "peer", 2,
    )
    .expect("re-announce same content, new id");

    let rows = storage::file_list_by_board(&b).expect("list by board");
    assert_eq!(
        rows.iter().filter(|f| f.hash == "hashX").count(),
        1,
        "the same file content collapses to a single row in the board"
    );

    // A genuinely different file (different hash) in the same board still lands.
    storage::file_insert(
        &format!("{b}-g"), Some(&g), Some(&w), Some(&b), "other.bin", "hashY", 7, "peer", 3,
    )
    .expect("distinct content");
    let rows = storage::file_list_by_board(&b).expect("list by board");
    assert_eq!(rows.len(), 2, "distinct content is not suppressed by the dedup guard");
}

/// F3: a file is resolvable by its stable `group_id:workspace_id:board_id:file_name` handle,
/// and a tombstoned file is no longer resolvable.
#[test]
fn file_resolvable_by_gwbf_handle() {
    support::ensure_db();
    let g = unique_group_id();
    let w = unique_group_id();
    let b = unique_group_id();
    let id = format!("{b}-f3");

    storage::file_insert_simple(&id, Some(&g), Some(&w), Some(&b), "report.bin", "h3", 10, Some("peer"), 1)
        .expect("insert file");

    let got = storage::file_resolve_handle(&g, &w, &b, "report.bin").expect("resolvable by handle");
    assert_eq!(got.id, id, "handle resolves to the right file");

    storage::file_soft_delete(&id, 2).expect("tombstone");
    assert!(
        storage::file_resolve_handle(&g, &w, &b, "report.bin").is_none(),
        "tombstoned file is not resolvable"
    );
}

/// F4: deleting a file tombstones it locally and the tombstone SYNCS to the peer (no hard
/// delete). The receiver applies the tombstone and surfaces `FileDeleted`.
#[tokio::test]
async fn delete_tombstones_and_syncs() {
    let _serial = serial().await;
    let nodes = spawn_mesh(2, cfg()).await.expect("mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet");

    // Host stages a board-scoped file into the shared DB.
    let board = format!("{group}-board");
    let content: Vec<u8> = (0..1024).map(|i| (i % 7) as u8).collect();
    let file_id = format!("del-{}", &group[16..32]);
    stage_file(&file_id, &group, Some(&group), Some(&board), &content, &nodes[0].node_id);
    assert_eq!(storage::file_is_deleted(&file_id), Some(false), "starts live");

    // Host deletes it: gossip the soft-delete to the peer.
    nodes[0].broadcast(
        &group,
        NetworkEvent::FileDeleted { id: file_id.clone(), deleted_at: 42 },
    );

    // The peer receives the deletion…
    let want = file_id.clone();
    nodes[1]
        .wait_network(
            move |e| matches!(e, NetworkEvent::FileDeleted { id, .. } if *id == want),
            SYNC_TIMEOUT,
        )
        .await
        .expect("peer received the file deletion");

    // …and applies the tombstone (no hard delete) in the shared store.
    assert_eq!(
        storage::file_is_deleted(&file_id),
        Some(true),
        "deletion tombstoned, not hard-deleted"
    );
    let files = storage::file_list_by_board(&board).expect("list by board");
    assert!(
        !files.iter().any(|f| f.id == file_id),
        "tombstoned file is excluded from the board's active files"
    );
}
