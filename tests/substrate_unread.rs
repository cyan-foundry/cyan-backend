//! Substrate — authoritative unread model (ROUND10_FEEDBACK §N).
//!
//! The unread ledger is per-reader, idempotent by `message_id` (a message counts once,
//! ever), and rolls up board → workspace → group. These bugs from the live dogfood are
//! killed here: counts going to 2 for one message, and opening a chat incrementing.
//!
//! Storage-level oracle: the engine DB is process-global, so these assert directly on
//! `storage::unread_*`. Each test uses unique scope ids so concurrently-running tests in
//! this binary never collide on the shared `{scope_id: count}` map.

mod support;

use cyan_backend::storage;

/// Three fresh, unique scope ids (group, workspace, board) for one test.
fn scope() -> (String, String, String) {
    (
        support::unique_group_id(),
        support::unique_group_id(),
        support::unique_group_id(),
    )
}

fn count_of(map: &std::collections::HashMap<String, i64>, id: &str) -> i64 {
    map.get(id).copied().unwrap_or(0)
}

/// One incoming message → exactly +1 at board, workspace and group.
#[test]
fn message_increments_unread_once() {
    support::ensure_db();
    let (g, w, b) = scope();
    let msg = format!("{b}-m1");

    let newly = storage::unread_record(&msg, "chat", Some(&g), Some(&w), Some(&b), 1)
        .expect("record unread");
    assert!(newly, "first record is a real increment");

    let counts = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&counts, &b), 1, "board +1");
    assert_eq!(count_of(&counts, &w), 1, "workspace +1");
    assert_eq!(count_of(&counts, &g), 1, "group +1");
}

/// Re-delivering the SAME message_id (gossip echo / re-sync) never re-increments — the
/// "count seems 2 but I sent 1" bug.
#[test]
fn unread_idempotent_by_message_id() {
    support::ensure_db();
    let (g, w, b) = scope();
    let msg = format!("{b}-dup");

    assert!(storage::unread_record(&msg, "chat", Some(&g), Some(&w), Some(&b), 1).unwrap());
    // Same message id again — must be a no-op, twice over.
    assert!(!storage::unread_record(&msg, "chat", Some(&g), Some(&w), Some(&b), 1).unwrap());
    assert!(!storage::unread_record(&msg, "chat", Some(&g), Some(&w), Some(&b), 9).unwrap());

    let counts = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&counts, &b), 1, "still exactly one");
    assert_eq!(count_of(&counts, &w), 1);
    assert_eq!(count_of(&counts, &g), 1);
}

/// Opening / re-opening a chat must NOT increment — opening is a read (listing messages),
/// never a write that records unread.
#[test]
fn reopening_chat_does_not_increment() {
    support::ensure_db();
    let (g, w, b) = scope();
    let msg = format!("{b}-open");

    // A message arrives from a peer and a chat row lands (the real load path reads this).
    storage::chat_insert(&msg, &w, "hi", "peer-author", None, 1).expect("chat insert");
    storage::unread_record(&msg, "chat", Some(&g), Some(&w), Some(&b), 1).expect("record");

    // "Open" the chat several times — i.e. drive the actual read/load path. It must not
    // touch the unread ledger.
    for _ in 0..3 {
        let _ = storage::chat_list_by_workspace(&w).expect("list chats (open)");
    }

    let counts = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&counts, &b), 1, "opening never inflates the count");
    assert_eq!(count_of(&counts, &w), 1);
    assert_eq!(count_of(&counts, &g), 1);
}

/// Marking a board read clears that board and adjusts the workspace + group rollups.
#[test]
fn mark_read_clears_and_rolls_up() {
    support::ensure_db();
    let (g, w, b) = scope();
    let msg = format!("{b}-mr");

    storage::unread_record(&msg, "chat", Some(&g), Some(&w), Some(&b), 1).expect("record");
    let before = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&before, &b), 1);
    assert_eq!(count_of(&before, &w), 1);
    assert_eq!(count_of(&before, &g), 1);

    let cleared = storage::unread_mark_read(&b).expect("mark read");
    assert_eq!(cleared, 1, "one item cleared");

    let after = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&after, &b), 0, "board cleared");
    assert_eq!(count_of(&after, &w), 0, "workspace rollup dropped");
    assert_eq!(count_of(&after, &g), 0, "group rollup dropped");

    // Re-delivery after read must NOT resurrect the count (read state is sticky).
    assert!(!storage::unread_record(&msg, "chat", Some(&g), Some(&w), Some(&b), 1).unwrap());
    assert_eq!(count_of(&storage::unread_counts().unwrap(), &b), 0);
}

/// Two messages on two boards under one workspace/group: counts roll up board → workspace
/// → group, and marking one board read drops only that board's contribution.
#[test]
fn rollup_board_to_workspace_to_group() {
    support::ensure_db();
    let g = support::unique_group_id();
    let w = support::unique_group_id();
    let b1 = support::unique_group_id();
    let b2 = support::unique_group_id();

    storage::unread_record(&format!("{b1}-m"), "chat", Some(&g), Some(&w), Some(&b1), 1).unwrap();
    storage::unread_record(&format!("{b2}-m"), "chat", Some(&g), Some(&w), Some(&b2), 1).unwrap();

    let counts = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&counts, &b1), 1, "board 1");
    assert_eq!(count_of(&counts, &b2), 1, "board 2");
    assert_eq!(count_of(&counts, &w), 2, "workspace = sum of its boards");
    assert_eq!(count_of(&counts, &g), 2, "group = sum of its workspaces");

    storage::unread_mark_read(&b1).expect("mark board 1 read");

    let after = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&after, &b1), 0, "board 1 cleared");
    assert_eq!(count_of(&after, &b2), 1, "board 2 untouched");
    assert_eq!(count_of(&after, &w), 1, "workspace rollup now 1");
    assert_eq!(count_of(&after, &g), 1, "group rollup now 1");
}
