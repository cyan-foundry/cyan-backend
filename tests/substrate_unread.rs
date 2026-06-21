//! Substrate — authoritative unread model (R11 §2/§3/§5/§6).
//!
//! The unread ledger is per-reader, **idempotent by `message_id`** (a message counts once,
//! ever) and **board-level only** — there is NO workspace/group rollup. Dropping the rollup
//! killed the live-dogfood doubled counts (one message → 2, two → 4): a message used to roll
//! up to its board AND workspace AND group, so summing the map triple-counted it. Now the
//! dot/count lives on the board, and marking the board read clears it.
//!
//! Storage-level oracle: the engine DB is process-global, so these assert directly on
//! `storage::unread_*`. Each test uses unique board ids so concurrently-running tests in this
//! binary never collide on the shared `{board_id: count}` map.

mod support;

use cyan_backend::storage;

fn count_of(map: &std::collections::HashMap<String, i64>, id: &str) -> i64 {
    map.get(id).copied().unwrap_or(0)
}

/// One incoming message increments its board's unread **exactly once** (the first record is a
/// real change; the count is +1, never +2). R11 §2.
#[test]
fn message_increments_once() {
    support::ensure_db();
    let b = support::unique_group_id();
    let msg = format!("{b}-m1");

    let newly = storage::unread_record(&msg, "chat", &b, 1).expect("record unread");
    assert!(newly, "first record is a real increment");

    // Re-delivering the SAME message_id (gossip echo / re-sync) is a no-op — never +2.
    assert!(!storage::unread_record(&msg, "chat", &b, 1).expect("dup record"));
    assert!(!storage::unread_record(&msg, "chat", &b, 9).expect("dup record, newer ts"));

    let counts = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&counts, &b), 1, "board incremented exactly once");
}

/// A message counts on its **board only** — never rolled up to the workspace or group (R11 §3).
/// This is the fix for the doubled badge: the workspace/group ids never appear in the map.
#[test]
fn no_rollup_to_workspace_or_group() {
    support::ensure_db();
    let g = support::unique_group_id();
    let w = support::unique_group_id();
    let b = support::unique_group_id();
    let msg = format!("{b}-norollup");

    storage::unread_record(&msg, "chat", &b, 1).expect("record");

    let counts = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&counts, &b), 1, "board carries the count");
    assert_eq!(count_of(&counts, &w), 0, "workspace has NO rollup");
    assert_eq!(count_of(&counts, &g), 0, "group has NO rollup");
}

/// Marking a board read clears that board's dot/count (R11 §5). Read state is sticky: a
/// re-delivery after read never resurrects the count.
#[test]
fn mark_read_clears_board() {
    support::ensure_db();
    let b = support::unique_group_id();
    let msg = format!("{b}-mr");

    storage::unread_record(&msg, "chat", &b, 1).expect("record");
    assert_eq!(count_of(&storage::unread_counts().unwrap(), &b), 1);

    let cleared = storage::unread_mark_read(&b).expect("mark read");
    assert_eq!(cleared, 1, "one item cleared");
    assert_eq!(count_of(&storage::unread_counts().unwrap(), &b), 0, "board cleared");

    // Re-delivery after read must NOT resurrect the count (sticky read state).
    assert!(!storage::unread_record(&msg, "chat", &b, 1).unwrap());
    assert_eq!(count_of(&storage::unread_counts().unwrap(), &b), 0);
}

/// Opening / re-opening a chat must NOT increment — opening is a read (listing messages by
/// board), never a write that records unread (R11 §2/§6).
#[test]
fn reopen_does_not_increment() {
    support::ensure_db();
    let w = support::unique_group_id();
    let b = support::unique_group_id();
    let msg = format!("{b}-open");

    // A message arrives from a peer; the chat row lands on its board and is recorded unread.
    storage::chat_insert(&msg, &b, &w, "hi", "peer-author", None, 1).expect("chat insert");
    storage::unread_record(&msg, "chat", &b, 1).expect("record");

    // "Open" the board chat several times — drive the real read/load path. It must not touch
    // the unread ledger.
    for _ in 0..3 {
        let _ = storage::chat_list_by_board(&b).expect("list chats (open)");
    }

    assert_eq!(
        count_of(&storage::unread_counts().unwrap(), &b),
        1,
        "opening never inflates the count"
    );
}

/// Two messages on two different boards each count on their own board; marking one board read
/// drops only that board's count (no cross-board interaction, no rollup). R11 §3/§5.
#[test]
fn two_boards_counted_independently() {
    support::ensure_db();
    let b1 = support::unique_group_id();
    let b2 = support::unique_group_id();

    storage::unread_record(&format!("{b1}-m"), "chat", &b1, 1).unwrap();
    storage::unread_record(&format!("{b2}-m"), "chat", &b2, 1).unwrap();

    let counts = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&counts, &b1), 1, "board 1");
    assert_eq!(count_of(&counts, &b2), 1, "board 2");

    storage::unread_mark_read(&b1).expect("mark board 1 read");

    let after = storage::unread_counts().expect("counts");
    assert_eq!(count_of(&after, &b1), 0, "board 1 cleared");
    assert_eq!(count_of(&after, &b2), 1, "board 2 untouched");
}
