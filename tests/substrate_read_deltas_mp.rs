//! Substrate (multi-process) — §1 `read_deltas` incremental-catch-up SERVE verb.
//!
//! `read_deltas <group> <since_cursor>` is the read-only HOLDER side of incremental catch-up
//! (SUPER_PEER_COMPLETION_SPEC §1): it returns the events of ONE group whose version is strictly
//! newer than the cursor, so a holder (the Lens `EmbeddedReplica`) can SERVE a late/returning peer
//! the delta instead of a full re-snapshot. These tests drive a real `cyan_node` process and assert
//! on its actual `deltas` JSON — honest per-process storage, no network needed (it is a local read).
//!
//! `seed_fixture` stamps every row at one `now`, so the high-water mark equals that timestamp and a
//! `since == high_water` read is provably caught-up (empty). DO NOT weaken assertions. Bounded waits
//! (per-request timeouts). iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use multiprocess::MpNode;
use support::{serial, unique_discovery_key, unique_group_id};

/// The fixture's data-row count, EXCLUDING the always-present group row that the Structure frame
/// carries (5 elements + 3 cells + 3 chats + 1 file + 1 workspace + 1 board = 14).
const FIXTURE_DELTA_ROWS: u64 = 14;

/// A holder serves the events newer than a cursor: a `since = 0` read returns the whole group's
/// rows (a returning-from-zero peer), and the high-water mark it reports is non-zero.
#[tokio::test]
async fn cyan_node_read_deltas_returns_events_since_cursor() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    // An empty node (no startup auto-host); seed the fixture via the verb so the actor loop stays
    // reachable. read_deltas is a local storage read — no peer/wiring required.
    let mut holder = MpNode::spawn("holder", &key, None, None)
        .await
        .expect("holder process spawns");
    holder.seed_fixture(&group).await.expect("seed fixture");

    let d = holder.read_deltas(&group, 0).await.expect("read_deltas since 0");
    assert_eq!(d.group_id, group, "delta is for the requested group");
    assert_eq!(
        d.count, FIXTURE_DELTA_ROWS,
        "a since=0 read returns every data row newer than epoch (the full fixture)"
    );
    assert!(
        d.high_water > 0,
        "the holder reports a real high-water cursor (max row version), got {}",
        d.high_water
    );
    assert!(
        !d.frames.is_empty(),
        "the holder serves the snapshot frames carrying those rows"
    );

    holder.shutdown().await;
}

/// A peer already at the holder's high-water mark gets an EMPTY delta — the "nothing to do, you're
/// caught up" path that keeps incremental catch-up from re-sending the whole group.
#[tokio::test]
async fn read_deltas_empty_when_caught_up() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    let mut holder = MpNode::spawn("holder", &key, None, None)
        .await
        .expect("holder process spawns");
    holder.seed_fixture(&group).await.expect("seed fixture");

    // Learn the holder's current cursor, then read AT it: every row's version == high_water, and the
    // serve is strictly-newer-than, so a caught-up reader receives zero data rows.
    let base = holder.read_deltas(&group, 0).await.expect("read_deltas since 0");
    assert_eq!(base.count, FIXTURE_DELTA_ROWS, "sanity: fixture present");

    let caught_up = holder
        .read_deltas(&group, base.high_water)
        .await
        .expect("read_deltas at high_water");
    assert_eq!(
        caught_up.count, 0,
        "a reader at the high-water mark is caught up — no rows served"
    );
    assert_eq!(
        caught_up.high_water, base.high_water,
        "the reported cursor is stable across reads (no new writes happened)"
    );

    holder.shutdown().await;
}

/// `read_deltas` is STRICTLY group-scoped: a node holding two groups serves only the requested
/// group's events and NEVER leaks another group's rows — the §6 isolation invariant on the serve path.
#[tokio::test]
async fn read_deltas_is_group_scoped() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let g1 = unique_group_id();
    let g2 = unique_group_id();

    let mut holder = MpNode::spawn("holder", &key, None, None)
        .await
        .expect("holder process spawns");
    holder.seed_fixture(&g1).await.expect("seed g1");
    holder.seed_fixture(&g2).await.expect("seed g2");

    let d1 = holder.read_deltas(&g1, 0).await.expect("read_deltas g1");
    assert_eq!(d1.group_id, g1);
    assert_eq!(
        d1.count, FIXTURE_DELTA_ROWS,
        "g1 serves exactly its own fixture rows even though g2 is also present"
    );

    // The serialized frames for g1 must not mention g2 anywhere (ids are namespaced by group id, so
    // any cross-group bleed would surface g2's id in g1's frames).
    let frames_json = serde_json::to_string(&d1.frames).expect("serialize g1 frames");
    assert!(
        !frames_json.contains(&g2),
        "g1's served delta must NOT contain any of g2's events (no cross-group bleed)"
    );

    // And the reverse: g2 serves its own rows, independent of g1.
    let d2 = holder.read_deltas(&g2, 0).await.expect("read_deltas g2");
    let frames2_json = serde_json::to_string(&d2.frames).expect("serialize g2 frames");
    assert_eq!(d2.count, FIXTURE_DELTA_ROWS, "g2 serves its own fixture rows");
    assert!(
        !frames2_json.contains(&g1),
        "g2's served delta must NOT contain any of g1's events"
    );

    holder.shutdown().await;
}
