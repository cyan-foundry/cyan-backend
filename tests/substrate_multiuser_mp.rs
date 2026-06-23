//! Substrate (multi-process) — LIVE invite → join → per-group snapshot, gated by a signed
//! capability grant. Done HONESTLY: host and joiner(s) are SEPARATE `cyan_node` OS processes,
//! each with its OWN SQLite database. Assertions are on the **receiver's own** storage counts,
//! never on log lines. Relay disabled (offline/LAN); peers dial directly over loopback.
//!
//! THE KEY PROPERTY (`peer_joins_with_grant_snapshots_only_that_group`): a joiner presenting a
//! valid grant for group G pulls ONLY G's state — zero rows of the host's OTHER group leak. This
//! is the union of two facts proven here: (1) the snapshot is built per-group, and (2) the
//! join-time read is gated — without/with a bad grant the joiner gets nothing.
//!
//! DO NOT weaken assertions. Bounded waits only (the child's `wait_sync` + per-request timeouts).
//! iroh 0.95.

mod support;
#[path = "support/multiprocess.rs"]
mod multiprocess;

use std::time::Duration;

use multiprocess::{wire_pair, MpNode};
use support::{serial, unique_discovery_key, unique_group_id};

/// How long a joiner waits to (NOT) reach SyncComplete on a refused snapshot. The refusal is
/// fast; this only bounds the "it never completed" assertion. Kept short so rejection tests stay
/// quick while still leaving room for the mesh to form and the request to actually be attempted.
const REJECT_WAIT: Duration = Duration::from_secs(10);
const SYNC_WAIT: Duration = Duration::from_secs(60);

/// A joiner with a valid grant for group G snapshots ONLY G. The host also holds a second,
/// unrelated group H (seeded into its own DB); after the joiner syncs G, its OWN database must
/// contain all of G's rows and ZERO rows of H. That is the no-leakage property.
#[tokio::test]
async fn peer_joins_with_grant_snapshots_only_that_group() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();      // G — the invited group
    let other = unique_group_id();      // H — the host's OTHER group, must not leak

    // Host: seed G's fixture BEFORE boot so the engine hosts G's topic. Then enforce G (turns on
    // grant gating + registers the host as G's Owner-admin so it can issue/verify grants).
    let mut host = MpNode::spawn("host", &key, None, Some(&group))
        .await
        .expect("host spawns + seeds G at startup");

    // Seed the OTHER group H into the host's DB AFTER boot (no topic needed — the joiner never
    // requests it; its presence proves the host genuinely holds H that could have leaked).
    host.seed_fixture(&other).await.expect("host seeds other group H");
    host.enforce_group(&group).await.expect("host enforces G");

    // Joiner: fresh empty DB, bootstrapped off the host.
    let mut joiner = MpNode::spawn("joiner", &key, Some(&host.node_id), None)
        .await
        .expect("joiner spawns clean");
    wire_pair(&mut host, &mut joiner).await.expect("exchange loopback addrs");

    // Host issues a Member grant for G (the QR the joiner "scanned"); joiner joins presenting it.
    let (_nonce, qr) = host.issue_grant(&group, "member", 3600).await.expect("issue grant for G");
    joiner
        .join_group_with_grant(&group, Some(&host.node_id), Some(&qr))
        .await
        .expect("joiner joins G with grant");

    let synced = joiner.wait_sync(&group, SYNC_WAIT).await.expect("wait_sync control call");
    assert!(synced, "joiner with a valid grant did not reach SyncComplete for G");

    // ── G arrived in full (receiver's own DB). ──
    assert_eq!(joiner.count("workspaces", &group).await.expect("count ws G"), 1,
        "joiner should have G's 1 workspace");
    assert_eq!(joiner.count("elements", &group).await.expect("count el G"), 5,
        "joiner should have G's 5 elements");
    assert_eq!(joiner.count("chats", &group).await.expect("count chat G"), 3,
        "joiner should have G's 3 chats");

    // ── THE KEY PROPERTY: zero leakage of the host's OTHER group H. ──
    assert_eq!(joiner.count("groups", &other).await.expect("count groups H"), 0,
        "joiner must NOT have the host's other group H");
    assert_eq!(joiner.count("workspaces", &other).await.expect("count ws H"), 0,
        "joiner must NOT have any of the host's other-group workspaces");
    assert_eq!(joiner.count("elements", &other).await.expect("count el H"), 0,
        "joiner must NOT have any of the host's other-group elements");

    // Sanity: the host really does hold H (so the 0s above mean "didn't leak", not "never existed").
    assert_eq!(host.count("workspaces", &other).await.expect("host count ws H"), 1,
        "host's own DB should hold the other group H");

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}

/// An enforced group refuses a joiner that presents NO grant: the snapshot is never served, so
/// the joiner's DB stays empty for that group.
#[tokio::test]
async fn peer_without_grant_rejected() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    let mut host = MpNode::spawn("host", &key, None, Some(&group))
        .await
        .expect("host spawns + seeds G");
    host.enforce_group(&group).await.expect("host enforces G");

    let mut joiner = MpNode::spawn("joiner", &key, Some(&host.node_id), None)
        .await
        .expect("joiner spawns clean");
    wire_pair(&mut host, &mut joiner).await.expect("exchange loopback addrs");

    // Join an ENFORCED group with no grant at all.
    joiner
        .join_group_with_grant(&group, Some(&host.node_id), None)
        .await
        .expect("joiner attempts join without grant");

    let synced = joiner.wait_sync(&group, REJECT_WAIT).await.expect("wait_sync control call");
    assert!(!synced, "an enforced group must NOT serve a snapshot to a grant-less joiner");
    assert_eq!(joiner.count("workspaces", &group).await.expect("count ws"), 0,
        "grant-less joiner must have received no rows");
    assert_eq!(joiner.count("groups", &group).await.expect("count groups"), 0,
        "grant-less joiner must have no trace of the group");

    // Oracle sanity: the host does hold the group (so 0 means refused, not empty source).
    assert_eq!(host.count("workspaces", &group).await.expect("host count ws"), 1,
        "host's own DB should hold the seeded group");

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}

/// All three invalid-grant cases are refused, and the one VALID grant is accepted:
///  - EXPIRED  grant → refused (verify Expired)
///  - REVOKED  grant → refused (verify Revoked)
///  - REPLAYED grant → first use succeeds, second use of the same QR is refused (nonce consumed)
#[tokio::test]
async fn expired_revoked_replayed_grant_rejected() {
    let _serial = serial().await;
    let key = unique_discovery_key();
    let group = unique_group_id();

    let mut host = MpNode::spawn("host", &key, None, Some(&group))
        .await
        .expect("host spawns + seeds G");
    host.enforce_group(&group).await.expect("host enforces G");

    // ── EXPIRED: ttl = -100s ⇒ already expired at issue time. ──
    let (_n_exp, qr_exp) = host.issue_grant(&group, "member", -100).await.expect("issue expired");
    let mut j_exp = MpNode::spawn("joiner-expired", &key, Some(&host.node_id), None)
        .await.expect("expired joiner spawns");
    wire_pair(&mut host, &mut j_exp).await.expect("wire expired joiner");
    j_exp.join_group_with_grant(&group, Some(&host.node_id), Some(&qr_exp))
        .await.expect("join with expired grant");
    assert!(!j_exp.wait_sync(&group, REJECT_WAIT).await.expect("wait_sync"),
        "an EXPIRED grant must not be served");
    assert_eq!(j_exp.count("workspaces", &group).await.expect("count"), 0,
        "expired-grant joiner received no rows");
    let _ = j_exp.quit().await;

    // ── REVOKED: issue a fresh valid grant, then revoke its nonce on the host. ──
    let (n_rev, qr_rev) = host.issue_grant(&group, "member", 3600).await.expect("issue to-revoke");
    host.revoke_grant(&group, &n_rev).await.expect("revoke grant");
    let mut j_rev = MpNode::spawn("joiner-revoked", &key, Some(&host.node_id), None)
        .await.expect("revoked joiner spawns");
    wire_pair(&mut host, &mut j_rev).await.expect("wire revoked joiner");
    j_rev.join_group_with_grant(&group, Some(&host.node_id), Some(&qr_rev))
        .await.expect("join with revoked grant");
    assert!(!j_rev.wait_sync(&group, REJECT_WAIT).await.expect("wait_sync"),
        "a REVOKED grant must not be served");
    assert_eq!(j_rev.count("workspaces", &group).await.expect("count"), 0,
        "revoked-grant joiner received no rows");
    let _ = j_rev.quit().await;

    // ── REPLAYED: one grant, used twice. First use SUCCEEDS; the second is refused (nonce gone). ──
    let (_n_rp, qr_rp) = host.issue_grant(&group, "member", 3600).await.expect("issue replay grant");

    let mut j_first = MpNode::spawn("joiner-first", &key, Some(&host.node_id), None)
        .await.expect("first joiner spawns");
    wire_pair(&mut host, &mut j_first).await.expect("wire first joiner");
    j_first.join_group_with_grant(&group, Some(&host.node_id), Some(&qr_rp))
        .await.expect("first join with grant");
    assert!(j_first.wait_sync(&group, SYNC_WAIT).await.expect("wait_sync"),
        "the FIRST use of a valid grant must be served");
    assert_eq!(j_first.count("workspaces", &group).await.expect("count"), 1,
        "first joiner synced G");

    // Replay the SAME QR from a different process — the nonce was consumed on first success.
    let mut j_replay = MpNode::spawn("joiner-replay", &key, Some(&host.node_id), None)
        .await.expect("replay joiner spawns");
    wire_pair(&mut host, &mut j_replay).await.expect("wire replay joiner");
    j_replay.join_group_with_grant(&group, Some(&host.node_id), Some(&qr_rp))
        .await.expect("replay join with same grant");
    assert!(!j_replay.wait_sync(&group, REJECT_WAIT).await.expect("wait_sync"),
        "a REPLAYED grant (consumed nonce) must not be served again");
    assert_eq!(j_replay.count("workspaces", &group).await.expect("count"), 0,
        "replay joiner received no rows");

    let _ = j_first.quit().await;
    let _ = j_replay.quit().await;
    let _ = host.quit().await;
}
