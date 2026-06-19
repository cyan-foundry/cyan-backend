//! Substrate resilience / chaos — peer churn, lone node, rejoin, offline startup.
//! Additive coverage the first substrate pass scoped out (RESILIENCE_RUN.md). It uses
//! ONLY the public `support::` harness (plus the additive `Node::shutdown` "pull the
//! plug" helper) and the real `NetworkActor`/`storage` APIs. No engine/FFI/storage edits.
//!
//! Discipline (unchanged from the rest of the suite):
//! - Bounded `tokio::time::timeout` waits only — a hang is a FAILURE, surfaced, never an
//!   infinite wait. Each test is wrapped by the harness's bounded `wait_*`/`meet`.
//! - Assert on the **receiver's per-node** oracle (`peers_per_group`, the `SwiftEvent`
//!   stream) — never log lines, never the shared process-global DB.
//! - Never weaken an assertion. Relay disabled + mDNS by default (offline path).

mod support;

use std::time::Duration;

use cyan_backend::models::events::NetworkEvent;

use support::{
    meet, serial, spawn_mesh, spawn_node, unique_discovery_key, unique_group_id, Node, NodeCfg,
    RelayPolicy, DiscoveryPolicy, SYNC_TIMEOUT,
};

/// The offline-friendly config every resilience scenario starts from: relay disabled,
/// mDNS, a fresh per-scenario discovery key (isolates concurrently-running binaries).
fn cfg() -> NodeCfg {
    NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        discovery_key: unique_discovery_key(),
    }
}

fn chat(id: &str, scope: &str, message: &str) -> NetworkEvent {
    NetworkEvent::ChatSent {
        id: id.to_string(),
        workspace_id: scope.to_string(),
        message: message.to_string(),
        author: "author-a".to_string(),
        parent_id: None,
        timestamp: 1,
    }
}

/// Robustly deliver one delta from `sender` to `receiver` on `group`: re-broadcast until
/// `receiver` surfaces the matching `NetworkEvent`, bounded by `timeout`. Mirrors the
/// harness `meet` probe (a freshly-(re)formed gossip topic can drop its first message),
/// so this is the honest "the delta actually arrived" oracle, not a single fire-and-pray.
async fn broadcast_until_received<F>(
    sender: &Node,
    group: &str,
    event: NetworkEvent,
    receiver: &Node,
    pred: F,
    timeout: Duration,
) -> anyhow::Result<()>
where
    F: Fn(&NetworkEvent) -> bool + Copy,
{
    tokio::time::timeout(timeout, async {
        loop {
            sender.broadcast(group, event.clone());
            if receiver
                .wait_network(pred, Duration::from_millis(200))
                .await
                .is_ok()
            {
                return;
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("delta not delivered to {} within {:?}", receiver.name, timeout))
}

/// A peerless fresh node must START fine and DEGRADE gracefully: spawning it (no peers,
/// and — since the resilience suite never persists a group row — no group in the shared
/// DB to load) reaches the command loop, and firing `JoinGroup`/chat/file-request at it
/// neither panics nor crashes the actor. We do NOT assert those commands *succeed* — a
/// peerless `JoinGroup` legitimately parks on `subscribe_and_join` (the engine finding) —
/// only that the node stays alive (its `SwiftEvent` channel remains open) within a bound.
#[tokio::test]
async fn lone_node_no_peers_degrades_gracefully() {
    let _serial = serial().await;

    let node = spawn_node("lone", cfg())
        .await
        .expect("a peerless fresh node still constructs and starts");

    let group = unique_group_id();

    // None of these have a peer to act on. Each is a fire-and-forget command; they must
    // return immediately and must not panic or crash the actor.
    node.join_group(&group, None);
    node.broadcast(&group, chat("lone-chat-1", &group, "anyone there?"));
    // `source_peer` is this node's own (64-hex) id: a valid key that is not actually a
    // download source, so the engine routes/looks-up and no-ops — never a panic.
    node.request_download("lone-file-1", "deadbeefdeadbeefdeadbeef", &node.node_id);

    // Liveness oracle: the actor task is still up iff its event channel is still OPEN. A
    // short bounded wait for an event that never comes must end in a *timeout* (alive,
    // degraded) — not a "channel closed" error (the actor panicked/aborted).
    let still_alive = node.wait_for(|_| false, Duration::from_secs(2)).await;
    let err = still_alive.expect_err("a peerless node emits no event");
    assert!(
        err.to_string().contains("timeout"),
        "peerless node must stay alive (timeout), not crash its event channel: {err}"
    );
}

/// Peer churn: 3 nodes meet, one is "unplugged" (`shutdown`), and the surviving two keep
/// working — they still meet and a fresh chat delta from one reaches the other. The
/// dropped node is a non-seed leaf (node-2); the seed↔node-1 topic edge that carries the
/// assertion is untouched by its departure.
#[tokio::test]
async fn peer_drops_others_keep_working() {
    let _serial = serial().await;

    let mut nodes = spawn_mesh(3, cfg()).await.expect("3-node mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("all three nodes meet");

    // Pull the plug on node-2 (a leaf).
    let victim = nodes.pop().expect("three nodes were spawned");
    let victim_name = victim.name.clone();
    victim.shutdown().await;

    // The survivors must still be a working mesh: a fresh chat from node-0 reaches node-1.
    broadcast_until_received(
        &nodes[0],
        &group,
        chat("post-drop-chat-1", &group, "still here?"),
        &nodes[1],
        |e| matches!(e, NetworkEvent::ChatSent { id, .. } if id == "post-drop-chat-1"),
        SYNC_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| panic!("survivors must keep working after {victim_name} dropped: {e}"));
}

/// The last remaining peer stays functional after its only neighbour leaves: 2 nodes
/// meet, one is unplugged, and the survivor (a) keeps its local group topic (state
/// intact) and (b) still processes commands without crashing — a broadcast on the now
/// peerless topic does not error, and the actor stays alive (its `SwiftEvent` channel
/// remains open). This is exactly the spec's bar ("broadcast doesn't error, local state
/// intact"); no new peer is introduced, so nothing depends on a fresh gossip join.
#[tokio::test]
async fn last_remaining_peer_still_functional() {
    let _serial = serial().await;

    let mut nodes = spawn_mesh(2, cfg()).await.expect("2-node mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("both nodes meet");

    // Unplug the only peer.
    let victim = nodes.pop().expect("two nodes were spawned");
    victim.shutdown().await;
    let survivor = &nodes[0];

    // (a) Local state intact: the survivor still holds the group topic.
    assert!(
        survivor.has_group(&group),
        "survivor must retain its group topic after the last peer drops"
    );

    // (b) Broadcast doesn't error and the actor stays alive: fire a delta on the (now
    // peerless) topic, then a short bounded wait for an event that never comes must end in
    // a *timeout* — proving the actor is still up (channel open), not crashed by losing
    // its only neighbour.
    survivor.broadcast(&group, chat("post-last-drop-chat", &group, "still serving"));
    let alive = survivor.wait_for(|_| false, Duration::from_secs(2)).await;
    let err = alive.expect_err("no peer remains to echo an event back");
    assert!(
        err.to_string().contains("timeout"),
        "survivor must stay alive after the last peer leaves (timeout, not channel close): {err}"
    );
}
