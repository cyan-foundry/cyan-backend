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
//!
//! Engine reality this file documents (NOT works around): a node whose only reachable
//! peers are absent blocks at `gossip.subscribe_and_join` (it awaits ≥1 neighbour, and
//! the hardcoded default bootstrap is relay-only → unreachable offline). That is why a
//! peerless node's `JoinGroup` parks its command loop, and why
//! `node_with_group_offline_startup_does_not_block` is `#[ignore]`d as the executable
//! spec for that fix — see STATUS_OVERNIGHT §"Engine finding".

mod support;

use std::time::Duration;

use cyan_backend::models::events::NetworkEvent;

use support::{
    meet, seed_group_fixture, serial, spawn_mesh, spawn_node, unique_discovery_key,
    unique_group_id, wait_until, wire_addrs, DiscoveryPolicy, Node, NodeCfg, RelayPolicy,
    SYNC_TIMEOUT,
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

/// A dropped peer's replacement rediscovers the mesh: 3 nodes meet, one is unplugged, and
/// a replacement carrying the SAME discovery key and group (bootstrapped to the original
/// seed) re-forms the group topic and is delivered to within the deadline. Content
/// re-sync is the multi-process rig's job; here we assert discovery/meet only.
#[tokio::test]
async fn dropped_peer_rejoins_and_meets_again() {
    let _serial = serial().await;

    let base = cfg();
    let mut nodes = spawn_mesh(3, base.clone()).await.expect("3-node mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("the initial three nodes meet");

    let seed_id = nodes[0].node_id.clone();

    // Drop node-2.
    let victim = nodes.pop().expect("three nodes were spawned");
    victim.shutdown().await;

    // Bring up its replacement: same discovery key (from `base`), bootstrapped to the seed.
    let replacement = spawn_node(
        "node-replacement",
        NodeCfg {
            discovery: DiscoveryPolicy::Bootstrap(seed_id.clone()),
            ..base
        },
    )
    .await
    .expect("replacement node spawns");
    nodes.push(replacement);

    // Make the replacement dialable by/to the survivors over loopback, then have it join
    // the SAME group topic seeded with the original seed.
    wire_addrs(&nodes, SYNC_TIMEOUT)
        .await
        .expect("wire the replacement to the survivors");
    nodes
        .last()
        .expect("replacement was pushed")
        .join_group(&group, Some(seed_id));

    // Rediscovery oracle: a FRESH (unique-id) delta from the seed reaches the replacement
    // over the re-formed group topic. A unique payload (not `meet`'s fixed probe) is used
    // so gossip's duplicate-suppression can't mask delivery on this already-used group.
    let replacement = nodes.last().expect("replacement was pushed");
    broadcast_until_received(
        &nodes[0],
        &group,
        chat("rejoin-delta-1", &group, "back online"),
        replacement,
        |e| matches!(e, NetworkEvent::ChatSent { id, .. } if id == "rejoin-delta-1"),
        SYNC_TIMEOUT,
    )
    .await
    .expect("the replacement rediscovers the mesh and receives a delta from the seed");
}

/// ENCODED FINDING (NOT a fix): a node that already has a group in its DB and cold-starts
/// fully offline (relay disabled, no reachable bootstrap) must start NON-BLOCKING — its
/// command loop should run so the app stays responsive. Per STATUS_OVERNIGHT §"Engine
/// finding" it does NOT: `start()` loads the persisted group and awaits
/// `gossip.subscribe_and_join`, which (with only the unreachable relay-only default
/// bootstrap as a candidate) parks until a neighbour connects — one never does offline,
/// so the command loop never runs. This test is the executable spec for that fix; it is
/// `#[ignore]`d so it does not gate the suite (and so its seeded group row never
/// contaminates other in-binary tests). Do NOT edit the engine to make it pass.
#[tokio::test]
#[ignore = "engine: offline startup blocks on unreachable default bootstrap — see STATUS_OVERNIGHT; fix is babysit"]
async fn node_with_group_offline_startup_does_not_block() {
    let _serial = serial().await;

    // Bring the process-global DB up WITHOUT a group row yet: spawning any node runs
    // `storage::init_db`. This node loads zero groups, so it is pure scaffolding (it stays
    // responsive) — its only job is to initialise the shared DB so the seed below lands.
    let _db_init = spawn_node("db-init", cfg())
        .await
        .expect("db-init node initialises the shared DB");

    // Persist a group into the (shared) DB so the NEXT cold-starting node loads it at
    // startup (`storage::group_list_ids()` → `spawn_topic_actor`). `seed_group_fixture`
    // writes a real `groups` row; `JoinGroup` does not, which is why the other tests stay
    // clean and a lone/fresh node has nothing to load.
    let group = unique_group_id();
    seed_group_fixture(&group, 1, 1);

    // Cold-start a fresh node fully offline: relay disabled, mDNS only, no reachable peer.
    // Its `start()` loads the seeded group before its command loop runs.
    let node = spawn_node("offline-cold-start", cfg())
        .await
        .expect("node constructs (start() runs on a background task)");

    // A healthy engine starts non-blocking: its command loop processes a NEW JoinGroup and
    // the topic appears within the deadline. Under the finding, startup parks on the
    // persisted group's `subscribe_and_join`, the command loop never runs, and this
    // bounded wait times out — which is exactly why the test is `#[ignore]`d.
    let probe = unique_group_id();
    node.join_group(&probe, None);
    wait_until(
        || node.has_group(&probe),
        SYNC_TIMEOUT,
        "offline cold-start node processes a command (non-blocking startup)",
    )
    .await
    .expect("a node with a group in its DB must start non-blocking when offline");
}
