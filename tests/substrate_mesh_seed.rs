//! Substrate tests for MESH_HARDENING §2 (mesh formation via the ONE seeding pipeline) and §3
//! (the persistent presence roster). In-process, loopback, bounded waits, asserting on real state
//! (`peers_per_group` neighbor sets + `storage::group_members`), never on log lines.
//!
//! ## Why these prove the seeding fix, honestly
//!
//! Every node here is spawned with a UNIQUE discovery key (and `MdnsOnly` + `RelayPolicy::Disabled`,
//! i.e. NO relay, NO bootstrap, NO cloud). Distinct discovery keys mean the two nodes never share a
//! discovery gossip topic, so the discovery `groups_exchange → JoinPeersToTopic` peer-intro path can
//! NEVER populate `peers_per_group`. After that, the ONLY way a peer can appear in a node's
//! `peers_per_group` for a group is the live group-topic `NeighborUp` — which can only fire once the
//! topic has a present, resolvable peer in its bootstrap set. That seeding is exactly what §2 builds,
//! and what these tests drive through the engine's `SeedGroupPeer` command (the source-agnostic core
//! that mDNS / QR / persisted-store / bootstrap / Lens all funnel through). No `wire_addrs`: the
//! engine's pipeline makes the peer resolvable itself (`add_endpoint_info`), so the address handling
//! is under test too, not pre-staged by the harness.

mod support;

use std::time::Duration;

use cyan_backend::models::events::NetworkEvent;
use support::{
    serial, spawn_node, unique_discovery_key, unique_group_id, wait_until, DiscoveryPolicy, Node,
    NodeCfg, RelayPolicy, SYNC_TIMEOUT,
};

/// LAN-sovereign, isolated config: no relay, mDNS only, and a UNIQUE discovery key so the pair can
/// only mesh through the seeding pipeline under test (never the discovery peer-intro path).
fn iso_cfg() -> NodeCfg {
    NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        discovery_key: unique_discovery_key(),
    }
}

/// Spawn two isolated LAN nodes, both joined to `group`, and form the group topic purely via the
/// seeding pipeline: each is handed the OTHER's full `EndpointAddr` through `seed_group_peer`
/// (modelling a peer learned via mDNS / QR / the persisted store). Returns once BOTH nodes report
/// the other in their live neighbor set (`peers_in_group == 1`). Bounded by `SYNC_TIMEOUT`.
async fn mesh_via_seed(group: &str) -> Vec<Node> {
    let n0 = spawn_node("seed-0", iso_cfg()).await.expect("seed node 0 spawns");
    let n1 = spawn_node("seed-1", iso_cfg()).await.expect("seed node 1 spawns");

    n0.join_group(group, None);
    n1.join_group(group, None);

    // Each node's full resolvable address — the payload every seed source carries.
    let a0 = n0.endpoint_addr_json(SYNC_TIMEOUT).await.expect("node 0 address");
    let a1 = n1.endpoint_addr_json(SYNC_TIMEOUT).await.expect("node 1 address");

    // Seed both directions (mirrors mDNS: both peers discover each other).
    n0.seed_group_peer(group, &a1);
    n1.seed_group_peer(group, &a0);

    let g = group.to_string();
    wait_until(
        || n0.peers_in_group(&g) == 1 && n1.peers_in_group(&g) == 1,
        SYNC_TIMEOUT,
        "both seeded nodes form a group-topic neighbor",
    )
    .await
    .expect("seeded peers form NeighborUp on both ends");

    vec![n0, n1]
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — MESH FORMATION (the seeding pipeline)
// ═══════════════════════════════════════════════════════════════════════════

/// THE core bug fix: a peer learned out-of-band (here: as an mDNS-style discovered address) is seeded
/// into the group topic and a real `NeighborUp` forms on BOTH ends — with no relay, no bootstrap, no
/// cloud. Single-laptop / same-WiFi mesh. Asserted on `peers_per_group` (the FFI presence oracle).
#[tokio::test]
async fn mdns_discovered_peer_seeds_topic_and_forms_neighbor() {
    let _serial = serial().await;
    let group = unique_group_id();
    let nodes = mesh_via_seed(&group).await;

    // The honest oracle: each node has exactly its one real gossip neighbor (the other node).
    assert_eq!(nodes[0].peers_in_group(&group), 1, "node 0 sees its seeded neighbor");
    assert_eq!(nodes[1].peers_in_group(&group), 1, "node 1 sees its seeded neighbor");
    assert!(nodes[0].group_peers(&group).contains(&nodes[1].node_id), "node 0's neighbor is node 1");
    assert!(nodes[1].group_peers(&group).contains(&nodes[0].node_id), "node 1's neighbor is node 0");

    for n in nodes {
        n.shutdown().await;
    }
}

/// §2.2 — the QR/inviter source: the QR carries the inviter's FULL address (a serialized
/// `EndpointAddr`, additive to the payload). Feeding that address through the same pipeline forms the
/// neighbor on first join with no other infrastructure — what `cyan_scan_grant_qr` does when the
/// invite has `inviter_addr`. (The QR round-trip of the field itself is unit-tested in `identity::qr`.)
#[tokio::test]
async fn qr_join_forms_neighbor_via_seeded_addr() {
    let _serial = serial().await;
    let group = unique_group_id();

    let inviter = spawn_node("qr-inviter", iso_cfg()).await.expect("inviter spawns");
    let joiner = spawn_node("qr-joiner", iso_cfg()).await.expect("joiner spawns");

    inviter.join_group(&group, None);
    // This is exactly the bytes the QR's `inviter_addr` field carries: the inviter's EndpointAddr.
    let inviter_addr = inviter.endpoint_addr_json(SYNC_TIMEOUT).await.expect("inviter address");

    // Joiner scans the QR → joins, then seeds the inviter's carried address (no bootstrap/relay).
    joiner.join_group(&group, None);
    joiner.seed_group_peer(&group, &inviter_addr);

    let g = group.clone();
    wait_until(
        || joiner.peers_in_group(&g) == 1,
        SYNC_TIMEOUT,
        "joiner forms a neighbor via the inviter's QR-carried address",
    )
    .await
    .expect("QR-seeded address forms NeighborUp");
    assert!(
        joiner.group_peers(&group).contains(&inviter.node_id),
        "joiner's neighbor is the inviter"
    );

    inviter.shutdown().await;
    joiner.shutdown().await;
}

/// §2.3 — the persisted known-peers source: once a peer has been seeded, its address is saved
/// per-group in SQLite. A RETURNING device that rejoins the group with NO peer handed to it must
/// re-seed the topic from that store and re-form the mesh on its own.
#[tokio::test]
async fn persisted_peer_reseeded_on_reconnect() {
    let _serial = serial().await;
    let group = unique_group_id();

    // Host stays up for the whole scenario; it is the peer the persisted store points back to.
    let host = spawn_node("persist-host", iso_cfg()).await.expect("host spawns");
    host.join_group(&group, None);
    let host_addr = host.endpoint_addr_json(SYNC_TIMEOUT).await.expect("host address");

    // A first device joins and seeds the host → mesh forms AND the host's address is persisted
    // under this group (the seeding pipeline writes `group_known_peers`).
    let first = spawn_node("persist-first", iso_cfg()).await.expect("first device spawns");
    first.join_group(&group, None);
    first.seed_group_peer(&group, &host_addr);
    let g = group.clone();
    wait_until(
        || first.peers_in_group(&g) == 1,
        SYNC_TIMEOUT,
        "first device meshes via an explicit seed (persisting the host addr)",
    )
    .await
    .expect("first device forms the neighbor");
    first.shutdown().await;

    // A returning device rejoins the SAME group with NO peer handed to it. The persisted store must
    // re-seed the host so the mesh re-forms with zero external help.
    let returning = spawn_node("persist-returning", iso_cfg())
        .await
        .expect("returning device spawns");
    returning.join_group(&group, None); // deliberately NO seed_group_peer
    let g = group.clone();
    wait_until(
        || returning.peers_in_group(&g) >= 1,
        SYNC_TIMEOUT,
        "returning device re-seeds the host from the persisted known-peers store",
    )
    .await
    .expect("persisted peer is re-seeded on reconnect");
    assert!(
        returning.group_peers(&group).contains(&host.node_id),
        "the re-seeded neighbor is the host"
    );

    returning.shutdown().await;
    host.shutdown().await;
}

/// The §10 headline invariant: the mesh FORMS and carries data with ALL infrastructure down (relay
/// disabled, mDNS only, no bootstrap). Beyond presence, a live broadcast must actually reach the peer.
#[tokio::test]
async fn mesh_works_lan_only_no_infra() {
    let _serial = serial().await;
    let group = unique_group_id();
    let nodes = mesh_via_seed(&group).await;

    // Liveness, not just presence: a delta broadcast on node 0 reaches node 1 over the seeded mesh.
    let probe_id = format!("lan-probe-{group}");
    let probe = NetworkEvent::WhiteboardElementAdded {
        id: probe_id.clone(),
        board_id: "lan-board".to_string(),
        element_type: "probe".to_string(),
        x: 0.0,
        y: 0.0,
        width: 0.0,
        height: 0.0,
        z_index: 0,
        style_json: None,
        content_json: None,
        created_at: 0,
        updated_at: 0,
    };

    let delivered = tokio::time::timeout(SYNC_TIMEOUT, async {
        loop {
            nodes[0].broadcast(&group, probe.clone());
            let pid = probe_id.clone();
            if nodes[1]
                .wait_network(
                    move |e| matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if *id == pid),
                    Duration::from_millis(200),
                )
                .await
                .is_ok()
            {
                return;
            }
        }
    })
    .await;
    assert!(delivered.is_ok(), "a LAN-only seeded mesh delivers live data");

    for n in nodes {
        n.shutdown().await;
    }
}

/// Reconnect/heal: after a peer churns away, a returning peer re-forms the mesh through the persisted
/// store (the §5/§10 "recover on return" property at the formation layer).
#[tokio::test]
async fn mesh_recovers_after_neighbor_down_up() {
    let _serial = serial().await;
    let group = unique_group_id();

    let nodes = mesh_via_seed(&group).await;
    // Pull the plug on node 1; node 0 stays as the durable anchor.
    let mut nodes = nodes;
    let n1 = nodes.remove(1);
    let n0 = nodes.remove(0);
    n1.shutdown().await;

    // A returning peer rejoins with nothing handed to it — the persisted store re-seeds node 0 and
    // the mesh recovers (a fresh neighbor forms).
    let returning = spawn_node("recover-returning", iso_cfg())
        .await
        .expect("returning peer spawns");
    returning.join_group(&group, None);
    let g = group.clone();
    wait_until(
        || returning.peers_in_group(&g) >= 1,
        SYNC_TIMEOUT,
        "mesh recovers: returning peer re-forms a neighbor after churn",
    )
    .await
    .expect("mesh recovers after neighbor down → up");
    assert!(
        returning.group_peers(&group).contains(&n0.node_id),
        "recovered neighbor is the durable anchor node"
    );

    returning.shutdown().await;
    n0.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — PRESENCE ROSTER (persistent members + live overlay)
// ═══════════════════════════════════════════════════════════════════════════

/// A peer met over the mesh is recorded as a persistent group MEMBER on first contact. Asserted on
/// the `storage::group_members` roster (via the harness `members()` accessor, which mirrors
/// `cyan_get_group_members`). The member set for the group is the union seen across the pair.
#[tokio::test]
async fn member_recorded_on_first_contact() {
    let _serial = serial().await;
    let group = unique_group_id();
    let nodes = mesh_via_seed(&group).await;

    let g = group.clone();
    let (id0, id1) = (nodes[0].node_id.clone(), nodes[1].node_id.clone());
    // Both peers should land in the roster once they meet (recorded on NeighborUp / first gossip).
    wait_until(
        || {
            let ids: Vec<String> = nodes[0].members(&g).into_iter().map(|m| m.0).collect();
            ids.contains(&id0) || ids.contains(&id1)
        },
        SYNC_TIMEOUT,
        "the met peer is recorded in the roster",
    )
    .await
    .expect("member recorded on first contact");

    let roster_ids: Vec<String> = nodes[0].members(&group).into_iter().map(|m| m.0).collect();
    assert!(
        roster_ids.contains(&nodes[1].node_id),
        "node 1 appears in node 0's persistent roster"
    );

    for n in nodes {
        n.shutdown().await;
    }
}

/// The `online` overlay reflects the LIVE neighbor set: a member currently in `peers_per_group` is
/// reported online (green); the roster row is the persistent half, the overlay the ephemeral half.
#[tokio::test]
async fn member_online_reflects_neighbor_set() {
    let _serial = serial().await;
    let group = unique_group_id();
    let nodes = mesh_via_seed(&group).await;

    // node 1 is a live neighbor of node 0 → its roster row must be marked online.
    let online_now = nodes[0]
        .members(&group)
        .into_iter()
        .find(|m| m.0 == nodes[1].node_id)
        .map(|m| m.3) // online flag
        .unwrap_or(false);
    assert!(online_now, "a live neighbor is reported online in the roster");

    for n in nodes {
        n.shutdown().await;
    }
}

/// Durability: after a peer goes offline (its node is torn down), it REMAINS in the roster (greyed,
/// with its cached name + last-seen). The membership row is never deleted — that is the chat-style
/// "offline peers show cached names" requirement. We assert the persistent membership survives the
/// peer's departure; the live `online` flip is timing-dependent (iroh's unbounded NeighborDown over
/// loopback) and is covered by the neighbor-set overlay test above + the multi-process scaffold.
#[tokio::test]
async fn member_persists_offline_after_neighbor_down() {
    let _serial = serial().await;
    let group = unique_group_id();
    let nodes = mesh_via_seed(&group).await;

    let mut nodes = nodes;
    let n1 = nodes.remove(1);
    let n0 = nodes.remove(0);
    let gone_id = n1.node_id.clone();
    n1.shutdown().await;

    // The departed peer is still a known member of the group (persisted, available to grey out).
    let roster_ids: Vec<String> = n0.members(&group).into_iter().map(|m| m.0).collect();
    assert!(
        roster_ids.contains(&gone_id),
        "an offline peer persists in the roster after it goes away"
    );

    n0.shutdown().await;
}

/// The roster survives a full restart (it is persisted in SQLite, not just in memory). After every
/// node that formed the group is torn down, a fresh node reads the SAME persistent roster.
#[tokio::test]
async fn members_survive_restart() {
    let _serial = serial().await;
    let group = unique_group_id();
    let nodes = mesh_via_seed(&group).await;
    let id0 = nodes[0].node_id.clone();
    let id1 = nodes[1].node_id.clone();

    // Tear the mesh down entirely ("restart").
    for n in nodes {
        n.shutdown().await;
    }

    // A fresh node reads the roster from persistent storage — both original members are still there.
    let fresh = spawn_node("restart-reader", iso_cfg()).await.expect("fresh node spawns");
    let roster_ids: Vec<String> = fresh.members(&group).into_iter().map(|m| m.0).collect();
    assert!(roster_ids.contains(&id0), "member 0 survives restart in the roster");
    assert!(roster_ids.contains(&id1), "member 1 survives restart in the roster");

    fresh.shutdown().await;
}
