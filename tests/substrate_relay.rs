//! Substrate G2 ladder / G8-R / G11 — the relay path, forced for REAL with the Docker rig
//! (SUBSTRATE_TEST_SPEC §3, §8; `harness/`, `STATUS_DOCKER_RIG.md`).
//!
//! These rungs are NOT in-process: to prove traffic took the relay (and the WebSocket-only
//! relay) the peers must run on ISOLATED Docker networks with a real `iroh-relay` fixture.
//! Each rung `docker run`s two `cyan_node` containers and drives them over the same
//! stdin/stdout line protocol the in-process multi-process rig uses, then asserts on the
//! JOINER's own `storage::*` row counts (the snapshot transferred intact) — the storage
//! oracle CLAUDE.md mandates, not log scraping.
//!
//! ## Oracle (no engine meter; SUBSTRATE_TEST_SPEC §8 / run brief)
//! Two independent signals prove the rung was really used:
//!   1. TOPOLOGY (deterministic). On the relay/WS rungs the peers sit on SEPARATE Docker
//!      bridges (`mesh_a` / `mesh_b`) with NO route between them; the relay is the only
//!      node bridging both. A successful sync can therefore ONLY have crossed the relay.
//!   2. iroh CONNECTION-TYPE (corroborating). We read iroh's OWN tracing (`home is now
//!      relay …`) from each container's stderr — iroh's notion of the relay path, not a
//!      custom byte meter. (Querying `Endpoint::remote_info` directly would need a new
//!      cyan_node verb = an engine `src/**` edit, which this additive run does not make.)
//!
//! ## Gating
//! Every rung is `#[ignore]` so a plain `cargo test` stays green AND Docker-free, and each
//! returns early unless `CYAN_RIG=1`. Run them via `make -C harness test-{lan,relay,ws,offline}`
//! (which brings up the relay + networks and builds the node image first).
//!
//! iroh 0.95. Bounded waits only.

#![allow(clippy::unwrap_used)] // unwraps are inside assert_eq! assertion helpers (non-#[test] async fns), which clippy.toml's allow-unwrap-in-tests does not reach; a failed unwrap here IS the test assertion failing.

#![allow(unused)]

#[path = "support/dockernode.rs"]
mod dockernode;

use std::time::Duration;

use dockernode::{relay_url, wire_pair, DockerNode, Relay, Spec};

/// The rig is opt-in: only run when `CYAN_RIG=1` (set by the Makefile rung targets). When a
/// `--ignored` run is invoked WITHOUT the rig, skip cleanly instead of failing on no Docker.
fn rig_enabled() -> bool {
    std::env::var("CYAN_RIG").as_deref() == Ok("1")
}

/// Fixed fixture group / discovery key (containers are `--rm` with fresh DBs each run).
const GROUP: &str = "rig-group-0000-1111-2222-3333-444444444444";

/// Assert the JOINER's OWN storage holds the host's full seeded fixture after sync — i.e.
/// the snapshot transferred intact across this rung's network path. Mirrors the honest
/// per-node assertion in `substrate_snapshot_mp::late_joiner_gets_full_snapshot`.
async fn assert_snapshot_intact(joiner: &mut DockerNode, group: &str) {
    assert_eq!(joiner.count("workspaces", group).await.unwrap(), 1, "1 workspace");
    assert_eq!(joiner.count("boards", group).await.unwrap(), 1, "1 board");
    assert_eq!(joiner.count("elements", group).await.unwrap(), 5, "5 elements");
    assert_eq!(joiner.count("cells", group).await.unwrap(), 3, "3 cells");
    assert_eq!(joiner.count("chats", group).await.unwrap(), 3, "3 chats");
    assert_eq!(joiner.count("files", group).await.unwrap(), 1, "1 file-meta");
}

/// Shared driver for the two relay rungs: host on `mesh_a`, joiner on `mesh_b` (no direct
/// route), relay reachable from both. `block_udp` adds the ws-entrypoint UDP black-hole so
/// the relay must carry traffic over its HTTP/WebSocket transport.
async fn run_relay_rung(block_udp: bool) {
    let relay = relay_url();

    // Host first: seed the fixture so the engine auto-hosts the group topic. The host's
    // discovery actor uses MdnsOnly (no bootstrap), so its `subscribe_and_join` returns
    // immediately and its control+command loops are live.
    let mut host = DockerNode::spawn(Spec {
        name: "cyan-rig-peer-a",
        network: "cyan-rig_mesh_a",
        relay: Relay::Url(relay.clone()),
        discovery_key: "cyan-rig",
        bootstrap_node_id: None,
        seed_fixture_group: Some(GROUP),
        block_udp,
    })
    .await
    .expect("host container spawns + seeds fixture");

    // Wait for the host to home to the relay so its advertised addr carries the
    // `{"Relay": ...}` entry the joiner needs to dial it across the split bridge.
    let host_addr = host
        .await_relay_addr(Duration::from_secs(60))
        .await
        .expect("host homes to relay");

    // The joiner's discovery actor bootstraps off the host and BLOCKS its command loop on
    // `gossip.subscribe_and_join(discovery_topic, [host]).await` until that neighbor
    // connects. Across the split bridge the only path is the relay, so the host's relay
    // addr must be in the joiner's StaticProvider before that join can succeed. The control
    // loop (node_id/addr/add_peer) runs independently of the blocked command loop, so we
    // inject the host addr IMMEDIATELY after boot — well before the joiner homes — and the
    // gossip bootstrap join completes on a later retry once the joiner's own relay is up.
    let mut joiner = DockerNode::spawn(Spec {
        name: "cyan-rig-peer-b",
        network: "cyan-rig_mesh_b",
        relay: Relay::Url(relay.clone()),
        discovery_key: "cyan-rig",
        bootstrap_node_id: Some(&host.node_id),
        seed_fixture_group: None,
        block_udp,
    })
    .await
    .expect("joiner container spawns clean");
    joiner
        .add_peer(&host_addr)
        .await
        .expect("joiner learns host relay addr early (before discovery bootstrap retry)");

    // Once the joiner has homed, give the host the joiner's relay addr too (symmetric path
    // for the snapshot response).
    let joiner_addr = joiner
        .await_relay_addr(Duration::from_secs(60))
        .await
        .expect("joiner homes to relay");
    host.add_peer(&joiner_addr).await.expect("host learns joiner addr");

    joiner
        .join_group(GROUP, Some(&host.node_id))
        .await
        .expect("joiner joins group");

    let synced = joiner
        .wait_sync(GROUP, Duration::from_secs(120))
        .await
        .expect("wait_sync control call");
    assert!(
        synced,
        "joiner did not reach SyncComplete — snapshot did not arrive over the {} path",
        if block_udp { "WebSocket relay" } else { "relay" }
    );

    // Storage oracle: the snapshot transferred intact across the forced network path.
    assert_snapshot_intact(&mut joiner, GROUP).await;

    // Connection-type oracle (corroborates the topology): iroh's own tracing shows the
    // relay path is live on both peers.
    assert!(host.homed_to_relay(), "host iroh log should show 'home is now relay'");
    assert!(joiner.homed_to_relay(), "joiner iroh log should show 'home is now relay'");

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}

// ───────────────────────────────── LAN / direct rung ─────────────────────────────────

/// G2-LAN: both peers on ONE bridge with a direct route, relay DISABLED → the snapshot
/// syncs over direct QUIC and no relay is ever homed.
#[ignore = "Docker rig rung; run via `make -C harness test-lan` (CYAN_RIG=1)"]
#[tokio::test]
async fn lan_direct_snapshot_intact() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping Docker rig rung");
        return;
    }
    let mut host = DockerNode::spawn(Spec {
        name: "cyan-rig-peer-a",
        network: "cyan-rig_lan",
        relay: Relay::Disabled,
        discovery_key: "cyan-rig",
        bootstrap_node_id: None,
        seed_fixture_group: Some(GROUP),
        block_udp: false,
    })
    .await
    .expect("host on lan");
    let mut joiner = DockerNode::spawn(Spec {
        name: "cyan-rig-peer-b",
        network: "cyan-rig_lan",
        relay: Relay::Disabled,
        discovery_key: "cyan-rig",
        bootstrap_node_id: Some(&host.node_id),
        seed_fixture_group: None,
        block_udp: false,
    })
    .await
    .expect("joiner on lan");

    // Same bridge → direct Ip addrs are routable; no relay needed.
    wire_pair(&mut host, &mut joiner).await.expect("exchange addrs");
    joiner.join_group(GROUP, Some(&host.node_id)).await.expect("join");
    let synced = joiner.wait_sync(GROUP, Duration::from_secs(90)).await.expect("wait_sync");
    assert!(synced, "joiner did not sync over the direct LAN path");

    assert_snapshot_intact(&mut joiner, GROUP).await;
    assert!(!host.homed_to_relay(), "LAN rung must not use a relay (relay disabled)");

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}

// ─────────────────────────────── Relay-only rung (G2) ────────────────────────────────

/// G2 ladder: peers on split bridges (no direct route); the connection forms through the
/// local relay; the snapshot syncs intact. Topology forces the relay; iroh's `home is now
/// relay` corroborates.
#[ignore = "Docker rig rung; run via `make -C harness test-relay` (CYAN_RIG=1)"]
#[tokio::test]
async fn connects_via_relay_when_direct_blocked() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping Docker rig rung");
        return;
    }
    run_relay_rung(false).await;
}

// ─────────────────────────── WebSocket-only rung (G2 worst) ───────────────────────────

/// G2 worst rung: split bridges AND outbound UDP black-holed in both peers → neither direct
/// QUIC nor QUIC-to-relay can work, so iroh-relay must carry traffic over its HTTP/WebSocket
/// transport. The snapshot still syncs intact.
#[ignore = "Docker rig rung; run via `make -C harness test-ws` (CYAN_RIG=1)"]
#[tokio::test]
async fn connects_via_websocket_when_udp_fully_blocked() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping Docker rig rung");
        return;
    }
    run_relay_rung(true).await;
}

// ──────────────────────────────────── Offline rung ───────────────────────────────────

/// G9 (truly air-gapped): both peers on an `internal` (no-gateway, no-internet) bridge.
///
/// FINDING (documented red, not faked): `cyan_node` cannot boot on an `internal` Docker
/// network — `NetworkActor::new` adds `MdnsDiscovery::builder()` UNCONDITIONALLY, and the
/// mDNS service fails to create on a gateway-less bridge, so `Endpoint::bind()` returns
/// `Service 'mdns' error` and the process exits before the control loop starts. Making mDNS
/// optional (tolerate its absence) is an engine `src/**` seam, out of scope for this
/// additive run. The relayless/offline substrate property is already proven GREEN by the
/// `lan_direct_snapshot_intact` rung (peers sync with `RELAY=Disabled`) and in-process by
/// `tests/substrate_offline.rs`; the only thing this rung would add — zero internet — is
/// what the mDNS-init failure blocks. See STATUS_DOCKER_RIG.md "Follow-ups".
#[ignore = "engine inits MdnsDiscovery unconditionally → 'Service mdns error' on a gateway-less \
            internal Docker net; making mDNS optional is an engine seam (out of scope). \
            Relayless/offline sync is already green via lan_direct_snapshot_intact. See \
            STATUS_DOCKER_RIG.md."]
#[tokio::test]
async fn offline_airgap_snapshot_intact() {
    unimplemented!("blocked on optional-mDNS engine seam; see the doc comment + STATUS_DOCKER_RIG.md");
}

// ───────────────────── Still-red scaffolds: need an engine seam (documented) ─────────────────────
//
// These cannot be honestly implemented in this ADDITIVE run because the test-only
// `cyan_node` bin has no large-blob transfer verb and no relayed-byte counter, and adding
// either would be an engine `src/**` edit (out of scope here). The relay/WebSocket PATHS
// themselves are already proven green by the rungs above; what is missing is a 100 MB blob
// payload and byte-level metering. See STATUS_DOCKER_RIG.md "Follow-ups".

/// G8-R: a 100 MB file completes intact (blake3) over the relay-only path.
#[ignore = "needs a cyan_node large-blob transfer verb (upload/fetch/blake3); the fixture \
            snapshot moves metadata only. Adding the verb is an engine src edit — see \
            STATUS_DOCKER_RIG.md. The relay PATH is proven by connects_via_relay_when_direct_blocked."]
#[tokio::test]
async fn large_file_100mb_over_relay_intact() {
    unimplemented!("blocked on a cyan_node blob-transfer verb (engine src edit, out of scope)");
}

/// G8-R: the same large transfer over the WebSocket-only rung.
#[ignore = "needs a cyan_node large-blob transfer verb; WebSocket PATH is proven by \
            connects_via_websocket_when_udp_fully_blocked. See STATUS_DOCKER_RIG.md."]
#[tokio::test]
async fn large_file_over_websocket_relay_intact() {
    unimplemented!("blocked on a cyan_node blob-transfer verb (engine src edit, out of scope)");
}

/// G8-R SLA: measure MB/s on the relay rung and assert ≥ an agreed floor.
#[ignore = "needs a blob-transfer verb to push a measurable payload; see STATUS_DOCKER_RIG.md"]
#[tokio::test]
async fn relay_path_meets_relay_throughput_floor() {
    unimplemented!("blocked on a cyan_node blob-transfer verb (engine src edit, out of scope)");
}

/// G11: per-(tenant,transfer) relayed-byte counter > 0 over relay, 0 over direct — the
/// billing rail. The relay PATH is already proven by topology + iroh's connection-type; what
/// is missing is the byte METER, which is an additive engine counter (a separate task).
#[ignore = "G11 relayed-byte meter is an additive ENGINE counter (src/** seam), explicitly \
            out of scope for this run. Path-usage is already proven via iroh connection-type. \
            See STATUS_DOCKER_RIG.md 'Follow-ups'."]
#[tokio::test]
async fn relayed_bytes_are_metered() {
    unimplemented!("G11 byte meter = engine seam; out of scope (path already proven)");
}
