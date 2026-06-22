//! Mesh-hardening Tier-2 e2e — MESH_HARDENING_SPEC §6/§7 (Docker + netem), the §10 degradation
//! matrix, and the §12 acceptance smoke — forced for REAL with the Docker rig.
//!
//! Each scenario `docker run`s `cyan_node` CONTAINERS (the real snapshot+delta engine) onto the
//! isolation networks from `harness/docker-compose.yml`, drives them over the stdin/stdout line
//! protocol (`tests/support/dockernode.rs`), applies netem/chaos via host-side `docker`
//! (pause = offline, network disconnect = partition, `tc netem` = latency), and asserts on the
//! RECEIVER's OWN `storage::*` row counts — the per-node convergence oracle CLAUDE.md mandates,
//! never log scraping. Each `cyan_node` has its own SQLite DB, so a count on the receiver really
//! proves it holds the data.
//!
//! ## Roles (all `cyan_node`; see the compose header for why)
//! - **peer** — an ordinary device.
//! - **super-peer** — a `cyan_node` that stays ONLINE and holds the group: the DURABLE-REPLICA
//!   substrate role from STATUS_HEADLESS_SUPERPEER.md. The Lens-specific AI/entitlement/
//!   offline-message-hold logic lives in cyan-lens (tested there against fakes) and is OUT of this
//!   substrate rig — so the one §10 row that needs Lens's offline-hold/redeliver is `#[ignore]`d
//!   with that reason, not faked.
//! - **bootstrap** — the discovery rendezvous: the REAL `xaeroflux_bootstrap` binary as a
//!   container (`harness/Dockerfile.bootstrap`, image `cyan/bootstrap:rig`; xaeroflux is read-only,
//!   never modified). It self-publishes a signed rendezvous config and acts as the cross-network
//!   gossip relay / peer-introducer, so two `cyan_node` peers on isolated bridges find each other
//!   THROUGH it — proven live by `bootstrap_seeded_cross_net_mesh` +
//!   `discovery_via_published_config_forms_cross_net_mesh` (+ the tampered/redeploy rungs). See
//!   STATUS_BOOTSTRAP_DISCOVERY_E2E.md.
//!
//! ## Oracle split (honest)
//! - **Convergence** (does the receiver end up holding the data) is asserted here, per-node.
//! - **Incremental-ness** of catch-up (delta NOT full re-snapshot) is proven in-process by
//!   `tests/substrate_catchup.rs` against the holder's served-snapshot metrics. The Docker rung
//!   drives the same `download_snapshot_since` path end-to-end and proves the data arrives.
//!
//! ## Gating
//! Every scenario is `#[ignore]` so a plain `cargo test` stays green AND Docker-free, and each
//! returns early unless `CYAN_RIG=1` (set by `make -C harness mesh-e2e`). iroh 0.95. Bounded waits.

#![allow(clippy::unwrap_used)] // unwraps are inside assert_eq! assertion helpers (non-#[test] async fns), which clippy.toml's allow-unwrap-in-tests does not reach; a failed unwrap here IS the test assertion failing.

#![allow(unused)]

#[path = "support/dockernode.rs"]
mod dockernode;

use std::time::Duration;

use dockernode::{
    relay_url, wire_into, wire_pair, BootstrapNode, ConfigServer, DockerNode, Relay, Spec,
};

/// The bundled cold-start fallback id (mirrors `rendezvous::BUNDLED_BOOTSTRAP_NODE_ID`). A peer
/// that adopted the LIVE published config resolves a DIFFERENT id; a peer that rejected a tampered
/// config falls back to exactly this one.
const BUNDLED_BOOTSTRAP_ID: &str =
    "f992aa3b5409410b373605002a47e5521f1f2a9d10d2910544c3b37f4d6ed618";

/// The rig is opt-in: only run when `CYAN_RIG=1`. A bare `--ignored` run without the rig skips
/// cleanly rather than failing on no Docker.
fn rig_enabled() -> bool {
    std::env::var("CYAN_RIG").as_deref() == Ok("1")
}

/// Shared discovery key — all roles share one mesh (compose/Makefile use the same).
const DKEY: &str = "cyan-rig";
/// Fixed fixture group (containers are `--rm` with fresh DBs each run).
const GROUP: &str = "rig-mesh-0000-1111-2222-3333-444444444444";

const FIXTURE_LAN: &str = "cyan-rig_lan";
const MESH_A: &str = "cyan-rig_mesh_a";
const MESH_B: &str = "cyan-rig_mesh_b";

/// Generous per-scenario sync budget: first container start can be slow.
const SYNC: Duration = Duration::from_secs(120);
/// Live-propagation budget once the mesh is up.
const LIVE: Duration = Duration::from_secs(60);
/// Cross-network discovery is slower: the bootstrap must hear both peers' groups_exchange,
/// introduce them, and the gossip overlay must stabilize through the relay node before a
/// broadcast floods across the two isolated bridges. Be generous.
const CROSS_NET: Duration = Duration::from_secs(180);

// ── spawn helpers ──────────────────────────────────────────────────────────────────────────

/// A host peer on `network` that seeds the fixture (so the engine auto-hosts the group topic),
/// relay disabled, no bootstrap (MdnsOnly) — its command loop is live immediately.
async fn spawn_host(name: &str, network: &str) -> DockerNode {
    DockerNode::spawn(Spec {
        name,
        network,
        relay: Relay::Disabled,
        discovery_key: DKEY,
        bootstrap_node_id: None,
        seed_fixture_group: Some(GROUP),
        block_udp: false,
    })
    .await
    .unwrap_or_else(|e| panic!("spawn host {name}: {e}"))
}

/// A fresh joiner on `network`, empty DB (so its command loop is reachable to process JoinGroup),
/// relay disabled, optional bootstrap id.
async fn spawn_joiner(name: &str, network: &str, bootstrap: Option<&str>) -> DockerNode {
    DockerNode::spawn(Spec {
        name,
        network,
        relay: Relay::Disabled,
        discovery_key: DKEY,
        bootstrap_node_id: bootstrap,
        seed_fixture_group: None,
        block_udp: false,
    })
    .await
    .unwrap_or_else(|e| panic!("spawn joiner {name}: {e}"))
}

// ── oracle helpers (assert on the receiver's OWN storage) ────────────────────────────────────

/// The host fixture shape — a synced joiner must hold exactly this.
async fn assert_fixture_intact(j: &mut DockerNode) {
    assert_eq!(j.count("workspaces", GROUP).await.unwrap(), 1, "1 workspace");
    assert_eq!(j.count("boards", GROUP).await.unwrap(), 1, "1 board");
    assert_eq!(j.count("elements", GROUP).await.unwrap(), 5, "5 elements");
    assert_eq!(j.count("cells", GROUP).await.unwrap(), 3, "3 cells");
    assert_eq!(j.count("chats", GROUP).await.unwrap(), 3, "3 chats");
    assert_eq!(j.count("files", GROUP).await.unwrap(), 1, "1 file-meta");
}

/// Bounded poll: wait until `node.count(kind, GROUP) >= want`, asserting on real storage. Returns
/// the last observed count (so the caller can assert exact equality) or panics on timeout.
async fn wait_count_at_least(
    node: &mut DockerNode,
    kind: &str,
    want: usize,
    timeout: Duration,
    what: &str,
) -> usize {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let got = node
            .count(kind, GROUP)
            .await
            .unwrap_or_else(|e| panic!("count {kind}: {e}"));
        if got >= want {
            return got;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{what}: {kind} reached {got}, wanted >= {want} within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Form the live mesh between an already-seeded host and a fresh joiner over one shared network:
/// exchange addrs (the resolvable-addr seed pipeline — see note below), join + full-snapshot sync.
/// Returns nothing; both ends are then live neighbors.
async fn form_and_sync(host: &mut DockerNode, joiner: &mut DockerNode) {
    // NOTE on "mDNS": Docker bridges don't reliably carry multicast, so we feed each peer the
    // other's resolvable `EndpointAddr` directly — the SAME §2 seed pipeline mDNS feeds in
    // production (`add_endpoint_info` → topic seed → NeighborUp). The LAN-direct / no-relay /
    // no-bootstrap / no-internet property is real; only the mDNS TRANSPORT is substituted.
    wire_pair(host, joiner).await.expect("exchange addrs (seed pipeline)");
    joiner.join_group(GROUP, Some(&host.node_id)).await.expect("join group");
    let synced = joiner.wait_sync(GROUP, SYNC).await.expect("wait_sync call");
    assert!(synced, "joiner did not reach SyncComplete over the LAN path");
}

// ════════════════════════════════ §6 core scenarios ═════════════════════════════════════════

/// §6 / §10 (all-infra-down core): peers ONLY, no bootstrap/relay/lens, no internet. The mesh
/// FORMS (NeighborUp) and then a LIVE edit propagates over gossip — the original bug was that the
/// one-shot snapshot worked but the live mesh never formed. Proves both: snapshot + live delta.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn lan_mesh_forms_and_live_delta_no_infra() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut host = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut joiner = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&host.node_id)).await;

    form_and_sync(&mut host, &mut joiner).await;
    assert_fixture_intact(&mut joiner).await;

    // Live mesh proof: a NEW edit authored on the host must reach the joiner over live gossip
    // WITHOUT it re-joining. 5 fixture elements + 3 new = 8.
    host.post_edits(GROUP, 3).await.expect("host posts 3 live edits");
    let got = wait_count_at_least(&mut joiner, "elements", 8, LIVE, "live delta over LAN gossip").await;
    assert_eq!(got, 8, "joiner converged on exactly the fixture + live edits");

    let _ = joiner.quit().await;
    let _ = host.quit().await;
}

/// §6: split-brain heal. Both peers edit while PARTITIONED (network detached), then on heal each
/// pulls the other's missing range via §5 catch-up → BIDIRECTIONAL convergence on the receivers'
/// own storage. (Incremental-vs-full is metric-proven in `substrate_catchup`; here we prove the
/// data actually converges across a real netem partition.)
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn partition_then_both_edit_then_heal_converges_via_delta() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut a = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut b = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&a.node_id)).await;

    form_and_sync(&mut a, &mut b).await; // both at fixture (5 elements)

    // Partition B off the LAN — A and B can no longer reach each other.
    b.partition().await.expect("partition B from LAN");

    // Each side edits during the split (namespaced ids never collide).
    a.post_edits(GROUP, 3).await.expect("A edits while partitioned");
    b.post_edits(GROUP, 2).await.expect("B edits while partitioned");

    // Heal: B rejoins the LAN (new IP) → re-exchange addrs so both can dial again.
    b.heal().await.expect("re-attach B to LAN");
    wire_pair(&mut a, &mut b).await.expect("re-exchange addrs after heal");

    // Bidirectional incremental reconcile from each other's high-water mark.
    a.catch_up(GROUP, &b.node_id, None).await.expect("A catches up from B");
    b.catch_up(GROUP, &a.node_id, None).await.expect("B catches up from A");

    // Convergence on each receiver's own storage: 5 fixture + 3 (A) + 2 (B) = 10 on BOTH.
    let a_n = wait_count_at_least(&mut a, "elements", 10, SYNC, "A converges after heal").await;
    let b_n = wait_count_at_least(&mut b, "elements", 10, SYNC, "B converges after heal").await;
    assert_eq!(a_n, 10, "A holds the full union after bidirectional catch-up");
    assert_eq!(b_n, 10, "B holds the full union after bidirectional catch-up");

    let _ = b.quit().await;
    let _ = a.quit().await;
}

/// §6 / §5: a peer goes OFFLINE (paused), the closest holder authors new state, the peer returns
/// and does an incremental catch-up from that holder — converging on its own storage WITHOUT a
/// full re-snapshot path. `pause`/`unpause` keeps the container's IP stable, so no re-wire needed.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn node_offline_then_reconnect_incremental_catchup() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    // The "closest holder" is the LAN host that stays online while the peer is away.
    let mut holder = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut peer = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&holder.node_id)).await;

    form_and_sync(&mut holder, &mut peer).await; // peer at fixture (5)

    // Peer goes offline; the holder authors 4 new edits the peer misses.
    peer.pause().await.expect("peer offline (pause)");
    holder.post_edits(GROUP, 4).await.expect("holder edits while peer offline");

    // Peer returns and pulls only the missing range from the (closest, LAN) holder.
    peer.unpause().await.expect("peer back online (unpause)");
    peer.catch_up(GROUP, &holder.node_id, None)
        .await
        .expect("peer incremental catch-up from closest holder");

    let n = wait_count_at_least(&mut peer, "elements", 9, SYNC, "returning peer catches up").await;
    assert_eq!(n, 9, "peer converged to fixture + the 4 it missed");

    let _ = peer.quit().await;
    let _ = holder.quit().await;
}

// ════════════════════════════════ §10 degradation matrix ════════════════════════════════════

/// §10 (Lens/super-peer offline): with NO super-peer container at all, two ONLINE peers still
/// form the mesh and sync P2P + live. Lens is additive durability, NOT required for liveness.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn lens_down_mesh_and_sync_continue() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut a = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut b = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&a.node_id)).await;

    form_and_sync(&mut a, &mut b).await;
    assert_fixture_intact(&mut b).await;
    a.post_edits(GROUP, 2).await.expect("live edit with no Lens present");
    let n = wait_count_at_least(&mut b, "elements", 7, LIVE, "sync continues with Lens down").await;
    assert_eq!(n, 7, "mesh + sync continue fully with no super-peer");

    let _ = b.quit().await;
    let _ = a.quit().await;
}

/// §10 (bootstrap offline, super-peer up): with NO bootstrap, a LAN peer + the durable super-peer
/// holder still mesh & sync. The super-peer plays the always-on data-holder; the joiner syncs from
/// it and receives a live edit — no bootstrap involved.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn bootstrap_down_lan_and_superpeer_still_work() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    // The super-peer (durable holder) seeds + hosts the group; the joiner has no bootstrap id.
    let mut superpeer = spawn_host("cyan-rig-superpeer", FIXTURE_LAN).await;
    let mut peer = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, None).await;

    form_and_sync(&mut superpeer, &mut peer).await;
    assert_fixture_intact(&mut peer).await;
    superpeer.post_edits(GROUP, 2).await.expect("super-peer authors a live edit");
    let n = wait_count_at_least(&mut peer, "elements", 7, LIVE, "sync via super-peer, no bootstrap").await;
    assert_eq!(n, 7, "LAN + super-peer mesh & sync with bootstrap down");

    let _ = peer.quit().await;
    let _ = superpeer.quit().await;
}

/// §10 (all infra down — LAN/mDNS SOVEREIGN): no relay, no bootstrap, no super-peer. Two peers on
/// the LAN do a full snapshot sync, a live chat exchange, and each records the other in the
/// persistent roster — sovereign mode works fully.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn all_infra_down_lan_sovereign_works() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut a = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut b = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&a.node_id)).await;

    form_and_sync(&mut a, &mut b).await;
    assert_fixture_intact(&mut b).await;

    // Live chat: 3 fixture chats + 2 new from A = 5 on B.
    a.post_chat(GROUP, 2).await.expect("sovereign live chat");
    let chats = wait_count_at_least(&mut b, "chats", 5, LIVE, "sovereign chat delivers live").await;
    assert_eq!(chats, 5, "live chat works air-gapped (no infra)");

    // Presence roster: B recorded A as a member over the sovereign mesh.
    let members = wait_count_at_least(&mut b, "members", 1, LIVE, "roster records the peer").await;
    assert!(members >= 1, "the met peer is in the persistent roster");

    let _ = b.quit().await;
    let _ = a.quit().await;
}

// ════════════════════════════════ §12 acceptance smoke ══════════════════════════════════════

/// §12: group/workspace/board CRUD synced on join, then CONTINUOUS bidirectional delta sync of
/// live edits. Two peers, one shared LAN.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn acceptance_crud_and_continuous_delta_sync() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut a = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut b = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&a.node_id)).await;

    form_and_sync(&mut a, &mut b).await;
    // CRUD baseline synced (group + workspace + board).
    assert_eq!(b.count("groups", GROUP).await.unwrap(), 1, "group synced");
    assert_eq!(b.count("workspaces", GROUP).await.unwrap(), 1, "workspace synced");
    assert_eq!(b.count("boards", GROUP).await.unwrap(), 1, "board synced");

    // Continuous delta, BOTH directions: A authors 3, B authors 2; each converges to 5+3+2 = 10.
    a.post_edits(GROUP, 3).await.expect("A live edits");
    b.post_edits(GROUP, 2).await.expect("B live edits");
    let a_n = wait_count_at_least(&mut a, "elements", 10, LIVE, "A sees B's live edits").await;
    let b_n = wait_count_at_least(&mut b, "elements", 10, LIVE, "B sees A's live edits").await;
    assert_eq!((a_n, b_n), (10, 10), "continuous delta converges bidirectionally");

    let _ = b.quit().await;
    let _ = a.quit().await;
}

/// §12: chat live — messages render live (no re-open) and converge on the receiver.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn acceptance_chat_live() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut a = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut b = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&a.node_id)).await;

    form_and_sync(&mut a, &mut b).await; // 3 fixture chats synced
    a.post_chat(GROUP, 4).await.expect("A sends 4 live chats");
    let n = wait_count_at_least(&mut b, "chats", 7, LIVE, "chat delivers live").await;
    assert_eq!(n, 7, "B holds the fixture + live chats, no loss/dupes");

    let _ = b.quit().await;
    let _ = a.quit().await;
}

/// §12 / §3: presence roster — a met peer is recorded, and its row PERSISTS (greyable, cached)
/// after it goes offline. Asserts on the persistent roster, not the live neighbor set.
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn acceptance_presence_roster_persists_offline() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut a = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    let mut b = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, Some(&a.node_id)).await;

    form_and_sync(&mut a, &mut b).await;
    // A records B in the persistent roster once they meet.
    let before = wait_count_at_least(&mut a, "members", 1, LIVE, "A records B in the roster").await;

    // B goes offline — its roster row must NOT be deleted (offline peer stays greyed/cached).
    b.pause().await.expect("B offline");
    let after = a.count("members", GROUP).await.expect("roster read while B offline");
    assert_eq!(after, before, "offline peer's roster row persists (cached name, greyable)");
    assert!(after >= 1, "roster is non-empty after a peer goes offline");

    let _ = b.unpause().await;
    let _ = b.quit().await;
    let _ = a.quit().await;
}

/// §12 / §11: air-gapped import baseline. The host EXPORTS a signed, grant-scoped, invitee-
/// encrypted `.cyangroup` bundle; an air-gapped importer (never joins, never syncs over the mesh)
/// IMPORTS it and ends up holding the full baseline — the cold-start dead-end fix. No network is
/// touched by the import; the bundle travels out-of-band over the harness (like email/AirDrop/USB).
#[ignore = "Docker rig; run via `make -C harness mesh-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn acceptance_airgapped_import_baseline() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping mesh-e2e rung");
        return;
    }
    let mut host = spawn_host("cyan-rig-peer-a", FIXTURE_LAN).await;
    // The importer boots but NEVER joins the group or syncs — its baseline comes purely from the
    // bundle. We even partition it off the LAN to make "air-gapped" literal.
    let mut importer = spawn_joiner("cyan-rig-peer-b", FIXTURE_LAN, None).await;
    importer.partition().await.expect("air-gap the importer (off LAN)");

    // The importer publishes its sealed-box recipient key; the host exports a bundle sealed to it.
    let invitee_pub = importer.bundle_pubkey().await.expect("importer recipient key");
    let bundle = host.export_group(GROUP, &invitee_pub).await.expect("host exports bundle");
    assert!(bundle.contains("\"sealed\""), "bundle carries the sealed (encrypted) payload");

    // Air-gapped import: verify + decrypt + seed + stamp watermark, zero network.
    let gid = importer.import_group(&bundle).await.expect("air-gapped import");
    assert_eq!(gid, GROUP, "imported the scoped group");

    // The importer now holds the full baseline WITHOUT ever syncing over the mesh.
    assert_fixture_intact(&mut importer).await;

    let _ = importer.heal().await;
    let _ = importer.quit().await;
    let _ = host.quit().await;
}

// ════════════════════ Honest red scaffolds — blocked on real infra (NOT faked) ═══════════════

/// §6 / §10 (bootstrap-seeded cross-network mesh): two peers on ISOLATED bridges that have NO
/// pre-shared peer addresses find EACH OTHER through the real xaeroflux discovery-rendezvous
/// bootstrap, and a live edit authored on one propagates to the other.
///
/// This is the make-or-break mesh property the §5 unit tests could not reach: not just "resolve
/// the bootstrap id from a config", but the LIVE loop — a real `xaeroflux_bootstrap` container
/// self-publishes its id, two `cyan_node` peers on networks with no route between them are told
/// ONLY how to reach the bootstrap (never each other), and the bootstrap's gossip peer-introduction
/// forms the cross-net mesh.
///
/// ## Why convergence here can ONLY have gone through the bootstrap
/// - `peer-a` is on `mesh_a`, `peer-b` on `mesh_b` — separate Docker bridges with no route between
///   them, and Docker bridges don't carry multicast, so mDNS cannot bridge them either.
/// - `RELAY=Disabled` everywhere: no iroh relay is in this scenario.
/// - The ONLY address material either peer receives is the BOOTSTRAP's `EndpointAddr` (read from
///   the config it self-published) — never the other peer's. No `wire_pair`.
/// So the only common reachability is the bootstrap; if `peer-b` ends up holding `peer-a`'s live
/// edit, the bootstrap discovered them to each other and relayed it across the two islands.
///
/// ## Oracle (receiver storage, never logs)
/// Both peers seed the SAME baseline so a gossiped element APPLIES on receipt (the cross-net
/// snapshot TRANSPORT is separately proven by `substrate_relay::connects_via_relay_when_direct_blocked`;
/// this scenario isolates the DISCOVERY + live-gossip-relay property). After the mesh forms through
/// the bootstrap, `peer-a`'s 3 live edits converge on `peer-b` (5 fixture + 3 = 8), and `peer-b`'s
/// roster records `peer-a` — a peer it has no direct route to and was never handed the address of.
#[ignore = "Docker rig (xaeroflux bootstrap container); run via `make -C harness bootstrap-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn bootstrap_seeded_cross_net_mesh() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping bootstrap cross-net rung");
        return;
    }

    // 1. The REAL xaeroflux bootstrap, reachable from BOTH isolated islands. We read its LIVE
    //    node_id + dialable addrs from the signed config it self-publishes — no hardcode.
    let boot = BootstrapNode::spawn("cyan-rig-bootstrap", &[MESH_A, MESH_B], DKEY)
        .await
        .unwrap_or_else(|e| panic!("xaeroflux bootstrap up + published config: {e}"));
    assert_ne!(boot.node_id.len(), 0, "bootstrap published a node_id");
    let boot_addr = boot.endpoint_addr_json();

    // 2. Two peers on DIFFERENT, isolated networks, each pinning the LIVE bootstrap id. Both seed
    //    the same baseline (so a gossiped edit applies on receipt — see oracle note above).
    let mut a = DockerNode::spawn(Spec {
        name: "cyan-rig-peer-a",
        network: MESH_A,
        relay: Relay::Disabled,
        discovery_key: DKEY,
        bootstrap_node_id: Some(&boot.node_id),
        seed_fixture_group: Some(GROUP),
        block_udp: false,
    })
    .await
    .unwrap_or_else(|e| panic!("spawn peer-a on mesh_a: {e}"));
    let mut b = DockerNode::spawn(Spec {
        name: "cyan-rig-peer-b",
        network: MESH_B,
        relay: Relay::Disabled,
        discovery_key: DKEY,
        bootstrap_node_id: Some(&boot.node_id),
        seed_fixture_group: Some(GROUP),
        block_udp: false,
    })
    .await
    .unwrap_or_else(|e| panic!("spawn peer-b on mesh_b: {e}"));

    // 3. The ONLY address material either peer is given is how to reach the BOOTSTRAP — never each
    //    other. Discovering the other peer is the bootstrap's job.
    a.add_peer(&boot_addr).await.expect("peer-a learns the bootstrap addr");
    b.add_peer(&boot_addr).await.expect("peer-b learns the bootstrap addr");

    // 4. Mesh-formation barrier: wait until BOTH peers have a group-topic neighbor (the bootstrap)
    //    in their roster, so the relay node has both ends before we author the (one-shot) edit.
    wait_count_at_least(&mut a, "members", 1, CROSS_NET, "peer-a meets the bootstrap on the group topic").await;
    wait_count_at_least(&mut b, "members", 1, CROSS_NET, "peer-b meets the bootstrap on the group topic").await;

    // 5. peer-a authors a live edit. With no A<->B route, no relay, and mDNS not carried, the only
    //    path to peer-b is A -> bootstrap (gossip relay) -> B. 5 fixture + 3 new = 8.
    a.post_edits(GROUP, 3).await.expect("peer-a posts 3 live edits");
    let b_got = wait_count_at_least(&mut b, "elements", 8, CROSS_NET, "peer-a's edit crosses networks via the bootstrap").await;
    assert_eq!(b_got, 8, "peer-b converged on the fixture + peer-a's live edits THROUGH the bootstrap");

    // 6. The reverse direction also crosses the islands: peer-b authors 2 edits → they flood
    //    B -> bootstrap -> A. peer-a converges to its 8 + peer-b's 2 = 10; peer-b is also 10.
    //    Bidirectional convergence with the bootstrap as the SOLE bridge is the cross-net mesh.
    //    (NOTE: the persistent roster records `delivered_from` — the relay neighbor, i.e. the
    //    bootstrap — not the multi-hop author, so each peer's roster holds the bootstrap, not the
    //    far peer. The converged element rows ARE the far peer's authored content; that is the
    //    proof the data crossed the islands.)
    b.post_edits(GROUP, 2).await.expect("peer-b posts 2 live edits");
    let a_got = wait_count_at_least(&mut a, "elements", 10, CROSS_NET, "peer-b's edit crosses back via the bootstrap").await;
    let b_final = wait_count_at_least(&mut b, "elements", 10, CROSS_NET, "peer-b holds the union").await;
    assert_eq!((a_got, b_final), (10, 10), "bidirectional cross-net convergence through the bootstrap");
    assert!(b.count("members", GROUP).await.unwrap() >= 1, "peer-b holds a live group-topic neighbor (the bootstrap bridge)");

    let _ = a.quit().await;
    let _ = b.quit().await;
    drop(boot);
}

/// §5 end-to-end: a peer DISCOVERS the live bootstrap purely from the SIGNED CONFIG the bootstrap
/// self-published — no hardcoded id, no `BOOTSTRAP_NODE_ID` env — then forms the cross-net mesh.
///
/// The bootstrap writes its signed rendezvous config to a volume a `busybox httpd` ConfigServer
/// serves at a well-known URL (the rig stand-in for the object store). Each peer is given ONLY
/// `CYAN_RENDEZVOUS_URL` + the pinned org key (`CYAN_ORG_PUBKEY` == the bootstrap node_id for a
/// self-signed config) — NOT the id or discovery_key. It must fetch + verify the config to learn
/// the LIVE bootstrap id (asserted: `bootstrap_id == live`, `!= bundled hardcode`), and then the
/// same cross-net loop as `bootstrap_seeded_cross_net_mesh` runs on top of it.
#[ignore = "Docker rig (xaeroflux bootstrap + config server); run via `make -C harness bootstrap-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn discovery_via_published_config_forms_cross_net_mesh() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping published-config discovery rung");
        return;
    }
    let boot = BootstrapNode::spawn("cyan-rig-bootstrap", &[MESH_A, MESH_B], DKEY)
        .await
        .unwrap_or_else(|e| panic!("bootstrap up + published config: {e}"));
    // Serve the bootstrap's self-published config at a URL reachable from both islands.
    let cfg = ConfigServer::spawn("cyan-rig-config", &[MESH_A, MESH_B])
        .await
        .unwrap_or_else(|e| panic!("config server up: {e}"));
    let boot_addr = boot.endpoint_addr_json();

    // Peers learn EVERYTHING discovery-related (bootstrap id + discovery_key) from the verified
    // config: no BOOTSTRAP_NODE_ID, empty DISCOVERY_KEY env. RELAY=Disabled (sovereign gossip via
    // the bootstrap, not a relay). The org key pins the bootstrap node_id (self-signed config).
    let env: [(&str, &str); 2] = [
        ("CYAN_RENDEZVOUS_URL", cfg.url.as_str()),
        ("CYAN_ORG_PUBKEY", boot.node_id.as_str()),
    ];
    let cfg_peer = |name: &'static str, net: &'static str| Spec {
        name,
        network: net,
        relay: Relay::Disabled,
        discovery_key: "", // ← intentionally empty: resolved from the verified config
        bootstrap_node_id: None, // ← intentionally absent: resolved from the verified config
        seed_fixture_group: Some(GROUP),
        block_udp: false,
    };
    let mut a = DockerNode::spawn_with_env(cfg_peer("cyan-rig-peer-a", MESH_A), &env)
        .await
        .unwrap_or_else(|e| panic!("spawn peer-a (config-driven): {e}"));
    let mut b = DockerNode::spawn_with_env(cfg_peer("cyan-rig-peer-b", MESH_B), &env)
        .await
        .unwrap_or_else(|e| panic!("spawn peer-b (config-driven): {e}"));

    // The peers adopted the LIVE bootstrap id FROM THE CONFIG — not the bundled hardcode.
    assert_eq!(a.bootstrap_id().await.unwrap(), boot.node_id, "peer-a pinned the LIVE bootstrap from the published config");
    assert_eq!(b.bootstrap_id().await.unwrap(), boot.node_id, "peer-b pinned the LIVE bootstrap from the published config");
    assert_ne!(boot.node_id, BUNDLED_BOOTSTRAP_ID, "the live id is NOT the bundled hardcode (no retune)");

    // Feed each peer the bootstrap's dialable addr (the config's `addr` field; the engine's
    // addr-seed path), never each other's, then run the cross-net loop.
    a.add_peer(&boot_addr).await.expect("peer-a learns the bootstrap addr");
    b.add_peer(&boot_addr).await.expect("peer-b learns the bootstrap addr");
    wait_count_at_least(&mut a, "members", 1, CROSS_NET, "peer-a meets the bootstrap").await;
    wait_count_at_least(&mut b, "members", 1, CROSS_NET, "peer-b meets the bootstrap").await;

    a.post_edits(GROUP, 3).await.expect("peer-a posts 3 live edits");
    let got = wait_count_at_least(&mut b, "elements", 8, CROSS_NET, "live edit crosses via the config-discovered bootstrap").await;
    assert_eq!(got, 8, "peer-b converged via a bootstrap discovered purely from the published config");

    let _ = a.quit().await;
    let _ = b.quit().await;
    drop(cfg);
    drop(boot);
}

/// §5 negative: a TAMPERED published config (signature no longer covers the bytes) is REJECTED — a
/// peer fetching it falls back to the bundled bootstrap and adopts NO false bootstrap id.
///
/// Positive oracle (not a "prove a negative" timeout): we assert the peer's RESOLVED bootstrap id
/// equals the bundled fallback — it did not adopt the tampered config's id.
#[ignore = "Docker rig (xaeroflux bootstrap + config server); run via `make -C harness bootstrap-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn tampered_published_config_rejected_peer_uses_fallback() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping tampered-config rung");
        return;
    }
    let boot = BootstrapNode::spawn("cyan-rig-bootstrap", &[MESH_A, MESH_B], DKEY)
        .await
        .unwrap_or_else(|e| panic!("bootstrap up: {e}"));
    let cfg = ConfigServer::spawn("cyan-rig-config", &[MESH_A, MESH_B])
        .await
        .unwrap_or_else(|e| panic!("config server up: {e}"));
    // Corrupt the served config BEFORE the peer fetches it.
    boot.tamper_served_config().await.expect("tamper the served config");

    let env: [(&str, &str); 2] = [
        ("CYAN_RENDEZVOUS_URL", cfg.url.as_str()),
        ("CYAN_ORG_PUBKEY", boot.node_id.as_str()),
    ];
    let mut peer = DockerNode::spawn_with_env(
        Spec {
            name: "cyan-rig-peer-a",
            network: MESH_A,
            relay: Relay::Disabled,
            discovery_key: DKEY,
            bootstrap_node_id: None,
            seed_fixture_group: None,
            block_udp: false,
        },
        &env,
    )
    .await
    .unwrap_or_else(|e| panic!("spawn peer (tampered config): {e}"));

    let resolved = peer.bootstrap_id().await.expect("resolved bootstrap id");
    assert_eq!(resolved, BUNDLED_BOOTSTRAP_ID, "tampered config rejected → bundled fallback, no false bootstrap");
    assert_ne!(resolved, boot.node_id, "the live (correctly-signed) id was NOT adopted from a tampered doc");

    let _ = peer.quit().await;
    drop(cfg);
    drop(boot);
}

/// §5 redeploy: the bootstrap is REDEPLOYED with a fresh identity (rotated `node.key`) and
/// republishes; a FRESH peer (TOFU, no pinned key) picks up the NEW id from the same URL with ZERO
/// app/env change — the whole point of §5 (no per-deploy retune).
#[ignore = "Docker rig (xaeroflux bootstrap + config server); run via `make -C harness bootstrap-e2e` (CYAN_RIG=1)"]
#[tokio::test]
async fn bootstrap_redeploy_new_id_picked_up_no_app_change() {
    if !rig_enabled() {
        eprintln!("CYAN_RIG!=1 — skipping redeploy rung");
        return;
    }
    let mut boot = BootstrapNode::spawn("cyan-rig-bootstrap", &[MESH_A, MESH_B], DKEY)
        .await
        .unwrap_or_else(|e| panic!("bootstrap up: {e}"));
    let cfg = ConfigServer::spawn("cyan-rig-config", &[MESH_A, MESH_B])
        .await
        .unwrap_or_else(|e| panic!("config server up: {e}"));
    let first_id = boot.node_id.clone();

    // TOFU mode (no CYAN_ORG_PUBKEY) — the trust model that lets the id rotate without an app retune.
    let env_for = |url: &str| -> Vec<(String, String)> {
        vec![("CYAN_RENDEZVOUS_URL".to_string(), url.to_string())]
    };
    let env0 = env_for(&cfg.url);
    let env0r: Vec<(&str, &str)> = env0.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let mut before = DockerNode::spawn_with_env(
        Spec { name: "cyan-rig-peer-a", network: MESH_A, relay: Relay::Disabled,
               discovery_key: DKEY, bootstrap_node_id: None, seed_fixture_group: None, block_udp: false },
        &env0r,
    )
    .await
    .unwrap_or_else(|e| panic!("spawn pre-redeploy peer: {e}"));
    assert_eq!(before.bootstrap_id().await.unwrap(), first_id, "fresh peer adopts the FIRST live id");
    let _ = before.quit().await;

    // Redeploy with a rotated identity; the same ConfigServer/URL now serves the NEW config.
    boot.redeploy(&[MESH_A, MESH_B], DKEY).await.expect("redeploy bootstrap with new id");
    assert_ne!(boot.node_id, first_id, "redeploy rotated the bootstrap node_id");

    // A FRESH peer, SAME URL, NO env change → discovers the NEW id (no per-deploy retune).
    let env1 = env_for(&cfg.url);
    let env1r: Vec<(&str, &str)> = env1.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let mut after = DockerNode::spawn_with_env(
        Spec { name: "cyan-rig-peer-b", network: MESH_B, relay: Relay::Disabled,
               discovery_key: DKEY, bootstrap_node_id: None, seed_fixture_group: None, block_udp: false },
        &env1r,
    )
    .await
    .unwrap_or_else(|e| panic!("spawn post-redeploy peer: {e}"));
    let picked = after.bootstrap_id().await.unwrap();
    assert_eq!(picked, boot.node_id, "fresh peer picked up the REDEPLOYED id from the same URL");
    assert_ne!(picked, first_id, "it is the new id, not the pre-redeploy one");
    assert_ne!(picked, BUNDLED_BOOTSTRAP_ID, "and not the bundled hardcode");

    let _ = after.quit().await;
    drop(cfg);
    drop(boot);
}

/// §10 (a peer is offline; its messages are held and delivered on return): a message addressed to
/// an OFFLINE peer is held by the super-peer and delivered when the peer returns.
///
/// RED (honest): hold-for-offline-peer + redeliver-on-return is the Lens SUPER-PEER's
/// `hold_message`/`deliver_on_reconnect` logic — it lives in cyan-lens (`src/superpeer.rs`) and is
/// tested THERE against fakes (`superpeer_holds_then_delivers_offline_message`). It is not a
/// runnable real binary, and `cyan_node`'s engine has only the §4 content-addressed `mesh_hold`
/// SEAM (persist outgoing broadcasts) — NOT a wire protocol that targets a specific returning peer
/// and replays its held messages. Driving this end-to-end needs the headless-cyan Lens binary
/// wired to a real `MeshHolder` over iroh (STATUS_HEADLESS_SUPERPEER.md "Tier-2"), which is not
/// built in this batch.
#[ignore = "offline-message hold+redeliver is Lens super-peer logic (cyan-lens, fakes-only; no \
            runnable real binary). cyan_node has the §4 mesh_hold seam but no per-peer redeliver \
            wire path. See STATUS_HEADLESS_SUPERPEER.md 'Tier-2' + STATUS_MESH_HARNESS.md."]
#[tokio::test]
async fn offline_peer_message_held_by_superpeer_delivered_on_return() {
    unimplemented!("blocked on the headless-cyan Lens super-peer binary; see the doc comment + STATUS");
}
