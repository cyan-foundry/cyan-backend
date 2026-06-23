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
//! - **bootstrap** — the thin discovery rendezvous; the TRUE cross-network stranger-introduction
//!   needs the xaeroflux bootstrap binary as a container (xaeroflux is untouched per the spec), so
//!   `bootstrap_seeded_cross_net_mesh` is `#[ignore]`d, pointing at the relay rung that already
//!   proves the cross-net TRANSPORT.
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

use dockernode::{relay_url, wire_into, wire_pair, DockerNode, Relay, Spec};

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

/// §6 / §10 (bootstrap-seeded cross-network mesh): two peers on ISOLATED bridges that have no
/// pre-shared addrs find each other through a thin discovery rendezvous.
///
/// RED (honest): introducing two strangers across isolated networks WITHOUT pre-shared addrs is
/// the xaeroflux bootstrap's gossip-discovery rendezvous role. `cyan_node` cannot relay third-
/// party addrs (it is a peer, not a rendezvous), and xaeroflux is untouched by this batch (no
/// bootstrap image is built here). The cross-network TRANSPORT itself is already proven GREEN by
/// `substrate_relay::connects_via_relay_when_direct_blocked` (peers on split bridges sync via the
/// relay). What remains unproven in-rig is only the auto-DISCOVERY of strangers — which needs the
/// real xaeroflux bootstrap container.
#[ignore = "needs the xaeroflux bootstrap discovery-rendezvous binary as a container (xaeroflux \
            untouched per spec; no bootstrap image built here). Cross-net TRANSPORT is proven by \
            substrate_relay::connects_via_relay_when_direct_blocked. See STATUS_MESH_HARNESS.md."]
#[tokio::test]
async fn bootstrap_seeded_cross_net_mesh() {
    unimplemented!("blocked on a real xaeroflux bootstrap container; see the doc comment + STATUS");
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
