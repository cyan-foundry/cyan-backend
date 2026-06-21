# Mesh seed + presence roster — MESH_HARDENING_SPEC §2 (formation) + §3 (roster)

**Branch:** `fix/mesh-seed-roster` (off the presence-fix tip `fix/presence-gossip-neighbors`)
**Scope:** additive. New engine command + storage tables + one FFI getter + a new QR field. No FFI
signatures changed or removed; no xcframework rebuild. iroh 0.95 only; no `unwrap`/`panic!` added on
the engine/FFI path; every test wait is a bounded `tokio::time::timeout`, asserting real state.

## The bug (§2)

Two peers exchanged a one-shot snapshot but never formed a LIVE gossip mesh — `NeighborUp` never
fired → presence 0, single-laptop/same-WiFi showed "no peers". Root cause: **a group's gossip topic
is only ever seeded with the (unreachable, relay-only) bootstrap node id.** `iroh-gossip` only dials
peers you put in a topic's bootstrap set; mDNS makes an address *resolvable* but does NOT put a peer
into a topic. With no relay/bootstrap reachable, the topic had no present, resolvable peer, so it
never connected — even when both laptops saw each other over mDNS.

## The fix — ONE seeding pipeline, many sources (§2)

A single engine entry point, `NetworkActor::seed_peer_into_group(group_id, EndpointAddr)`, turns "an
address" into "a present peer in a topic's bootstrap set":

1. `static_discovery.add_endpoint_info(addr)` — make the peer **resolvable** (the mechanism
   `cyan_node.rs:366` already proved).
2. `storage::group_known_peer_upsert(..)` — **persist** the NodeAddr per-group for rejoin.
3. `TopicCommand::JoinPeers([peer])` (spawning the topic first if needed) — route the peer into the
   group topic, so `gossip` dials it and `NeighborUp` fires → the existing presence fix fills
   `peers_per_group` → live presence + live gossip.

Every source funnels through that one function:

| # | Source | Wiring |
|---|--------|--------|
| 1 | **mDNS (LAN/offline)** | `MdnsDiscovery` is now built with a handle; `start()` `subscribe()`s its discovery stream and feeds each discovered peer back as `NetworkCommand::SeedDiscoveredPeer` → seeded into **every** joined group (gossip only forms a neighbor where the topic is genuinely shared). **This is the single-laptop / no-infra fix.** |
| 2 | **QR/inviter** | `GrantInvite` gains an additive `inviter_addr: Option<String>` (a serialized `EndpointAddr`, not just a pubkey). `cyan_issue_grant_qr` stamps the issuer's live address in; `cyan_scan_grant_qr` → `join_from_invite` forwards it as `SeedGroupPeer` on join. |
| 3 | **Persisted known-peers** | New `group_known_peers` table. `spawn_topic_actor` re-seeds every saved NodeAddr for the group on (re)join/restart — so the mesh re-forms with **no** bootstrap/relay/peer handed to it. |
| 4 / 5 | **Bootstrap / Lens addr** | Same `SeedGroupPeer` path when a resolvable addr is configured (the existing bootstrap-pubkey behavior is unchanged; this adds the *full-addr* seed when present). |

New additive commands: `NetworkCommand::SeedGroupPeer { group_id, addr_json }` (one group) and
`SeedDiscoveredPeer { addr_json }` (all joined groups — the mDNS fan-out). `addr_json` is a
serialized `iroh::EndpointAddr` (same serde the `cyan_node` `addr`/`add_peer` verbs use).

Local-addr seam: `NetworkActor` publishes its resolvable `EndpointAddr` to a process global
(`LOCAL_ENDPOINT_ADDR`) once the endpoint has an address, so `cyan_issue_grant_qr` can stamp it into
the QR. Best-effort and additive; absent ⇒ the QR is byte-identical to before.

## Presence roster (§3)

- **New `group_members` table** `(group_id, peer_id, first_seen, last_seen)` — the persistent roster.
  A member is recorded the moment it is seen over the mesh: `TopicActor` `NeighborUp` and any received
  gossip event's author both call `storage::member_seen(..)`. `first_seen` is set once; `last_seen`
  advances; the row is **never deleted**, so an offline peer stays in the roster (greyed, cached name).
- **New FFI `cyan_get_group_members(group_id)`** → `[{peer_id, name, avatar, online, last_seen}]`.
  `name`/`avatar` resolve from `user_profiles` (null until a profile is seen); **`online`** is overlaid
  from the live neighbor set (`peers_per_group`) at read time. `cyan_get_group_peers` (live-only) is
  unchanged. Tenant-scoped by `group_id` (a group is one tenant — see `storage.rs` note).

## Tests (test-first; bounded waits; assert real state, never logs)

New `tests/substrate_mesh_seed.rs` — every node is spawned with a **unique discovery key** + `MdnsOnly`
+ `RelayPolicy::Disabled` (no relay, no bootstrap, no cloud), so the discovery peer-intro path can
NEVER populate `peers_per_group`; the only way a peer lands there is the live group-topic `NeighborUp`
the seeding pipeline produces. No `wire_addrs` — the engine's pipeline makes peers resolvable itself.

| Test | Proves |
|------|--------|
| `mdns_discovered_peer_seeds_topic_and_forms_neighbor` | seeded addr → both ends `NeighborUp`, `peer_count == 1`, no infra |
| `qr_join_forms_neighbor_via_seeded_addr` | the inviter's QR-carried address forms the neighbor on first join |
| `persisted_peer_reseeded_on_reconnect` | a returning device re-forms the mesh from the persisted store with NOTHING handed to it |
| `mesh_works_lan_only_no_infra` | LAN-only mesh both **forms and delivers live data** with all infra down |
| `mesh_recovers_after_neighbor_down_up` | mesh recovers (fresh neighbor) after a peer churns away |
| `member_recorded_on_first_contact` | a met peer is recorded in the persistent roster |
| `member_online_reflects_neighbor_set` | a live neighbor is reported `online` in the roster overlay |
| `member_persists_offline_after_neighbor_down` | an offline peer persists in the roster (greyable) |
| `members_survive_restart` | the roster survives a full teardown (read back by a fresh node) |

Result: **9 passed, 0 failed, 0 ignored.**

Plus `tests/qr_test.rs::qr_carries_optional_inviter_addr` — unit-proves the additive `inviter_addr`
field round-trips through the QR payload and defaults to absent (drop-in for old QRs).

**Un-ignored** `substrate_presence::neighbor_down_decrements_presence` (per the brief, "if reachable"):
it drops the peer via `Node::shutdown()` → `Endpoint::close()`, which ACTIVELY tears the connection
down so `NeighborDown` fires within the bounded `SYNC_TIMEOUT` (passes reliably, ~32 s incl. spawn).
`presence_tracks_join_leave_for_n_peers` stays ignored — it waits on the separate, still-timing-soft
`MeshReachability{local_only}` emit, not the bounded endpoint-close decrement.

## No regression

- `cargo build --tests` clean (only pre-existing warnings).
- `cargo clippy --lib --tests`: **no new findings in any touched file** (the `unwrap`/`disallowed_methods`
  hits in `network_actor.rs`/`ffi/core.rs` are pre-existing in untouched functions; the new FFI getter
  uses a no-`unwrap` `lock().ok()` fallback).
- FFI is additive: `cyan_get_group_members` is new; `GrantInvite.inviter_addr` is `#[serde(default,
  skip_serializing_if)]` so old QRs and old FFI callers are unaffected; the two new `NetworkCommand`
  variants are additive.
- `substrate_mesh_seed` green (9/9); `substrate_presence` green incl. the newly un-ignored test;
  storage lib unit tests green.

## Honest gaps / notes (not faked)

- The **mDNS source (§2.1)** is wired for production (real `MdnsDiscovery::subscribe()` stream) but is
  NOT exercised by an in-process test — real multicast is flaky/forbidden in CI (the harness docs say
  so), so the seeding pipeline it feeds is tested deterministically through the `SeedGroupPeer` seam
  with the peer's real loopback `EndpointAddr`. The mDNS task is the production fan-out into that same,
  tested pipeline.
- The roster/known-peers tables live in the **process-global** substrate DB (one DB for all in-process
  nodes). Roster assertions are made on the group-scoped member **set**, which is node-independent
  (a peer id is global), so the shared DB does not weaken them. The `online` overlay is read from each
  node's own `peers_per_group`, which IS per-node.
- §2.4/§2.5 (bootstrap/Lens **full-addr** seeds) reuse `SeedGroupPeer`; they fire only when a resolvable
  addr is configured. The existing bootstrap-by-pubkey behavior is untouched.
- Out of scope here (per the brief): §4/§5 durability + incremental catch-up, §11 export bundle, the
  Docker/netem rig — left for the rest of the batch.
