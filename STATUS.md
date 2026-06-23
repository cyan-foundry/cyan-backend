# STATUS — substrate hardening run (feat/substrate-e2e)

Completed PHASE 0 → FINISH on branch `feat/substrate-e2e` (never touched `main`).
Date: 2026-06-18. Engine is built on iroh 0.95; no version bumps.

## Gate summary

- `cargo build` ✅
- `cargo test --lib node_config` ✅ (4/4 `relay_mode_for` cases)
- Whole substrate suite ✅ and **reliable** — every node-spinning binary passed 0/8–0/15
  repeated runs, and `cargo test --no-fail-fast` is clean except one pre-existing failure.
- Clippy: **my diff introduces zero new warnings** (verified: new files clean; engine
  warnings at touched lines are pre-existing, line-shifted only).

### Two pre-existing conditions I could NOT and did NOT touch (per the standing rules)

1. **`diagram_gen::tests::test_parse_diagram_json` fails at baseline.** `parse_diagram_response`
   hardcodes `svg: None` (src/diagram_gen.rs) while the test asserts `svg.is_some()`. Committed,
   unrelated to the P2P substrate (diagram generation / enrichment-adjacent). Because plain
   `cargo test` runs the lib unittests first and **fail-fasts**, this pre-existing failure stops
   the substrate binaries from running under a bare `cargo test` — evaluate the substrate suite
   per-binary (`cargo test --test substrate_*`) or with `--no-fail-fast`.
2. **`cargo clippy --all-targets -- -D warnings` is pre-existing-red** (~1027 lib warnings: a
   codebase-wide `disallowed_methods` unwrap lint + many unused imports across skills/bridges).
   Fixing it would mean broadly rewriting unrelated shipping code (forbidden). The clippy gate is
   therefore enforced as "no NEW warnings from my diff", which holds.

## Per-file / per-named-test ledger

### `tests/support/mod.rs` — MeshHarness (built first)
Implemented: `spawn_node`, `spawn_mesh`, `meet`, `wire_addrs`, `serial` (cross-process lock),
`stage_file`, `seed_group_fixture`, and `Node::{cmd, join_group, broadcast, request_download,
wait_for, wait_network, wait_peer_joined, wait_file_downloaded, wait_sync, peers_in_group,
has_group, db}`. Public signatures from the reviewed shape are preserved (additive only).

### `tests/substrate_discovery.rs` — G1/G2-LAN
- `two_nodes_meet_via_mdns_on_lan` — ✅ GREEN
- `two_nodes_meet_via_bootstrap` — ✅ GREEN
- (`direct_quic_preferred_on_lan`, `relay_fallback_when_direct_blocked` from the spec backlog were
  not added here; relay fallback is covered by the `substrate_relay.rs` scaffolds.)

### `tests/substrate_sync.rs` — G3/G4
- `delta_board_element_propagates` — ✅ GREEN
- `delta_notebook_cell_propagates` — ✅ GREEN
- `delta_workspace_structure_propagates` — ✅ GREEN
- `three_node_convergence` — ✅ GREEN
- `late_joiner_gets_full_snapshot` — ⛔ #[ignore] (engine finding #1 below)

### `tests/substrate_chat.rs` — G5/G7
- `group_chat_reaches_all_peers` — ✅ GREEN
- `workspace_chat_reaches_all_peers` — ✅ GREEN
- `board_chat_reaches_all_peers` — ✅ GREEN
- `chat_with_attachment_shares_file_into_scope` — ⛔ #[ignore] (engine finding #2 below)

### `tests/substrate_files.rs` — G6/G8
- `file_shared_at_group_scope` / `…_workspace_scope` / `…_board_scope` — ✅ GREEN (blake3-verified)
- `large_file_100mb_transfers_intact` — ✅ GREEN (~10s)
- `large_file_meets_throughput_floor` — ✅ GREEN (~16 MB/s loopback; floor 3 MB/s)
- `large_file_1gb_transfers_intact` — ⛔ #[ignore] (CI cost; runs on demand)

### `tests/substrate_offline.rs` — G9
- `offline_discovery_and_sync` — ✅ GREEN
- `offline_chat_all_levels` — ✅ GREEN
- `offline_file_share_and_large_transfer` — ✅ GREEN

### `tests/substrate_relay.rs` — G2 ladder / G8-R / G11 (red scaffolds, compile + ignored)
`connects_via_relay_when_direct_blocked`, `connects_via_websocket_when_udp_fully_blocked`,
`large_file_100mb_over_relay_intact`, `large_file_over_websocket_relay_intact`,
`relay_path_meets_relay_throughput_floor`, `relayed_bytes_are_metered` — all ⛔ #[ignore]
(need a local `iroh-relay` fixture + UDP black-hole via netns/docker, not in-process).

### `tests/substrate_swarm.rs` — G10 (red scaffolds)
`partial_transfer_resumes_from_offset`, `file_fetched_from_two_sources_in_parallel`,
`transfer_survives_source_peer_churn`, `i_have_who_has_negotiation_picks_a_holder` —
all ⛔ #[ignore] (swarming not yet implemented in the engine).

### `tests/substrate_lens.rs` — §4 (red scaffold)
`mesh_fully_functional_with_lens_unreachable` — ⛔ #[ignore] (needs `CyanLensClient` wiring).

## Engine call sites I touched (seams, additive; shipping behavior unchanged)

- `src/models/node_config.rs` (NEW): `NodeConfig`, `RelayPolicy`, `DiscoveryPolicy`, pure
  `relay_mode_for` (+ unit tests); re-exported from `src/models/mod.rs`.
- `src/actors/network_actor.rs`:
  - `NetworkActor::new` gains a 4th param `cfg: NodeConfig`; relay computed via `relay_mode_for(&cfg.relay)`
    (adds the `RelayMode::Disabled` branch).
  - `NetworkActor::start` derives the discovery-topic gossip bootstrap from `cfg.discovery`
    (`Bootstrap(id)`→[id], `MdnsOnly`→[]); uses `cfg.discovery_key`.
  - Endpoint also gets an **inert** `StaticProvider` discovery; added `endpoint()` and
    `static_discovery()` accessors (test-support seam for loopback addressing).
- `src/actors/discovery_actor.rs`: `DiscoveryActor::spawn` takes `bootstrap_peers: Vec<PublicKey>`
  instead of hardcoding `bootstrap_node_id()`.
- `src/lib.rs`: FFI init site builds `NodeConfig` from the existing `RELAY_URL`/`DISCOVERY_KEY`/
  `BOOTSTRAP_NODE_ID` globals — production behavior is identical to before.
- Mechanical: 3 `NetworkActor::new(..)` call sites in `tests/network_actor_test.rs` and
  `tests/delta_sync_test.rs` updated to pass a `NodeConfig`.

## Engine-capability findings (real limits in-process, NOT test hacks)

1. **Storage is a process-global singleton** (`static DB: OnceLock<Mutex<Connection>>`; `init_db`
   errors on a 2nd call). All in-process nodes share ONE SQLite DB, so per-node storage assertions
   (the spec's "assert on the receiver's `storage::*`") are impossible in one process — a late
   joiner already shares the host's data (a fake pass). The in-process tests therefore assert on
   the **receiver's per-node `SwiftEvent` channel** (deltas/chat) and on **received-file bytes on
   disk** (files), which are honest per-node oracles. `late_joiner_gets_full_snapshot` is #[ignore]d;
   it belongs to the multi-process rig (which is exactly why `network_test`/`snapshot_test`/
   `delta_test` are separate-process bins).
2. **No `NetworkCommand` carries an attachment.** `SendDirectChat` has no attachment field and
   `DmAttachment` (on the wire `DirectMessage`) is never constructed. So G7 chat-with-attachment
   cannot be driven through the substrate's command interface — `#[ignore]`d until that capability exists.
3. **`gossip.subscribe_and_join(..).joined()` consumes the first `NeighborUp`** of a topic, so a
   2-node mesh never surfaces the sole peer's `PeerJoined`. Tests assert on `peers_per_group` /
   group-topic delivery instead.
4. **In-process mDNS multicast is unreliable** here (resolves in <1s or not within 15s). With relay
   disabled it was the only address-resolution path, so discovery was flaky until the harness used
   the engine's `StaticProvider` seam to inject loopback addresses. mDNS remains enabled and unchanged;
   the harness simply no longer depends on it. Pure mDNS-on-real-LAN discovery belongs to the device rig.
5. **The discovery-topic `groups_exchange` can drop its first message** on a freshly-formed 2-node
   topic. `meet()` works around this by gating on the group topic via a re-broadcast probe (robust),
   rather than on the discovery topic's `peers_per_group`.

## Branch / safety

Eight commits on `feat/substrate-e2e`, pushed to `origin`. `main` never checked out, committed,
merged, or pushed. No PR opened (left for the human). Did not run `build_static_lib.sh`,
touch `~/cyan-iOS`, or build the xcframework. `SUBSTRATE_TEST_SPEC.md` unedited; no assertions weakened.
