# PROGRESS — substrate hardening run (feat/substrate-e2e)

Timestamped log for remote watching. One line at phase start + end with gate result.

## PHASE 0 — branch safety + baseline

- 2026-06-18 12:48 PDT — START. On branch `feat/substrate-e2e` (already created; NOT main).
  `git status -s` shows only the expected untracked substrate files. `cargo build` ✅.
  `cargo test --no-run` ✅ (all targets compile, including `tests/substrate_discovery`).
- 2026-06-18 12:48 PDT — Baseline note: `cargo test --lib` = 19 passed, **1 pre-existing
  FAILURE**: `diagram_gen::tests::test_parse_diagram_json`. Root cause: `parse_diagram_response`
  hardcodes `svg: None` (src/diagram_gen.rs:543) while the test asserts `svg.is_some()`.
  This is committed shipping code, **unrelated to the P2P substrate**, in enrichment-adjacent
  diagram-generation code that the spec marks out of scope. Standing rules forbid touching it
  ("never chase pre-existing red", "never change shipping behavior"). **Substrate-relevant
  baseline is GREEN** (engine compiles; substrate scaffolds are the `todo!()` backlog this run
  implements). Per-phase gates are scoped to substrate-relevant targets; the pre-existing
  diagram red is excluded and re-noted at FINISH.
- 2026-06-18 12:48 PDT — END. Gate: substrate-relevant baseline GREEN. Proceeding to PHASE 1.

### Gate finding — `clippy -D warnings` is pre-existing-red (whole tree)
`cargo clippy --all-targets -- -D warnings` is **not** green at baseline: the lib alone
emits ~1027 warnings (unused imports across skills/bridges, plus a codebase-wide
`disallowed_methods` lint that flags every `.unwrap()`), ~711–751 across all targets.
These are pre-existing in shipping code unrelated to the substrate. Standing rules forbid
fixing them ("never chase pre-existing red", "never change shipping behavior", "small
reviewable diffs"). **Decision (documented, not a spec/test weakening):** the clippy gate
is enforced as "my diff introduces ZERO new clippy warnings", verified per phase. Whole-tree
`-D warnings` cannot be made green within scope; re-noted at FINISH as a real finding.

### Major finding — storage is a process-global singleton (blocks in-process storage-oracle tests)
`src/storage.rs` keys everything off `static DB: OnceLock<Mutex<Connection>>`; `init_db` errors
on the 2nd call ("DB already initialized") and every `storage::*` fn uses the one global `db()`.
So **N in-process nodes share ONE SQLite DB** — there is no per-node storage. This is why the
existing engine tests (`network_test`/`snapshot_test`/`delta_test`) are separate-process bins.
Consequences for the substrate suite:
- **Discovery (G1, PHASE 2): FEASIBLE in-process** — asserts on per-node `peers_per_group`
  (a per-node `Arc<Mutex<..>>` handed to each `NetworkActor`) and per-node `PeerJoined` events,
  not storage. Shared DB is acceptable here.
- **Snapshot/delta/chat/file storage-oracle tests (G3–G9, PHASE 3): NOT honestly isolatable
  in-process** — a shared DB makes "receiver got the data" indistinguishable from the sender's
  own writes, i.e. a fake pass. Per spec ("assert on the receiver's storage", "do not fake a
  pass") these need per-node storage, which the engine lacks in-process. A storage refactor
  (instance-based DB threaded through every actor + FFI) is far out of scope ("never change
  shipping behavior", "small diffs"). **Action:** implement these test files but mark the
  storage-asserting ones `#[ignore]` with this reason; they belong to the multi-process rig.

## PHASE 1 — NodeConfig seam

- 2026-06-18 12:55 PDT — START. Add `src/models/node_config.rs` (NodeConfig, RelayPolicy,
  DiscoveryPolicy, pure `relay_mode_for`); thread `cfg: NodeConfig` into `NetworkActor::new`;
  add `RelayMode::Disabled` branch; replace `RELAY_URL`/`DISCOVERY_KEY` reads with cfg fields;
  build NodeConfig from globals at the FFI init site (seam, not change); update 2 test bins.
- 2026-06-18 12:55 PDT — END. Gate: `cargo build` ✅; `cargo test --lib node_config` ✅ (4/4
  relay_mode_for cases: Disabled→Disabled, Url→Custom, invalid Url→Default, Default→Default);
  all test targets compile ✅; clippy = 0 new warnings from my diff (verified node_config.rs
  clean; network_actor/lib warnings all pre-existing, line-shifted only). Shipping behavior
  unchanged — production still derives NodeConfig from RELAY_URL/DISCOVERY_KEY/BOOTSTRAP_NODE_ID.
  GREEN within scope. Committing.

## PHASE 2 — MeshHarness + discovery green

- 2026-06-18 13:30 PDT — START. Implement `tests/support/mod.rs` and make
  `tests/substrate_discovery.rs::{two_nodes_meet_via_mdns_on_lan, two_nodes_meet_via_bootstrap}`
  pass. Two engine seams were required (both additive, production behavior unchanged):
  - **Per-node discovery bootstrap**: `NetworkActor::start` now derives the discovery-topic
    gossip bootstrap from `cfg.discovery` (`Bootstrap(id)`→[id], `MdnsOnly`→[]); `DiscoveryActor::spawn`
    takes it as a param instead of hardcoding the global default. Production passes
    `Bootstrap(bootstrap_node_id())` → identical to before. Needed because `subscribe_and_join`
    blocks on `joined()` until ≥1 neighbour, so an in-process node must be seeded off a real peer,
    not the unreachable production default.
  - **StaticProvider address seam**: the endpoint now also gets an (empty, inert) `StaticProvider`
    discovery; `NetworkActor::endpoint()`/`static_discovery()` expose it. The harness reads each
    node's loopback `EndpointAddr` and injects it into the others, so nodes dial by id **without
    mDNS**. This was essential: in-process iroh mDNS multicast resolved only intermittently here
    (flaky even single-threaded — see finding below), but static loopback addressing is 100%
    reliable (5/5 runs, 0.17s).
- Finding (mDNS): in-process `MdnsDiscovery` resolution is unreliable on this host (binary: works
  in <1s or never within 15s; independent of test parallelism). With relay disabled it was the only
  address-resolution path, so discovery was flaky until the StaticProvider seam bypassed it. mDNS is
  still enabled (unchanged); the harness just no longer depends on it. The real mDNS-on-real-LAN
  guarantee belongs to the multi-process/device rig.
- Finding (PeerJoined): `gossip.subscribe_and_join(..).joined()` **consumes the first NeighborUp**
  of a topic, so in a 2-node mesh the sole peer's `PeerJoined` is never surfaced by the TopicActor.
  The harness therefore asserts mutual discovery on the per-node `peers_per_group` map (populated by
  the discovery `groups_exchange` message), which the spec explicitly endorses ("PeerJoined/peers_per_group").
- Harness notes: shared (leaked) global DB initialised once; unique per-test discovery key + group
  id for isolation; a process-wide serial guard for node-spinning tests; bounded `wait_until` polling
  of real oracles (never sleep-as-sync without a deadline); two-phase `meet` re-announce to beat the
  discovery join-order race.
- 2026-06-18 13:30 PDT — END. Gate: `cargo build` ✅; `cargo test --test substrate_discovery` ✅
  (2/2, reliable across 5 runs); `cargo test --lib node_config` ✅ (4/4); all targets compile ✅;
  0 new clippy warnings from my diff. GREEN within scope. Committing.

## PHASE 3 — fan out the in-process suite

- 2026-06-18 13:30–14:18 PDT — Built and greened, one file per commit:
  - `substrate_sync.rs` (G3/G4): 4 delta tests green (board element, notebook cell, workspace
    structure, three-node convergence) via the receiver's per-node `SwiftEvent::Network` channel.
    `late_joiner_gets_full_snapshot` (G3) **#[ignore]** — snapshot needs per-node storage (engine
    DB is a process-global singleton) and the blocking `subscribe_and_join` dead-locks a one-process
    host-seeds-then-joiner-syncs ordering; belongs to the multi-process rig.
  - `substrate_chat.rs` (G5/G7): 3 chat tests green (group/workspace/board via `ChatSent`).
    `chat_with_attachment_shares_file_into_scope` (G7) **#[ignore]** — no `NetworkCommand` carries
    an attachment (`DmAttachment` is never wired to a command).
  - `substrate_files.rs` (G6/G8): file share at group/workspace/board scope, 100MB transfer, and a
    throughput floor — all green, blake3-verified on the received bytes. Measured ~16 MB/s direct-QUIC
    loopback (floor 3). `large_file_1gb_transfers_intact` **#[ignore]** (CI cost; runs on demand).
  - `substrate_offline.rs` (G9): discovery+delta, chat-all-levels, file share + multi-MB transfer
    re-run under `RelayPolicy::Disabled` + `MdnsOnly` — all green; guarded with `assert_offline`.
- Reliability: root-caused an intrinsic ~25% meeting flake (discovery-topic groups_exchange drop) and
  fixed it — `meet()` now gates on the group topic via a re-broadcast probe; `serial()` is a
  cross-process file lock. Each binary 0 failures over 8–15 runs; full `cargo test --no-fail-fast`
  clean except the pre-existing `diagram_gen` lib failure.

## PHASE 4 — red scaffolds

- 2026-06-18 14:00 PDT — `substrate_relay.rs` (6 tests), `substrate_swarm.rs` (4), `substrate_lens.rs`
  (1): all `#[ignore]` + `unimplemented!()`, compile clean, stay ignored. Need the netns/docker relay
  rig (relay), the swarming engine work (swarm), and `CyanLensClient` wiring (lens).
- Gate: `cargo build` ✅; whole substrate suite green/ignored and reliable; 0 new clippy warnings from
  my diff. The only red anywhere is the pre-existing, untouched `diagram_gen` lib test.

## FINISH — see STATUS.md for the full per-test ledger and findings.
