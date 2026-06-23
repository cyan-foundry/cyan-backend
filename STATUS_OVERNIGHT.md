# STATUS — overnight run (reliability + multi-process snapshot rig)

Branch `feat/substrate-e2e` (never touched `main`, never touched `~/cyan-iOS`, never
ran `build_static_lib.sh` / the xcframework). **Additive only** — no engine/FFI/storage
source modified (see "Safety" below). iroh 0.95 throughout; no version bumps.

Date: 2026-06-19. Two commits added this run:
- `c019689` reliability: stress + repeat suite (substrate stays green under load)
- `45af2c1` multiprocess rig: per-process DB snapshot truth

Both pushed to `origin/feat/substrate-e2e`. No PR opened (left for the human).

---

## PHASE 0 — baseline (GREEN)

`cargo build` ✅. Baseline substrate suite green via
`cargo test --no-fail-fast --test substrate_discovery --test substrate_sync
 --test substrate_chat --test substrate_files --test substrate_offline`:
discovery 2/2, sync 4 + 1 ignored, chat 3 + 1 ignored, files 5 + 1 ignored,
offline 3/3. The pre-existing `diagram_gen` unit failure and ~1000 pre-existing clippy
warnings were left untouched (out of scope, per the standing rules).

## PHASE 1 — reliability suite (GREEN)

Added (additive):
- `scripts/reliability.sh` — runs each green substrate binary in a bounded loop `N`
  times (default 20, `RELIABILITY_N` env), fails on the FIRST red, prints a per-binary
  pass tally. No infinite loops; builds once up front.
- `tests/substrate_reliability.rs`:
  - `repeat_discovery_is_stable` — 15 sequential fresh 2-node meshes, each must meet.
  - `concurrent_meshes_do_not_interfere` — 3 independent meshes (unique keys) converge
    concurrently.
  - `larger_mesh_converges` — a 5-node mesh fully converges (seed broadcast reaches all;
    each node holds the group topic).
  All assert on the per-node `meet`/`peers_per_group` oracles (never shared storage),
  use bounded waits only, and reuse the `support::` harness.

**Exact reliability numbers** — `RELIABILITY_N=20 ./scripts/reliability.sh`: **ALL GREEN**
- substrate_discovery — 20/20
- substrate_sync — 20/20
- substrate_chat — 20/20
- substrate_files — 20/20
- substrate_offline — 20/20

`tests/substrate_reliability.rs` ran **green 3× in a row** (~18s each, 3 passed/0 failed
per run); re-verified green again after PHASE 2.

## PHASE 2 — multi-process snapshot rig (GREEN)

Added (additive):
- `src/bin/cyan_node.rs` — a test-only peer binary (the one new bin allowed) that boots a
  `NetworkActor` against its OWN sqlite DB (env `NODE_DB`/`DISCOVERY_KEY`/`RELAY`/
  `BOOTSTRAP_NODE_ID`/`SEED_FIXTURE`/`DATA_DIR`), driven over a `@@CYAN@@`-tagged
  stdin/stdout line protocol (`node_id`, `addr`, `add_peer`, `seed_fixture`,
  `seed_empty_group`, `join_group`, `wait_sync`, `count`, `quit`). Uses ONLY the crate's
  public API — no FFI, no engine edits. Opt-in iroh tracing to stderr behind `RUST_LOG`.
- `tests/support/multiprocess.rs` — spawns N `cyan_node` children (each its own temp DB),
  exchanges their serialized `EndpointAddr`s (JSON) into each other's `StaticProvider` for
  direct loopback dialing with relay disabled, drives the protocol with bounded timeouts,
  and asserts on each process's own `count`.
- `tests/substrate_snapshot_mp.rs` — `late_joiner_gets_full_snapshot`, done honestly.
- `Cargo.toml` — the `[[bin]] cyan_node` entry (explicitly allowed).

**Result: GREEN and stable.** The host seeds the fixture and a separate joiner process
joins; after `SyncComplete`, the **joiner's OWN database** contains:
workspaces=1, boards=1, elements=5, cells=3, chats=3, files=1 — matching the host's seed.
Because the two nodes have genuinely separate DBs, those counts can only be non-zero if
the snapshot actually transferred over the mesh (the in-process suite cannot prove this —
its storage is a process-global singleton). Test runtime ~2.5s; **stable across 6
consecutive runs** (1 passed/0 failed each).

The in-process `tests/substrate_sync.rs::late_joiner_gets_full_snapshot` **remains
`#[ignore]`d** (shared process-global DB makes it a fake pass); this honest multi-process
version is added alongside it, exactly as the run plan specified.

### Engine finding surfaced (NOT worked around in the engine)

With relay disabled, the engine's startup group-load awaits
`gossip.subscribe_and_join([…, default_bootstrap]).await`, which **blocks until a
neighbor connects**. The hardcoded default bootstrap (`f992aa3b54…`, `src/lib.rs:67`) is
relay-only and therefore unreachable offline, and on the startup path it is the *only*
peer in the list — so any node that has the group in its DB at start blocks before its
command loop ever runs (this is exactly why a naive multi-process attempt hung 60s with
no `JoinGroup` ever processed). The rig accommodates this WITHOUT touching the engine:
the host seeds its fixture *before* the actor starts (so startup auto-hosts the group
topic and waits for the joiner — it recovers the moment the joiner connects), and the
joiner starts with an *empty* DB so its command loop is reachable to process `JoinGroup`
with the reachable host supplied as the bootstrap peer. (`spawn_topic_actor` dedups by
group id; the host's `main()` control loop is a separate task from the blocked `start()`,
so `addr`/`add_peer` stay responsive and its `TopicActor` serves the snapshot once joined.)

---

## What's green

- PHASE 1: `scripts/reliability.sh` 20/20 on all five binaries; `substrate_reliability`
  (3 tests) green and stable.
- PHASE 2: `substrate_snapshot_mp::late_joiner_gets_full_snapshot` green and stable.
- The whole pre-existing substrate suite remains green; `cargo test --no-run` compiles
  cleanly across lib + bins + tests.

## What's ignored (unchanged this run) + why

- `substrate_sync::late_joiner_gets_full_snapshot` — shared process-global DB in-process
  (now superseded by the green multi-process version).
- `substrate_chat::chat_with_attachment_shares_file_into_scope` — no `NetworkCommand`
  carries an attachment (G7 wiring; deferred).
- `substrate_files::large_file_1gb_transfers_intact` — CI cost; on-demand.
- `substrate_relay.rs` (G2 ladder/G8-R/G11) and `substrate_swarm.rs` (G10) and
  `substrate_lens.rs` — red scaffolds needing the relay/netns rig or unbuilt engine
  capability; out of in-process scope.

## Clippy

New files introduce **zero** new warnings (verified per file; two lints in `cyan_node.rs`
were fixed during PHASE 2). The pre-existing whole-tree `-D warnings` red (~1027 lib
warnings) is unchanged and out of scope.

## Safety

- No `src/**` lib/engine/FFI/storage source modified. `git diff` for tracked files shows
  only `Cargo.toml` (the allowed `[[bin]] cyan_node` entry) and `PROGRESS.md`.
- New files only: `scripts/reliability.sh`, `tests/substrate_reliability.rs`,
  `src/bin/cyan_node.rs`, `tests/support/multiprocess.rs`, `tests/substrate_snapshot_mp.rs`.
- `SUBSTRATE_TEST_SPEC.md` unedited; no assertion weakened; `tests/support/mod.rs` public
  signatures unchanged (PHASE 2 includes its module via `#[path]`, not by editing mod.rs).
- Stayed on `feat/substrate-e2e`; `main` and `~/cyan-iOS` untouched; no xcframework build.
