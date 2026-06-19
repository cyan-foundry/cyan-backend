# Overnight run — reliability + multi-process snapshot rig (SAFE, additive only)

Unattended overnight work that CANNOT damage the stable engine: it adds test code
and at most one new test-only binary. It does NOT modify lib/engine source, the
FFI, or storage. Paste into `claude --dangerously-skip-permissions` from
`~/cyan-backend` and leave it.

---

You are extending the cyan-backend substrate test suite overnight, unattended.
Read `CLAUDE.md`, `SUBSTRATE_TEST_SPEC.md`, and `STATUS.md` first. Work the PHASES
in order; gate between them; commit per green phase; stop clean on red.

## Standing rules (absolute)
- **Branch:** stay on `feat/substrate-e2e` (PHASE 0 verifies). NEVER `main`, NEVER
  `~/cyan-iOS`, NEVER run `build_static_lib.sh` or build the xcframework.
- **ADDITIVE ONLY — do not touch stable code.** You may ADD files under `tests/`
  and ONE new test-only bin `src/bin/cyan_node.rs`, plus `[[bin]]`/`[[test]]`
  entries in `Cargo.toml`. You may NOT edit existing `src/**` lib code, change any
  `cyan_*` FFI signature or command JSON, or refactor `storage` (the global DB).
  If a task seems to require editing engine source, STOP and write STATUS — that
  task is for human babysitting, not tonight.
- **Explicitly deferred (DO NOT attempt tonight):** chat-attachment / G7 command
  wiring; the per-node storage refactor. Leave both `#[ignore]`d as they are.
- **Do not touch pre-existing issues:** the `diagram_gen` unit-test failure and the
  ~1000 pre-existing clippy warnings are out of scope; leave them.
- **Gate** = `cargo build` ✅ and the substrate suite green. Because a bare
  `cargo test` fail-fasts on the pre-existing `diagram_gen` failure, evaluate with
  `cargo test --no-fail-fast` or per-binary `cargo test --test substrate_<name>`.
  New work must add **zero** new clippy warnings in its own files.
- **Iterate to green ≤6 honest attempts per phase; else STOP + STATUS.** Commit
  after each green phase (`git add -A && commit`); push `feat/substrate-e2e`. Keep
  `PROGRESS.md` updated. Never weaken an assertion or edit `SUBSTRATE_TEST_SPEC.md`.

## PHASE 0 — branch + baseline
1. `git rev-parse --abbrev-ref HEAD` must be `feat/substrate-e2e` (if not, checkout
   it; if it doesn't exist or you're on main, STOP + STATUS). `git status` clean.
2. Baseline: `cargo build` ✅ and `cargo test --no-fail-fast --test substrate_discovery
   --test substrate_sync --test substrate_chat --test substrate_files --test substrate_offline`
   all green. If not green as-is, STOP + STATUS (we start from green). Append baseline to PROGRESS.md.

## PHASE 1 — reliability suite (the guaranteed win; do this first)
Goal: prove the green substrate tests are not flaky and survive load.
1. Add `scripts/reliability.sh`: run each green substrate test binary in a loop
   `N` times (default 20; `RELIABILITY_N` env), failing on the FIRST red, printing
   a pass/fail tally per binary. Bounded; no infinite loops.
2. Add `tests/substrate_reliability.rs`:
   - `repeat_discovery_is_stable` — spin/meet 2 nodes in a loop (e.g. 15×) within one
     test; assert every iteration meets within `SYNC_TIMEOUT`.
   - `concurrent_meshes_do_not_interfere` — several independent 2-node meshes (unique
     discovery keys) formed concurrently; all converge.
   - `larger_mesh_converges` — a 4–5 node mesh; assert all pairs/peers meet (uses the
     existing `meet`/`peers_per_group` oracles; respects the shared-DB rule — assert
     per-node `peers_per_group`/events, NOT shared storage).
   Bounded waits only; reuse `support::` helpers; add `[[test]]` if needed.
GATE: `./scripts/reliability.sh` green for all binaries at N=20, plus the new file
green 3× in a row → commit "reliability: stress + repeat suite (substrate stays green under load)".

## PHASE 2 — multi-process rig (stretch; additive; the honest snapshot path)
Goal: make `late_joiner_gets_full_snapshot` (and storage-truth assertions) honest by
giving each node its OWN process (hence its own global DB) — WITHOUT refactoring storage.
1. Add `src/bin/cyan_node.rs` (test-only bin; uses only the crate's PUBLIC API):
   boot a `NetworkActor` from env (`NODE_DB`, `DISCOVERY_KEY`, `RELAY=disabled|url`,
   `BOOTSTRAP_NODE_ID`, `NODE_ADDR` for the StaticProvider seam, `CONTROL` = a simple
   line-protocol on stdin/stdout or a localhost TCP port). Control verbs needed by the
   test: `join_group <id> [bootstrap]`, `seed_fixture` (create group/workspace/board/
   chats/file like the in-process harness), `count <kind>` (read this process's storage),
   `node_id`, `addr`, `quit`. No FFI, no lib edits.
2. Add `tests/support/multiprocess.rs`: spawn N `cyan_node` processes (each own temp
   `NODE_DB`), wire their addrs to each other, drive them over the control protocol,
   and assert on EACH process's own `count` (real per-node storage truth).
3. Add `tests/substrate_snapshot_mp.rs`: implement `late_joiner_gets_full_snapshot`
   for real — host seeds a group; a late joiner process joins; assert the joiner's
   OWN storage counts match the host's after sync. Bounded waits.
4. Remove the `#[ignore]` from the in-process `late_joiner_gets_full_snapshot` ONLY if
   you instead point it at the multi-process path; otherwise leave the in-process one
   ignored and add the green multi-process version alongside.
If the rig cannot be completed cleanly within the attempt budget, leave the new test
`#[ignore]` with a precise reason, ensure everything still COMPILES and the PHASE 1
work is intact, commit what's done, and record the blocker in STATUS. Do NOT leave the
tree red.
GATE: everything compiles; multi-process snapshot test green (or cleanly ignored with
reason) → commit "multiprocess rig: per-process DB snapshot truth".

## FINISH
Write `STATUS_OVERNIGHT.md`: what's green, what's ignored + why, the exact reliability
numbers (N, pass tallies), whether the multi-process snapshot test is green or blocked
(with the blocker), and confirm NO engine/FFI/storage source was modified. End session.
