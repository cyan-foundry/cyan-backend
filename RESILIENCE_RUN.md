# Resilience / chaos test run — additive, in-process (safe to run autonomously)

Adds the failure-mode coverage (peer churn, lone node, rejoin, offline startup) that
the first substrate pass scoped out. Additive only: a new test file + one test-only
harness helper. No engine/FFI/storage edits. Paste into
`claude --dangerously-skip-permissions` from `~/cyan-backend` and leave it.

See `../anthropic_data_dump/RESILIENCE_TEST_ADDENDUM.md` for the full rationale (the
relay/WebSocket/mid-transfer chaos tests are NOT here — they need the Docker/netns rig).

---

You are adding in-process resilience tests to the cyan-backend substrate, unattended.
Read `CLAUDE.md`, `SUBSTRATE_TEST_SPEC.md`, and `STATUS_OVERNIGHT.md` first (the last
documents the offline-startup-block finding — you will ENCODE it as a test, NOT fix it).
Work the PHASES in order; gate between them; commit per green phase; stop clean on red.

## Standing rules (absolute)
- **Branch:** stay on `feat/substrate-e2e` (PHASE 0 verifies). NEVER `main`, NEVER
  `~/cyan-iOS`, NEVER run `build_static_lib.sh` / build the xcframework.
- **ADDITIVE ONLY.** You may ADD `tests/substrate_resilience.rs` and ADD a `shutdown()`
  method (+ store the actor `JoinHandle`) to `tests/support/mod.rs` — that's test
  harness, not engine. You may NOT edit any `src/**` lib/engine/FFI/storage code, and
  you may NOT change existing public signatures in `tests/support/mod.rs` (only add).
- **Do NOT fix the engine.** The offline-startup-block (a node with a group in its DB
  cold-starting offline blocks on the unreachable default bootstrap — STATUS_OVERNIGHT
  §"Engine finding") is a real bug, but fixing it is human babysit work. If a test would
  require that fix to pass, mark it `#[ignore]` with the precise finding and move on —
  do NOT touch the startup path.
- **Gate** = `cargo build` ✅; the targeted suites green via
  `cargo test --no-fail-fast --test substrate_resilience` (+ existing substrate tests
  still green); your new files add **zero** new clippy warnings. The pre-existing
  `diagram_gen` failure and ~1000 clippy warnings are out of scope.
- ≤6 honest attempts per phase, else STOP + write `STATUS_RESILIENCE.md`. Commit after
  each green phase (`git add -A && commit`); push `feat/substrate-e2e`. Bounded
  `tokio::time::timeout` waits only — never an unbounded `recv()`; a "graceful" test must
  itself complete within a deadline (a hang is a FAILURE, surfaced, not an infinite wait).
  Never weaken an assertion or edit `SUBSTRATE_TEST_SPEC.md`.

## PHASE 0 — branch + baseline
1. `git rev-parse --abbrev-ref HEAD` == `feat/substrate-e2e`; `git status` clean.
2. `cargo build` + the existing substrate suite green (per the STATUS_OVERNIGHT command).
   If not, STOP + STATUS. Note baseline.

## PHASE 1 — harness helper + core chaos (additive)
1. In `tests/support/mod.rs` (additive): store each node's actor `JoinHandle` on `Node`
   and add `pub fn shutdown(&self)` / `pub async fn shutdown(self)` that aborts the actor
   task and drops the endpoint — "pull the plug" on a peer. Public signatures stay; only add.
2. `tests/substrate_resilience.rs`:
   - `lone_node_no_peers_degrades_gracefully` — spawn ONE fresh node (no peers, no group);
     `join_group`/chat/file-request return or no-op **within a deadline**, no panic, no
     deadlock. (A fresh node has no group to load, so it should start fine — this proves it.)
   - `peer_drops_others_keep_working` — 3 nodes meet; `shutdown()` one; assert the other two
     still meet and a delta/chat from one reaches the other within `SYNC_TIMEOUT`.
   - `last_remaining_peer_still_functional` — 2 nodes meet; shut one down; the survivor still
     processes commands (e.g. broadcast doesn't error, local state intact).
GATE → commit "resilience: shutdown helper + peer-churn chaos".

## PHASE 2 — rejoin + the offline-startup finding (encode, don't fix)
- `dropped_peer_rejoins_and_meets_again` — meet, `shutdown()` a peer, spawn a replacement
  with the same discovery key/group, assert it rediscovers the mesh within `SYNC_TIMEOUT`.
  (Content re-sync belongs to the multi-process rig; assert discovery/meet here.)
- `node_with_group_offline_startup_does_not_block` — a node that already has a group in its
  DB starts with relay disabled and NO reachable bootstrap. Per the STATUS finding this
  currently BLOCKS at startup. Write the test to assert non-blocking startup within a
  deadline; if it blocks/fails, mark it `#[ignore = "engine: offline startup blocks on
  unreachable default bootstrap — see STATUS_OVERNIGHT; fix is babysit"]` so it stands as
  the executable spec for that fix. Do NOT edit the engine to make it pass.
GATE → commit "resilience: rejoin + offline-startup finding encoded".

## FINISH
Write `STATUS_RESILIENCE.md`: per-test green/ignored + reasons, confirm the offline-startup
test is ignored-not-faked, and confirm NO `src/**` engine source was modified (git diff of
tracked files should show only `tests/**` + docs). End session.
