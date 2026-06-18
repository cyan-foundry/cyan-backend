# Autonomous backend-hardening run (one shot)

Paste this whole file as the prompt into `claude --dangerously-skip-permissions`
from `~/cyan-backend`, then walk away. It runs four phases with hard gates; it
commits after each green phase and STOPS with a STATUS.md if any gate fails.

---

You are hardening the cyan-backend P2P substrate, unattended, in one session.
Read `CLAUDE.md` and `SUBSTRATE_TEST_SPEC.md` fully first. Work through the PHASES
in order. Between phases, run the GATE; only advance when it is green.

## Standing rules (apply to every phase)
- **BRANCH DISCIPLINE — this is stable production code.** NEVER commit to `main`.
  NEVER merge, rebase, or fast-forward `main`. All work happens on the branch
  created in PHASE 0. If for any reason you find yourself on `main`, STOP
  immediately and write STATUS.md. Do not `git checkout main`, do not delete
  branches, do not force-push.
- **Gate = green build, tests, lints.** A phase is done only when:
  `cargo build` ✅, `cargo test` ✅ (ignored stay ignored), and
  `cargo clippy --all-targets -- -D warnings` ✅.
- **Iterate to green within a phase** (patch, re-run). If after ~6 honest attempts
  a gate is still red, **STOP**: write `STATUS.md` (what's green, what failed, the
  exact error, your hypothesis) and exit. Do **not** start the next phase on red.
- **Commit after each green phase**: `git add -A && git commit -m "<phase>"`. This
  preserves progress; never amend or force-push.
- **Stay inside cyan-backend.** Do NOT run `build_static_lib.sh`, `push_dylib.sh`,
  or any script that writes outside this repo; do NOT touch `~/cyan-iOS` or build
  the xcframework. That (and the FFI symbol check) is a human step done later. You
  only run `cargo …` and `git …` within this repo.
- **Progress for remote watching.** Keep a `PROGRESS.md`: append a timestamped line
  at the start and end of each phase (what you're doing + gate result). After each
  green-phase commit, if a remote `origin` exists, run
  `git push -u origin feat/substrate-e2e` — the FEATURE branch ONLY, NEVER `main`.
  This lets a human watch commits + PROGRESS.md from the GitHub mobile app.
- **Never weaken the contract.** Do not edit `SUBSTRATE_TEST_SPEC.md`. Do not
  weaken assertions in `tests/substrate_*.rs`. The `todo!()`s in
  `tests/support/mod.rs` ARE for you to implement; its **public signatures** stay.
- **No unwrap/panic in engine code**; bounded `tokio::time::timeout` waits in tests
  (never unbounded `recv()`); iroh **0.95** only; assert on the receiver's
  `storage::*`, not log lines.
- **In-process scope only.** Do NOT attempt the relay or WebSocket rungs — they
  need network isolation (the Docker rig), not this run. Scaffold them red (below).

## PHASE 0 — branch safety (do this FIRST, before any edit or commit)
1. `git rev-parse --abbrev-ref HEAD` — confirm starting point. `git status -s`
   should show only the untracked substrate files (SUBSTRATE_TEST_SPEC.md,
   HARDEN_BACKEND_RUN.md, tests/support/, tests/substrate_discovery.rs). If there
   are OTHER uncommitted changes you didn't make, STOP and write STATUS.md.
2. Create and switch to a work branch: `git checkout -b feat/substrate-e2e`.
   (The untracked files come with you; `main` stays clean and untouched.)
3. Verify you are NOT on main: `git rev-parse --abbrev-ref HEAD` must print
   `feat/substrate-e2e`. Every commit in later phases lands here, never on main.
   Do NOT open a PR or merge — leave that to the human.
4. **Baseline must be green before you edit.** Run `cargo build` (and
   `cargo test`) on the pristine tree. If it does NOT build/pass as-is, STOP and
   write STATUS.md — we start from green, never chase pre-existing red. Create
   PROGRESS.md with the baseline result, then begin PHASE 1.

## PHASE 1 — engine: injectable per-node config (the prerequisite)
Goal: multiple NetworkActors in one process with different relay/discovery, plus a
relay-disabled path. SHIPPING BEHAVIOR MUST NOT CHANGE.
1. Add canonical types in `src/models` (e.g. `node_config.rs`, re-exported):
   `NodeConfig { relay: RelayPolicy, discovery: DiscoveryPolicy, discovery_key: String }`,
   `RelayPolicy { Disabled, Url(String), Default }`,
   `DiscoveryPolicy { MdnsOnly, Bootstrap(String) }`.
2. Add a 4th param `cfg: NodeConfig` to `NetworkActor::new(...)`. Replace the
   `RELAY_URL`/`DISCOVERY_KEY` OnceLock reads inside with `cfg` fields. Add the
   `RelayMode::Disabled` branch. Factor a pure
   `fn relay_mode_for(&RelayPolicy) -> RelayMode` and **unit-test it**
   (Disabled→Disabled, Url→Custom, Default→Default).
3. Update the production FFI init site to build a `NodeConfig` from the existing
   globals/env so behavior is unchanged (seam, not change). Update the 3 existing
   test bins' `new(..)` calls (mechanical).
GATE → commit "phase1: NodeConfig seam".

## PHASE 2 — harness: implement MeshHarness
Implement the `todo!()`s in `tests/support/mod.rs` using the setup pattern from
`tests/network_actor_test.rs` (tempdir DB, `storage::init_db`, `SecretKey::generate`,
unbounded channels, `peers_per_group`, `NetworkActor::new(.., NodeConfig)`,
`spawn(actor.start(cmd_rx))`). Map the harness `NodeCfg` to the engine `NodeConfig`
(or switch the harness to import the engine types and delete the dupes).
Make `tests/substrate_discovery.rs::two_nodes_meet_via_mdns_on_lan` pass, then
`two_nodes_meet_via_bootstrap`. If `cargo test --test substrate_discovery` isn't
picked up, add a `[[test]]` entry. Bounded waits; assert mutual discovery via
`PeerJoined`/`peers_per_group`.
GATE → commit "phase2: MeshHarness + discovery green".

## PHASE 3 — fan out the in-process suite
Create and make green, one file at a time (commit after each green file):
- `tests/substrate_files.rs` — G6/G8: `file_shared_at_{group,workspace,board}_scope`,
  `large_file_100mb_transfers_intact` (blake3 match + `local_path` set).
- `tests/substrate_sync.rs` — G3/G4: `late_joiner_gets_full_snapshot`,
  `delta_board_element_propagates`, `delta_notebook_cell_propagates`,
  `delta_workspace_structure_propagates`, `three_node_convergence`.
- `tests/substrate_chat.rs` — G5/G7: group/workspace/board chat reaches peers,
  `chat_with_attachment_shares_file_into_scope`.
- `tests/substrate_offline.rs` — G9: re-run discovery+chat+file with
  `RelayPolicy::Disabled` + `MdnsOnly`; assert no reliance on any non-LAN endpoint.
Confirm each event/command name against the real `SwiftEvent`/`NetworkCommand`/
`storage::*` before asserting; if the engine genuinely lacks a capability a test
needs, that's a real finding — note it in STATUS.md, mark that test `#[ignore]`
with the reason, and keep going.
GATE after each file → commit.

## PHASE 4 — scaffold the deferred rungs red, then report
- Create `tests/substrate_relay.rs` and `tests/substrate_swarm.rs` with their
  named tests from the spec as `#[ignore]` + `todo!()` (they need the Docker rig /
  swarming work). They must compile.
- Create `tests/substrate_lens.rs` with `mesh_fully_functional_with_lens_unreachable`
  as `#[ignore]` (optionality test; needs lens wiring).
GATE (everything compiles, ignored stay ignored) → commit "phase4: red scaffolds".

## FINISH
Write `STATUS.md`: per-file, per-named-test green/red/ignored; the engine call
sites you touched; any findings or capabilities the engine lacked. End the session.
