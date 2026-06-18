# cyan-backend — Agent Context

cyan-backend is the **offline-first P2P engine** behind the Cyan app: a Rust
library (built on XaeroFlux + iroh 0.95, XaeroID identity) that the SwiftUI iOS
app drives over **FFI** (the `cyan_*` C ABI in `src/ffi/`), and that syncs
device-to-device over **iroh QUIC + gossip**. Hierarchy: Group → Workspace →
Board → Cell. Read this fully before changing anything.

## THIS IS STABLE, SHIPPING CODE — be very careful

The features that exist (discovery, snapshot+delta sync, chat, file transfer) work
today and the iOS app depends on them through the FFI. Treat every change as
production surgery:

- **Branch discipline.** Never commit to `main`. Never merge/rebase/force-push
  `main`. Do all work on a feature branch; leave PRs/merges to the human. If you
  find yourself on `main`, STOP.
- **Never change shipping behavior as a side effect.** Refactors that add a seam
  (e.g. threading `NodeConfig` into `NetworkActor::new`) must keep the existing
  FFI init path behaving identically — build the new config from the current
  globals/env at the call site. A seam is not a rewrite.
- **The FFI contract is load-bearing.** `cyan_*` signatures, the component
  command/event JSON shapes, and `SwiftEvent`/`NetworkCommand` variants are
  consumed by cyan-iOS. Don't rename, reorder, or repurpose them without a noted
  reason; prefer additive change.
- **Small, reviewable diffs.** One concern per commit. If a change balloons past
  ~500 lines or touches the FFI surface broadly, pause and write down why.
- **No `unwrap()`/`panic!` in engine or FFI paths.** Use `?` / `map_err`. Panics
  cross the FFI boundary as crashes in the iOS app.

## Build & test

```bash
cargo build
cargo test                       # includes the substrate suite (tests/substrate_*.rs)
cargo clippy --all-targets -- -D warnings
```

The existing multi-process test bins (`network_test`, `snapshot_test`,
`delta_test` in `Cargo.toml`) run host/join across two machines. The new
**in-process** substrate suite (`tests/substrate_*.rs` + `tests/support/`) spins N
nodes in one process — see `SUBSTRATE_TEST_SPEC.md`.

## Substrate test discipline (tests/substrate_*.rs, tests/support/)

- **`SUBSTRATE_TEST_SPEC.md` is the contract.** Do not edit it to make code pass.
- **Never weaken an assertion.** If a test looks wrong, stop and ask. The
  `todo!()`s in `tests/support/mod.rs` are meant to be implemented; its public
  signatures are the interface — keep them stable.
- **Bounded waits only.** Every wait is a `tokio::time::timeout` with a clear
  failure; never an unbounded `recv()`, never `sleep`-as-synchronization.
- **Assert on the receiver's `storage::*`**, not on log lines.
- **iroh 0.95 only** — no APIs from 1.x; do not bump the version.
- **In-process scope** = discovery, snapshot+delta sync, chat, file transfer over
  loopback, and offline (`RelayPolicy::Disabled`). The **relay / WebSocket rungs**
  (G2 ladder, G8-R, G11) are NOT in-process — they need the Docker rig in
  `cyan-local-harness/`. Leave them as `#[ignore]` red scaffolds here.
- **If the engine genuinely lacks a capability a test needs**, that is a real
  finding: note it, mark the test `#[ignore]` with the reason, and keep going —
  do not hack the engine to fake a pass.

## Out of scope for substrate work

The Iggy enrichment pipeline, integration events, and nudges/asks/decisions
extraction are moving to an MCP/workflow model — **do not test or refactor them**
as part of substrate hardening. Cyan Lens is an HTTP client leg (`cyan_lens_client.rs`),
not a mesh peer; the only substrate property that matters is that the mesh works
fully with Lens unreachable.

## Layout

`src/ffi/` (C ABI), `src/actors/` (network_actor, discovery_actor, topic_actor),
`src/models/` (commands, events, …), `src/storage.rs` (SQLite, the assertion
oracle). `tests/` = the multi-process bins + the in-process substrate suite.
