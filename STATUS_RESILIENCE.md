# STATUS — resilience / chaos run (peer churn, lone node, rejoin, offline startup)

Branch `feat/substrate-e2e` (never touched `main`, never touched `~/cyan-iOS`, never ran
`build_static_lib.sh` / the xcframework). **Additive only** — no `src/**` engine/FFI/storage
source modified (see "Safety"). iroh 0.95 throughout; no version bumps. Date: 2026-06-19.

Two commits added this run, both on `feat/substrate-e2e`:
- `c714bb2` resilience: shutdown helper + peer-churn chaos
- `5709c41` resilience: rejoin + offline-startup finding encoded

No PR opened (left for the human).

---

## PHASE 0 — branch + baseline (GREEN)

- `git rev-parse --abbrev-ref HEAD` = `feat/substrate-e2e`; working tree clean (only the
  untracked run-plan `RESILIENCE_RUN.md`).
- `cargo build` ✅.
- Baseline substrate suite green (the STATUS_OVERNIGHT command):
  discovery 2/2, sync 4 + 1 ignored, chat 3 + 1 ignored, files 5 + 1 ignored, offline 3/3.
- Pre-existing `diagram_gen` failure and ~1000 whole-tree clippy warnings left untouched
  (out of scope per the standing rules).

## PHASE 1 — shutdown helper + peer-churn chaos (GREEN) — `c714bb2`

Harness (additive, `tests/support/mod.rs`):
- Store each node's actor `JoinHandle` on `Node`.
- Add `pub async fn shutdown(self)` — abort the actor task (dropping its gossip/topic/router)
  and `Endpoint::close().await` to "pull the plug" on a peer, consuming the node.
- **No existing public signature changed** (only the new field + method added).

`tests/substrate_resilience.rs` (new):
- **`lone_node_no_peers_degrades_gracefully`** — GREEN. A peerless fresh node spawns
  (the resilience suite never persists a group row, so its DB has nothing to load → it
  reaches its command loop), and `JoinGroup`/chat/file-request fired at it neither panic
  nor crash the actor; its `SwiftEvent` channel stays open (liveness = a short wait ends in
  *timeout*, not *channel closed*).
- **`peer_drops_others_keep_working`** — GREEN. 3 nodes meet; node-2 (a leaf) is
  `shutdown()`; the surviving two still deliver a fresh chat delta node-0 → node-1.
- **`last_remaining_peer_still_functional`** — GREEN. 2 nodes meet; the only peer is
  `shutdown()`; the survivor keeps its group topic (`has_group`, state intact) and stays
  alive after a broadcast on the now-peerless topic (channel open). Exactly the spec's bar
  ("broadcast doesn't error, local state intact"); no fresh gossip join is required.

## PHASE 2 — rejoin + offline-startup finding encoded (GREEN) — `5709c41`

- **`dropped_peer_rejoins_and_meets_again`** — GREEN. 3 nodes meet; node-2 is `shutdown()`;
  a replacement with the **same discovery key + group**, bootstrapped to the original seed,
  is wired and re-joins, then receives a fresh (unique-id) delta from the seed over the
  re-formed group topic — i.e. it rediscovered the mesh. Discovery/meet is asserted here;
  content re-sync stays with the multi-process rig.
- **`node_with_group_offline_startup_does_not_block`** — **IGNORED (finding encoded, NOT
  faked, NOT fixed).** A node with a group already in its DB cold-starts fully offline
  (relay disabled, mDNS only, no reachable bootstrap). The test asserts non-blocking
  startup (its command loop processes a new `JoinGroup` within `SYNC_TIMEOUT`). **Verified
  it times out as predicted** (run with `-- --ignored`: *"timeout after 30s … non-blocking
  startup"*), confirming the STATUS_OVERNIGHT engine finding — `start()` loads the persisted
  group and awaits `gossip.subscribe_and_join`, whose only candidate is the unreachable
  relay-only default bootstrap, so it parks until a neighbour connects and the command loop
  never runs. Marked
  `#[ignore = "engine: offline startup blocks on unreachable default bootstrap — see
  STATUS_OVERNIGHT; fix is babysit"]`. The engine startup path was left untouched.

### Why the offline test is ignored, not green

The fix lives in the engine startup path (`src/lib.rs`/`network_actor.rs`/`topic_actor.rs`:
stop blocking the command loop on an unreachable bootstrap), which is out of scope for this
additive run and is human babysit work. The test stands as the executable spec for that fix:
when the engine is fixed, remove the `#[ignore]` and it goes green. It is `#[ignore]`d so it
(a) does not gate the suite and (b) never persists a `groups` row that would block other
in-binary tests at their startup.

### Engine reality the tests live with (documented, not worked around)

`TopicActor::spawn` always injects the hardcoded relay-only default bootstrap and calls
`gossip.subscribe_and_join(..).await`, which blocks until ≥1 neighbour connects. So **any**
peerless `JoinGroup` (lone node, or a survivor joining a brand-new group) parks the command
loop — which is why the peerless-liveness oracle is the *event channel staying open*, not a
`has_group` flip, and why the offline cold-start finding above exists. `JoinGroup` does not
persist a `groups` row (only `seed_group_fixture`/`group_insert_simple` do), so the
churn/rejoin tests keep the shared DB group-row-free and fresh nodes start fine.

A second harness detail surfaced and handled honestly: `meet()`'s probe uses a **fixed**
payload (`__probe__{group}`), so calling `meet()` twice on the same group re-broadcasts an
identical message that iroh-gossip's duplicate-suppression drops — the second `meet()` never
re-confirms already-seen nodes. The rejoin test therefore asserts rediscovery with a
**unique-id delta** via `broadcast_until_received` instead of a second `meet()`. No assertion
was weakened; `meet()` and `SUBSTRATE_TEST_SPEC.md` were not edited.

---

## What's green

- `cargo test --test substrate_resilience`: **4 passed, 1 ignored** (`finished in ~15s`),
  stable across repeated runs.
- Full substrate suite re-run with the additive harness change: discovery 2/2, sync 4 + 1i,
  chat 3 + 1i, files 5 + 1i, offline 3/3, **resilience 4 + 1i** — all green.
- `cargo build` green.

## What's ignored + why

- `node_with_group_offline_startup_does_not_block` — the encoded engine finding above
  (offline cold-start blocks on the unreachable default bootstrap); verified-blocking, not
  faked; fix is human babysit work on the engine startup path.

## Clippy

New/changed files (`tests/substrate_resilience.rs`, `tests/support/mod.rs`) add **zero** new
clippy warnings (`cargo clippy --test substrate_resilience`, scoped grep clean). The
pre-existing whole-tree `-D warnings` red (~700 reported here, all in lib/other bins) is
unchanged and out of scope.

## Safety

- **No `src/**` source modified.** `git diff --name-only 89244ab HEAD -- src/` is empty.
  `git diff --stat 89244ab HEAD` shows only `tests/substrate_resilience.rs` (new) and
  `tests/support/mod.rs` (+19/-1, the additive `JoinHandle` field + `shutdown` method).
- `tests/support/mod.rs` existing public signatures unchanged (only added).
- `SUBSTRATE_TEST_SPEC.md` unedited; no assertion weakened.
- Stayed on `feat/substrate-e2e`; `main` and `~/cyan-iOS` untouched; no xcframework build.
- Bounded `tokio::time::timeout` waits throughout; no unbounded `recv()`, no sleep-as-sync;
  every "graceful" test completes within a deadline (a hang would be a surfaced FAILURE).
