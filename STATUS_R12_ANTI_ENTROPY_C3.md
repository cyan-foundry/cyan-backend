# STATUS — ROUND 12 backend, anti-entropy C3 (`feat/r12-backend`)

Scope: cyan-backend engine/FFI only. Additive (no FFI signature/shape removed or repurposed;
the one wire change — a new `SnapshotFrame::Metadata` field — is `#[serde(default)]`, so it is
back/forward compatible across mixed-version peers, exactly like `notes`/`pins` before it). iroh
0.95, no `unwrap`/`panic` on engine/FFI paths, bounded `tokio::time::timeout` waits in tests.

**Verify:** `cargo build` ✓ · `cargo clippy --all-targets -- -D warnings` ✓ clean (remaining
warnings are the upstream `xaeroid` crate + the pre-existing Cargo.toml dup-target notices, not this
change) · regression green: `substrate_anti_entropy_lanes` (4), `substrate_pin_activity` (8, C1/C2),
`workflow_lock_test` (3, D2/E1), `substrate_stress` (4 + 5 on-demand `#[ignore]`), `substrate_notes_mp`,
`substrate_files` (6), `substrate_templates_mp`, `--lib` (29).

---

## The gap C3 closes

C1/C2 made the **board-pin** (`board_metadata.is_pinned` / `pin_updated_at`) a convergent LWW delta
(`BoardPinned`), and D2/E1 added the per-board **workflow-state** (`board_workflow_state`:
deployed/dashboard/locked, LWW on `updated_at`). Both are merged correctly and the **snapshot already
carried board_metadata** — but the **anti-entropy `group_digest` covered neither lane**. The board
entry was hashed on `created_at` only, so flipping `is_pinned` never changed the group hash; the
workflow-state row wasn't in the digest *or* the snapshot at all.

Consequence: anti-entropy is the layer that repairs a **dropped** live delta (`Lagged` under load —
the whole reason the sweep exists). With these lanes invisible to the digest, a dropped `BoardPinned`
— or a deploy a peer was offline for — was **never detected and therefore never repaired**: it
diverged silently and permanently, defeating C1/C2 under exactly the load conditions anti-entropy was
built to handle. The repair *transport* was fine (snapshot serves `board_metadata`, applies it via the
LWW `board_metadata_upsert`); the **detector** was blind. C3 makes the detector cover everything the
repair can heal.

---

## Full audit — every convergent/LWW group-state lane vs. digest coverage

The repairable surface is exactly what the snapshot serializes/applies (an AE repair *is* a snapshot
pull). So the audit cross-references `snapshot::{build,apply}_snapshot_frames` against
`anti_entropy::group_digest`. **Any lane in the snapshot but not the digest is a silent-divergence gap.**

| Lane | Version key | In snapshot | In digest (before C3) | Verdict |
|------|-------------|:-----------:|:---------------------:|---------|
| group / workspaces / boards | `created_at` | ✓ | ✓ `g`/`w`/`b` | covered |
| whiteboard elements | `updated_at` | ✓ | ✓ `e` | covered |
| notebook cells | `updated_at` | ✓ | ✓ `c` | covered |
| chats | `timestamp` | ✓ | ✓ `h` | covered |
| notes (ROUND8 §W2) | `updated_at` | ✓ | ✓ `n` | covered |
| workflow-pin (ROUND8 §W4, `pins` table) | `updated_at` | ✓ | ✓ `p` | covered |
| files | `hash` | ✓ | ✓ `f` | covered |
| **board_metadata — pin lane** (`is_pinned`, R12 C1/C2) | `pin_updated_at` | ✓ | ✗ | **FIXED — new `bm` digest line** |
| **board_metadata — descriptive lane** (labels/rating/type/view/accessed) | `meta_updated_at` | ✓ | ✗ | **FIXED — folded into the same `bm` line (small)** |
| **board_workflow_state** (deployed/dashboard/locked, R12 D2/E1) | `updated_at` | ✗ | ✗ | **FIXED — added to snapshot + new `wf` digest line** |
| integration bindings | `created_at` | ✓ | ✗ | **tracked follow-up — OUT OF SCOPE (see below)** |

### Tracked follow-up (not fixed here, on purpose)

- **integration bindings** — in the snapshot, not in the digest, and immutable-by-id (`created_at`),
  so a dropped integration insert would not be reconciled by AE. This is a real gap, **but CLAUDE.md
  puts integrations out of scope** ("the Iggy enrichment pipeline, integration events … are moving to
  an MCP/workflow model — do not test or refactor them"). Folding `integrations` into the digest is a
  one-line change, but it would mean adding an integration test to prove it, which the project has
  explicitly deferred. **Tracked, not done.** When integrations are revisited under the MCP model, add
  an `i` digest line keyed on `created_at` next to `f`.

### Out of digest BY DESIGN (verified — not gaps)

These are convergent/LWW tables that are deliberately **not** group-state in the snapshot, so their
absence from `group_digest` is correct, not a hole:

- **user_profiles** (presence/display name, `updated_at` LWW) — node-scoped; propagated by presence
  gossip, never in the group-state snapshot.
- **group_peers / peer addresses** (roster, `updated_at`) — MESH_HARDENING persistent roster with its
  own gossip + reconnect path; not group content.
- **group_sync_state** (`synced_as_of`, `MAX`) — local catch-up watermark, purely local bookkeeping.
- **unread** (R11 account-scoped, board-only, idempotent) — per-account read state, not replicated as
  shared group state.
- **templates** (tenant-scoped, `INSERT OR REPLACE`) — tenant-global, replicated on its own path
  (`substrate_templates_mp` green), not group-scoped; outside `group_digest` by scope.
- **direct_messages** (peer-scoped), **file_transfers** (local transfer progress), **mesh_hold**
  (hold-and-serve payloads), **anonymous_sessions** (local) — local/peer-scoped, never group convergent state.

---

## The fix

**Digest (`src/anti_entropy.rs::group_digest`).** Two new lines, in the same style as the `n`/`p`
lanes added in ROUND8:
- `bm␁{board_id}␁{meta_updated_at}␁{pin_updated_at}` per `board_metadata` row — versioned on **both**
  LWW clocks the merge actually uses, so a dropped `BoardPinned` (pin lane) **or** `UpdateBoardMetadata`
  (descriptive lane) flips the hash. (Using the LWW clock — not the value — as the version means two
  peers that are genuinely converged hash identically, and an equal-clock tie never triggers a
  pointless repair pull.)
- `wf␁{board_id}␁{updated_at}` per `board_workflow_state` row. Default-authoring boards have no row ⇒
  no line ⇒ two peers with no deployments still hash identically.

**Snapshot (`src/snapshot.rs` + `src/models/protocol.rs`).** `board_workflow_state` wasn't carried at
all, so detection alone couldn't heal it. Added `workflow_states` to `SnapshotFrame::Metadata`
(`#[serde(default)]`, wire-compatible), to `build_snapshot_frames` (sent in **full** like
`board_metadata` — one tiny row per deployed board, so an incremental `since`-pull still carries it),
to `apply_snapshot_frame` (via the new LWW `workflow_state_upsert`), to `frame_row_count`, and to
`group_high_water` (the watermark now counts the board_metadata + workflow-state clocks so it stays
the true max version). board_metadata needed **no** snapshot change — it was already carried; only the
digest was blind.

**Storage (`src/storage.rs`).** Two additions, reusing the existing LWW conventions (no forked merge):
- `workflow_state_list_by_boards` — the read the digest + snapshot use.
- `workflow_state_upsert` — full-record LWW on `updated_at` (strictly-newer-wins, like `pin_upsert` /
  `board_metadata_upsert`): a stale clock never clobbers a newer deploy/lock, and re-applying the same
  state (a replayed/debounced repair frame) is an idempotent no-op.

---

## Proof

**Tier-1 — `tests/substrate_anti_entropy_lanes.rs` (in-process, deterministic, CI):** proves the three
properties the heal rests on, on `storage::*` / `group_digest` / the built frame (never log lines):
- `digest_detects_board_pin_and_workflow_state_changes` — the digest flips on a pin change and on a
  deploy (the gap), and is deterministic for identical state.
- `snapshot_frame_carries_board_pin_and_workflow_state` — the repair transport carries both lanes.
- `board_pin_lww_is_order_independent` / `workflow_state_lww_is_order_independent` — stale loses,
  newer wins, equal is a no-op (**no-stale-clobber**, both lanes).

(In-process nodes share one process-global DB, so they cannot diverge — the cross-process heal is Tier-2.)

**Tier-2 — `tests/substrate_stress.rs::dropped_board_pin_and_workflow_state_repaired_by_sweep`
(multi-process, CI):** real divergent storage (each `cyan_node` has its own DB); a local-only write is
a genuine divergence only anti-entropy can reconcile. Driven on the fast AE test cadence
(`CYAN_AE_SWEEP_MS`), bounded `converge_count` waits:
- **workflow-state, host→peers:** host deploys the fixture board with no broadcast → every peer
  reconciles `deployed` via the sweep.
- **board-pin, host→peers:** host pins with no broadcast (the dropped `BoardPinned`) → every peer
  reconciles `board_pins` via the sweep — the headline repair.
- **both directions + no stale clobber:** a *different* peer then unpins at a **newer** clock, also
  local-only → the mesh converges to unpinned **everywhere** (newer wins on every peer = the peer→host
  direction; the older pin it raced never clobbers it back).
- repair rode a **bounded, debounced** pull (`ae_repair` ≥ 1 and `< 50` on every peer — not a
  per-message storm), the same oracle as the element dropped-delta test.

**New test verbs** (deterministic, no RNG; mirror the existing `post_local`/`set_pin` stand-ins for a
dropped delta): `cyan_node` `set_board_pin` / `deploy_local` (local write, no broadcast, explicit LWW
clock) + `board_pins` / `deployed` count oracles; MP helpers `set_board_pin` / `deploy_local`.

---

## FFI for iOS

None new. `SnapshotFrame::Metadata` gains a `#[serde(default)]` `workflow_states` field — a peer↔peer
wire shape, additive and version-compatible; iOS does not parse snapshot frames. The board-pin and
workflow-state reads iOS already uses (`cyan_board_workflow_state`, `BoardPinned`/`BoardMetadataUpdated`
events) are unchanged; they now also reconcile after a dropped delta instead of staying diverged.

---

## Files touched

- `src/anti_entropy.rs` — `bm` + `wf` digest lines (+ doc).
- `src/snapshot.rs` — `workflow_states` in build/apply/`frame_row_count`/`group_high_water`.
- `src/models/protocol.rs` — `SnapshotFrame::Metadata::workflow_states` (`#[serde(default)]`).
- `src/storage.rs` — `workflow_state_list_by_boards` + `workflow_state_upsert` (LWW).
- `src/actors/topic_actor.rs` — destructure the new field in the snapshot-download log arm.
- `src/bin/cyan_node.rs` — `set_board_pin` / `deploy_local` verbs + `board_pins` / `deployed` counts.
- `tests/support/multiprocess.rs` — `set_board_pin` / `deploy_local` MP helpers.
- `tests/substrate_anti_entropy_lanes.rs` (new) — Tier-1 detect/carry/merge.
- `tests/substrate_stress.rs` — Tier-2 multi-process heal (both lanes, both directions, no clobber).

## Rules honored
- Additive: no FFI signature/shape removed or repurposed; the lone wire change is `#[serde(default)]`.
- Reused the existing LWW merges (`board_metadata_upsert`, the `pin_upsert`/`set_*` conventions) —
  did not fork them.
- No `unwrap()`/`panic!` on engine/FFI paths (`?`/`map_err`); tests use `expect`, bounded waits only.
- No assertion weakened; no spec edited to pass. Out-of-scope/balloon-risk lanes (integrations) are
  **listed as tracked follow-ups**, not silently dropped or force-fitted.
