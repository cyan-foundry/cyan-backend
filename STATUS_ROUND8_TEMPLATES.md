# STATUS — Round 8 / W4: Templates + pinned workflows

**Branch:** `feat/round8-templates` (off `feat/round8-workspaces`)
**Scope:** backend only. Additive FFI. iOS template gallery / pinned row / "create from
template" is batch 2a (not here). xcframework NOT rebuilt (the chain script does it).

## What shipped

A **template** = a pre-written English workflow (steps + bound plugins) you clone into a
board. W4 ships the backend contract for that, plus **pinned workflows**:

### The built-in media seed set (`src/templates.rs`)
Three media seeds, the spec's verbatim names, always present for every tenant (code
constants — never persisted, no migration seeding, deterministic across peers):
1. **"Transcode master → deliver to Contido"** — transcode → deliver, with the **Contido
   plugin bound** to the delivery step (a template = steps + bound plugins).
2. **"Transcribe + compliance QC"** — transcribe → compliance QC.
3. **"Conform + approve + master"** — conform → gate for approval → master.

### Templates module (`src/templates.rs`)
- `seed_templates()` — the built-in set (`source = "builtin"`, tenant-agnostic).
- `list_templates(tenant)` — seeds **+** the tenant's user-saved templates (tenant-scoped).
- `get_template(id, tenant)` — a seed (tenant-agnostic) or one of the tenant's own user
  templates; `None` for an unknown id **or** a user template owned by another tenant.
- `save_as_template(tenant, name, desc, steps)` — persists a board's steps as a reusable
  **user** template (tenant-scoped, generated id).
- `clone_to_board(template_id, board, tenant)` — materializes **real W1 `step` cells**
  (one per template step, appended in order, text verbatim, bound plugin recorded in the
  cell's `metadata_json`). The clone compiles + syncs like any authored step. Returns the
  created cells so the command path can broadcast each.

### Storage (`src/storage.rs`)
- New `templates` table (idempotent migration, exercised on a DB that predates it):
  `template_insert` / `template_list_by_tenant` / `template_get` (tenant-scoped read —
  no cross-tenant leak). Steps stored as JSON; **only user templates are persisted**.
- New `pins` table (idempotent migration): `pin_upsert` (idempotent upsert-by-`board_id`,
  **LWW on `updated_at`**, like `note_upsert`), `pin_get`, `pin_list_by_boards`.

### Pin state replicates via the existing path (no new transfer)
A pinned workflow is a **board-level pin** (`PinDTO { board_id, tenant_id, pinned,
updated_at }`) in its own store, so it rides exactly what notes ride:
- **`group_digest`** (`src/anti_entropy.rs`): a `p␁<board_id>␁<updated_at>` line, versioned
  on `updated_at`, so a pin/unpin flips the hash and the bounded sweep pulls the latest.
- **Snapshot `Metadata` frame** (`src/models/protocol.rs`, `#[serde(default)]` ⇒
  wire-compatible both ways): pins serialize in the snapshot server
  (`network_actor.rs`) and apply via the idempotent LWW upsert on the receiver
  (`topic_actor.rs`) — a cold joiner converges, and an already-live peer converges via
  the anti-entropy merge.
- **Live path**: `PinSet` event (`models/events.rs`) broadcast by the `SetPin` command
  (`lib.rs`), applied via `pin_upsert` on receive.

### Additive FFI verbs (the iOS batch consumes these)
- `cyan_template_list(tenant_id) -> json` — seeds + the tenant's user templates.
- `cyan_workflow_from_template(template_id, board_id, tenant_id)` — clone into a board
  (broadcasts each cloned step).
- `cyan_pin_set(board_id, pinned)` — set replicated pin state (LWW).
- `cyan_template_save(tenant_id, name, description, steps_json) -> json` — save-as-template.

All additive. No existing `cyan_*` signature, event, or command was renamed/reordered/
repurposed. New `CommandMsg::{WorkflowFromTemplate, SetPin}`, `NetworkEvent::PinSet`.

## Tests (all named per §W4; none weakened)

Storage / module (`tests/templates_test.rs`, 4/4 green):
- `seed_templates_present` — the three media seeds present for any tenant; each is
  built-in with pre-written steps; the delivery seed binds the Contido plugin.
- `clone_template_creates_workflow_steps` — clone → one real `step` cell per step, in
  order, text verbatim; the clone compiles to a plan.
- `save_as_template` — captures steps as a user template, tenant-scoped, retrievable,
  listed alongside seeds, and itself clonable.
- `template_tenant_scoped` — tenant A's saved template never appears in tenant B's
  list/get; seeds appear for both.

Multi-process convergence (`tests/substrate_templates_mp.rs`, 1/1 green):
- `pin_state_syncs_across_peers` — two real `cyan_node` OS processes. Host pins the
  fixture board **locally only** (no broadcast); ONLY the anti-entropy digest+snapshot
  path can carry it, and the joiner converges to the pinned state. Asserted on each
  peer's own `count pins` (storage), never on logs. Bounded waits; iroh 0.95.

Harness additions (additive): `count pins` kind + `set_pin` boot verb in `cyan_node`,
`MpNode::set_pin` helper.

## No regression
- W4 suites green: `templates_test` 4/4, `substrate_templates_mp` 1/1.
- Replication path re-run with the new `pins` Metadata field + digest line:
  `substrate_snapshot_mp`, `substrate_sync`, `substrate_notes_mp`,
  `substrate_workspaces_mp`, `workflow_step_test`, `workspaces_test`, `notes_test` — all ✅.
- `cargo build --tests` green. My changed surface is clippy-clean; the repo base is not
  `-D warnings`-clean (pre-existing unused-import lints in untouched files) and the one
  `cyan_node` `unwrap` clippy lint (the `metrics` json macro, line ~692) predates W4 — no
  new lint introduced.
- Full `cargo test --no-fail-fast` on this branch: **112 passed, 2 failed** — both
  **pre-existing, NOT W4 regressions** (each reproduced on the clean
  `feat/round8-workspaces` tip via an in-place stash):
  - `diagram_gen::tests::test_parse_diagram_json` — fails on the base too (the file is
    byte-identical to `feat/round8-workspaces`).
  - `substrate_multiuser_mp::expired_revoked_replayed_grant_rejected` — a timing-flaky
    grant/QUIC test: it fails on the clean base tip too, at a *different* assertion
    (line 190 vs 179) and *different* runtime (24s vs 80s) run-to-run. It touches grant
    verification + snapshot serving, neither of which W4 changes; the full snapshot path
    *with* the new `pins` frame field is independently green
    (`substrate_snapshot_mp`, `substrate_sync`).
  - (`substrate_stress::swarm_blob_multi_fetch_integrity` remains the documented
    single-box CPU/port contention flake when stacking the heaviest MP scenarios.)

## Tier-2 / deferred (out of scope here)
- iOS template cards gallery + pinned row + "create from template" — batch 2a
  (`template_gallery_lists_seeds`, `clone_from_card_opens_workflow`, `pin_toggles_and_persists`).
- User-saved templates are intentionally **local/tenant-scoped** (not replicated) — only
  **pin state** syncs per the spec (`pin state (syncs)`). Replicating user templates
  across peers is a later affordance if the product wants it.
- Pinning is **team state** (pinned-by-anyone surfaces for the group). Per-user pins, if
  wanted, are a later split.

## New FFI verbs the iOS batch will consume
`cyan_template_list`, `cyan_workflow_from_template`, `cyan_pin_set`, `cyan_template_save`.
