# STATUS — Dashboard producer events + signed-grant QR client FFI (Round 7)

Branch: `feat/dashboard-qr` (off `feat/anti-entropy`; **never `main`**). Contracts:
`../anthropic_data_dump/DASHBOARD_CONTRACT.md`, `IDENTITY_RBAC_SPEC.md`,
`STATUS_IDENTITY_GRANTS.md`, and cyan-iOS `STATUS_IOS_LOGIN_PRESENCE.md` (which stubbed
`issueGrantQR`/`scanGrantQR` waiting for these verbs). **Additive only** — no existing
`SwiftEvent`/`cyan_*`/`NetworkCommand` renamed, reordered, or repurposed.

> ⚠️ **XCFRAMEWORK MUST BE REBUILT.** This adds **new `cyan_*` C ABI verbs**
> (`cyan_issue_grant_qr`, `cyan_scan_grant_qr`) **and new `SwiftEvent` variants** (the
> dashboard events). cyan-iOS cannot call the verbs or decode the events until
> `Cyan/CyanBackend.xcframework` is **relinked** against this build. The xcframework was
> **NOT** rebuilt here (per instructions). Flip iOS `CyanBackendFFI.issueGrantQR/
> scanGrantQR` off their "unavailable" stub once relinked.

---

## PART A — Dashboard-producer events (live observability)

The dashboard is a **consumer**; this makes the engine the **producer**. The REAL run path
(`pipeline::run_pipeline`) now emits additive, **receive-only** `SwiftEvent`s as state
changes and computes a rolled-up `DashboardSnapshot` from the obs/cost rail. Nothing is
faked — the dashboard lights up from an actual run.

### New `SwiftEvent` variants (`src/models/events.rs`) — additive, receive-only
```
WorkflowRunStarted   { tenant_id, run_id, board_id, workflow_id, workflow_label, total_steps, started_at }
StepStateChanged     { tenant_id, run_id, board_id, workflow_id, step_id, name, stage, state, actor, plugin?, at }
                       state ∈ pending|running|awaiting_approval|approved|done|failed ; actor ∈ human|ai
StepProgress         { tenant_id, run_id, board_id, workflow_id, step_id, stage, processed, total, current_item?, detail? }
ApprovalRequested    { tenant_id, run_id, board_id, workflow_id, step_id, name, stage, requested_at }
ApprovalResolved     { tenant_id, run_id, board_id, workflow_id, step_id, stage, decision, by, at }
WorkflowRunFinished  { tenant_id, run_id, board_id, workflow_id, state, finished_at }   // state ∈ done|failed|cancelled
WorkflowStatsUpdated { tenant_id, run_id, board_id, workflow_id, snapshot: DashboardSnapshot }
```
Every variant carries `tenant_id` + the scoping keys (`run_id`/`board_id`/`workflow_id`);
per-step variants also carry the `stage`/`actor`/`plugin?` slicing dimensions. Routed to the
existing **`network`/`status`** poll buffer in `route_event_to_buffers` (`src/lib.rs`) — they
ride `cyan_poll_events("status")` like any live update. **No new client COMMAND FFI** (events
are receive-only, exactly as `DASHBOARD_CONTRACT` §A2 / `MCP_ARCHITECTURE` §5 require).

### `DashboardSnapshot` producer (`src/dashboard.rs`)
A new pure module owns the read-model + aggregation, kept separately testable:
- `DashboardSnapshot { tenant_id, board_id, run_id, workflow_id, workflow_label, updated_at,
  current_item, current_stage, items_processed, items_total, totals, per_stage,
  gate_minutes_by_stage }`.
- `Totals { wall_minutes, human_minutes, ai_minutes, files_processed, est_cost_usd }`.
- `StageStat { stage, state, minutes, human_minutes, ai_minutes, cost_usd, plugins }`.
- `RunObs` accumulates per-step `StepObs` and computes the snapshot (group-by-stage,
  first-appearance order). `items_processed`/`files_processed` = steps in a terminal state.
- **Cost rail** (`StepCost::est_usd`, `DASHBOARD_CONTRACT` §C): `Σ(tokens×rate) +
  Σ(gpu_ms×rate) + external-plugin USD`. Rates are documented consts.

### Wiring (`src/pipeline.rs`)
- `run_pipeline` tags the run (`tenant_id` = the board's **group** — matches mesh
  `tenant=group_id`, falling back to `CYAN_TENANT_ID`/`device`; `workflow_id` = `board_id`,
  the board IS the workflow surface; `workflow_label` = board name), emits
  `WorkflowRunStarted`, then per step: `StepStateChanged(running)` + `StepProgress`; a
  `manual` step → `awaiting_approval` + `ApprovalRequested`; deps-pending → `pending`; on
  finish → `done`/`failed`. It feeds a `RunObs` and, at the end, emits
  `WorkflowStatsUpdated{snapshot}` + `WorkflowRunFinished`.
- `approve_step` now also emits `ApprovalResolved` + `StepStateChanged(approved)` (additive
  `event_tx` arg; `cyan_pipeline_approve` passes `system.event_tx`).
- Added an **additive `stage: Option<String>`** to `PipelineStepConfig` (`#[serde(default)]`
  → absent means the step is its own stage; iOS Codable ignores it). It is the per-stage
  grouping key.
- The existing `StatusUpdate` pipeline emissions are **unchanged** (kept alongside) — no
  shipping behavior altered.

**Honest limitation (noted, not faked):** for AI/lens steps the cost rail uses each step's
compute **wall time as a `gpu_ms` proxy** (a real, if rough, number); external plugin USD is
attributed on the executor's own obs and is **not yet threaded back** into the run snapshot
(it would arrive on a later recomputed snapshot). Human/gate minutes within a single
synchronous run are 0 (approval is a separate later FFI call); the snapshot recomputes on the
next run. The pure unit test exercises the full per-stage minutes / gate-attribution / cost
formula deterministically.

## PART B — Signed-grant QR client FFI verbs

The mesh-half capability `Grant` (signed, expiring, revocable — `STATUS_IDENTITY_GRANTS`)
already existed with **no client FFI**. This exposes it to the app with the names iOS expects.

### New module `src/identity/qr.rs`
- `GrantInvite { group_id, group_name, group_icon?, group_color?, inviter_node_id, grant }`
  — the QR envelope: the signed grant + the group identity + the inviter's node id (the
  bootstrap peer to dial). `to_qr_payload`/`from_qr_payload`.
- `issue_grant_qr(...)` — signs via `Grant::issue` (which **rejects a non-admin issuer**) and
  packs the invite.
- `scan_grant_qr_at(qr, now)` — decode + **local pre-verify** (signature · expiry · group
  match), returning a joinable invite. The authoritative **issuer-is-admin · revocation ·
  anti-replay** checks are the snapshot **holder's** job at join time
  (`MeshAuthorizer::authorize_snapshot`) — the only referee that knows the group's current
  admins/revocations. A scan that passes locally can still be refused by the holder (by design).

### New `cyan_*` FFI verbs (`src/ffi/core.rs`) — additive
- **`cyan_issue_grant_qr(group_id, role, ttl_seconds) -> *char`** (iOS `issueGrantQR`).
  **Admin/Owner only**: gated on `storage::group_is_owner` (the persisted mesh authority =
  Owner). Signs with the node's own identity (`sys.secret_key`), `inviter_node_id` = this
  node. Returns `{"success":true,"qr":...,"nonce":...,"expiry":...,"role":...}` or
  `{"success":false,"error":...}`.
- **`cyan_scan_grant_qr(qr_payload) -> *char`** (iOS `scanGrantQR`). Pre-verifies, then
  **joins** by reusing the existing join path (refactored a `join_from_invite` seam shared
  with `xaero_join_group_from_invite` — behavior identical), forwarding the signed grant so
  the holder authorizes the **grant-gated per-group snapshot**. Returns the same JSON shape as
  `xaero_join_group_from_invite`, or `{"success":false,"error":...}` for a malformed / forged
  / expired QR. Secrets are never logged.

## Tests — all GREEN (test-first), default `cargo test` needs no live deps

`tests/dashboard_test.rs`:
- `run_emits_dashboard_events_in_order_with_correct_snapshot` — a REAL `run_pipeline` over a
  seeded board (one offline-failing local-plugin step + one manual gate) emits
  `WorkflowRunStarted` first, `running → progress → terminal` per step, the manual gate's
  `awaiting_approval` + `ApprovalRequested`, exactly one `WorkflowStatsUpdated`, and
  `WorkflowRunFinished` last — all correctly tagged; snapshot has both stages + the plugin tag.
- `stats_snapshot_perstage_minutes_cost_gate_correct` — pure `RunObs`→snapshot over a scripted
  3-stage run: per-stage minutes, **gate minutes attributed to the right stage**, the cost
  rail (`tokens×rate + gpu_ms×rate + external`), and totals — all exact.
- `events_tenant_scoped` — two runs in two groups; every event carries its own run's
  `tenant_id`/`board_id` and a single `run_id` (no cross-tenant leakage).

`tests/qr_test.rs`:
- `qr_issue_requires_admin` — admin issues a decodable, signed invite; a non-admin gets
  `GrantError::NotAuthorized`.
- `qr_scan_verifies_and_joins` — scan pre-verifies → the holder `authorize_snapshot` serves
  the per-group snapshot at the granted role and records the joiner as a writer; a replay of
  the same grant is refused.
- `expired_or_revoked_qr_rejected` — an expired grant is rejected at scan (explicit clock); a
  revoked grant is refused by the holder's snapshot gate (`VerifyError::Revoked`).

### Suite status
- New tests: **6/6 green.** `tests/grant_test.rs` (7) + `tests/substrate_identity.rs` (2)
  still green — the grant primitive is unchanged (QR only ADDS).
- **In-process substrate suite: green** (chat 4, discovery 2, files 5, identity 2, offline 3,
  offline_multiuser_mp 2, presence 1, reliability 3, resilience 5, snapshot_mp 1, swarm 5,
  sync 4; pre-existing `#[ignore]`s unchanged).
- **Pre-existing failures, NOT caused by this change** (verified): the lib unit test
  `diagram_gen::tests::test_parse_diagram_json` fails on `feat/anti-entropy` too;
  `substrate_multiuser_mp::expired_revoked_replayed_grant_rejected` (a **multiprocess** test)
  fails identically on the base branch; the two `substrate_stress` chaos tests
  (`concurrent_edits_converge_no_dupes`, `dropped_delta_is_repaired_by_next_sweep`) **pass
  cleanly in isolation** (~4s each) and only flaked when run after a 267s CPU-saturating
  stress pass under the machine-wide `serial()` lock.
- Clippy: the new modules/tests add **zero** warnings (the new FFI verbs avoid `json!`/
  `unwrap`). The repo has a large pre-existing warning baseline that is untouched.

## Files
- New: `src/dashboard.rs`, `src/identity/qr.rs`, `tests/dashboard_test.rs`, `tests/qr_test.rs`.
- Touched: `src/models/events.rs` (additive variants), `src/lib.rs` (module decl + routing),
  `src/pipeline.rs` (producer wiring + `stage` field + `approve_step` events),
  `src/identity/mod.rs` (re-exports), `src/ffi/core.rs` (the two QR verbs + `join_from_invite`
  seam + `cyan_pipeline_approve` passes `event_tx`), `tests/crux_smoke.rs` (one `stage: None`).

## Decisions
1. `tenant_id` for a run = the board's **group_id** (matches mesh `tenant=group_id`), so the
   dashboard bill is attributable and tenant-scoped without a new identity source.
2. `workflow_id` = `board_id` (the board's notebook IS the workflow); `stage` is an additive
   optional config field defaulting to `step_id`.
3. QR **issue authority** = group ownership (`group_is_owner`, the persisted mesh authority);
   QR **scan** does a local pre-check but the **holder** is the authoritative referee
   (issuer-admin · revocation · replay) at the snapshot gate — no new trust assumption.
4. Reused the existing `xaero_join_group_from_invite` machinery via a `join_from_invite` seam
   so the signed-grant join and the legacy invite join share one code path.

**Do NOT merge to `main`** (leave PR/merge to the human). **Rebuild the xcframework** before
iOS can use the new verbs/events.
