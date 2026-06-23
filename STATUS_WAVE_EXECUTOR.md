# STATUS — Wave-concurrent executor (Round 7)

Branch: `feat/wave-executor` (off `feat/dashboard-qr`; **never `main`**). Contracts:
`../anthropic_data_dump/WORKFLOW_MATERIALIZATION.md` §1 (optimizer/executor split),
cyan-lens `STATUS_LENS_WAVE_PARALLEL.md` (the `PhysicalPlan` I consume), and
`DASHBOARD_CONTRACT.md` / `STATUS_DASHBOARD_QR.md` (the exec events I re-emit).

The backend is the **executor**: it consumes Lens's compiled `PhysicalPlan` and runs
it **wave-concurrently**, and stays the **sequential offline fallback** when no plan
is present. **One executor, two entry conditions — not two engines.** No FFI change:
`cyan_run_pipeline`/`run_pipeline(board, cmd_tx, event_tx)` keep their signatures; the
events are the same additive, receive-only `SwiftEvent`s from Round 7.

> No xcframework rebuild (per instructions). No new client FFI verbs — the app drives
> runs via the existing verbs; the dashboard events are receive-only/additive and
> already shipped in `feat/dashboard-qr`.

---

## What shipped

### The plan I consume — `src/exec_plan.rs` (new, receive-only)
A JSON-portable mirror of the Lens `plan.rs` types so the artifact round-trips:
`PhysicalPlan { tenant_id, waves[], max_concurrency, max_cost_usd, total_cost_usd }`,
`Wave { index, steps[], batches[][] }`,
`PlannedStep { id, placement, cache_key, cache_hit, is_gate, gate_barrier?, cost_usd,
concurrency_weight }`. `from_json` returns `None` on malformed input (a bad plan
degrades to the sequential fallback, never a panic); `ordered_waves()` /
`ordered_batches()` give the run order (a wave with no explicit batches ⇒ one batch of
the whole wave). `#[serde(default)]` throughout for forward-compat.

### One executor — `src/pipeline.rs`
`run_pipeline` now loads any persisted plan (`load_physical_plan`) and delegates to
`run_pipeline_with_plan(board, Option<PhysicalPlan>, …)` — the core, **exposed** so
tests (and the future Lens compile→backend wiring) can hand the plan in directly.

- **`Some(plan)` → `execute_waves`** (concurrent): waves run in `index` order; within
  a wave, `batches` run one after another and **each batch's steps run concurrently**
  via a `tokio::task::JoinSet` (a task per step). The batch size **is** the in-flight
  degree (spawn-all-then-await-all), so the plan's `max_concurrency` cap is honored by
  construction — no extra semaphore.
  - **Cache hits** (`cache_hit`) → `cache_hit_outcome`: reuse the cell's prior
    persisted artifact (its `output`), mark the step `done`, **skip execution**.
  - **Gates are BRANCH barriers** (`gate_barrier` / `is_gate`): a manual/gate step
    surfaces as `awaiting_approval` (+ `ApprovalRequested`); a step behind an
    **unapproved** gate is marked `pending` and **does not run** — but **only that
    branch** stalls. Independent branches (`gate_barrier = None`) proceed. A gate is
    "satisfied" once it is `human_approved` (so a re-run after approval flows down).
- **`None` → `execute_sequential`** (fallback): the prior toposort loop, **unchanged**
  — manual ⇒ gate, deps-not-met ⇒ pending, else execute, one step at a time. This is
  the offline path; behavior is byte-for-byte the pre-Round-7 run.

Both paths share the **same** per-step body (`exec_one_step`) and the same non-executing
helpers (`gate_outcome` / `pending_outcome` / `cache_hit_outcome`), so they emit the
**same dashboard exec events** (`StepStateChanged`/`StepProgress`/`ApprovalRequested`/
`WorkflowStatsUpdated`/…) and feed the same `RunObs` snapshot. The dashboard lights up
identically whether a run was sequential or wave-concurrent.

The run result JSON gains two **additive** fields for observability: `"mode"`
(`wave`|`sequential`) and `"peak_concurrency"` (the largest batch the executor
launched concurrently — `1` for sequential).

### Plan source (the seam)
`load_physical_plan` reads `objects.data.physical_plan` for the board. Lens's
compile→backend persistence is **deferred** (per its STATUS), so today this is
normally `None` ⇒ sequential — i.e. **shipping behavior is unchanged** until a plan is
actually wired in. When Lens persists a plan, the wave path activates with no further
backend change.

---

## Tests — all GREEN (test-first), `cargo test` needs no live deps

`tests/wave_executor_test.rs` (executed steps are `local` plugin steps against an empty
offline plugin root ⇒ deterministic fast-fail; every wait is bounded — the run is
awaited, then the event channel drained):
- `diamond_dag_runs_independent_branch_concurrently` — A→{B,C}→D: B and C share one
  wave (`peak_concurrency == 2`); both reach `running` before D (next-wave barrier).
- `gate_barrier_stalls_only_its_branch` — a manual gate `g` opens
  (`awaiting_approval` + `ApprovalRequested`); the independent branch `x` runs; the
  gated dependent `b` is `pending` and **never** `running`.
- `cache_hit_skips_rerun` — a `cache_hit` step ends `done` (never `running`/`failed`,
  proving execution was skipped — the offline plugin would have failed), reports
  `cache_hit`, and the prior artifact is preserved on the cell.
- `budget_cap_limits_in_flight` — 4 independent steps, plan batched 2+2 for cap 2:
  `peak_concurrency == 2` (never the full wave of 4); all 4 still execute.
- `no_plan_falls_back_to_sequential` — `mode == sequential`, `peak_concurrency == 1`;
  the root runs, dependents stay `pending` under the prior sequential dep-gating.
- `exec_events_emitted_from_concurrent_path` — the concurrent run emits
  `WorkflowRunStarted` first, per-step `running`/`progress`/terminal, exactly one
  tenant-scoped `WorkflowStatsUpdated` (both stages present), and
  `WorkflowRunFinished` last.

`src/exec_plan.rs` unit tests (3): Lens-shape deserialize, batches default, malformed
JSON ⇒ `None`.

### Suite status
- New: **6/6** wave tests + **3/3** exec_plan unit tests green.
- Regression green: `dashboard_test` (3), `qr_test` (3), `crux_smoke` (3),
  `mcp_tool_test` (3), `mcp_host_test` (4); substrate spot-check `substrate_sync` (4)
  + `substrate_lens` green.
- Clippy: lib warning count **unchanged** vs `feat/dashboard-qr` (576 → 576) — the new
  modules add **zero** warnings (no `unwrap`/`panic!` in engine paths; DB errors in the
  spawned step body are logged via `map_err`, never propagated as panics).

---

## Files
- New: `src/exec_plan.rs`, `tests/wave_executor_test.rs`, `STATUS_WAVE_EXECUTOR.md`.
- Touched: `src/lib.rs` (module decl), `src/pipeline.rs` (the wave/sequential executor
  split + shared step body + plan loader; `update_step_state*` drop the unused `cell`
  arg; `fire_notifications` takes a `&[StepNotification]`).

## Decisions
1. **One core, two entry conditions** — `run_pipeline_with_plan` dispatches to
   `execute_waves` or `execute_sequential`; both call the same `exec_one_step`. No forked
   engine; the sequential path is the unchanged offline fallback.
2. **Batching IS the concurrency cap** — spawn-all-then-await-all per batch makes the
   peak in-flight equal the batch size, which Lens already capped to `max_concurrency`.
   Simpler than a runtime semaphore and exactly the contract.
3. **Cache hit = reuse the cell's persisted output** — the real artifact we have
   on-device; honest reuse, no fake store. (A content-addressed artifact store is future
   work; noted.)
4. **Gates satisfied by persisted `human_approved`** — within a single run a gate is
   never approved (approval is a separate later FFI call), so dependents stall this run
   and flow on the next run after approval — consistent with the existing dep-gating.
5. **Plan source is a seam** — read from `objects.data.physical_plan`; `None` today ⇒
   sequential, so shipping behavior is unchanged until Lens wires the plan in.

**Honest limitations (noted, not faked):** (a) Lens's compile→backend plan persistence
is deferred, so the wave path is exercised via `run_pipeline_with_plan` in tests and
will activate in production once a plan is stored — no backend change needed then.
(b) `placement` (`local|cloud|hybrid`) is carried through but every step still runs via
the existing `execute_pipeline_step` (the local/lens hybrid loop); per-placement
dispatch is future work. (c) Cache reuse is the cell's prior `output`, not a separate
content-addressed store. (d) Concurrency is proven structurally
(`peak_concurrency` = max batch) + by event ordering (independent steps reach `running`
before the next wave) rather than by wall-clock timing, to stay deterministic.

**Do NOT merge to `main`** (leave PR/merge to the human). **No xcframework rebuild.**
