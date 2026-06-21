# STATUS_ROUND10_DISTWF — distributed workflow RUN in the substrate harness

Round 10 adds a **distributed workflow-execution** rung to the live harness. The prior harness
proves sync + authoring-convergence; this proves a workflow actually **RUNS** (the real wave
executor) and its **run-state propagates across peers** over the same gossip/event path that carries
chat and boards. STABLE engine — **no engine edits**: all new code is test-only (`src/bin/cyan_node.rs`
verbs, `tests/support/multiprocess.rs` helpers, `tests/substrate_workflow_run.rs`, `harness/live.sh`).

Branch `feat/round10-distwf` (never `main`). iroh 0.95. Bounded waits throughout; every assertion is
on each peer's OWN `storage::*` run-state or on the producer's OWN captured exec-event stream — never
a log line.

## What the run-execution test asserts

With **N loopback peers in one group** (host + joiners, each its own DB + auto identity), the host
**authors + RUNS** a local-placement workflow through the existing executor
(`pipeline::run_pipeline_with_plan` — the wave-concurrent path with the sequential toposort fallback).

1. **The run EXECUTES.** Steps move `pending → running → terminal`, and the dashboard exec events
   fire (DASHBOARD_CONTRACT §A): `WorkflowRunStarted` (×1), `StepStateChanged` (running + terminal per
   step), `StepProgress`, `WorkflowStatsUpdated` (×1), `WorkflowRunFinished` (×1). These are captured
   from the producer's OWN `SwiftEvent` stream for the run and summarized as JSON by the `wf_run` verb.
2. **The run-state + results CONVERGE on every peer.** As the run progresses, each step-state change
   rides `CommandMsg::UpdateNotebookCell → NetworkEvent::NotebookCellUpdated` (the SAME gossip path as
   chat/boards) and every peer applies it to its OWN `notebook_cells`. The test polls each peer's
   `pipeline.state.status` (the `wf_state` verb) until it converges — bounded.
3. **Gate = branch barrier.** A `manual` (human-approval) gate stalls only its branch: run 1 of the
   `gated` shape shows the gate `awaiting_approval`, the independent branch `x` running, and the gated
   `b` `pending` (never running). An **approval on ONE (non-host) peer** (`wf_approve`) broadcasts and
   converges to the host; **run 2** then unblocks `b` for ALL peers — its terminal state converges.
4. **Wave concurrency.** Independent steps in a wave run concurrently: the diamond `a→{b,c}→d` reports
   `peak_concurrency == 2` (b and c launched in one batch) and both reach `running`.

### Tests (`tests/substrate_workflow_run.rs`, gated by `CYAN_LIVE=1`)

| Test | Proves |
|---|---|
| `distributed_workflow_run_executes_and_converges` | run executes (all 5 exec-event kinds) + every step converges terminal on every peer (diamond) |
| `gate_barrier_unblocks_run_for_all_peers` | gate stalls only its branch; a one-peer approval unblocks the run for all (gated) |
| `independent_steps_run_concurrently_in_a_wave` | `peak==2`; b,c run concurrently + converge (diamond) |
| `run_finished_state_consistent_across_peers` | the per-step terminal run-states are identical on every peer, so each derives the same run verdict (linear) |

A plain `cargo test` (no `CYAN_LIVE`) returns each test instantly, so the default matrix stays light.

### New `cyan_node` verbs (test-only line protocol)

- `wf_author <gid> <shape>` — author a RUNNABLE workflow (board + step cells carrying pipeline configs),
  broadcast `BoardCreated` + per-cell `NotebookCellAdded`/`NotebookCellUpdated` so peers hold the
  configs. Shapes: `linear` (s0→s1→s2), `diamond` (a→{b,c}→d), `gated` (g·b·x).
- `wf_run <board> [wave|seq]` — RUN via `run_pipeline_with_plan`. `wave` builds a level-set
  `PhysicalPlan` (the minimal materializer the harness emits in Lens's stead); `seq` is the sequential
  fallback. Returns a JSON summary of the exec events that fired.
- `wf_state <board> <step_id>` — THIS peer's run-state for a step, from its OWN storage (the oracle).
- `wf_approve <board> <step_id>` — approve a human gate on THIS peer and broadcast it.

A small bridge task in `cyan_node` is the `CommandActor` seam for the one command a run emits
(`UpdateNotebookCell`): apply it to local storage AND broadcast `NotebookCellUpdated` — mirroring the
engine's `lib.rs` arm — so run-state converges. `CYAN_PLUGINS_ROOT` defaults to an empty dir so a
step's `local` `mcp_tool` resolves "not installed" and reaches a terminal state fast + offline.

## The new `live.sh` scenario

`./harness/live.sh --scenario workflow-run [--peers N]` (macos tier) routes to the gated orchestrator
`substrate_workflow_run::workflow_run_live`, which forms one group, authors+runs a diamond, and emits
the machine-readable per-peer `@@LIVE@@` table + a `verdict=PASS|FAIL` line that `live.sh` renders and
turns into the exit code — exactly like the existing scenarios. It is **distinct from** the existing
`workflow` scenario (which asserts authoring-convergence only). Verified green:

```
  SCENARIO     PEER   RESULT DETAIL
  workflow-run host   PASS   run-executed-peak=2
  workflow-run host   PASS   all-steps-converged
  workflow-run peer1  PASS   all-steps-converged
  workflow-run peer2  PASS   all-steps-converged
  [PASS] all 3 peers converged on every scenario  (net=home, mode=macos)
```

## What is still local/MCP (out of substrate scope)

Per CLAUDE.md, the LOCAL/MCP step **execution** itself is out of substrate scope. We drive it with the
test-only local step the harness already has — a `local` step whose `mcp_tool` names a **non-installed**
plugin, so `resolve_installed_tool` returns "not installed" and the step reaches a terminal state
**deterministically + offline** (no network, no plugin spawn, no backoff). The asserted substrate
properties are therefore **run-state propagation**, **exec-event emission**, **gate/branch-barrier
semantics**, and **wave concurrency** — NOT the step's business result. That terminal verdict is
deterministically `failed` (no plugin installed in the loopback rig), and crucially **that verdict
itself converges identically on every peer**. Real local/MCP/cloud step execution (installed plugins,
Lens reasoning, partner agents) remains the cyan-mcp/Lens job and the Docker rig's concern — unchanged.

## No regression

- `cargo build --tests` — green.
- `cargo clippy --all-targets -- -D warnings` — clean (only pre-existing manifest/xaeroid warnings).
- Substrate suite spot-check green: `substrate_chat` (4/4), `substrate_sync` (4/4, 1 ignored),
  `wave_executor_test` (6/6). All four gated run-tests + the orchestrator pass under `CYAN_LIVE=1`.
- All changes are additive + test-only; the FFI surface and shipping engine behavior are untouched.
```
