# STATUS — Dev Stack + LIVE Crux Smoke (Round 5, integration centerpiece)

Branch: `feat/dev-stack` (off `feat/backend-mcp-tool`; never `main`).
Result: **DONE.** One command boots the local stack, and a gated LIVE smoke proves
the crux pipeline runs end to end on the REAL wires — a real HTTP compile + a real
on-device MCP-tool step — not fakes. The fakes hid two real integration gaps; both
are found and fixed (below). Default `cargo test` stays green with no live deps
(the smoke is gated `CRUX_REAL=1`). Substrate suite unchanged. Do NOT rebuild the
xcframework. No new client `cyan_*` FFI.

---

## DELIVER 1 — `scripts/dev_stack.sh` (one command boots the stack)

```bash
scripts/dev_stack.sh                  # lens infra + compile vLLM stub + a backend peer
LENS_SERVER=1 scripts/dev_stack.sh    # ALSO `cargo run` the lens HTTP API (:8080)
CYAN_LENS_DIR=/path scripts/dev_stack.sh
```

What it does, then idles until Ctrl-C (clean teardown of every child + `make down`):

1. **lens infra** — `make -C ~/cyan-lens up` (the lens repo's runbook): postgres
   :5432, iggy :8090, **vllm-stub :8000**, sample mcp-plugin :8077. We RUN the lens
   repo; we never edit it. If the lens dir or docker is absent it prints the exact
   handoff and continues (the crux compile only needs the stub below).
2. **compile vLLM stub** — `scripts/crux_vllm_stub.py` on a free port, exported as
   `CYAN_VLLM_URL` (why a second stub: see GAP 2).
3. **`cyan_node` backend peer** — the existing test/host bin; prints its **node id**
   and the **LENS_URL** it targets (RELAY=disabled, DISCOVERY_KEY=cyan-dev). Driven
   over its stdin/stdout line protocol (a fifo held open so it stays alive).
4. Prints exactly **how to run the crux smoke** and **how to launch the app**.

Verified booting locally: stub up, real `cyan_node` peer with a real 64-hex node
id, summary printed, Ctrl-C tears down. (lens infra needs Docker; skipped cleanly
when absent.)

## DELIVER 2 — the LIVE crux smoke (`tests/crux_smoke.rs`, gated `CRUX_REAL=1`)

```bash
# with the stack up (dev_stack.sh prints this line with the right port):
CRUX_REAL=1 CYAN_VLLM_URL=http://127.0.0.1:<port> \
  cargo test --test crux_smoke -- --nocapture
```

`crux_pipeline_runs_live_mcp_tool_end_to_end` exercises the REAL wires:

- create **group → workspace → board** (real `storage::*`),
- **COMPILE** the notebook over a real HTTP round-trip: `pipeline::compile_via_llm`
  → POST `/v1/chat/completions` → parses the model's JSON config array → applies it
  (WIRE A: backend↔HTTP),
- **RUN** the pipeline: `pipeline::run_pipeline` with ONE `McpTool` step that is
  dispatched **on-device** through the supervised cyan-mcp host, spawning a **REAL
  plugin subprocess** (`StdioTransport` → `scripts/crux_plugin_run.py`, newline
  JSON-RPC: `initialize` → `tools/call`) (WIRE B: the real MCP-tool path),
- **asserts**: the step is `ai_complete` with a non-empty result; the dashboard exec
  events fired (`step_started` / `step_completed` / `finding_created`); and the
  plugin's cost landed on the **EXTERNAL** rail (`tool_called`, `source=external`,
  tagged `plugin_id`/`tool`, `cost_usd=0.07`) — cost isolation, captured off the
  real `obs` tracing rail.

Run result: **1 passed** live (`CRUX_REAL=1`), **skips** (1 passed, no-op) by
default. Every wait is bounded (TCP-listen poll ≤10s; no unbounded recv/sleep).

---

## LIVE integration gaps the fakes hid — FOUND + FIXED

**GAP 1 (real fix) — the on-device MCP path was unreachable from a real run.**
`run_pipeline` passed `find_asset_metadata(board_id)` (hard-coded Tears-of-Steel
asset JSON) into `execute_pipeline_step`, so the cell's `mcp_tool` spec never
reached `parse_mcp_tool_step`. The local MCP branch could ONLY fire when a unit
test called `execute_pipeline_step` directly (which `mcp_tool_test.rs` does) — never
through `run_pipeline`. Fix (`src/pipeline.rs`): merge the cell's own
`metadata_json.mcp_tool` into the metadata passed down. Ordinary steps (no
`mcp_tool`) are byte-for-byte unchanged — `find_asset_metadata` always returned
`Some`, and a cell with no `mcp_tool` adds nothing. This is the headline fix: it's
what makes the keystone actually run inside a pipeline.

**GAP 2 (found; resolved at the stack, lens untouched) — compile/stub shape
mismatch.** Backend `compile_via_llm` expects the model to return a **JSON array of
pipeline step configs**. The lens e2e `vllm_stub.py` only answers the enrichment /
query / workflow-ReAct prompts (it returns a canned extraction **object**), so
compiling against `:8000` fails to parse. Since the lens repo is read-only here, the
dev stack runs a **compile-aware** stub (`scripts/crux_vllm_stub.py`) that returns a
config array for compile prompts and the same OpenAI shape otherwise. Productionizing
= the real lens vLLM returns configs for compile prompts (or compile routes through a
lens endpoint that does). Documented, not faked: it's a real HTTP server on the wire.

**GAP 3 (additive, for the dashboard) — the local MCP path emitted no exec events
and no finding.** `execute_local_mcp_tool_step` returned `(summary, vec![])` and sent
only ad-hoc status text — so the dashboard read-model (DASHBOARD_CONTRACT §A/§C)
stayed dark for on-device steps, unlike the cloud path. Fix
(`src/pipeline_executor.rs`): emit `step_started` / `step_completed` /
`finding_created` (and `step_needs_human` when gated), turn the plugin's JSON result
into a `Finding` + a timecoded note, and carry `cost_usd` + `source=external` on the
completion event so the cost rail can attribute the external bill per run/plugin.

**Adjacent finding (noted, NOT changed):** the backend default `CYAN_LENS_URL` is
`http://localhost:9080`, but the lens API binds `:8080` (`API_ADDR`). The crux does
not use the lens `/execute` path, so this is out of scope here — `dev_stack.sh`
prints the real `LENS_URL` and the note so anyone running `LENS_SERVER=1` sets
`CYAN_LENS_URL` explicitly.

---

## What's gated / deferred

- **The crux smoke is gated `CRUX_REAL=1`** so default `cargo test` needs no live
  deps (no python, no docker, no network). It starts its own compile stub if
  `CYAN_VLLM_URL` is unset, so it is self-contained given `python3`.
- **lens HTTP API server** is opt-in (`LENS_SERVER=1`) — the crux proves
  backend↔HTTP via the vLLM compile wire and the LOCAL MCP host; it does not need
  the lens `/api/v1/execute` ReAct loop. Wiring a live `lens`-executor step into the
  same smoke (and reconciling the :9080/:8080 default) is the natural next rung.
- **Device plugin lifecycle** (real cred injection at spawn, runtime→entrypoint
  mapping beyond the bundle `run` entrypoint) remains the deferred device lifecycle
  noted in `STATUS_BACKEND_MCP_TOOL.md`. The crux exercises the real `run`-entrypoint
  spawn; richer runtimes are still ahead.

## Gate status

- `cargo build` ✅
- `cargo test --test crux_smoke` ✅ (skips, default) · ✅ **1 passed live** with
  `CRUX_REAL=1`.
- Substrate suite ✅ **unchanged** vs baseline: chat 4, discovery 2, files 5 (+1
  ignored), offline 3, resilience 5, snapshot_mp 1, sync 4 (+1 ignored). mcp_tool 4,
  mcp_host 3 still green.
- `cargo test --lib` — 19 pass, 1 fail (`diagram_gen::tests::test_parse_diagram_json`)
  **pre-existing and unrelated** (fails identically with my source changes stashed;
  I never touched `diagram_gen.rs`).
- Clippy: `crux_smoke.rs` is warning-free (it uses a `jval()` parse helper, not
  `json!`, since the workspace lint flags `json!`'s internal `unwrap`). The new
  exec-event code in `pipeline_executor.rs` uses `json!` for `PipelineEvent.data`
  **to match the module's existing style** (the pre-existing lens path + every
  `publish_pipeline_event` call already do, and are flagged identically) — no new
  KIND of finding; the legacy lib carries its pre-existing warnings as before.

## Files

- `scripts/dev_stack.sh` — one-command stack (lens infra + compile stub + peer).
- `scripts/crux_vllm_stub.py` — compile-aware vLLM stub (the GAP-2 stand-in).
- `scripts/crux_plugin_run.py` — a REAL `.cyanplugin` `run` entrypoint (stdio JSON-RPC).
- `tests/crux_smoke.rs` — the gated LIVE crux smoke.
- `src/pipeline.rs` — GAP 1 fix (thread the cell's `mcp_tool` into the executor).
- `src/pipeline_executor.rs` — GAP 3 fix (exec events + finding + external-cost tag
  on the local MCP path).
