# STATUS — Backend MCP Tool (Wave 2 keystone, LOCAL/device half)

Branch: `feat/backend-mcp-tool` (off `feat/identity-grants`; never `main`).
Result: **DONE.** A pipeline step can now call a plugin tool that runs **LOCALLY,
on-device** through the supervised cyan-mcp host — the device-side mirror of what
cyan-lens shipped cloud-side (`STATUS_LENS_MCP_TOOL.md`). Same `McpTool` contract,
same cost-isolation. Test-first; substrate suite green throughout. No new client
`cyan_*` FFI. **Do not rebuild the xcframework.**

This builds on the local MCP host from `feat/mcp-local-host` (`PluginHost` /
`Supervisor` / `MeshRelaySink`, already merged into `feat/identity-grants`).

## The local dispatch path (`src/mcp_host.rs`)

`PluginHost::dispatch_mcp_tool` is the one seam — the device mirror of the lens
`McpHost::call_tool`:

```rust
pub fn dispatch_mcp_tool<F>(&self, scope: &RunScope, step: &McpTool,
    side_effects: &[String], approved: bool, ledger: &RunCostLedger, connect: F)
    -> anyhow::Result<McpDispatch>
where F: FnOnce() -> anyhow::Result<Box<dyn PluginTransport>>;
```

- **`McpTool { plugin_id, tool, args }`** — mirrors the lens contract exactly
  (WORKFLOW_MATERIALIZATION §2). The backend uses the canonical field name `tool`;
  lens had to call it `tool_name` only because its enum's serde tag already
  occupied `tool` — there is no such collision in a plain struct here.
- It composes a cyan-mcp **`Client`** over the transport: `initialize` →
  `call_tool(tool, args)`. The plugin's JSON `result` threads back into the step
  output (`McpDispatch::Ran(McpToolResult { result, duration_ms, cost_usd })`), so
  the next pipeline step can consume it — exactly like the lens ReAct context.
- **`connect` is a closure, not an eager transport**, so a GATED tool never opens
  a transport — in prod that means a side-effecting plugin process is never even
  spawned before approval. Tests pass a pre-scripted `ScriptedTransport`; prod
  passes a closure that spawns a real `StdioTransport` (see executor wiring).
- Per the prior finding (Supervisor and Client can't share one transport), the
  request/response tool call gets its **own** transport via `connect`; the
  `Supervisor` remains the long-running event-relay composition point.

## Registry → tools (`PluginHost::resolve_installed_tool`)

"Registry = files." A `.cyanplugin` the file-swarm fetched into the group's
**"Plugins" workspace** (`storage::plugin_bundles_in_group` → `discover_bundles`,
from the prior run) is unpacked into a subdir of the device plugins root.
`resolve_installed_tool(plugins_root, tool)` indexes those bundles via cyan-mcp's
**`Registry`** and resolves a tool name to `(plugin_id, ToolBlock)` — turning an
installed bundle into a tool a local step can dispatch, and surfacing the tool's
manifest `side_effects` so the dispatcher can gate it. A bad bundle is skipped by
the registry (not fatal); `Ok(None)` = no such tool installed.

## Cost isolation (matches lens, WORKFLOW_MATERIALIZATION §3)

`RunCostLedger` has **two separate rails**:
- **LLM rail** — `record_llm(tokens_in, tokens_out)` for our own vLLM reasoning.
- **External rail** — `record_external_tool(ToolCalledObs)` for plugin/partner
  tool calls.

A plugin/partner reports its OWN billing as `cost_usd` in the tool result (mirrors
cyan-mcp reading `tokens` for model-backed tools). `dispatch_mcp_tool` records that
on the **external rail only** as a flat obs line:

```
tool_called { tenant_id, run_id, plugin_id, tool, duration_ms, cost_usd?, source="external" }
```

…and adds **ZERO** to the LLM tally. cyan-mcp's plugin-internal `tool_called` obs
is discarded (a `DiscardEmitter`) so a call is counted exactly once, on our rail.
The obs is also emitted on the `"obs"` tracing target for prod. Proven by
`local_mcp_tool_cost_is_external_not_tokens`.

## The gate tie-in (reuses the existing human-approval path)

`requires_approval(side_effects)` is true for `external_send` / `delete`. When a
tool requires approval and is not yet `approved`, `dispatch_mcp_tool` returns
`McpDispatch::Gated { side_effects }` **without opening a transport / spawning the
process** — nothing runs, nothing is billed.

The prod wiring (`pipeline_executor.rs`) ties this to the existing pipeline gate:
`approved` is read from `pipeline.state.status == "human_approved"` (set by
`pipeline.rs::approve_step`), and a `Gated` outcome surfaces a `needs_human` error
on the same path the executor already uses — so the user approves via the existing
flow and a re-run flips `approved`. No new gate machinery.

## Prod wiring (`src/pipeline_executor.rs`) — where steps execute

`execute_pipeline_step` gains a **guarded** branch at the top: a `local` step whose
metadata names a plugin tool (`{ "mcp_tool": { plugin_id, tool, args } }`) is
dispatched on-device — **no cloud round-trip** — via `execute_local_mcp_tool_step`.
Ordinary steps (no `mcp_tool`) fall straight through unchanged, so shipping
behavior is byte-for-byte identical. That function:
1. resolves the tool in the installed registry → its manifest `side_effects`,
2. reads the human-approval gate (`step_is_approved`),
3. spawns a real `StdioTransport` lazily from the bundle (`<bundle>/run` entrypoint),
4. dispatches; `Ran` threads the result into the step summary, `Gated` surfaces
   `needs_human`.

**Real vs scripted (noted per the spec):** the dispatch/registry/cost/gate LOGIC is
real and unit-tested via cyan-mcp's `ScriptedTransport` (no subprocess). The prod
device lifecycle that's exercised only at runtime: the actual `StdioTransport`
spawn, short-lived **cred injection** at spawn, and the runtime→entrypoint mapping
(today a bundle `run` entrypoint) — these are the deferred device lifecycle, the
backend's to own (the lens STATUS left them to this run).

## Named tests (RED-first → green) — `tests/mcp_tool_test.rs`

Driven by `ScriptedTransport` — NO real subprocess; deterministic, no unbounded
wait. `cargo test --test mcp_tool_test` → **4 passed, 0 failed**.

1. **`pipeline_step_invokes_local_plugin_tool`** ✅ — an `McpTool` step dispatches
   via the local host; the scripted plugin result threads into the step output.
2. **`local_mcp_tool_cost_is_external_not_tokens`** ✅ — cost recorded on the
   external rail (`source=external`, `cost_usd`); the LLM tally is unchanged (zero
   added).
3. **`plugin_tool_from_plugins_workspace_is_discoverable`** ✅ — an installed
   `.cyanplugin` in the group's "Plugins" workspace is picked up
   (`discover_bundles`) and its tool is found via cyan-mcp's `Registry`.
4. **`local_mcp_tool_external_send_requires_approval_gate`** ✅ (extra, proves the
   gate) — an unapproved `external_send` tool gates without opening a transport;
   once approved it runs and bills.

## Rules honored

- **No new `cyan_*` FFI.** The app drives pipelines via existing verbs; a plugin
  tool is just step metadata (`mcp_tool`). FFI surface unchanged.
- **No `unwrap()`/`panic!` in engine/FFI paths** — `?` / `map_err` / `unwrap_or*`
  throughout. Tests use `.expect()` on static literals + a `jval()` parse helper
  (the `json!` macro expands to `unwrap()`, which the workspace lint rejects in
  tests).
- **Bounded waits** — `ScriptedTransport` never blocks (empty queue = immediate
  `Err` = EOF); no timeout plumbing needed.
- **Clippy clean for new code** — zero warnings attributable to `mcp_host.rs`,
  `pipeline_executor.rs`'s new functions, or `mcp_tool_test.rs`. (The legacy lib
  carries its pre-existing warnings; my changes add none.)
- **Substrate suite green**, identical to baseline: `substrate_chat` 4,
  `substrate_discovery` 2, `substrate_files` 5 (+1 ignored), `substrate_offline` 3,
  `substrate_resilience` 5, `substrate_snapshot_mp` 1, `substrate_sync` 4 (+1
  ignored). The 3 prior `mcp_host_test` tests still green.

## Do NOT

- Rebuild the xcframework / run `build_static_lib.sh` (the strip-integrations FFI
  break still stands; this adds nothing to the client FFI).
- Merge to `main` (leave PRs/merges to the human).
