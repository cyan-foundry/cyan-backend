# STATUS — MeshTransport (Contido local, Lens on AWS)

The sovereignty seam from PRODUCTION_HARDENING_SET §4: the **cloud** Lens dispatches an
MCP tool call **over the mesh** to a **local** cyan-backend host, which runs the local
plugin against local data and returns the result. Plugin + data stay local; Lens only
orchestrates.

**Branches** (each off its current tip; never `main`):
- cyan-lens: `feat/mesh-transport` (off `feat/spec-e-binding`) — the cloud transport.
- cyan-backend: `feat/mesh-remote-invoke` (off `feat/r12-c3-antientropy`) — the local handler.

This file is mirrored in both repos.

---

## The protocol (the cross-repo wire contract)

One typed request → one typed response, serialized as JSON. Defined independently in each
repo with **identical field names / shape** (the serialized JSON is the contract; the repos
stay decoupled — no shared protocol crate). Locked by a `protocol_wire_shape_is_stable`
test on each side.

```
ToolCallRequest  { tenant_id, plugin_id, tool, args_json, corr_id }
ToolCallResponse { corr_id, result_json? , error? }      // exactly one of result_json/error
```

- `cyan-lens`: `src/workflow/mesh_transport.rs`
- `cyan-backend`: `src/mesh_invoke.rs` (+ `NetworkEvent::RemoteToolCall` / `RemoteToolResult`
  carriers in `src/models/events.rs`, with `to_event`/`from_event` conversions).

`tenant_id` rides every hop and flows into the host's `RunScope` → external cost rail.

---

## cyan-lens — the MeshTransport (REAL, behind the existing seam)

`src/workflow/mesh_transport.rs` (additive; the `StdioTransport` path is **untouched**):

- **`MeshTransport`** implements the existing `cyan_mcp::PluginTransport` trait. Instead of
  spawning a subprocess, it translates the two JSON-RPC messages the cyan-mcp `Client`
  emits per call:
  - `initialize` → a synthetic local ack (the mesh has no separate handshake; the host
    initializes the plugin per call).
  - `tools/call` → a typed `ToolCallRequest` dispatched via the `MeshClient` seam; the
    `ToolCallResponse` becomes the JSON-RPC reply the `Client` then `recv`s.
  So the cyan-mcp `Client` and `CyanMcpHost` are **unchanged**.
- **`MeshClient`** trait = the seam to the actual mesh (dial the host peer, send the request
  frame, await the response). `async fn invoke(req) -> ToolCallResponse`.
- **Bounded timeout**: `MeshTransport` wraps every `invoke` in `tokio::time::timeout`. An
  unreachable peer fails after the bound as a clean `McpError::Transport` → the executor
  sees a failed `call_tool`, never a hang. (The async `invoke` is driven from the sync
  `send` via `Handle::block_on`, safe because `CyanMcpHost` runs the call on a
  `spawn_blocking` thread.)
- **Selection rule** (`SelectingTransportFactory`): picks mesh vs stdio per plugin by
  manifest **`locality`**. A plugin with any tool whose `locality` ∈
  {`remote`, `device-local`, `device`, `local`} routes over the **mesh**; everything else
  (e.g. `cloud-local`, `external`) keeps the **identical** stdio spawn path. This is the one
  place the rule lives, so `CyanMcpHost` just calls `factory.connect`.

Exports added to `src/workflow/mod.rs`.

## cyan-backend — the remote-invoke handler (REAL, reuses the plugin host)

`src/mesh_invoke.rs` (additive; FFI contract untouched):

- **`RemoteInvokeHandler::handle(req) -> ToolCallResponse`** decodes the request, builds a
  `RunScope` + `McpTool`, and runs the local tool via the **existing**
  `PluginHost::dispatch_mcp_tool` — the same cyan-mcp `Client` path the on-device pipeline
  already uses. No new plugin-host machinery; the FFI is not touched.
- **`RemoteToolConnector`** seam = how the handler reaches a local plugin (read manifest
  `side_effects` + open a transport). Prod resolves the installed bundle and spawns a
  `StdioTransport`; tests pass a scripted echo transport.
- **Fail-closed approval**: a read-only tool runs; a side-effecting tool
  (`external_send` / `delete`) is **gated** and never opens a transport, because the
  protocol carries no approval token yet (see "Deferred").
- **Mesh carrier**: additive `NetworkEvent::RemoteToolCall` / `RemoteToolResult` variants
  (mesh pass-through in the FFI event router, mirroring the existing `PluginRelay`
  precedent) + `handle_event(NetworkEvent) -> Option<NetworkEvent>` convenience.

---

## The loopback proof (no AWS, no real Contido, no network)

Both halves meet at the identical protocol JSON; each is proven with fakes.

**cyan-lens** `tests/mesh_transport_test.rs` (5 tests, green):
- `mesh_tool_round_trips_over_loopback` — `CyanMcpHost.call_tool` → `SelectingTransportFactory`
  (device-local → mesh) → `MeshTransport` → loopback `MeshClient` (serializes through the
  real wire JSON, echoes args) → result threaded back; external `cost_usd` surfaced.
- `mesh_tool_is_tenant_scoped` — each call carries + is stamped with its own tenant.
- `mesh_unreachable_peer_fails_bounded_not_hang` — a never-resolving `MeshClient` + a 100ms
  bound → clean timeout error, wrapped in an outer 5s guard that proves no hang.
- `selection_routes_by_locality` — device-local → mesh, cloud-local → the (scripted) stdio
  path; asserts each plugin took only its lane.
- `protocol_wire_shape_is_stable` — locks the JSON field names.

**cyan-backend** `tests/mesh_invoke_test.rs` (6 tests, green):
- `remote_invoke_runs_local_plugin_and_returns_result` — handler → `dispatch_mcp_tool` →
  scripted echo plugin → result; the plugin is opened tenant-scoped.
- `remote_invoke_is_tenant_scoped` — per-call tenant scoping.
- `remote_invoke_carrier_event_round_trips` — `RemoteToolCall` → `RemoteToolResult` via the
  mesh carrier; non-call events ignored.
- `remote_invoke_unresolvable_plugin_is_clean_error` — connect failure → error response, no
  panic.
- `remote_invoke_side_effecting_tool_is_gated` — fail-closed; no transport opened.
- `protocol_wire_shape_is_stable` — locks the JSON field names (matches the lens side).

Default `cargo test` is green on both with **NO** Postgres/Iggy/vLLM/iroh/subprocess.
`cargo clippy --all-targets -D warnings` clean on both; both build with no DB.

---

## What is REAL vs scaffolded / deferred (kept bounded on purpose)

**Real now:**
- The `MeshTransport` ↔ `PluginTransport` translation, the locality selection rule, the
  bounded-timeout failure path, the protocol, and the backend handler reusing the real
  plugin-host dispatch. The whole round trip is proven over loopback end-to-end.

**Deferred (the real-iroh plumbing — intentionally NOT built to keep the diff bounded):**
1. **The prod `MeshClient` (cyan-lens).** Dial the plugin's host peer over iroh, frame the
   `ToolCallRequest`, await the `ToolCallResponse` correlated by `corr_id`. The seam is
   defined and fully tested with a loopback fake; only the iroh-backed impl remains. (A
   real cross-repo round trip would balloon well past the ~500-line bound, so it stops at
   the seam — per the run discipline.)
2. **The network-actor routing (cyan-backend).** The `NetworkEvent::RemoteToolCall` carrier
   + handler exist and are proven; routing an inbound call off gossip into
   `RemoteInvokeHandler` and dialing the `RemoteToolResult` back to the originating Lens
   peer (a request/response lane — gossip is one-way today) is the next bounded step.
3. **The prod `RemoteToolConnector` (cyan-backend).** Resolve the installed `.cyanplugin`
   bundle (`PluginHost::resolve_installed_tool`) + spawn `StdioTransport`. Proven with a
   scripted echo connector; the resolve-and-spawn impl remains.
4. **An approval token in the protocol.** Today the device fails closed for side-effecting
   tools. Carrying the cloud Lens `Enforcer`'s approval decision in the request (so an
   approved side-effecting remote call can run) is a small additive refinement.

**Unrelated pre-existing fix folded in (cyan-lens):** `tests/support/mod.rs` was missing the
new `ToolBlock.aliases` field (Spec-E, uncommitted in `../cyan-mcp`), which broke test
compilation on the base branch. Added `aliases: vec![]` to the two manifest-builder helpers
— additive, no assertion weakened.
