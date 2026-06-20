# STATUS — MCP local-host scaffold (cyan-backend = local device host)

Branch: `feat/mcp-local-host` (off the merged `feat/substrate-e2e`; never `main`).
Result: **DONE.** cyan-backend now hosts MCP plugins via the shared `cyan-mcp`
crate, test-first. Substrate suite green throughout. No new `cyan_*` FFI.
**Do not rebuild the xcframework** (the strip-integrations FFI break still stands).

## PHASE 0 — the strip-integrations merge

The merge was already landed: `cca9b09 Merge branch 'feat/strip-integrations'
into feat/substrate-e2e` is the tip of `feat/substrate-e2e`, and
`feat/mcp-local-host` is branched cleanly off it. So PHASE 0 was a **verification**,
not a re-merge.

`bash scripts/verify_merge.sh` result — **the in-process gate is GREEN**; the one
red is a false negative I did NOT force:

| Step | Result |
|------|--------|
| build (lib + bins + tests) | ✅ |
| Agent 1 — offline-startup (`substrate_resilience`) | ✅ |
| Agent 2 — chat-attachment / G7 (`substrate_chat`) | ✅ |
| Regression — `substrate_discovery` / `_sync` / `_files` / `_offline` / `_snapshot_mp` | ✅ (all) |
| Agent 3 — docker rig (relay/WebSocket rungs) | ❌ **false red — see below** |

**Finding — `verify_merge.sh` has a cwd bug, and Agent 3 is out of in-process
scope.** The script does `cd "$(dirname "$0")"` (into `scripts/`) and then runs
`make -C harness test-all`. The harness lives at the **repo root** (`harness/`),
not `scripts/harness`, so make dies with `make: *** harness: No such file or
directory`. Docker *was* running, so the step ran and failed purely on this path
bug. The Docker relay rig (G2 ladder / G8-R / G11) is explicitly **out of
in-process scope** (CLAUDE.md, SUBSTRATE_TEST_SPEC §1) and is **orthogonal to an
integration-strip merge** (the strip changed zero transport/relay code). I did not
edit the script or fake a pass ("force nothing").

Recommendation for the human: either fix the script's `cd`, or verify the relay
rungs directly with `make -C harness test-all` **from the repo root** (it builds
two Docker images — relay + cyan_node — so it takes minutes). This is unrelated to
this branch's work.

## PHASE 1 — what landed (commit `f27f8cc`)

Dependency: `cyan-mcp = { path = "../cyan-mcp" }` (complete crate: Supervisor,
Client, transport, registry, obs, plus the `ScriptedTransport` / `RecordingSink` /
`RecordingEmitter` / `FakeClock` fakes). It compiles as a dep (rustc 1.98-nightly,
edition 2024).

### Sink / supervisor wiring (`src/mcp_host.rs`)

- **`MeshRelaySink`** implements `cyan_mcp::EventSink`. `deliver(PluginEvent)`
  broadcasts the relayed output INTO the group mesh via
  `NetworkCommand::Broadcast { group_id, NetworkEvent::PluginRelay { plugin_id,
  method, payload } }` on the engine's existing `network_tx`. The device has **no
  local Iggy**, so the mesh is the transport: the super-peer (Lens replica) picks
  `PluginRelay` off gossip and feeds its Iggy/enricher. `deliver` is best-effort —
  the trait returns `()`, so a closed channel is logged-and-dropped rather than
  crashing the supervision loop. It sits behind the `EventSink` trait, so tests
  swap in `RecordingSink` and never touch the network.
- **`PluginHost`** composes `cyan_mcp::Supervisor` with the host seams (sink,
  obs `Emitter`, `Clock`, `BackoffPolicy`, tenant). `supervise(transport,
  plugin_id)` builds a supervised plugin over any transport — `StdioTransport`
  (real child) in prod, `ScriptedTransport` in tests. Prod runs the
  `start` → `supervise_once` loop on a dedicated blocking thread (cyan-mcp is
  synchronous; `recv` blocks).
- **`NetworkEvent::PluginRelay`** (additive wire carrier). Only one exhaustive
  match needed an arm (`route_event_to_buffers`); it is **pass-through** — a normal
  device surfaces nothing to the app (plugins are files, not events), only the
  super-peer enriches it. `persist_event` and the ffi/core matches have catch-alls.

**Finding — Supervisor and Client cannot share one transport.** `cyan-mcp`'s
`Supervisor` and `Client` each *own* a `Box<dyn PluginTransport>`, and a plugin is
one process with one stdin/stdout, so a single transport instance can't back both.
For the device host's two load-bearing behaviors — relaying events to the sink and
surviving crashes — the **Supervisor is the composition point**. The `Client`
(request/response tool calls) is the Lens-side ReAct concern and gets its own
transport when that work lands. Documented inline on `PluginHost::supervise`.

### Plugins-workspace pickup path

"Registry = files" (MCP_ARCHITECTURE §3): installing a plugin == a `.cyanplugin`
bundle file appearing in the group's **"Plugins" workspace**, distributed by the
file-swarm. Detection is minimal and reuses existing storage — **no new FFI, no
new tables**:

- `storage::plugin_bundles_in_group(group_id, "Plugins", ".cyanplugin")` joins
  `objects` (type='file') to `workspaces` by name and returns the downloaded
  bundles (`local_path` set) as `PluginBundleFile { file_id, name, local_path }`.
- `PluginHost::discover_bundles(group_id)` is the thin pass-through.
- Next step (noted in code, not built here): unpack a bundle → manifest →
  `cyan_mcp::Registry`/`Manifest` → a `SpawnConfig` to actually run it.

### The two consumer tests (green) — `tests/mcp_host_test.rs`

Driven by `ScriptedTransport` — **NO real subprocess**; deterministic, no
unbounded wait (an empty scripted queue returns `Err` immediately = EOF).

1. **`plugin_event_forwarded_to_iggy`** ✅ — a scripted plugin pushes a relayed
   notification; the host (with `RecordingSink` swapped for `MeshRelaySink`)
   forwards it verbatim (method/params/plugin_id/tenant) to the sink.
2. **`plugin_supervised_across_crash`** ✅ — one event then EOF → the Supervisor
   backs off (asserted via `FakeClock` sleeps == base) and restarts (spawn_count
   1→2), refuses a duplicate `start` while running, and emits tenant/plugin-scoped
   `plugin_started` / `plugin_crashed` / `plugin_restarted{attempt:1}` obs.
3. **`mesh_relay_sink_broadcasts_plugin_event_into_group`** ✅ (extra, proves the
   prod sink) — `MeshRelaySink::deliver` puts a `Broadcast(PluginRelay)` on the
   network channel with the right group/plugin/method/payload.

`cargo test --test mcp_host_test` → **3 passed, 0 failed**.

## Rules honored

- **No new `cyan_*` FFI.** The app sees plugins as files; the FFI surface is
  unchanged. (The strip-integrations coordinated FFI break is unaffected.)
- **No `unwrap()`/`panic!` in new code** (`?` / `map_err`; `MeshRelaySink::deliver`
  logs-and-drops on a closed channel). The tests use `.expect()` (allowed) and a
  `jval()` parse helper instead of the `json!` macro, because `json!` expands to
  `unwrap()` which the workspace `disallowed_methods` lint rejects even in tests.
- **Clippy clean for new code.** `cargo clippy --test mcp_host_test` reports **zero**
  warnings attributable to `mcp_host.rs` / `mcp_host_test.rs`. Note: the legacy lib
  carries ~593 pre-existing clippy warnings, so a repo-wide
  `cargo clippy --all-targets -- -D warnings` cannot pass independent of this work;
  my changes add none.
- **Substrate suite green** after the change, identical to baseline: `substrate_chat`
  4, `substrate_discovery` 2, `substrate_files` 5 (+1 ignored), `substrate_offline`
  3, `substrate_resilience` 5, `substrate_snapshot_mp` 1, `substrate_sync` 4 (+1
  ignored).

## Commits

- `f27f8cc feat(mcp): scaffold the local device plugin host on cyan-mcp`

(PHASE 0 added no commit — the merge `cca9b09` pre-existed and was only verified.)

## Do NOT

- Rebuild the xcframework / run `build_static_lib.sh`. The strip-integrations FFI
  removals require iOS to drop `IntegrationComponentActor` first; this MCP scaffold
  adds nothing to the client FFI to change that.
