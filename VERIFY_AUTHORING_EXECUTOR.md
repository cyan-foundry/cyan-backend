# VERIFY — Authoring + Executor fixes (branch `feat/authoring-and-executor-fixes`)

Additive changes for BURST contracts C1, C2 (backend receiver half), and C4. No existing
`cyan_*` FFI, `SwiftEvent`, or `NetworkCommand` variant renamed. `cyan_autocomplete_path`
untouched. No `unwrap()`/`panic!` on any new FFI/engine path.

**BUILT + GREEN (verified 2026-07-03).** On branch `feat/authoring-and-executor-fixes`:
- `cargo build --all-targets` — clean (only pre-existing multi-target manifest notes + the
  external `xaeroid` crate warnings; none from cyan-backend).
- `cargo test` — all suites pass, **0 failed**. Ignored = the documented relay/Docker
  red-scaffolds only (no in-process capability faked).
- `cargo clippy --all-targets -- -D warnings` — **exit 0**, no cyan-backend lint warnings.

Working tree clean; the 4 prior commits stand as-is (no fixes were needed — the code compiled
and tested green as written). The cyan-media arg key was reviewed against `tests/mcp_tool_test.rs`
(fixture uses `"src"`) and the C3 frameio precedent: the injected superset `src`/`input`/`uri`
always includes the confirmed canonical `src`, so no change required.

## Confirmed FFI C signatures for the iOS side
```c
char* cyan_workflow_autocomplete(const char* board_id, const char* partial);
char* cyan_install_plugin_bundle(const char* group_id,
                                 const char* plugin_id,
                                 const char* bundle_bytes_b64);
// both: NULL only on a bad pointer / JSON-encode failure; free with cyan_free_string(ptr).
```

---

## TASK 1 (C1) — Workflow autocomplete FFI

New FFI in `src/ffi/core.rs` (added right after `cyan_autocomplete_path`):

```c
// C signature for the iOS bind:
char* cyan_workflow_autocomplete(const char* board_id, const char* partial);
// free the returned string with cyan_free_string(ptr).
```

- Delegates to new `workflow::filter_autocomplete(board_id, partial)` (`src/workflow.rs`),
  which builds `workflow::autocomplete_index(board_id)` (unchanged) and narrows it by the
  trailing `@`/`#`/`/` trigger + query parsed from `partial` (new `workflow::parse_trigger`).
- Returns JSON:
  `{"tenant_id":"…","plugins":[{"trigger":"@","kind":"plugin","value":"…","label":"…"}],`
  `"artifacts":[{"trigger":"#",…}],"actions":[{"trigger":"/",…}]}`.
  (`trigger` serializes as a 1-char string. The extra `tenant_id` is harmless — the iOS
  `WorkflowViewModel.parseSuggestions` shape tolerates it.)
- `@sl` → only matching plugins (artifacts/actions empty). `#` / `/` behave the same for
  their list. No active trigger → the FULL index (all three lists). Null return only on a
  bad `board_id` pointer or a JSON-encode failure; a null/invalid `partial` degrades to the
  empty query (full index), not a failure.

**What to test (has tests):** `tests/workflow_step_test.rs`
- `parse_trigger_reads_the_trailing_trigger_and_query` — pure, no DB.
- `filter_autocomplete_narrows_to_the_active_trigger` — seeds plugins + a file, asserts each
  trigger narrows correctly and no-trigger returns the full index.

**Unverifiable here:** the FFI ↔ Swift round-trip (needs the iOS `CyanBackend.workflowAutocomplete`
bind + `fetchSuggestions` wiring — cyan-iOS side of C1). The Rust logic it wraps is tested.

---

## TASK 2 (C2, backend receiver half) — Local plugin install

New FFI in `src/ffi/core.rs`:

```c
// C signature for the iOS bind:
char* cyan_install_plugin_bundle(const char* group_id,
                                 const char* plugin_id,
                                 const char* bundle_bytes_b64);
// free the returned string with cyan_free_string(ptr).
```

- Base64-decodes `bundle_bytes_b64`, then calls new
  `storage::install_plugin_bundle(group_id, plugin_id, bytes)` which:
  - ensures the group's system **Plugins** workspace exists (`provision_group_workspaces`),
  - writes the tar bytes to `plugin_bundles_dir()/<plugin_id>.cyanplugin`
    (`CYAN_PLUGINS_ROOT`, else `$HOME/.cyan/plugins` — mirrors the executor's `plugins_root`),
  - inserts an `objects` file row (type='file', that workspace, `local_path` set,
    name `…​.cyanplugin`) so **`plugin_bundles_in_group` + `autocomplete_index` find it**.
  - **Idempotent:** deterministic file id `blake3("plugin-bundle:{group}:{plugin_id}")` ⇒
    re-install REPLACES the row and overwrites the bytes (no duplicate).
- On success emits a fresh **TreeLoaded** (via `CommandMsg::Snapshot`) so the Explorer /
  authoring surface refreshes.
- Returns JSON: `{"success":true,"plugin_id":"…","file_id":"…"}` or
  `{"success":false,"error":"…"}`. Never null unless the JSON encode itself fails.

**What to test (has test):** `tests/workflow_step_test.rs`
- `install_plugin_bundle_is_discoverable_and_idempotent` — install → appears in
  `plugin_bundles_in_group` + under `@` in `autocomplete_index`; bytes on disk; re-install
  replaces (same id, one row, new bytes). Uses a temp `CYAN_PLUGINS_ROOT`.

**Deferred / unverifiable here (documented TODO in code):**
- **XaeroID signature verification of the bundle.** The `.cyanplugin` internal layout
  (`manifest.yaml` + detached signature) is a cyan-forge artifact; this repo has no
  unpack path for it and no `tar`/`flate2` dependency. Verification is a TODO on the FFI —
  the bytes arrive from the authenticated Lens marketplace endpoint (same auth as `/browse`).
  Wire `xaeroid::XaeroID::verify(...)` here once an unpack path lands. This matches the task's
  "verify if the repo has a verify path (else TODO-note it and proceed)" instruction.
- **Unpacking to a runnable `<plugin_id>/run` dir.** The on-device MCP dispatch
  (`execute_local_mcp_tool_step`) expects an unpacked bundle dir; that unpack is the deferred
  device lifecycle (already noted in `pipeline_executor.rs`). We record the bundle FILE so the
  authoring/registry surface (the C2 acceptance criterion) works now.
- The Lens `GET /api/v1/marketplace/bundle/{plugin_id}` endpoint and the iOS Install action
  are the other two legs of C2 (not this repo).

---

## TASK 3 (C4) — Intent resolver stands down on a direct bind

`src/pipeline_executor.rs`, `execute_pipeline_step`:
- The direct-bind guard now fires for **any** `executor_type` when the step's metadata carries
  an `mcp_tool` bind (was gated to `executor_type == "local"`). A bound step is dispatched
  straight through `execute_local_mcp_tool_step` and RETURNS before the license gate, the
  demo cache, the Lens ReAct path, and the local `skills::registry().resolve_intent` — so the
  SkillRegistry/intent resolver never runs on a directly-bound step (no wasted turn, no
  wrong-tool risk like ingest→qc_analysis / upload→ssai_break_detection).

**What to test:** existing `tests/mcp_tool_test.rs` exercises `dispatch_mcp_tool` directly and
is unaffected. The behavioral guarantee (resolver skipped) is a control-flow property: a bound
step short-circuits at the top of `execute_pipeline_step`. A full integration test would drive
`pipeline::run_pipeline` with a bound step and assert the resolver is not consulted (harder —
needs the vLLM/skill seam faked; `crux_smoke` covers the live bound-dispatch path but is gated).

**Unverifiable here:** that the Lens ReAct loop itself stands down — that is enforced by NOT
reaching Lens for a bound step (this change), plus the cyan-lens side of C4 (separate repo).

---

## TASK 4 (C4) — Downstream #asset path resolution for every cyan-media step

`src/pipeline_executor.rs`, `execute_local_mcp_tool_step` (new `resolve_media_args`):
- Root cause: compile writes `mcp_tool = {plugin_id:"cyan-media", tool}` with **no `args`**
  (`pipeline.rs:1337`), so EVERY cyan-media step (ingest AND proxy/conform/…) reached the
  plugin with no input path → `path_denied`, `input_bytes:0`.
- Fix: before dispatch, for a `cyan-media` step, resolve the board's asset via the SAME
  `find_video_uri(board_id)` the ingest path uses and inject it into the args under `src`
  (canonical, matches the plugin + the `mcp_tool_test` `"src"` fixture) mirrored onto `input`
  and `uri`. So every consumer resolves to the identical container path.
- Guardrails: no-op unless plugin is `cyan-media`; if the author already supplied any input
  key (`src`/`input`/`uri`/`path`/`input_uri`/`source_url`, non-empty) we respect it; if the
  URI can't be resolved we leave args untouched so the plugin surfaces its own clear error
  (we never fabricate a bad path).

**What to test (has tests):** `src/pipeline_executor.rs` `mod media_args_tests`
- `non_cyan_media_step_is_untouched`, `author_supplied_src_is_respected`,
  `author_supplied_input_alias_is_respected` — all DB-free (early-return branches).

**Unverifiable here:** the inject-from-`find_video_uri` branch needs a seeded DB + a bound
asset; and the exact arg key the shipped cyan-media Python plugin reads is external to this
repo — we inject `src`/`input`/`uri` (superset) to be safe. Confirm against the live plugin
that `src` is honored (the `mcp_tool_test` fixture and C3's frameio precedent both use `src`).

---

## Files touched
- `src/workflow.rs` — `parse_trigger`, `entry_matches`, `filter_autocomplete` (additive).
- `src/ffi/core.rs` — `cyan_workflow_autocomplete`, `cyan_install_plugin_bundle` (additive).
- `src/storage.rs` — `plugin_bundles_dir`, `install_plugin_bundle` (additive).
- `src/pipeline_executor.rs` — direct-bind guard broadened; `resolve_media_args` +
  `MEDIA_INPUT_KEYS`; unit tests.
- `tests/workflow_step_test.rs` — C1 + C2 tests (additive; no existing assertion weakened).
