# VERIFY — conform-in-loop (the auto-technical-edit loop closes)

Branch `feat/conform-in-loop` off `feat/frameio`. **NOT BUILT by agent** — no Rust
toolchain in the agent sandbox. Rick: run `cargo test` + `cargo clippy --all-targets
-- -D warnings`.

## What this closes (FORMAT_SUPERSET Part 7a + 8b)

When the review-loop workflow reaches its step **"apply confirmed mechanical edits
and conform proxy"**, Cyan now APPLIES the approved mechanical ops ITSELF — via the
new cyan-media `conform` tool — to render the next review proxy, register it, freeze
a new ledger Version, and advance the round. **No Avid, no editor.** The editor
re-enters only for creative work and the final online.

## The flow (`review_loop::conform_proxy`, steps a–e)

`conform_proxy(conn, tenant_id, proxy_ref, new_proxy_frameio_ref, dispatch)`:

- **(a) resolve** — `proxy_ref` (the current round's Frame.io file id) →
  `asset_registry::find_by_remote_ref("frameio", …)` → the proxy asset →
  `derived_from_asset` (MASTER) + `derived_from_version` (the frozen conform plan) +
  the master's `fps`.
- **guard** — the machine must be `CONFORMING` (the human already fired
  `confirm_notes`). Checked BEFORE dispatch so an un-confirmed round never triggers a
  render.
- **(b) gather** — `changelist::approved_ops(master, branch)` = the `active=1 AND
  state='approved' AND kind='op'` entries, in `seq` order. These are the confirmed
  mechanical edits. **Notes / creative (`kind=note`) are NEVER conformed.**
- **(c) dispatch** — build the cyan-media `conform` args and call the tool through
  the `ConformDispatch` seam. `conform` is `side_effects: none` → runs un-gated
  through the SAME McpDispatch path `pipeline_executor` uses. The follow-up
  PUBLISH/upload of the new proxy stays `external_send`-gated (a separate
  `@frameio.upload /needs-approval` step; `publish_proxy` is HUMAN-fired).
- **(d) register + version + surface** — `conform_run` (AUTO) freezes the active set
  as the next Version; the returned proxy is registered as a DERIVED asset
  (`derived_from_asset = master`, `derived_from_version = the NEW version`), with the
  tool's `output_path` on `profile_json`. Every `needs_manual` op the engine returned
  becomes a durable `kind=note, source=cyan` ledger ask (`ask:
  "conform_needs_manual"`) — **surfaced, never dropped** (same shape as the loop's
  creative-note / max-rounds escalations).
- **(e) round++** — `review_state::conform_run` advances the machine (→ CONFORMING /
  next version) so the NEXT SENSE ingest on the new proxy remaps through
  `conform_map::for_version(new_version_id)` (round-2+ tc shift). Not published —
  `publish_proxy` (external_send) stays the human's move.

## The cyan-media `conform` arg shape this EMITS (must agree with cyan-media)

Emitted (matches `cyan-media/schemas/conform.in.json`, branch `feat/conform-tool`):

```json
{
  "input": "<proxy path / ref>",
  "fps": 24.0,
  "ops": [
    { "op": "lift", "tc_in": 48, "tc_out": 72, "params": {} },
    { "op": "mute", "tc_in": 200, "tc_out": 224, "params": {} }
  ]
}
```

Consumed (matches `cyan-media/schemas/conform.out.json`):

```json
{
  "output_path": "<rendered proxy path>",
  "applied": [ { "op": "...", "tc_in": …, "tc_out": … } ],
  "needs_manual": [ { "op": "...", "reason": "..." } ],
  "size_bytes": 123456
}
```

### Schema agreement — verified, with ONE flagged gap

- ✅ `input` (required, minLength 1), `fps` (number), `ops[]` items
  `{op, tc_in, tc_out, params}` — the engine emits exactly this. `additionalProperties:
  false` on the op item → we send ONLY those four keys (we do).
- ✅ output `output_path` / `applied` / `needs_manual` (`{op, reason}`) — consumed as
  written; `size_bytes` ignored.
- ⚠️ **FLAG (not a bug, a design note):** cyan-media returns `output_path` (a
  filesystem path, itself content-addressed by the plugin over the op-list + fps via
  `derived_path`) — it does **NOT** return a Blake3 of the rendered ESSENCE. So this
  side derives the new proxy asset's `hash` by Blake3-ing the `output_path`
  (`proxy_hash_from_output`). That keeps the derivation edge deterministic and
  re-runnable. If/when cyan-media adds a real essence-hash field to `conform.out.json`,
  switch `proxy_hash_from_output` to read it (the asset `hash` should be essence
  identity, not path identity). Until then this is the honest best available.
- ⚠️ `input`: prod path-resolves the proxy to the board's real container (via
  `find_video_uri`, like every other cyan-media step) in
  `pipeline_executor::HostConformDispatch`; the engine seeds `input` with `proxy_ref`
  as a non-empty fallback so the arg is never empty (cyan-media requires it).

## Wiring

- **Template** — the `builtin:frameio-review-loop` seed's conform step is now bound to
  `@cyan-media.conform` (`templates.rs`). The three `@frameio.*` binds and both
  `/needs-approval` uploads are unchanged.
- **Prod adapter** — `pipeline_executor::execute_conform_step` +
  `HostConformDispatch` wrap the real supervised cyan-mcp `dispatch_mcp_tool` path as
  a `ConformDispatch`, so the engine function runs the REAL cyan-media `conform` tool
  on-device (spawns the bundle, side_effects:none → runs). This is the prod entry the
  run loop calls for the conform step. (It is additive; binding the run-loop
  step-dispatch to it is the one remaining hookup — see "Unverifiable / next" — the
  function is ready and unit-covered through the seam.)

## What is TESTED (fakes, no ffmpeg / no infra) — `tests/conform_in_loop_test.rs`

All on one in-memory SQLite DB with the four migrations; assertions on
storage/changelist/asset rows, never logs. A `FakeConform` implements
`ConformDispatch`, CAPTURES the args, and returns a scripted `conform.out.json`.

1. `conform_applies_approved_ops_registers_proxy_and_advances_round` — approved ops
   gathered in **seq order** (`lift`, `mute`), the creative note EXCLUDED; the FAKE
   dispatch's captured args carry **exactly** those two ops + `fps` (asserts each
   `{op, tc_in, tc_out, params}`) and NOT the note text; the returned proxy registers
   as a DERIVED asset (`derived_from_asset == master`, `derived_from_version == the new
   version`), with `output_path` recorded; a new Version (v2) is frozen and its
   conform_plan is the two applied ops; the round advanced (CONFORMING).
2. `conform_surfaces_needs_manual_as_ledger_asks` — a `needs_manual` op comes back and
   is surfaced as a durable `kind=note, source=cyan, ask:"conform_needs_manual"` ledger
   entry (op + reason in params); exactly one such ask (dedups by content) — **never
   dropped**.
3. `round2_sense_on_new_proxy_remaps_through_conform_map` — after conforming a
   structural `lift [48,72)`, the new version's `conform_map` is NOT identity;
   `proxy_to_master(100) == 124`; a round-2 SENSE ingest on the NEW proxy (`file_r2`)
   lands the comment at **master 124** (`remap_observed` keys off the new version's
   ops), with the raw proxy observation preserved.
4. `conform_requires_confirmed_state_no_rogue_render` — an un-confirmed round
   (NOTES_IN, not CONFORMING) is rejected at the guard; **no** rogue Version is frozen
   and **no** proxy registered.

No existing `review_loop` / `conform_map` / `review_state` test was modified or
weakened. Additive: `changelist::approved_ops` (new pub helper) and the whole conform
section in `review_loop.rs`.

## Unverifiable here (needs the real rig)

- The **actual ffmpeg conform** — the FAKE returns a scripted `output_path`; the real
  op→ffmpeg render (Part 8b) is exercised in cyan-media's own suite
  (`tests/test_conform.py` / `test_realmedia.py`), not here.
- The **live re-upload** of the new proxy to Frame.io (the following
  `@frameio.upload /needs-approval` external_send step) — human-gated, not driven in
  the unit suite.
- The **prod run-loop hookup**: `execute_conform_step` is ready and seam-covered, but
  binding the run-loop's conform-step dispatch to it (resolving the current
  `proxy_ref` + tenant from board state) is the one integration wire left; it needs
  the process-global DB + an installed cyan-media bundle, i.e. the `make up` rig, not
  a fake unit test.
- **Essence-hash identity** for the new proxy (see the flagged gap) — depends on a
  cyan-media schema addition.
