# STATUS — ROUND 8 / W1: Notebook → Workflow surface (backend)

Branch: `feat/round8-workflow` (off `71ff625`, the anti-entropy + wave-executor tip).
Scope: collapse the notebook to **one authoring primitive — the plain-English step**.
FFI kept strictly additive. All new rows/queries tenant-scoped (tenant = board's group).

## The model collapse

The six former authorable cell kinds (`markdown` / `mermaid` / `canvas` / `image` /
`code` / `model`) are no longer authorable. There is exactly **one** authorable kind:
`step` (markdown *content* is kept verbatim as the step text).

- New module `src/workflow.rs` owns the model:
  - `authorable_kinds()` → `["step"]`, `is_authorable_kind()`, `is_system_kind()`.
  - `coerce_authoring_cell_type(requested)` — the single canonicalization every
    authoring write goes through. Any authorable intent → `step`; system kinds
    (`timecode_note`) and the `archived` sentinel pass through unchanged.
- Enforced at **every** write seam (no path can author a non-step):
  - FFI `cyan_save_notebook_cell` (`src/ffi/core.rs`) coerces `cell_type`.
  - `CommandActor` `AddNotebookCell` / `UpdateNotebookCell` (`src/lib.rs`) coerce.
- **mermaid/DAG is compiled OUTPUT, never an input** — it already lives on the
  dashboard (`diagram_gen` / dashboard producer); nothing authors it.

## Migration (no data loss)

`storage::migrate_legacy_authoring_cells()` (run automatically from `run_migrations`,
and callable directly) collapses existing boards:

- Text-bearing kinds (`markdown`, `code`) → **`step`** (text becomes the step text).
- Non-text kinds (`mermaid`, `canvas`, `image`, `model`) → **`archived`**: the row is
  KEPT, content preserved, original kind stashed in
  `metadata_json.original_cell_type` (reversible). **Never silently dropped.**
- **Idempotent** — re-running migrates 0 rows.
- **Digest-safe:** the migration does **not** touch `created_at`/`updated_at`, so it is
  invisible to the anti-entropy convergence digest. Each peer reaches the same
  migrated state deterministically — no spurious repair churn, sync thresholds intact.
- `archived` cells are excluded from `compile` (`load_pipeline_cells` filters them) but
  remain loadable over the existing FFI (iOS decides how to surface them).

## Compile preserved

`cyan_pipeline_compile` is unchanged in shape. `pipeline::compile_pipeline` /
`compile_via_llm` read step cells exactly as before (archived rows excluded), so the
plan still materializes. `compile_still_produces_plan` proves a 2-step board compiles.

## Autocomplete index query (`@` / `#` / `/`)

`workflow::autocomplete_index(board_id) -> AutocompleteIndex` — tenant-scoped to the
board's group:

- `@` **plugins** — installed `.cyanplugin` bundles in the group's Plugins workspace
  (`storage::plugin_bundles_in_group`).
- `#` **artifacts** — the tenant's files (`file_list_by_group`) + this board's
  **prior-step outputs** (cells with a non-empty `output`).
- `/` **actions** — the controlled verb set `workflow::CONTROL_ACTIONS`
  (`run`, `approve`, `needs-approval`, `send-to`, `connect`, `compile`, `retry`, `skip`).
- Every entry carries its trigger char; the index carries `tenant_id`. Cross-tenant
  plugins/files never leak (proven in the test).

## Tests (test-first, all green)

`tests/workflow_step_test.rs` — the five §W1 names, written RED then green:

- `single_step_type_is_the_only_authorable_kind`
- `compile_still_produces_plan`
- `legacy_cells_migrate_without_data_loss`
- `step_text_roundtrips_and_syncs`  (rides the existing cell snapshot + group digest)
- `autocomplete_index_query_returns_tools_artifacts_actions`

```
cargo test --test workflow_step_test → 5 passed
```

## No regression

- `cargo build --tests` — clean.
- Full in-process **substrate** suite re-run green (sync/snapshot/chat/discovery,
  reliability/resilience/presence/offline/identity/files/lens, grant/qr/crux,
  scaffolds): convergence + snapshot round-trip thresholds preserved. Steps are just
  cells, so they converge through the unchanged anti-entropy path.
- Pre-existing, **unrelated** baseline failure (present at `71ff625`):
  `diagram_gen::tests::test_parse_diagram_json` — not touched by W1.

## Gate notes

- New engine/FFI code uses `?`/`map_err` — **no `unwrap()`/`panic!`** added.
- `cargo clippy --all-targets -- -D warnings`: my new files (`workflow.rs`,
  `workflow_step_test.rs`) produce **zero** findings. The repo's existing 600+
  `disallowed unwrap` lints (e.g. `db().lock().unwrap()` throughout `storage.rs`)
  pre-date this branch and are unchanged — I introduced none.

## What's iOS-side / Tier-2 (batch 2a)

- Workflow face (rename from Notebook), `+ Add step`, guided inferred chips + inline
  ambiguity prompts, and the `@ # /` autocomplete UI consuming
  `workflow::autocomplete_index` (needs a thin FFI verb to expose it to Swift —
  additive `cyan_autocomplete_index(board_id)` is the natural next seam; not added
  yet to keep this diff backend-only and reviewable).
- Surfacing/hiding `archived` legacy cells in the Workflow view (rows are preserved
  and loadable today; presentation is an iOS choice).
- iOS tests: `add_step_creates_english_step`, `compile_shows_inferred_tool_and_gaps`,
  `autocomplete_triggers_at_hash_slash`, `no_cell_type_picker_present`.
- xcframework rebuild from the backend chain tip (do NOT rebuild here).
