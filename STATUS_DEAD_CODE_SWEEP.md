# STATUS — Dead-code sweep (canvas / whiteboard / mermaid / diagram)

Branch: `chore/dead-code-sweep`. Goal: delete the dead surface left behind when
canvas/whiteboard + the mermaid cell type were removed, **without** touching any
live feature. The failing `diagram_gen::tests::test_parse_diagram_json` had to go.

## TL;DR

The genuinely-dead surface — the whole `diagram_gen` module (including the failing
`test_parse_diagram_json`) and the `generate_diagram` command/dispatch that fed it
— was already removed on this branch in commit `b339274`. Everything else the
prompt listed as a deletion target turned out to be **still live or load-bearing**
and was kept. The safety gate (build + tests + clippy) is green; the one failing
test is a pre-existing, unrelated grant-replay test.

## DELETED (files + symbols)

Removed in `b339274 chore(dead-code): remove diagram_gen (canvas mermaid/SVG generation)`:

- **`src/diagram_gen.rs`** — entire 645-line module, including the failing
  `tests::test_parse_diagram_json` (it asserted `svg.is_some()` against a path that
  always returned `svg=None`). Gone, not "fixed".
- **`pub mod diagram_gen;`** in `src/lib.rs`.
- **`AICommand::GenerateDiagram`** variant + its dispatch arm + `cmd_generate_diagram`
  in `src/ai_bridge.rs` (88 lines). No live face sends this command.

Verified clean: `grep -rn "diagram_gen\|GenerateDiagram\|generate_diagram\|test_parse_diagram_json" src/ tests/` → no matches. No `DROP TABLE` introduced anywhere.

## KEPT — because still live / load-bearing (do NOT delete)

The prompt estimated "~82 refs" of removable canvas/whiteboard/mermaid surface.
Investigation (and the build/tests, the stated safety net) shows the remainder is
live. Each was checked for a real caller before being kept:

- **`type='whiteboard'` object paths + `WhiteboardDTO`** = the live **Board shell**.
  Boards (Notebook/Notes/Dashboard) are stored as `objects` rows with
  `type='whiteboard'`; `board_list`, `board_insert`, snapshot structure, and the
  board-mode FFI all key off it. Removing it deletes boards. — `src/storage.rs`,
  `src/lib.rs`, `src/ffi/core.rs`, `src/models/dto.rs`.
- **`whiteboard_elements` table + `WhiteboardElementDTO` + element commands/events/FFI**
  = **load-bearing P2P sync + DB schema**, and read by the live iOS
  `BoardPreviewLoader` (calls `cyan_load_whiteboard_elements` / `cyan_get_board_mode`).
  Proof of liveness: the in-process substrate snapshot test syncs
  `CONTENT: elements=5 cells=3` — element rows travel the live snapshot/delta path
  and are asserted on. Deleting this would change shipping P2P behavior and break a
  covered test. — `src/storage.rs`, `src/actors/topic_actor.rs`, `src/snapshot.rs`,
  `src/ffi/core.rs`, `src/models/{commands,events,dto,protocol}.rs`.
- **`image_to_mermaid` / `MermaidResult`** (`src/ai_bridge.rs`) = a generic AI command,
  separate from the deleted diagram generator. It is reachable from iOS via the
  generic `cyan_ai_command` FFI (JSON tag `image_to_mermaid` → `AICommand::ImageToMermaid`).
  Not provably dead from the backend, so kept.
- **mermaid / canvas cell-type variants** — already collapsed to `step` via the live
  `coerce_authoring_cell_type` / `migrate_legacy_authoring_cells` migration. The
  `AUTHORING_CELL_TYPES` list (`["markdown","mermaid","canvas","image","code","model"]`
  in `src/workflow.rs`) is the legacy-input list that migration *reads* to collapse old
  rows — it is live migration code, not a dead variant. Boards expose only
  Notebook/Notes/Dashboard; the legacy kinds are no longer authorable but must remain
  recognized so old data migrates.

## DB-migration note

No schema break, no data loss:
- No `DROP TABLE` / `DROP COLUMN` added anywhere.
- `whiteboard_elements`, `objects`, `board_metadata`, `notebook_cells` tables are all
  still created (`CREATE TABLE IF NOT EXISTS …`) and their forward migrations
  (`cell_id`, `board_mode`, legacy `freeform→canvas` normalization) are untouched.
- `open_db` / `run_migrations` behave identically. Existing devices' data is preserved.

## Verify gate — cyan-backend: PASS

- `cargo build --tests` — **green**.
- `cargo clippy --all-targets -- -D warnings` — **exit 0** (only warnings are from the
  external `xaeroid` path dependency, out of scope; `cyan-backend` itself: zero).
- `cargo test` — all binaries green **except one pre-existing, unrelated failure**:
  `expired_revoked_replayed_grant_rejected` (substrate_multiuser_mp). It asserts a
  replayed grant QR (consumed nonce) is not re-served — a grant/revocation test in the
  identity subsystem, nothing to do with canvas/diagram. It failed identically before
  this sweep (documented in `b339274`). The diagram test is **gone, not failing**.

## cyan-iOS leg — NOT executed here

The iOS repo is **not present in this environment** (no `cyan-iOS` checkout; only a
stale `~/Downloads/BoardPreviewLoader_Fixed.swift` snapshot, which itself still calls
`cyan_load_whiteboard_elements` — corroborating that the backend element FFI is live).
The iOS dead-UI removal (Canvas view, mermaid/diagram cell UI, dead enum cases) and
`xcodebuild build` + CyanTests **could not be run or verified** from cyan-backend and
remain TODO for whoever has the iOS checkout. Because iOS liveness could not be
verified, the conservative rule held: keep the backend FFI/DTO surface iOS may still
call.

## Bottom line

Dead `diagram_gen` surface (incl. the failing test) is removed; the remaining
canvas/whiteboard/mermaid references are live board/sync/AI code and were kept per the
"build + tests are the safety net, keep anything live" rule. Backend gate is green.
