# STATUS_R10FB_BACKEND — Round-10 dogfood feedback, ENGINE side

Branch: `feat/r10fb-backend` (stacked on `harness/mesh-e2e`; never main). iroh 0.95.
Additive FFI only — no existing `cyan_*` shape changed. Tenant-scoped, no `unwrap()`/`panic!`
in engine/FFI paths, bounded `tokio::time::timeout` in tests, asserted on real state.

Implements ROUND10_FEEDBACK_SPEC §N (notifications), §F (files), §B3 (pin sync),
§L (live activity), §D (remove demo-seed). iOS side is a separate prompt; the FFI/event
names iOS must match are listed below.

`cargo build --tests` ✅ · `cargo clippy --all-targets -- -D warnings` ✅ (lint baseline stays
green) · 13 new tests ✅ · affected existing substrate suites ✅ (chat, files, live, sync,
catchup, snapshot_mp, offline, presence, notes). Pre-existing unrelated failure
`diagram_gen::tests::test_parse_diagram_json` fails identically on the base branch (needs an
SVG renderer; not touched here).

---

## §N — Authoritative unread model

A **per-reader unread ledger**, idempotent **by `message_id`** — a message counts once, ever.
This kills both live bugs: the "count went to 2 for 1 message" (idempotent insert) and
"opening chat increments" (opening is a READ via the chat-list path; it never records unread).

**Storage** (`src/storage.rs`): new `unread` table — `message_id` PRIMARY KEY (idempotency),
`kind` (`'chat'` now; the clean seam for nudges/asks/decisions §N5), `group_id/workspace_id/
board_id` (rollup scopes), `read`, `created_at`. Functions:
- `unread_record(message_id, kind, group, ws, board, created_at) -> bool` — `INSERT OR IGNORE`;
  returns `true` only on a real first-time increment.
- `unread_counts() -> HashMap<scope_id, count>` — each open item adds +1 to each of its
  non-null board/workspace/group ids, so one map answers all three levels + the dock total.
- `unread_mark_read(scope_id) -> usize` — clears every open item whose board OR workspace OR
  group equals `scope_id`; read state is sticky so re-delivery never resurrects the count.

**Increment hook** (`src/actors/topic_actor.rs`): the single-FIFO persist worker calls
`account_unread(evt, local_node_id)` — an incoming `ChatSent` from a **non-author** records
unread once and emits `UnreadChanged` *before* the chat event so badges update live. Own
messages (`author == local_node_id`) and re-deliveries are no-ops.

**Mark-read** (`src/lib.rs`): `CommandMsg::MarkRead { scope_id }` clears + emits `UnreadChanged`.

### Scope note (engine reality)
Chat is **workspace-scoped** in this engine (`ChatSent.workspace_id`, no board id). So an
incoming chat increments unread under its **workspace + group**; the **board dimension** is
populated by board-scoped item types (the §N5 seam). The rollup machinery itself is fully
board→workspace→group and is exercised end-to-end in `rollup_board_to_workspace_to_group`.
`cyan_mark_read` therefore accepts **any** scope id (board/workspace/group) — iOS marks read
with whatever container it opened, and the rollups drop automatically.

### FFI for iOS to match (additive)
- `cyan_unread_counts() -> *mut c_char` — JSON `{scope_id: count}` (board, ws, group ids).
- `cyan_mark_read(scope_id: *const c_char)` — open == read; clears + adjusts rollups.
- Event `SwiftEvent::UnreadChanged { counts: {scope_id: count} }` — routed to `file_tree`,
  `board_grid` and `network` (dock badge) buffers; receive-only, re-read the map.

Tests (`tests/substrate_unread.rs`, all green): `message_increments_unread_once`,
`reopening_chat_does_not_increment`, `mark_read_clears_and_rolls_up`,
`unread_idempotent_by_message_id`, `rollup_board_to_workspace_to_group`.

---

## §F — Files

- **F1 board-scoped (verified, kept):** files persist at board level via `objects.board_id`;
  new `storage::file_list_by_board`.
- **F2 unique names + dedupe:** `storage::file_insert_dedup` enforces unique names within a
  level (board → workspace → group, most-specific wins). Same name + same content ⇒ dedupe to
  the existing row; same name + different content ⇒ auto-rename `name (2)`, `name (3)`… The
  plain `file_insert*` sync paths are unchanged (ids preserved so replicas converge).
- **F3 stable handle:** `storage::file_resolve_handle(group, ws, board, file_name)` resolves
  the `group_id:workspace_id:board_id:file_name` handle. FFI `cyan_resolve_file_handle` → JSON.
- **F4 delete (soft/tombstone, syncs):** new `objects.deleted` column; `storage::file_soft_delete`
  tombstones (never hard-deletes). `CommandMsg::DeleteFile` + FFI `cyan_delete_file` gossip
  `NetworkEvent::FileDeleted { id, deleted_at }`; the peer applies the same tombstone. All file
  reads (`file_list_by_group`, `file_list_by_board`, `file_resolve_handle`) filter `deleted=0`.

### FFI for iOS (additive)
- `cyan_delete_file(file_id)` · `cyan_resolve_file_handle(group_id, workspace_id, board_id, file_name) -> JSON|null`.

Tests (`tests/substrate_files_r10.rs`, green): `files_are_board_scoped`,
`duplicate_name_rejected_or_deduped`, `file_resolvable_by_gwbf_handle`,
`delete_tombstones_and_syncs`.

---

## §B3 — Pin sync

A board's pinned flag (`board_metadata.is_pinned`, what iOS board cards already read) is now a
**synced board property**. `CommandMsg::SetBoardPinned` upserts the flag and gossips
`NetworkEvent::BoardPinned { board_id, is_pinned, updated_at }`; the peer applies it via
`storage::board_meta_set_pinned` (upsert, so it lands even with no prior metadata row). The
previous handler did a local-only `UPDATE` — that was the "pin didn't show on peer 2" bug.
(The unrelated ROUND8 `pins`/`PinSet` workflow-pin path is untouched.)

Test (`tests/substrate_pin_activity.rs`, green): `pin_propagates_to_peer`.

---

## §L — Live activity (board-changed)

New `NetworkEvent::BoardChanged { board_id, editor, ts }`, gossiped on every local board edit
(`note_board_activity` is called from the whiteboard element add/update/delete + clear, notebook
cell add/update/delete/reorder, and board rename handlers in `src/lib.rs`). Receive-only on the
peer side — surfaced as `SwiftEvent::Network(BoardChanged)` (routed to `file_tree` + `board_grid`)
so peers refresh that board's preview LIVE and show a "recently active/edited" marker attributed
to `editor`. No storage write (transient signal).

Tests (`tests/substrate_pin_activity.rs`, green): `board_edit_emits_change_event`,
`change_event_carries_editor_and_board`. (As with `substrate_chat`, the in-process harness drives
the gossip wire via `broadcast()`; the production edit handlers emit the same `BoardChanged`.)

---

## §D — Remove demo-seed

The demo-seed helper `seed_demo_if_empty()` (was lib.rs:1512) is **removed** — a fresh/empty DB
never auto-creates a "Demo Group"/"Demo Board". `CommandMsg::SeedDemoIfEmpty` and the FFI
`cyan_seed_demo_if_empty` are kept as **inert no-ops** (gated) so the C ABI stays stable until
iOS stops calling them (iOS removal is its own prompt). The engine now creates no data on its
own; first-run is the app's empty state.

Test (`tests/substrate_pin_activity.rs`, green): `fresh_db_creates_no_demo_group`.

---

## Files touched
- `src/models/events.rs` — `NetworkEvent::{BoardChanged, BoardPinned, FileDeleted}`,
  `SwiftEvent::UnreadChanged` (additive).
- `src/models/commands.rs` — `CommandMsg::{MarkRead, DeleteFile}` (additive).
- `src/storage.rs` — migrations (`unread` table, `objects.deleted`); unread/file/pin functions.
- `src/lib.rs` — SetBoardPinned now syncs; MarkRead/DeleteFile handlers; `note_board_activity`
  + wiring; event routing for the new variants; demo-seed helper removed.
- `src/actors/topic_actor.rs` — persist arms for BoardPinned/FileDeleted/BoardChanged; unread
  accounting in the persist worker.
- `src/ffi/core.rs` — `cyan_unread_counts`, `cyan_mark_read`, `cyan_delete_file`,
  `cyan_resolve_file_handle`; demo-seed FFI gated.
- `tests/substrate_unread.rs`, `tests/substrate_files_r10.rs`, `tests/substrate_pin_activity.rs`.

No xcframework rebuild (per prompt). cbindgen auto-exposes the new `cyan_*` symbols on the next
header generation.
