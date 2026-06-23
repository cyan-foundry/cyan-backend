# STATUS — Round-11 backend (fix/r11-backend)

Engine-side fixes for the R11 dogfood punchlist (items 1, 2, 3, 5, 6, 9, 9b + the
board-state-sync PATTERN note). Branch `fix/r11-backend` off `feat/w17-backend`. Stable
engine/FFI — additive, no `cyan_*` removed/reordered, no `unwrap`/`panic` in engine/FFI,
bounded waits in tests, asserts on real `storage::*` state. iroh 0.95 untouched.

Out of scope (left for iOS / later): items 4, 7, 8, 10, 11, 11b (UI/iOS), and 12–16 (bigger
builds). This doc is engine-only.

---

## 1 — Chat re-keyed WORKSPACE → BOARD (P0, item 1)

Chat was keyed by `workspace_id`, so every board in a workspace shared one thread (chat bled
across boards). Chat is now **board-scoped**.

- `storage.rs`:
  - `chat_insert(id, board_id, workspace_id, …)` / `chat_insert_simple(id, board_id, workspace_id, …)`
    — the `objects` chat row now carries **both** `board_id` (the scope key) and `workspace_id`
    (kept for workspace→group snapshot scoping + gossip resolution).
  - **new** `chat_list_by_board(board_id)` — the read the chat panel opens with.
  - `chat_list_by_workspace(s)` retained (snapshot scoping); every `ChatDTO` now carries `board_id`.
  - **migration** `migrate_chats_to_boards()` (run from `run_migrations` on the init connection,
    idempotent): each legacy chat → its workspace's **deterministic default board** (earliest
    board by `created_at,id`); a chat whose workspace has no board is kept on a stable default
    board id = its `workspace_id` (never dropped).
- Threaded through: `ChatDTO.board_id` (DTO), `NetworkEvent::ChatSent.board_id` (gossip),
  `CommandMsg::SendChat { board_id, .. }` / `LoadChatHistory { board_id }`,
  `SwiftEvent::ChatHistoryComplete { board_id, workspace_id }`, snapshot build/apply, the
  topic-actor persist path, and the tree-snapshot builder.
- All new event/DTO fields are `#[serde(default)]` → gossip + snapshot stay wire-compatible
  with a pre-R11 peer (a missing `board_id` falls back to the workspace and re-keys on migration).

**Tests** (`tests/substrate_chat.rs`): `chat_is_board_scoped`,
`two_boards_same_workspace_have_separate_chats`, `legacy_chat_migrates_to_board`.

## 2 / 3 / 5 / 6 — Notifications: idempotent, board-level only

- **Idempotent by `message_id`** (unchanged guarantee, kept): a message increments unread
  exactly once per reader, ever — gossip echoes / re-syncs never re-increment; opening a chat
  is a read, never a write.
- **Board-level ONLY** — dropped the workspace + group rollup. That rollup was the doubled-count
  bug (one message rolled up to board AND workspace AND group, so summing the map triple-counted
  it → `1→2`, `2→4`).
  - `unread_record(message_id, kind, board_id, created_at)` — board only.
  - `unread_counts() -> {board_id: count}` — board only (sum the map for the dock badge).
  - `unread_mark_read(board_id)` — clears that board; `cyan_mark_read(board_id)` emits
    `UnreadChanged` so iOS + the dock badge update live.
- `topic_actor::account_unread` records board-scoped (board from the `ChatSent` event).

**Tests** (`tests/substrate_unread.rs`): `message_increments_once`,
`no_rollup_to_workspace_or_group`, `mark_read_clears_board`, `reopen_does_not_increment`,
`two_boards_counted_independently`.

## 9 / 9b / PATTERN — Board-state convergent sync

Audited `board_metadata` gossip+merge as a unit. The merge was a **whole-record replace**, so a
stale snapshot row clobbered any field a peer had edited (and un-pinned boards a peer just
pinned). Fixed to **per-field convergent LWW, never whole-record replace**.

- **`board_metadata_upsert(…, meta_updated_at, pin_updated_at)`** — three independent lanes:
  - **descriptive** (labels/rating/contains_model/contains_skills/board_type) applied only when
    `meta_updated_at` is strictly newer;
  - **pin** (`is_pinned`) applied only when `pin_updated_at` is strictly newer;
  - **activity counters** (`view_count`, `last_accessed`) merged with `MAX` (monotonic).
  New columns `meta_updated_at` / `pin_updated_at` (canonical schema + idempotent migration).
  `BoardMetadataDTO` carries both clocks (`#[serde(default)]`); `snapshot_insert_metadata` now
  routes through this merge (no more `INSERT OR REPLACE`).
- **Pin (9b)** — `board_meta_set_pinned(board_id, is_pinned, updated_at)` is now a per-board
  convergent LWW flag, gossiped per-board via `BoardPinned { updated_at }`. Pins from multiple
  peers MERGE; a stale pin never clobbers. (Shared/team pins, per the punchlist "for now: shared,
  convergent, no clobber"; per-USER pins remain a future toggle.)
- **Live board preview (9)** — `NetworkEvent::BoardChanged` now carries `name` + `preview`
  (built by `storage::board_preview`: board name + latest cell/note snippet, truncated). The
  peer's preview card refreshes live on an edit instead of staying blank. Receive-only/additive
  (no storage write on the peer).

**Tests** (`tests/substrate_pin_activity.rs`): `board_change_event_carries_preview_data`,
`pins_from_two_peers_merge_not_clobber`, `board_metadata_field_lww_no_whole_record_clobber`.

---

## FFI surface (for iOS)

All additive / ABI-compatible:

- `cyan_send_chat(board_id, message, parent_id)` — **same C ABI** (3 `char*`); arg 1 is now a
  **board id** (was workspace id). iOS passes the board id.
- `cyan_load_chat_history(board_id)` — **new**, board-scoped; replays `ChatSent` onto the
  chat-panel buffer then emits `ChatHistoryComplete { board_id, workspace_id }`.
- `cyan_unread_counts()` — JSON `{board_id: count}` (board-level only; sum for the dock badge).
- `cyan_mark_read(board_id)` — clears that board's unread, emits `UnreadChanged`.
- Events: `ChatSent` gains `board_id`; `ChatHistoryComplete` gains `board_id`; `BoardChanged`
  gains `name` + `preview`. All `#[serde(default)]`.

## Verification

- `cargo build --tests` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean (cyan-backend).
- New tests green: chat (3), unread (5), board-state (3). Regression suites green:
  `substrate_chat`, `substrate_unread`, `substrate_pin_activity`, `substrate_sync`,
  `substrate_catchup`, `substrate_offline`, `substrate_presence`, `substrate_resilience`,
  `substrate_snapshot_mp`, `substrate_live`.
- Pre-existing (NOT introduced here): `diagram_gen::tests::test_parse_diagram_json` fails on
  `feat/w17-backend` too (verified by stash) — unrelated to these changes.

No xcframework rebuild.
