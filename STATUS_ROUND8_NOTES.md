# STATUS — Round 8 / W2: Notes as a board-level authored LWW ledger

**Branch:** `feat/round8-notes` (off `feat/round8-workflow`)
**Scope:** backend only. Additive FFI. iOS Notes face is batch 2b (not here). xcframework
NOT rebuilt (per the prompt).

## What shipped

Notes are now their **own store with their own sync stream** — fully decoupled from the
notebook/cell model. A `Note { id, board_id, tenant_id, author_id, author_name, text,
created_at, updated_at }` is board-level, authored, editable, and converges under the
existing anti-entropy digest exactly like chats.

### The store (`src/storage.rs`, `notes` table)
- New `notes` table (created in `lib.rs::ensure_schema` for fresh DBs + an idempotent
  migration in `storage::run_migrations` for DBs that predate it).
- `note_upsert(&NoteDTO) -> bool` — idempotent **upsert-by-id** with **LWW on
  `updated_at`**: `INSERT … ON CONFLICT(id) DO UPDATE … WHERE excluded.updated_at >
  notes.updated_at`. Older **or equal** writes are no-ops (so snapshot apply /
  anti-entropy repair re-apply the same state without churn). `created_at` is preserved
  across edits. Returns whether state actually changed.
- `note_list_by_board(board_id, tenant_id)` — **tenant-scoped** read (a note never
  crosses the tenant boundary even when the board id is known).
- `note_list_by_boards(board_ids)` — all notes under a board set (digest + snapshot).
- `note_get(id)`, `note_delete(id)`.

### LWW
Conflict resolution is last-writer-wins on `updated_at`. The `>` comparison makes equal
timestamps idempotent, which is what keeps snapshot apply and the digest repair
convergent (no resurrection / no churn).

### Digest inclusion (`src/anti_entropy.rs`)
`group_digest` now folds notes in as `n␁<id>␁<updated_at>` lines. Adding a note advances
the count + flips the hash; an edit (new `updated_at`) flips the hash without changing the
count; an idempotent re-apply leaves the digest stable. So the existing bounded,
jittered sweep detects a peer missing/behind on notes and pulls a merge snapshot — the
**same mechanism chats use**, no new transfer protocol.

### Snapshot transfer (`src/models/protocol.rs`, network_actor, topic_actor)
`SnapshotFrame::Metadata` carries `notes: Vec<NoteDTO>` (`#[serde(default)]` ⇒
wire-compatible both ways with older peers). The holder serializes notes; the receiver
applies them via the idempotent LWW `note_upsert`. The anti-entropy repair pull reuses
this path, so digest-detected divergence is reconciled by a snapshot merge.

### Events / commands / FFI (additive only)
- `NetworkEvent::NoteAdded / NoteUpdated / NoteDeleted` (Added/Updated carry the full
  note; both apply identically via LWW upsert — the split is informational for the UI).
- `CommandMsg::PutNote { board_id, note_id?, tenant_id?, text } / DeleteNote { id }`.
- FFI verbs: **`cyan_note_put`** (note_id null ⇒ create, non-null ⇒ edit; tenant null ⇒
  derived from the board's group), **`cyan_note_list`** (JSON `[NoteDTO]`, caller frees
  with `cyan_free_string`), **`cyan_note_delete`**. No existing `cyan_*` signature,
  event, or command was renamed/reordered/repurposed.
- `author_name` resolves from the author's XaeroID profile (`storage::profile_get`, the
  same path presence/chat use), falling back to the raw id when no profile exists yet.
- Tenant: every note row, query, event, and the obs line carries `tenant_id` (derived
  from the board's group when not supplied). One obs line per put: flat
  `obs note_put …` with `tenant_id`.

## Tests (all named per §W2; none weakened)
Storage + digest (`tests/notes_test.rs`, 5/5 green):
- `note_carries_author_and_timestamps`
- `note_update_is_lww_by_updated_at`
- `notes_are_not_notebook_cells`
- `notes_included_in_digest`
- `note_tenant_scoped`

Multi-process convergence (`tests/substrate_notes_mp.rs`, green):
- `two_peers_converge_on_notes` — two real `cyan_node` OS processes, each posts notes
  **locally only (no broadcast)** into its own SQLite DB; ONLY the anti-entropy
  digest+snapshot path can reconcile them, and both converge to the exact union (no loss,
  no dupes). Bounded waits; asserted on each receiver's own `count notes`.

Harness additions (additive): `count notes` kind + `post_notes` verb in `cyan_node`,
`MpNode::post_notes`.

## No regression
- `tests/notes_test.rs` 5/5, `tests/substrate_notes_mp.rs` 1/1.
- Anti-entropy convergence threshold re-runs and **holds with notes in the digest**:
  `concurrent_edits_converge_no_dupes` ✅ and `expired_revoked_replayed_grant_rejected`
  ✅ pass in isolation. (When the *entire* multi-process suite is run in one `cargo test`
  invocation, these two intermittently fail on single-box CPU/port contention — an
  iroh-gossip panic, the documented "single-box pressure" of stacking the heaviest MP
  scenarios — not a notes regression; each is green run on its own.)
- `cargo build --tests` green. `cargo clippy --all-targets` adds exactly **one** lint vs
  the base: a single `whiteboard.lock().unwrap()` in the event-router arm, identical to
  every sibling arm in that block (the repo already carries 485 of these and is not
  `-D warnings`-clean on the base). No new lint *type* introduced.
- Pre-existing unrelated failure not touched: `diagram_gen::tests::test_parse_diagram_json`
  fails on the base branch too.

## Tier-2 / deferred (out of scope here)
- iOS Notes face (author + time per note, edit = LWW, no markdown-cell creation) — batch 2b.
- Note **deletes** are hard deletes + a live `NoteDeleted` broadcast (mirrors
  `chat_delete`); there is no delete-tombstone, so a delete is not reconciled by the
  digest against a peer that never heard the live event. W2 ships additive-note
  convergence; a tombstoned delete-convergence is a later hardening if needed.
- `TreeSnapshotDTO` (the FFI tree-refresh blob) was intentionally left unchanged — notes
  have their own authoritative read path (`cyan_note_list`), so they do not ride the tree
  refresh.
