# STATUS — ROUND 12 backend (`feat/r12-backend`)

Branch `feat/r12-backend` off `fix/grant-replay-nonce`. Scope: cyan-backend engine/FFI only.
All items below are additive (no FFI signature/shape removed or repurposed). iroh 0.95, no
`unwrap`/`panic` on engine/FFI paths, bounded `tokio::time::timeout` waits in tests.

**Verify:** `cargo build` ✓ · `cargo test` (full substrate suite) ✓ all green (only the pre-existing
`#[ignore]` relay/CYAN_LIVE scaffolds skipped) · `cargo clippy --all-targets -- -D warnings` ✓ clean
(remaining warnings are in the upstream `xaeroid` crate, not this repo).

---

## P0 — C2 Pin convergence (bidirectional) + C1 Pin delta is live

**Root cause.** The board-pin merge layer was already correct (per-board LWW on `pin_updated_at`,
`storage::board_meta_set_pinned`), and the convergent command `CommandMsg::SetBoardPinned` (applies
the LWW clock, gossips `BoardPinned`, emits the SwiftEvent) already existed — **but nothing in the
FFI ever sent it.** The only board-pin FFI surface, `cyan_pin_board` / `cyan_unpin_board`
(`src/ffi/core.rs`), wrote `board_metadata.is_pinned` with a **direct local SQL UPDATE**:

- it never set `pin_updated_at`, so the LWW clock stayed `0` on both peers → the merge could never
  establish an ordering (C2: a re-pin on peer B had no effect back on peer A); and
- it never broadcast anything → the change never left the device (C1: unpin/pin was invisible on the
  other peer until a full group re-fetch — the "click the VFX group" workaround in the dogfood).

**Fix.** Route `cyan_pin_board`/`cyan_unpin_board` through `CommandMsg::SetBoardPinned`
(`src/ffi/core.rs`, `set_board_pinned_via_command`). That single seam gives both a real
`pin_updated_at` clock (per-board LWW = a valid CRDT LWW-Register, converges regardless of who wrote
last or arrival order) **and** the `BoardPinned` live delta to peers. No storage/merge change was
needed — the merge was sound; the producer was dead-code-unreachable from the app.

**Proof.** `tests/substrate_pin_activity.rs::pin_converges_bidirectionally` (new): two nodes meet;
A pins (baseline) → B observes via delta + store; **A unpins → B observes live**; **B re-pins → A
observes back** (the broken direction); then A re-broadcasts a *stale* unpin (lower clock) and it
does **not** clobber the newer re-pin. Each step asserts on the (process-global) `storage::board_is_pinned`
convergence oracle and uses `wait_network` to prove the change arrived as a delta, not a re-fetch.
Existing pin/merge tests still green.

**FFI for iOS.** No new symbols. `cyan_pin_board`/`cyan_unpin_board` keep their `(board_id) -> bool`
signature; the bool now means "convergent pin command enqueued" rather than "local SQL row written".
iOS should keep reacting to `BoardPinned` / `BoardMetadataUpdated` events (it already does) rather
than the return value for the persisted state.

---

## P1 — B1 File-received event

**Root cause.** An inbound file (`NetworkEvent::FileAvailable`) was persisted and forwarded only as a
generic `SwiftEvent::Network(FileAvailable)`; there was no distinct "a file arrived from a peer"
signal, so the receiving peer could only notify off the chat-message event.

**Fix.** New additive event `SwiftEvent::FileReceived { id, board_id, workspace_id, group_id, name,
hash, size, source_peer, created_at }` (`src/models/events.rs`), board-scoped like an inbound chat.
Emitted once per inbound `FileAvailable` in the topic-actor persist loop
(`src/actors/topic_actor.rs`, helper `file_received_event`), **guarded by `source_peer != local node`**
so the sender's own gossip echo never self-notifies (mirrors the `account_unread` non-author guard).
Routed to the file/board poll surfaces in `lib.rs` so iOS picks it up on the existing event poll.

**Proof.** `tests/substrate_files.rs::inbound_file_raises_file_received_event` (new): peer broadcasts
a `FileAvailable`; the other node receives a distinct `SwiftEvent::FileReceived` carrying the file
name + sender attribution.

**FFI for iOS.** New `SwiftEvent::FileReceived` variant (JSON `{"type":"FileReceived","data":{…}}`,
same `tag="type", content="data"` envelope as every other SwiftEvent). Raise the "file received"
notification off this; `board_id` is present for board-scoped routing.

---

## P1 — B3 File dedup

**Root cause.** The inbound file apply path used `storage::file_insert` = `INSERT OR IGNORE` keyed on
the `id` primary key only. The same content re-announced under a **different id** (a file followed by
a message re-shared the file) produced a second `objects` row with the same content hash in the same
board → rendered twice on the receiver.

**Fix.** `storage::file_insert` (`src/storage.rs`) now has two idempotency layers: `INSERT OR IGNORE`
on the `id` PK (replay of the same delta — preserves the id verbatim so snapshot convergence is
unaffected) **plus** a content-addressed `WHERE NOT EXISTS` guard on `(board scope, hash)` so the same
content under a new id collapses to the row already present in that board. Soft-deleted/tombstoned
rows don't suppress a re-share (delete→re-add still lands).

**Proof.** `tests/substrate_files_r10.rs::file_delta_applied_twice_dedups_to_one_row` (new): same
delta twice **and** same content/new-id both collapse to one row; a genuinely different hash still
inserts (the guard doesn't over-suppress). Existing F1–F4 file tests still green.

**FFI for iOS.** None (storage-layer behavior). iOS still reads `file_list_by_board`; it now returns a
single row per content per board.

---

## P1 — A2 Cold-chat latency (engine-side findings + fix shipped)

**Profile / findings (engine side of the cold path):**

1. **Full table scan on first open (fixed).** `storage::chat_list_by_board`
   (`SELECT … FROM objects WHERE type='chat' AND board_id=? ORDER BY created_at`) ran against the big
   multi-purpose `objects` table (chats + files + boards + whiteboard elements) with **no supporting
   index** → a full scan + sort on every first open of a board's chat.
   **Fix:** partial index `idx_objects_chat_board ON objects(board_id, created_at) WHERE type='chat'`
   (`src/storage.rs` migrations). `EXPLAIN QUERY PLAN` confirms the query is now
   `SEARCH objects USING INDEX idx_objects_chat_board (board_id=?)` — an index range scan that also
   supplies the `created_at` ordering (no separate sort). Partial (`WHERE type='chat'`) keeps the
   index small and off the file/board write paths. The warm cache is untouched.

2. **Per-message FFI event fan-out (noted, not changed — partly iOS).** `LoadChatHistory`
   (`src/lib.rs`) emits **one** `SwiftEvent::Network(ChatSent)` per message through the poll queue,
   then a `ChatHistoryComplete`. The engine decode itself is cheap; the avoidable serial work on the
   cold path is the N separate per-message JSON encodes + FFI hops the iOS side then decodes one-by-one
   before first paint. A batched first-page event (or replay only the most recent K, lazily paging
   older) would cut this, **but it changes the FFI event-stream shape iOS consumes**, so per
   CLAUDE.md ("additive FFI only, no shape change as a side effect") it is flagged here for the iOS
   agent rather than done unilaterally. The index above is the safe pure-engine win shipped this round.

**FFI for iOS.** None. (Recommendation for the iOS/FFI co-design: a batched first-page chat event.)

---

## Support state for iOS — D2 (deployed + dashboard) and E1 (lock + grant unlock)

Additive engine support state + the real org-grant round-trip. No enforcement UI here.

**D2 — per-board "workflow deployed + dashboard available".** New `board_workflow_state` table
(`board_id` PK, `deployed`, `dashboard_available`, `locked`, `updated_at`; LWW on `updated_at`) with
`storage::workflow_state_get` / `workflow_state_set_deployed` / `workflow_state_set_locked`, surfaced
as `dto::WorkflowStateDTO`. `workflow::mark_deployed(board, dashboard_available, now)` records that a
workflow is running (and locks it). A board with no row reads the default authoring state
(editable/unlocked/no dashboard), so the getter is always safe.

**E1 — workflow locked + unlock requires an org-XaeroID grant (W17).** A deployed workflow is
`locked`. `workflow::request_unlock(board, tenant, token, verifier, xaero_pubkey, now, revocation?)`
wires the real round-trip **request → org-grant check → unlock** on top of the existing W17 machinery
(`sso_grant::SsoSession::from_org_token{,_checked}` + `OrgGrantVerifier`). The unlock is approved
**iff** the presented token (1) verifies org-signed against the tenant's pinned org key (offline;
binding + `exp` + grace; optionally rejected by an org-signed revocation list), (2) is scoped to this
board's tenant, and (3) carries ≥ `Admin` authority. On any failure the board **stays locked** and an
error explains why — there is no unsigned/ad-hoc unlock path. The org signing key is the approval
authority, exactly as DECIDED in the punchlist.

**Proof.** `tests/workflow_lock_test.rs` (new):
- `deploy_sets_face_gating_state` — deploy sets `deployed + dashboard_available + locked`; an
  undeployed board reads the authoring default; state persists.
- `unlock_requires_org_admin_grant` — unlock is refused (board stays locked) for an unverifiable
  grant, a valid-but-Member grant, and a valid grant scoped to the wrong tenant; it succeeds (lock
  cleared, still deployed) only for a valid org **Admin** grant for the board's tenant.
- `revoked_approver_cannot_unlock` — an otherwise-valid, unexpired Admin grant is refused once the
  org publishes a signed revocation list naming the approver's device (W17 §C).

**FFI for iOS.** New additive read-only getter `cyan_board_workflow_state(board_id) -> JSON`
(`{"board_id","deployed","dashboard_available","locked","updated_at"}`) to gate the board face
(deployed + dashboard → show the running dashboard, not the editor). The unlock approval itself is an
engine function (`workflow::request_unlock`) gated by the org grant; wiring the verifier from pinned
per-tenant org config (and any unlock-request FFI verb) is the SSO/Lens-layer seam and is intentionally
left to that integration — the engine-side grant check + lock state are complete and tested here.

---

## Files touched

- `src/ffi/core.rs` — C1/C2 pin rewire; `cyan_board_workflow_state` getter (D2/E1).
- `src/models/events.rs` — `SwiftEvent::FileReceived` (B1).
- `src/actors/topic_actor.rs` — emit `FileReceived` in persist loop (B1).
- `src/lib.rs` — route `FileReceived` to poll surfaces (B1).
- `src/storage.rs` — `file_insert` content-hash dedup (B3); chat-by-board partial index (A2);
  `board_workflow_state` table + accessors (D2/E1).
- `src/models/dto.rs` — `WorkflowStateDTO` (D2/E1).
- `src/workflow.rs` — `mark_deployed` + grant-gated `request_unlock` (D2/E1).
- `tests/substrate_pin_activity.rs`, `tests/substrate_files.rs`, `tests/substrate_files_r10.rs`,
  `tests/workflow_lock_test.rs` — proving tests.
