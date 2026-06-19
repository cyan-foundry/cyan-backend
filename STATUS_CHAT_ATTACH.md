# STATUS — file-via-chat / G7 (branch `feat/chat-attachment`)

**Result: ✅ done.** `chat_with_attachment_shares_file_into_scope` is implemented, un-ignored,
and green; the whole substrate suite stays green; the `cyan_*` FFI export set is unchanged
(additive only).

## The gap that was closed
`tests/substrate_chat.rs::chat_with_attachment_shares_file_into_scope` was `#[ignore]`d
because **no `NetworkCommand` carried an attachment** — `SendDirectChat` had only
`{peer_id, workspace_id, message, parent_id}`, and the wire `DmAttachment` (on `DirectMessage`)
was never constructed from a command. So a chat could not share a file into its scope.
(STATUS_OVERNIGHT finding #2.)

## Command shape added (optional / additive)
`NetworkCommand::SendDirectChat` gained **one optional field**:

```rust
SendDirectChat {
    peer_id: String,
    workspace_id: String,
    message: String,
    parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attachment: Option<DmAttachment>,   // ← NEW, optional, additive
}
```

- `DmAttachment { file_id, name, hash, size }` is the existing wire type (re-exported from
  `cyan_backend::actors`); nothing about it changed.
- `#[serde(default, skip_serializing_if = "Option::is_none")]` makes the field **drop-in**:
  absent in the JSON ⇒ deserializes to `None` ⇒ today's behavior exactly; when `None` it is
  not serialized, so the wire is byte-identical for callers that don't set it.
- No existing field was removed, renamed, or reordered.

### Send path
`network_actor.rs` `SendDirectChat` handler now moves the optional `attachment` into the wire
`DirectMessage` (it was hard-coded `None` before).

### Receive path — "shares the file into scope"
A new helper `fetch_attachment_into_scope(dm, peer_id, self_cmd_tx)` runs on **both** DM
receive paths (`handle_dm_stream` acceptor + `handle_dm_stream_with_streams` outbound). When a
received message carries an attachment it:
1. registers the file in the message's scope locally if unknown (best-effort
   `file_insert_simple`, group resolved from the DM's `workspace_id`), so the file belongs to
   the workspace/group the chat was posted to; and
2. enqueues a `RequestFileDownload` from the sending peer via a new internal **self-command
   channel** (`self_cmd_tx`/`self_cmd_rx`, drained in `run_loop` next to the FFI command
   channel) — the file is then fetched + blake3-verified by the existing transfer path,
   emitting `FileDownloaded`.

The self-command channel is an internal seam only; it does not touch the FFI command path
(`start(cmd_rx)` is unchanged). No `unwrap()`/`panic!` was added on engine/FFI paths.

## The now-green G7 test
`chat_with_attachment_shares_file_into_scope` (no longer `#[ignore]`): host stages a ~32 KB
file at workspace scope, sends a DM carrying its `DmAttachment` to the peer; the peer must end
with **both** the message (`SwiftEvent::DirectMessageReceived { is_incoming: true }`) **and**
the file fetched into scope (`SwiftEvent::FileDownloaded`, bytes blake3-verified against the
source). The assertion was authored to honestly require both halves; it is not weakened.

```
test chat_with_attachment_shares_file_into_scope ... ok
test result: ok. 4 passed; 0 failed; 0 ignored   (substrate_chat)
```

## Suite stays green (`--no-fail-fast`, in-process scope)
| binary | result |
| --- | --- |
| substrate_chat | 4 passed, 0 ignored |
| substrate_discovery | 2 passed |
| substrate_files | 5 passed, 1 ignored (1 GB, on-demand) |
| substrate_sync | 4 passed, 1 ignored |
| substrate_offline | 3 passed |
| substrate_reliability | 3 passed |
| substrate_resilience | 4 passed, 1 ignored |
| substrate_snapshot_mp | 1 passed |
| substrate_lens | 1 ignored (HTTP leg, out of in-process scope) |
| substrate_relay | 6 ignored (relay/WS rungs — docker rig) |
| substrate_swarm | 4 ignored |

No previously-green test went red. The still-`#[ignore]`d tests are the same ones ignored
before this change (relay ladder, swarm, lens HTTP leg, 1 GB transfer).

## FFI additivity check (the load-bearing contract)
`cyan_*` exported-symbol set vs `feat/substrate-e2e`:

```
symbols only in current (additions): (none)
symbols only in baseline (removals): (none)
counts: 107 == 107
```

The compiled `libcyan_backend.dylib` also exports exactly 107 `cyan_*` symbols. The C ABI of
`cyan_send_direct_chat` is unchanged; the additive field lives only in the internal
command/JSON shape.

`cargo build` ✅. Clippy: the repo does not pass `-D warnings` at baseline (688 pre-existing
errors in out-of-scope code — integration/lens/skills); this change adds **zero** new
warnings (688 == 688 with and without the diff).

## For the maintainer (cross-repo, later)
cyan-iOS will add the matching **optional** `attachment` field when it consumes this. Until it
does, iOS that never sends the field behaves exactly as before (absent ⇒ `None` ⇒ original
chat-send). No xcframework was built and `~/cyan-iOS` was not touched.
