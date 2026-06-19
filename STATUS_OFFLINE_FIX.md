# STATUS — Offline cold-start must not block (feat/offline-startup-fix)

**Result: FIXED and green.** The offline cold-start finding is resolved with a
2-line, behavior-preserving engine change; the executable spec is now un-ignored
and passing, the full substrate gate suite stayed green, and the `cyan_*` FFI
export surface is byte-for-byte unchanged.

## The bug

A node with a group persisted in its DB, cold-starting fully offline (relay
disabled, no reachable bootstrap), blocked its command loop at startup. `start()`:

1. `DiscoveryActor::spawn(..).await` → `gossip.subscribe_and_join(discovery_topic, bootstrap)`
2. then loads persisted groups → `TopicActor::spawn(..)` → `gossip.subscribe_and_join(group_topic, peers)`

`subscribe_and_join` = `subscribe` **+ `joined().await`**, and `joined()` parks
until ≥1 neighbour connects. Empirically (verified with `--nocapture`) a lone
offline node blocks **first** at the *discovery* subscribe: with `DiscoveryPolicy::MdnsOnly`
the bootstrap set is empty, so `Command::Join([])` adds no peer, no `NeighborUp`
ever fires, and `joined()` never returns — `start()` never reaches its `run_loop`.
The *group* topic has the same defect (it always appends the relay-only default
`bootstrap_node_id()`, unreachable offline). Either one alone wedges the command
loop, so the node is dead until something connects.

## The fix

Replace the two blocking `subscribe_and_join(..)` calls on the startup path with
the non-blocking `subscribe(..)` (iroh-gossip 0.95 — "Returns a `GossipTopic`
instantly … messages queued until a first connection is available"). The join now
completes **in the background**: each actor's existing `run` loop already surfaces
`NeighborUp` through `handle_gossip_event` (TopicActor emits `PeerJoined` + re-sends
the snapshot request; DiscoveryActor re-broadcasts `groups_exchange`). Dropping the
`joined().await` only removes the *blocking wait* — the first `NeighborUp`, which
`joined()` used to consume, now flows to the run loop exactly like every subsequent
one. Online mesh formation is unchanged (peers still connect and are discovered);
startup is simply no longer gated on an unreachable/absent bootstrap.

### Exact change (2 lines of behavior, + comments)

- `src/actors/topic_actor.rs` — `gossip.subscribe_and_join(topic_id, peers)` → `gossip.subscribe(topic_id, peers)`
- `src/actors/discovery_actor.rs` — `gossip.subscribe_and_join(topic_id, bootstrap_peers)` → `gossip.subscribe(topic_id, bootstrap_peers)`

No FFI files touched. No `unwrap()`/`panic!` added. No new clippy warnings from the
changed lines (the lib-wide pre-existing `disallowed_methods` lints on `json!` are
unrelated to this change).

## The now-green offline test

`tests/substrate_resilience.rs::node_with_group_offline_startup_does_not_block`
un-ignored (the `#[ignore = "engine: offline startup blocks …"]` removed) and its
stale "ENCODED FINDING / Do NOT edit the engine" doc comment updated to a
"REGRESSION GUARD (now green)" note. **The test body and its assertions are
unchanged** — it still seeds a real `groups` row, cold-starts a fresh offline
node, fires a new `JoinGroup`, and requires `has_group(probe)` within `SYNC_TIMEOUT`.

```
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out
```

## Regression net — full substrate gate stayed green

`cargo test --no-fail-fast --test substrate_discovery --test substrate_sync
--test substrate_chat --test substrate_files --test substrate_offline
--test substrate_resilience --test substrate_snapshot_mp`:

| binary               | result                              |
|----------------------|-------------------------------------|
| substrate_discovery  | ok. 2 passed; 0 failed              |
| substrate_sync       | ok. 4 passed; 0 failed; 1 ignored   |
| substrate_chat       | ok. 3 passed; 0 failed; 1 ignored   |
| substrate_files      | ok. 5 passed; 0 failed; 1 ignored   |
| substrate_offline    | ok. 3 passed; 0 failed              |
| substrate_resilience | ok. **5 passed**; 0 failed; **0 ignored** |
| substrate_snapshot_mp| ok. 1 passed; 0 failed              |

`substrate_resilience` went from 4 passed + 1 ignored → **5 passed + 0 ignored**
(the newly un-ignored offline test). No previously-green test went red; the
remaining `#[ignore]`s (snapshot/file per-node-storage findings) are unchanged.

## FFI surface unchanged

`nm -gU` of `libcyan_backend.dylib`, `cyan_*` exports only, this branch vs
`feat/substrate-e2e` (built in its own worktree):

```
diff /tmp/cyan_exports_e2e.txt /tmp/cyan_exports_now.txt   → empty
```

107 exports on each side, **diff empty** — the FFI contract is byte-for-byte
identical.

## Gates

- `cargo build` ✅
- Target test passes, un-ignored, assertions intact ✅
- Full substrate gate suite green ✅
- FFI `cyan_*` exports diff empty vs `feat/substrate-e2e` ✅
- No `unwrap()`/`panic!`, bounded waits only, small reviewable diff (2 actor files
  + 1 test file) ✅
