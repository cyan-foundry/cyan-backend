# Presence fix — honest peer count from the live gossip neighbor set

**Branch:** `fix/presence-gossip-neighbors` (off `feat/round9-grant`)
**Scope:** additive, receive-only event side. No FFI signatures changed. No xcframework rebuild.

## Symptom

The live test showed data syncing across peers — chat, boards, and files all
propagated — while the **peer count / Peers panel / status bar read 0**
("Local-only · no peers"). Presence was blind to a mesh that was demonstrably
carrying data.

## Root cause

The presence FFI — `cyan_get_group_peers`, `cyan_get_group_peer_count`,
`cyan_get_total_peer_count` (`src/ffi/core.rs`) — all read `sys.peers_per_group`.
That map was only ever populated by the **discovery peer-intro layer**
(`DiscoveryActor` `groups_exchange` → `NetworkActor::JoinPeersToTopic`), a
separate gossip topic from the per-group data topics.

The actual data rides each group's own gossip topic, owned by the `TopicActor`.
`TopicActor` already handled `GossipEvent::NeighborUp/NeighborDown` for that
topic — updating its local `known_peers` and emitting `PeerJoined` / `PeerLeft` /
`PeerCountChanged` — but it **never wrote the shared `peers_per_group` map** the
FFI reads. So whenever the discovery peer-intro didn't land (the live-network
case) while group-topic gossip flowed fine, presence stayed 0 despite a live mesh.

## Fix

Make presence reflect the **same gossip channel that carries the data**. In
`TopicActor` (`src/actors/topic_actor.rs`), which owns the per-group topic and
already knows its `group_id`:

- **`NeighborUp(peer)`** → insert `peer` into `peers_per_group[group_id]` (the
  set dedupes). The existing `SwiftEvent::PeerJoined { group_id, peer_id }` emit
  and `emit_presence()` (`PeerCountChanged` + `MeshReachability`) are unchanged.
- **`NeighborDown(peer)`** → remove `peer` from `peers_per_group[group_id]`, so
  the count falls back toward 0. The existing `SwiftEvent::PeerLeft` emit is
  unchanged.

Per-group attribution is exact: the update keys off the `TopicActor`'s own
`group_id`, so a node in N groups with the same neighbor reflects that neighbor
once **per group** (and `cyan_get_total_peer_count` sums to N).

Wiring: `TopicActor::spawn` takes the shared
`Arc<Mutex<HashMap<String, HashSet<PublicKey>>>>`; the sole caller
(`NetworkActor::spawn_topic_actor`) passes `self.peers_per_group.clone()`. The
lock is taken with `if let Ok(..)` (no `unwrap`/`panic!` on the engine path).
`known_peers` behavior is untouched, and the discovery-intro writes still happen —
the gossip-neighbor source is purely **additive**, and is now the reliable one.

Result: `cyan_get_group_peers` / `cyan_get_group_peer_count` /
`cyan_get_total_peer_count` now return live neighbors. The status-bar tier stays
correct ("Local-only" still holds for a relay-disabled node with zero neighbors),
but **"no peers" becomes "N peers"** the moment data is actually flowing — the
"honest status bar" the existing `emit_presence` comment intends. Presence now
matches data connectivity.

## Tests (test-first, substrate harness, bounded `timeout`, assert real state)

New tests in `tests/substrate_presence.rs` assert on the **`peers_per_group`
oracle the FFI reads** (via new additive harness accessors `group_peers` /
`total_peers`, alongside the existing `peers_in_group`), not on log lines.

To prove the **gossip** path specifically — not the discovery peer-intro path,
which *does* populate in-process over loopback and would mask the bug — the new
tests use `spawn_isolated_pair`: two nodes with **different discovery keys** (so
they never share a discovery topic, and the peer-intro path can never populate
`peers_per_group`), wired over loopback, forming the group topic via a direct
bootstrap-peer dial. After this, the only way a peer can land in `peers_per_group`
is the gossip NeighborUp wiring under test.

- `presence_reflects_gossip_neighbors` — both peers join one group → each reports
  `peer_count == 1` and the other node's id in `group_peers`.
- `presence_matches_data_connectivity` — after a chat broadcast the joiner
  actually receives, the joiner's `peer_count > 0` for that group.
- `total_peer_count_sums_groups` — the pair shares two groups → `total_peers == 2`
  (the neighbor counted once per group).
- `neighbor_down_decrements_presence` — `#[ignore]`d (NOT weakened): the engine
  removes the peer on every `NeighborDown`, so the assertion is correct by
  construction, but iroh's NeighborDown latency over loopback is not
  engine-bounded, so it can't be asserted under the substrate bounded-wait
  discipline. Same rationale as the pre-existing `presence_tracks_join_leave_for_n_peers`;
  the leave path is covered by the multi-process partition scaffold.

Verification:
- Pre-fix, `presence_reflects_gossip_neighbors` **fails** (probe delivered → data
  flows, but `peer_count` stuck at 0) — proving the bug and the test's honesty.
- Post-fix: `substrate_presence` = **4 passed, 2 ignored** (the 2 ignored carry
  documented unbounded-NeighborDown-latency reasons).
- `substrate_discovery`, `substrate_sync` green; no new clippy findings in any
  touched file.

## Files

- `src/actors/topic_actor.rs` — field + spawn arg + NeighborUp/Down → `peers_per_group`.
- `src/actors/network_actor.rs` — pass `peers_per_group.clone()` at the spawn site.
- `tests/substrate_presence.rs` — the four tests + isolated-pair harness helpers.
- `tests/support/mod.rs` — additive `group_peers` / `total_peers` accessors.
