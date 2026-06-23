# STATUS — Round 6: live multi-user (presence · grant-gated per-group snapshot · status events)

Branch: `feat/multiuser` (off `feat/dev-stack`). Builds on the identity/RBAC **mesh half**
(`feat/identity-grants`, see `STATUS_IDENTITY_GRANTS.md`): the XaeroID-signed, expiring, revocable
capability grant + verifier + QR + receive-side mesh-write enforcement already existed. This round
makes the **join** itself grant-gated, lights up **live presence/status**, and adds a **hard
multi-process test matrix** for the silo. Everything is additive; the substrate suite stays green.

> Note: the prompt referenced `MULTIUSER_HARDENING_SPEC.md`, `IDENTITY_RBAC_SPEC.md`,
> `SSO_REAL_SPEC.md` under `../anthropic_data_dump/` — those files are not present in this checkout.
> Work was driven from `SUBSTRATE_TEST_SPEC.md`, `STATUS_IDENTITY_GRANTS.md`, the named test matrix
> in the prompt, and the actual engine.

---

## What shipped (green)

### 1. Grant-gated per-group snapshot join — THE KEY PROPERTY
A joining peer presenting a valid grant for group **G** pulls **only G's** state — **zero leakage of
the holder's other groups**. This is the union of two facts, both enforced server-side:

- **Per-group build** (pre-existing): `handle_snapshot_server` only ever queries the single requested
  `group_id` (`workspace_list_by_group` → boards → elements/cells/chats/files for G alone).
- **Join-time read gate** (new): the snapshot request now carries the scanned grant, and the holder
  refuses to serve an **enforced** group unless the joiner presents a grant FOR THAT group that
  verifies (signature · issuer-is-admin · not expired · not revoked · nonce unseen).

**Wire (additive, backward-compatible).** The joiner's opening message on `SNAPSHOT_ALPN` is now a
length-prefixed JSON `SnapshotRequest { group_id, grant: Option<String> }` (`models/protocol.rs`).
The server first parses it as JSON; if that fails it falls back to treating the bytes as a bare
legacy `group_id` (grant `None`). So pre-existing/un-enforced snapshot flows are unchanged
(`substrate_snapshot_mp::late_joiner_gets_full_snapshot` still green).

**Gate logic.** `MeshAuthorizer::authorize_snapshot(peer, group, grant)` (`identity/mesh.rs`):
fail-open if the group isn't enforced; else require a grant whose `group_id` matches and that
`present_grant`-verifies. On success the **nonce is consumed** (so a replayed QR is refused) and the
peer is recorded at its granted role (so its later mesh writes pass `authorize_write` without
re-presenting). On refusal the holder finishes the stream with **no frames** → the joiner reads it as
a refused snapshot and its storage stays empty for G. Refusals emit an obs-only line
(`target=obs tenant=<group> peer=… action=snapshot decision=deny reason=…`); assertions are on
storage, never logs.

**Grant threading.** `NetworkCommand::JoinGroup` gains an additive `grant: Option<String>`
(`#[serde(default)]`), threaded JoinGroup → `TopicActor` (stored per-group) → `download_snapshot`,
which sends it in the `SnapshotRequest`. The iOS invite FFI (`xaero_join_group_from_invite`)
additively reads an optional `"grant"` field from the invite JSON.

### 2. Live presence + honest status-bar events (additive, receive-only)
The group roster reflects **real connected mesh peers**, driven off each node's `TopicActor` peer set:

- `SwiftEvent::PeerCountChanged { group_id, count }` — live connected-peer count, emitted on every
  gossip NeighborUp/NeighborDown.
- `SwiftEvent::MeshReachability { group_id, state }` — `"online"` (≥1 peer) vs `"local_only"`
  (0 peers; working against just this device's copy). This is the "0 peers → local-only" vs
  "≥1 peer → synced" distinction the status bar needs.

Both route to the existing `network_status` FFI buffer. The pre-existing
`SyncStarted/SyncStructureReceived/SyncBoardReady/SyncFilesReceived/SyncComplete` and
`PeerJoined/PeerLeft/FileDownloadProgress` already cover sync lifecycle + transfer progress.
**Not added: `ConnectionTier` (direct|relayed|websocket)** — see Gaps; it needs an iroh conn-type
watcher and is deliberately not a never-emitted enum case.

### 3. Multi-process test rig extensions
`cyan_node` (the per-process peer binary) gained identity verbs — `admin_pubkey`, `enforce_group`
(enforce + register self as Owner-admin), `set_admin`, `issue_grant <gid> <role> [ttl]` (ttl may be
negative ⇒ already-expired), `revoke_grant`, `join_group_grant <gid> <boot|-> <qr>` — each driven
over the same `@@CYAN@@` stdin/stdout protocol, with matching `MpNode` methods. The node's
`MeshAuthorizer` (grabbed as a seam before the actor moves) is the honest per-process oracle; its
grant keypair is seeded from the node secret. The `wait_sync` timeout response changed from
`err wait_sync timeout` to a non-error `timeout wait_sync` so the rig can assert `Ok(false)` on a
refused snapshot (existing callers only ever asserted success, so this is drop-in).

---

## Multi-process / substrate matrix — GREEN vs honestly-IGNORED

| Spec test | Status | Where |
|---|---|---|
| `peer_joins_with_grant_snapshots_only_that_group` | ✅ green | `substrate_multiuser_mp` |
| `peer_without_grant_rejected` | ✅ green | `substrate_multiuser_mp` |
| `expired_revoked_replayed_grant_rejected` | ✅ green (all 3 cases, 5 procs) | `substrate_multiuser_mp` |
| `late_joiner_full_snapshot` | ✅ green (pre-existing) | `substrate_snapshot_mp` |
| `concurrent_edits_converge_no_lost_update` | ✅ green (pre-existing delta) | `substrate_sync` |
| `presence_tracks_join_leave_for_n_peers` (join) | ✅ green | `substrate_presence::presence_roster_reflects_connected_peers` |
| `n_user_chat_…`, `file_transfer_…`, swarm `file_fetched_from_n_holders_resume` | ✅ green (pre-existing) | `substrate_chat` / `substrate_files` / `substrate_swarm` |
| `peer_drops_midsync…`, `partition_then_heal…`, `unauthorized_or_malformed_peer_rejected_mesh_unharmed` | ✅ green (pre-existing) | `substrate_resilience` |
| `group_join_via_qr_works_offline` | ✅ green | `substrate_offline_multiuser_mp` |
| `mesh_rbac_enforced_offline_via_signed_grant` / `unauthorized_peer_rejected_offline` | ✅ green | `substrate_offline_multiuser_mp` |
| `presence_tracks_join_leave` (LEAVE) | ⏸ ignored | `substrate_presence` — NeighborDown latency over loopback not engine-bounded (engine DOES emit roster-shrink on NeighborDown; can't be asserted with a bounded wait) |
| `snapshot_served_multi_source_no_single_peer_overload` | ⏸ ignored | snapshot is single-source; swarm multi-source exists for files only |
| `lens_replica_serves_snapshot_only_when_all_devices_offline` | ⏸ ignored | Lens client is HTTP-only, no snapshot replica (cross-repo) |
| `concurrent_joiners_all_converge` | ⏸ ignored | engine supports it; concurrent multi-process joiner fan-out unwritten |
| `xaeroid_sso_binding_resolves_same_identity` | ⏸ ignored | no XaeroID↔SSO binding store (cross-repo: cyan-identity broker) |
| `xaeroid_login_when_sso_unavailable` (cached session) | ⏸ ignored | depends on the binding store; pure-P2P XaeroID offline auth already works |
| `revocation_made_online_propagates_on_reconnect` | ⏸ ignored | revocation is in-memory per node; gossiped tombstone is the follow-up |
| ENTERPRISE SCALE (`many_*`, `sustained_churn_*`) | ⏸ ignored | parameterized-N rig + `CYAN_SCALE` gate not built; primitives green at N=1..3 |

The ignored cases live as **red scaffolds** in `substrate_multiuser_scaffolds.rs` (each `#[ignore]`d
with the reason above; bodies `unimplemented!()` so `--ignored` fails loudly, never a fake pass) and
`substrate_presence::presence_tracks_join_leave_for_n_peers`.

**Full suite re-run after this round (in-process + the new mp tests): green.**
discovery 2 · chat 4 · sync 4 (+1 ign) · files 5 (+1 ign) · offline 3 · swarm 5 · reliability 3 ·
resilience 5 · identity 2 · snapshot_mp 1 · grant 7 · **multiuser_mp 3 · offline_multiuser_mp 2 ·
presence 1 (+1 ign)**. relay 8 / lens 1 ignored (need the Docker rig). New scaffolds: 7 ignored.

---

## FFI / additive surface (load-bearing contract respected)

- **`NetworkCommand::JoinGroup`** gains `grant: Option<String>` (`#[serde(default)]`) — additive;
  existing callers (lib.rs, ffi, tests) updated to `grant: None`.
- **`SwiftEvent`** gains `PeerCountChanged`, `MeshReachability` — additive, receive-only, routed to
  the existing `network_status` buffer.
- **`SnapshotRequest`** is a new mesh-internal wire type (NOT FFI); the `SNAPSHOT_ALPN` handshake is
  backward-compatible (legacy bare-`group_id` still accepted).
- No `cyan_*` C function was renamed/removed; no event/command variant was repurposed. The
  xcframework was **not** rebuilt (additive only).

## Decisions

1. **Grant-gating is fail-open until `enforce_group`** — same philosophy as the mesh-write half, so
   it's a pure additive seam (un-enforced groups serve snapshots exactly as before).
2. **The grant nonce is consumed at snapshot time** — presenting the grant to pull the snapshot is a
   real presentation, so replay protection falls out for free (a second pull with the same QR is
   refused). A holder records the joiner's role at that moment, binding XaeroID-grant ↔ iroh node id.
3. **Refusal = empty stream**, not an error frame — keeps the wire trivially backward-compatible and
   lets the joiner detect refusal as "no first frame".
4. **Presence is emitted off the topic's own peer set** (`known_peers`), the honest per-node oracle —
   no separate presence protocol; reachability is derived (0 peers ⇒ `local_only`).
5. **ConnectionTier not added** to avoid a never-emitted enum case; deferred until the iroh conn-type
   watcher is wired (documented follow-up, not vaporware in the FFI).

## Follow-ups (the ignored scaffolds, in build order)

1. Multi-source snapshot (discover holders → parallel/deduped pull) + Lens replica fallback (Lens
   serves only when all device holders are offline) — needs a Lens snapshot endpoint (cross-repo).
2. XaeroID↔SSO binding store (signed `{xaeroid, sso_user, provider, proof}` + bind verb) — coordinate
   with cyan-identity/lens; then the SSO cached-session offline path.
3. Gossiped revocation tombstone (revocation propagates on reconnect).
4. `ConnectionTier` emission via iroh conn-type watcher.
5. Parameterized-N enterprise-scale rig behind `CYAN_SCALE`.

Do not merge to `main`; do not rebuild the xcframework (additive surface only).
