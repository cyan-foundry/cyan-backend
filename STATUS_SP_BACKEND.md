# STATUS_SP_BACKEND — super-peer / mesh-infra gaps (SUPER_PEER_COMPLETION_SPEC §1, §5, §6)

Branch `feat/sp-backend` (off `fix/r11-backend`). Production surgery on the stable engine/FFI:
additive only, no `unwrap`/`panic!` in engine/FFI, bounded `tokio::time::timeout`, iroh 0.95, test-first.
No `main` touched. No xcframework rebuild.

---

## §1 — `read_deltas` verb (incremental catch-up SERVE side)

**What it does.** The headless `cyan_node` line protocol had `addr`/`add_peer`/`join_group`/
`join_group_grant`/`wait_sync` but no way to READ a group's events since a cursor. Added the verb so the
Lens `EmbeddedReplica` (which drives a real `cyan_node` child) can SERVE incremental catch-up — the delta
since a peer's cursor — not just a full snapshot.

**Verb / shape (for Lens).**
```
read_deltas <group_id> <since_cursor>   →   @@CYAN@@ deltas <json>
```
`<json>` is single-line (no whitespace):
```json
{ "group_id": "<g>", "since": <i64>, "high_water": <i64>, "count": <u64>, "frames": [ <SnapshotFrame>... ] }
```
- `frames` — the engine's existing `SnapshotFrame` array (Structure → Content → Metadata → Complete),
  apply-able with the same idempotent `snapshot::apply_snapshot_frame` path the live download uses, so a
  delta SERVED here and a delta PULLED via the `CatchUp` command converge identically.
- `count` — DATA rows strictly newer than `since_cursor`, **excluding** the always-present group row in
  the Structure frame (so a caught-up reader sees `count = 0`).
- `high_water` — this holder's current max row version; the cursor a caller sends next to stay current.

**Engine support.** No new engine path was needed — the verb reuses the already-shipped
`snapshot::build_snapshot_frames(group, Some(since))` (the same since-bounded builder behind the live
`CatchUp` command) plus `snapshot::frames_row_count` / `snapshot::group_high_water`. The verb is a
read-only local storage read (no network), so it is the natural HOLDER counterpart to the existing
`catch_up` REQUESTER verb.

**Tests** (`tests/substrate_read_deltas_mp.rs`, real `cyan_node` process, per-process storage):
- `cyan_node_read_deltas_returns_events_since_cursor` — a `since = 0` read returns the whole fixture
  (14 data rows) and a non-zero high-water cursor.
- `read_deltas_empty_when_caught_up` — a read AT the high-water mark returns `count = 0`.
- `read_deltas_is_group_scoped` — a node holding two groups serves only the requested group's events;
  the served frames never mention the other group's ids (the §6 isolation invariant on the serve path).

All 3 green.

---

## §5 — Discoverable signed rendezvous config (kill the hardcoded bootstrap id)

**What replaces the hardcode.** `DEFAULT_BOOTSTRAP_NODE_ID = f992aa3b…` in `src/lib.rs` is gone as a
standalone hardcode. New module `src/rendezvous.rs`:

- At startup the engine **fetches an org-signed per-env rendezvous config** from `CYAN_RENDEZVOUS_URL`
  (when set), verifies the org Ed25519 signature against the pinned org key (`CYAN_ORG_PUBKEY`), and on
  success sets `BOOTSTRAP_NODE_ID` / `DISCOVERY_KEY` / `RELAY_URL` from it. Config shape:
  ```json
  { "env": "...", "discovery_key": "...", "bootstrap": { "node_id": "...", "addr": "...?" }, "relay_url": "...?" }
  ```
  signed as `{ "config": "<exact-json>", "signature": "<hex-ed25519>" }` (the literal signed string is
  carried so re-serialization can never break the signature).
- **Bundled fallback** (`rendezvous::BUNDLED_BOOTSTRAP_NODE_ID`, value = the old default) covers
  cold-start / offline / no-URL / bad-signature. So `bootstrap_node_id()` now returns the
  config-resolved id, else the one bundled fallback — no scattered hardcode.
- **mDNS / LAN-sovereign path needs none** — the fetch is skipped entirely when no URL is configured.

**Seam (shipping behavior identical when no URL).** `rendezvous::fetch_and_apply_if_configured()` is
called inside the FFI init thread (`src/ffi/core.rs`, both `cyan_init` and `cyan_init_with_identity`)
**before** the tokio runtime is built (so the bounded blocking fetch runs outside any async context) and
**after** the explicit FFI `relay_url`/`discovery_key` args are set. `OnceCell::set` is first-wins, so an
FFI-provided value still wins — the config only fills in what FFI didn't set (notably the bootstrap id).
When `CYAN_RENDEZVOUS_URL` is unset the network is never touched and the result is identical to pre-§5.

`reqwest` gained the `blocking` feature (additive Cargo change) for the bounded (5s) best-effort GET.

**Tests** (`src/rendezvous.rs` unit tests — the pure resolve/verify/fallback decision, no network):
- `config_sets_bootstrap_relay_from_signed_doc` — a verified doc drives bootstrap+relay+discovery_key.
- `bad_signature_rejected_falls_back` — a tampered doc, and a doc signed by a non-pinned key, both fall
  back to bundled (the tampered value never takes effect).
- `offline_uses_bundled_fallback` — no doc (offline) and a doc with no pinned key both resolve to bundled.

All 3 green.

> Not in scope here (cyan-lens / cyan-iac): the bootstrap node SELF-PUBLISHING its config to the
> well-known URL, and provisioning the config store. The backend side — fetch, verify, apply, fallback —
> is complete and is the half that removes the app/engine hardcode.

---

## §6 — Tenant/group ISOLATION (no bleed)

### Entitlement-gated join (NEW gate) — `join_non_granted_group_rejected`
The audit found the engine gated reads at the snapshot HOLDER (`authorize_snapshot`) but a JOINER could
subscribe to any group's gossip topic without an entitlement check. Added the JOINER-side gate:

- `identity::MeshAuthorizer::authorize_join(group_id, grant)` (+ `_at`) — a peer may join/subscribe ONLY
  a group it holds a valid grant for. Fail-open for un-enforced groups (the seam — un-enforced joins
  behave exactly as before). For an enforced group: a missing grant → `NoGrant`, a grant for a different
  group → `WrongGroup`, a bad/expired/revoked grant → `Verify(..)`. Uses a NEW non-consuming
  `GrantVerifier::check`/`check_at` so the single-use nonce is NOT burned here (the holder spends it at
  snapshot time).
- Wired into `NetworkActor`'s `JoinGroup` handler (`src/actors/network_actor.rs`): a refused join does
  NOT spawn the TopicActor — the node never subscribes to, nor enumerates, a non-granted group. A poisoned
  authorizer fails open (never deadlocks the join path; the holder serve-gate still applies).

Test (`tests/substrate_join_gate_mp.rs`, two real processes): the HOST does NOT enforce (it would serve),
so every refusal is provably the JOINER's own gate — no grant and a wrong-group grant are both refused
(no sync, zero trace of the group), while a valid grant for the joined group DOES sync the full fixture.
Green.

### Audit of the rest of the mesh state (files / board-state / notifications / presence / chat)
Verified correctly scoped (the R11 chat-bleed fix pattern holds for the rest):

| State | Keying | Verdict |
|-------|--------|---------|
| **chat** | board-scoped (`chat_insert` by `board_id`, R11 §1) | OK — fixed in R11, re-confirmed |
| **files** | `objects.group_id` + `board_id`; `file_list_by_group`/`file_list_by_board` filter on the key | OK (no cross-group/board bleed) |
| **board-state** (pins/metadata) | `board_id` PK, group via `board→workspace→group` chain (`board_get_group_id`), LWW merge | OK (group-scoped) |
| **notifications/unread** | `board_id` only — no workspace/group rollup (R11 §2/§3; killed the doubled counts) | OK (board-scoped) |
| **presence/roster** | `group_members (group_id, peer_id)` PK; `peers_per_group` keyed by group | OK (tenant=group scoped) |
| **gossip topic** | `blake3("cyan/group/" + group_id)` — one TopicActor per group | OK (strictly group-scoped) |

Regression-guard tests (`tests/substrate_isolation.rs`, storage-level oracle): `files_board_scoped`,
`board_state_group_scoped`, `presence_tenant_scoped` — all green.

### ⚠️ ENCRYPTION FINDING — **GAP** (group content is NOT group-key-encrypted)
**Group content is broadcast/served in PLAINTEXT** (serde_json), relying solely on iroh's transport-layer
QUIC/TLS — NOT on a per-group key. So a relay/bootstrap (or any other-tenant super-peer) is **not
cryptographically content-blind**; confidentiality depends on the transport session, not on a key only
entitled members hold.

- Gossip broadcast: `topic_actor::broadcast_event` (`src/actors/topic_actor.rs:1042-1056`) —
  `serde_json::to_vec(event)` → `sender.broadcast(Bytes::from(data))`. The offline-hold blob
  (`storage::hold_put`) and its blake3 are over the **plaintext** too.
- Snapshot serve: `network_actor.rs:1751` — `serde_json::to_vec(frame)` written straight to the QUIC
  stream. `snapshot::build_snapshot_frames` builds plaintext rows.
- `src/group_rekey.rs` (`GroupEpochStore`) exists and tracks per-group membership epochs, but is **not
  instantiated anywhere** and is **not used to encrypt** any gossip/snapshot payload.

**Verdict:** the §6 "relay + bootstrap are content-blind / content encrypted with the group key
(`GroupEpoch`)" property is **NOT met** — flagged here per the spec (do not silently pass). Closing it is
an E2E group-encryption workstream (wrap each gossip/snapshot payload in a `GroupEpoch`-keyed AEAD before
`broadcast`/stream write; relay/bootstrap stay key-less; a super-peer decrypts only its own tenant's
groups). It is a larger change than this §1/§5/§6 slice and was intentionally NOT bundled in — it touches
the broadcast/serve hot paths and the wire format, which deserves its own reviewed diff. Tracked as the
open hardening gap.

---

## Regression / gates
- `cargo build --tests` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean (only upstream `xaeroid` dep warnings, not promoted).
- New tests green: `substrate_read_deltas_mp` (3), `substrate_isolation` (3), `substrate_join_gate_mp` (1),
  `rendezvous` unit (3).
- Regression slice green: `grant_test`, `org_grant_test`, `qr_test`, `substrate_identity`,
  `substrate_snapshot_mp`, `substrate_catchup`, `catchup_export_test`, `substrate_discovery`.
- **Pre-existing failures (NOT introduced here — confirmed failing on `fix/r11-backend` base):**
  - `diagram_gen::tests::test_parse_diagram_json` (needs a graphviz `dot` binary).
  - `substrate_multiuser_mp::expired_revoked_replayed_grant_rejected` — timing flake in the multi-process
    REPLAY scenario (holder-side nonce consumption); fails at line 190/192 on base too. Unrelated to the
    joiner-side gate added here (the host enforces; the joiner does not).

## FFI surface
Additive only. No `cyan_*` signature changed. New: `read_deltas` line-protocol verb (test bin),
`rendezvous` module + `MeshAuthorizer::authorize_join` / `GrantVerifier::check` (engine internals),
`reqwest` `blocking` feature. The FFI init path is byte-for-byte unchanged when `CYAN_RENDEZVOUS_URL` is
unset.
