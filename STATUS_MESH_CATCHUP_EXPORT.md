# Mesh catch-up + portable export — MESH_HARDENING §5 + §11 + §4

**Branch:** `fix/mesh-catchup-export` (off `fix/mesh-seed-roster` tip `f01942d`)
**Scope:** additive. Two new engine modules (`snapshot`, `group_bundle`), two new storage tables,
one additive protocol field, one additive `NetworkCommand`, three additive FFI functions. No FFI
signatures changed/removed; no xcframework rebuild. iroh 0.95 only; no new `unwrap`/`panic!` on the
engine/FFI path; every test wait is a bounded `tokio::time::timeout`, asserting real state.

`MESH_HARDENING_SPEC.md` is not checked into the repo; this work was built from the §0b/§4/§5/§11
summaries in the run brief and the engine's actual snapshot/delta machinery (which I read end-to-end:
`network_actor::handle_snapshot_server`, `topic_actor::download_snapshot`, `anti_entropy`).

---

## §5 — incremental catch-up (pull only the missing range)

**The gap (§0b):** snapshot+delta existed P2P, but "delta" meant *live gossip after a FULL
snapshot* — a peer offline too long was forced to re-pull the WHOLE group. There was no way to pull
*only the range since T*.

**The fix — one shared builder, a `since` seam:**

- New `src/snapshot.rs` factors the snapshot build/apply (previously inlined in two actors) into one
  place and adds the `since`-bounded path:
  - `build_snapshot_frames(group_id, None)` → the full snapshot (unchanged fallback).
  - `build_snapshot_frames(group_id, Some(t))` → only rows whose version is **strictly newer than
    `t`** — the missing range. Same `SnapshotFrame` wire shape + order, so the apply path and any
    older holder/joiner are unaffected.
  - `group_high_water(group_id)` → the watermark a returning peer sends as its `since` (max version
    across the same columns the anti-entropy digest uses).
  - `apply_snapshot_frame` → the single idempotent upsert-by-id apply, now shared by the live
    download AND the §11 import (a delta merge, a full snapshot, and an air-gapped import all
    converge to the identical state).
  - `pick_catchup_holder(offers, lan_peers, super_peer)` → **closest-holder preference**: a direct
    LAN/mesh neighbor beats a remoter device holder; the configured super-peer is the last resort.

- `SnapshotRequest` gains an additive `since: Option<i64>` (`#[serde(default, skip_serializing_if)]`).
  `handle_snapshot_server` honors it via the shared builder and records whether it served an
  **incremental** vs **full** snapshot (+ row count) in `metrics`. Legacy/`None` ⇒ full, byte-compatible.

- `topic_actor::download_snapshot_since(.., since, ..)` carries the watermark through; the old
  `download_snapshot` forwards to it with `since=None`. The frame loop now applies via
  `snapshot::apply_snapshot_frame` (no duplicated insert logic) and still emits the per-node `Sync*`
  progress events.

- New additive `NetworkCommand::CatchUp { group_id, source_peer, since }` → routed to the group's
  `TopicActor::CatchUp` → a **quiet** since-bounded merge. When `since` is absent the engine falls
  back to the persisted "synced as of T" watermark (set by a §11 import), then the local high-water
  mark — so a returning peer asks for exactly what it lacks.

- `commit_snapshot_pick` now prefers a LAN-direct offerer (this group's live gossip neighbor set)
  over a remoter one, keeping the original random spread only when no LAN holder offered.

**Bidirectional heal** is the existing anti-entropy property (each side pulls a divergent peer's
state); the catch-up just makes those pulls incremental. The full-snapshot path remains the fallback
when no common base exists.

## §11 — portable, signed, invitee-encrypted Group Export bundle

New `src/group_bundle.rs`. A `.cyangroup` bundle = the existing `GroupSnapshot` (group / workspaces /
boards / file-**metadata**-by-hash / recent chats / board content), packaged for out-of-band hand-off
and air-gapped import. **Reuses the snapshot serialization — not a new sync engine.**

- **Signed.** The whole bundle is XaeroID/Ed25519-signed (`GroupBundle::sign`/`signing_payload`) by the
  exporter; import verifies it against `issued_by`. Tampering the ciphertext, scope, recipient, or
  watermark breaks the signature → rejected.
- **Strictly grant-scoped — never over-share.** The bundle embeds the invitee's signed capability
  `Grant`. Export refuses a grant for a different group (`ScopeMismatch`); import enforces
  `grant.group_id == bundle.group_id`, the grant's own signature, AND that every decrypted Structure
  frame carries only the scoped group (`ScopeLeak` defense-in-depth).
- **Encrypted to the invitee.** The snapshot payload is sealed with an **X25519 sealed box**
  (`crypto_box`, already in the tree via iroh — only its `seal` feature is enabled, no new crate).
  The invitee's X25519 keypair is derived deterministically from its Ed25519 identity
  (`blake3::derive_key`, NOT the signing scalar reused), so no extra key management. Wrong key →
  `Undecryptable`.
- **No media bytes, ever.** Files travel as `(name, hash, size)`; the test asserts the secret file
  bytes are absent from the serialized bundle and the payload is ciphertext, not plaintext.
- **Baseline + reconcile.** Import applies the snapshot (idempotent, fully offline) and stamps
  `storage::group_sync_state` ("synced as of T"), which §5 catch-up uses as its `since` on first
  online contact.

Additive FFI: `cyan_export_group(group_id, invitee_pubkey)` (Owner-only; returns the bundle JSON +
on-disk `.cyangroup` path), `cyan_import_group(bundle)` (verify + scope + decrypt + seed + stamp),
and `cyan_bundle_pubkey()` (this device's X25519 recipient key for an inviter to seal to). Secrets are
the node's identity bytes only, never logged or persisted in clear.

## §4 — offline-hold seam

`broadcast_event` now persists every outgoing group broadcast into a durable, **content-addressed**
hold store (`storage::mesh_hold`, keyed by `blake3(payload)`, idempotent on re-broadcast) **before**
putting it on the best-effort gossip wire. `hold_list_since(group_id, since)` replays exactly what an
offline peer missed. This is the clean seam the Lens super-peer (separate prompt) consumes to hold and
re-serve messages for offline peers; it is best-effort and behavior-neutral to the live path (a
hold-store error never blocks a send).

---

## Tests (test-first; bounded waits; honest oracles)

`tests/catchup_export_test.rs` — unit (no network; storage + crypto), DB isolated by unique group id:

| Test | Proves |
|------|--------|
| `reconnect_pulls_only_delta_not_full_snapshot` | `build_snapshot_frames(since=Some)` yields exactly the delta rows (3), strictly fewer than the full snapshot; old rows never leak into the delta |
| `closest_holder_preferred_for_catchup` | `pick_catchup_holder` picks the LAN neighbor over a sooner-sorting remote, falls back to a device holder, then the super-peer, then `None` |
| `export_bundle_is_signed_and_grant_scoped` | signed + scoped + encrypted; no media bytes / no plaintext in the bundle; foreign-group grant refused at export |
| `import_rejects_unsigned_or_out_of_scope_bundle` | forged sig / tampered ciphertext → `BadSignature`; validly-signed-but-foreign-grant → `OutOfScope`; wrong recipient → `Undecryptable` |
| `import_seeds_baseline_then_reconciles_on_reconnect` | wipe group → import re-seeds group+content AND stamps the `synced_as_of` watermark that drives §5 reconcile |
| `airgapped_import_works_with_no_network` | full export→JSON→import round-trip with no `NetworkActor`/endpoint/relay constructed |

`tests/substrate_catchup.rs` — over real loopback QUIC; oracle = the holder's process-global
served-snapshot metrics (reset per serialized scenario):

| Test | Proves |
|------|--------|
| `catchup_serves_incremental_over_the_wire` | a CatchUp serves an INCREMENTAL snapshot of exactly the 2 missing rows; `full_served == 0` |
| `partition_heals_bidirectional_converge` | both sides pull the other's delta — two incremental serves, zero full serves (converge from the watermark, not a re-snapshot) |
| `catchup_uses_import_watermark_when_since_absent` | CatchUp with `since=None` uses the persisted import watermark, serving only post-import rows — the §11→§5 reconcile seam is live |

**Result:** `catchup_export_test` 6/6, `substrate_catchup` 3/3.

### Honest scope notes (not faked)

- The engine's `storage` is a process-global singleton, so an in-process node pair shares ONE DB —
  "B was behind, then converged" is **not** honestly assertable via per-node DB content (the existing
  `substrate_sync::late_joiner_gets_full_snapshot` is `#[ignore]`d for exactly this reason). The
  catch-up substrate tests therefore assert on the **holder's served metrics** (incremental vs full,
  row count) — an honest record of what each holder put on the wire — and the delta/scope correctness
  is pinned precisely by the unit tests. A real netem partition / relay rung remains the Docker rig's
  job (per `tests/support` docs).
- `since` filtering is done in Rust over the existing `storage::*_list_by_*` reads (same `O(state)` the
  anti-entropy digest already costs) rather than adding a `since` SQL variant of every query — one
  filter, one place (`snapshot::newer_than`), simplest thing that works.

## No regression

- `cargo build --tests` + `cargo build --bins` clean (delta_test / snapshot_test / network_test build).
- `cargo clippy` on the new modules + new test files: **no findings** (lock is no-`unwrap`: new storage
  fns use `map_err`/`.ok()`; FFI uses `match CString::new`). Pre-existing warnings in touched files
  (e.g. `metrics::rss_kb` collapsible-if, `storage` `dm_*` unwraps) are untouched.
- Regression suites green: `substrate_sync` 4/0/1, `substrate_snapshot_mp` 1/0, `substrate_reliability`
  3/0, `substrate_mesh_seed` 9/0 (the prior §2/§3 work). `delta_sync_test` builds unchanged (it drives
  `NetworkActor`/`NetworkCommand`, which stayed additive).
- The one lib unit-test failure, `diagram_gen::tests::test_parse_diagram_json`, is **pre-existing**
  (fails identically on the clean `fix/mesh-catchup-export` tree with my work stashed) and in a module
  this change never touches.

## Cargo

`crypto_box = { version = "0.10.0-pre.0", features = ["seal"] }` — already in the tree transitively via
iroh 0.95; only the `seal` feature is newly enabled (Cargo.lock: +`blake2`, no version bumps to any
existing crate, no iroh change).

No xcframework rebuild. Stop.
