# STATUS — Grant single-use-nonce replay fix (`fix/grant-replay-nonce`)

Closes the grant-security gap the gated full run caught: a **replayed grant** (same
QR/nonce presented a second time, from a different joiner process) was getting **SERVED**
data — `tests/substrate_multiuser_mp.rs::expired_revoked_replayed_grant_rejected` saw the
replay joiner receive **1 row** (`left:1 right:0`).

Branch: `fix/grant-replay-nonce`, cut off `chore/dead-code-sweep`. No `main`. iroh 0.95.
No FFI signatures, command/event JSON shapes, or protocol structs changed — purely additive
engine grant-enforcement. No `unwrap()`/`panic!` added. Test assertions untouched.

## Root cause — the leak was at the *second holder*, not the issuer

The host-side consume **was** already correct: `MeshAuthorizer::authorize_snapshot` →
`present_grant` → `GrantVerifier::verify_at` consumes the nonce **before** any frame is sent,
and a second presentation to the host is refused `ReplayedNonce`. The host did refuse the
replay (confirmed in logs: `decision=deny reason="verify:ReplayedNonce"`).

The real bug: in the replay test the **first joiner stays alive** (it is `quit()` only after
the replay assertions) and now **holds group G's data**. Both the host *and* the first joiner
sit on the same discovery key / gossip topic, so when the replay joiner broadcasts its snapshot
request it collects **two offers** and picks one — at random / by LAN preference:

```
📥 [SNAP-PICK] Picked holder <first-joiner>... of 2 offer(s)
✅ [SNAP] Snapshot SENT      ← first joiner served the replay
```

The first joiner **never called `enforce_group`**, so its snapshot serve path was
**fail-open** and happily re-served G's snapshot to *anyone*, bypassing the host's
consumed-nonce gate entirely. That is the `left:1 right:0` leak — and it was
**non-deterministic** because the replay joiner sometimes picked the host (refused) and
sometimes the first joiner (served).

Two distinct properties were missing:
1. **Single-use must hold at *every* holder**, not just the issuer — a peer that received a
   grant-gated snapshot must not become an open re-distribution point for a replayed QR.
2. The atomic consume-before-serve must not **permanently burn the grant for the one entitled
   holder** if its transfer stream drops and retries (the load-induced failure mode, below).

## The fix — consume-then-serve, enforced at every holder

Three small, additive changes:

**1. `GrantVerifier` (`src/identity/mod.rs`)** — expose the consumed-nonce set:
- `mark_consumed(nonce)` — record a nonce spent **without** re-verifying (a joiner trusts the
  holder that just served it).
- `is_consumed(nonce)` — the replay predicate (a nonce is spent once `verify_at` accepted it,
  or `mark_consumed` marked it).

**2. `MeshAuthorizer` (`src/identity/mesh.rs`)**
- `note_grant_used(grant)` — a joiner that **spends** a grant to pull a snapshot marks that
  nonce consumed in **its own** authority, so if it later serves the group it refuses a replay
  of the same QR.
- `authorize_snapshot` gains a **replay gate checked BEFORE the fail-open shortcut**, so even an
  un-enforcing holder refuses a spent grant:
  - If the SAME peer is already authorized via this exact nonce → **allow re-serve** (idempotent;
    a dropped/timed-out stream retried by the one entitled holder is *not* a replay).
  - Else if the nonce is already consumed → refuse `ReplayedNonce`.
  - `role_via_nonce(group, peer, nonce)` distinguishes "the entitled holder re-pulling" from
    "a different peer replaying a spent nonce".

**3. `NetworkActor` join path (`src/actors/network_actor.rs`)** — when a node is about to spend a
grant to pull `group_id` (entitlement gate passes with a grant for that group), it calls
`note_grant_used` **at join time**. Marking here (not at completion) is deterministic: it lands
before this node could ever serve a peer, so by the time any replay joiner exists, this holder
already refuses the spent nonce.

Net effect: the replay joiner is refused `ReplayedNonce` at **every** holder it can pick — the
host (consumed on its first serve) and the first joiner (`note_grant_used` at its own join) —
so the pick is irrelevant and the result is deterministic. EXPIRED and REVOKED refusals are
untouched (they fail earlier in `verify_at`, before the replay gate is relevant).

Why no SQLite persistence was needed: the per-node `MeshAuthorizer` is a single
`Arc<Mutex<…>>` created once in `NetworkActor::new` and shared by the snapshot-server handler
and every command — so the consumed-nonce set already **survives across requests** for the
life of the process, which is the property the serve gate needs. Cross-process, each node
consumes independently (issuer on serve, joiner on use), so no shared store is required.
Kept in-memory per the simplicity rule.

## The load-flake (caught running the FULL suite) and why the idempotent re-serve is needed

First isolated 3× run was green, but the **full `cargo test`** (heavy concurrent multi-process
load) flaked at a *different* line — 179, the **first** joiner's legitimate first use timing out,
not the replay. Cause: `present_grant` consumes the nonce *before* streaming; under CPU
contention the host's first serve stream can drop ("connection lost"), and the joiner's retry
(re-request on `NeighborUp`, same QR) was then refused `ReplayedNonce` — the one entitled holder
could never recover → 60s `wait_sync` timeout.

The idempotent re-serve (allow the SAME peer already authorized via this nonce to re-pull) fixes
this without weakening replay protection: a *different* peer reusing the nonce is still refused.
This is exactly "consume-then-serve atomic, reject if already consumed" with the correct
exception for the holder finishing its own transfer.

## Proof

**Target test, 3× back-to-back in isolation — all green:**
```
cargo test --test substrate_multiuser_mp expired_revoked_replayed_grant_rejected -- --nocapture
RUN 1: ok (1 passed)   RUN 2: ok (1 passed)   RUN 3: ok (1 passed)
```
Logs confirm the replay joiner is refused `verify:ReplayedNonce` at the holder it picks
(including the first joiner) and `✅ SNAP SENT` never goes to it.

**Full suite, 3× back-to-back:** `cargo test` — run 1 exit=0, run 2 exit=0, run 3 exit=0
(57 `test result: ok` lines, **0** `FAILED` per run; the target test went `ok` in all three
full runs — i.e. green 3× under the harshest concurrent multi-process load, stronger than the
isolated runs). The earlier line-179 load-flake is gone after the idempotent re-serve fix.

**Clippy:** `cargo clippy --all-targets -- -D warnings` — clean (only pre-existing upstream
`xaeroid` warnings remain; none from this change).

## Files changed
- `src/identity/mod.rs` — `GrantVerifier::{mark_consumed, is_consumed}` (additive).
- `src/identity/mesh.rs` — `MeshAuthorizer::{note_grant_used, role_via_nonce}` + replay gate in
  `authorize_snapshot` (idempotent re-serve, then refuse spent nonce; before the fail-open shortcut).
- `src/actors/network_actor.rs` — mark the grant nonce consumed at join time when a node spends it.

No xcframework rebuild.
