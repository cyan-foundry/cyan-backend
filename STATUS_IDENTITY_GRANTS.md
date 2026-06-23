# STATUS — Identity / RBAC, the MESH HALF (signed capability grants)

Branch: `feat/identity-grants` (off `feat/file-swarm-consumer`). Contract:
`IDENTITY_RBAC_SPEC.md` (mesh half, build-order step 2) + `WAVE2_DESIGN.md` §10. The cloud half
(SSO + `authorize()`) is already done in cyan-lens (`feat/lens-identity`) and was **not** touched
here.

## What this delivers

A **XaeroID-signed, short-lived, revocable capability grant** — the offline, integrity-only proof
of *who may write/administer a group* — plus QR encode/decode and mesh-write enforcement on the
receiver. This replaces the old bearer-blob QR (no expiry/signature) with a signed, expiring,
anti-replay, revocable grant.

## Where the `Grant` type lives + why

**`src/identity/` in cyan-backend** (`mod.rs` = grant/verifier/QR, `mesh.rs` = enforcement). Per the
prompt ("prefer homing the Grant type in cyan-backend for now"):

- A *capability grant* is a cyan-backend domain concept — it references cyan's group hierarchy and
  the fixed RBAC role vocabulary, and it is enforced by cyan's mesh actors. xaeroID owns only the
  raw device identity + Ed25519 primitives.
- The module **borrows** xaeroID's existing primitives for sign/verify — `XaeroID::ed25519_sign`,
  `XaeroID::ed25519_pubkey`, `XaeroID::verify`, `XaeroID::now_secs`. **No change to the xaeroID
  crate was needed** (no additive helper added there).

## The model

- `Role` = the fixed vocabulary `Owner · Admin · Member · Viewer · Guest` (matches the lens cloud
  half). Computed permissions: `can_administer()` = Owner/Admin; `can_write()` = Owner/Admin/Member
  (Viewer/Guest read-only). No policy DSL, no per-object ACLs.
- `Grant { version, group_id, role, issued_by (issuer Ed25519 pubkey hex), issued_at, expiry,
  nonce } + signature` (Ed25519 over a deterministic payload covering every trusted field).
- `GroupRoster` = `group_id → (pubkey_hex → Role)` — the admin-authority oracle (who may issue).

### Issue — `Grant::issue(group_id, role, issuer_secret, issued_at, expiry, nonce, roster)`
Only an **Admin/Owner** of the group (per `roster`) may sign; a non-admin issuer gets
`Err(GrantError::NotAuthorized)`. `issue_unchecked` exists for negative tests (forge an issuer
field → signature breaks).

### Verify — `GrantVerifier::verify_at(grant, now)` (or `verify` against the wall clock)
Checks in order: **signature valid · issuer is a current admin · not expired (`now >= expiry`) ·
not revoked · nonce unseen (anti-replay)**. The nonce is consumed only on full success, so a replay
is rejected. Returns the granted `Role`.

### Revoke — `GrantVerifier::revoke(group_id, nonce)`
A `(group_id, nonce)` tombstone; idempotent. After it, that grant fails with `VerifyError::Revoked`.
`MeshAuthorizer::revoke` additionally drops any peer it had already authorized via that nonce.
(Production gossips this tombstone like any group state — see the follow-up note.)

### QR — `Grant::to_qr_payload()` / `Grant::from_qr_payload()`
Compact JSON encode→decode roundtrip; the decoded grant still verifies. Panic-free (no `unwrap` in
the FFI-reachable path).

## Mesh-write enforcement (the receiver-side seam)

`MeshAuthorizer` (`src/identity/mesh.rs`) is one node's authority state: which groups it enforces,
and which peers have presented a valid grant (and at what role). It wraps a `GrantVerifier` so
presentation reuses the same checks as QR scanning.

- Mounted on `NetworkActor` (a per-node `Arc<Mutex<MeshAuthorizer>>`, exposed via `authorizer()`
  exactly like `swarm()`), and shared with every `TopicActor`.
- The inbound-write path (`TopicActor::handle_network_event`) now gates persist+forward on
  `authorize_write(group_id, from_peer)`. A refused write is **dropped before persist/forward**, and
  a flat obs line (`target:"obs", tenant=group_id, peer, action="mesh_write", decision="deny"`) is
  emitted at the refusal point (obs only — assertions use state, never log lines).
- **Fail-open until enforced.** A group is enforced only after `enforce_group`; until then every
  write is allowed. So this is **inert for groups that have not opted into grant enforcement** — no
  shipping behavior changes (the "seam, not a rewrite" rule). `NetworkActor::new` and the FFI init
  path are unchanged.

## Named tests — all green (RED-first)

`tests/grant_test.rs` (pure logic, in-memory keypairs, deterministic clock):
- `admin_issues_grant_member_verifies`
- `non_admin_cannot_issue_grant`
- `expired_grant_rejected`
- `replayed_nonce_rejected`
- `revoked_grant_rejected`
- `qr_payload_roundtrips_and_verifies`
- (+ `forged_signature_rejected` — extra defense-in-depth, not required by the spec)

`tests/substrate_identity.rs` (two real loopback nodes, asserts on the **receiver's** own
authorizer state + its event channel — unaffected by the shared SQLite DB):
- `mesh_write_rejected_without_valid_grant` — receiver enforces the group; an un-granted peer's
  write is denied (`NoGrant`) and never surfaces on the receiver's event channel.
- `mesh_write_allowed_with_valid_grant` — positive companion: once the receiver verifies that
  peer's signed Member grant, the same write is accepted and surfaces. Strengthens the seam (guards
  against a deny-all bug); not in the named list.

Full substrate suite re-run after the actor wiring: **green** (chat 4, discovery 2, files 5,
identity 2, offline 3, reliability 3, resilience 5, snapshot_mp 1, swarm 5, sync 4; pre-existing
`#[ignore]`s unchanged — relay/lens rungs). Zero new clippy warnings from the new files.

## FFI / additive surface

- **No new client `cyan_*` C FFI was added**, and **no `NetworkCommand`/`SwiftEvent` variant was
  added or changed.** The substrate test drives enforcement through the exposed `authorizer()`
  test-support seam, so the load-bearing FFI/wire surface is untouched. The xcframework was **not**
  rebuilt.
- **iOS-facing follow-up (additive, not built here):** the Rust API for QR issue/scan is ready
  (`Grant::to_qr_payload`/`from_qr_payload`, `MeshAuthorizer::present_grant`). Wiring it to the app
  needs (a) one additive `cyan_*` verb to issue/scan-and-verify a grant from Swift, and (b) a
  gossiped grant-presentation path (a peer broadcasts its grant over the group topic; receivers run
  the same `present_grant` verification) so the production positive path matches the test's
  shortcut, plus a gossiped revocation tombstone. Both are additive and belong to the binding/iOS
  step (build-order steps 3–4), after the xcframework relink.

## Decisions logged

1. `Grant` homed in cyan-backend, not xaeroID (domain concept; xaeroID change avoided).
2. Enforcement is **fail-open by default** so it is a pure additive seam over shipping gossip.
3. Mesh enforcement keyed on the transport identity (`from_peer` = iroh node id) recorded at
   grant-presentation time — the binding of XaeroID grant ↔ iroh node happens when a peer presents
   its grant.
4. Anti-replay + revocation state live in the per-node `GrantVerifier`/`MeshAuthorizer` (in-memory,
   one per node). Persisting/gossiping the revocation tombstone is the production follow-up above.
