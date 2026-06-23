# STATUS — Round 9 / W16 — cyan-backend (cache · verify · seed-groups · enforce-role)

Branch: `feat/round9-grant` (off `feat/round8-gating`).

## What shipped

The **device side** of the W15 front door — the backend consumer of the
cyan-identity SSO **session grant** (sibling to the W11 entitlement consumer in
`src/licensing.rs`). It caches, verifies offline, seeds groups, and enforces role,
all offline-first and graceful.

> NOTE — this is distinct from `src/identity::Grant`, the MESH-half Ed25519
> *capability* grant (group-write authorization). To avoid any collision the new
> module is **`src/sso_grant.rs`** and its tests are **`tests/sso_grant_test.rs`**;
> the existing `tests/grant_test.rs` (mesh capability grant) is untouched.

- **`src/sso_grant.rs`** — new module:
  - **`GrantCache`** — the offline-cacheable signed grant (`store`/`load`/`clear`/
    `has_grant`). Prod backing is the iOS keychain; this in-memory cell is the seam.
    The token stays a `SecretString`, never logged.
  - **`GrantVerifier` (cyan-identity)** verifies: RS256 signature + issuer + the
    **XaeroID-pubkey binding** + `exp + grace`. `SsoSession::from_cached_token(...)`
    is the offline path.
  - **`SsoSession`** — a verified grant + the shared `RolePolicy`. `enforce(action,
    resource)` builds an `Actor` from the bound XaeroID + tenant + grant role and
    delegates to `RolePolicy` (`same-tenant AND role.level() >= action.min_level()`)
    — the backend never re-implements RBAC. Tenant-scoped.
  - **`GroupJoiner`** seam — `join(tenant, group, xaero_pubkey)`. Prod
    **`NetworkGroupJoiner`** drives the EXISTING `NetworkCommand::JoinGroup` path
    (additive caller — no new FFI shape).
  - **`sign_in(token, verifier, xaero_pubkey, now, joiner)`** — verifies the cached
    grant offline; on success seeds the device into every granted group (idempotent)
    and returns `SignIn::Active(session)`; on any verification failure returns
    `SignIn::Reauth { reason }`. **Local data stays readable in both states**
    (`SignIn::local_read_allowed()` is always `true`) — the grant never gates local
    reads (X-CUT). A verification failure is the graceful re-auth path, not an `Err`.
- **`src/lib.rs`** — registers `pub mod sso_grant;`.

## Tests (all green — `cargo test`, fakes only, no PG/Iggy/vLLM/network)

`tests/sso_grant_test.rs` (uses the embedded cyan-identity RS256 test key, a
`RecordingJoiner` fake for the mesh join path):
- `signin_seeds_grant_groups` — sign-in seeds the device into every granted group,
  tenant-scoped; re-running is idempotent.
- `role_enforced_from_grant` — Member may run/read but not install-plugin or
  approve-delete; Admin lifts the install gate; a grant never authorizes another
  tenant's resource.
- `expired_grant_requires_reauth_keeps_local` — past `exp + grace` ⇒ `Reauth`, no
  groups seeded, **but local data stays readable**.
- `cached_grant_works_offline` — a cached grant signs in OFFLINE within the live
  window AND inside grace; a grant bound to a different XaeroID is rejected
  (no replay); `clear()` (sign-out) leaves local data untouched.

No regression: existing `tests/grant_test.rs` (7) + `tests/licensing_test.rs` (7)
stay green. W7/W11 paths untouched; the only network surface used is the existing
`JoinGroup` command (additive caller).

## Gate
`cargo build` green; `cargo test --test sso_grant_test` green; the new module +
test add **zero** clippy warnings (the backend's large pre-existing `clippy
-D warnings` baseline in `ffi/`, `skills/`, `pipeline.rs` is unrelated and left
untouched per the "production surgery / additive only" rule).

## Seam W15 (iOS) consumes
On SSO signup the broker hands the device a signed grant → the engine stores it via
`GrantCache`, then every startup calls `sign_in(...)` to verify offline, seed
groups, and enforce role. Past grace ⇒ re-auth, local data readable. Full
SSO-signup→seeded-groups E2E is **Tier-2** (needs Lens running) → the live test's
SSO rung.
