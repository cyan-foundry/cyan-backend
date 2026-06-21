# STATUS — W17 Backend: Org-key grant verify · revocation · device vault

Branch `feat/w17-backend` (off `feat/round10-distwf`; never main). The
**consumer** half of IDENTITY_W16_W17_SPEC §A/§B/§C — it builds on the shared
cyan-identity W17 crate (`feat/w17-identity`, see
`../cyan-identity/STATUS_W17_IDENTITY.md`) and matches its names verbatim.

Additive only — no `cyan_*` shape changed, no shipping path altered. No
`unwrap`/`panic!` in engine/FFI; secrets are `SecretString`, never logged; time is
passed explicitly (`*_at(.., now)`). `cargo build --tests`, the new + existing
suites, and `cargo clippy --all-targets -- -D warnings` are all green with no
network / Keychain / broker (the one pre-existing failure,
`diagram_gen::tests::test_parse_diagram_json`, also fails on the base branch and is
unrelated).

---

## §A — Verify grants against the per-tenant Org key (+ revocation)  `src/sso_grant.rs`

The SSO session-grant consumer gains an **org-key** path alongside the existing
legacy RSA path, both additive:

- **`SsoSession::from_org_token(token, &OrgGrantVerifier, xaero_pubkey, now)`** —
  verifies the grant against the tenant's pinned **org key** (issuer = the org
  DID), rebuilding the `grant ← delegate ← org root` chain offline. An
  `OrgGrantVerifier` built `.with_legacy(GrantVerifier)` keeps accepting legacy
  `"cyan-lens"`-issued grants during the cutover; a strict one refuses them.
- **`SsoSession::from_org_token_checked(.., &SignedRevocationList)`** — additionally
  rejects a grant whose `xaero_pubkey` is on the **org-signed revocation list**,
  even when the grant is otherwise valid and unexpired (the fired-employee case).
  The list is verified org-signed against the grant's pinned org key before it is
  trusted, so a forged list can neither suppress nor fabricate a revocation.

The original `from_cached_token(.., &GrantVerifier, ..)` is untouched, so every
existing caller/test keeps working. Both new methods reuse `SsoSession::new`, so
role enforcement still flows through the shared `RolePolicy`.

## §C — Consume group re-key  `src/group_rekey.rs`

**`GroupEpochStore`** — the device's receive-only view of each group's current
`GroupEpoch` (the model + transition live in cyan-identity; the ~7-day scheduler +
deprovision trigger live in lens).

- `apply(GroupEpoch) -> bool` ingests a rotation **monotonically**: accepted only
  if it strictly supersedes the epoch we hold for that group, so a stale epoch
  can't be replayed to slip a revoked member back in.
- `current` / `epoch_of` / `includes` query the applied epoch. Once the
  post-revocation epoch is applied, a revoked member is no longer `includes(..)` —
  they get no new epoch material and so cannot read post-rekey content.

## §B — Device identity vault + migration + delete FFI  `src/device_vault.rs`

- Re-exports the cyan-identity seam (`Vault`, `MemVault`) so the FFI/engine import
  from one site. Device key id: `DEVICE_KEY_ID = "cyan.device.xaero_id"`.
- **`KeychainVault`** (`#[cfg(target_os = "macos")]`) — the real OS secure store via
  the Security framework generic-password API (`security-framework` crate, macOS-
  only dep). `store` overwrites; `load` of an absent key is a clean `Ok(None)`;
  `delete` is idempotent — the exact contract `MemVault` mirrors.
- **`default_device_vault() -> Arc<dyn Vault>`** — Keychain on macOS, the in-memory
  fake elsewhere or when `CYAN_VAULT=mem` (headless/CI), so `cargo test` never
  prompts for Keychain access.
- **`migrate_file_key_into_vault(file, vault, key_id) -> Result<bool>`** — moves a
  legacy plaintext file-stored key into the vault **once**: the vault is
  authoritative, so if it already holds the key this is a no-op and a later/stale
  file never clobbers it; on first migration the plaintext file is removed.
- Helpers `store_device_key` / `load_device_key` / `delete_identity(vault)`.

### FFI (additive) — for iOS

- **`cyan_delete_identity() -> bool`** (`src/ffi/core.rs`) — the engine half of the
  iOS "delete identity" flow: wipes the device XaeroID key from the process vault
  (`crate::device_vault()`, lazily built from `default_device_vault`) and best-
  effort removes `node_id.txt` so a fresh identity mints next launch. Idempotent
  and panic-free; local data/DB untouched. Global `DEVICE_VAULT` + `device_vault()`
  added in `src/lib.rs`.

iOS names to wire: `cyan_delete_identity()` (delete-identity button). The real
Keychain vault behind it and the org broker are validated later with the app/Lens;
the seam + fakes are the deliverable here (no xcframework rebuild).

---

## Tests (deterministic seeds + explicit `now` + `MemVault`)

`tests/org_grant_test.rs` (§A/§C, 4) + `tests/device_vault_test.rs` (§B, 3):

- `grant_verifies_against_org_key` — org grant verifies against the pinned org key;
  another org's key and an unpinned tenant both reject (fail-closed).
- `legacy_issuer_accepted` — a legacy `"cyan-lens"` grant still verifies with
  `.with_legacy(..)`; a strict verifier refuses it.
- `revoked_pubkey_grant_rejected` — a revoked `xaero_pubkey` is rejected on an
  otherwise-valid, unexpired grant; a non-revoked device on the same list still
  verifies.
- `revoked_member_stops_getting_new_epoch` — re-keyed epoch excludes the revoked
  member; replaying the stale genesis epoch is ignored.
- `file_key_migrates_into_vault_once` — plaintext file migrates in, file removed;
  re-running is a no-op and a later stale file never overwrites the vaulted key.
- `vault_missing_key_clean_absent_not_panic` — absent key loads as `Ok(None)`;
  delete of an absent key is a clean no-op.
- `delete_identity_wipes_key` — stored key is gone after `delete_identity`; the
  second delete is idempotent.

Existing identity suites (`sso_grant_test`, `grant_test`, `substrate_identity`)
stay green.
