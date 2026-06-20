# STATUS — ROUND 8 / W11 — Licensing gate (cyan-backend half)

Branch: `feat/round8-gating` (off `feat/round8-templates`).
Scope: gate the engine behind the license — **offline-first, graceful, LAN never
gated**. The backend is the consumer of the cyan-identity entitlement model; this
half adds the startup check, the offline cached-entitlement path, and the
per-action cloud gates. Test-first; no `unwrap()`/`panic!` on engine/FFI paths;
secrets stay `SecretString` and are never logged.

## Identity branch reconcile (READ FIRST)

This depends on the cyan-identity entitlement crate on **`feat/round8-license`**
(off `feat/round8-onboarding`). `Cargo.toml` points at the sibling checkout by
**path**:

```toml
cyan-identity = { path = "../cyan-identity" }            # [dependencies]
cyan-identity = { path = "../cyan-identity", features = ["testing"] }  # [dev-dependencies]
```

The local `../cyan-identity` checkout must be on `feat/round8-license` for this to
build (it currently is). **Reconcile note for the human:** when the identity
branch merges, no code change is needed here — only confirm the merged tip still
exports `Entitlement` / `EntitlementPolicy` / `EntitlementVerifier` / `Entitled` /
`Feature` / `Features` / `Plan` / `Meter` / `Decision` and the `testing` feature's
`test_entitlement_minter` / `test_entitlement_verifier`. We consume that surface
verbatim and re-implement none of the commercial rules.

The lock file gained ONLY the licensing crates (`cyan-identity`, `jsonwebtoken`,
`secrecy`, `num-bigint`, `num-integer`, `simple_asn1`). **No** networking crate
(`rustls`, `ring`, `quinn`, `iroh`, `webpki`) changed version — the QUIC/TLS stack
is byte-identical to the base branch, so sync behavior is untouched.

## What shipped

### `src/licensing.rs` — the one gate path
A thin BACKEND consumer of the identity entitlement model. All decisions delegate
to `EntitlementPolicy` / `EntitlementVerifier`; the backend re-implements nothing.

- **`CloudAction { RunWorkflow, Codegen, MarketplacePublish }`** — the backend's
  genuinely-cloud, paid surfaces. Each maps to exactly one identity `Feature`
  (`Lens` / `Codegen` / `MarketplacePublish`). Local steps, local MCP tools, chat,
  files, notes and sync are deliberately NOT here — they are never gated.
- **`OpenState { Full, LocalOnly }`** — the startup-check result. The app ALWAYS
  opens (local data is never locked out); this only distinguishes full access
  from a degraded, local-only session.
- **`LicenseGate`** — holds a tenant's resolved `Entitlement` + an
  `EntitlementPolicy`:
  - `from_cached_token(token, verifier, now)` — the OFFLINE startup path:
    `EntitlementVerifier::verify_at` checks RS256 + issuer and enforces
    `exp + grace` against `now`, so a cached token rides a short outage with the
    license server unreachable. Past `exp + grace` it errors → caller opens
    local-only.
  - `open_state(now)` — `Full` for a valid paid entitlement or an active trial,
    else `LocalOnly`.
  - `authorize(want, now)` / `authorize_for(tenant, want, now)` — delegate to the
    policy; `LocalRead` is ALWAYS `Allow`, tenant isolation is enforced.
  - `gate_cloud(action, now)` + `deny_reason(action, now)` — the per-action gate
    with a clear, user-facing reason ("'codegen' is not included in the pro
    plan" / "… the trial has expired; renew to use cloud features").
  - `within_seat_cap(active_seats)` — the per-seat (subscription) gate.
- **Process install + the single helper**:
  - `static GATE: OnceLock<Arc<LicenseGate>>` — `None` (default) means licensing
    is NOT configured, so the engine behaves exactly as before any gating
    existed (existing deployments + the local test rigs are unaffected). The
    FFI/iOS init installs a gate once it has resolved the tenant's cached
    entitlement. `install_gate` is idempotent and never panics.
  - `gate_cloud_action(action) -> Result<(), String>` — the ONE call the cloud
    dispatch makes. `Ok(())` when allowed OR when no gate is installed; `Err`
    with a clear reason when a configured license denies. It never touches local
    data, sync, or LAN collaboration.

Tokens are `SecretString` end-to-end (verified via the embedded RS256 keypair in
tests) and are never logged or persisted in the clear.

### Wiring — additive, one site
`src/pipeline_executor.rs::execute_pipeline_step`: before a genuinely-cloud
(`"cloud" | "lens"`) Lens run, it calls `gate_cloud_action(RunWorkflow)`. With no
gate installed this is a no-op (behavior identical to today). A denied tenant gets
a clear `StatusUpdate` ("🔒 … needs a license: …") and the step ends in a clear
gated state **without blocking the local steps** (local-placement steps run
unconditionally, before and below this guard). Local MCP tool steps and the local
fallback path never consult the gate.

## Tests (RED→green; bounded waits; assert real state)

`tests/licensing_test.rs` (7, all green) — unit-level, signs/verifies with the
identity `testing` keypair:
- `app_opens_with_valid_or_trial` — paid + active-trial gates both report
  `OpenState::Full` and allow LocalRead + the granted cloud surface.
- `expired_gates_paid_keeps_local` — expired trial → `LocalOnly`; LocalRead stays
  `Allow`; every cloud surface `Deny` with a reason.
- `offline_uses_cached_entitlement` — a token that expired an hour ago (server
  unreachable) still authorizes within the 7-day grace; past `exp + grace` it is
  rejected.
- `license_check_works_offline_keeps_lan` (X-CUT) — offline cached entitlement
  authorizes AND LocalRead is never gated even once the license lapses.
- `seat_cap_enforced` — within-cap `Allow`, over-cap `Deny`.
- `paid_surface_denied_without_feature` — a valid plan denies the features it
  doesn't include while allowing the one it does.
- `gate_tenant_scoped` — a gate never authorizes another tenant (not even
  LocalRead).

`tests/substrate_gating.rs` (1, green) — `lan_collab_not_gated_offline`: installs
an EXPIRED `LicenseGate` process-wide, proves it DENIES the cloud Lens run, then
runs offline (`RelayPolicy::Disabled` + `DiscoveryPolicy::MdnsOnly`) discovery +
delta sync across two loopback peers and asserts convergence end-to-end. Proves
the sync path never consults the gate, so an expired/absent license can never
break LAN/local P2P use.

## Gate status

- `cargo build --tests` — green.
- `cargo test` — my new suites green (`licensing_test` 7/7, `substrate_gating`
  1/1); all in-process substrate suites green; the other multi-process suites
  (`snapshot_mp`, `notes_mp`, `templates_mp`, `workspaces_mp`,
  `offline_multiuser_mp`) green. The anti-entropy / snapshot / offline thresholds
  re-run and HOLD with the gate in place.
- `cargo clippy` — `src/licensing.rs`, `tests/licensing_test.rs`,
  `tests/substrate_gating.rs`, and the one-site `pipeline_executor.rs` edit are
  clean under `-D warnings`. (The repo's pre-existing legacy `--all-targets` debt
  — edition-2024 `unsafe_op_in_unsafe_fn` warnings and dead code in the legacy
  multi-process test bins / FFI — exists identically on the base branch and is
  out of scope here.)

### Pre-existing failures (NOT regressions — verified on the base branch)
- `diagram_gen::tests::test_parse_diagram_json` (lib) — fails identically on
  `feat/round8-templates`.
- `substrate_multiuser_mp::expired_revoked_replayed_grant_rejected` — a
  multi-process grant-replay test (unrelated to licensing); flaky on the base
  branch too (fails at varying lines).
- `substrate_stress::concurrent_edits_converge_no_dupes` — the documented 4-node
  snapshot-under-load ceiling; **passes on this branch when the machine is calm**
  (N=2 and N=4 both ~4s green). The transient failures were resource contention
  during the full heavy suite, not a functional break — networking deps are
  byte-identical to base.

## Seams left for the FFI/iOS batch
- `install_gate(LicenseGate::from_cached_token(token, verifier, now))` at startup,
  once the iOS layer has the tenant's cached signed entitlement (XaeroID-first)
  and the broker's public key + grace window.
- `LicenseGate::{open_state, within_seat_cap, entitlement}` feed the trial banner
  ("N days left"), the seat-management view, and the locked/upgrade states.
- `gate_cloud_action` already guards the cloud Lens run; `Codegen` /
  `MarketplacePublish` map onto the same helper for when their backend dispatch
  sites land (today they live server-side in cyan-lens).

Do NOT rebuild the xcframework here — the live-test build will.
