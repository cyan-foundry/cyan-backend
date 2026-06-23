//! Round 8 / W11 — backend licensing gate (offline-first, graceful).
//!
//! The backend consumes the cyan-identity entitlement model (`Entitlement` +
//! `EntitlementPolicy` + `EntitlementVerifier`). These tests pin the locked
//! defaults:
//!   - the app opens with a valid entitlement OR an active 7-day trial;
//!   - on expiry the PAID surfaces gate but local data stays usable (graceful);
//!   - the startup check works fully OFFLINE off a cached signed token + grace
//!     (the license server need NOT be reachable);
//!   - LocalRead / LAN-local collaboration is NEVER gated;
//!   - seat-cap + per-feature gates deny cloud surfaces with a clear reason.
//!
//! Tokens are `SecretString` end-to-end — minted/verified with the embedded
//! RS256 test keypair via cyan-identity's `testing` feature, never logged.

use cyan_backend::licensing::{CloudAction, LicenseGate, OpenState};
use cyan_identity::testing::{test_entitlement_minter, test_entitlement_verifier};
use cyan_identity::{Decision, Entitled, Entitlement, Feature, Features, Meter, Plan};

const ISSUER: &str = "cyan-license-test";
/// A fixed "now" so the grace-window arithmetic is deterministic.
const NOW: u64 = 1_700_000_000;
const DAY: u64 = 86_400;

fn meter() -> Meter {
    Meter {
        included_minutes: 1_000,
        rate_cents_per_minute: 5,
    }
}

/// A trial entitlement hard-stopping at `trial_expiry` (full feature grant).
fn trial(tenant: &str, trial_expiry: u64) -> Entitlement {
    Entitlement {
        tenant: tenant.to_string(),
        plan: Plan::Trial,
        seats: 5,
        features: Features::all(),
        trial_expiry: Some(trial_expiry),
        meter: meter(),
    }
}

/// A paid entitlement (no trial clock) with the given feature flags.
fn paid(tenant: &str, features: Features) -> Entitlement {
    Entitlement {
        tenant: tenant.to_string(),
        plan: Plan::Pro,
        seats: 3,
        features,
        trial_expiry: None,
        meter: meter(),
    }
}

// ── startup: app opens with a valid entitlement OR an active trial ──────────

#[test]
fn app_opens_with_valid_or_trial() {
    // A paid, valid entitlement → full open.
    let paid_gate = LicenseGate::new(paid("acme", Features::all()));
    assert_eq!(paid_gate.open_state(NOW), OpenState::Full);
    assert!(paid_gate.open_state(NOW).is_full());

    // An active 7-day trial (3 days left) → full open.
    let trial_gate = LicenseGate::new(trial("acme", NOW + 3 * DAY));
    assert_eq!(trial_gate.open_state(NOW), OpenState::Full);

    // Both surfaces work while open: LocalRead always, and the paid surfaces too.
    for g in [&paid_gate, &trial_gate] {
        assert_eq!(g.authorize(Entitled::LocalRead, NOW), Decision::Allow);
        assert_eq!(g.gate_cloud(CloudAction::RunWorkflow, NOW), Decision::Allow);
    }
}

// ── graceful expiry: paid surfaces gate, local data stays usable ────────────

#[test]
fn expired_gates_paid_keeps_local() {
    // A trial that ran out an hour ago.
    let gate = LicenseGate::new(trial("acme", NOW - 3_600));

    // The app still OPENS — degraded to local-only, never locked out.
    assert_eq!(gate.open_state(NOW), OpenState::LocalOnly);

    // Local data / LAN / local P2P read are ALWAYS allowed (graceful default).
    assert_eq!(gate.authorize(Entitled::LocalRead, NOW), Decision::Allow);

    // Every genuinely-cloud paid surface gates with a clear reason.
    for action in [
        CloudAction::RunWorkflow,
        CloudAction::Codegen,
        CloudAction::MarketplacePublish,
    ] {
        assert_eq!(
            gate.gate_cloud(action, NOW),
            Decision::Deny,
            "{action:?} must gate after trial expiry"
        );
        assert!(
            gate.deny_reason(action, NOW).is_some(),
            "{action:?} denial must carry a reason"
        );
    }
}

// ── offline: a cached signed token + grace authorizes with the server down ──

#[test]
fn offline_uses_cached_entitlement() {
    let minter = test_entitlement_minter(ISSUER);
    // 7-day offline grace past `exp`.
    let verifier = test_entitlement_verifier(ISSUER, 7 * DAY);

    // The token was minted yesterday and EXPIRED an hour ago; the license server
    // is unreachable, so it can't be refreshed — only the cached copy exists.
    let ent = paid("acme", Features::all());
    let iat = NOW - DAY;
    let exp = NOW - 3_600;
    let token = minter.issue(&ent, iat, exp).expect("mint cached token");

    // Verifying offline within the grace window still resolves the entitlement.
    let gate = LicenseGate::from_cached_token(&token, &verifier, NOW)
        .expect("cached token within grace authorizes offline");
    assert_eq!(gate.open_state(NOW), OpenState::Full);
    assert_eq!(gate.gate_cloud(CloudAction::RunWorkflow, NOW), Decision::Allow);

    // Past `exp + grace` the cached token is rejected (license has truly lapsed).
    let lapsed = LicenseGate::from_cached_token(&token, &verifier, NOW + 8 * DAY);
    assert!(
        lapsed.is_err(),
        "a token past exp+grace must not authorize"
    );
}

// ── X-CUT: the offline check authorizes AND never gates local/LAN ───────────

#[test]
fn license_check_works_offline_keeps_lan() {
    let minter = test_entitlement_minter(ISSUER);
    let verifier = test_entitlement_verifier(ISSUER, 7 * DAY);

    // Server unreachable: only a cached, just-expired token is on disk.
    let token = minter
        .issue(&paid("acme", Features::all()), NOW - DAY, NOW - 60)
        .expect("mint");
    let gate = LicenseGate::from_cached_token(&token, &verifier, NOW)
        .expect("offline cached entitlement authorizes");

    // LocalRead — the LAN/local-P2P surface — is allowed with the server down.
    assert_eq!(gate.authorize(Entitled::LocalRead, NOW), Decision::Allow);

    // Even once the trial/license has fully lapsed, LocalRead stays allowed:
    // local collaboration is never gated, only cloud surfaces degrade.
    let expired = LicenseGate::new(trial("acme", NOW - DAY));
    assert_eq!(expired.authorize(Entitled::LocalRead, NOW), Decision::Allow);
    assert_eq!(
        expired.gate_cloud(CloudAction::RunWorkflow, NOW),
        Decision::Deny
    );
}

// ── per-seat subscription cap ───────────────────────────────────────────────

#[test]
fn seat_cap_enforced() {
    // `paid()` carries a 3-seat cap.
    let gate = LicenseGate::new(paid("acme", Features::all()));
    assert_eq!(gate.within_seat_cap(3), Decision::Allow);
    assert_eq!(gate.within_seat_cap(2), Decision::Allow);
    assert_eq!(gate.within_seat_cap(4), Decision::Deny);
}

// ── a paid surface the plan doesn't include is denied even while valid ──────

#[test]
fn paid_surface_denied_without_feature() {
    // A paid plan with ONLY Lens enabled (no codegen, no marketplace publish).
    let features = Features {
        lens: true,
        codegen: false,
        marketplace_publish: false,
    };
    let gate = LicenseGate::new(paid("acme", features));

    // Lens is on → allowed; the two absent features gate despite a valid plan.
    assert_eq!(gate.gate_cloud(CloudAction::RunWorkflow, NOW), Decision::Allow);
    assert_eq!(gate.gate_cloud(CloudAction::Codegen, NOW), Decision::Deny);
    assert_eq!(
        gate.gate_cloud(CloudAction::MarketplacePublish, NOW),
        Decision::Deny
    );

    // And local read is always fine.
    assert_eq!(gate.authorize(Entitled::LocalRead, NOW), Decision::Allow);
}

// ── tenant isolation: a gate never authorizes another tenant ────────────────

#[test]
fn gate_tenant_scoped() {
    let gate = LicenseGate::new(paid("acme", Features::all()));
    // The gate's own tenant is authorized for its features…
    assert_eq!(
        gate.authorize_for("acme", Entitled::Feature(Feature::Lens), NOW),
        Decision::Allow
    );
    // …but it never speaks for another tenant — not even for LocalRead.
    assert_eq!(
        gate.authorize_for("globex", Entitled::Feature(Feature::Lens), NOW),
        Decision::Deny
    );
    assert_eq!(
        gate.authorize_for("globex", Entitled::LocalRead, NOW),
        Decision::Deny
    );
    assert_eq!(gate.tenant(), "acme");
}
