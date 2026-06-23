//! Round 8 / W11 — backend licensing gate (offline-first, graceful).
//!
//! The commercial model — `Entitlement`, the `EntitlementPolicy` gate, and the
//! signed-token primitives — lives in cyan-identity (`feat/round8-license`). This
//! module is the thin BACKEND consumer of it: ONE gate path the engine consults
//! before a genuinely-cloud paid surface runs.
//!
//! Locked defaults (graceful, offline-first):
//!   - **LocalRead is ALWAYS allowed.** Local data + LAN/local P2P collaboration
//!     are NEVER gated — the discovery/sync/chat/files/notes paths never call
//!     here, so an expired or absent license can never break offline/LAN use.
//!   - The app **opens** with a valid entitlement OR an active 7-day trial; on
//!     expiry it stays open, degraded to local-only (cloud surfaces gate).
//!   - The startup check runs **offline** off a cached, signed entitlement token
//!     verified with a grace window — the license server need NOT be reachable.
//!   - Only genuinely-cloud surfaces gate: Lens cloud runs (`RunWorkflow`),
//!     `Codegen`, `MarketplacePublish`, and the per-seat cap.
//!
//! Tokens are `SecretString` end-to-end and are never logged or persisted in the
//! clear.

use std::sync::{Arc, OnceLock};

use anyhow::Result;
use cyan_identity::{
    now_unix, Decision, Entitled, Entitlement, EntitlementPolicy, EntitlementVerifier, Feature,
};
use secrecy::SecretString;

// ============================================================================
// CloudAction — the backend's genuinely-cloud, paid surfaces
// ============================================================================

/// A paid action the backend dispatches to the cloud, gated behind the
/// entitlement. Each maps onto exactly one identity `Feature`. Local-placement
/// steps, local MCP tools, chat, files, notes and sync are NOT here — they are
/// never gated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudAction {
    /// Run a workflow step through Lens cloud (the metered, GPU-backed surface).
    RunWorkflow,
    /// Codegen a plugin via cyan-forge (per-job fee).
    Codegen,
    /// Publish a plugin/template to the marketplace (30% transaction fee).
    MarketplacePublish,
}

impl CloudAction {
    /// The identity feature flag this action requires.
    pub fn feature(self) -> Feature {
        match self {
            CloudAction::RunWorkflow => Feature::Lens,
            CloudAction::Codegen => Feature::Codegen,
            CloudAction::MarketplacePublish => Feature::MarketplacePublish,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            CloudAction::RunWorkflow => "run_workflow",
            CloudAction::Codegen => "codegen",
            CloudAction::MarketplacePublish => "marketplace_publish",
        }
    }
}

// ============================================================================
// OpenState — what the startup license check resolves to
// ============================================================================

/// The result of the startup license check. The app ALWAYS opens (local data is
/// never locked out); this only distinguishes full access from a degraded,
/// local-only session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenState {
    /// Valid paid entitlement or an active trial — paid surfaces are available.
    Full,
    /// Trial/license lapsed — local data + LAN/local P2P stay fully usable, but
    /// cloud/paid surfaces gate until renewed.
    LocalOnly,
}

impl OpenState {
    pub fn is_full(self) -> bool {
        matches!(self, OpenState::Full)
    }
}

// ============================================================================
// LicenseGate — the one gate path
// ============================================================================

/// Holds a tenant's resolved `Entitlement` and answers the gate questions.
/// Pure and synchronous: all decisions delegate to the identity
/// `EntitlementPolicy`, so the backend never re-implements the commercial rules.
#[derive(Debug, Clone)]
pub struct LicenseGate {
    entitlement: Entitlement,
    policy: EntitlementPolicy,
}

impl LicenseGate {
    /// Build a gate around an already-resolved entitlement (e.g. a freshly
    /// minted trial, or a token a caller verified itself).
    pub fn new(entitlement: Entitlement) -> Self {
        Self {
            entitlement,
            policy: EntitlementPolicy::new(),
        }
    }

    /// Resolve the gate from a cached, signed entitlement token — the OFFLINE
    /// startup path. The verifier checks the RS256 signature + issuer and
    /// enforces `exp + grace` against `now`, so a cached token rides a short
    /// outage with the license server unreachable. Past `exp + grace` this errors
    /// (the license has truly lapsed) and the caller opens local-only.
    pub fn from_cached_token(
        token: &SecretString,
        verifier: &EntitlementVerifier,
        now: u64,
    ) -> Result<Self> {
        let entitlement = verifier.verify_at(token, now)?;
        Ok(Self::new(entitlement))
    }

    /// The tenant this gate speaks for.
    pub fn tenant(&self) -> &str {
        &self.entitlement.tenant
    }

    /// The resolved entitlement (read-only; for usage/billing views, never logs).
    pub fn entitlement(&self) -> &Entitlement {
        &self.entitlement
    }

    /// The startup decision: `Full` for a valid paid entitlement or an active
    /// trial, else `LocalOnly`. Local data opens in BOTH states.
    pub fn open_state(&self, now: u64) -> OpenState {
        if self.entitlement.is_expired_at(now) {
            OpenState::LocalOnly
        } else {
            OpenState::Full
        }
    }

    /// Authorize a want for THIS gate's tenant (the common case).
    pub fn authorize(&self, want: Entitled, now: u64) -> Decision {
        self.authorize_for(self.tenant(), want, now)
    }

    /// Authorize a want for an explicit `tenant` — an entitlement never
    /// authorizes another tenant (tenant isolation).
    pub fn authorize_for(&self, tenant: &str, want: Entitled, now: u64) -> Decision {
        self.policy.authorize(&self.entitlement, tenant, want, now)
    }

    /// Gate a genuinely-cloud paid action for this gate's tenant.
    pub fn gate_cloud(&self, action: CloudAction, now: u64) -> Decision {
        self.authorize(Entitled::Feature(action.feature()), now)
    }

    /// A human-readable reason a cloud action is denied, or `None` if allowed.
    pub fn deny_reason(&self, action: CloudAction, now: u64) -> Option<String> {
        if self.gate_cloud(action, now).allowed() {
            return None;
        }
        let feature = action.feature();
        let reason = if !self.entitlement.features.has(feature) {
            format!(
                "'{}' is not included in the {} plan",
                action.as_str(),
                self.entitlement.plan.as_str()
            )
        } else {
            format!(
                "'{}' is unavailable — the {} has expired; renew to use cloud features",
                action.as_str(),
                self.entitlement.plan.as_str()
            )
        };
        Some(reason)
    }

    /// The per-seat (subscription) gate: an active-seat count is allowed iff it
    /// is within the entitlement's cap.
    pub fn within_seat_cap(&self, active_seats: u32) -> Decision {
        self.policy.within_seat_cap(&self.entitlement, active_seats)
    }
}

// ============================================================================
// Process-wide install — the engine's single gate handle
// ============================================================================

/// The installed gate, if licensing is configured for this process.
///
/// `None` (default) means licensing is NOT configured — the engine behaves
/// exactly as before any gating existed, so existing deployments and the local
/// test rigs are unaffected. The iOS/FFI init installs a gate once it has
/// resolved the tenant's cached entitlement.
static GATE: OnceLock<Arc<LicenseGate>> = OnceLock::new();

/// Install the process gate. Idempotent: the first install wins (returns `false`
/// if a gate was already installed). Never panics — safe on the FFI path.
pub fn install_gate(gate: LicenseGate) -> bool {
    GATE.set(Arc::new(gate)).is_ok()
}

/// The installed gate, if any.
pub fn installed_gate() -> Option<Arc<LicenseGate>> {
    GATE.get().cloned()
}

/// Gate a genuinely-cloud paid action against the installed license, at the real
/// clock. Returns `Ok(())` when allowed OR when no gate is installed (licensing
/// not configured — legacy/local behavior preserved). Returns `Err(reason)` with
/// a clear, user-facing message when a configured license denies the action.
///
/// This is the ONE call the cloud dispatch paths make. It NEVER touches local
/// data, sync, or LAN collaboration — those paths must not call it.
pub fn gate_cloud_action(action: CloudAction) -> std::result::Result<(), String> {
    match installed_gate() {
        None => Ok(()),
        Some(gate) => {
            let now = now_unix();
            if gate.gate_cloud(action, now).allowed() {
                Ok(())
            } else {
                Err(gate.deny_reason(action, now).unwrap_or_else(|| {
                    format!("'{}' is not available on your plan", action.as_str())
                }))
            }
        }
    }
}
