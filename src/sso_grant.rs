//! Round 9 / W16 — backend SSO **grant** consumer (offline-first, graceful).
//!
//! This is the device side of the W15 front door: it consumes the signed,
//! expiring, XaeroID-bound **session grant** the Lens broker mints (the
//! `cyan_identity::Grant` — sibling to the entitlement token the [`crate::licensing`]
//! module already consumes). Do NOT confuse it with [`crate::identity::Grant`],
//! the MESH-half Ed25519 *capability* grant that authorizes a group write; this
//! module is purely the SSO **session** grant (role + group membership for one
//! XaeroID in one tenant).
//!
//! Four jobs, all offline-capable:
//!   1. **Cache** the signed grant ([`GrantCache`]) so it verifies with NO broker
//!      reachable — the prod backing is the iOS keychain; this is the seam.
//!   2. **Verify** it (RS256 signature + XaeroID-pubkey binding + `exp` + grace)
//!      via the cyan-identity `GrantVerifier`.
//!   3. On sign-in, **seed** the device into the grant's `groups` through the
//!      existing group-join path (the [`GroupJoiner`] seam → `JoinGroup`).
//!   4. **Enforce** the grant's `role` via the shared `RolePolicy`.
//!
//! Graceful (per X-CUT): a grant past `exp + grace` ⇒ re-auth required, but local
//! data + LAN/local P2P collaboration stay fully READABLE — those paths never
//! consult this module, exactly like the licensing gate.
//!
//! Tokens are `SecretString` end-to-end and are never logged or persisted in the
//! clear.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use cyan_identity::{
    Action, Actor, AuthZ, Decision, Grant, GrantVerifier, Resource, Role, RolePolicy,
};
use secrecy::SecretString;

use crate::models::commands::NetworkCommand;

// ============================================================================
// GroupJoiner — the seam onto the existing group-join / replicate path
// ============================================================================

/// Seeds the signed-in device into a group (join + replicate). The prod impl
/// ([`NetworkGroupJoiner`]) drives the EXISTING `NetworkCommand::JoinGroup` path
/// — additive, no new FFI shape. Tenant-scoped and idempotent: re-joining a
/// group the device already holds is a harmless no-op on the mesh side.
#[async_trait]
pub trait GroupJoiner: Send + Sync {
    /// Join/replicate `group` (under `tenant`) for `xaero_pubkey`.
    async fn join(&self, tenant: &str, group: &str, xaero_pubkey: &str) -> Result<()>;
}

#[async_trait]
impl<T: GroupJoiner + ?Sized> GroupJoiner for Arc<T> {
    async fn join(&self, tenant: &str, group: &str, xaero_pubkey: &str) -> Result<()> {
        (**self).join(tenant, group, xaero_pubkey).await
    }
}

/// Prod `GroupJoiner`: emits the existing `JoinGroup` command on the network
/// channel for each granted group. Reuses the same path `CreateGroup` and the
/// invite flow already use — purely an additional caller, no behavior change.
pub struct NetworkGroupJoiner {
    network_tx: tokio::sync::mpsc::UnboundedSender<NetworkCommand>,
}

impl NetworkGroupJoiner {
    pub fn new(network_tx: tokio::sync::mpsc::UnboundedSender<NetworkCommand>) -> Self {
        Self { network_tx }
    }
}

#[async_trait]
impl GroupJoiner for NetworkGroupJoiner {
    async fn join(&self, _tenant: &str, group: &str, _xaero_pubkey: &str) -> Result<()> {
        // The signed SSO grant is what authorized this membership; the mesh-side
        // capability grant is carried separately, so this join presents none.
        self.network_tx
            .send(NetworkCommand::JoinGroup {
                group_id: group.to_string(),
                bootstrap_peer: None,
                grant: None,
            })
            .map_err(|e| anyhow::anyhow!("failed to send JoinGroup for {group}: {e}"))?;
        Ok(())
    }
}

// ============================================================================
// GrantCache — the offline-cacheable signed grant (the prod backing is keychain)
// ============================================================================

/// An offline cache of the device's current signed grant token. The engine
/// stores the broker-minted token here on a successful sign-in, then verifies it
/// OFFLINE at every subsequent startup — no broker needed. The prod backing is
/// the iOS keychain / on-disk store; this in-memory cell is the seam.
///
/// The token never leaves this type as anything but a `SecretString`; it is never
/// logged or written in clear by the cache itself.
#[derive(Clone, Default)]
pub struct GrantCache {
    token: Arc<Mutex<Option<SecretString>>>,
}

impl GrantCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store (replace) the cached signed grant — called after a fresh broker mint
    /// or a re-auth re-mint.
    pub fn store(&self, token: SecretString) {
        *self.token.lock().expect("grant cache lock") = Some(token);
    }

    /// Load the cached signed grant, if any.
    pub fn load(&self) -> Option<SecretString> {
        self.token.lock().expect("grant cache lock").clone()
    }

    /// Clear the cache (e.g. explicit sign-out). Local data is untouched.
    pub fn clear(&self) {
        *self.token.lock().expect("grant cache lock") = None;
    }

    /// Whether a grant is currently cached (does not validate it).
    pub fn has_grant(&self) -> bool {
        self.token.lock().expect("grant cache lock").is_some()
    }
}

// ============================================================================
// SsoSession — a verified grant + the RolePolicy it enforces
// ============================================================================

/// A verified SSO session: the resolved `Grant` plus the shared computed
/// `RolePolicy`. Role enforcement delegates to identity's `RolePolicy`, so the
/// backend never re-implements the RBAC rules. Tenant-scoped: the session only
/// ever speaks for the grant's own tenant.
#[derive(Debug, Clone)]
pub struct SsoSession {
    grant: Grant,
    policy: RolePolicy,
}

impl SsoSession {
    /// Wrap an already-verified grant.
    pub fn new(grant: Grant) -> Self {
        Self {
            grant,
            policy: RolePolicy::new(),
        }
    }

    /// Resolve a session from a cached signed token — the OFFLINE path. The
    /// verifier checks the RS256 signature + issuer, the **XaeroID-pubkey
    /// binding**, and `exp + grace` against `now`, so a cached grant rides a
    /// short outage with the broker unreachable. Past `exp + grace` (or a grant
    /// bound to a different XaeroID, or a tampered one) this errors and the caller
    /// must re-auth — local data stays readable regardless.
    pub fn from_cached_token(
        token: &SecretString,
        verifier: &GrantVerifier,
        xaero_pubkey: &str,
        now: u64,
    ) -> Result<Self> {
        let grant = verifier.verify_at(token, xaero_pubkey, now)?;
        Ok(Self::new(grant))
    }

    pub fn tenant(&self) -> &str {
        &self.grant.tenant
    }

    pub fn role(&self) -> Role {
        self.grant.role
    }

    pub fn groups(&self) -> &[String] {
        &self.grant.groups
    }

    pub fn xaero_pubkey(&self) -> &str {
        &self.grant.xaero_pubkey
    }

    /// The verified grant (read-only).
    pub fn grant(&self) -> &Grant {
        &self.grant
    }

    /// Enforce the grant's `role` for `action` on `resource` via `RolePolicy`
    /// (`same-tenant AND role.level() >= action.min_level()`). The actor is the
    /// bound XaeroID in the grant's tenant — so a grant never authorizes another
    /// tenant's resource.
    pub fn enforce(&self, action: &Action, resource: &Resource) -> Decision {
        let actor = Actor::new(&self.grant.xaero_pubkey, &self.grant.tenant, self.grant.role);
        self.policy.authorize(&actor, action, resource)
    }
}

// ============================================================================
// Sign-in — verify the cached grant, seed its groups, yield the session
// ============================================================================

/// What a sign-in attempt resolves to. The device ALWAYS retains read access to
/// local data; this only distinguishes an active grant-backed session from a
/// lapsed one that needs re-auth.
pub enum SignIn {
    /// The grant verified (within `exp + grace`): an active session whose groups
    /// have been seeded and whose role is enforceable.
    Active(SsoSession),
    /// The grant was absent, bound to another XaeroID, tampered, or past
    /// `exp + grace`: re-authentication is required. **Local data + LAN/local P2P
    /// stay fully readable** — this never locks the device out.
    Reauth { reason: String },
}

impl SignIn {
    pub fn is_active(&self) -> bool {
        matches!(self, SignIn::Active(_))
    }

    /// The active session, if the grant verified.
    pub fn session(&self) -> Option<&SsoSession> {
        match self {
            SignIn::Active(s) => Some(s),
            SignIn::Reauth { .. } => None,
        }
    }

    /// Local data + LAN/local P2P reads stay READABLE in EVERY state — the grant
    /// never gates them (graceful, per X-CUT). Always `true`.
    pub const fn local_read_allowed(&self) -> bool {
        true
    }
}

/// Sign in from a cached signed grant: verify it OFFLINE (signature + binding +
/// `exp` + grace) at `now`; on success seed the device into each granted group
/// (idempotent join/replicate) and return an active session; on any verification
/// failure return [`SignIn::Reauth`] — local data is never touched either way.
///
/// A verification failure is NOT an error (it is the graceful re-auth path); only
/// a `joiner` failure surfaces as `Err`.
pub async fn sign_in(
    token: &SecretString,
    verifier: &GrantVerifier,
    xaero_pubkey: &str,
    now: u64,
    joiner: &dyn GroupJoiner,
) -> Result<SignIn> {
    let grant = match verifier.verify_at(token, xaero_pubkey, now) {
        Ok(grant) => grant,
        Err(e) => {
            return Ok(SignIn::Reauth {
                reason: e.to_string(),
            })
        }
    };

    // Seed the signed-in device into every granted group (idempotent).
    for group in &grant.groups {
        joiner
            .join(&grant.tenant, group, &grant.xaero_pubkey)
            .await?;
    }

    Ok(SignIn::Active(SsoSession::new(grant)))
}
