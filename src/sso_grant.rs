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
//!      via the cyan-identity `GrantVerifier` — or, per W17 §A, against the
//!      pinned **per-tenant org key** via `OrgGrantVerifier` (legacy `"cyan-lens"`
//!      grants still accepted, and a revoked `xaero_pubkey` rejected even when the
//!      grant is otherwise live). See [`SsoSession::from_org_token`].
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

use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;
use async_trait::async_trait;
use cyan_identity::{
    Action, Actor, AuthZ, Decision, Grant, GrantVerifier, OrgGrantVerifier, OrgPubKey, Resource,
    Role, RolePolicy, SignedRevocationList,
};
use secrecy::SecretString;
use tokio::sync::mpsc::UnboundedSender;

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

    /// Resolve a session by verifying `token` against the pinned **per-tenant org
    /// key** (W17 §A) rather than the single global broker key. The
    /// [`OrgGrantVerifier`] rebuilds the `grant ← delegate ← org root` chain and
    /// pins the issuer to the tenant's org DID; built `.with_legacy(..)` it ALSO
    /// keeps accepting legacy `"cyan-lens"`-issued grants during the cutover. Same
    /// offline contract as [`SsoSession::from_cached_token`] (binding + `exp`
    /// + grace), so a cached org grant verifies with NO broker reachable.
    pub fn from_org_token(
        token: &SecretString,
        verifier: &OrgGrantVerifier,
        xaero_pubkey: &str,
        now: u64,
    ) -> Result<Self> {
        let grant = verifier.verify_at(token, xaero_pubkey, now)?;
        Ok(Self::new(grant))
    }

    /// Like [`SsoSession::from_org_token`] but additionally rejects a grant whose
    /// `xaero_pubkey` is on the org-signed `revocation` list (W17 §A/§C) — even if
    /// the grant is otherwise valid and unexpired (the "fired employee" who still
    /// holds a live token). The list is itself verified org-signed against the
    /// grant's pinned org key before it is trusted, so a forged list can neither
    /// suppress nor fabricate a revocation.
    pub fn from_org_token_checked(
        token: &SecretString,
        verifier: &OrgGrantVerifier,
        xaero_pubkey: &str,
        now: u64,
        revocation: &SignedRevocationList,
    ) -> Result<Self> {
        let grant = verifier.verify_with_revocation_at(token, xaero_pubkey, now, revocation)?;
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

// ============================================================================
// A2 §7 — the INSTALLED session (the ONE process-global RBAC tier source)
// ============================================================================
//
// Grounded gap this closes: `sign_in()` had ZERO callers, `GrantCache` was
// unwired, and no FFI verb accepted a grant. `cyan_sso_install_grant` (ffi/core)
// is the front door; this module owns the verify + seed + store core so tests
// drive it without the FFI layer.
//
// GrantCache is UNTOUCHED by install (its prod backing is the iOS keychain —
// account `cyan_session_grant`, service `io.blockxaero.cyan.sso`, D-A2.20; iOS
// re-installs each launch after `cyan_init_with_identity`; the engine persists
// nothing). `SsoSession` resolves offline from the cached token.
//
// Honesty (R9): revocation is checked at VERIFY (install) time only — a
// revoked-but-unexpired grant stays Active until exp+grace / sign-out /
// re-install; mitigation = short broker TTLs.
//
// Writers of the device `production_role` pref (cross-package XP-1) are NOT
// here — that pref is a `local_prefs` row (storage); this global is org-RBAC
// (tier) only, never craft-role provenance.

/// A verified, installed SSO session plus its offline grace window.
pub struct InstalledSession {
    pub session: SsoSession,
    pub grace_secs: u64,
}

/// The process-global installed session. `None` = fail-open (`NoSession`);
/// `Some` = Active until `exp + grace`, then fail-open again (`Expired`) —
/// the tier fn does the arithmetic per write, no timer.
pub static SSO_SESSION: RwLock<Option<InstalledSession>> = RwLock::new(None);

/// The RBAC tier fn (`notes_rbac`'s injected source): the installed session's
/// role while live at `now` (`now <= exp + grace`), else `None` ⇒ fail-open.
pub fn installed_tier_at(now: u64) -> Option<Role> {
    let guard = SSO_SESSION.read().ok()?;
    let installed = guard.as_ref()?;
    let grant = installed.session.grant();
    if now > grant.exp.saturating_add(installed.grace_secs) {
        return None; // Expired past grace ⇒ fail-open locally (mesh/lens still enforce).
    }
    Some(installed.session.role())
}

/// [`installed_tier_at`] at the wall clock — the prod closure `dispatch_put_note`
/// samples per write.
pub fn installed_tier() -> Option<Role> {
    installed_tier_at(chrono::Utc::now().timestamp().max(0) as u64)
}

/// Clear the installed session (`cyan_sso_sign_out`). Local data untouched.
pub fn sign_out() {
    if let Ok(mut guard) = SSO_SESSION.write() {
        *guard = None;
    }
}

/// The parsed `trust_json` of `cyan_sso_install_grant` (§7): `tenant` required;
/// ≥1 trust source (`org_did` and/or `legacy_rsa_public_pem`) required; grace
/// defaults to 7 days.
#[derive(serde::Deserialize)]
pub struct TrustConfig {
    pub tenant: String,
    #[serde(default)]
    pub org_did: Option<String>,
    #[serde(default)]
    pub legacy_rsa_public_pem: Option<String>,
    #[serde(default = "default_grace_secs")]
    pub grace_secs: u64,
}

fn default_grace_secs() -> u64 {
    604_800
}

/// Rebuild an [`OrgPubKey`] from its `did:cyan:<base64url-pubkey>` DID (the
/// grant-issuer string a broker publishes; `OrgPubKey::did()` is the inverse).
fn org_pubkey_from_did(did: &str) -> Result<OrgPubKey> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    let b64 = did
        .strip_prefix("did:cyan:")
        .ok_or_else(|| anyhow::anyhow!("org_did must start with did:cyan:"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(b64)
        .map_err(|e| anyhow::anyhow!("org_did key is not base64url: {e}"))?;
    OrgPubKey::from_bytes(&bytes)
}

/// The verify + seed + store core behind `cyan_sso_install_grant` (§7, Seq 9).
///
/// Builds `OrgGrantVerifier::new(grace).trust_org(tenant, OrgPubKey::from(did))`
/// [+ `.with_legacy(GrantVerifier::from_rsa_pem(pem, "cyan-lens", grace))`],
/// verifies via [`SsoSession::from_org_token`] against `xaero_pubkey` (the
/// engine identity from `cyan_init_with_identity`) at `now`, and on success:
/// seeds the granted groups through the EXISTING `JoinGroup` path (the
/// `sign_in()` pattern; `network_tx == None` in storage-only tests skips the
/// seed), stores the session in [`SSO_SESSION`], and returns
/// `{"active":true,"tenant":…,"role":…,"exp":…}`.
///
/// On ANY failure the previously installed session is left UNTOUCHED and
/// `{"active":false,"reason":…}` is returned — a bad re-install never signs the
/// device out. GrantCache is never written (T24c).
pub fn install_grant(
    grant_token: &str,
    trust_json: &str,
    xaero_pubkey: &str,
    now: u64,
    network_tx: Option<&UnboundedSender<NetworkCommand>>,
) -> serde_json::Value {
    let fail = |reason: String| serde_json::json!({ "active": false, "reason": reason });

    let trust: TrustConfig = match serde_json::from_str(trust_json) {
        Ok(t) => t,
        Err(e) => return fail(format!("bad trust_json: {e}")),
    };
    if trust.tenant.is_empty() {
        return fail("trust_json.tenant required".to_string());
    }
    if trust.org_did.is_none() && trust.legacy_rsa_public_pem.is_none() {
        return fail("no trust material".to_string());
    }

    let mut verifier = OrgGrantVerifier::new(trust.grace_secs);
    if let Some(did) = trust.org_did.as_deref() {
        match org_pubkey_from_did(did) {
            Ok(org) => verifier = verifier.trust_org(trust.tenant.clone(), org),
            Err(e) => return fail(format!("bad org_did: {e}")),
        }
    }
    if let Some(pem) = trust.legacy_rsa_public_pem.as_deref() {
        match GrantVerifier::from_rsa_pem(pem.as_bytes(), "cyan-lens", trust.grace_secs) {
            Ok(legacy) => verifier = verifier.with_legacy(legacy),
            Err(e) => return fail(format!("bad legacy_rsa_public_pem: {e}")),
        }
    }

    let token = SecretString::new(grant_token.to_string());
    let session = match SsoSession::from_org_token(&token, &verifier, xaero_pubkey, now) {
        Ok(s) => s,
        Err(e) => {
            // Previously installed session stays untouched — verified above by T24/T24c.
            return fail(format!("grant did not verify: {e}"));
        }
    };

    // Seed granted groups via the EXISTING JoinGroup path (idempotent; the mesh
    // capability grant is carried separately, so this join presents none).
    if let Some(tx) = network_tx {
        for group in session.groups() {
            let _ = tx.send(NetworkCommand::JoinGroup {
                group_id: group.clone(),
                bootstrap_peer: None,
                grant: None,
            });
        }
    }

    let out = serde_json::json!({
        "active": true,
        "tenant": session.tenant(),
        "role": session.role().as_str(),
        "exp": session.grant().exp,
    });
    if let Ok(mut guard) = SSO_SESSION.write() {
        *guard = Some(InstalledSession { session, grace_secs: trust.grace_secs });
    }
    tracing::info!("obs sso_grant_installed tenant={} exp={}", out["tenant"], out["exp"]);
    out
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
