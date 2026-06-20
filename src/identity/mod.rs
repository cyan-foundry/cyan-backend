//! Identity / RBAC — the **mesh half** (IDENTITY_RBAC_SPEC, build-order step 2).
//!
//! The cloud half (SSO + `authorize()`) lives in cyan-lens. Here we own the **XaeroID-signed
//! capability grant**: the offline, integrity-only proof of *who may write/administer a group*.
//!
//! ## Why the `Grant` type is homed here (not in xaeroID)
//! xaeroID owns the raw Ed25519 primitives (`ed25519_sign`/`ed25519_pubkey`/`verify`) and the
//! device/peer identity. A *capability grant* is a cyan-backend domain concept — it references
//! cyan's group hierarchy and its fixed RBAC role vocabulary, and it is enforced by cyan's mesh
//! actors. Per the spec ("prefer homing the Grant type in cyan-backend for now"), the type lives
//! here and only *borrows* xaeroID's keypair/sign/verify primitives. No additive helper was needed
//! in the xaeroID crate.
//!
//! ## Shape
//! `Grant { group_id, role, issued_by (admin pubkey), issued_at, expiry, nonce } + Ed25519 signature`.
//! - **Issue** ([`Grant::issue`]): only an Admin/Owner of the group (per a [`GroupRoster`]) may sign.
//! - **Verify** ([`GrantVerifier::verify`]): signature valid · issuer is a current admin · not
//!   expired · nonce unseen (anti-replay) · not revoked.
//! - **Revoke** ([`GrantVerifier::revoke`]): a `(group_id, nonce)` tombstone — gossips like any
//!   group state (see [`mesh`] for the mesh-write enforcement seam).
//! - **QR** ([`Grant::to_qr_payload`] / [`Grant::from_qr_payload`]): encode→decode→verify roundtrip.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use xaeroid::XaeroID;

pub mod mesh;
pub use mesh::{DenyReason, MeshAuthorizer, SnapshotDenial, WriteDecision};

/// Current grant wire/QR version. Bumped only on a breaking payload change.
const GRANT_VERSION: u8 = 1;

fn default_version() -> u8 {
    GRANT_VERSION
}

/// The fixed RBAC role vocabulary (matches the cloud/lens half). Group-scoped.
///
/// Serializes as its variant name (`"Owner"`, `"Admin"`, …) on the wire and in QR payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Owner,
    Admin,
    Member,
    Viewer,
    Guest,
}

impl Role {
    /// Can this role administer the group (issue/revoke grants, admin actions)?
    pub fn can_administer(&self) -> bool {
        matches!(self, Role::Owner | Role::Admin)
    }

    /// Can this role write group state (deltas, chat, board content)?
    /// Owner/Admin/Member write; Viewer/Guest are read-only.
    pub fn can_write(&self) -> bool {
        matches!(self, Role::Owner | Role::Admin | Role::Member)
    }

    /// Stable lowercase token used in the deterministic signing payload.
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Owner => "owner",
            Role::Admin => "admin",
            Role::Member => "member",
            Role::Viewer => "viewer",
            Role::Guest => "guest",
        }
    }

    /// Parse a role token (case-insensitive). Returns `None` for an unknown role.
    pub fn parse(s: &str) -> Option<Role> {
        match s.to_ascii_lowercase().as_str() {
            "owner" => Some(Role::Owner),
            "admin" => Some(Role::Admin),
            "member" => Some(Role::Member),
            "viewer" => Some(Role::Viewer),
            "guest" => Some(Role::Guest),
            _ => None,
        }
    }
}

/// Hex of the Ed25519 public key derived from a 32-byte secret. The canonical issuer/peer
/// identity used throughout grants (avoids tests/callers depending on hex + xaeroID directly).
pub fn pubkey_hex(secret: &[u8; 32]) -> String {
    hex::encode(XaeroID::ed25519_pubkey(secret))
}

/// Why issuing a grant failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantError {
    /// The issuer is not an Admin/Owner of the group — not authorized to sign a grant for it.
    NotAuthorized,
    /// The QR/wire payload could not be decoded into a `Grant`.
    Malformed,
}

impl std::fmt::Display for GrantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrantError::NotAuthorized => write!(f, "issuer is not an admin of the group"),
            GrantError::Malformed => write!(f, "grant payload is malformed"),
        }
    }
}
impl std::error::Error for GrantError {}

/// Why verifying a grant on scan/receipt failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// Signature does not verify against the claimed issuer pubkey (forged or tampered).
    BadSignature,
    /// The issuer is not a current admin of the group.
    IssuerNotAdmin,
    /// `now >= expiry`.
    Expired,
    /// This nonce has already been accepted (anti-replay).
    ReplayedNonce,
    /// The grant was revoked (a `(group_id, nonce)` tombstone exists).
    Revoked,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            VerifyError::BadSignature => "bad signature",
            VerifyError::IssuerNotAdmin => "issuer is not a current admin",
            VerifyError::Expired => "grant expired",
            VerifyError::ReplayedNonce => "nonce already seen (replay)",
            VerifyError::Revoked => "grant revoked",
        };
        write!(f, "{s}")
    }
}
impl std::error::Error for VerifyError {}

/// A signed capability grant: the bearer's role in a group, signed by a group admin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    #[serde(default = "default_version")]
    pub version: u8,
    pub group_id: String,
    pub role: Role,
    /// Issuer's Ed25519 public key, hex-encoded (32 bytes → 64 hex chars).
    pub issued_by: String,
    /// Issue time, unix seconds.
    pub issued_at: u64,
    /// Absolute expiry, unix seconds (`now >= expiry` ⇒ rejected).
    pub expiry: u64,
    /// Single-use nonce (anti-replay + revocation key). Unique per grant within a group.
    pub nonce: String,
    /// Ed25519 signature over [`Grant::signing_payload`], hex-encoded (64 bytes → 128 hex chars).
    pub signature: String,
}

impl Grant {
    /// Deterministic bytes the signature covers. MUST NOT include `signature` itself, and MUST
    /// cover every field a verifier trusts (so tampering any of them breaks the signature).
    fn signing_payload(&self) -> String {
        format!(
            "cyan_grant:v{}:{}:{}:{}:{}:{}:{}",
            self.version,
            self.group_id,
            self.role.as_str(),
            self.issued_by,
            self.issued_at,
            self.expiry,
            self.nonce,
        )
    }

    /// Sign a grant with `issuer_secret` (no authority check). The `issued_by` field is the
    /// issuer's own pubkey. Used by [`Grant::issue`] after its authority check, and exposed for
    /// negative tests (forge an issuer field, then re-check the signature fails).
    pub fn issue_unchecked(
        group_id: &str,
        role: Role,
        issuer_secret: &[u8; 32],
        issued_at: u64,
        expiry: u64,
        nonce: &str,
    ) -> Grant {
        let mut grant = Grant {
            version: GRANT_VERSION,
            group_id: group_id.to_string(),
            role,
            issued_by: pubkey_hex(issuer_secret),
            issued_at,
            expiry,
            nonce: nonce.to_string(),
            signature: String::new(),
        };
        let sig = XaeroID::ed25519_sign(grant.signing_payload().as_bytes(), issuer_secret);
        grant.signature = hex::encode(sig);
        grant
    }

    /// Issue (sign) a grant — **only if** `issuer_secret`'s pubkey is an Admin/Owner of `group_id`
    /// per `roster`. Returns [`GrantError::NotAuthorized`] otherwise (a non-admin cannot mint a
    /// grant for the group).
    pub fn issue(
        group_id: &str,
        role: Role,
        issuer_secret: &[u8; 32],
        issued_at: u64,
        expiry: u64,
        nonce: &str,
        roster: &GroupRoster,
    ) -> Result<Grant, GrantError> {
        let issuer = pubkey_hex(issuer_secret);
        if !roster.is_admin(group_id, &issuer) {
            return Err(GrantError::NotAuthorized);
        }
        Ok(Grant::issue_unchecked(
            group_id,
            role,
            issuer_secret,
            issued_at,
            expiry,
            nonce,
        ))
    }

    /// Verify only the cryptographic signature against the claimed `issued_by` pubkey.
    /// (Authority/expiry/replay/revocation are layered on by [`GrantVerifier`].)
    pub fn verify_signature(&self) -> bool {
        let Ok(pk) = decode_pubkey(&self.issued_by) else {
            return false;
        };
        let Ok(sig) = decode_signature(&self.signature) else {
            return false;
        };
        XaeroID::verify(self.signing_payload().as_bytes(), &sig, &pk)
    }

    /// Encode to the QR payload (compact JSON). The scanner decodes with
    /// [`Grant::from_qr_payload`] and then verifies via [`GrantVerifier`].
    pub fn to_qr_payload(&self) -> String {
        // Infallible in practice (all fields are plain JSON values); empty string on the
        // impossible error keeps this panic-free for FFI paths.
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Decode a grant from its QR payload. Does NOT verify — call [`GrantVerifier::verify`] next.
    pub fn from_qr_payload(payload: &str) -> Result<Grant, GrantError> {
        serde_json::from_str(payload).map_err(|_| GrantError::Malformed)
    }
}

fn decode_pubkey(hex_str: &str) -> Result<[u8; 32], ()> {
    let bytes = hex::decode(hex_str).map_err(|_| ())?;
    bytes.try_into().map_err(|_| ())
}

fn decode_signature(hex_str: &str) -> Result<[u8; 64], ()> {
    let bytes = hex::decode(hex_str).map_err(|_| ())?;
    bytes.try_into().map_err(|_| ())
}

/// Who the current admins of each group are: `group_id → (pubkey_hex → role)`.
///
/// This is the verifier's authority oracle. In production it is seeded from group state (the
/// founding owner, plus whoever they promoted) and kept current as roles change; in tests it is
/// built explicitly. Only roles that `can_administer()` count as issuers.
#[derive(Debug, Clone, Default)]
pub struct GroupRoster {
    roles: HashMap<String, HashMap<String, Role>>,
}

impl GroupRoster {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `pubkey_hex`'s role in `group_id` (overwrites any prior role).
    pub fn set_role(&mut self, group_id: &str, pubkey_hex: &str, role: Role) {
        self.roles
            .entry(group_id.to_string())
            .or_default()
            .insert(pubkey_hex.to_string(), role);
    }

    /// The role of `pubkey_hex` in `group_id`, if any.
    pub fn role_of(&self, group_id: &str, pubkey_hex: &str) -> Option<Role> {
        self.roles.get(group_id).and_then(|m| m.get(pubkey_hex)).copied()
    }

    /// Whether `pubkey_hex` is a current admin (Owner/Admin) of `group_id`.
    pub fn is_admin(&self, group_id: &str, pubkey_hex: &str) -> bool {
        self.role_of(group_id, pubkey_hex)
            .map(|r| r.can_administer())
            .unwrap_or(false)
    }
}

/// Verifies grants and tracks per-group anti-replay + revocation state.
///
/// Stateful by design: it remembers every nonce it has accepted (anti-replay) and every
/// `(group_id, nonce)` tombstone it has been told to revoke. One verifier per node.
#[derive(Debug)]
pub struct GrantVerifier {
    roster: GroupRoster,
    seen_nonces: HashSet<String>,
    revoked: HashSet<(String, String)>,
}

impl GrantVerifier {
    pub fn new(roster: GroupRoster) -> Self {
        GrantVerifier {
            roster,
            seen_nonces: HashSet::new(),
            revoked: HashSet::new(),
        }
    }

    /// Mutable access to the authority roster (e.g. to promote/demote an admin as group state
    /// changes). Kept current so `IssuerNotAdmin` reflects *current* admins, not founding ones.
    pub fn roster_mut(&mut self) -> &mut GroupRoster {
        &mut self.roster
    }

    /// Revoke a grant by its `(group_id, nonce)` — a tombstone. Idempotent. After this, any
    /// presentation of that grant fails with [`VerifyError::Revoked`].
    pub fn revoke(&mut self, group_id: &str, nonce: &str) {
        self.revoked
            .insert((group_id.to_string(), nonce.to_string()));
    }

    /// Whether `(group_id, nonce)` has been revoked.
    pub fn is_revoked(&self, group_id: &str, nonce: &str) -> bool {
        self.revoked
            .contains(&(group_id.to_string(), nonce.to_string()))
    }

    /// Verify a grant against the wall clock. See [`GrantVerifier::verify_at`].
    pub fn verify(&mut self, grant: &Grant) -> Result<Role, VerifyError> {
        self.verify_at(grant, XaeroID::now_secs())
    }

    /// Verify a grant as-of `now` (unix seconds). On success, records the nonce (so a second
    /// presentation is rejected as a replay) and returns the granted role.
    ///
    /// Checks, in order: signature · issuer-is-current-admin · not expired · not revoked ·
    /// nonce-unseen. The nonce is consumed only on full success.
    pub fn verify_at(&mut self, grant: &Grant, now: u64) -> Result<Role, VerifyError> {
        if !grant.verify_signature() {
            return Err(VerifyError::BadSignature);
        }
        if !self.roster.is_admin(&grant.group_id, &grant.issued_by) {
            return Err(VerifyError::IssuerNotAdmin);
        }
        if now >= grant.expiry {
            return Err(VerifyError::Expired);
        }
        if self.is_revoked(&grant.group_id, &grant.nonce) {
            return Err(VerifyError::Revoked);
        }
        if self.seen_nonces.contains(&grant.nonce) {
            return Err(VerifyError::ReplayedNonce);
        }
        self.seen_nonces.insert(grant.nonce.clone());
        Ok(grant.role)
    }
}
