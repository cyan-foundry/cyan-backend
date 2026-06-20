//! Signed-grant QR: the role-carrying invite an admin ISSUES and a joiner SCANS.
//!
//! The mesh-half capability [`Grant`](super::Grant) is the security primitive (signed,
//! expiring, revocable). This module wraps it into the **QR envelope** the app moves
//! over a QR code: the grant PLUS the minimum a joiner needs to actually join the group
//! (its name + the inviter's node id to dial as a bootstrap peer).
//!
//! Two verbs, mirroring the iOS surface (`issueGrantQR` / `scanGrantQR`):
//! - **Issue** ([`issue_grant_qr`]) — admin-only (via [`Grant::issue`]'s authority check):
//!   sign a role grant and pack it into a [`GrantInvite`].
//! - **Scan** ([`scan_grant_qr_at`]) — decode + locally pre-verify (signature · expiry ·
//!   group present), then hand off to the join path. The **authoritative** checks
//!   (issuer-is-a-current-admin · revocation · anti-replay) are the snapshot HOLDER's
//!   job at join time (`MeshAuthorizer::authorize_snapshot`) — the only referee that
//!   knows the group's current admins and revocation set. So a scan that passes locally
//!   can still be refused by the holder; that is by design (DASHBOARD/IDENTITY_RBAC_SPEC:
//!   "peers verify before serving snapshots").
//!
//! Panic-free (no `unwrap` on the FFI-reachable path).

use serde::{Deserialize, Serialize};

use super::{Grant, GrantError, Role};

/// The QR payload an admin issues: a signed [`Grant`] plus the group identity and
/// the inviter's node id (the bootstrap peer the scanner dials to join).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantInvite {
    pub group_id: String,
    pub group_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_icon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_color: Option<String>,
    /// The issuing admin's node id — used as the gossip bootstrap peer on join.
    pub inviter_node_id: String,
    /// The signed capability grant carrying the joiner's role.
    pub grant: Grant,
}

impl GrantInvite {
    /// Encode to the QR payload (compact JSON). Panic-free for FFI paths.
    pub fn to_qr_payload(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Decode a grant-invite from its QR payload. Does NOT verify the grant.
    pub fn from_qr_payload(payload: &str) -> Result<GrantInvite, GrantError> {
        serde_json::from_str(payload).map_err(|_| GrantError::Malformed)
    }

    /// The role this invite grants.
    pub fn role(&self) -> Role {
        self.grant.role
    }
}

/// Why a local scan pre-check rejected a grant-invite QR.
///
/// These are the checks the SCANNER can make on its own. The remaining authoritative
/// checks (issuer-is-current-admin · revoked · replayed) belong to the snapshot holder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanError {
    /// The QR payload could not be decoded into a [`GrantInvite`].
    Malformed,
    /// The grant's signature does not verify (forged or tampered).
    BadSignature,
    /// `now >= expiry` — the grant has expired.
    Expired,
    /// The envelope's `group_id` does not match the signed grant's `group_id`.
    GroupMismatch,
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ScanError::Malformed => "grant QR is malformed",
            ScanError::BadSignature => "grant signature is invalid",
            ScanError::Expired => "grant has expired",
            ScanError::GroupMismatch => "grant group does not match the invite",
        };
        write!(f, "{s}")
    }
}
impl std::error::Error for ScanError {}

/// Issue a role-carrying grant QR — **admin-only**. Signs the grant (rejecting a
/// non-admin issuer with [`GrantError::NotAuthorized`] via [`Grant::issue`]) and packs
/// it into a [`GrantInvite`] the scanner can join from.
#[allow(clippy::too_many_arguments)]
pub fn issue_grant_qr(
    group_id: &str,
    group_name: &str,
    group_icon: Option<&str>,
    group_color: Option<&str>,
    role: Role,
    issuer_secret: &[u8; 32],
    inviter_node_id: &str,
    issued_at: u64,
    expiry: u64,
    nonce: &str,
    roster: &super::GroupRoster,
) -> Result<String, GrantError> {
    let grant = Grant::issue(group_id, role, issuer_secret, issued_at, expiry, nonce, roster)?;
    let invite = GrantInvite {
        group_id: group_id.to_string(),
        group_name: group_name.to_string(),
        group_icon: group_icon.map(String::from),
        group_color: group_color.map(String::from),
        inviter_node_id: inviter_node_id.to_string(),
        grant,
    };
    Ok(invite.to_qr_payload())
}

/// Decode + locally pre-verify a scanned grant-invite QR as-of `now`. Checks the
/// signature, expiry, and group consistency — everything the scanner can verify
/// without the group's admin roster. On success returns the invite ready to join;
/// the holder runs the authoritative issuer-admin / revocation / replay checks when
/// it serves the per-group snapshot.
pub fn scan_grant_qr_at(qr_payload: &str, now: u64) -> Result<GrantInvite, ScanError> {
    let invite = GrantInvite::from_qr_payload(qr_payload).map_err(|_| ScanError::Malformed)?;
    if invite.grant.group_id != invite.group_id {
        return Err(ScanError::GroupMismatch);
    }
    if !invite.grant.verify_signature() {
        return Err(ScanError::BadSignature);
    }
    if now >= invite.grant.expiry {
        return Err(ScanError::Expired);
    }
    Ok(invite)
}
