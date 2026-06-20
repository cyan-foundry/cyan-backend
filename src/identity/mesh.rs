//! Mesh-write enforcement — the offline, integrity-only half of RBAC.
//!
//! There is no referee offline, so "who may write/administer a group" is enforced by every peer
//! checking a **XaeroID-signed capability grant** before it accepts a write. [`MeshAuthorizer`] is
//! one node's view of that: which groups it enforces, and which peers have presented a valid grant
//! (and at what role). It wraps a [`GrantVerifier`] so presentation reuses the same signature /
//! issuer-admin / expiry / replay / revocation checks as QR scanning.
//!
//! **Fail-open by default.** A group is only enforced after [`MeshAuthorizer::enforce_group`].
//! Until then every write is allowed — so wiring this into the receive path does **not** change
//! shipping behavior for groups that have not opted into grant enforcement (the "seam, not a
//! rewrite" rule). Protects *who can write*; it cannot stop an already-synced member from reading.

use std::collections::{HashMap, HashSet};

use super::{Grant, GrantVerifier, GroupRoster, Role, VerifyError};

/// The outcome of a mesh-write authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteDecision {
    /// The write is accepted (group not enforced, or peer holds a write-capable grant).
    Allow,
    /// The write is refused.
    Deny(DenyReason),
}

impl WriteDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, WriteDecision::Allow)
    }
}

/// Why a mesh write was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DenyReason {
    /// The peer has not presented a valid grant for this (enforced) group.
    NoGrant,
    /// The peer presented a grant, but its role is read-only (Viewer/Guest).
    InsufficientRole,
}

/// Why a snapshot request was refused (the join-time read gate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotDenial {
    /// The group is enforced but the joiner presented no grant.
    NoGrant,
    /// The grant was for a different group than the one requested.
    WrongGroup,
    /// The grant failed verification (signature · issuer-admin · expiry · replay · revocation).
    Verify(VerifyError),
}

/// A recorded authorization for one peer in one group.
#[derive(Debug, Clone)]
struct Authorization {
    role: Role,
    /// The grant nonce that authorized this peer — so a later revocation of that nonce can
    /// drop the authorization.
    nonce: String,
}

/// One node's mesh-write authority state across all the groups it participates in.
#[derive(Debug)]
pub struct MeshAuthorizer {
    verifier: GrantVerifier,
    /// Groups for which grant enforcement is ON. A group not in this set is fail-open.
    enforced: HashSet<String>,
    /// `group_id → (peer_id → authorization)`. `peer_id` is the transport identity the write
    /// arrives from (the iroh node id on the gossip path).
    authorized: HashMap<String, HashMap<String, Authorization>>,
}

impl Default for MeshAuthorizer {
    fn default() -> Self {
        Self::new()
    }
}

impl MeshAuthorizer {
    /// An authorizer with an empty roster — fail-open for every group until `enforce_group`.
    pub fn new() -> Self {
        Self::with_roster(GroupRoster::new())
    }

    /// An authorizer whose verifier trusts `roster`'s admins.
    pub fn with_roster(roster: GroupRoster) -> Self {
        MeshAuthorizer {
            verifier: GrantVerifier::new(roster),
            enforced: HashSet::new(),
            authorized: HashMap::new(),
        }
    }

    /// Turn ON grant enforcement for `group_id`. After this, a peer must have presented a valid
    /// write-capable grant for its writes to this group to be accepted.
    pub fn enforce_group(&mut self, group_id: &str) {
        self.enforced.insert(group_id.to_string());
    }

    /// Whether `group_id` is currently enforced.
    pub fn is_enforced(&self, group_id: &str) -> bool {
        self.enforced.contains(group_id)
    }

    /// Seed/update an admin in the authority roster (who may issue grants for a group).
    /// In production this is fed from group state; in tests it is set explicitly.
    pub fn set_admin(&mut self, group_id: &str, pubkey_hex: &str, role: Role) {
        self.verifier.roster_mut().set_role(group_id, pubkey_hex, role);
    }

    /// A peer presents a grant. Verifies it (signature · issuer-admin · expiry · replay ·
    /// revocation); on success records `peer_id` as authorized at the granted role and returns it.
    /// On failure nothing is recorded and the peer remains unauthorized.
    pub fn present_grant(&mut self, peer_id: &str, grant: &Grant) -> Result<Role, VerifyError> {
        let role = self.verifier.verify(grant)?;
        self.authorized
            .entry(grant.group_id.clone())
            .or_default()
            .insert(
                peer_id.to_string(),
                Authorization {
                    role,
                    nonce: grant.nonce.clone(),
                },
            );
        Ok(role)
    }

    /// As [`present_grant`], but verifying as-of `now` (deterministic clock for tests).
    pub fn present_grant_at(
        &mut self,
        peer_id: &str,
        grant: &Grant,
        now: u64,
    ) -> Result<Role, VerifyError> {
        let role = self.verifier.verify_at(grant, now)?;
        self.authorized
            .entry(grant.group_id.clone())
            .or_default()
            .insert(
                peer_id.to_string(),
                Authorization {
                    role,
                    nonce: grant.nonce.clone(),
                },
            );
        Ok(role)
    }

    /// Decide whether to serve `group_id`'s snapshot to a joining `peer_id`. This is the
    /// **join-time read gate** — the security property is "a peer only pulls a group it holds a
    /// valid grant for", which together with the per-group snapshot build means zero leakage of
    /// the holder's other groups.
    ///
    /// Fail-open if the group is not enforced (un-enforced groups serve snapshots exactly as
    /// before). If enforced, the joiner must present a grant FOR THIS group that verifies; on
    /// success the grant's nonce is consumed (so a replay of the same QR is rejected) and the
    /// peer is recorded as authorized at its granted role (so its subsequent mesh writes pass
    /// `authorize_write` without re-presenting). Any role — including read-only Viewer/Guest —
    /// is allowed to read a snapshot.
    pub fn authorize_snapshot(
        &mut self,
        peer_id: &str,
        group_id: &str,
        grant: Option<&Grant>,
    ) -> Result<Role, SnapshotDenial> {
        if !self.is_enforced(group_id) {
            return Ok(Role::Member);
        }
        let grant = grant.ok_or(SnapshotDenial::NoGrant)?;
        if grant.group_id != group_id {
            return Err(SnapshotDenial::WrongGroup);
        }
        self.present_grant(peer_id, grant)
            .map_err(SnapshotDenial::Verify)
    }

    /// Revoke a grant by `(group_id, nonce)` — tombstones it in the verifier AND drops any peer
    /// this node had authorized via that nonce (so an already-authorized peer loses write access).
    pub fn revoke(&mut self, group_id: &str, nonce: &str) {
        self.verifier.revoke(group_id, nonce);
        if let Some(peers) = self.authorized.get_mut(group_id) {
            peers.retain(|_, auth| auth.nonce != nonce);
        }
    }

    /// The role this node has recorded for `peer_id` in `group_id`, if any.
    pub fn role_of_peer(&self, group_id: &str, peer_id: &str) -> Option<Role> {
        self.authorized
            .get(group_id)
            .and_then(|m| m.get(peer_id))
            .map(|a| a.role)
    }

    /// Decide whether `peer_id` may write to `group_id`. Fail-open if the group is not enforced;
    /// otherwise the peer must hold a recorded, write-capable (Owner/Admin/Member) grant.
    pub fn authorize_write(&self, group_id: &str, peer_id: &str) -> WriteDecision {
        if !self.is_enforced(group_id) {
            return WriteDecision::Allow;
        }
        match self.role_of_peer(group_id, peer_id) {
            Some(role) if role.can_write() => WriteDecision::Allow,
            Some(_) => WriteDecision::Deny(DenyReason::InsufficientRole),
            None => WriteDecision::Deny(DenyReason::NoGrant),
        }
    }
}
