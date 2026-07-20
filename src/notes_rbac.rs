//! A2 — the notes write-door RBAC matrix (Build Package A, DETAILED §6).
//!
//! ONE scope-major, kind-agnostic table: `note_write_allowed(tier, scope, kind,
//! scope_anchor, node_id)`. The engine matrix is the enforcement source of truth
//! (SYN-12); iOS's kind×scope table is a stricter UX layer on top, never security.
//!
//! **Cited precedent (D-A2.23):** the check sequence is exactly `request_unlock`'s
//! (`workflow.rs::request_unlock`) — (1) a verified grant/session, (2) tenant
//! equality, (3) `role.level() >= min.level()` (the same `RolePolicy` algebra) —
//! the LIVE org-grant-verified, tenant-checked, Admin-minimum gate. Two deliberate
//! deltas, per-surface policy (not contradiction): `request_unlock` verifies a
//! grant PER CALL, this module samples the INSTALLED session (§7's `SSO_SESSION`
//! global, `sso_grant::installed_tier`); `request_unlock` hard-fails without a
//! grant, this module FAIL-OPENS without an installed session (mirrors the mesh
//! fail-open posture — `MeshAuthorizer::authorize_write` on an un-enforced group).
//! `request_unlock` itself is NOT rewired.
//!
//! **Entitlement is NOT an input (D-A2.24):** the commercial axis
//! (`Entitlement{plan,seats,features}`) gates cloud features; notes writes are
//! local-first data — NO entitlement/seat check here, deliberately.
//!
//! **Honesty:** ALL rows are NEW enforcement — before A2 the write door validated
//! vocab only. Fail-open preserves today's literal behavior: no installed session
//! ⇒ EVERY check passes, the `user` self-anchor row included (the frozen pre-A2
//! sovereignty tests pin it); with a session installed, a `user` write requires
//! `scope_anchor == node_id` at ANY tier.
//!
//! Authority state machine: `NoSession (fail-open) —install ok→ Active (enforce)
//! —exp+grace→ Expired (fail-open locally; mesh/lens still enforce) —re-install→
//! Active; sign_out → NoSession`.
//!
//! **Inbound (mesh) half — ORCH-10 / D-A2.22:** A1's TR-1 governs the DEFAULT
//! (mesh-open) inbound path — apply never drops unknown scopes/kinds; only the
//! pre-existing user-scope sovereign drop runs. Inside the opt-in ENFORCED-group
//! RBAC arm ONLY (BOTH lanes — the topic-actor gossip arm AND the snapshot apply,
//! or snapshot is a backdoor): a known scope checks the roster tier against the
//! row's min; an UNKNOWN scope DROPS with [`CHECK_UNKNOWN_SCOPE`] — an enforced
//! group chose strictness, and an unknown scope's policy tier is unknowable.

use cyan_identity::Role;

use crate::models::dto::note_scope_valid;

// ── The named checks (each emitted verbatim in the obs deny line) ──────────

pub const CHECK_TENANT_WRITE: &str = "CHECK_TENANT_WRITE";
pub const CHECK_GROUP_WRITE: &str = "CHECK_GROUP_WRITE";
pub const CHECK_ROLE_WRITE: &str = "CHECK_ROLE_WRITE";
pub const CHECK_PROJECT_WRITE: &str = "CHECK_PROJECT_WRITE";
pub const CHECK_BOARD_WRITE: &str = "CHECK_BOARD_WRITE";
pub const CHECK_PRODUCER_WRITE: &str = "CHECK_PRODUCER_WRITE";
pub const CHECK_USER_WRITE: &str = "CHECK_USER_WRITE";
/// The 9th named const (D-A2.22): used ONLY inside the opt-in enforced-group
/// inbound arm, on BOTH lanes — never on the local write door (the vocab check
/// rejects unknown scopes there BEFORE RBAC runs).
pub const CHECK_UNKNOWN_SCOPE: &str = "CHECK_UNKNOWN_SCOPE";

/// A structured deny: which named check failed, the tier it needed, the tier
/// held (`"none"` when no session/roster entry). `check` doubles as the
/// `NoteRejected.reason` and the obs `check=` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied {
    pub check: &'static str,
    pub needed: &'static str,
    pub held: String,
}

impl std::fmt::Display for Denied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} needed={} held={}", self.check, self.needed, self.held)
    }
}

/// The scope-major matrix row: `(check, minimum tier)`. `None` = the `user` row
/// (no tier minimum — a structural self-anchor equality instead).
fn matrix_row(scope: &str) -> Option<(&'static str, Option<Role>)> {
    match scope {
        "tenant" => Some((CHECK_TENANT_WRITE, Some(Role::Admin))),
        "group" => Some((CHECK_GROUP_WRITE, Some(Role::Admin))),
        // Role rules drive agentification = policy (Q4). Slug validity is A1's
        // job and runs BEFORE this check.
        "role" => Some((CHECK_ROLE_WRITE, Some(Role::Admin))),
        "project" => Some((CHECK_PROJECT_WRITE, Some(Role::Member))),
        "board" | "workflow" => Some((CHECK_BOARD_WRITE, Some(Role::Member))),
        // Behavior change on a LIVE scope, flagged (open question Q3).
        "producer" => Some((CHECK_PRODUCER_WRITE, Some(Role::Member))),
        "user" => Some((CHECK_USER_WRITE, None)),
        _ => None,
    }
}

fn held_str(tier: Option<Role>) -> String {
    tier.map(|t| t.as_str().to_string()).unwrap_or_else(|| "none".to_string())
}

/// The LOCAL write-door check (enforcement point 1). `scope_anchor` is the SCOPE
/// anchor — the caller passes the command's `board_id` FIELD, NEVER the
/// within-board `anchor_id` field/local (dto.rs `NoteDTO.board_id` vs
/// `anchor_id`; a local named `anchor_id` is in scope at the wiring site — do
/// not pass it). Kind-agnostic by design (`_kind` kept for the stated matrix
/// signature). Fail-open: `tier == None` (no installed session / expired past
/// grace) passes EVERY row — the pre-A2 literal behavior, sovereignty tests
/// included; the `user` self-anchor is enforced at any INSTALLED tier.
pub fn note_write_allowed(
    tier: Option<Role>,
    scope: &str,
    _kind: &str,
    scope_anchor: &str,
    node_id: &str,
) -> Result<(), Denied> {
    let Some((check, min)) = matrix_row(scope) else {
        // Unreachable from the local door (vocab check runs first); deny loudly
        // rather than fail-open on a scope with no policy row.
        return Err(Denied {
            check: CHECK_UNKNOWN_SCOPE,
            needed: "known scope",
            held: scope.to_string(),
        });
    };
    match min {
        // `user`: the self-anchor row — at ANY INSTALLED tier the anchor must be
        // this node. With NO session the row fail-opens like every other (the
        // frozen pre-A2 sovereignty tests pin that literal behavior; the
        // sovereign properties — never gossiped/snapshot — hold structurally on
        // the replication lanes regardless).
        None => match tier {
            None => Ok(()),
            Some(_) if scope_anchor == node_id => Ok(()),
            Some(t) => Err(Denied {
                check,
                needed: "self anchor",
                held: t.as_str().to_string(),
            }),
        },
        Some(min) => match tier {
            // Fail-open: no installed session ⇒ pass (today's literal behavior).
            None => Ok(()),
            Some(t) if t.level() >= min.level() => Ok(()),
            Some(t) => Err(Denied {
                check,
                needed: min.as_str(),
                held: t.as_str().to_string(),
            }),
        },
    }
}

// ── Inbound (mesh) half — the opt-in enforced-group arm, BOTH lanes ─────────

/// What the inbound note-apply should do with one row (topic-actor gossip arm
/// AND snapshot apply — the two lanes share this one verdict fn so snapshot can
/// never become a backdoor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundVerdict {
    /// Upsert the row (LWW, idempotent) — the TR-1 convergence path.
    Apply,
    /// The pre-existing sovereign drop: a foreign `user`-scope row. Runs FIRST,
    /// enforced or not.
    DropSovereign,
    /// Enforced-group deny: drop the row + obs `note_apply_denied`.
    Deny(Denied),
}

/// Decide one inbound note row by its SCOPE (the matrix is kind-agnostic).
/// `enforced` = the group opted into grant enforcement; `roster_tier` = the
/// AUTHOR's roster tier in that group (`GroupRoster::role_of` mapped — Admin+ ⇔
/// can_administer, Member+ ⇔ can_write), `None` when the roster has no entry
/// for the author.
///
/// Un-enforced groups apply everything except user scope — exactly today
/// (TR-1, ORCH-10). Enforced groups: unknown scope ⇒ [`CHECK_UNKNOWN_SCOPE`]
/// drop; known scope ⇒ the matrix row's min vs the roster tier (a missing
/// roster entry denies — an enforced group is deny-by-default for authors it
/// cannot tier).
pub fn note_apply_verdict(enforced: bool, roster_tier: Option<Role>, scope: &str) -> InboundVerdict {
    // The sovereign drop wins FIRST, on both paths (T25b pins the order).
    if scope == "user" {
        return InboundVerdict::DropSovereign;
    }
    if !enforced {
        return InboundVerdict::Apply;
    }
    let row = if note_scope_valid(scope) { matrix_row(scope) } else { None };
    let Some((check, min)) = row else {
        return InboundVerdict::Deny(Denied {
            check: CHECK_UNKNOWN_SCOPE,
            needed: "known scope",
            held: scope.to_string(),
        });
    };
    match min {
        // `user` never reaches here (dropped sovereign above).
        None => InboundVerdict::DropSovereign,
        Some(min) => match roster_tier {
            Some(t) if t.level() >= min.level() => InboundVerdict::Apply,
            other => InboundVerdict::Deny(Denied {
                check,
                needed: min.as_str(),
                held: held_str(other),
            }),
        },
    }
}

/// The inbound-apply enforcement bundle both lanes thread (topic-actor gossip
/// arm + snapshot apply — one type, so snapshot can never become a backdoor).
/// `tier_of` maps an AUTHOR id to its roster tier for the applying group.
pub struct InboundEnforcement<'a> {
    pub enforced: bool,
    pub tier_of: &'a dyn Fn(&str) -> Option<Role>,
}

/// Run the verdict for one inbound row under an optional enforcement bundle
/// (`None` = the default mesh-open path — TR-1, user-drop only) and emit the
/// obs `note_apply_denied` line on a deny. Returns whether the row APPLIES.
pub fn inbound_note_applies(
    enforcement: Option<&InboundEnforcement<'_>>,
    note_id: &str,
    scope: &str,
    author_id: &str,
    lane: &str,
) -> bool {
    let (enforced, tier) = match enforcement {
        Some(e) => (e.enforced, (e.tier_of)(author_id)),
        None => (false, None),
    };
    match note_apply_verdict(enforced, tier, scope) {
        InboundVerdict::Apply => true,
        InboundVerdict::DropSovereign => {
            tracing::debug!("dropping inbound user-scoped note {note_id} (sovereign scope, {lane})");
            false
        }
        InboundVerdict::Deny(d) => {
            tracing::warn!(
                "obs note_apply_denied id={note_id} lane={lane} scope={scope} check={} needed={} held={}",
                d.check,
                d.needed,
                d.held
            );
            false
        }
    }
}

/// Map the MESH grant role (`crate::identity::Role`, the capability-grant
/// vocabulary) onto the org-RBAC tier this matrix speaks (`cyan_identity::Role`):
/// Admin+ ⇔ `can_administer`, Member+ ⇔ `can_write` (§6 enforcement point 2).
pub fn tier_from_mesh(role: crate::identity::Role) -> Role {
    match role {
        crate::identity::Role::Owner => Role::Owner,
        crate::identity::Role::Admin => Role::Admin,
        crate::identity::Role::Member => Role::Member,
        crate::identity::Role::Viewer => Role::Viewer,
        crate::identity::Role::Guest => Role::Guest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deny_check(r: Result<(), Denied>) -> &'static str {
        r.expect_err("expected deny").check
    }

    #[test]
    fn matrix_rows_enforce_min_tiers() {
        // Admin rows.
        for scope in ["tenant", "group", "role"] {
            assert!(note_write_allowed(Some(Role::Admin), scope, "constitution", "g1", "n1").is_ok());
            assert!(note_write_allowed(Some(Role::Owner), scope, "constitution", "g1", "n1").is_ok());
            assert!(note_write_allowed(Some(Role::Member), scope, "constitution", "g1", "n1").is_err());
        }
        assert_eq!(
            deny_check(note_write_allowed(Some(Role::Viewer), "tenant", "constitution", "g1", "n1")),
            CHECK_TENANT_WRITE
        );
        assert_eq!(
            deny_check(note_write_allowed(Some(Role::Member), "group", "constitution", "g1", "n1")),
            CHECK_GROUP_WRITE
        );
        assert_eq!(
            deny_check(note_write_allowed(Some(Role::Member), "role", "constitution", "g1", "n1")),
            CHECK_ROLE_WRITE
        );
        // Member rows.
        for scope in ["project", "board", "workflow", "producer"] {
            assert!(note_write_allowed(Some(Role::Member), scope, "editor-note", "b1", "n1").is_ok());
            assert!(note_write_allowed(Some(Role::Viewer), scope, "editor-note", "b1", "n1").is_err());
        }
        assert_eq!(
            deny_check(note_write_allowed(Some(Role::Viewer), "project", "editor-note", "w1", "n1")),
            CHECK_PROJECT_WRITE
        );
        assert_eq!(
            deny_check(note_write_allowed(Some(Role::Viewer), "producer", "editor-note", "p1", "n1")),
            CHECK_PRODUCER_WRITE
        );
    }

    #[test]
    fn user_scope_is_self_anchor_at_any_installed_tier() {
        assert!(note_write_allowed(None, "user", "editor-note", "node-1", "node-1").is_ok());
        assert!(note_write_allowed(Some(Role::Owner), "user", "editor-note", "node-1", "node-1").is_ok());
        assert!(note_write_allowed(Some(Role::Guest), "user", "editor-note", "node-1", "node-1").is_ok());
        let d = note_write_allowed(Some(Role::Owner), "user", "editor-note", "node-2", "node-1")
            .expect_err("foreign anchor denies at any installed tier");
        assert_eq!(d.check, CHECK_USER_WRITE);
        // No session ⇒ fail-open, user row included (frozen pre-A2 behavior).
        assert!(note_write_allowed(None, "user", "editor-note", "node-2", "node-1").is_ok());
    }

    #[test]
    fn no_session_fail_open_everywhere() {
        for scope in ["tenant", "group", "role", "project", "board", "workflow", "producer", "user"] {
            assert!(note_write_allowed(None, scope, "constitution", "any", "n1").is_ok());
        }
    }

    #[test]
    fn denied_carries_needed_and_held() {
        let d = note_write_allowed(Some(Role::Viewer), "tenant", "constitution", "g1", "n1")
            .expect_err("deny");
        assert_eq!(d.needed, "admin");
        assert_eq!(d.held, "viewer");
        assert_eq!(d.to_string(), "CHECK_TENANT_WRITE needed=admin held=viewer");
    }

    #[test]
    fn inbound_unenforced_applies_all_but_sovereign() {
        assert_eq!(note_apply_verdict(false, None, "board"), InboundVerdict::Apply);
        // TR-1: unknown scopes APPLY on the default path.
        assert_eq!(note_apply_verdict(false, None, "asset2"), InboundVerdict::Apply);
        assert_eq!(note_apply_verdict(false, None, "user"), InboundVerdict::DropSovereign);
    }

    #[test]
    fn inbound_enforced_checks_roster_and_drops_unknown_scope() {
        // Sovereign drop wins first, even enforced.
        assert_eq!(note_apply_verdict(true, Some(Role::Owner), "user"), InboundVerdict::DropSovereign);
        // Unknown scope drops with the 9th const.
        match note_apply_verdict(true, Some(Role::Owner), "asset2") {
            InboundVerdict::Deny(d) => assert_eq!(d.check, CHECK_UNKNOWN_SCOPE),
            v => panic!("expected unknown-scope deny, got {v:?}"),
        }
        // Roster Member writes board, not tenant.
        assert_eq!(note_apply_verdict(true, Some(Role::Member), "board"), InboundVerdict::Apply);
        match note_apply_verdict(true, Some(Role::Member), "tenant") {
            InboundVerdict::Deny(d) => assert_eq!(d.check, CHECK_TENANT_WRITE),
            v => panic!("expected tenant deny, got {v:?}"),
        }
        // No roster entry => deny (enforced groups are deny-by-default).
        match note_apply_verdict(true, None, "board") {
            InboundVerdict::Deny(d) => {
                assert_eq!(d.check, CHECK_BOARD_WRITE);
                assert_eq!(d.held, "none");
            }
            v => panic!("expected no-roster deny, got {v:?}"),
        }
    }

    #[test]
    fn mesh_tier_mapping_preserves_write_algebra() {
        assert!(tier_from_mesh(crate::identity::Role::Admin).level() >= Role::Admin.level());
        assert!(tier_from_mesh(crate::identity::Role::Member).level() >= Role::Member.level());
        assert!(tier_from_mesh(crate::identity::Role::Viewer).level() < Role::Member.level());
    }
}
