//! R12 D2/E1 — workflow deploy/lock lifecycle + org-XaeroID-grant-gated unlock.
//!
//! D2: a board's workflow state (deployed + dashboard_available) is the engine-side support
//! state iOS gates the board face on (dashboard vs editor). E1: a deployed workflow is LOCKED,
//! and unlocking it mid-flight requires an **org grant (W17)** — the org signing key is the
//! approval authority, not an ad-hoc flag. These assert directly on `storage::*` (process-
//! global DB) and exercise the full request → org-grant-check → unlock round-trip with the
//! shared cyan-identity primitives. Deterministic seeds + explicit `now`; no RNG, no network.

mod support;

use cyan_backend::storage;
use cyan_backend::workflow;
use cyan_identity::testing::{test_delegate, test_org};
use cyan_identity::{
    Grant, OrgGrantMinter, OrgGrantVerifier, OrgXaeroId, RevocationList, Role, SignedRevocationList,
};
use secrecy::SecretString;

const TENANT: &str = "acme";
const HOUR: u64 = 3_600;
const NOW: u64 = 1_700_000_000;
const ADMIN_PK: &str = "xaero-admin-approver";
const MEMBER_PK: &str = "xaero-member";

/// Mint an org-issued grant for `xaero_pubkey` under `org`, signed by a vouched delegate —
/// mirrors how the Lens broker mints (see org_grant_test).
fn mint_org_grant(org: &OrgXaeroId, xaero_pubkey: &str, role: Role) -> SecretString {
    let delegate = test_delegate(3);
    let cert = org.issue_delegate(&delegate.public_bytes(), NOW, 30 * 24 * HOUR);
    let minter = OrgGrantMinter::new(delegate, cert).expect("delegate cert matches delegate");
    let grant = Grant::new(
        xaero_pubkey,
        TENANT,
        role,
        vec!["eng".to_string()],
        NOW,
        HOUR,
    );
    minter.mint(&grant).expect("mint org grant")
}

/// D2: deploying a workflow records the support state iOS gates the face on — deployed, with a
/// dashboard available, and locked. An undeployed board reads the default authoring state.
#[test]
fn deploy_sets_face_gating_state() {
    support::ensure_db();
    let board = support::unique_group_id();

    // Default (no deployment) = authoring: editable, unlocked, no dashboard.
    let before = storage::workflow_state_get(&board);
    assert!(!before.deployed && !before.locked && !before.dashboard_available);

    let state = workflow::mark_deployed(&board, /*dashboard_available*/ true, NOW as i64)
        .expect("mark deployed");
    assert!(state.deployed, "deployed");
    assert!(state.dashboard_available, "dashboard available → iOS shows the dashboard face");
    assert!(state.locked, "a deployed workflow is locked for editing");
    // Persisted (a fresh read sees the same).
    assert_eq!(storage::workflow_state_get(&board), state);
}

/// E1: a deployed workflow is LOCKED; an unlock is approved ONLY by a valid org-XaeroID grant
/// with Admin authority for the board's tenant. Every other path leaves it locked.
#[test]
fn unlock_requires_org_admin_grant() {
    support::ensure_db();
    let org = test_org(1);
    let verifier = OrgGrantVerifier::new(0).trust_org(TENANT, org.public());

    let board = support::unique_group_id();
    workflow::mark_deployed(&board, false, NOW as i64).expect("deploy");
    assert!(storage::workflow_state_get(&board).locked, "deployed → locked");

    // 1) No root of trust for the tenant → rejected, stays locked.
    let admin_token = mint_org_grant(&org, ADMIN_PK, Role::Admin);
    let untrusted = OrgGrantVerifier::new(0); // pins no org
    assert!(
        workflow::request_unlock(&board, TENANT, &admin_token, &untrusted, ADMIN_PK, NOW + 10, None)
            .is_err(),
        "an unverifiable grant must NOT unlock"
    );
    assert!(storage::workflow_state_get(&board).locked, "still locked after a bad grant");

    // 2) A valid grant but only Member authority → below the approval bar, stays locked.
    let member_token = mint_org_grant(&org, MEMBER_PK, Role::Member);
    assert!(
        workflow::request_unlock(&board, TENANT, &member_token, &verifier, MEMBER_PK, NOW + 10, None)
            .is_err(),
        "a non-admin grant must NOT unlock"
    );
    assert!(storage::workflow_state_get(&board).locked, "still locked after a member grant");

    // 3) A valid grant for the WRONG tenant → does not own this board, stays locked.
    assert!(
        workflow::request_unlock(&board, "other-tenant", &admin_token, &verifier, ADMIN_PK, NOW + 10, None)
            .is_err(),
        "a grant scoped to another tenant must NOT unlock this board"
    );
    assert!(storage::workflow_state_get(&board).locked, "still locked for the wrong tenant");

    // 4) A valid org Admin grant for THIS tenant → approved, unlocked.
    let unlocked = workflow::request_unlock(&board, TENANT, &admin_token, &verifier, ADMIN_PK, NOW + 10, None)
        .expect("a valid org admin grant unlocks");
    assert!(!unlocked.locked, "the org grant cleared the lock");
    assert!(unlocked.deployed, "still deployed — only the edit lock is lifted");
    assert!(!storage::workflow_state_get(&board).locked, "unlock persisted");
}

/// E1/W17 §C: a revoked approver cannot unlock — even holding an otherwise-valid, unexpired
/// admin grant — once the org publishes a signed revocation list naming their device.
#[test]
fn revoked_approver_cannot_unlock() {
    support::ensure_db();
    let org = test_org(1);
    let verifier = OrgGrantVerifier::new(0).trust_org(TENANT, org.public());

    let board = support::unique_group_id();
    workflow::mark_deployed(&board, false, NOW as i64).expect("deploy");

    let admin_token = mint_org_grant(&org, ADMIN_PK, Role::Admin);

    // The org revokes the approver's device pubkey and signs the list.
    let mut list = RevocationList::new(TENANT, org.did(), 1);
    list.revoke_pubkey(ADMIN_PK, "deprovisioned", NOW);
    let signed = SignedRevocationList::sign(&org, list).expect("org signs revocation list");

    assert!(
        workflow::request_unlock(&board, TENANT, &admin_token, &verifier, ADMIN_PK, NOW + 10, Some(&signed))
            .is_err(),
        "a revoked approver must NOT unlock even with a live admin grant"
    );
    assert!(storage::workflow_state_get(&board).locked, "stays locked under revocation");
}
