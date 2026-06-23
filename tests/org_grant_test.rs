//! W17 §A/§C — backend grant consumer against the **per-tenant Org XaeroID** key,
//! revocation enforcement, and group re-key consumption (IDENTITY_W16_W17_SPEC).
//!
//! These pin the consumer-side behaviors the backend owns on top of the shared
//! cyan-identity primitives:
//!   - an org-issued grant verifies against the tenant's pinned org key
//!     ([`SsoSession::from_org_token`]);
//!   - a legacy `"cyan-lens"` RSA grant is still accepted during the cutover;
//!   - a grant whose `xaero_pubkey` is on the org-signed revocation list is
//!     rejected even while otherwise valid + unexpired;
//!   - a revoked member is dropped from the next group epoch the device consumes.
//!
//! Deterministic seeds + explicit `now` (the FakeClock-equivalent seam) — no RNG,
//! no network, no broker. Tokens are `SecretString`, never logged.

use cyan_backend::group_rekey::GroupEpochStore;
use cyan_backend::sso_grant::SsoSession;
use cyan_identity::testing::{test_grant_minter, test_grant_verifier, test_delegate, test_org};
use cyan_identity::{
    Grant, GroupEpoch, OrgGrantMinter, OrgGrantVerifier, RevocationList, Role, SignedRevocationList,
};
use secrecy::SecretString;

const TENANT: &str = "acme";
const HOUR: u64 = 3_600;
const NOW: u64 = 1_700_000_000;
const PUBKEY: &str = "xaero-pub-device-1";
const OTHER_PUBKEY: &str = "xaero-pub-device-2";
const LEGACY_ISSUER: &str = "cyan-lens";

/// Mint an org-issued grant for `xaero_pubkey` under `org`, signed by a delegate
/// the org vouches for. Mirrors how the Lens broker mints with a delegate key.
fn mint_org_grant(
    org: &cyan_identity::OrgXaeroId,
    xaero_pubkey: &str,
    role: Role,
    groups: &[&str],
) -> SecretString {
    let delegate = test_delegate(2);
    let cert = org.issue_delegate(&delegate.public_bytes(), NOW, 30 * 24 * HOUR);
    let minter = OrgGrantMinter::new(delegate, cert).expect("delegate cert matches delegate");
    let grant = Grant::new(
        xaero_pubkey,
        TENANT,
        role,
        groups.iter().map(|g| g.to_string()).collect(),
        NOW,
        HOUR,
    );
    minter.mint(&grant).expect("mint org grant")
}

#[test]
fn grant_verifies_against_org_key() {
    let org = test_org(1);
    let token = mint_org_grant(&org, PUBKEY, Role::Member, &["eng", "ops"]);

    // The verifier pins THIS tenant to THIS org key.
    let verifier = OrgGrantVerifier::new(0).trust_org(TENANT, org.public());
    let session = SsoSession::from_org_token(&token, &verifier, PUBKEY, NOW + 10)
        .expect("org grant verifies against the pinned org key");
    assert_eq!(session.tenant(), TENANT);
    assert_eq!(session.role(), Role::Member);
    assert_eq!(session.groups(), &["eng".to_string(), "ops".to_string()]);

    // A verifier pinning a DIFFERENT org key rejects the same grant (no trust).
    let wrong = OrgGrantVerifier::new(0).trust_org(TENANT, test_org(9).public());
    assert!(
        SsoSession::from_org_token(&token, &wrong, PUBKEY, NOW + 10).is_err(),
        "a grant must NOT verify against another org's pinned key"
    );

    // Fail-closed: an unpinned tenant has no root of trust → rejected.
    let empty = OrgGrantVerifier::new(0);
    assert!(
        SsoSession::from_org_token(&token, &empty, PUBKEY, NOW + 10).is_err(),
        "no pinned org for the tenant must reject"
    );
}

#[test]
fn legacy_issuer_accepted() {
    let org = test_org(1);

    // A legacy RSA grant stamped with the global "cyan-lens" issuer.
    let minter = test_grant_minter(LEGACY_ISSUER);
    let grant = Grant::new(PUBKEY, TENANT, Role::Admin, vec!["eng".to_string()], NOW, HOUR);
    let legacy_token = minter.mint(&grant).expect("mint legacy grant");

    // An org verifier built `.with_legacy(..)` still accepts it during cutover.
    let verifier = OrgGrantVerifier::new(0)
        .trust_org(TENANT, org.public())
        .with_legacy(test_grant_verifier(LEGACY_ISSUER, 0));
    let session = SsoSession::from_org_token(&legacy_token, &verifier, PUBKEY, NOW + 10)
        .expect("legacy cyan-lens grant still accepted");
    assert_eq!(session.role(), Role::Admin);

    // A strict verifier (no legacy) refuses the legacy grant — ready to harden
    // once the tenant has fully cut over to org-issued grants.
    let strict = OrgGrantVerifier::new(0).trust_org(TENANT, org.public());
    assert!(
        SsoSession::from_org_token(&legacy_token, &strict, PUBKEY, NOW + 10).is_err(),
        "a strict (no-legacy) verifier must refuse a legacy cyan-lens grant"
    );
}

#[test]
fn revoked_pubkey_grant_rejected() {
    let org = test_org(1);
    let verifier = OrgGrantVerifier::new(0).trust_org(TENANT, org.public());

    // A perfectly valid, unexpired grant for the fired employee's device.
    let token = mint_org_grant(&org, PUBKEY, Role::Member, &["eng"]);
    // Sanity: with no revocation list, it verifies fine.
    assert!(SsoSession::from_org_token(&token, &verifier, PUBKEY, NOW + 10).is_ok());

    // The org publishes an org-signed revocation list naming that device pubkey.
    let mut list = RevocationList::new(TENANT, org.did(), 1);
    list.revoke_pubkey(PUBKEY, "deprovisioned", NOW);
    let signed = SignedRevocationList::sign(&org, list).expect("org signs the revocation list");

    // Now the SAME valid grant is rejected — even though it has not expired.
    assert!(
        SsoSession::from_org_token_checked(&token, &verifier, PUBKEY, NOW + 10, &signed).is_err(),
        "a revoked xaero_pubkey must be rejected even on an otherwise-valid grant"
    );

    // Control: a non-revoked device on the same list still verifies.
    let other = mint_org_grant(&org, OTHER_PUBKEY, Role::Member, &["eng"]);
    assert!(
        SsoSession::from_org_token_checked(&other, &verifier, OTHER_PUBKEY, NOW + 10, &signed)
            .is_ok(),
        "a device NOT on the revocation list must still verify"
    );
}

#[test]
fn revoked_member_stops_getting_new_epoch() {
    const ME: &str = "xaero-me";
    const FIRED: &str = "xaero-fired";
    let org = test_org(1);
    let mut store = GroupEpochStore::new();

    // Genesis epoch: both members are in.
    let genesis = GroupEpoch::genesis(
        "eng",
        vec![ME.to_string(), FIRED.to_string()],
        &SecretString::new("epoch-secret-0".to_string()),
    );
    assert!(store.apply(genesis.clone()), "genesis epoch is applied");
    assert!(store.includes("eng", ME));
    assert!(store.includes("eng", FIRED));
    assert_eq!(store.epoch_of("eng"), Some(0));

    // The org revokes the fired member and the group re-keys (broker-side); the
    // device consumes the new epoch, which excludes the revoked member.
    let mut list = RevocationList::new(TENANT, org.did(), 1);
    list.revoke_pubkey(FIRED, "deprovisioned", NOW);
    let next = genesis.rekey(&list, &SecretString::new("epoch-secret-1".to_string()));
    assert!(store.apply(next), "the re-keyed epoch supersedes genesis");

    assert_eq!(store.epoch_of("eng"), Some(1));
    assert!(store.includes("eng", ME), "an active member keeps access");
    assert!(
        !store.includes("eng", FIRED),
        "a revoked member is dropped from the new epoch (no post-rekey access)"
    );

    // Replay guard: re-applying the stale genesis epoch is ignored, so the fired
    // member cannot be slipped back into the roster.
    assert!(!store.apply(genesis), "a stale (lower) epoch is rejected");
    assert!(!store.includes("eng", FIRED));
    assert_eq!(store.epoch_of("eng"), Some(1));
}
