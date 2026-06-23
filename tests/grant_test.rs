//! Mesh-half identity tests (IDENTITY_RBAC_SPEC §"Named tests" → mesh half).
//!
//! These exercise the **XaeroID-signed capability grant** primitive that lives in
//! `cyan_backend::identity`: issue (admin authority), verify (signature · issuer-is-admin ·
//! expiry · anti-replay nonce · revocation) and the QR encode→decode→verify roundtrip.
//!
//! Pure-logic tests (no mesh, no DB) — they use in-memory Ed25519 keypairs (xaeroID's
//! primitives, via the `identity` module's helpers) and a deterministic clock (`verify_at`)
//! so expiry is not wall-clock-flaky. The mesh-enforcement test
//! (`mesh_write_rejected_without_valid_grant`) lives in `tests/substrate_identity.rs`.

use cyan_backend::identity::{pubkey_hex, Grant, GrantVerifier, GroupRoster, Role, VerifyError};

const GROUP: &str = "group-acme-1234";

/// A deterministic "now" so expiry assertions never race the wall clock.
const NOW: u64 = 1_700_000_000;

/// Distinct, deterministic secret keys (any 32 bytes is a valid Ed25519 secret).
fn secret(seed: u8) -> [u8; 32] {
    [seed; 32]
}

/// A roster where `admin_secret`'s pubkey is an Admin of `GROUP`.
fn roster_with_admin(admin_secret: &[u8; 32]) -> GroupRoster {
    let mut roster = GroupRoster::new();
    roster.set_role(GROUP, &pubkey_hex(admin_secret), Role::Admin);
    roster
}

#[test]
fn admin_issues_grant_member_verifies() {
    let admin = secret(1);
    let roster = roster_with_admin(&admin);

    let grant = Grant::issue(
        GROUP,
        Role::Member,
        &admin,
        NOW,
        NOW + 3600, // expires in 1h
        "nonce-A",
        &roster,
    )
    .expect("admin is authorized to issue");

    let mut verifier = GrantVerifier::new(roster);
    let role = verifier
        .verify_at(&grant, NOW + 60)
        .expect("a freshly-issued grant from a current admin verifies");
    assert_eq!(role, Role::Member, "verify returns the granted role");
}

#[test]
fn non_admin_cannot_issue_grant() {
    let admin = secret(1);
    let stranger = secret(9); // not in the roster at all
    let roster = roster_with_admin(&admin);

    let err = Grant::issue(
        GROUP,
        Role::Member,
        &stranger,
        NOW,
        NOW + 3600,
        "nonce-B",
        &roster,
    )
    .expect_err("a non-admin must not be able to sign a grant for the group");
    // The authority check is what fails (not signing itself).
    assert!(
        matches!(err, cyan_backend::identity::GrantError::NotAuthorized),
        "expected NotAuthorized, got {err:?}"
    );
}

#[test]
fn expired_grant_rejected() {
    let admin = secret(1);
    let roster = roster_with_admin(&admin);

    let grant = Grant::issue(
        GROUP,
        Role::Member,
        &admin,
        NOW,
        NOW + 100, // short-lived
        "nonce-C",
        &roster,
    )
    .expect("admin issues");

    let mut verifier = GrantVerifier::new(roster);
    let err = verifier
        .verify_at(&grant, NOW + 101) // one second past expiry
        .expect_err("an expired grant must be rejected");
    assert_eq!(err, VerifyError::Expired);
}

#[test]
fn replayed_nonce_rejected() {
    let admin = secret(1);
    let roster = roster_with_admin(&admin);

    let grant = Grant::issue(GROUP, Role::Member, &admin, NOW, NOW + 3600, "nonce-D", &roster)
        .expect("admin issues");

    let mut verifier = GrantVerifier::new(roster);
    // First scan: accepted.
    verifier
        .verify_at(&grant, NOW + 1)
        .expect("first presentation of an unseen nonce verifies");
    // Second scan of the SAME grant (same nonce): anti-replay must reject.
    let err = verifier
        .verify_at(&grant, NOW + 2)
        .expect_err("a replayed nonce must be rejected");
    assert_eq!(err, VerifyError::ReplayedNonce);
}

#[test]
fn revoked_grant_rejected() {
    let admin = secret(1);
    let roster = roster_with_admin(&admin);

    let grant = Grant::issue(GROUP, Role::Member, &admin, NOW, NOW + 3600, "nonce-E", &roster)
        .expect("admin issues");

    let mut verifier = GrantVerifier::new(roster);
    // Admin revokes the grant (tombstone by group_id + nonce).
    verifier.revoke(GROUP, "nonce-E");

    let err = verifier
        .verify_at(&grant, NOW + 1)
        .expect_err("a revoked grant must be rejected even if otherwise valid");
    assert_eq!(err, VerifyError::Revoked);
}

#[test]
fn forged_signature_rejected() {
    // Defense-in-depth: a grant claiming an admin issuer but signed by someone else
    // (or tampered after signing) must fail signature verification.
    let admin = secret(1);
    let attacker = secret(7);
    let roster = roster_with_admin(&admin);

    // Attacker self-signs but stamps the admin's pubkey as issuer (forge the issuer field).
    let mut forged = Grant::issue_unchecked(
        GROUP,
        Role::Owner,
        &attacker,
        NOW,
        NOW + 3600,
        "nonce-F",
    );
    forged.issued_by = pubkey_hex(&admin);

    let mut verifier = GrantVerifier::new(roster);
    let err = verifier
        .verify_at(&forged, NOW + 1)
        .expect_err("a forged/tampered signature must be rejected");
    assert_eq!(err, VerifyError::BadSignature);
}

#[test]
fn qr_payload_roundtrips_and_verifies() {
    let admin = secret(1);
    let roster = roster_with_admin(&admin);

    let grant = Grant::issue(GROUP, Role::Viewer, &admin, NOW, NOW + 3600, "nonce-G", &roster)
        .expect("admin issues");

    // Encode to the QR payload, decode it back — must be byte-for-byte the same grant…
    let qr = grant.to_qr_payload();
    let decoded = Grant::from_qr_payload(&qr).expect("QR payload decodes");
    assert_eq!(decoded.group_id, grant.group_id);
    assert_eq!(decoded.role, grant.role);
    assert_eq!(decoded.issued_by, grant.issued_by);
    assert_eq!(decoded.nonce, grant.nonce);
    assert_eq!(decoded.signature, grant.signature);

    // …and the decoded grant still verifies against the issuer's authority.
    let mut verifier = GrantVerifier::new(roster);
    let role = verifier
        .verify_at(&decoded, NOW + 1)
        .expect("a grant decoded from its QR payload verifies");
    assert_eq!(role, Role::Viewer);
}
