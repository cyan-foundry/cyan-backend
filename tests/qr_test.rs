//! Signed-grant QR tests (IDENTITY_RBAC_SPEC mesh half; STATUS_DASHBOARD_QR Part B).
//!
//! Drives the issue/scan logic layer that the `cyan_issue_grant_qr` / `cyan_scan_grant_qr`
//! FFI verbs wrap, plus the holder's `MeshAuthorizer` join-time read gate — the real
//! security path. Deterministic keypairs + an explicit clock; no DB, no mesh, no live deps.

use cyan_backend::identity::{
    issue_grant_qr, pubkey_hex, scan_grant_qr_at, GrantError, GrantInvite, GroupRoster,
    MeshAuthorizer, Role, ScanError, SnapshotDenial, VerifyError,
};

const GROUP: &str = "g-qr-001";
const NOW: u64 = 1_700_000_000;

/// Deterministic Ed25519 seed (matches the grant-test convention).
fn secret(seed: u8) -> [u8; 32] {
    [seed; 32]
}

fn roster_with_admin(admin_secret: &[u8; 32], role: Role) -> GroupRoster {
    let mut roster = GroupRoster::new();
    roster.set_role(GROUP, &pubkey_hex(admin_secret), role);
    roster
}

#[test]
fn qr_issue_requires_admin() {
    let admin = secret(1);
    let non_admin = secret(2);
    let roster = roster_with_admin(&admin, Role::Admin);

    // Admin issues a role-carrying grant QR → decodes back to a valid GrantInvite.
    let qr = issue_grant_qr(
        GROUP,
        "Sales",
        Some("folder.fill"),
        Some("#00AEEF"),
        Role::Member,
        &admin,
        "node-admin",
        NOW,
        NOW + 3600,
        "nonce-issue",
        &roster,
    )
    .expect("an admin is authorized to issue a grant QR");

    let invite = GrantInvite::from_qr_payload(&qr).expect("QR decodes to a GrantInvite");
    assert_eq!(invite.group_id, GROUP);
    assert_eq!(invite.role(), Role::Member);
    assert_eq!(invite.inviter_node_id, "node-admin");
    assert_eq!(invite.group_name, "Sales");
    assert!(invite.grant.verify_signature(), "issued grant is correctly signed");

    // A non-admin signing for the same group is refused (authority check).
    let err = issue_grant_qr(
        GROUP,
        "Sales",
        None,
        None,
        Role::Member,
        &non_admin,
        "node-attacker",
        NOW,
        NOW + 3600,
        "nonce-forge",
        &roster,
    )
    .expect_err("a non-admin must NOT be able to issue a grant");
    assert_eq!(err, GrantError::NotAuthorized);
}

#[test]
fn qr_scan_verifies_and_joins() {
    let admin = secret(1);
    let admin_pk = pubkey_hex(&admin);
    let roster = roster_with_admin(&admin, Role::Admin);

    // Far-future expiry so the holder's wall-clock verify accepts it (it is the
    // expiry/revocation checks we exercise elsewhere with an explicit clock).
    let qr = issue_grant_qr(
        GROUP,
        "Sales",
        None,
        None,
        Role::Member,
        &admin,
        "node-admin",
        NOW,
        u64::MAX,
        "nonce-join",
        &roster,
    )
    .expect("admin issues the join QR");

    // SCAN: local pre-verify (signature · expiry · group) → a joinable invite.
    let invite = scan_grant_qr_at(&qr, NOW + 60).expect("scan verifies the grant locally");
    assert_eq!(invite.group_id, GROUP);
    assert_eq!(invite.role(), Role::Member);
    assert_eq!(invite.inviter_node_id, "node-admin", "carries the bootstrap peer for the join");

    // JOIN: the snapshot HOLDER authorizes the per-group snapshot read with the
    // scanned grant — the authoritative issuer-admin/replay gate.
    let mut holder = MeshAuthorizer::new();
    holder.enforce_group(GROUP);
    holder.set_admin(GROUP, &admin_pk, Role::Admin);
    let role = holder
        .authorize_snapshot("joiner-peer", GROUP, Some(&invite.grant))
        .expect("holder serves the per-group snapshot to a valid grant holder");
    assert_eq!(role, Role::Member);

    // The joiner is now a recorded writer at its granted role.
    assert!(
        holder.authorize_write(GROUP, "joiner-peer").is_allowed(),
        "a granted Member may write after presenting its grant"
    );

    // A replay of the same grant (same nonce) is rejected by the holder.
    let replay = holder.authorize_snapshot("joiner-peer-2", GROUP, Some(&invite.grant));
    assert_eq!(replay, Err(SnapshotDenial::Verify(VerifyError::ReplayedNonce)));
}

#[test]
fn expired_or_revoked_qr_rejected() {
    let admin = secret(1);
    let admin_pk = pubkey_hex(&admin);
    let roster = roster_with_admin(&admin, Role::Admin);

    // ── Expired: the scanner rejects it locally (explicit clock past expiry). ──
    let short = issue_grant_qr(
        GROUP,
        "Sales",
        None,
        None,
        Role::Member,
        &admin,
        "node-admin",
        NOW,
        NOW + 10,
        "nonce-expired",
        &roster,
    )
    .expect("issue a short-lived grant");
    let scan = scan_grant_qr_at(&short, NOW + 100);
    assert_eq!(scan, Err(ScanError::Expired), "an expired grant is rejected at scan time");

    // ── Revoked: the holder rejects it at the snapshot gate after a tombstone. ──
    let valid = issue_grant_qr(
        GROUP,
        "Sales",
        None,
        None,
        Role::Member,
        &admin,
        "node-admin",
        NOW,
        u64::MAX,
        "nonce-revoked",
        &roster,
    )
    .expect("issue a valid grant");
    let invite = scan_grant_qr_at(&valid, NOW + 60).expect("scan accepts the un-revoked grant");

    let mut holder = MeshAuthorizer::new();
    holder.enforce_group(GROUP);
    holder.set_admin(GROUP, &admin_pk, Role::Admin);
    holder.revoke(GROUP, "nonce-revoked");

    let decision = holder.authorize_snapshot("joiner-peer", GROUP, Some(&invite.grant));
    assert_eq!(
        decision,
        Err(SnapshotDenial::Verify(VerifyError::Revoked)),
        "a revoked grant is refused the per-group snapshot"
    );
    // And it confers no write capability.
    assert!(!holder.authorize_write(GROUP, "joiner-peer").is_allowed());
}

/// MESH_HARDENING §2.2 — the additive `inviter_addr` field (the inviter's full resolvable
/// `EndpointAddr`, serialized) round-trips through the QR payload and defaults to absent on the
/// legacy/pure path. This is the payload half of the QR seed source; the engine half (seeding it
/// forms a neighbor) is `substrate_mesh_seed::qr_join_forms_neighbor_via_seeded_addr`.
#[test]
fn qr_carries_optional_inviter_addr() {
    let admin = secret(1);
    let roster = roster_with_admin(&admin, Role::Admin);
    let qr = issue_grant_qr(
        GROUP, "Sales", None, None, Role::Member, &admin, "node-admin",
        NOW, NOW + 3600, "nonce-addr", &roster,
    )
    .expect("issue a grant QR");

    // Legacy/pure issue path leaves the address absent (drop-in, unchanged QR shape).
    let mut invite = GrantInvite::from_qr_payload(&qr).expect("decode invite");
    assert_eq!(invite.inviter_addr, None, "addr defaults to absent");

    // Stamp a serialized EndpointAddr (what the issuing device does) and round-trip it.
    let addr_json = r#"{"id":"ed25519pubkeyhex","addrs":[]}"#.to_string();
    invite.inviter_addr = Some(addr_json.clone());
    let round = GrantInvite::from_qr_payload(&invite.to_qr_payload()).expect("re-decode");
    assert_eq!(round.inviter_addr, Some(addr_json), "inviter_addr survives the QR round-trip");
    assert_eq!(round.inviter_node_id, "node-admin", "existing fields unaffected");
}
