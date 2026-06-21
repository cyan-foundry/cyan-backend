//! Round 9 / W16 — backend SSO **session grant** consumer (offline-first).
//!
//! The device side of the W15 front door. These pin the locked behaviors:
//!   - sign-in seeds the device into the grant's `groups` (the group-join seam);
//!   - the grant's `role` is enforced through the shared `RolePolicy`;
//!   - a grant past `exp + grace` ⇒ re-auth required, but local data stays
//!     READABLE (graceful, per X-CUT);
//!   - a cached grant verifies fully OFFLINE (signature + XaeroID binding + grace)
//!     with NO broker reachable.
//!
//! Grants are minted/verified with the embedded cyan-identity RS256 test keypair
//! (the `testing` feature) bound to a XaeroID pubkey; tokens are `SecretString`
//! and never logged. This is the SSO session grant — distinct from the mesh-half
//! Ed25519 capability grant exercised in `tests/grant_test.rs`.

#![allow(clippy::unwrap_used)] // Mutex unwraps live in the RecordingJoiner test mock's impl methods (not #[test] fns), so clippy.toml's allow-unwrap-in-tests doesn't reach them; this is test-support code.

use std::sync::Mutex;

use async_trait::async_trait;
use cyan_backend::sso_grant::{sign_in, GrantCache, GroupJoiner, SignIn, SsoSession};
use cyan_identity::testing::{test_grant_minter, test_grant_verifier};
use cyan_identity::{Action, Grant, Resource, Role, SideEffect};
use secrecy::SecretString;

const ISS: &str = "cyan-lens";
const HOUR: u64 = 3_600;
const NOW: u64 = 1_700_000_000;
const PUBKEY: &str = "xaero-pub-device-1";

/// A `GroupJoiner` fake that records every (tenant, group, pubkey) join — the
/// in-process stand-in for the mesh `JoinGroup` path. Idempotent like the mesh.
#[derive(Default)]
struct RecordingJoiner {
    joins: Mutex<Vec<(String, String, String)>>,
}

impl RecordingJoiner {
    fn new() -> Self {
        Self::default()
    }
    fn groups(&self) -> Vec<String> {
        let mut g: Vec<String> = self
            .joins
            .lock()
            .unwrap()
            .iter()
            .map(|(_, group, _)| group.clone())
            .collect();
        g.sort();
        g.dedup();
        g
    }
    fn joined(&self, tenant: &str, group: &str) -> bool {
        self.joins
            .lock()
            .unwrap()
            .iter()
            .any(|(t, g, _)| t == tenant && g == group)
    }
}

#[async_trait]
impl GroupJoiner for RecordingJoiner {
    async fn join(&self, tenant: &str, group: &str, xaero_pubkey: &str) -> anyhow::Result<()> {
        self.joins.lock().unwrap().push((
            tenant.to_string(),
            group.to_string(),
            xaero_pubkey.to_string(),
        ));
        Ok(())
    }
}

/// Mint a signed grant bound to `PUBKEY` for `tenant`/`role`/`groups`, valid for
/// `[iat, iat+ttl]`.
fn mint(tenant: &str, role: Role, groups: &[&str], iat: u64, ttl: u64) -> SecretString {
    let minter = test_grant_minter(ISS);
    let grant = Grant::new(
        PUBKEY,
        tenant,
        role,
        groups.iter().map(|g| g.to_string()).collect(),
        iat,
        ttl,
    );
    minter.mint(&grant).expect("mint grant")
}

#[tokio::test]
async fn signin_seeds_grant_groups() {
    let token = mint("acme", Role::Member, &["eng", "design", "ops"], NOW, HOUR);
    let verifier = test_grant_verifier(ISS, 0);
    let joiner = RecordingJoiner::new();

    let result = sign_in(&token, &verifier, PUBKEY, NOW + 10, &joiner)
        .await
        .expect("sign-in succeeds");

    assert!(result.is_active(), "a valid grant yields an active session");

    // The device was seeded into EVERY granted group, tenant-scoped.
    assert_eq!(joiner.groups(), vec!["design", "eng", "ops"]);
    assert!(joiner.joined("acme", "eng"));
    assert!(!joiner.joined("other-tenant", "eng"), "joins are tenant-scoped");

    // Re-running sign-in re-seeds the same groups (idempotent on the mesh side).
    let _ = sign_in(&token, &verifier, PUBKEY, NOW + 20, &joiner)
        .await
        .expect("re-signin");
    assert_eq!(joiner.groups(), vec!["design", "eng", "ops"]);
}

#[tokio::test]
async fn role_enforced_from_grant() {
    // A Member grant: may run a workflow, may NOT install a plugin (Admin-only).
    let token = mint("acme", Role::Member, &["eng"], NOW, HOUR);
    let verifier = test_grant_verifier(ISS, 0);
    let session = SsoSession::from_cached_token(&token, &verifier, PUBKEY, NOW + 10)
        .expect("verify member grant");

    assert_eq!(session.role(), Role::Member);
    let board = Resource::board("acme", "eng");

    assert!(
        session.enforce(&Action::RunWorkflow, &board).allowed(),
        "Member may run a workflow"
    );
    assert!(
        session.enforce(&Action::ReadBoard, &board).allowed(),
        "Member may read a board"
    );
    assert!(
        !session.enforce(&Action::InstallPlugin, &board).allowed(),
        "Member may NOT install a plugin (Admin-gated)"
    );
    assert!(
        !session
            .enforce(&Action::ApproveSideEffect(SideEffect::Delete), &board)
            .allowed(),
        "Member may NOT approve a destructive side effect"
    );

    // Tenant isolation: the grant never authorizes another tenant's resource.
    let other = Resource::board("globex", "eng");
    assert!(
        !session.enforce(&Action::ReadBoard, &other).allowed(),
        "a grant for acme never authorizes globex — not even a read"
    );

    // An Admin grant lifts the plugin-install gate.
    let admin_token = mint("acme", Role::Admin, &["eng"], NOW, HOUR);
    let admin = SsoSession::from_cached_token(&admin_token, &verifier, PUBKEY, NOW + 10)
        .expect("verify admin grant");
    assert!(admin.enforce(&Action::InstallPlugin, &board).allowed());
}

#[tokio::test]
async fn expired_grant_requires_reauth_keeps_local() {
    // A grant that expired an hour ago; the verifier allows a 10-minute grace.
    let iat = NOW - 2 * HOUR;
    let token = mint("acme", Role::Member, &["eng"], iat, HOUR); // exp = NOW - HOUR
    let verifier = test_grant_verifier(ISS, 600);
    let joiner = RecordingJoiner::new();

    let result = sign_in(&token, &verifier, PUBKEY, NOW, &joiner)
        .await
        .expect("sign-in returns gracefully even when the grant has lapsed");

    // Past exp + grace ⇒ re-auth required (no active session, no groups seeded)…
    assert!(!result.is_active());
    assert!(matches!(result, SignIn::Reauth { .. }));
    assert!(joiner.groups().is_empty(), "a lapsed grant seeds nothing");

    // …but LOCAL DATA STAYS READABLE — the device is never locked out (X-CUT).
    assert!(result.local_read_allowed());

    // A direct verify past grace also fails (the session can't be resolved).
    assert!(SsoSession::from_cached_token(&token, &verifier, PUBKEY, NOW).is_err());
}

#[tokio::test]
async fn cached_grant_works_offline() {
    // The broker is unreachable: only a cached, signed grant token is on disk.
    let cache = GrantCache::new();
    let iat = NOW - HOUR; // exp = NOW (just at the boundary)
    cache.store(mint("acme", Role::Admin, &["eng", "ops"], iat, HOUR));
    assert!(cache.has_grant());

    // 10-minute offline grace so a just-expired cached grant still verifies.
    let verifier = test_grant_verifier(ISS, 600);
    let joiner = RecordingJoiner::new();

    // Within the live window: the cached grant signs us in OFFLINE (no broker).
    let token = cache.load().expect("cached token present");
    let result = sign_in(&token, &verifier, PUBKEY, NOW - 60, &joiner)
        .await
        .expect("offline sign-in");
    let session = result.session().expect("cached grant yields a session offline");
    assert_eq!(session.tenant(), "acme");
    assert_eq!(session.role(), Role::Admin);
    assert_eq!(session.groups(), &["eng".to_string(), "ops".to_string()]);
    assert_eq!(joiner.groups(), vec!["eng", "ops"]);

    // Just past exp but inside grace: the cached grant STILL verifies offline.
    let within_grace = SsoSession::from_cached_token(&token, &verifier, PUBKEY, NOW + 300)
        .expect("cached grant rides the grace window offline");
    assert!(within_grace.enforce(&Action::InstallPlugin, &Resource::board("acme", "eng")).allowed());

    // A cached grant bound to a DIFFERENT XaeroID is rejected offline (no replay).
    assert!(SsoSession::from_cached_token(&token, &verifier, "xaero-other", NOW - 60).is_err());

    // Clearing the cache (sign-out) leaves local data untouched.
    cache.clear();
    assert!(!cache.has_grant());
}
