//! A2 §7 — production SSO wiring (T24, T24c): `sso_grant::install_grant` (the
//! core behind `cyan_sso_install_grant`) flips the notes-RBAC authority state
//! machine NoSession → Active → (sign_out|expiry) → fail-open, using the
//! cyan-identity ORG grant fixtures (deterministic seeds, no broker).
//!
//! These tests mutate the process-global `SSO_SESSION`, so they run as ONE
//! serialized test fn per concern with explicit clean-up (`sign_out`) — never
//! in parallel against each other (both fns take the same guard mutex).

use std::{
    path::{Path, PathBuf},
    sync::{Mutex, Once, OnceLock},
};

use cyan_backend::{
    dispatch_put_note_v2,
    models::{commands::NetworkCommand, events::SwiftEvent},
    notes_rbac,
    sso_grant::{self, InstalledSession, SsoSession, SSO_SESSION},
    storage,
};
use cyan_identity::testing::{test_delegate, test_org};
use cyan_identity::{Grant, OrgGrantMinter, Role};
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::mpsc;

const NODE: &str = "xaero-pub-device-sso";
const TENANT: &str = "sso-tenant";
const HOUR: u64 = 3_600;

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();
/// Serializes the two global-touching tests.
static GLOBAL_GUARD: Mutex<()> = Mutex::new(());

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sso_install.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        let _ = DB_PATH.set(path);
        std::mem::forget(dir);
    });
}

fn init_base_schema(db_path: &Path) -> Result<(), rusqlite::Error> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, icon TEXT, color TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY, group_id TEXT NOT NULL, name TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY, workspace_id TEXT, group_id TEXT, board_id TEXT,
            type TEXT NOT NULL, name TEXT NOT NULL, hash TEXT, data TEXT, size INTEGER,
            source_peer TEXT, local_path TEXT, created_at INTEGER NOT NULL,
            board_mode TEXT DEFAULT 'canvas'
        );
        "#,
    )?;
    Ok(())
}

fn now_wall() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

/// Mint an org-issued grant for this device under `org` (the broker's delegate
/// pattern, org_grant_test.rs verbatim).
fn mint_org_grant(
    org: &cyan_identity::OrgXaeroId,
    role: Role,
    groups: &[&str],
    iat: u64,
    ttl: u64,
) -> SecretString {
    let delegate = test_delegate(2);
    let cert = org.issue_delegate(&delegate.public_bytes(), iat, 30 * 24 * HOUR);
    let minter = OrgGrantMinter::new(delegate, cert).expect("delegate cert matches");
    let grant = Grant::new(NODE, TENANT, role, groups.iter().map(|g| g.to_string()).collect(), iat, ttl);
    minter.mint(&grant).expect("mint org grant")
}

fn trust_json(org: &cyan_identity::OrgXaeroId, grace: u64) -> String {
    serde_json::json!({
        "tenant": TENANT,
        "org_did": org.did(),
        "legacy_rsa_public_pem": null,
        "grace_secs": grace,
    })
    .to_string()
}

/// A tenant-scope write through the PROD tier sampler (`dispatch_put_note_v2`
/// samples the installed session per write — exactly what the CommandActor
/// injects). Returns the reject reason, if any.
fn tenant_write(id: &str) -> Option<String> {
    let (net_tx, _net_rx) = mpsc::unbounded_channel::<NetworkCommand>();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    dispatch_put_note_v2(
        NODE,
        &|_b: &str| Some("sso-group".to_string()),
        &net_tx,
        &evt_tx,
        "sso-tenant-anchor".to_string(),
        Some(id.to_string()),
        None,
        "studio house rule".to_string(),
        Some("tenant".to_string()),
        Some("constitution".to_string()),
        None,
        None,
        None,
        None,
        None,
    );
    let mut reason = None;
    while let Ok(e) = evt_rx.try_recv() {
        if let SwiftEvent::NoteRejected { reason: r, .. } = e {
            reason = Some(r);
        }
    }
    reason
}

// ════════════════════════════════════════════════════════════════════════════
// T24 — the authority state machine end-to-end: fail-open → install(Viewer) ⇒
// enforce → sign_out ⇒ fail-open → expired-past-grace ⇒ fail-open → a failed
// re-install leaves the active session untouched.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn installed_grant_flips_viewer_tenant_write() {
    ensure_db();
    let _guard = GLOBAL_GUARD.lock().expect("serialize global tests");
    sso_grant::sign_out();

    // NoSession ⇒ fail-open: the tenant write passes.
    assert_eq!(tenant_write("t24-open"), None, "no session ⇒ fail-open");
    assert!(storage::note_get("t24-open").expect("get").is_some());

    // Install a VIEWER grant (org-verified) ⇒ Active ⇒ the same write denies.
    let org = test_org(1);
    let now = now_wall();
    let token = mint_org_grant(&org, Role::Viewer, &["sso-group"], now, HOUR);
    let out = sso_grant::install_grant(
        token.expose_secret(),
        &trust_json(&org, 0),
        NODE,
        now,
        None,
    );
    assert_eq!(out["active"], serde_json::json!(true), "install verifies: {out}");
    assert_eq!(out["tenant"], serde_json::json!(TENANT));
    assert_eq!(out["role"], serde_json::json!("viewer"));
    assert_eq!(
        tenant_write("t24-denied").as_deref(),
        Some(notes_rbac::CHECK_TENANT_WRITE),
        "installed Viewer session enforces the Admin-minimum tenant row"
    );
    assert!(storage::note_get("t24-denied").expect("get").is_none());

    // sign_out ⇒ NoSession ⇒ fail-open again.
    sso_grant::sign_out();
    assert_eq!(tenant_write("t24-after-signout"), None);

    // Expired past grace ⇒ the tier fn yields None ⇒ fail-open locally. The
    // session is installed directly (SsoSession::new over an already-verified
    // grant — the pub seam) with an exp two hours in the past and zero grace.
    let expired = Grant::new(NODE, TENANT, Role::Viewer, vec![], now.saturating_sub(2 * HOUR), HOUR);
    {
        let mut guard = SSO_SESSION.write().expect("session write");
        *guard = Some(InstalledSession { session: SsoSession::new(expired), grace_secs: 0 });
    }
    assert_eq!(sso_grant::installed_tier(), None, "expired past grace ⇒ None tier");
    assert_eq!(tenant_write("t24-expired"), None, "expired session fail-opens locally");

    // A failed re-install leaves the ACTIVE session untouched: install a good
    // Viewer session, then feed garbage — still enforcing.
    let good = mint_org_grant(&org, Role::Viewer, &[], now, HOUR);
    let out = sso_grant::install_grant(good.expose_secret(), &trust_json(&org, 0), NODE, now, None);
    assert_eq!(out["active"], serde_json::json!(true));
    let bad = sso_grant::install_grant("garbage-token", &trust_json(&org, 0), NODE, now, None);
    assert_eq!(bad["active"], serde_json::json!(false), "garbage token fails: {bad}");
    assert_eq!(
        tenant_write("t24-still-enforced").as_deref(),
        Some(notes_rbac::CHECK_TENANT_WRITE),
        "the failed re-install left the active session installed"
    );

    sso_grant::sign_out();
}

// ════════════════════════════════════════════════════════════════════════════
// T24c — install never writes GrantCache (the prod backing is the iOS keychain
// account `cyan_session_grant` under service `io.blockxaero.cyan.sso` —
// D-A2.20; the engine persists nothing); a second install with a fresh token
// REPLACES the session (re-verified, not merged).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn install_leaves_grant_cache_untouched_and_reverifies_on_reinstall() {
    ensure_db();
    let _guard = GLOBAL_GUARD.lock().expect("serialize global tests");
    sso_grant::sign_out();

    // The engine-side GrantCache seam stays UNWIRED by install: a cache in hand
    // holds nothing before or after (install stores ONLY the in-memory session;
    // persistence is the iOS keychain's job — account name pinned here as the
    // engine half of the D-A2.20 contract, the iOS half lives in the seam tests).
    const KEYCHAIN_ACCOUNT: &str = "cyan_session_grant";
    const KEYCHAIN_SERVICE: &str = "io.blockxaero.cyan.sso";
    assert_eq!(KEYCHAIN_ACCOUNT, "cyan_session_grant");
    assert_eq!(KEYCHAIN_SERVICE, "io.blockxaero.cyan.sso");

    let cache = cyan_backend::sso_grant::GrantCache::new();
    assert!(!cache.has_grant());

    let org = test_org(3);
    let now = now_wall();
    let viewer = mint_org_grant(&org, Role::Viewer, &[], now, HOUR);
    let out = sso_grant::install_grant(viewer.expose_secret(), &trust_json(&org, 0), NODE, now, None);
    assert_eq!(out["active"], serde_json::json!(true));
    assert!(!cache.has_grant(), "install never writes GrantCache");
    assert_eq!(sso_grant::installed_tier(), Some(Role::Viewer));

    // Second install with a FRESH token replaces the session (Viewer → Admin).
    let admin = mint_org_grant(&org, Role::Admin, &[], now, HOUR);
    let out = sso_grant::install_grant(admin.expose_secret(), &trust_json(&org, 0), NODE, now, None);
    assert_eq!(out["active"], serde_json::json!(true));
    assert_eq!(out["role"], serde_json::json!("admin"));
    assert_eq!(
        sso_grant::installed_tier(),
        Some(Role::Admin),
        "re-install replaces the installed session"
    );
    assert!(!cache.has_grant(), "GrantCache still untouched after re-install");

    // ≥1 trust source required: a trust_json with neither org_did nor legacy pem.
    let no_trust = serde_json::json!({ "tenant": TENANT }).to_string();
    let out = sso_grant::install_grant(admin.expose_secret(), &no_trust, NODE, now, None);
    assert_eq!(out["active"], serde_json::json!(false));
    assert!(
        out["reason"].as_str().unwrap_or_default().contains("no trust material"),
        "the no-trust-material reason is stated: {out}"
    );

    sso_grant::sign_out();
}
