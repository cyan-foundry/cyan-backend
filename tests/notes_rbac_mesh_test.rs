//! A2 — inbound-apply notes RBAC, BOTH lanes (T25, T25b): the opt-in
//! ENFORCED-group arm tiers each inbound note row by its AUTHOR's roster role;
//! un-enforced groups keep the TR-1 mesh-open behavior (user-scope drop only).
//!
//! The gossip arm and the snapshot apply share ONE verdict fn
//! (`notes_rbac::inbound_note_applies`), so this file drives the snapshot lane
//! END-TO-END (`snapshot::apply_snapshot_frame_enforced` — the public inbound
//! door; `TopicActor::persist_event` is private, per the A1 harness note) and
//! pins the shared-verdict equivalence the gossip arm rides.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Once, OnceLock},
};

use cyan_backend::{
    identity::MeshAuthorizer,
    models::protocol::SnapshotFrame,
    notes_rbac::{self, InboundEnforcement},
    snapshot, storage,
};
use serde_json::json;

const GROUP: &str = "mesh-rbac-group";
const AUTHOR_MEMBER: &str = "peer-author-member";
const AUTHOR_ADMIN: &str = "peer-author-admin";
const AUTHOR_UNKNOWN: &str = "peer-author-unknown";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_rbac_mesh.db");
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

// ── obs capture ──────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buf").extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn global_logs() -> &'static LogBuf {
    static LOGS: OnceLock<LogBuf> = OnceLock::new();
    LOGS.get_or_init(|| {
        let buf = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("one global subscriber");
        buf
    })
}

fn logs_containing(marker: &str) -> String {
    let bytes = global_logs().0.lock().expect("log buf").clone();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|l| l.contains(marker))
        .collect::<Vec<_>>()
        .join("\n")
}

// ── fixtures ─────────────────────────────────────────────────────────────────

/// The roster-backed enforcement bundle both prod lanes build from the group's
/// `MeshAuthorizer` (the gossip worker and the snapshot download thread the
/// SAME authorizer through `note_tier_of` + `tier_from_mesh`).
fn authorizer_with_roster() -> MeshAuthorizer {
    let mut auth = MeshAuthorizer::new();
    auth.set_admin(GROUP, AUTHOR_ADMIN, cyan_backend::identity::Role::Admin);
    auth.set_admin(GROUP, AUTHOR_MEMBER, cyan_backend::identity::Role::Member);
    auth
}

fn note_json(id: &str, scope: &str, author: &str) -> serde_json::Value {
    json!({
        "id": id,
        "board_id": GROUP,
        "tenant_id": GROUP,
        "author_id": author,
        "author_name": "Peer",
        "text": format!("inbound {id}"),
        "created_at": 100,
        "updated_at": 100,
        "scope": scope,
        "kind": "constitution",
    })
}

fn metadata_frame(notes: Vec<serde_json::Value>) -> SnapshotFrame {
    serde_json::from_value(json!({
        "frame_type": "Metadata",
        "chats": [], "files": [], "integrations": [], "board_metadata": [],
        "notes": notes,
    }))
    .expect("metadata frame decodes")
}

fn apply_enforced(auth: &MeshAuthorizer, enforced: bool, notes: Vec<serde_json::Value>) {
    let tier_of = |author: &str| {
        auth.note_tier_of(GROUP, author).map(notes_rbac::tier_from_mesh)
    };
    let enforcement = InboundEnforcement { enforced, tier_of: &tier_of };
    snapshot::apply_snapshot_frame_enforced(&metadata_frame(notes), Some(&enforcement))
        .expect("apply");
}

// ════════════════════════════════════════════════════════════════════════════
// T25 — enforced group + roster Member ⇒ a tenant-scope inbound note DROPS with
// obs note_apply_denied; the un-enforced group applies it (fail-open); the
// snapshot lane enforces identically to the gossip verdict.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn inbound_tenant_note_from_member_peer_dropped_when_enforced() {
    ensure_db();
    let _ = global_logs();
    let mut auth = authorizer_with_roster();

    // Un-enforced (the default) ⇒ TR-1 apply, whatever the roster says.
    apply_enforced(&auth, false, vec![note_json("t25-open", "tenant", AUTHOR_MEMBER)]);
    assert!(storage::note_get("t25-open").expect("get").is_some(), "un-enforced applies");

    // Enforced ⇒ a Member author cannot land a TENANT (Admin-min) row.
    auth.enforce_group(GROUP);
    apply_enforced(&auth, auth.is_enforced(GROUP), vec![note_json("t25-denied", "tenant", AUTHOR_MEMBER)]);
    assert!(storage::note_get("t25-denied").expect("get").is_none(), "enforced denies Member→tenant");
    let line = logs_containing("t25-denied");
    assert!(
        line.contains("note_apply_denied") && line.contains(notes_rbac::CHECK_TENANT_WRITE),
        "obs deny names the check:\n{line}"
    );

    // The Admin author lands the same row; the Member author still lands BOARD rows.
    apply_enforced(&auth, true, vec![note_json("t25-admin", "tenant", AUTHOR_ADMIN)]);
    assert!(storage::note_get("t25-admin").expect("get").is_some());
    apply_enforced(&auth, true, vec![note_json("t25-board", "board", AUTHOR_MEMBER)]);
    assert!(storage::note_get("t25-board").expect("get").is_some());

    // An author the roster cannot tier is denied on an enforced group.
    apply_enforced(&auth, true, vec![note_json("t25-unknown", "board", AUTHOR_UNKNOWN)]);
    assert!(storage::note_get("t25-unknown").expect("get").is_none());

    // The GOSSIP arm rides the SAME verdict fn — pinned here so the two lanes
    // can never diverge silently.
    let verdict = notes_rbac::note_apply_verdict(
        true,
        auth.note_tier_of(GROUP, AUTHOR_MEMBER).map(notes_rbac::tier_from_mesh),
        "tenant",
    );
    assert!(
        matches!(verdict, notes_rbac::InboundVerdict::Deny(ref d) if d.check == notes_rbac::CHECK_TENANT_WRITE),
        "gossip-lane verdict identical: {verdict:?}"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T25b — the unknown-scope carve-out (ORCH-10 / D-A2.22): enforced ⇒ DROP with
// check=CHECK_UNKNOWN_SCOPE on BOTH lanes; un-enforced ⇒ upsert (TR-1);
// the sovereign user-scope drop wins FIRST either way.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn inbound_unknown_scope_dropped_when_enforced_applied_when_not() {
    ensure_db();
    let _ = global_logs();
    let mut auth = authorizer_with_roster();
    auth.enforce_group(GROUP);

    // Enforced + scope "asset2" (∉ vocab) ⇒ dropped with the 9th const — even
    // for an ADMIN author (an unknown scope's policy tier is unknowable).
    apply_enforced(&auth, true, vec![note_json("t25b-unknown", "asset2", AUTHOR_ADMIN)]);
    assert!(storage::note_get("t25b-unknown").expect("get").is_none());
    let line = logs_containing("t25b-unknown");
    assert!(
        line.contains("note_apply_denied") && line.contains(notes_rbac::CHECK_UNKNOWN_SCOPE),
        "obs names CHECK_UNKNOWN_SCOPE:\n{line}"
    );

    // The gossip-lane verdict is identical (both lanes, or snapshot is a backdoor).
    let verdict = notes_rbac::note_apply_verdict(
        true,
        auth.note_tier_of(GROUP, AUTHOR_ADMIN).map(notes_rbac::tier_from_mesh),
        "asset2",
    );
    assert!(
        matches!(verdict, notes_rbac::InboundVerdict::Deny(ref d) if d.check == notes_rbac::CHECK_UNKNOWN_SCOPE),
        "gossip-lane verdict identical: {verdict:?}"
    );

    // Un-enforced ⇒ the TR-1 path applies the unknown scope on BOTH lanes.
    apply_enforced(&auth, false, vec![note_json("t25b-open", "asset2", AUTHOR_UNKNOWN)]);
    assert!(storage::note_get("t25b-open").expect("get").is_some(), "TR-1 applies unknown scopes");
    assert_eq!(
        notes_rbac::note_apply_verdict(false, None, "asset2"),
        notes_rbac::InboundVerdict::Apply,
    );

    // The user-scope drop still wins FIRST — enforced or not, roster or not.
    apply_enforced(&auth, true, vec![note_json("t25b-user-enf", "user", AUTHOR_ADMIN)]);
    apply_enforced(&auth, false, vec![note_json("t25b-user-open", "user", AUTHOR_ADMIN)]);
    assert!(storage::note_get("t25b-user-enf").expect("get").is_none());
    assert!(storage::note_get("t25b-user-open").expect("get").is_none());
    assert_eq!(
        notes_rbac::note_apply_verdict(true, None, "user"),
        notes_rbac::InboundVerdict::DropSovereign,
        "sovereign drop precedes the enforced-arm checks"
    );
}
