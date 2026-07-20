//! A2 — the notes write-door RBAC matrix (T19-T23): the scope-major table wired
//! into `dispatch_put_note_v3` / `dispatch_delete_note_v2` with an INJECTED tier
//! source (the prod closure samples the §7 SSO global; these tests inject).
//!
//! Drives the extracted dispatch fns with captured channels (the A1 house
//! pattern) over the process-global temp DB.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Once, OnceLock},
};

use cyan_backend::{
    dispatch_delete_note_v2, dispatch_put_note_v3,
    models::{commands::NetworkCommand, events::SwiftEvent},
    notes_rbac, storage,
};
use cyan_identity::Role;
use tokio::sync::mpsc;

const NODE: &str = "node-rbac-test";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_rbac.db");
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
        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, element_type TEXT NOT NULL,
            x REAL, y REAL, width REAL, height REAL, z_index INTEGER DEFAULT 0,
            style_json TEXT, content_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL, content TEXT, output TEXT,
            collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

// ── obs capture (the A1 global-subscriber pattern) ──────────────────────────

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

// ── dispatch harness ─────────────────────────────────────────────────────────

struct PutOutcome {
    events: Vec<SwiftEvent>,
    network: Vec<NetworkCommand>,
}

impl PutOutcome {
    fn rejection(&self) -> Option<String> {
        self.events.iter().find_map(|e| match e {
            SwiftEvent::NoteRejected { reason, .. } => Some(reason.clone()),
            _ => None,
        })
    }
}

fn put_with_tier(
    board: &str,
    id: &str,
    scope: &str,
    kind: &str,
    anchor: Option<(&str, &str)>,
    tier: Option<Role>,
) -> PutOutcome {
    let (net_tx, mut net_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    dispatch_put_note_v3(
        NODE,
        &|_b: &str| Some("rbac-group".to_string()),
        &net_tx,
        &evt_tx,
        board.to_string(),
        Some(id.to_string()),
        None,
        format!("rule body for {id}"),
        Some(scope.to_string()),
        Some(kind.to_string()),
        anchor.map(|(k, _)| k.to_string()),
        anchor.map(|(_, a)| a.to_string()),
        None,
        None,
        None,
        &move || tier,
    );
    let mut events = Vec::new();
    while let Ok(e) = evt_rx.try_recv() {
        events.push(e);
    }
    let mut network = Vec::new();
    while let Ok(c) = net_rx.try_recv() {
        network.push(c);
    }
    PutOutcome { events, network }
}

fn delete_with_tier(id: &str, tier: Option<Role>) -> Vec<SwiftEvent> {
    let (net_tx, _net_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    dispatch_delete_note_v2(
        &|_b: &str| Some("rbac-group".to_string()),
        &net_tx,
        &evt_tx,
        id.to_string(),
        NODE,
        &move || tier,
    );
    let mut events = Vec::new();
    while let Ok(e) = evt_rx.try_recv() {
        events.push(e);
    }
    events
}

// ════════════════════════════════════════════════════════════════════════════
// T19 — a Viewer session cannot write a tenant constitution: no row, no
// NetworkEvent, the obs deny names CHECK_TENANT_WRITE, NoteRejected captured.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn viewer_cannot_write_tenant_constitution() {
    ensure_db();
    let _ = global_logs();

    let out = put_with_tier("t19-anchor", "t19-n1", "tenant", "constitution", None, Some(Role::Viewer));

    assert_eq!(
        out.rejection().as_deref(),
        Some(notes_rbac::CHECK_TENANT_WRITE),
        "the deny reason IS the named check"
    );
    assert!(out.network.is_empty(), "a denied write never gossips");
    assert!(
        storage::note_get("t19-n1").expect("note_get").is_none(),
        "a denied write never persists"
    );
    let deny_line = logs_containing("t19-n1");
    assert!(
        deny_line.contains("note_put_denied") && deny_line.contains("CHECK_TENANT_WRITE"),
        "obs deny line names the check:\n{deny_line}"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T20 — the matrix rows: Member+board passes; Member+group denied; Admin+group
// passes; Member+producer passes; Member+role denied (Admin required).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn member_writes_board_note_admin_writes_group_rule() {
    ensure_db();

    let ok = put_with_tier("t20-board", "t20-n1", "board", "editor-note", None, Some(Role::Member));
    assert!(ok.rejection().is_none(), "Member writes a board note");
    assert!(storage::note_get("t20-n1").expect("get").is_some());

    let denied = put_with_tier("t20-group", "t20-n2", "group", "constitution", None, Some(Role::Member));
    assert_eq!(denied.rejection().as_deref(), Some(notes_rbac::CHECK_GROUP_WRITE));

    let admin = put_with_tier("t20-group", "t20-n3", "group", "constitution", None, Some(Role::Admin));
    assert!(admin.rejection().is_none(), "Admin writes a group rule");

    let producer =
        put_with_tier("t20-producer", "t20-n4", "producer", "editor-note", None, Some(Role::Member));
    assert!(producer.rejection().is_none(), "Member writes producer scope (Q3 posture)");

    let role = put_with_tier(
        "t20-role-group",
        "t20-n5",
        "role",
        "constitution",
        Some(("role", "colorist")),
        Some(Role::Member),
    );
    assert_eq!(
        role.rejection().as_deref(),
        Some(notes_rbac::CHECK_ROLE_WRITE),
        "role scope is policy — Admin required"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T21 — user scope requires the SELF anchor, at any tier; the check compares
// the command's board_id FIELD, not the within-board anchor_id (the fixture
// sets both, differently).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn user_scope_requires_self_anchor() {
    ensure_db();

    // board_id = a FOREIGN node; the within-board anchor pair carries THIS node
    // id — if the check read anchor_id it would wrongly pass.
    for tier in [Some(Role::Owner), Some(Role::Member), Some(Role::Viewer)] {
        let denied = put_with_tier(
            "node-somebody-else",
            "t21-n1",
            "user",
            "editor-note",
            Some(("step", NODE)),
            tier,
        );
        assert_eq!(
            denied.rejection().as_deref(),
            Some(notes_rbac::CHECK_USER_WRITE),
            "foreign user anchor denies at any installed tier {tier:?}"
        );
    }
    assert!(storage::note_get("t21-n1").expect("get").is_none());

    for (i, tier) in [None, Some(Role::Guest), Some(Role::Owner)].into_iter().enumerate() {
        let id = format!("t21-self-{i}");
        let ok = put_with_tier(NODE, &id, "user", "editor-note", None, tier);
        assert!(ok.rejection().is_none(), "self anchor passes at any tier");
        assert!(storage::note_get(&id).expect("get").is_some());
    }

    // NO session ⇒ the user row fail-opens like every other (the frozen pre-A2
    // sovereignty behavior — sovereignty holds structurally on the lanes).
    let open = put_with_tier("node-somebody-else", "t21-open", "user", "editor-note", None, None);
    assert!(open.rejection().is_none());
}

// ════════════════════════════════════════════════════════════════════════════
// T22 — no installed session ⇒ fail-open: the tier source returning None passes
// every local write (the mesh fail-open posture, D-A2.23).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn no_session_fail_open_matches_mesh_posture() {
    ensure_db();

    type Case<'a> = (&'a str, &'a str, &'a str, Option<(&'a str, &'a str)>);
    let cases: &[Case<'_>] = &[
        ("t22-tenant", "tenant", "constitution", None),
        ("t22-group", "group", "preference", None),
        ("t22-board", "board", "editor-note", None),
        ("t22-producer", "producer", "editor-note", None),
        ("t22-role-group", "role", "constitution", Some(("role", "sound"))),
    ];
    for (i, (board, scope, kind, anchor)) in cases.iter().enumerate() {
        let id = format!("t22-n{i}");
        let out = put_with_tier(board, &id, scope, kind, *anchor, None);
        assert!(
            out.rejection().is_none(),
            "tier None fail-opens scope {scope}: {:?}",
            out.rejection()
        );
        assert!(storage::note_get(&id).expect("get").is_some(), "row persisted for {scope}");
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T23 — DeleteNote runs the same matrix for the STORED note's scope: a
// tenant-scope rule survives a Member delete and falls to an Admin delete.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn delete_tenant_rule_requires_admin() {
    ensure_db();

    // Seed the tenant rule (fail-open path).
    let seeded = put_with_tier("t23-anchor", "t23-n1", "tenant", "constitution", None, None);
    assert!(seeded.rejection().is_none());
    assert!(storage::note_get("t23-n1").expect("get").is_some());

    let member_events = delete_with_tier("t23-n1", Some(Role::Member));
    let member_reason = member_events.iter().find_map(|e| match e {
        SwiftEvent::NoteRejected { reason, .. } => Some(reason.clone()),
        _ => None,
    });
    assert_eq!(member_reason.as_deref(), Some(notes_rbac::CHECK_TENANT_WRITE));
    assert!(storage::note_get("t23-n1").expect("get").is_some(), "denied delete keeps the row");

    let _ = delete_with_tier("t23-n1", Some(Role::Admin));
    assert!(storage::note_get("t23-n1").expect("get").is_none(), "Admin delete lands");
}
