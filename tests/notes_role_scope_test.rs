//! A1 §1 — the `role` scope (Phase 1): GROUP-anchored so it actually replicates,
//! with the craft slug riding the REPURPOSED anchor pair (`"role"`, `<slug>`).
//!
//! Covers T16-T18: correct-by-construction tenant stamping + all three
//! replication lanes for role rows, the role-anchor validation rule (both
//! directions), and user-scope sovereignty holding for payload-bearing notes.

use std::{path::Path, sync::Once};

use cyan_backend::{
    dispatch_put_note_v2,
    models::{
        commands::NetworkCommand,
        events::{NetworkEvent, SwiftEvent},
    },
    note_payload::REASON_ROLE_ANCHOR_INVALID,
    snapshot, storage,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

const NODE_ID: &str = "node-role-test";

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_role_scope.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir); // leak for the process lifetime
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

/// Dispatch a PutNote with the full A1 surface; returns (net commands, events).
#[allow(clippy::too_many_arguments)]
fn put(
    board_id: &str,
    id: &str,
    tenant_id: Option<&str>,
    scope: &str,
    kind: &str,
    anchor: Option<(&str, &str)>,
    payload: Option<Value>,
    author_role: Option<&str>,
) -> (Vec<NetworkCommand>, Vec<SwiftEvent>) {
    let (net_tx, mut net_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    dispatch_put_note_v2(
        NODE_ID,
        // Even a resolvable board group must not divert non-board scopes: role
        // rows broadcast to the ANCHOR itself (the group id in board_id).
        &|_b| Some("rs-some-board-group".to_string()),
        &net_tx,
        &evt_tx,
        board_id.to_string(),
        Some(id.to_string()),
        tenant_id.map(str::to_string),
        "colorist house rule: never crush blacks".to_string(),
        Some(scope.to_string()),
        Some(kind.to_string()),
        anchor.map(|(k, _)| k.to_string()),
        anchor.map(|(_, a)| a.to_string()),
        None,
        payload,
        author_role.map(str::to_string),
    );
    let mut net = Vec::new();
    while let Ok(c) = net_rx.try_recv() {
        net.push(c);
    }
    let mut evts = Vec::new();
    while let Ok(e) = evt_rx.try_recv() {
        evts.push(e);
    }
    (net, evts)
}

fn rejection_reason(events: &[SwiftEvent]) -> Option<String> {
    events.iter().find_map(|e| match e {
        SwiftEvent::NoteRejected { reason, .. } => Some(reason.clone()),
        _ => None,
    })
}

// ════════════════════════════════════════════════════════════════════════════
// T16 — a role note is GROUP-anchored: tenant stamps correctly WITHOUT a
// tenant_id, it rides the sync feed, its broadcast targets the group, and the
// new role-scoped query finds it.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn role_scope_group_anchored_and_replicates() {
    ensure_db();
    let group = "rs-t16-group";

    let (net, evts) = put(
        group,
        "rs-t16-note",
        None, // NO tenant_id — the client rule; the engine derives it
        "role",
        "constitution",
        Some(("role", "colorist")),
        None,
        Some("colorist"),
    );
    assert!(rejection_reason(&evts).is_none(), "a valid role write passes");

    // Tenant stamping is correct by construction: tenant == the group anchor.
    let row = storage::note_get("rs-t16-note").expect("get").expect("stored");
    assert_eq!(row.tenant_id, group, "tenant derives from the group anchor, never a slug");
    assert_eq!(row.board_id, group, "board_id column IS the group anchor");
    assert_eq!(row.anchor_kind.as_deref(), Some("role"));
    assert_eq!(row.anchor_id.as_deref(), Some("colorist"));

    // The row appears in the sync feed for that group (the digest/snapshot feed).
    let feed = storage::note_list_by_boards(&[group.to_string()]).expect("feed");
    assert!(feed.iter().any(|n| n.id == "rs-t16-note"), "role row rides the sweep lanes");

    // The captured live-gossip Broadcast targets the GROUP (the `_ =>` arm:
    // non-board scopes broadcast to the anchor itself — zero lane changes).
    let target = net.iter().find_map(|c| match c {
        NetworkCommand::Broadcast { group_id, event } => {
            assert!(matches!(event, NetworkEvent::NoteAdded { .. }));
            Some(group_id.clone())
        }
        _ => None,
    });
    assert_eq!(target.as_deref(), Some(group), "broadcast targets the group anchor");

    // The A1 resolver query finds it (tenant == group id in this engine).
    let listed = storage::note_list_role_scoped(group, group, "colorist", "constitution")
        .expect("role-scoped query");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "rs-t16-note");
    // …and a different slug does not.
    assert!(
        storage::note_list_role_scoped(group, group, "sound", "constitution")
            .expect("query")
            .is_empty(),
        "slug filters"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T17 — the role-anchor rule, both directions: scope "role" demands the valid
// pair; the pair is reserved for scope "role". Nothing persists on any reject.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn role_scope_missing_or_bad_slug_rejected() {
    ensure_db();
    let group = "rs-t17-group";

    // scope "role" with: no anchor pair / a non-vocab slug / the "agent"
    // provenance extra (NOT a craft slug) / the wrong anchor kind.
    let cases: Vec<(&str, Option<(&str, &str)>)> = vec![
        ("rs-t17-a", None),
        ("rs-t17-b", Some(("role", "dj"))),
        ("rs-t17-c", Some(("role", "agent"))),
        ("rs-t17-d", Some(("scene", "colorist"))),
    ];
    for (id, anchor) in cases {
        let (net, evts) = put(group, id, None, "role", "constitution", anchor, None, None);
        assert_eq!(
            rejection_reason(&evts).as_deref(),
            Some(REASON_ROLE_ANCHOR_INVALID),
            "{id} rejects role_anchor_invalid"
        );
        assert!(net.is_empty(), "{id}: nothing gossips");
        assert!(storage::note_get(id).expect("get").is_none(), "{id}: nothing persists");
    }

    // The pair is RESERVED: anchor_kind "role" on a scope "board" note rejects.
    let (net, evts) = put(
        "rs-t17-board", "rs-t17-e", None, "board", "editor-note",
        Some(("role", "colorist")), None, None,
    );
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_ROLE_ANCHOR_INVALID));
    assert!(net.is_empty());
    assert!(storage::note_get("rs-t17-e").expect("get").is_none());
}

// ════════════════════════════════════════════════════════════════════════════
// T18 — user-scope sovereignty holds for PAYLOAD-BEARING notes: excluded from
// the sync feed, dropped on inbound apply.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn user_scope_still_sovereign_with_payload() {
    ensure_db();
    // A payload-bearing user-scoped note authored locally (anchor = own node id).
    let payload = json!({"v":1, "category":"creative", "rule":"morning-review",
                         "value":"I review before 10am"});
    let (net, evts) = put(
        NODE_ID, "rs-t18-user", None, "user", "constitution", None,
        Some(payload), Some("editor"),
    );
    assert!(rejection_reason(&evts).is_none(), "user write is valid");
    assert!(
        !net.iter().any(|c| matches!(c, NetworkCommand::Broadcast { .. })),
        "a sovereign note never broadcasts, payload or not"
    );
    assert!(storage::note_get("rs-t18-user").expect("get").is_some(), "persists locally");

    // Excluded from the sync feed even on anchor collision.
    let feed = storage::note_list_by_boards(&[NODE_ID.to_string()]).expect("feed");
    assert!(
        !feed.iter().any(|n| n.id == "rs-t18-user"),
        "a payload-bearing user note never rides snapshot/anti-entropy"
    );

    // Dropped on inbound apply — nobody writes into another node's sovereign layer.
    let frame: cyan_backend::models::protocol::SnapshotFrame =
        serde_json::from_value(json!({
            "frame_type": "Metadata",
            "chats": [], "files": [], "integrations": [], "board_metadata": [],
            "notes": [{
                "id": "rs-t18-foreign", "board_id": "rs-t18-anchor", "tenant_id": "rs-t18-t",
                "author_id": "peer-9", "author_name": "Mallory",
                "text": "injected into your sovereign layer",
                "created_at": 5, "updated_at": 5,
                "scope": "user", "kind": "constitution",
                "payload": {"v":1, "category":"creative", "rule":"evil", "value":"x"}
            }]
        }))
        .expect("frame decodes");
    snapshot::apply_snapshot_frame(&frame).expect("apply");
    assert!(
        storage::note_get("rs-t18-foreign").expect("get").is_none(),
        "inbound payload-bearing user-scoped note dropped on apply"
    );
}
