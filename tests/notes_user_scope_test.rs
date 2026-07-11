//! LENS_AI_NOTES P1 — USER SCOPE IS SOVEREIGN (local-first).
//!
//! A `scope = "user"` note is the person's own, device-local layer of the
//! constitution chain: it persists in the local ledger and merges innermost, but it
//! must NEVER leave the device. Notes leave a device on exactly three paths, and
//! every one is gated here:
//!
//! 1. **Live gossip** — `PutNote` broadcasts `NoteAdded`/`NoteUpdated` on the group topic
//!    (`dispatch_put_note`, the extracted `CommandActor` arm). For user scope the broadcast is
//!    suppressed; the local `SwiftEvent` still fires so the UI sees its own note.
//! 2. **Snapshot + anti-entropy** — both serializers feed from `storage::note_list_by_boards`,
//!    which excludes `scope = 'user'` rows outright (even if a user anchor id ever collided with a
//!    board/group id).
//! 3. **Inbound apply** — a peer's user-scoped note (malicious or buggy) is dropped on snapshot
//!    apply rather than written into this node's sovereign layer.
//!
//! Driven with captured channels (the mcp_host test pattern) — no live network.

use std::{path::Path, sync::Once};

use cyan_backend::{
    dispatch_put_note,
    models::{
        commands::NetworkCommand,
        dto::NoteDTO,
        events::{NetworkEvent, SwiftEvent},
    },
    snapshot, storage,
};
use tokio::sync::mpsc;

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_user_scope.db");
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

/// Captured channels + a fixed board→group resolver, standing in for the actor's
/// live wiring. Returns (net_tx, net_rx, evt_tx, evt_rx).
type Channels = (
    mpsc::UnboundedSender<NetworkCommand>,
    mpsc::UnboundedReceiver<NetworkCommand>,
    mpsc::UnboundedSender<SwiftEvent>,
    mpsc::UnboundedReceiver<SwiftEvent>,
);

fn channels() -> Channels {
    let (net_tx, net_rx) = mpsc::unbounded_channel();
    let (evt_tx, evt_rx) = mpsc::unbounded_channel();
    (net_tx, net_rx, evt_tx, evt_rx)
}

// ════════════════════════════════════════════════════════════════════════════
// 1. A user-scoped PutNote persists locally and surfaces to the local UI, but issues NO network
//    broadcast of any kind.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn user_scope_note_persists_locally_but_never_broadcasts() {
    ensure_db();
    let (net_tx, mut net_rx, evt_tx, mut evt_rx) = channels();
    let (tenant, user_anchor) = ("us-sov-t", "us-sov-user");

    dispatch_put_note(
        "node-sovereign",
        // Even a resolvable group must not tempt the dispatch into gossiping.
        &|_board| Some("us-sov-group".to_string()),
        &net_tx,
        &evt_tx,
        user_anchor.to_string(),
        None,
        Some(tenant.to_string()),
        "my private rule: I review before 10am".to_string(),
        Some("user".to_string()),
        Some("constitution".to_string()),
        None,
        None,
        None,
    );

    // Persisted locally, listed through the scoped query the chain resolver uses.
    let listed = storage::note_list_scoped(tenant, "user", user_anchor, "constitution")
        .expect("list user notes");
    assert_eq!(listed.len(), 1, "user note persisted locally");
    assert_eq!(listed[0].text, "my private rule: I review before 10am");

    // SOVEREIGN: no NetworkCommand was issued at all — nothing to gossip.
    assert!(
        net_rx.try_recv().is_err(),
        "a user-scoped note must never produce a network broadcast"
    );

    // The local UI still hears about its own note.
    match evt_rx.try_recv() {
        Ok(SwiftEvent::Network(NetworkEvent::NoteAdded { scope, text, .. })) => {
            assert_eq!(scope, "user");
            assert_eq!(text, "my private rule: I review before 10am");
        }
        other => panic!("expected a local NoteAdded SwiftEvent, got {other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Control: a board-scoped PutNote still broadcasts NoteAdded to the board's group — the
//    sovereignty gate is user-scope-only.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn board_scope_note_still_broadcasts() {
    ensure_db();
    let (net_tx, mut net_rx, evt_tx, _evt_rx) = channels();

    dispatch_put_note(
        "node-sovereign",
        &|_board| Some("us-ctl-group".to_string()),
        &net_tx,
        &evt_tx,
        "us-ctl-board".to_string(),
        None,
        Some("us-ctl-t".to_string()),
        "board rule everyone shares".to_string(),
        Some("board".to_string()),
        Some("constitution".to_string()),
        None,
        None,
        None,
    );

    match net_rx.try_recv() {
        Ok(NetworkCommand::Broadcast { group_id, event }) => {
            assert_eq!(group_id, "us-ctl-group");
            match event {
                NetworkEvent::NoteAdded { scope, .. } => assert_eq!(scope, "board"),
                other => panic!("expected NoteAdded, got {other:?}"),
            }
        }
        other => panic!("board-scoped note must broadcast; got {other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 3. The snapshot / anti-entropy feed (`note_list_by_boards`) never carries a user-scoped note —
//    even with a worst-case anchor-id collision.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn user_scope_notes_never_enter_the_sync_feed() {
    ensure_db();
    let group = "us-feed-anchor"; // deliberately ONE id used as both group and user anchor

    let mk = |id: &str, scope: &str, text: &str| NoteDTO {
        id: id.to_string(),
        board_id: group.to_string(),
        tenant_id: "us-feed-t".to_string(),
        author_id: "node-1".to_string(),
        author_name: "Ada".to_string(),
        text: text.to_string(),
        created_at: 1,
        updated_at: 1,
        scope: scope.to_string(),
        kind: "constitution".to_string(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: None,
    };
    storage::note_upsert(&mk("us-feed-g", "group", "shared house rule")).expect("group note");
    storage::note_upsert(&mk("us-feed-u", "user", "sovereign private rule")).expect("user note");

    let feed = storage::note_list_by_boards(&[group.to_string()]).expect("feed");
    let ids: Vec<&str> = feed.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"us-feed-g"), "group note rides the sync feed");
    assert!(
        !ids.contains(&"us-feed-u"),
        "a user-scoped note must never ride snapshot/anti-entropy, even on anchor collision"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 4. Inbound: a peer's user-scoped note in a snapshot Metadata frame is DROPPED on apply — nobody
//    writes into another node's sovereign layer.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn inbound_user_scope_note_is_dropped_on_snapshot_apply() {
    ensure_db();

    let frame: cyan_backend::models::protocol::SnapshotFrame =
        serde_json::from_value(serde_json::json!({
            "frame_type": "Metadata",
            "chats": [], "files": [], "integrations": [], "board_metadata": [],
            "notes": [
                {
                    "id": "us-in-user", "board_id": "us-in-anchor", "tenant_id": "us-in-t",
                    "author_id": "peer-9", "author_name": "Mallory",
                    "text": "injected into your sovereign layer",
                    "created_at": 5, "updated_at": 5,
                    "scope": "user", "kind": "constitution"
                },
                {
                    "id": "us-in-board", "board_id": "us-in-board-id", "tenant_id": "us-in-t",
                    "author_id": "peer-9", "author_name": "Mallory",
                    "text": "a normal shared board note",
                    "created_at": 5, "updated_at": 5,
                    "scope": "board", "kind": "editor-note"
                }
            ]
        }))
        .expect("metadata frame decodes");

    snapshot::apply_snapshot_frame(&frame).expect("apply");

    assert!(
        storage::note_get("us-in-user").expect("get").is_none(),
        "inbound user-scoped note must be dropped on snapshot apply"
    );
    assert!(
        storage::note_get("us-in-board").expect("get").is_some(),
        "inbound board note still applies (the gate is user-scope-only)"
    );
}
