//! GAP 2 — inbound comments → Cyan (PULL shape), Tier-1 consumer test.
//!
//! A scheduled step polls the installed cyan-email plugin's `poll_inbound(cursor,
//! max)` → `{ events, next_cursor }`, and ROUTES each inbound event to a board
//! NOTE (never an approval — cyan-email D2). PULL only; `events_emitted` has no
//! consumer. This drives cyan-mcp's `ScriptedTransport` (NO subprocess) so the
//! runner + router + external-id dedup are deterministic on any Mac.
//!
//! The load-bearing proofs:
//!   • one scripted `poll_inbound` event  →  exactly ONE board note (text ==
//!     content, id/origin_ref == external_id);
//!   • a SECOND poll of the SAME event (unchanged cursor) re-upserts the SAME id
//!     — the note count stays 1 (external_id stability = free idempotency);
//!   • MUTANT: with no due source, the due-sweep polls nothing → 0 notes.

use std::sync::{Arc, Once};
use std::time::Duration;

use cyan_backend::inbound::{self, InboundSource};
use cyan_backend::mcp_host::PluginHost;
use cyan_backend::storage;

use cyan_mcp::{
    BackoffPolicy, Clock, Emitter, EventSink, PluginTransport, RecordingEmitter, RecordingSink,
    ScriptedTransport, SystemClock,
};
use serde_json::Value;

const PLUGIN: &str = "cyan-email";

fn jval(s: &str) -> Value {
    serde_json::from_str(s).expect("valid JSON literal in test")
}

fn test_host() -> PluginHost {
    PluginHost::new(
        Arc::new(RecordingSink::new()) as Arc<dyn EventSink>,
        Arc::new(RecordingEmitter::new()) as Arc<dyn Emitter>,
        Arc::new(SystemClock::new()) as Arc<dyn Clock>,
        BackoffPolicy {
            base: Duration::from_millis(100),
            max: Duration::from_secs(5),
            max_restarts: 3,
        },
        "device".to_string(),
    )
}

/// A scripted transport that answers `initialize` (id=1), the registration
/// contract check `tools/list` (id=2, registering exactly `poll_inbound` with
/// `side_effects: []` — a read-only tool), then one `tools/call` (id=3) whose
/// `result` is a `{ events, next_cursor }` inbound-poll envelope.
fn scripted_poll(result_json: &str) -> Box<dyn PluginTransport> {
    let mut t = ScriptedTransport::new();
    t.push_reply(jval(r#"{ "jsonrpc": "2.0", "id": 1, "result": {} }"#));
    t.push_reply(jval(
        r#"{ "jsonrpc": "2.0", "id": 2, "result": { "tools": [
             { "name": "poll_inbound", "description": "pull inbound", "inputSchema": { "type": "object" } }
        ] } }"#,
    ));
    t.push_reply(jval(&format!(
        r#"{{ "jsonrpc": "2.0", "id": 3, "result": {result_json} }}"#
    )));
    Box::new(t)
}

/// One in-memory source pointed at the installed cyan-email plugin, on a caller-
/// chosen tenant/board namespace. `cursor_json` starts absent (first poll).
fn a_source(tenant: &str, board: &str) -> InboundSource {
    InboundSource {
        id: format!("isrc-{tenant}"),
        tenant_id: tenant.to_string(),
        board_id: board.to_string(),
        plugin_id: PLUGIN.to_string(),
        schedule_secs: Some(60),
        last_poll_at: None,
        cursor_json: None,
        created_at: 1000,
    }
}

/// The process-global DB is a SINGLETON: init it exactly once for the whole test
/// binary (a temp file this binary owns), with the base group/workspace/object
/// tables the migrations assume, then `init_db` (→ `run_migrations`, which creates
/// `notes` + `inbound_source`). Every test namespaces its own tenant/board so
/// their notes + sources never collide across the shared store.
static INIT: Once = Once::new();
fn ensure_db() {
    INIT.call_once(|| {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("inbound.db");
        let conn = rusqlite::Connection::open(&db_path).expect("open base");
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
        )
        .expect("base schema");
        drop(conn);
        // Leak the tempdir so the DB file outlives the whole binary run.
        std::mem::forget(tmp);
        storage::init_db(db_path.to_str().expect("utf8 db path")).expect("init_db");
    });
}

/// HEADLINE: one scripted `poll_inbound` event routes to exactly ONE board note,
/// with the note text == the event content and the note id/origin_ref == the
/// stable external_id.
#[test]
fn inbound_poll_routes_one_event_to_a_board_note() {
    ensure_db();
    let (tenant, board) = ("t-headline", "b-headline");
    let host = test_host();
    let source = a_source(tenant, board);

    let report = inbound::poll_inbound_source(&host, &source, &[], || {
        Ok(scripted_poll(
            r#"{ "events": [
                 { "external_id": "<msg-1@x>", "content": "please fix reel 2",
                   "from_addr": "dir@studio.com", "author_name": "Dir", "ts": 1700000000 }
               ], "next_cursor": "cur-2" }"#,
        ))
    })
    .expect("poll runner succeeds");

    assert_eq!(report.routed, 1, "exactly one note routed");
    assert_eq!(report.next_cursor.as_deref(), Some("\"cur-2\""));

    let notes = storage::note_list_by_board(board, tenant).expect("list notes");
    assert_eq!(notes.len(), 1, "exactly one board note landed, got {}", notes.len());
    let note = &notes[0];
    assert_eq!(note.text, "please fix reel 2", "note text == event content");
    assert_eq!(note.id, "<msg-1@x>", "note id == stable external_id");
    assert_eq!(note.origin_ref.as_deref(), Some("<msg-1@x>"), "origin_ref == external_id");
    assert_eq!(note.author_id, "dir@studio.com", "author_id == from_addr");
    assert_eq!(note.author_name, "Dir");
    assert_eq!(note.scope, "board");
    assert_eq!(note.anchor_kind.as_deref(), Some("board"));
    assert_eq!(note.kind, "editor-note", "an inbound event is a NOTE, never an approval");
}

/// A SECOND poll of the SAME event (unchanged cursor) re-upserts the SAME id —
/// external_id stability gives idempotent dedup, so the note count stays 1.
#[test]
fn second_poll_same_event_reupserts_same_id_count_stays_one() {
    ensure_db();
    let (tenant, board) = ("t-dedup", "b-dedup");
    let host = test_host();
    let source = a_source(tenant, board);
    let event = r#"{ "events": [
         { "external_id": "<msg-dedup@x>", "content": "note body", "ts": 1700000000 }
       ], "next_cursor": "cur-9" }"#;

    let first = inbound::poll_inbound_source(&host, &source, &[], || Ok(scripted_poll(event)))
        .expect("first poll");
    assert_eq!(first.routed, 1, "first poll routes the note");

    // Same cursor, same event: the upsert is a no-op (external_id already present).
    let second = inbound::poll_inbound_source(&host, &source, &[], || Ok(scripted_poll(event)))
        .expect("second poll");
    assert_eq!(second.routed, 0, "re-polling the same event routes zero new notes");

    let notes = storage::note_list_by_board(board, tenant).expect("list notes");
    assert_eq!(notes.len(), 1, "the same external_id upserts one row, count stays 1");
}

/// MUTANT: with NO due source registered, the due-sweep selects nothing to poll —
/// so zero notes land. Registering a scheduled source makes it due.
#[test]
fn no_due_source_polls_nothing() {
    ensure_db();
    let (tenant, board) = ("t-mutant", "b-mutant");
    // This tenant starts with no notes and no inbound source.
    let notes0 = storage::note_list_by_board(board, tenant).expect("list notes");
    assert!(notes0.is_empty(), "no source, no notes");

    // The due-sweep selects nothing for this fresh tenant (filter the tenant-
    // agnostic global sweep to our namespace so it is deterministic under the
    // shared process DB).
    let due_empty: Vec<_> = inbound::due_inbound_sources_global(1700000000)
        .expect("due sweep")
        .into_iter()
        .filter(|s| s.tenant_id == tenant)
        .collect();
    assert!(due_empty.is_empty(), "no registered source ⇒ nothing due to poll");

    // Register a scheduled inbound source; now it is due.
    let added = inbound::inbound_source_add_global(tenant, board, PLUGIN, Some(60))
        .expect("add source");
    assert_eq!(added.plugin_id, PLUGIN);
    let due: Vec<_> = inbound::due_inbound_sources_global(1700000000)
        .expect("due sweep after add")
        .into_iter()
        .filter(|s| s.tenant_id == tenant)
        .collect();
    assert_eq!(due.len(), 1, "the scheduled source is due");
    assert_eq!(due[0].id, added.id);
}

/// Compile-time guard: the table + CRUD round-trips on the process DB.
#[test]
fn inbound_source_add_list_remove_roundtrip() {
    ensure_db();
    let (tenant, board) = ("t-crud", "b-crud");
    let a = inbound::inbound_source_add_global(tenant, board, PLUGIN, Some(120)).expect("add");
    let list = inbound::inbound_source_list_global(tenant).expect("list");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, a.id);
    inbound::inbound_source_remove_global(tenant, &a.id).expect("remove");
    let list2 = inbound::inbound_source_list_global(tenant).expect("list after remove");
    assert!(list2.is_empty());
}
