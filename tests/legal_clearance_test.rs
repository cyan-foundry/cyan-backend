//! A1 §4.9 — the `legal-clearance` state machine at BOTH doors (Phase 1).
//!
//! Drives the extracted `dispatch_put_note_v2` / `dispatch_delete_note` with
//! captured channels. Covers T9-T15 + T-A1-R6: born-pending-only, payload-required
//! on create AND edit, the producer gate + server-controlled stamps, no direct
//! cleared⇄rejected, decided-record freeze (byte-identical re-put only), the
//! delete gate (frozen/unreadable), whole-row LWW convergence, and the fail-closed
//! handling of unreadable STORED payloads injected through the inbound path.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Once, OnceLock},
};

use cyan_backend::{
    dispatch_delete_note, dispatch_put_note_v2,
    models::{commands::NetworkCommand, events::{NetworkEvent, SwiftEvent}},
    note_payload::{
        REASON_LEGAL_IDENTITY_FROZEN, REASON_LEGAL_PAYLOAD_REQUIRED, REASON_LEGAL_RECORD_FROZEN,
        REASON_LEGAL_RECORD_UNREADABLE, REASON_LEGAL_TRANSITION_DENIED,
        REASON_LEGAL_VERSION_UNKNOWN,
    },
    snapshot, storage,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

const NODE_ID: &str = "node-legal-test";

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("legal_clearance.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        let _ = DB_PATH.set(path);
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

/// Rewind a row's LWW clock (second-resolution) so the next dispatch write is
/// strictly newer — deterministic, no sleeps.
fn rewind_updated_at(id: &str, secs: i64) {
    let path = DB_PATH.get().expect("db initialized");
    let conn = rusqlite::Connection::open(path).expect("second connection");
    conn.busy_timeout(std::time::Duration::from_secs(5)).expect("busy timeout");
    let n = conn
        .execute(
            "UPDATE notes SET updated_at = updated_at - ?1 WHERE id = ?2",
            rusqlite::params![secs, id],
        )
        .expect("rewind");
    assert_eq!(n, 1, "rewound exactly one row");
}

/// Dispatch a legal-clearance PutNote; returns (network commands, local events).
fn put_legal(
    id: &str,
    text: &str,
    payload: Option<Value>,
    author_role: Option<&str>,
) -> (Vec<NetworkCommand>, Vec<SwiftEvent>) {
    let (net_tx, mut net_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    dispatch_put_note_v2(
        NODE_ID,
        &|_b| Some("lc-group".to_string()),
        &net_tx,
        &evt_tx,
        "lc-board".to_string(),
        Some(id.to_string()),
        None,
        text.to_string(),
        Some("board".to_string()),
        Some("legal-clearance".to_string()),
        None,
        None,
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

fn delete(id: &str) -> (Vec<NetworkCommand>, Vec<SwiftEvent>) {
    let (net_tx, mut net_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    dispatch_delete_note(&|_b| Some("lc-group".to_string()), &net_tx, &evt_tx, id.to_string());
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

fn pending_payload(item: &str) -> Value {
    json!({"v": 1, "item": item, "kind": "music", "status": "pending"})
}

fn with_status(mut p: Value, status: &str) -> Value {
    p["status"] = json!(status);
    p
}

/// Create a clearance born pending, then transition it to `status` as producer
/// (with the clock rewound so the LWW edit applies). Returns the stored row.
fn seed_decided(id: &str, item: &str, status: &str) -> cyan_backend::models::dto::NoteDTO {
    let (_, evts) = put_legal(id, "clear it", Some(pending_payload(item)), Some("producer"));
    assert!(rejection_reason(&evts).is_none(), "seed create must pass");
    rewind_updated_at(id, 10);
    let (_, evts) =
        put_legal(id, "clear it", Some(with_status(pending_payload(item), status)), Some("producer"));
    assert!(rejection_reason(&evts).is_none(), "seed transition to {status} must pass");
    storage::note_get(id).expect("get").expect("seeded")
}

/// Process-global obs capture (see notes_payload_test — tests run in parallel, so
/// ONE global subscriber accumulates and each test greps its unique marker).
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

/// Inject a clearance row through the inbound snapshot door — NO validation runs
/// there (TR-1); this is how unreadable stored rows come to exist locally.
fn apply_inbound_clearance(id: &str, payload: Value) {
    let frame: cyan_backend::models::protocol::SnapshotFrame =
        serde_json::from_value(json!({
            "frame_type": "Metadata",
            "chats": [], "files": [], "integrations": [], "board_metadata": [],
            "notes": [{
                "id": id, "board_id": "lc-board", "tenant_id": "lc-group",
                "author_id": "peer-9", "author_name": "Newer Peer",
                "text": "a clearance from a newer peer",
                "created_at": 5, "updated_at": 5,
                "scope": "board", "kind": "legal-clearance",
                "payload": payload
            }]
        }))
        .expect("metadata frame decodes");
    snapshot::apply_snapshot_frame(&frame).expect("inbound apply");
}

// ════════════════════════════════════════════════════════════════════════════
// T9 — create: born pending only.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn born_pending_only() {
    ensure_db();
    // Create with status "cleared" REJECTS — even for a producer.
    let (_, evts) = put_legal(
        "lc-t9-cleared",
        "t",
        Some(with_status(pending_payload("Track A"), "cleared")),
        Some("producer"),
    );
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_TRANSITION_DENIED));
    assert!(storage::note_get("lc-t9-cleared").expect("get").is_none(), "nothing persists");

    // Born pending succeeds for ANY author_role (here: none at all).
    let (_, evts) = put_legal("lc-t9-pending", "t", Some(pending_payload("Track A")), None);
    assert!(rejection_reason(&evts).is_none());
    let got = storage::note_get("lc-t9-pending").expect("get").expect("row");
    assert_eq!(got.payload.as_ref().expect("payload")["status"], json!("pending"));

    // Status ABSENT defaults to pending — also a valid birth.
    let p = json!({"v":1, "item":"Track B", "kind":"music"});
    let (_, evts) = put_legal("lc-t9-absent", "t", Some(p), Some("editor"));
    assert!(rejection_reason(&evts).is_none(), "absent status = the schema default pending");
    assert!(storage::note_get("lc-t9-absent").expect("get").is_some());
}

// ════════════════════════════════════════════════════════════════════════════
// T10 — payload REQUIRED on create AND edit; v != 1 rejects.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn legal_payload_required_and_v1_only() {
    ensure_db();
    // Create WITHOUT payload.
    let (_, evts) = put_legal("lc-t10-nopay", "t", None, Some("producer"));
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_PAYLOAD_REQUIRED));
    assert!(storage::note_get("lc-t10-nopay").expect("get").is_none());

    // Payload-absent EDIT of an existing clearance rejects; the stored row —
    // status included — is NOT clobbered.
    let (_, evts) = put_legal("lc-t10-row", "t", Some(pending_payload("Track C")), None);
    assert!(rejection_reason(&evts).is_none());
    rewind_updated_at("lc-t10-row", 10);
    let before = storage::note_get("lc-t10-row").expect("get").expect("row");
    let (_, evts) = put_legal("lc-t10-row", "totally new text", None, Some("producer"));
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_PAYLOAD_REQUIRED));
    let after = storage::note_get("lc-t10-row").expect("get").expect("row");
    assert_eq!(after.payload, before.payload, "status/payload NOT clobbered");
    assert_eq!(after.text, before.text, "text NOT clobbered");

    // {"v":2,…} rejects legal_version_unknown (the one no-opaque-store kind).
    let p = json!({"v":2, "item":"Track D", "kind":"music", "status":"pending"});
    let (_, evts) = put_legal("lc-t10-v2", "t", Some(p), Some("producer"));
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_VERSION_UNKNOWN));
    assert!(storage::note_get("lc-t10-v2").expect("get").is_none());
}

// ════════════════════════════════════════════════════════════════════════════
// T11 — the producer gate on every transition + SERVER-controlled stamps.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn producer_gate_on_every_transition() {
    ensure_db();
    let (_, evts) = put_legal("lc-t11", "clear it", Some(pending_payload("Track E")), Some("editor"));
    assert!(rejection_reason(&evts).is_none(), "pending create by any role");

    // Non-producer transitions reject.
    for role in [Some("editor"), None] {
        rewind_updated_at("lc-t11", 10);
        let (_, evts) = put_legal(
            "lc-t11", "t",
            Some(with_status(pending_payload("Track E"), "cleared")),
            role,
        );
        assert_eq!(
            rejection_reason(&evts).as_deref(),
            Some(REASON_LEGAL_TRANSITION_DENIED),
            "role {role:?} may not decide"
        );
    }
    let still = storage::note_get("lc-t11").expect("get").expect("row");
    assert_eq!(still.payload.as_ref().expect("p")["status"], json!("pending"));

    // Changing IDENTITY in the transition write rejects.
    rewind_updated_at("lc-t11", 10);
    let (_, evts) = put_legal(
        "lc-t11", "t",
        Some(with_status(pending_payload("Track E — RENAMED"), "cleared")),
        Some("producer"),
    );
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_IDENTITY_FROZEN));

    // Producer clears — and the caller's stamp values are OVERWRITTEN server-side.
    rewind_updated_at("lc-t11", 10);
    let mut forged = with_status(pending_payload("Track E"), "cleared");
    forged["cleared_by"] = json!("evil-node");
    forged["cleared_at"] = json!(1);
    let (_, evts) = put_legal("lc-t11", "cleared it", Some(forged), Some("producer"));
    assert!(rejection_reason(&evts).is_none(), "producer transition allowed");
    let cleared = storage::note_get("lc-t11").expect("get").expect("row");
    let p = cleared.payload.as_ref().expect("payload");
    assert_eq!(p["status"], json!("cleared"));
    assert_eq!(p["cleared_by"], json!(NODE_ID), "cleared_by is the SERVER's node id");
    assert!(
        p["cleared_at"].as_i64().expect("stamped") > 1_700_000_000,
        "cleared_at is the server clock, not the caller's 1"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T12 — cleared ⇄ rejected never direct; re-open is producer-only, remark-free,
// stamp-stripping, identity-frozen.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn no_direct_cleared_rejected_and_reopen() {
    ensure_db();
    seed_decided("lc-t12", "Track F", "cleared");

    // cleared → rejected direct: NEVER (must pass through pending).
    rewind_updated_at("lc-t12", 10);
    let (_, evts) = put_legal(
        "lc-t12", "clear it",
        Some(with_status(pending_payload("Track F"), "rejected")),
        Some("producer"),
    );
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_TRANSITION_DENIED));

    // cleared → pending re-open by producer, with a changed remark AND text
    // (REMARK class is free) — and the stamps are STRIPPED.
    rewind_updated_at("lc-t12", 10);
    let mut reopen = pending_payload("Track F");
    reopen["note"] = json!("license term expired — re-checking");
    let (_, evts) = put_legal("lc-t12", "re-opening this one", Some(reopen), Some("producer"));
    assert!(rejection_reason(&evts).is_none(), "producer re-open allowed");
    let row = storage::note_get("lc-t12").expect("get").expect("row");
    let p = row.payload.as_ref().expect("payload");
    assert_eq!(p["status"], json!("pending"));
    assert!(p.get("cleared_by").is_none() && p.get("cleared_at").is_none(), "stamps stripped");
    assert_eq!(row.text, "re-opening this one", "text (REMARK) free on transitions");

    // Re-open with a changed item would have rejected identity_frozen — prove it
    // on a second decided row.
    seed_decided("lc-t12b", "Track G", "rejected");
    rewind_updated_at("lc-t12b", 10);
    let (_, evts) = put_legal(
        "lc-t12b", "clear it",
        Some(pending_payload("Track G — RENAMED")),
        Some("producer"),
    );
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_IDENTITY_FROZEN));
}

// ════════════════════════════════════════════════════════════════════════════
// T13 — a decided record is FROZEN: any difference rejects; only a byte-identical
// re-put passes (updated_at bumps only).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn decided_record_frozen() {
    ensure_db();
    let stored = seed_decided("lc-t13", "Track H", "cleared");
    let stored_payload = stored.payload.clone().expect("payload");

    // Edits of item, note, or text each reject legal_record_frozen.
    let mut item_edit = stored_payload.clone();
    item_edit["item"] = json!("Track H — edited");
    let mut note_edit = stored_payload.clone();
    note_edit["note"] = json!("sneaky remark");
    for (payload, text) in [
        (item_edit, stored.text.clone()),
        (note_edit, stored.text.clone()),
        (stored_payload.clone(), "edited text".to_string()),
    ] {
        rewind_updated_at("lc-t13", 10);
        let (_, evts) = put_legal("lc-t13", &text, Some(payload), Some("producer"));
        assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_RECORD_FROZEN));
    }

    // A byte-identical re-put succeeds — updated_at bumps, nothing else moves.
    rewind_updated_at("lc-t13", 10);
    let before = storage::note_get("lc-t13").expect("get").expect("row");
    let (_, evts) =
        put_legal("lc-t13", &before.text, Some(stored_payload.clone()), Some("producer"));
    assert!(rejection_reason(&evts).is_none(), "byte-identical re-put passes");
    let after = storage::note_get("lc-t13").expect("get").expect("row");
    assert!(after.updated_at > before.updated_at, "updated_at bumps");
    assert_eq!(after.payload, before.payload, "payload verbatim");
    assert_eq!(after.text, before.text);
}

// ════════════════════════════════════════════════════════════════════════════
// T14 — the DELETE door: a decided clearance never deletes (no local delete, no
// gossip); after a producer re-open → pending, the same delete succeeds.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn delete_decided_clearance_rejected() {
    ensure_db();
    global_logs();
    seed_decided("lc-t14", "Track I", "cleared");

    let (net, evts) = delete("lc-t14");
    assert!(storage::note_get("lc-t14").expect("get").is_some(), "row still present");
    assert!(
        !net.iter().any(|c| matches!(
            c,
            NetworkCommand::Broadcast { event: NetworkEvent::NoteDeleted { .. }, .. }
        )),
        "NO NoteDeleted on the network channel"
    );
    assert_eq!(
        rejection_reason(&evts).as_deref(),
        Some(REASON_LEGAL_RECORD_FROZEN),
        "NoteRejected captured with the typed reason"
    );
    let obs = logs_containing("lc-t14");
    assert!(
        obs.contains("note_delete_rejected") && obs.contains("reason=legal_record_frozen"),
        "obs note_delete_rejected reason=legal_record_frozen (got: {obs})"
    );

    // Producer re-opens → pending → the same delete succeeds.
    rewind_updated_at("lc-t14", 10);
    let (_, evts) = put_legal("lc-t14", "clear it", Some(pending_payload("Track I")), Some("producer"));
    assert!(rejection_reason(&evts).is_none(), "re-open allowed");
    let (net, _) = delete("lc-t14");
    assert!(storage::note_get("lc-t14").expect("get").is_none(), "pending record deletes");
    assert!(
        net.iter().any(|c| matches!(
            c,
            NetworkCommand::Broadcast { event: NetworkEvent::NoteDeleted { .. }, .. }
        )),
        "the allowed delete gossips NoteDeleted"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T15 — LWW is whole-row, last-writer-wins, on BOTH arrival orders (the
// documented B6 behavior — replication never consults the state machine).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn lww_conflict_last_writer_wins_whole_row() {
    ensure_db();
    let mk = |id: &str, status: &str, updated: i64| cyan_backend::models::dto::NoteDTO {
        id: id.to_string(),
        board_id: "lc-board".into(),
        tenant_id: "lc-group".into(),
        author_id: "peer-x".into(),
        author_name: "Peer".into(),
        text: "conflicting decision".into(),
        created_at: 100,
        updated_at: updated,
        scope: "board".into(),
        kind: "legal-clearance".into(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: None,
        payload: Some(with_status(pending_payload("Track J"), status)),
        author_role: Some("producer".into()),
    };

    // Order 1: cleared@t1 then rejected@t2 (t2 > t1) ⇒ rejected wins.
    storage::note_upsert(&mk("lc-t15-a", "cleared", 1000)).expect("t1");
    storage::note_upsert(&mk("lc-t15-a", "rejected", 2000)).expect("t2");
    let a = storage::note_get("lc-t15-a").expect("get").expect("row");
    assert_eq!(a.payload.as_ref().expect("p")["status"], json!("rejected"));

    // Order 2: rejected@t2 arrives FIRST, cleared@t1 arrives late ⇒ still rejected.
    storage::note_upsert(&mk("lc-t15-b", "rejected", 2000)).expect("t2 first");
    assert!(!storage::note_upsert(&mk("lc-t15-b", "cleared", 1000)).expect("stale"), "stale no-op");
    let b = storage::note_get("lc-t15-b").expect("get").expect("row");
    assert_eq!(b.payload.as_ref().expect("p")["status"], json!("rejected"));
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R6 — fail-closed on unreadable STORED payloads, BOTH doors. Rows are
// injected through the inbound path (never validated, TR-1) and then frozen
// locally; the current status NEVER defaults to pending; rows still list.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn unreadable_stored_row_frozen_locally() {
    ensure_db();
    // (a) a v:2 clearance from a newer peer.
    apply_inbound_clearance("lc-r6-v2", json!({"v":2, "item":"Track K", "kind":"music",
        "status":"escrowed"}));
    // Local EDIT (a well-formed v1 write) rejects on the STORED-row check.
    let (_, evts) = put_legal("lc-r6-v2", "t", Some(pending_payload("Track K")), Some("producer"));
    assert_eq!(
        rejection_reason(&evts).as_deref(),
        Some(REASON_LEGAL_VERSION_UNKNOWN),
        "stored v>1 is upgrade-shaped: legal_version_unknown"
    );
    // Local DELETE rejects unreadable (v>1 status cannot be read).
    let (_, evts) = delete("lc-r6-v2");
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_RECORD_UNREADABLE));
    assert!(storage::note_get("lc-r6-v2").expect("get").is_some(), "row not deleted");

    // (b) a status-∉-vocab clearance.
    apply_inbound_clearance("lc-r6-status", json!({"v":1, "item":"Track L", "kind":"music",
        "status":"granted"}));
    let (_, evts) = put_legal("lc-r6-status", "t", Some(pending_payload("Track L")), Some("producer"));
    assert_eq!(
        rejection_reason(&evts).as_deref(),
        Some(REASON_LEGAL_RECORD_UNREADABLE),
        "current status never defaults to pending — the edit is frozen, not allowed through"
    );
    let (_, evts) = delete("lc-r6-status");
    assert_eq!(rejection_reason(&evts).as_deref(), Some(REASON_LEGAL_RECORD_UNREADABLE));
    assert!(storage::note_get("lc-r6-status").expect("get").is_some());

    // Both rows still LIST (tolerant read — frozen ≠ hidden).
    let listed = storage::note_list_by_board("lc-board", "lc-group").expect("list");
    for id in ["lc-r6-v2", "lc-r6-status"] {
        assert!(listed.iter().any(|n| n.id == id), "{id} still lists");
    }
}
