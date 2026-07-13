//! A1 structured notes — the additive payload/author_role DTO (Phase 1).
//!
//! Drives `storage::*` + the extracted `dispatch_put_note_v2` with captured
//! channels (the mcp_host house pattern). Covers T1-T8, T8b and T-A1-R1..R5/R7/R8:
//! payload round-trip + legacy-None reads, the §6 write-door validation block
//! (typed rejects + `NoteRejected`), size caps, opaque v>1 storage, `_meta`
//! ignore-and-preserve, the pinned P-4 batch-id vectors (fixture copied VERBATIM
//! from the spec package — never regenerated), tolerant inbound reads (TR-1), and
//! the origin_ref grammar-v2 carry/clobber semantics.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Once, OnceLock},
};

use cyan_backend::{
    dispatch_put_note_v2,
    models::{
        commands::NetworkCommand,
        dto::{self, NoteDTO},
        events::SwiftEvent,
    },
    note_payload::{self, PayloadError},
    snapshot, storage,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("notes_payload.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        let _ = DB_PATH.set(path);
        std::mem::forget(dir); // leak for the process lifetime
    });
}

/// Base tables the engine migrations assume exist, PLUS a PRE-A1 `notes` table
/// (C7 shape: scope/kind/anchors/origin_ref but NO payload_json/author_role) with
/// one legacy row — the additive A1 column migration is exercised against a DB
/// that predates it (T2).
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
        -- PRE-A1 notes table (C7 shape, no payload_json/author_role).
        CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY,
            board_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            author_id TEXT NOT NULL,
            author_name TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            scope TEXT NOT NULL DEFAULT 'board',
            kind TEXT NOT NULL DEFAULT 'editor-note',
            anchor_kind TEXT, anchor_id TEXT, origin_ref TEXT
        );
        INSERT OR IGNORE INTO notes
            (id, board_id, tenant_id, author_id, author_name, text, created_at, updated_at)
        VALUES
            ('legacy-note-a1', 'legacy-board', 'legacy-tenant', 'node-legacy',
             'Legacy Author', 'a pre-A1 board note', 500, 500);
        "#,
    )?;
    Ok(())
}

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

/// Dispatch one PutNote with the A1 fields over captured channels; returns the
/// captured local events (drained synchronously — dispatch is synchronous).
#[allow(clippy::too_many_arguments)]
fn put(
    board: &str,
    id: &str,
    scope: &str,
    kind: &str,
    text: &str,
    payload: Option<Value>,
    author_role: Option<&str>,
    anchor: Option<(&str, &str)>,
    origin_ref: Option<&str>,
) -> Vec<SwiftEvent> {
    let (net_tx, _net_rx, evt_tx, mut evt_rx) = channels();
    dispatch_put_note_v2(
        "node-payload-test",
        &|_b| Some("np-group".to_string()),
        &net_tx,
        &evt_tx,
        board.to_string(),
        Some(id.to_string()),
        None,
        text.to_string(),
        Some(scope.to_string()),
        Some(kind.to_string()),
        anchor.map(|(k, _)| k.to_string()),
        anchor.map(|(_, a)| a.to_string()),
        origin_ref.map(str::to_string),
        payload,
        author_role.map(str::to_string),
    );
    let mut events = Vec::new();
    while let Ok(e) = evt_rx.try_recv() {
        events.push(e);
    }
    events
}

fn rejection_reason(events: &[SwiftEvent]) -> Option<String> {
    events.iter().find_map(|e| match e {
        SwiftEvent::NoteRejected { reason, .. } => Some(reason.clone()),
        _ => None,
    })
}

/// The LWW clock is second-resolution; sequential dispatch edits inside one second
/// would be storage no-ops. Rewind a row's clock through a second connection so
/// the next dispatch write is strictly newer (deterministic, no sleeps).
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

/// Process-global obs-line capture. Tests run in parallel threads and tracing's
/// callsite-interest cache is process-wide, so a thread-scoped subscriber misses
/// lines — instead ONE global subscriber accumulates everything and each test
/// greps for its own unique marker (board/note ids are test-unique).
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

/// All captured log lines containing `marker` (joined). Empty if none yet.
fn logs_containing(marker: &str) -> String {
    let bytes = global_logs().0.lock().expect("log buf").clone();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|l| l.contains(marker))
        .collect::<Vec<_>>()
        .join("\n")
}

fn valid_shot_log() -> Value {
    json!({"v": 1, "scene": "12A", "setup": "A-cam CU", "take": 3, "circle": true,
           "rating": "print", "tc_in": "01:02:03:04", "tc_out": "01:02:44:12",
           "camera_roll": "A001", "sync": "synced"})
}

/// Apply one inbound note row through the snapshot Metadata frame — the public
/// inbound-apply door (the same idempotent LWW upsert the topic-actor gossip arm
/// lands on; `TopicActor::persist_event` itself is private).
fn apply_inbound_note(note_json: Value) {
    let frame: cyan_backend::models::protocol::SnapshotFrame =
        serde_json::from_value(json!({
            "frame_type": "Metadata",
            "chats": [], "files": [], "integrations": [], "board_metadata": [],
            "notes": [note_json]
        }))
        .expect("metadata frame decodes");
    snapshot::apply_snapshot_frame(&frame).expect("inbound apply");
}

// ════════════════════════════════════════════════════════════════════════════
// T1 — a valid typed payload round-trips through dispatch + store, with
// author_role and the has_payload obs marker.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn put_note_with_payload_round_trips() {
    ensure_db();
    let payload = valid_shot_log();

    global_logs();
    let events = put(
        "np-t1-board", "np-t1-note", "board", "shot-log",
        "Sc 12A tk3 — print, sync ok",
        Some(payload.clone()), Some("agent"), None, None,
    );
    assert!(rejection_reason(&events).is_none(), "valid write never rejects");
    let obs = logs_containing("id=np-t1-note");
    assert!(
        obs.contains("obs note_put") && obs.contains("has_payload=true"),
        "obs note_put line carries has_payload=true (got: {obs})"
    );

    let got = storage::note_get("np-t1-note").expect("get").expect("persisted");
    assert_eq!(got.payload.as_ref(), Some(&payload), "payload Value identical");
    assert_eq!(got.author_role.as_deref(), Some("agent"));
    assert_eq!(got.kind, "shot-log");
}

// ════════════════════════════════════════════════════════════════════════════
// T2 — a pre-A1 row lists with payload=None/author_role=None and the wire JSON
// omits both keys.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn legacy_note_reads_with_none_payload() {
    ensure_db();
    let legacy = storage::note_get("legacy-note-a1")
        .expect("get")
        .expect("legacy row survives the A1 migration");
    assert!(legacy.payload.is_none(), "pre-A1 row reads back payload=None");
    assert!(legacy.author_role.is_none(), "pre-A1 row reads back author_role=None");

    let wire = serde_json::to_string(&legacy).expect("serializes");
    assert!(
        !wire.contains("\"payload\"") && !wire.contains("author_role"),
        "unset A1 fields must not appear on the wire (got {wire})"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T3 — malformed payloads REJECT locally with a field-naming PayloadError; the
// store is unchanged and a NoteRejected event is captured.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn malformed_payload_rejected_locally() {
    ensure_db();
    // (a) missing tc_in on shot-log.
    let mut p = valid_shot_log();
    p.as_object_mut().expect("obj").remove("tc_in");
    let events = put("np-t3-board", "np-t3-a", "board", "shot-log", "t", Some(p), None, None, None);
    let reason = rejection_reason(&events).expect("NoteRejected captured");
    assert!(reason.contains("tc_in"), "reason names the field: {reason}");
    assert!(storage::note_get("np-t3-a").expect("get").is_none(), "nothing persists");

    // (b) rating outside the closed enum.
    let mut p = valid_shot_log();
    p["rating"] = json!("maybe");
    let events = put("np-t3-board", "np-t3-b", "board", "shot-log", "t", Some(p), None, None, None);
    let reason = rejection_reason(&events).expect("NoteRejected captured");
    assert!(reason.contains("rating"), "reason names the field: {reason}");
    assert!(storage::note_get("np-t3-b").expect("get").is_none());

    // (c) a non-object payload.
    let events = put(
        "np-t3-board", "np-t3-c", "board", "shot-log", "t",
        Some(json!("not an object")), None, None, None,
    );
    let reason = rejection_reason(&events).expect("NoteRejected captured");
    assert!(reason.contains("object"), "reason names the object rule: {reason}");
    assert!(storage::note_get("np-t3-c").expect("get").is_none());
}

// ════════════════════════════════════════════════════════════════════════════
// T4 — size caps: 16KB for every kind, 256KB for script; the error names the cap.
// ════════════════════════════════════════════════════════════════════════════

/// A §4.6-valid script payload padded to EXACTLY `target` serialized bytes.
fn script_payload_of_size(target: usize) -> Value {
    let mut p = json!({"v":1, "scenes":[{"scene_id":"sc:9f3a2c1d:12", "number":"12A",
        "heading":"INT. EDIT BAY - NIGHT", "action": ""}]});
    let base = serde_json::to_string(&p).expect("json").len();
    p["scenes"][0]["action"] = json!("y".repeat(target - base));
    assert_eq!(serde_json::to_string(&p).expect("json").len(), target);
    p
}

#[test]
fn oversized_payload_rejected() {
    ensure_db();
    // 16KB+1 constitution payload rejects through the write door.
    let mut c = json!({"v":1, "category":"technical", "rule":"loudness", "value":"-14 LUFS",
                       "rationale": ""});
    let base = serde_json::to_string(&c).expect("json").len();
    c["rationale"] = json!("r".repeat(note_payload::PAYLOAD_MAX_BYTES + 1 - base));
    let events =
        put("np-t4-board", "np-t4-a", "board", "constitution", "t", Some(c), None, None, None);
    let reason = rejection_reason(&events).expect("NoteRejected captured");
    assert!(
        reason.contains(&note_payload::PAYLOAD_MAX_BYTES.to_string()),
        "error names the cap: {reason}"
    );
    assert!(storage::note_get("np-t4-a").expect("get").is_none());

    // Script boundary: 256KB-1 passes validation, 256KB+1 rejects naming the cap.
    let mut ok = script_payload_of_size(note_payload::SCRIPT_PAYLOAD_MAX_BYTES - 1);
    assert_eq!(note_payload::validate("script", &mut ok), Ok(()));
    let mut over = script_payload_of_size(note_payload::SCRIPT_PAYLOAD_MAX_BYTES + 1);
    match note_payload::validate("script", &mut over) {
        Err(PayloadError::TooLarge { cap, .. }) => {
            assert_eq!(cap, note_payload::SCRIPT_PAYLOAD_MAX_BYTES);
        }
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T5 — inbound apply NEVER validates: a garbage payload upserts via LWW
// (convergence over validation, TR-1).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn inbound_malformed_payload_still_applies() {
    ensure_db();
    // Malformed per §4.3 (scene is a number, no TCs) — a local put would reject.
    apply_inbound_note(json!({
        "id": "np-t5-inbound", "board_id": "np-t5-board", "tenant_id": "np-t5-t",
        "author_id": "peer-9", "author_name": "Mallory",
        "text": "garbage-payload shot log from a peer",
        "created_at": 5, "updated_at": 5,
        "scope": "board", "kind": "shot-log",
        "payload": {"v": 1, "scene": 123, "junk": [null]}
    }));
    let got = storage::note_get("np-t5-inbound").expect("get").expect("row applied");
    assert_eq!(got.payload, Some(json!({"v":1, "scene":123, "junk":[null]})));
}

// ════════════════════════════════════════════════════════════════════════════
// T6 — v>1 payloads on typed NON-legal kinds store opaque and round-trip
// byte-equal.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn v2_payload_stored_opaque() {
    ensure_db();
    let p = json!({"v": 2, "junk": {"future": ["fields", 42]}, "scene": false});
    let events =
        put("np-t6-board", "np-t6-note", "board", "shot-log", "t", Some(p.clone()), None, None, None);
    assert!(rejection_reason(&events).is_none(), "v>1 stores opaque, never rejects");
    let got = storage::note_get("np-t6-note").expect("get").expect("persisted");
    assert_eq!(got.payload, Some(p), "opaque round-trip is value-identical");
}

// ════════════════════════════════════════════════════════════════════════════
// T7 — scene_id is deterministic and script-note-scoped (§4.7).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn scene_id_stable_across_reimport() {
    let a = note_payload::scene_id("script-note-1", "12A");
    let b = note_payload::scene_id("script-note-1", "12A");
    assert_eq!(a, b, "same note + number ⇒ same id (stable across re-imports)");
    assert_ne!(
        a,
        note_payload::scene_id("script-note-2", "12A"),
        "different script note ⇒ different id"
    );
    assert!(a.starts_with("sc:") && a.ends_with(":12A"), "sc:<8hex>:<number> shape: {a}");
}

// ════════════════════════════════════════════════════════════════════════════
// T8 — author_role is provenance: unknown values coerce to None (note accepted);
// vocabulary values persist as-is.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn unknown_author_role_coerces_to_none() {
    ensure_db();
    let events =
        put("np-t8-board", "np-t8-a", "board", "editor-note", "t", None, Some("grip"), None, None);
    assert!(rejection_reason(&events).is_none(), "provenance never blocks");
    let got = storage::note_get("np-t8-a").expect("get").expect("accepted");
    assert!(got.author_role.is_none(), "\"grip\" coerces to None");

    put("np-t8-board", "np-t8-b", "board", "editor-note", "t", None, Some("studio_exec"), None, None);
    assert_eq!(
        storage::note_get("np-t8-b").expect("get").expect("row").author_role.as_deref(),
        Some("studio_exec")
    );
    put("np-t8-board", "np-t8-c", "board", "editor-note", "t", None, Some("agent"), None, None);
    assert_eq!(
        storage::note_get("np-t8-c").expect("get").expect("row").author_role.as_deref(),
        Some("agent")
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T8b — the craft-role vocab is pinned: values AND order (the canonical ordered
// literal cyan-identity mirrors); AUTHOR_ROLE_EXTRA is exactly ["agent"].
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn production_role_vocab_pinned_ordered() {
    assert_eq!(
        dto::PRODUCTION_ROLE_VOCAB,
        ["producer", "assistant_editor", "editor", "director", "colorist", "sound", "studio_exec"],
        "PRODUCTION_ROLE_VOCAB values and ORDER are canonical program-wide"
    );
    assert_eq!(dto::AUTHOR_ROLE_EXTRA, ["agent"]);
    assert!(dto::author_role_valid("agent") && !dto::production_role_valid("agent"));
    assert!(dto::production_role_valid("colorist") && dto::author_role_valid("colorist"));
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R1 — the kind vocab is pinned at the exact ordered 13-value literal
// (supersedes any 11-count assertion); scope 8 and anchor-kind 6 ride along.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn note_kind_vocab_pinned_13() {
    assert_eq!(
        dto::NOTE_KIND_VOCAB,
        [
            "constitution", "preference", "editor-note", "decision", "creative-dna",
            "creative-brief", "shot-log", "lined-script", "continuity", "script",
            "legal-clearance", "turnover", "qc-report",
        ],
        "NOTE_KIND_VOCAB is the exact ordered 13-value literal"
    );
    assert_eq!(
        dto::NOTE_SCOPE_VOCAB,
        ["tenant", "group", "board", "workflow", "producer", "user", "project", "role"]
    );
    assert_eq!(dto::ANCHOR_KIND_VOCAB, ["step", "board", "run", "frame", "scene", "role"]);
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R2 — turnover (§4.10): valid persists; closed enums reject typed; unknown
// craft slugs coerce absent; status is DESCRIPTIVE — transitions free.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn turnover_payload_validates() {
    ensure_db();
    let valid = json!({"v":1, "to_stage":"sound", "from_role":"assistant_editor",
        "items":[{"kind":"aaf", "ref":"frameio://pkg-1", "note":"48kHz stems"},
                 {"kind":"wav", "ref":"vault://stems-3"}],
        "cut_ref":"v12", "status":"staged", "notes":"for the mix"});
    let events = put(
        "np-r2-board", "np-r2-note", "board", "turnover", "Turnover to sound",
        Some(valid.clone()), Some("agent"), None, Some("agent:run-77"),
    );
    assert!(rejection_reason(&events).is_none());
    let got = storage::note_get("np-r2-note").expect("get").expect("persisted");
    assert_eq!(got.payload.as_ref(), Some(&valid));

    // Typed rejects: missing items / unknown to_stage / unknown status.
    for (id, p) in [
        ("np-r2-x1", json!({"v":1, "to_stage":"sound"})),
        ("np-r2-x2", json!({"v":1, "to_stage":"marketing", "items":[{"kind":"aaf","ref":"r"}]})),
        ("np-r2-x3", json!({"v":1, "to_stage":"sound", "items":[{"kind":"aaf","ref":"r"}], "status":"lost"})),
    ] {
        let events = put("np-r2-board", id, "board", "turnover", "t", Some(p), None, None, None);
        assert!(rejection_reason(&events).is_some(), "{id} rejects typed");
        assert!(storage::note_get(id).expect("get").is_none(), "{id} never persists");
    }

    // Unknown from_role coerces absent (provenance posture).
    let p = json!({"v":1, "to_stage":"color", "from_role":"dj",
                   "items":[{"kind":"edl","ref":"r1"}]});
    let events = put("np-r2-coerce", "np-r2-c", "board", "turnover", "t", Some(p), None, None, None);
    assert!(rejection_reason(&events).is_none());
    let got = storage::note_get("np-r2-c").expect("get").expect("row");
    assert!(got.payload.as_ref().expect("payload").get("from_role").is_none(), "coerced absent");

    // Status transitions FREE: staged → rejected → sent all accepted.
    for status in ["rejected", "sent"] {
        rewind_updated_at("np-r2-note", 10);
        let mut p = valid.clone();
        p["status"] = json!(status);
        let events = put(
            "np-r2-board", "np-r2-note", "board", "turnover", "Turnover to sound",
            Some(p), Some("agent"), None, Some("agent:run-77"),
        );
        assert!(rejection_reason(&events).is_none(), "status {status} accepted (descriptive)");
        let got = storage::note_get("np-r2-note").expect("get").expect("row");
        assert_eq!(got.payload.as_ref().expect("payload")["status"], json!(status));
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R3 — qc-report (§4.11): valid persists; empty checks rejects; a `fail`
// overall drives NO engine behavior.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn qc_report_validates_and_gates_nothing() {
    ensure_db();
    let p = json!({"v":1, "target":"proxy://cut-4", "overall":"fail",
        "checks":[{"check":"loudness", "expected":"-14 LUFS", "measured":"-11 LUFS",
                   "result":"fail", "rule_ref":"n-rule-1"},
                  {"check":"black_frames", "expected":"none", "measured":"none",
                   "result":"pass"}],
        "tool":"cyan-media.probe"});

    let (net_tx, mut net_rx, evt_tx, mut evt_rx) = channels();
    dispatch_put_note_v2(
        "node-payload-test",
        &|_b| Some("np-group".to_string()),
        &net_tx,
        &evt_tx,
        "np-r3-board".into(),
        Some("np-r3-note".into()),
        None,
        "QC failed on loudness".into(),
        Some("board".into()),
        Some("qc-report".into()),
        None,
        None,
        None,
        Some(p.clone()),
        Some("agent".into()),
    );
    let got = storage::note_get("np-r3-note").expect("get").expect("persisted");
    assert_eq!(got.payload.as_ref(), Some(&p));

    // A failing report is ADVISORY DATA — the ONLY engine effects are the one
    // note broadcast + the local note event. No gate, no state change, nothing else.
    let mut net = Vec::new();
    while let Ok(c) = net_rx.try_recv() {
        net.push(c);
    }
    assert_eq!(net.len(), 1, "exactly the NoteAdded broadcast, nothing gate-shaped");
    let mut evts = 0;
    while let Ok(e) = evt_rx.try_recv() {
        assert!(
            matches!(e, SwiftEvent::Network(_)),
            "only the note event itself — no rejection, no gate: {e:?}"
        );
        evts += 1;
    }
    assert_eq!(evts, 1);

    // Empty checks rejects.
    let bad = json!({"v":1, "target":"t", "overall":"pass", "checks":[]});
    let events = put("np-r3-board", "np-r3-x", "board", "qc-report", "t", Some(bad), None, None, None);
    assert!(rejection_reason(&events).expect("rejects").contains("checks"));
    assert!(storage::note_get("np-r3-x").expect("get").is_none());
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R4 — `_meta` is reserved-and-preserved on every typed kind: never
// inspected, survives round-trip byte-intact, counts toward the cap.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn meta_key_reserved_and_preserved() {
    ensure_db();
    let meta = json!({"structured_by":"lens", "tier":"deterministic",
        "prompt_ver":"structure.v1", "confidence":0.92,
        "request_key16":"9f3a2c1d8b40e6aa", "row":3});

    // Every typed kind's minimal valid payload passes with `_meta` attached.
    let cases: Vec<(&str, Value)> = vec![
        ("constitution", json!({"v":1,"category":"technical","rule":"loudness","value":"-14 LUFS"})),
        ("creative-brief", json!({"v":1,"objective":"launch teaser"})),
        ("shot-log", valid_shot_log()),
        ("lined-script", json!({"v":1,"scene_id":"sc:9f3a2c1d:12",
            "setups":[{"setup":"A-cam CU","coverage":[{"from_line":1,"to_line":4}]}]})),
        ("continuity", json!({"v":1,"scene":"7","take":1})),
        ("script", json!({"v":1,"scenes":[{"scene_id":"sc:9f3a2c1d:12","number":"12A",
            "heading":"INT. EDIT BAY - NIGHT"}]})),
        ("creative-dna", json!({"v":1,"dimension":"pace","value":"fast"})),
        ("decision", json!({"v":1,"status":"proposed"})),
        ("legal-clearance", json!({"v":1,"item":"Track: 'Neon Nights'","kind":"music"})),
        ("turnover", json!({"v":1,"to_stage":"sound","items":[{"kind":"aaf","ref":"r"}]})),
        ("qc-report", json!({"v":1,"target":"t","overall":"pass",
            "checks":[{"check":"c","expected":"e","measured":"m","result":"pass"}]})),
    ];
    for (kind, mut p) in cases {
        p["_meta"] = meta.clone();
        note_payload::validate(kind, &mut p)
            .unwrap_or_else(|e| panic!("{kind} with _meta must validate: {e}"));
        assert_eq!(p["_meta"], meta, "{kind}: _meta interior untouched");
    }

    // Round-trip byte-intact through dispatch + store.
    let mut p = valid_shot_log();
    p["_meta"] = meta.clone();
    let events =
        put("np-r4-board", "np-r4-note", "board", "shot-log", "t", Some(p.clone()), None, None, None);
    assert!(rejection_reason(&events).is_none());
    let got = storage::note_get("np-r4-note").expect("get").expect("row");
    assert_eq!(got.payload.as_ref().expect("payload")["_meta"], meta, "survives round-trip");

    // `_meta` counts toward the 16KB cap.
    let mut p = valid_shot_log();
    p["_meta"] = json!({"pad": "m".repeat(note_payload::PAYLOAD_MAX_BYTES)});
    assert!(
        matches!(note_payload::validate("shot-log", &mut p), Err(PayloadError::TooLarge { .. })),
        "_meta bytes count toward the cap"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R5 — the pinned P-4 batch-id vectors (ORCH-2). The fixture is the spec
// package's file copied VERBATIM — never regenerated; this test computes the
// formula ONCE to verify the pinned constants (runtime code never re-derives).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn batch_note_id_pinned_vectors() {
    ensure_db();
    let raw = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/batch_note_id_vectors.json"),
    )
    .expect("pinned fixture present (copied verbatim from the spec package)");
    let fixture: Value = serde_json::from_str(&raw).expect("fixture parses");
    let vectors = fixture["vectors"].as_array().expect("vectors array");
    assert_eq!(vectors.len(), 6, "six pinned vectors");

    let mut ids = Vec::new();
    for v in vectors {
        let kind = v["kind"].as_str().expect("kind");
        let board_id = v["board_id"].as_str().expect("board_id");
        // The fixture escapes the literal 0x1F separator as the 4-char sequence
        // `\x1f`; restore the byte VERBATIM — no other transformation, and NO
        // Unicode normalization (the `12Á` vector pins that rule).
        let natural_key = v["natural_key"].as_str().expect("natural_key").replace("\\x1f", "\u{1f}");
        let input = format!("structrow:{kind}:{board_id}:{natural_key}");
        let computed = blake3::hash(input.as_bytes()).to_hex().to_string();
        let expected = v["expected_id"].as_str().expect("expected_id");
        assert_eq!(computed, expected, "P-4 formula matches the pinned vector for {input:?}");
        assert!(
            note_payload::batch_note_id_format_valid(expected),
            "format helper accepts every vector id"
        );
        ids.push(expected.to_string());
    }

    // Differing camera_roll ⇒ different id (vectors 0 and 1 differ only there).
    assert_ne!(ids[0], ids[1], "camera_roll A001 vs \"-\" fallback mints different ids");

    // Re-upsert with a vector id ⇒ a single row (LWW upsert, no duplicates).
    let row = |updated: i64| NoteDTO {
        id: ids[0].clone(),
        board_id: "stk-nh1-a".into(),
        tenant_id: "stk-nh1-a".into(),
        author_id: "node-payload-test".into(),
        author_name: "Ada".into(),
        text: "Sc 12A tk3".into(),
        created_at: 1000,
        updated_at: updated,
        scope: "board".into(),
        kind: "shot-log".into(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: Some("import:ale:abc123".into()),
        payload: Some(valid_shot_log()),
        author_role: Some("assistant_editor".into()),
    };
    assert!(storage::note_upsert(&row(1000)).expect("first"));
    assert!(storage::note_upsert(&row(1001)).expect("re-import converges"));
    let listed = storage::note_list_by_board("stk-nh1-a", "stk-nh1-a").expect("list");
    assert_eq!(
        listed.iter().filter(|n| n.id == ids[0]).count(),
        1,
        "unchanged natural key converges on ONE row"
    );

    // Format check rejects non-64-lowercase-hex.
    assert!(!note_payload::batch_note_id_format_valid("abc"));
    assert!(!note_payload::batch_note_id_format_valid(&"Z".repeat(64)));
    assert!(!note_payload::batch_note_id_format_valid(&ids[0].to_uppercase()));
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R7 — TR-1 tolerant read: an inbound row with an unknown kind + arbitrary
// payload lists with the payload intact-or-None, never errors (the default
// mesh-open path, ORCH-10).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn unknown_kind_payload_tolerant_read() {
    ensure_db();
    apply_inbound_note(json!({
        "id": "np-r7-future", "board_id": "np-r7-board", "tenant_id": "np-r7-t",
        "author_id": "peer-2029", "author_name": "Future Peer",
        "text": "a note kind this engine has never heard of",
        "created_at": 9, "updated_at": 9,
        "scope": "board", "kind": "future-kind",
        "payload": {"v": 9, "hologram": true, "tracks": [1, 2, 3]},
        "author_role": "quantum_editor"
    }));

    let listed = storage::note_list_by_board("np-r7-board", "np-r7-t").expect("list never errors");
    let row = listed.iter().find(|n| n.id == "np-r7-future").expect("row listed, never dropped");
    assert_eq!(row.kind, "future-kind");
    assert_eq!(
        row.payload,
        Some(json!({"v":9, "hologram":true, "tracks":[1,2,3]})),
        "payload rides intact (or would degrade to None on parse failure — never an error)"
    );
    assert_eq!(row.author_role.as_deref(), Some("quantum_editor"), "unknown role tolerated on read");
}

// ════════════════════════════════════════════════════════════════════════════
// T-A1-R8 — origin_ref grammar v2 + the P-1 carry/clobber semantics: lanes
// round-trip verbatim; an edit CARRYING origin_ref keeps it; an edit OMITTING it
// clobbers to NULL (the engine behavior B19 warns about — null ≡ absent).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn origin_ref_grammar_v2_and_carry() {
    ensure_db();
    // struct:<key16> and import:csv:<hex> accepted + round-trip verbatim.
    put(
        "np-r8-board", "np-r8-struct", "board", "constitution", "always -14 LUFS",
        Some(json!({"v":1,"category":"technical","rule":"loudness","value":"-14 LUFS"})),
        Some("producer"), None, Some("struct:9f3a2c1d8b40e6aa"),
    );
    assert_eq!(
        storage::note_get("np-r8-struct").expect("get").expect("row").origin_ref.as_deref(),
        Some("struct:9f3a2c1d8b40e6aa")
    );
    put(
        "np-r8-board", "np-r8-import", "board", "shot-log", "Sc 4 tk12",
        Some(valid_shot_log()), Some("assistant_editor"),
        None, Some("import:csv:0a1b2c3d4e5f60718293a4b5c6d7e8f9"),
    );
    assert_eq!(
        storage::note_get("np-r8-import").expect("get").expect("row").origin_ref.as_deref(),
        Some("import:csv:0a1b2c3d4e5f60718293a4b5c6d7e8f9")
    );

    // A chat-promoted note keeps `chat:` across a full-row edit that CARRIES it (P-1).
    put(
        "np-r8-board", "np-r8-chat", "board", "decision", "ship the LUT fix",
        Some(json!({"v":1,"status":"proposed"})), Some("producer"), None, Some("chat:msg-abc"),
    );
    rewind_updated_at("np-r8-chat", 10);
    put(
        "np-r8-board", "np-r8-chat", "board", "decision", "ship the LUT fix (rev 2)",
        Some(json!({"v":1,"status":"locked"})), Some("producer"), None, Some("chat:msg-abc"),
    );
    let after = storage::note_get("np-r8-chat").expect("get").expect("row");
    assert_eq!(after.origin_ref.as_deref(), Some("chat:msg-abc"), "carried forward");
    assert_eq!(after.text, "ship the LUT fix (rev 2)");

    // An edit OMITTING origin_ref clobbers it to NULL — whole-row LWW; null ≡
    // absent; the engine does NOT preserve the stored value (B19, documented).
    rewind_updated_at("np-r8-chat", 10);
    put(
        "np-r8-board", "np-r8-chat", "board", "decision", "ship the LUT fix (rev 3)",
        Some(json!({"v":1,"status":"locked"})), Some("producer"), None, None,
    );
    let clobbered = storage::note_get("np-r8-chat").expect("get").expect("row");
    assert!(
        clobbered.origin_ref.is_none(),
        "omission clobbers to NULL — edit envelopes MUST carry origin_ref forward"
    );
}
