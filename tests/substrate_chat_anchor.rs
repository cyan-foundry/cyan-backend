//! Substrate — CHAT Stage 1 contract (Anchored Lane): C1 chat anchors, C7 note
//! anchors/provenance/decision kind, and the stable `step_uid` (§4.1).
//!
//! Everything here is ADDITIVE and serde-default: the tests pin BOTH directions of
//! wire compatibility — a pre-C1/C7 payload (no new fields) must decode exactly as
//! before, and an unanchored post-C1/C7 payload must serialize byte-identically to
//! the old wire (`skip_serializing_if` proof), so pre-Stage-1 peers are never broken.

mod support;

use cyan_backend::models::commands::CommandMsg;
use cyan_backend::models::dto::{self, NoteDTO};
use cyan_backend::models::events::NetworkEvent;
use cyan_backend::storage;
use cyan_backend::workflow;

// ════════════════════════════════════════════════════════════════════════════
// C1 — wire compatibility
// ════════════════════════════════════════════════════════════════════════════

/// A pre-C1 `SendChat` JSON (exactly what every shipped app sends today) decodes,
/// and its absent anchor reads as `None` (⇒ the board's general slot).
#[test]
fn c1_pre_anchor_send_chat_decodes() {
    let old_wire = r#"{"type":"SendChat","board_id":"b1","message":"hi","parent_id":null}"#;
    let cmd: CommandMsg = serde_json::from_str(old_wire).expect("old SendChat decodes");
    match cmd {
        CommandMsg::SendChat { board_id, message, anchor_kind, anchor_id, .. } => {
            assert_eq!(board_id, "b1");
            assert_eq!(message, "hi");
            assert!(anchor_kind.is_none(), "absent anchor_kind decodes as None");
            assert!(anchor_id.is_none(), "absent anchor_id decodes as None");
        }
        other => panic!("decoded wrong variant: {other:?}"),
    }
}

/// An anchored `SendChat` round-trips its anchor; an UNANCHORED one serializes with
/// NO anchor keys at all — the wire is byte-compatible with pre-C1 peers.
#[test]
fn c1_anchor_roundtrip_and_absent_when_unset() {
    let anchored = r#"{"type":"SendChat","board_id":"b1","message":"hi","parent_id":null,"anchor_kind":"step","anchor_id":"uid-42"}"#;
    let cmd: CommandMsg = serde_json::from_str(anchored).expect("anchored SendChat decodes");
    match &cmd {
        CommandMsg::SendChat { anchor_kind, anchor_id, .. } => {
            assert_eq!(anchor_kind.as_deref(), Some("step"));
            assert_eq!(anchor_id.as_deref(), Some("uid-42"));
        }
        other => panic!("decoded wrong variant: {other:?}"),
    }

    let unanchored = CommandMsg::SendChat {
        board_id: "b1".into(),
        message: "hi".into(),
        parent_id: None,
        anchor_kind: None,
        anchor_id: None,
    };
    let json = serde_json::to_string(&unanchored).expect("serializes");
    assert!(
        !json.contains("anchor_kind") && !json.contains("anchor_id"),
        "unset anchors must not appear on the wire (got {json})"
    );
}

/// A pre-C1 `ChatSent` gossip event decodes (anchor `None`); an anchored one
/// round-trips; an unanchored one emits no anchor keys.
#[test]
fn c1_chat_sent_event_wire_compat() {
    let old_wire = r#"{"type":"ChatSent","id":"m1","board_id":"b1","workspace_id":"w1","message":"hi","author":"a1","parent_id":null,"timestamp":7}"#;
    let evt: NetworkEvent = serde_json::from_str(old_wire).expect("old ChatSent decodes");
    match &evt {
        NetworkEvent::ChatSent { anchor_kind, anchor_id, .. } => {
            assert!(anchor_kind.is_none() && anchor_id.is_none());
        }
        other => panic!("decoded wrong variant: {other:?}"),
    }

    let anchored = NetworkEvent::ChatSent {
        id: "m2".into(),
        board_id: "b1".into(),
        workspace_id: "w1".into(),
        message: "the LUT clips".into(),
        author: "a1".into(),
        parent_id: None,
        timestamp: 8,
        anchor_kind: Some("step".into()),
        anchor_id: Some("uid-42".into()),
    };
    let json = serde_json::to_string(&anchored).expect("serializes");
    let back: NetworkEvent = serde_json::from_str(&json).expect("round-trips");
    match back {
        NetworkEvent::ChatSent { anchor_kind, anchor_id, .. } => {
            assert_eq!(anchor_kind.as_deref(), Some("step"));
            assert_eq!(anchor_id.as_deref(), Some("uid-42"));
        }
        other => panic!("round-tripped wrong variant: {other:?}"),
    }

    let unanchored = NetworkEvent::ChatSent {
        id: "m3".into(),
        board_id: "b1".into(),
        workspace_id: "w1".into(),
        message: "hi".into(),
        author: "a1".into(),
        parent_id: None,
        timestamp: 9,
        anchor_kind: None,
        anchor_id: None,
    };
    let json = serde_json::to_string(&unanchored).expect("serializes");
    assert!(
        !json.contains("anchor"),
        "unset anchors must not appear on the gossip wire (got {json})"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// C1 — storage round-trip
// ════════════════════════════════════════════════════════════════════════════

/// An anchored chat row persists its anchor and lists it back; an unanchored row
/// (and every pre-C1 row, which reads the same NULL columns) lists back `None`.
#[test]
fn c1_chat_anchor_persists_and_lists() {
    support::ensure_db();
    let w = support::unique_group_id();
    let b = support::unique_group_id();

    storage::chat_insert(
        &format!("{b}-m1"), &b, &w, "anchored msg", "author", None, 1,
        Some("step"), Some("uid-42"),
    )
    .expect("insert anchored");
    storage::chat_insert(
        &format!("{b}-m2"), &b, &w, "board msg", "author", None, 2, None, None,
    )
    .expect("insert unanchored");

    let chats = storage::chat_list_by_board(&b).expect("list");
    assert_eq!(chats.len(), 2);
    assert_eq!(chats[0].anchor_kind.as_deref(), Some("step"));
    assert_eq!(chats[0].anchor_id.as_deref(), Some("uid-42"));
    assert!(chats[1].anchor_kind.is_none() && chats[1].anchor_id.is_none());
}

// ════════════════════════════════════════════════════════════════════════════
// C7 — notes: decision kind, anchor, origin_ref
// ════════════════════════════════════════════════════════════════════════════

/// `decision` joins the closed kind vocabulary; garbage is still rejected.
#[test]
fn c7_decision_kind_is_valid_garbage_is_not() {
    assert!(dto::note_kind_valid("decision"), "decision is a valid kind (C7)");
    assert!(dto::note_kind_valid("editor-note"), "pre-C7 kinds unchanged");
    assert!(!dto::note_kind_valid("verdict"), "closed vocab still closed");
}

/// A promoted note (anchor + origin_ref + decision kind) persists all three C7
/// fields and reads them back; LWW upsert keeps them through an edit.
#[test]
fn c7_note_anchor_and_provenance_roundtrip() {
    support::ensure_db();
    let b = support::unique_group_id();
    let tenant = support::unique_group_id();
    let id = format!("{b}-note-1");

    let note = NoteDTO {
        id: id.clone(),
        board_id: b.clone(),
        tenant_id: tenant.clone(),
        author_id: "node-1".into(),
        author_name: "Ana".into(),
        text: "Decision: ship the LUT fix".into(),
        created_at: 10,
        updated_at: 10,
        scope: "board".into(),
        kind: "decision".into(),
        anchor_kind: Some("step".into()),
        anchor_id: Some("uid-42".into()),
        origin_ref: Some("chat:msg-abc".into()),
    };
    assert!(storage::note_upsert(&note).expect("upsert"), "insert is a change");

    let got = storage::note_get(&id).expect("get").expect("exists");
    assert_eq!(got.kind, "decision");
    assert_eq!(got.anchor_kind.as_deref(), Some("step"));
    assert_eq!(got.anchor_id.as_deref(), Some("uid-42"));
    assert_eq!(got.origin_ref.as_deref(), Some("chat:msg-abc"));

    // LWW edit (newer updated_at) keeps the C7 fields.
    let mut edited = got.clone();
    edited.text = "Decision: ship the LUT fix (rev 2)".into();
    edited.updated_at = 11;
    assert!(storage::note_upsert(&edited).expect("edit"));
    let after = storage::note_get(&id).expect("get").expect("exists");
    assert_eq!(after.origin_ref.as_deref(), Some("chat:msg-abc"));
    assert_eq!(after.anchor_id.as_deref(), Some("uid-42"));
}

/// A pre-C7 `NoteAdded` gossip payload (no anchor/origin keys) decodes with `None`s;
/// an unanchored NoteDTO serializes with no C7 keys (wire byte-compat).
#[test]
fn c7_note_event_wire_compat() {
    let old_wire = r#"{"type":"NoteAdded","id":"n1","board_id":"b1","tenant_id":"t1","author_id":"a1","author_name":"Ana","text":"note","created_at":1,"updated_at":1,"scope":"board","kind":"editor-note"}"#;
    let evt: NetworkEvent = serde_json::from_str(old_wire).expect("old NoteAdded decodes");
    match evt {
        NetworkEvent::NoteAdded { anchor_kind, anchor_id, origin_ref, .. } => {
            assert!(anchor_kind.is_none() && anchor_id.is_none() && origin_ref.is_none());
        }
        other => panic!("decoded wrong variant: {other:?}"),
    }

    let plain = NoteDTO {
        id: "n2".into(),
        board_id: "b1".into(),
        tenant_id: "t1".into(),
        author_id: "a1".into(),
        author_name: "Ana".into(),
        text: "plain".into(),
        created_at: 1,
        updated_at: 1,
        scope: "board".into(),
        kind: "editor-note".into(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: None,
    };
    let json = serde_json::to_string(&plain).expect("serializes");
    assert!(
        !json.contains("anchor") && !json.contains("origin_ref"),
        "unset C7 fields must not appear on the wire (got {json})"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// §4.1 — stable step_uid
// ════════════════════════════════════════════════════════════════════════════

/// Minted at first write when absent (value = the cell id, so legacy readers that
/// fall back to the cell id converge on the same identity).
#[test]
fn step_uid_minted_when_absent() {
    let meta = workflow::ensure_step_uid(None, None, "cell-1");
    let v: serde_json::Value = serde_json::from_str(&meta).expect("valid json");
    assert_eq!(v["step_uid"].as_str(), Some("cell-1"));
}

/// The uid is INHERITED from the row being rewritten — the carry that keeps a chat
/// thread anchored across a 1:1 rewrite, even when the client omits metadata (the
/// app's save path sends none, which used to wipe the column).
#[test]
fn step_uid_survives_rewrite_without_client_metadata() {
    let existing = r#"{"step_uid":"uid-original","pipeline":{"step_id":"transcode"}}"#;
    let meta = workflow::ensure_step_uid(None, Some(existing), "cell-1");
    let v: serde_json::Value = serde_json::from_str(&meta).expect("valid json");
    assert_eq!(
        v["step_uid"].as_str(),
        Some("uid-original"),
        "rewrite inherits the row's uid, never re-mints"
    );
}

/// A client write that carries its own metadata keeps every key AND gains/keeps the
/// uid; an incoming explicit uid wins over the row's.
#[test]
fn step_uid_preserves_client_metadata_keys() {
    let incoming = r#"{"pipeline":{"step_id":"conform"},"custom":"kept"}"#;
    let existing = r#"{"step_uid":"uid-original"}"#;
    let meta = workflow::ensure_step_uid(Some(incoming), Some(existing), "cell-1");
    let v: serde_json::Value = serde_json::from_str(&meta).expect("valid json");
    assert_eq!(v["step_uid"].as_str(), Some("uid-original"), "inherited into client write");
    assert_eq!(v["custom"].as_str(), Some("kept"), "client keys pass through");
    assert_eq!(v["pipeline"]["step_id"].as_str(), Some("conform"));

    let explicit = r#"{"step_uid":"uid-explicit"}"#;
    let meta2 = workflow::ensure_step_uid(Some(explicit), Some(existing), "cell-1");
    let v2: serde_json::Value = serde_json::from_str(&meta2).expect("valid json");
    assert_eq!(v2["step_uid"].as_str(), Some("uid-explicit"), "explicit uid wins");
}
