//! LENS_AI_NOTES P5 — chat is SIGNAL: the SendChat dispatch relays board chat to
//! Lens as an `EventRequest` (source `cyan_chat`, content kind `CyanChat`).
//!
//! HARD CONTRACT under test:
//! - The relay is env-gated by `CYAN_LENS_CHAT_RELAY=1`, DEFAULT OFF — with the gate off the path
//!   is inert: the gate check runs before any HTTP client is built or any runtime is touched, so
//!   the mesh is byte-identical without the env var.
//! - The chat→`EventRequest` mapping is a pure function (anchored chat threads on its anchor id;
//!   unanchored chat has no thread).
//!
//! Offline by construction: no network, no live Lens, no tokio runtime needed.

use cyan_backend::chat_lens_relay::{
    CHAT_RELAY_ENV, RelayedChat, chat_event_request, maybe_relay_chat, relay_enabled,
    relay_enabled_value,
};

fn sent_chat(anchor_id: Option<&str>) -> RelayedChat {
    RelayedChat {
        id: "msg-abc123".to_string(),
        board_id: "board-77".to_string(),
        workspace_id: "ws-9".to_string(),
        message: "the LUT clips on the bridge shot".to_string(),
        author_id: "node-ana".to_string(),
        author_name: "Ana".to_string(),
        parent_id: Some("msg-parent".to_string()),
        anchor_id: anchor_id.map(str::to_string),
        timestamp: 1_750_000_000,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 1. The pure mapping — anchored: the anchor id becomes the Lens thread_id.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn mapping_anchored_chat_threads_on_the_anchor() {
    let req = chat_event_request(&sent_chat(Some("uid-42")));

    assert_eq!(req.source, "cyan_chat");
    assert_eq!(req.content_kind, "CyanChat");
    assert_eq!(
        req.external_id, "msg-abc123",
        "external_id is the message id"
    );
    assert_eq!(
        req.id, "msg-abc123",
        "event id rides the message id (idempotent)"
    );
    assert_eq!(req.group_id, "board-77", "board is the Lens group scope");
    assert_eq!(req.workspace_id, "ws-9");
    assert_eq!(req.content, "the LUT clips on the bridge shot");
    assert_eq!(req.author_id, "node-ana");
    assert_eq!(req.author_name, "Ana");
    assert_eq!(
        req.thread_id.as_deref(),
        Some("uid-42"),
        "anchored chat threads on its anchor"
    );
    assert_eq!(req.parent_id.as_deref(), Some("msg-parent"));
    assert_eq!(req.ts, 1_750_000_000);
    assert!(
        req.captured_at >= req.ts,
        "captured_at stamped at relay time"
    );
    assert_eq!(req.url, "", "no url for mesh chat");
    assert!(req.title.is_none(), "no title for mesh chat");
}

// ════════════════════════════════════════════════════════════════════════════
// 2. The pure mapping — unanchored: no thread_id at all.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn mapping_unanchored_chat_has_no_thread() {
    let req = chat_event_request(&sent_chat(None));
    assert!(
        req.thread_id.is_none(),
        "unanchored chat must not invent a thread"
    );
    assert_eq!(req.source, "cyan_chat");
    assert_eq!(req.external_id, "msg-abc123");
}

// ════════════════════════════════════════════════════════════════════════════
// 3. The gate rule is pure and STRICT: only the exact value "1" opts in.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn gate_only_the_exact_value_1_enables() {
    assert!(relay_enabled_value(Some("1")), "\"1\" enables the relay");
    for off in [
        None,
        Some(""),
        Some("0"),
        Some("true"),
        Some("yes"),
        Some("1 "),
        Some("ON"),
    ] {
        assert!(
            !relay_enabled_value(off),
            "{off:?} must leave the relay OFF (default-off gate)"
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 4. Gate off ⇒ the relay path is INERT. The gate is checked before any client is built or any
//    runtime is touched; calling the relay OUTSIDE a tokio runtime with the env unset must be a
//    silent, instant no-op.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn relay_is_inert_when_gate_is_off() {
    // Force the gate off for this process (edition-2024 env mutation is unsafe;
    // this test binary is the only reader of the var).
    unsafe { std::env::remove_var(CHAT_RELAY_ENV) };
    assert!(!relay_enabled(), "unset env ⇒ relay OFF");

    // No tokio runtime exists here: if the relay touched a client or tried to
    // spawn before the gate, this would panic or hang. It must simply return.
    maybe_relay_chat(sent_chat(Some("uid-42")));
    maybe_relay_chat(sent_chat(None));
}
