//! LENS_AI_NOTES P5 — chat is SIGNAL: relay board chat to Lens (the cloud intake).
//!
//! The SendChat dispatch calls [`maybe_relay_chat`] after the local insert +
//! broadcast succeed. HARD CONTRACT:
//!
//! - **Env-gated, default OFF.** `CYAN_LENS_CHAT_RELAY=1` (exactly `"1"`) enables the relay;
//!   anything else leaves the mesh byte-identical to pre-P5 behavior. The gate is checked BEFORE
//!   any HTTP client is built or any runtime is touched.
//! - **Fire-and-forget.** The send is spawned onto the existing runtime; the chat path never blocks
//!   on, or fails because of, Lens. Lens unreachable is a `tracing::debug` no-op. No panics on this
//!   path (a client build failure is a debug-logged `None`, never an `expect`).
//! - **No secrets, no content in logs.** Errors log the reqwest error only; the message body and
//!   env values are never logged.

use crate::cyan_lens_client::{CyanLensClient, EventRequest};

/// The gate env var. Only the exact value `"1"` enables the relay.
pub const CHAT_RELAY_ENV: &str = "CYAN_LENS_CHAT_RELAY";

/// The pure gate rule, split from the env read so it is unit-testable without
/// mutating process env: only exactly `"1"` opts in — default OFF.
pub fn relay_enabled_value(v: Option<&str>) -> bool {
    v == Some("1")
}

/// Is the chat→Lens relay ON for this process? Read fresh per call (cheap, and a
/// test/ops toggle takes effect without a restart).
pub fn relay_enabled() -> bool {
    relay_enabled_value(std::env::var(CHAT_RELAY_ENV).ok().as_deref())
}

/// One sent chat message, exactly as the SendChat dispatch knows it after the
/// local insert + broadcast.
#[derive(Debug, Clone)]
pub struct RelayedChat {
    /// The message id (blake3 of board+text+time) — the Lens `external_id`.
    pub id: String,
    pub board_id: String,
    pub workspace_id: String,
    pub message: String,
    pub author_id: String,
    /// The display name when the dispatch knows it; empty is fine (Lens tolerates it).
    pub author_name: String,
    pub parent_id: Option<String>,
    /// The C1 anchor id when the message is anchored (step/run/frame): it becomes
    /// the Lens `thread_id`, so anchored chatter threads in the graph.
    pub anchor_id: Option<String>,
    pub timestamp: i64,
}

/// The PURE chat→`EventRequest` mapping (unit-tested): source `cyan_chat`, content
/// kind `CyanChat`, `external_id` = the message id, `group_id` = the board id
/// (boards are the workflow scope Lens nudges per), `thread_id` = the anchor when
/// present. `id` rides the message id too, so a re-relay is idempotent Lens-side.
pub fn chat_event_request(m: &RelayedChat) -> EventRequest {
    EventRequest {
        id: m.id.clone(),
        group_id: m.board_id.clone(),
        workspace_id: m.workspace_id.clone(),
        source: "cyan_chat".to_string(),
        content_kind: "CyanChat".to_string(),
        external_id: m.id.clone(),
        content: m.message.clone(),
        author_id: m.author_id.clone(),
        author_name: m.author_name.clone(),
        url: String::new(),
        title: None,
        thread_id: m.anchor_id.clone(),
        parent_id: m.parent_id.clone(),
        ts: m.timestamp.max(0) as u64,
        captured_at: chrono::Utc::now().timestamp().max(0) as u64,
    }
}

/// Relay a sent chat to Lens — GATE FIRST, then fire-and-forget. Never blocks the
/// caller, never fails the chat path, never panics.
pub fn maybe_relay_chat(m: RelayedChat) {
    if !relay_enabled() {
        return; // default: inert — no client, no runtime touch
    }
    let req = chat_event_request(&m);
    let fut = async move {
        let Some(client) = CyanLensClient::try_from_env() else {
            tracing::debug!("chat→Lens relay: client build failed (non-fatal)");
            return;
        };
        if let Err(e) = client.send_event(req).await {
            tracing::debug!("chat→Lens relay dropped (non-fatal): {e}");
        }
    };
    // The SendChat dispatch runs inside the engine runtime, so spawn in place;
    // outside a runtime (tests, exotic embeddings) fall back to the global
    // RUNTIME, else drop with a debug line — never block, never panic.
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(fut);
    } else if let Some(rt) = crate::RUNTIME.get() {
        rt.spawn(fut);
    } else {
        tracing::debug!("chat→Lens relay: no async runtime; message not relayed");
    }
}
