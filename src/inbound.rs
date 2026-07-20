//! GAP 2 — inbound comments → Cyan (PULL shape).
//!
//! A scheduled workflow step POLLS an installed inbound plugin (cyan-email) for
//! new messages and ROUTES each one to a board NOTE. This is the mirror image of
//! the [`crate::ingest`] media-sensor path (watched folder/s3/frameio_c2c → per-
//! asset runs), but for TEXT inbound: an `inbound_source` row watches a plugin,
//! the app tick calls the poll runner, and every returned event lands as a
//! `NoteDTO` on the source's board.
//!
//! **PULL only.** The runner calls the plugin's `poll_inbound(cursor, max)` tool
//! → `{ events, next_cursor }` through the SAME in-process MCP dispatch the
//! pipeline uses ([`crate::mcp_host::PluginHost::dispatch_mcp_tool`]). `poll_inbound`
//! advertises `side_effects: []`, so it runs UN-GATED (no human approval). There is
//! deliberately NO push rail — `events_emitted` has no consumer.
//!
//! **Every inbound event is a NOTE, never an approval** (cyan-email D2). The router
//! maps each event to a board-scoped `editor-note` (general/free-text), keyed by
//! the plugin's stable `external_id`. The note store upserts by id with LWW on
//! `updated_at` ([`crate::storage::note_upsert`]), so external_id stability gives
//! free idempotency/dedup: a re-poll of the same event re-upserts the SAME row.
//!
//! **Cursor discipline.** The opaque `next_cursor` is persisted ONLY AFTER the
//! notes durably land, and `last_poll_at` advances alongside — so a mid-poll crash
//! re-delivers rather than skips.
//!
//! Design seam mirrors [`crate::ingest`]: every DB op takes an explicit
//! `&Connection` (tests run on the process DB), while the FFI (`cyan_ingest_command`
//! ops `inbound_source_add` / `inbound_source_list` / `inbound_source_remove` /
//! `poll_inbound_due`) drives the process-global `storage::db()`.

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::mcp_host::{McpDispatch, McpTool, PluginHost, RunCostLedger, RunScope};
use crate::models::dto::NoteDTO;

/// The plugin tool the runner calls (cyan-email `poll_inbound(cursor, max)`).
pub const POLL_TOOL: &str = "poll_inbound";
/// The note kind every inbound event becomes: a general/free-text board note
/// (never an approval — cyan-email D2). Member of `NOTE_KIND_VOCAB`.
pub const INBOUND_NOTE_KIND: &str = "editor-note";
/// Default page size passed to `poll_inbound(cursor, max)`.
pub const DEFAULT_MAX: i64 = 50;

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

// ============================================================================
// Rows.
// ============================================================================

/// One watched inbound source: a board's sensor pointed at an installed inbound
/// plugin (cyan-email). `schedule_secs = None` means manual-only. `cursor_json`
/// is the plugin's opaque pagination cursor, persisted AFTER notes land.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboundSource {
    pub id: String,
    /// Tenant boundary (the group id) — every query carries it.
    pub tenant_id: String,
    /// The board every polled event lands a NOTE on.
    pub board_id: String,
    /// The installed plugin whose `poll_inbound` tool the runner calls.
    pub plugin_id: String,
    /// Poll cadence in seconds (the Schedule button); `None` = manual only.
    #[serde(default)]
    pub schedule_secs: Option<i64>,
    /// Unix seconds of the last SUCCESSFUL poll; `None` = never polled.
    #[serde(default)]
    pub last_poll_at: Option<i64>,
    /// The plugin's opaque cursor from the last poll (serialized JSON), advanced
    /// ONLY after that poll's notes durably landed. `None` = poll from the start.
    #[serde(default)]
    pub cursor_json: Option<String>,
    pub created_at: i64,
}

/// One inbound message parsed out of a `poll_inbound` result's `events` array.
/// `external_id` is the plugin's STABLE identity (dedup key). `content` becomes
/// the note text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundEvent {
    /// Stable, plugin-assigned identity → the note id + origin_ref (free dedup).
    pub external_id: String,
    /// The message body → the note text.
    pub content: String,
    /// Sender address → the note author_id (provenance, not authz).
    pub from_addr: Option<String>,
    /// Display name → the note author_name.
    pub author_name: Option<String>,
    /// Optional subject/title (carried for future use; not folded into text).
    pub title: Option<String>,
    /// Message time (Unix seconds) → the note created_at/updated_at.
    pub ts: i64,
    /// Threading parent, if any (carried for future use).
    pub in_reply_to: Option<String>,
}

/// What one poll did — every dropped event is COUNTED, never silent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PollReport {
    /// Events parsed out of the `events` array.
    pub seen: usize,
    /// Notes newly landed (an upsert that changed state — a genuinely new event).
    pub routed: usize,
    /// Events skipped for a missing/empty `external_id` (can't dedup — surfaced).
    pub malformed: usize,
    /// The plugin's opaque `next_cursor` from this poll (serialized), if any.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// One source's outcome from a [`poll_inbound_due_global`] sweep — errors are
/// carried, not thrown, so one bad source never blocks the rest of the tick.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PollDueOutcome {
    pub source_id: String,
    #[serde(default)]
    pub report: Option<PollReport>,
    #[serde(default)]
    pub error: Option<String>,
}

// ============================================================================
// Migration.
// ============================================================================

/// Create `inbound_source`. Idempotent; called from `storage::run_migrations`
/// (alongside `ingest::migrate`) and directly from tests. Additive — no existing
/// table or behavior changes.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS inbound_source (
            id            TEXT PRIMARY KEY,
            tenant_id     TEXT NOT NULL,
            board_id      TEXT NOT NULL,
            plugin_id     TEXT NOT NULL,
            schedule_secs INTEGER,
            last_poll_at  INTEGER,
            cursor_json   TEXT,
            created_at    INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_inbound_source_tenant
            ON inbound_source(tenant_id);
        "#,
    )?;
    Ok(())
}

// ============================================================================
// Sources (conn-explicit; mirrors ingest::source_*).
// ============================================================================

fn row_to_source(row: &rusqlite::Row) -> rusqlite::Result<InboundSource> {
    Ok(InboundSource {
        id: row.get("id")?,
        tenant_id: row.get("tenant_id")?,
        board_id: row.get("board_id")?,
        plugin_id: row.get("plugin_id")?,
        schedule_secs: row.get("schedule_secs")?,
        last_poll_at: row.get("last_poll_at")?,
        cursor_json: row.get("cursor_json")?,
        created_at: row.get("created_at")?,
    })
}

/// Register a watched inbound source on a board. `schedule_secs = None` = manual.
pub fn inbound_source_add(
    conn: &Connection,
    tenant_id: &str,
    board_id: &str,
    plugin_id: &str,
    schedule_secs: Option<i64>,
) -> Result<InboundSource> {
    if tenant_id.trim().is_empty() {
        return Err(anyhow!("tenant_id required"));
    }
    if board_id.trim().is_empty() {
        return Err(anyhow!("board_id required"));
    }
    if plugin_id.trim().is_empty() {
        return Err(anyhow!("plugin_id required"));
    }
    if let Some(s) = schedule_secs
        && s <= 0
    {
        return Err(anyhow!("schedule_secs must be positive (or absent for manual-only)"));
    }
    let source = InboundSource {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant_id.to_string(),
        board_id: board_id.to_string(),
        plugin_id: plugin_id.to_string(),
        schedule_secs,
        last_poll_at: None,
        cursor_json: None,
        created_at: now(),
    };
    conn.execute(
        "INSERT INTO inbound_source (id, tenant_id, board_id, plugin_id, schedule_secs, last_poll_at, cursor_json, created_at) \
         VALUES (?1,?2,?3,?4,?5,NULL,NULL,?6)",
        params![
            source.id,
            source.tenant_id,
            source.board_id,
            source.plugin_id,
            source.schedule_secs,
            source.created_at
        ],
    )?;
    Ok(source)
}

/// Every inbound source a tenant holds, oldest first.
pub fn inbound_source_list(conn: &Connection, tenant_id: &str) -> Result<Vec<InboundSource>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM inbound_source WHERE tenant_id=?1 ORDER BY created_at ASC, rowid ASC",
    )?;
    let rows = stmt
        .query_map(params![tenant_id], row_to_source)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// One inbound source by id. Errors if unknown.
pub fn inbound_source_get(conn: &Connection, source_id: &str) -> Result<InboundSource> {
    conn.query_row(
        "SELECT * FROM inbound_source WHERE id=?1",
        params![source_id],
        row_to_source,
    )
    .map_err(|e| anyhow!("inbound source {} not found: {}", source_id, e))
}

/// Remove an inbound source (tenant-scoped). Already-landed notes are untouched.
pub fn inbound_source_remove(conn: &Connection, tenant_id: &str, source_id: &str) -> Result<()> {
    let n = conn.execute(
        "DELETE FROM inbound_source WHERE tenant_id=?1 AND id=?2",
        params![tenant_id, source_id],
    )?;
    if n == 0 {
        return Err(anyhow!("inbound source {} not found", source_id));
    }
    Ok(())
}

/// Sources whose poll cadence has elapsed at `now`: `schedule_secs` set and either
/// never polled or `now - last_poll_at >= schedule_secs`. Manual-only sources
/// (`schedule_secs = None`) are never due. Mirrors [`crate::ingest::due_sources`].
pub fn due_inbound_sources(conn: &Connection, now: i64) -> Result<Vec<InboundSource>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM inbound_source \
         WHERE schedule_secs IS NOT NULL \
           AND (last_poll_at IS NULL OR ?1 - last_poll_at >= schedule_secs) \
         ORDER BY created_at ASC, rowid ASC",
    )?;
    let rows = stmt
        .query_map(params![now], row_to_source)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Advance a source's cursor + `last_poll_at` — called ONLY after this poll's
/// notes durably landed (so a mid-poll crash re-delivers, never skips).
pub fn advance_poll_progress(
    conn: &Connection,
    source_id: &str,
    next_cursor: Option<&str>,
    at: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE inbound_source SET cursor_json=?1, last_poll_at=?2 WHERE id=?3",
        params![next_cursor, at, source_id],
    )?;
    Ok(())
}

// ============================================================================
// Router — inbound event → board NOTE (mirror of review_loop::ingest_sense_result).
// ============================================================================

/// Parse the `events` array out of a `poll_inbound` tool result. Each event needs
/// a non-empty `external_id` (the dedup key) — one missing it is malformed and
/// COUNTED, never routed to an un-dedupable note. `content` defaults to empty;
/// `ts` defaults to now. Tolerant of extra fields.
pub fn parse_inbound_events(result: &Value) -> (Vec<InboundEvent>, usize) {
    let Some(items) = result.get("events").and_then(|v| v.as_array()) else {
        return (Vec::new(), 0);
    };
    let mut out = Vec::new();
    let mut malformed = 0usize;
    for item in items {
        let external_id = item
            .get("external_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if external_id.is_empty() {
            malformed += 1;
            continue;
        }
        let content = item
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let str_field = |k: &str| item.get(k).and_then(|v| v.as_str()).map(str::to_string);
        out.push(InboundEvent {
            external_id,
            content,
            from_addr: str_field("from_addr"),
            author_name: str_field("author_name"),
            title: str_field("title"),
            ts: item.get("ts").and_then(|v| v.as_i64()).unwrap_or_else(now),
            in_reply_to: str_field("in_reply_to"),
        });
    }
    (out, malformed)
}

/// Map one inbound event to a board NOTE. Board-scoped, general/free-text
/// `editor-note` (NEVER an approval — cyan-email D2). The note id AND origin_ref
/// are the stable `external_id`: the store upserts by id with LWW on `updated_at`,
/// so a re-poll of the same event converges without a duplicate.
pub fn event_to_note(source: &InboundSource, event: &InboundEvent) -> NoteDTO {
    NoteDTO {
        id: event.external_id.clone(),
        board_id: source.board_id.clone(),
        tenant_id: source.tenant_id.clone(),
        author_id: event.from_addr.clone().unwrap_or_default(),
        author_name: event.author_name.clone().unwrap_or_default(),
        text: event.content.clone(),
        created_at: event.ts,
        updated_at: event.ts,
        scope: "board".to_string(),
        kind: INBOUND_NOTE_KIND.to_string(),
        anchor_kind: Some("board".to_string()),
        anchor_id: None,
        origin_ref: Some(event.external_id.clone()),
        payload: None,
        author_role: None,
    }
}

// ============================================================================
// Runner — PULL one source (host + connect injected; the Tier-1 seam).
// ============================================================================

/// Poll ONE inbound source and route its events to board notes. The MCP dispatch
/// (`dispatch_mcp_tool`) runs `poll_inbound` on the plugin; `poll_inbound`
/// advertises `side_effects: []` so it runs UN-GATED. Each returned event upserts
/// a board note (global note store, idempotent on external_id). Returns the poll
/// report (incl. `next_cursor`) — the CALLER persists the cursor only after this
/// returns Ok, so notes are durable before the cursor advances.
///
/// `side_effects` is passed in (the FFI path resolves it from the installed
/// registry; tests pass `&[]`). `connect` produces a live transport for this call
/// (prod: a spawned `StdioTransport`; tests: a scripted transport).
pub fn poll_inbound_source<F>(
    host: &PluginHost,
    source: &InboundSource,
    side_effects: &[String],
    connect: F,
) -> Result<PollReport>
where
    F: FnOnce() -> Result<Box<dyn cyan_mcp::PluginTransport>>,
{
    // The plugin gets back the SAME opaque cursor it emitted last poll.
    let cursor_value: Value = source
        .cursor_json
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);
    let step = McpTool {
        plugin_id: source.plugin_id.clone(),
        tool: POLL_TOOL.to_string(),
        args: json!({ "cursor": cursor_value, "max": DEFAULT_MAX }),
    };
    let scope = RunScope {
        tenant_id: source.tenant_id.clone(),
        run_id: format!("inbound:{}", source.id),
    };
    let ledger = RunCostLedger::new();

    // PULL: unapproved is fine — poll_inbound is read-only (side_effects: []).
    let dispatched = host.dispatch_mcp_tool(&scope, &step, side_effects, false, &ledger, connect)?;
    let result = match dispatched {
        McpDispatch::Ran(r) => r.result,
        McpDispatch::Gated { side_effects } => {
            return Err(anyhow!(
                "poll_inbound unexpectedly gated on side_effects {side_effects:?} — an inbound \
                 PULL must be read-only (side_effects: []); the bundle mis-declares it"
            ));
        }
    };

    let (events, malformed) = parse_inbound_events(&result);
    let next_cursor = result
        .get("next_cursor")
        .filter(|v| !v.is_null())
        .map(|v| v.to_string());

    // ROUTE each event → a board NOTE. external_id stability = free dedup.
    let mut routed = 0usize;
    for event in &events {
        let note = event_to_note(source, event);
        if crate::storage::note_upsert(&note)? {
            routed += 1;
        }
    }

    Ok(PollReport {
        seen: events.len(),
        routed,
        malformed,
        next_cursor,
    })
}

// ============================================================================
// Process-global wrappers (the FFI / app-tick path).
// ============================================================================

/// Short-lock helper against the process DB (mirrors `ingest::with_global`).
fn with_global<T>(f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    let lock = crate::storage::try_db()
        .ok_or_else(|| anyhow!("DB not initialized"))?
        .lock()
        .map_err(|e| anyhow!("DB lock: {}", e))?;
    f(&lock)
}

/// `inbound_source_add` against the process DB.
pub fn inbound_source_add_global(
    tenant_id: &str,
    board_id: &str,
    plugin_id: &str,
    schedule_secs: Option<i64>,
) -> Result<InboundSource> {
    with_global(|c| inbound_source_add(c, tenant_id, board_id, plugin_id, schedule_secs))
}

/// `inbound_source_list` against the process DB.
pub fn inbound_source_list_global(tenant_id: &str) -> Result<Vec<InboundSource>> {
    with_global(|c| inbound_source_list(c, tenant_id))
}

/// `inbound_source_remove` against the process DB.
pub fn inbound_source_remove_global(tenant_id: &str, source_id: &str) -> Result<()> {
    with_global(|c| inbound_source_remove(c, tenant_id, source_id))
}

/// `due_inbound_sources` against the process DB.
pub fn due_inbound_sources_global(at: i64) -> Result<Vec<InboundSource>> {
    with_global(|c| due_inbound_sources(c, at))
}

/// Poll ONE source against the process DB, then — only if the poll succeeded —
/// advance its cursor + `last_poll_at`. The note writes and the transport phases
/// run with the process-DB lock RELEASED (each `note_upsert` takes its own short
/// guard); the cursor advance takes one final short guard. Mirrors
/// [`crate::ingest::scan_now_global`]'s lock discipline.
pub fn poll_inbound_source_global<F>(
    host: &PluginHost,
    source_id: &str,
    side_effects: &[String],
    connect: F,
    at: i64,
) -> Result<PollReport>
where
    F: FnOnce() -> Result<Box<dyn cyan_mcp::PluginTransport>>,
{
    let source = with_global(|c| inbound_source_get(c, source_id))?;
    let report = poll_inbound_source(host, &source, side_effects, connect)?;
    // Cursor advances ONLY after the notes above durably landed.
    with_global(|c| advance_poll_progress(c, source_id, report.next_cursor.as_deref(), at))?;
    Ok(report)
}

/// Poll every DUE inbound source against the process DB — the app tick's one call
/// (mirror of [`crate::ingest::scan_due_global`]). Builds the device plugin host,
/// resolves each source's `poll_inbound` side_effects from the installed registry,
/// spawns the plugin over a `StdioTransport`, polls, and routes. Per-source
/// failures are carried in the outcome, never thrown.
pub fn poll_inbound_due_global(at: i64) -> Result<Vec<PollDueOutcome>> {
    let due = due_inbound_sources_global(at)?;
    if due.is_empty() {
        return Ok(Vec::new());
    }

    let tenant = std::env::var("CYAN_TENANT_ID").unwrap_or_else(|_| "device".to_string());
    let host = PluginHost::new(
        std::sync::Arc::new(cyan_mcp::RecordingSink::new()) as std::sync::Arc<dyn cyan_mcp::EventSink>,
        std::sync::Arc::new(cyan_mcp::LogEmitter::new()) as std::sync::Arc<dyn cyan_mcp::Emitter>,
        std::sync::Arc::new(cyan_mcp::SystemClock::new()) as std::sync::Arc<dyn cyan_mcp::Clock>,
        cyan_mcp::BackoffPolicy {
            base: std::time::Duration::from_millis(500),
            max: std::time::Duration::from_secs(30),
            max_restarts: 3,
        },
        tenant.clone(),
    );
    let root = crate::mcp_host::plugins_root();

    let mut out = Vec::new();
    for source in due {
        match poll_one_due(&host, &root, &tenant, &source, at) {
            Ok(report) => out.push(PollDueOutcome {
                source_id: source.id,
                report: Some(report),
                error: None,
            }),
            Err(e) => out.push(PollDueOutcome {
                source_id: source.id,
                report: None,
                error: Some(e.to_string()),
            }),
        }
    }
    Ok(out)
}

/// Resolve one due source's `poll_inbound` side_effects from the installed
/// registry, build a lazy `StdioTransport` spawn, and poll+route it. Factored out
/// so [`poll_inbound_due_global`] stays a thin per-source loop.
fn poll_one_due(
    host: &PluginHost,
    root: &std::path::Path,
    tenant: &str,
    source: &InboundSource,
    at: i64,
) -> Result<PollReport> {
    // The tool must be installed; its manifest side_effects drive the gate (and a
    // correctly-authored cyan-email declares `poll_inbound` as `side_effects: []`).
    let side_effects = host
        .resolve_installed_tool(root, POLL_TOOL)
        .map_err(|e| anyhow!("resolve plugin tool {POLL_TOOL}: {e}"))?
        .map(|(_, tb)| tb.side_effects)
        .ok_or_else(|| {
            anyhow!(
                "tool '{POLL_TOOL}' is not installed (no bundle in {})",
                root.display()
            )
        })?;

    let bundle_dir = root.join(&source.plugin_id);
    let plugin_id = source.plugin_id.clone();
    let spawn_tenant = tenant.to_string();
    let connect = move || -> Result<Box<dyn cyan_mcp::PluginTransport>> {
        let config = crate::mcp_host::bundle_spawn_config(&plugin_id, &bundle_dir, &spawn_tenant)?;
        let mut transport = cyan_mcp::StdioTransport::new();
        // `spawn` comes from the `PluginTransport` trait.
        use cyan_mcp::PluginTransport as _;
        transport
            .spawn(&config)
            .map_err(|e| anyhow!("spawn plugin {plugin_id}: {e}"))?;
        Ok(Box::new(transport))
    };

    poll_inbound_source_global(host, &source.id, &side_effects, connect, at)
}
