//! THE JOIN (SESSION_JOIN §2) — the constitution flows through the proposer seam.
//!
//! Three wires, each tested here against isolated in-memory DBs (no live deps):
//!
//!   1. **`ProposeCtx` is populated at the spine's call site**:
//!      `propose_from_note[_with]` resolves the asset's loop board → the board's
//!      group → `constitution::effective_notes_with` and hands the proposer the
//!      merged constitution + preferences + the closed-vocab `tool_schemas`.
//!      The regex impl still ignores ctx (frozen contract); the LLM impl consumes it.
//!   2. **`ProposedOp.confidence` reaches the ledger as
//!      `ChangeEntry.params["confidence"]`** — exactly where the batch-confirm
//!      gate reads it (batch_confirm.rs). Absent = deterministic proposer.
//!   3. **`review_loop::board_constitution_markdown`** produces the Lens
//!      `constitution_markdown` context (None when the board has no rules —
//!      the request field stays absent, old-client behavior).
//!
//! Design mirrors review_loop_workflow_test.rs: explicit `&Connection`,
//! store-row assertions, no assertion weakened.

use std::sync::Mutex;

use cyan_backend::changelist;
use cyan_backend::ops_proposer::{OpsProposer, ProposeCtx, ProposedOp, ReviewNote};
use cyan_backend::review_loop::{self as rl};
use cyan_backend::review_state::{self as rv, Actor, ConfirmDecision};
use cyan_backend::{asset_registry, constitution};
use rusqlite::Connection;
use serde_json::json;

const T: &str = "tenant-join";
const MASTER: &str = "master-join-1";
const B: &str = "main";
const BOARD: &str = "board-join-1";
const WS: &str = "ws-join-1";

/// One in-memory DB with the four review migrations PLUS the notes/workspace
/// tables the constitution resolver reads (their storage.rs shapes).
fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    asset_registry::migrate(&conn).expect("migrate asset_registry");
    rl::migrate(&conn).expect("migrate review_loop");
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
            author_id TEXT, author_name TEXT, text TEXT NOT NULL,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
            scope TEXT NOT NULL DEFAULT 'board', kind TEXT NOT NULL DEFAULT 'editor-note',
            anchor_kind TEXT, anchor_id TEXT, origin_ref TEXT
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
    .expect("aux tables");
    conn
}

/// Register the master, publish v1, register the round-1 proxy, and ingest one
/// sensed note so `propose_from_note*` has an open note to propose from.
fn seed_with_note(conn: &Connection, proxy_ref: &str, note_text: &str) {
    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: MASTER.to_string(),
            tenant_id: T.to_string(),
            kind: Some("master".to_string()),
            fps: Some(24.0),
            duration_ms: Some(60_000),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: None,
            created_at: 0,
        },
    )
    .expect("register master");

    let level = changelist::append(
        conn,
        MASTER,
        B,
        changelist::ChangeEntry {
            id: String::new(),
            entry_hash: String::new(),
            asset_hash: MASTER.to_string(),
            tenant_id: T.to_string(),
            branch: None,
            track: Some("V1".to_string()),
            tc_in: 120,
            tc_out: Some(144),
            kind: "op".to_string(),
            op: Some("level".to_string()),
            params: json!({"gain_db": -2}),
            intent: "seed level".to_string(),
            source: Some("cyan".to_string()),
            source_ref: None,
            author: Some("editor-1".to_string()),
            role: Some("editor".to_string()),
            proposed_by: Some("human".to_string()),
            created_at: 0,
            state: String::new(),
            active: true,
            approved_by: None,
            approved_at: None,
            supersedes: None,
            superseded_by: None,
            seq: 0,
            depends_on: None,
            version_ref: None,
            outcome: None,
            updated_at: 0,
            updated_by: None,
                referent: None,
        region: None,
        intent_struct: None,
        capture_ctx: None,
},
    )
    .expect("append seed level");
    rv::start_draft(conn, T, MASTER, B).expect("start_draft");
    rv::confirm_op(conn, T, &level.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm seed");
    rv::publish_draft(conn, T, MASTER, B, Actor::Human).expect("publish_draft");
    let v1 = changelist::get(conn, T, MASTER, B)
        .expect("get")
        .head_version
        .expect("v1")
        .version_id;

    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: "proxy-join-1".to_string(),
            tenant_id: T.to_string(),
            kind: Some("proxy".to_string()),
            fps: Some(24.0),
            duration_ms: None,
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: Some("proxy-540p".to_string()),
            created_at: 0,
        },
    )
    .expect("register proxy");
    asset_registry::set_derivation(conn, T, "proxy-join-1", MASTER, &v1).expect("derivation");
    asset_registry::set_remote_ref(conn, T, "proxy-join-1", "frameio", proxy_ref)
        .expect("remote ref");

    let fixture = json!({ "data": [ { "id": "c-join-1", "text": note_text, "frame": 60 } ] });
    let ingest = rl::ingest_sense_result(conn, T, proxy_ref, &fixture).expect("ingest");
    assert_eq!(ingest.appended.len(), 1, "the note landed");
}

/// Anchor BOARD in group `g` (objects → workspaces, the board_get_group_id join).
fn anchor_board_in_group(conn: &Connection, g: &str) {
    conn.execute(
        "INSERT OR REPLACE INTO workspaces (id, group_id, name, created_at) VALUES (?1, ?2, 'Main', 0)",
        rusqlite::params![WS, g],
    )
    .expect("workspace row");
    conn.execute(
        "INSERT OR REPLACE INTO objects (id, workspace_id, type, name, created_at)
         VALUES (?1, ?2, 'whiteboard', 'Board', 0)",
        rusqlite::params![BOARD, WS],
    )
    .expect("board object row");
}

/// Insert one scoped note (the notes-model shape: board_id doubles as the anchor).
fn note_row(conn: &Connection, scope: &str, anchor: &str, kind: &str, text: &str, at: i64) {
    note_row_tenant(conn, T, scope, anchor, kind, text, at);
}

fn note_row_tenant(
    conn: &Connection,
    tenant: &str,
    scope: &str,
    anchor: &str,
    kind: &str,
    text: &str,
    at: i64,
) {
    conn.execute(
        "INSERT INTO notes (id, board_id, tenant_id, author_id, author_name, text,
                            created_at, updated_at, scope, kind)
         VALUES (?1, ?2, ?3, 'a1', 'alice', ?4, ?5, ?5, ?6, ?7)",
        rusqlite::params![
            format!("n-{scope}-{kind}-{at}"),
            anchor,
            tenant,
            text,
            at,
            scope,
            kind
        ],
    )
    .expect("note row");
}

/// A proposer that RECORDS the ctx it was handed (the seam assertion surface)
/// and answers from a script. `ProposedOp` is deliberately not `Clone` (the
/// frozen contract file stays untouched) — the script is a builder closure.
struct SpyProposer {
    seen: Mutex<Vec<(String, String, String)>>, // (constitution, preferences, tool_schemas)
    emit: Box<dyn Fn() -> Vec<ProposedOp> + Send + Sync>,
}

impl SpyProposer {
    fn new(emit: impl Fn() -> Vec<ProposedOp> + Send + Sync + 'static) -> Self {
        Self { seen: Mutex::new(Vec::new()), emit: Box::new(emit) }
    }
    fn seen(&self) -> Vec<(String, String, String)> {
        self.seen.lock().expect("spy lock").clone()
    }
}

impl OpsProposer for SpyProposer {
    fn propose_ops(&self, _note: &ReviewNote, ctx: &ProposeCtx) -> Vec<ProposedOp> {
        self.seen.lock().expect("spy lock").push((
            ctx.constitution.to_string(),
            ctx.preferences.to_string(),
            ctx.tool_schemas.to_string(),
        ));
        (self.emit)()
    }
}

fn scripted_op(confidence: Option<f32>) -> ProposedOp {
    ProposedOp {
        op: "mute".to_string(),
        params: json!({ "track": "A1" }),
        tc_in: Some(10),
        tc_out: Some(20),
        confidence,
        rationale: Some("scripted".to_string()),
    }
}

// ============================================================================
// 1. The spine's call site populates ProposeCtx from the board's effective notes.
// ============================================================================

#[test]
fn propose_ctx_carries_the_boards_effective_constitution() {
    let conn = db();
    seed_with_note(&conn, "file_ctx1", "the vibe is off in the middle");
    rl::register(&conn, T, BOARD, MASTER, B, 3).expect("register loop");
    anchor_board_in_group(&conn, "group-join-1");

    note_row(&conn, "tenant", T, "constitution", "Deliver -14 LUFS integrated.", 10);
    note_row(&conn, "group", "group-join-1", "constitution", "House cut: trim cold opens 3-5s.", 20);
    note_row(&conn, "board", BOARD, "constitution", "This board: VO sits -2 dB under music.", 30);
    note_row(&conn, "board", BOARD, "preference", "producer prefers cuts on action", 40);

    let spy = SpyProposer::new(Vec::new); // stays empty → escalates (valid outcome)
    let _ = rl::propose_from_note_with(&conn, T, MASTER, B, &spy);

    let seen = spy.seen();
    assert!(!seen.is_empty(), "the proposer ran at least once");
    let (constitution, preferences, tool_schemas) = &seen[0];

    // The merged constitution: precedence rule stated, all three scopes present,
    // most-specific LAST (board wins in-context).
    assert!(
        constitution.contains("Precedence: board > group > tenant"),
        "the merge states its precedence rule; got: {constitution}"
    );
    assert!(constitution.contains("Deliver -14 LUFS integrated."), "tenant rule present");
    assert!(constitution.contains("House cut: trim cold opens 3-5s."), "group rule present");
    assert!(constitution.contains("VO sits -2 dB under music."), "board rule present");
    let (t_pos, g_pos, b_pos) = (
        constitution.find("Deliver -14 LUFS").expect("tenant pos"),
        constitution.find("House cut").expect("group pos"),
        constitution.find("VO sits").expect("board pos"),
    );
    assert!(t_pos < g_pos && g_pos < b_pos, "tenant → group → board, most-specific last");

    assert!(
        preferences.contains("producer prefers cuts on action"),
        "preferences ride their own merged string; got: {preferences}"
    );

    // The closed-vocab tool schemas hand every impl the SAME op vocabulary.
    for op in ["trim", "level", "mute", "fade", "delete"] {
        assert!(tool_schemas.contains(op), "tool_schemas names '{op}'; got: {tool_schemas}");
    }
}

#[test]
fn propose_ctx_is_empty_when_no_loop_anchors_the_asset() {
    let conn = db();
    seed_with_note(&conn, "file_ctx2", "make it pop");
    // NO rl::register — the asset drives no board loop; notes exist but are
    // unreachable without a board anchor. The ctx must be EMPTY, not a guess.
    note_row(&conn, "board", BOARD, "constitution", "unreachable rule", 10);

    let spy = SpyProposer::new(Vec::new);
    let _ = rl::propose_from_note_with(&conn, T, MASTER, B, &spy);

    let seen = spy.seen();
    assert!(!seen.is_empty(), "the proposer ran");
    assert_eq!(seen[0].0, "", "no loop → empty constitution (never a guess)");
    assert_eq!(seen[0].1, "", "no loop → empty preferences");
    assert!(!seen[0].2.is_empty(), "tool_schemas is static — always supplied");
}

// ============================================================================
// 2. ProposedOp.confidence lands in ChangeEntry.params["confidence"] — the
//    batch-confirm gate's read location. Absent = deterministic proposer.
// ============================================================================

#[test]
fn proposer_confidence_lands_in_change_entry_params() {
    let conn = db();
    seed_with_note(&conn, "file_conf1", "kill the hum");

    let spy = SpyProposer::new(|| vec![scripted_op(Some(0.9))]);
    let prop = rl::propose_from_note_with(&conn, T, MASTER, B, &spy)
        .expect("scripted proposal lands");
    let c = prop.params["confidence"].as_f64().expect("confidence in params");
    assert!((c - 0.9).abs() < 1e-6, "params[\"confidence\"] carries the proposer's 0.9, got {c}");
    assert_eq!(prop.params["track"], json!("A1"), "op params are preserved alongside");

    // The persisted row (not just the return) carries it — batch_confirm reads
    // the STORE.
    let stored = changelist::get_entry(&conn, T, &prop.id).expect("stored entry");
    let sc = stored.params["confidence"].as_f64().expect("stored confidence");
    assert!((sc - 0.9).abs() < 1e-6);
}

#[test]
fn absent_confidence_stays_absent_deterministic_contract() {
    let conn = db();
    seed_with_note(&conn, "file_conf2", "kill the hum");

    let spy = SpyProposer::new(|| vec![scripted_op(None)]);
    let prop = rl::propose_from_note_with(&conn, T, MASTER, B, &spy)
        .expect("scripted proposal lands");
    assert!(
        prop.params.get("confidence").is_none(),
        "no proposer confidence → params key ABSENT (batch_confirm's deterministic read)"
    );
}

#[test]
fn regex_proposer_full_confidence_reaches_params() {
    let conn = db();
    seed_with_note(&conn, "file_conf3", "Trim 12 frames off the tail — it hangs too long");

    let prop = rl::propose_from_note(&conn, T, MASTER, B).expect("regex trim proposes");
    assert_eq!(prop.op.as_deref(), Some("trim"));
    assert_eq!(
        prop.params["confidence"].as_f64(),
        Some(1.0),
        "the regex impl's 1.0 rides params — the mechanical batch tier reads it"
    );
    // Conform-compat: the op params the conform consumes are still intact.
    assert_eq!(prop.params["edge"], json!("tail"));
    assert_eq!(prop.params["frames"], json!(12));
}

// ============================================================================
// 3. The Lens context wire: board_constitution_markdown → constitution_markdown.
// ============================================================================

#[test]
fn board_constitution_markdown_merges_for_the_lens() {
    let conn = db();
    anchor_board_in_group(&conn, "group-join-lens");
    // In this engine tenant == group id: `board_tenant` resolves the board's
    // group and queries notes under it — the fixture seeds them there.
    let g = "group-join-lens";
    note_row_tenant(&conn, g, "tenant", g, "constitution", "Tenant delivery spec.", 10);
    note_row_tenant(&conn, g, "board", BOARD, "constitution", "Board rule: -2 dB on VO.", 20);

    let md = rl::board_constitution_markdown(&conn, BOARD)
        .expect("a board with rules yields the merged markdown");
    assert!(md.contains("Precedence: board > group > tenant"));
    assert!(md.contains("Tenant delivery spec."));
    assert!(md.contains("Board rule: -2 dB on VO."));
}

#[test]
fn board_constitution_markdown_none_when_no_rules() {
    let conn = db();
    anchor_board_in_group(&conn, "group-join-empty");
    assert!(
        rl::board_constitution_markdown(&conn, BOARD).is_none(),
        "no rules → None: the Lens request field stays ABSENT (old-client shape)"
    );
}

// ============================================================================
// 3b. The conform step's LOOP ROUTING resolves the BOARD'S tenant (E2E run-3
//     finding): the loop rows live under the board's GROUP tenant, but the
//     dispatch resolved under the device tenant — `current_proxy_ref` found
//     nothing, the conform fell to the PLAIN bind (no fps, no loop
//     bookkeeping), and the "applied" trim silently cut NOTHING (the 25fps
//     schema default put the tail bound past the media end).
// ============================================================================

#[test]
fn current_proxy_ref_for_board_resolves_the_group_tenant() {
    let conn = db();
    // The loop + assets live under the GROUP tenant (tenant == group id in
    // this engine); the board anchors in that group.
    let g = "group-join-conform";
    anchor_board_in_group(&conn, g);

    asset_registry::upsert(
        &conn,
        &asset_registry::Asset {
            hash: MASTER.to_string(),
            tenant_id: g.to_string(),
            kind: Some("master".to_string()),
            fps: Some(30.0),
            duration_ms: Some(10_000),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: None,
            created_at: 0,
        },
    )
    .expect("register master");
    asset_registry::upsert(
        &conn,
        &asset_registry::Asset {
            hash: "proxy-conform-1".to_string(),
            tenant_id: g.to_string(),
            kind: Some("proxy".to_string()),
            fps: Some(30.0),
            duration_ms: None,
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: Some("proxy-540p".to_string()),
            created_at: 0,
        },
    )
    .expect("register proxy");
    asset_registry::set_derivation(&conn, g, "proxy-conform-1", MASTER, "v-1")
        .expect("derivation");
    asset_registry::set_remote_ref(&conn, g, "proxy-conform-1", "frameio", "file_conform_1")
        .expect("remote ref");
    rl::register(&conn, g, BOARD, MASTER, B, 3).expect("register loop");

    // The board-keyed resolver finds the loop's published proxy under the
    // board's OWN tenant — the routing the conform dispatch rides.
    let got = rl::current_proxy_ref_for_board(&conn, BOARD)
        .expect("query ok")
        .expect("the board's active loop resolves its published proxy");
    assert_eq!(got, "file_conform_1");

    // Under the WRONG tenant (the old device-tenant bug) the same query finds
    // nothing — the regression this test pins.
    assert!(
        rl::current_proxy_ref(&conn, "device", BOARD)
            .expect("query ok")
            .is_none(),
        "the loop is invisible under the device tenant — resolving there routed \
         the conform to the plain bind (the run-3 silent no-op trim)"
    );
}

// ============================================================================
// 4. The conn-passing resolver variant (the deadlock-safe JOIN seam) agrees
//    with the tenant⊕group⊕board contract on an isolated connection.
// ============================================================================

#[test]
fn effective_notes_with_conn_merges_scopes_board_last() {
    let conn = db();
    note_row(&conn, "tenant", T, "constitution", "tenant-wide rule", 10);
    note_row(&conn, "group", "g-1", "constitution", "group rule", 20);
    note_row(&conn, "board", "b-1", "constitution", "board rule", 30);
    note_row(&conn, "board", "b-1", "preference", "board pref", 40);

    let eff = constitution::effective_notes_with(&conn, T, Some("g-1"), "b-1")
        .expect("resolve");
    assert!(eff.constitution.contains("tenant-wide rule"));
    assert!(eff.constitution.contains("group rule"));
    assert!(eff.constitution.contains("board rule"));
    assert!(eff.preferences.contains("board pref"));

    // Tenant isolation: another tenant sees NOTHING of these rows.
    let other = constitution::effective_notes_with(&conn, "tenant-other", Some("g-1"), "b-1")
        .expect("resolve other");
    assert_eq!(other.constitution, "");
    assert_eq!(other.preferences, "");
}
