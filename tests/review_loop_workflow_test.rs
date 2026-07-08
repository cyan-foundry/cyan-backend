//! The review LOOP as a WORKFLOW (CYAN_CHANGELIST_STORE_AND_REVIEW_LOOP.md
//! Part 2 + engine delta #3; CYAN_REVIEW_LOOP_TRANSITION_CONTRACT.md).
//!
//! Proves the culmination layer end-to-end, fixture-driven (canned
//! `frameio.list_comments` JSON — no live API, no network):
//!
//!   * **SENSE → ledger glue** — a SENSE step result ingests: proxy ref →
//!     `asset_registry` → conform_map remap to MASTER coords (+ `observed`
//!     provenance), own write-backs dropped (`is_own_source_ref` IN USE),
//!     dedup on `entry_hash`, `kind=note, source=frameio, source_ref=<id>`,
//!     first new note advances IN_REVIEW → NOTES_IN.
//!   * **The loop controller** — parks at publish (IN_REVIEW), resumes on new
//!     notes (NOTES_IN), exits on external approval (outcome=shipped), and hits
//!     the max_rounds cap as a durable HUMAN ask — never a silent stop.
//!   * **Rounds as sequential runs** — each round = a run, stamped with
//!     `review_state.round`.
//!   * **The template** — "Frame.io review loop" instantiates per asset from
//!     the builtin seed set and compiles (its own global-storage test below).
//!
//! Design mirrors the sibling suites: every store/state op takes an explicit
//! `&Connection` on an isolated in-memory DB; assertions are synchronous on the
//! store's own rows (the oracle), never logs. No assertion is weakened.

use cyan_backend::changelist::{self, ChangeEntry};
use cyan_backend::review_loop::{self as rl, LoopDecision};
use cyan_backend::review_state::{self as rv, Actor, ConfirmDecision};
use cyan_backend::{asset_registry, templates};
use rusqlite::Connection;
use serde_json::json;

const T: &str = "tenant-loop";
const MASTER: &str = "master-asset-1";
const B: &str = "main";
const BOARD: &str = "board-loop-1";

/// One in-memory DB with ALL four migrations — the shared oracle.
fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory db");
    changelist::migrate(&conn).expect("migrate changelist");
    rv::migrate(&conn).expect("migrate review_state");
    asset_registry::migrate(&conn).expect("migrate asset_registry");
    rl::migrate(&conn).expect("migrate review_loop");
    conn
}

/// A mechanical op entry on the master. `append` fills id/hash/seq/state.
fn op_entry(op: &str, tc_in: i64, tc_out: Option<i64>, params: serde_json::Value) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: MASTER.to_string(),
        tenant_id: T.to_string(),
        branch: None,
        track: Some("V1".to_string()),
        tc_in,
        tc_out,
        kind: "op".to_string(),
        op: Some(op.to_string()),
        params,
        intent: format!("{op} at {tc_in}"),
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
    }
}

/// Register the master, seed a NON-structural op, publish v1 (IN_REVIEW,
/// identity map), and register the round-1 proxy under `frameio ref proxy_ref`.
/// Returns the v1 version id.
fn seed_published_round1(conn: &Connection, proxy_ref: &str) -> String {
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

    // A non-structural seed op → v1's conform map is the identity.
    let level = changelist::append(conn, MASTER, B, op_entry("level", 120, Some(144), json!({"gain_db": -2})))
        .expect("append level");
    rv::start_draft(conn, T, MASTER, B).expect("start_draft");
    rv::confirm_op(conn, T, &level.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm level");
    rv::publish_draft(conn, T, MASTER, B, Actor::Human).expect("publish_draft");

    let v1 = changelist::get(conn, T, MASTER, B)
        .expect("get")
        .head_version
        .expect("v1 exists");
    register_proxy(conn, "proxy-asset-1", &v1.version_id, proxy_ref);
    v1.version_id
}

/// Register a proxy rendered from `version_id`, published as `frameio_ref`.
fn register_proxy(conn: &Connection, proxy_hash: &str, version_id: &str, frameio_ref: &str) {
    asset_registry::upsert(
        conn,
        &asset_registry::Asset {
            hash: proxy_hash.to_string(),
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
    asset_registry::set_derivation(conn, T, proxy_hash, MASTER, version_id).expect("derivation");
    asset_registry::set_remote_ref(conn, T, proxy_hash, "frameio", frameio_ref).expect("remote ref");
}

/// The canned round-1 `list_comments` SENSE result: one mechanical note, one
/// creative note, one of OUR OWN write-backs, and one malformed row (no text).
fn round1_fixture() -> serde_json::Value {
    json!({
        "data": [
            { "id": "c-mech-1", "text": "music too loud at 0:42", "frame": 1008,
              "owner": { "name": "producer-paula" } },
            { "id": "c-creative-1", "text": "the opening feels rushed", "timestamp": 0 },
            { "id": "c-own-1", "text": "Uploaded v1 proxy for review", "frame": 0 },
            { "id": "c-bad-1" }
        ]
    })
}

/// Drive one full revision round from NOTES_IN back to IN_REVIEW: agent
/// proposes an op, human confirms, batch-confirms, conform runs, human
/// publishes. Returns the new head version id.
fn drive_round_to_publish(conn: &Connection, entry: ChangeEntry, max_rounds: i64) -> String {
    let prop = rv::propose_op(conn, MASTER, B, entry, Actor::Agent).expect("propose");
    rv::confirm_op(conn, T, &prop.id, None, ConfirmDecision::Approve, Actor::Human)
        .expect("confirm op");
    rv::confirm_notes(conn, T, MASTER, B, Actor::Human).expect("confirm_notes");
    rv::conform_run(conn, T, MASTER, B, Actor::Auto).expect("conform_run");
    rv::publish_proxy(conn, T, MASTER, B, Actor::Human, max_rounds).expect("publish_proxy");
    changelist::get(conn, T, MASTER, B)
        .expect("get")
        .head_version
        .expect("head version")
        .version_id
}

// ============================================================================
// 1. SENSE → ledger glue: remap + observed provenance + echo filter + dedup +
//    the IN_REVIEW → NOTES_IN advance.
// ============================================================================

#[test]
fn sense_result_ingests_remapped_deduped_and_filters_own_refs() {
    let conn = db();
    seed_published_round1(&conn, "file_p1");

    // The PUBLISH actuator recorded its own comment write-back — the echo the
    // sensor must drop.
    changelist::record_own_ref(&conn, T, "frameio", "c-own-1").expect("own ref");

    let ingest = rl::ingest_sense_result(&conn, T, "file_p1", &round1_fixture()).expect("ingest");

    // Exactly the two REAL comments landed; the echo and the malformed row are
    // counted, never silent.
    assert_eq!(ingest.appended.len(), 2, "two real comments ingested");
    assert_eq!(ingest.own_refs_skipped, 1, "our own write-back was echo-suppressed");
    assert_eq!(ingest.malformed, 1, "the textless row is counted malformed");
    assert_eq!(ingest.deduped, 0);
    assert_eq!(ingest.unmappable, 0);

    // The mechanical note: kind=note, source=frameio, source_ref=<comment id>,
    // MASTER coords (v1 has no structural op → identity), observed provenance.
    let mech = ingest
        .appended
        .iter()
        .find(|e| e.source_ref.as_deref() == Some("c-mech-1"))
        .expect("mechanical note present");
    assert_eq!(mech.kind, "note");
    assert!(mech.op.is_none(), "a comment is never guessed into an op");
    assert_eq!(mech.source.as_deref(), Some("frameio"));
    assert_eq!(mech.intent, "music too loud at 0:42");
    assert_eq!(mech.tc_in, 1008, "round-1 identity map: master == proxy frame");
    assert_eq!(mech.params["observed"]["proxy_ref"], json!("file_p1"));
    assert_eq!(mech.params["observed"]["tc_in"], json!(1008));
    assert_eq!(mech.author.as_deref(), Some("producer-paula"));
    assert_eq!(mech.state, "proposed");

    // The creative note anchored via `timestamp` fallback at frame 0.
    let creative = ingest
        .appended
        .iter()
        .find(|e| e.source_ref.as_deref() == Some("c-creative-1"))
        .expect("creative note present");
    assert_eq!(creative.tc_in, 0);

    // First new note advanced the machine: IN_REVIEW → NOTES_IN (AUTO).
    assert_eq!(
        ingest.state.as_ref().expect("state").state,
        "NOTES_IN",
        "sensor ingest advances IN_REVIEW → NOTES_IN"
    );

    // Re-ingesting the SAME result is a no-op: dedup rides entry_hash.
    let before = changelist::get(&conn, T, MASTER, B).expect("get").entries.len();
    let again = rl::ingest_sense_result(&conn, T, "file_p1", &round1_fixture()).expect("re-ingest");
    assert_eq!(again.appended.len(), 0, "identical comments append nothing");
    assert_eq!(again.deduped, 2, "both real comments dedup by entry_hash");
    assert_eq!(again.own_refs_skipped, 1);
    let after = changelist::get(&conn, T, MASTER, B).expect("get").entries.len();
    assert_eq!(before, after, "re-ingest adds no rows");
}

// ============================================================================
// 1b. TIER 3 (found live): a reviewer leaves a MECHANICAL note and then a
//     CREATIVE one in the same round — the newest note is creative, but the
//     agent must still propose from the mechanical one; escalate ONLY when no
//     open note is mechanical.
// ============================================================================

#[test]
fn propose_picks_the_mechanical_note_even_when_the_newest_is_creative() {
    let conn = db();
    seed_published_round1(&conn, "file_pmix");

    // The mechanical trim lands FIRST, the creative reaction SECOND (newest).
    let fixture = json!({
        "data": [
            { "id": "c-mix-mech", "text": "Please trim 12 frames off the tail — it hangs too long",
              "frame": 60 },
            { "id": "c-mix-creative", "text": "I don't love the vibe of the middle section — thoughts?",
              "timestamp": 0 }
        ]
    });
    let ingest = rl::ingest_sense_result(&conn, T, "file_pmix", &fixture).expect("ingest");
    assert_eq!(ingest.appended.len(), 2, "both notes land");

    // The OLD behavior looked only at the newest (creative) note and escalated,
    // so the trim never proposed — the live Tier-3 gate caught it.
    let prop = rl::propose_from_note(&conn, T, MASTER, B)
        .expect("the mechanical note must yield a proposal despite a newer creative note");
    assert_eq!(prop.op.as_deref(), Some("trim"));
    assert_eq!(prop.params["frames"], json!(12));
    assert!(
        prop.intent.contains("trim 12 frames off the tail")
            || prop.intent.contains("Trim 12 frames off the tail")
            || prop.intent.to_lowercase().contains("trim 12 frames"),
        "the proposal carries the note's intent; got {:?}",
        prop.intent
    );

    // With ONLY creative notes open, propose still ESCALATES (never guesses) —
    // and the error names every escalated note.
    let conn2 = db();
    seed_published_round1(&conn2, "file_pcreative");
    let creative_only = json!({
        "data": [
            { "id": "c-only-creative", "text": "the ending needs more energy", "timestamp": 0 }
        ]
    });
    rl::ingest_sense_result(&conn2, T, "file_pcreative", &creative_only).expect("ingest");
    let err = rl::propose_from_note(&conn2, T, MASTER, B)
        .expect_err("creative-only notes must escalate");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("escalate to the human") && msg.contains("the ending needs more energy"),
        "the escalation names the note; got: {msg}"
    );
}

// ============================================================================
// 2. Round 1 is the identity map; round 2 REMAPS once a structural op landed
//    in the v2 conform plan (the burst-2 tc-remap, exercised through the glue).
// ============================================================================

#[test]
fn round1_identity_map_round2_remaps() {
    let conn = db();
    seed_published_round1(&conn, "file_p1");

    // Round 1: identity — proxy frame 1008 lands at master 1008.
    let r1 = rl::ingest_sense_result(&conn, T, "file_p1", &round1_fixture()).expect("ingest r1");
    let mech = r1
        .appended
        .iter()
        .find(|e| e.source_ref.as_deref() == Some("c-mech-1"))
        .expect("round-1 note");
    assert_eq!(mech.tc_in, 1008, "round 1: identity map");

    // Round 2: a STRUCTURAL op (lift master [48,72) — 24 frames gone from the
    // proxy) is confirmed and conformed into v2; the v2 proxy publishes.
    let v2 = drive_round_to_publish(
        &conn,
        op_entry("lift", 48, Some(72), json!({})),
        rv::DEFAULT_MAX_ROUNDS,
    );
    register_proxy(&conn, "proxy-asset-2", &v2, "file_p2");

    // A round-2 comment at proxy frame 100 sits PAST the lifted range: master
    // coordinate = 100 + 24 = 124. The raw proxy observation is preserved.
    let r2_fixture = json!({
        "data": [
            { "id": "c-r2-1", "text": "swap the music sting here", "frame": 100 }
        ]
    });
    let r2 = rl::ingest_sense_result(&conn, T, "file_p2", &r2_fixture).expect("ingest r2");
    assert_eq!(r2.appended.len(), 1);
    let note = &r2.appended[0];
    assert_eq!(note.tc_in, 124, "round 2: proxy 100 remaps past the 24-frame lift to master 124");
    assert_eq!(note.params["observed"]["proxy_ref"], json!("file_p2"));
    assert_eq!(note.params["observed"]["tc_in"], json!(100), "raw proxy observation preserved");
    assert_eq!(
        r2.state.as_ref().expect("state").state,
        "NOTES_IN",
        "round-2 notes advance the machine again"
    );
}

// ============================================================================
// 3. The loop parks at publish-done and resumes when SENSE brings new notes.
// ============================================================================

#[test]
fn loop_pauses_on_publish_resumes_on_new_notes() {
    let conn = db();
    seed_published_round1(&conn, "file_p1");
    rl::register(&conn, T, BOARD, MASTER, B, rv::DEFAULT_MAX_ROUNDS).expect("register loop");

    // Round published (IN_REVIEW) → the run PARKS.
    assert_eq!(
        rl::tick(&conn, T, BOARD, MASTER).expect("tick parked"),
        LoopDecision::Park { round: 0 },
        "published round parks the loop"
    );

    // SENSE brings new notes → NOTES_IN → the loop RESUMES (INTERPRET/CONFIRM).
    changelist::record_own_ref(&conn, T, "frameio", "c-own-1").expect("own ref");
    let ingest = rl::ingest_sense_result(&conn, T, "file_p1", &round1_fixture()).expect("ingest");
    assert!(!ingest.appended.is_empty(), "new notes landed");
    assert_eq!(
        rl::tick(&conn, T, BOARD, MASTER).expect("tick resumed"),
        LoopDecision::Resume { round: 0 },
        "new notes resume the loop"
    );

    // Mid-flight machinery (CONFORMING) is the machinery's business, not the
    // controller's.
    rv::confirm_notes(&conn, T, MASTER, B, Actor::Human).expect("confirm_notes");
    assert_eq!(
        rl::tick(&conn, T, BOARD, MASTER).expect("tick conforming"),
        LoopDecision::Working { state: "CONFORMING".to_string() }
    );
}

// ============================================================================
// 4. The round cap forces a HUMAN escalation as a durable ask on the ledger —
//    never a silent stop.
// ============================================================================

#[test]
fn max_rounds_escalates_as_ask_never_silent() {
    let conn = db();
    seed_published_round1(&conn, "file_p1");
    rl::register(&conn, T, BOARD, MASTER, B, 1).expect("register loop, cap 1");

    // Round 1 runs to publish (round == 1 — the cap is now spent).
    rl::ingest_sense_result(&conn, T, "file_p1", &round1_fixture()).expect("ingest r1");
    assert_eq!(rl::tick(&conn, T, BOARD, MASTER).expect("tick"), LoopDecision::Resume { round: 0 });
    drive_round_to_publish(&conn, op_entry("mute", 200, Some(224), json!({})), 1);
    assert_eq!(rl::tick(&conn, T, BOARD, MASTER).expect("tick"), LoopDecision::Park { round: 1 });

    // MORE notes arrive — resuming would publish round 2 > cap 1 → ESCALATE.
    let r2_fixture = json!({ "data": [ { "id": "c-r2-x", "text": "tighten the tail", "frame": 40 } ] });
    rl::ingest_sense_result(&conn, T, "file_p1", &r2_fixture).expect("ingest r2");
    let decision = rl::tick(&conn, T, BOARD, MASTER).expect("tick escalate");
    let LoopDecision::Escalate { round, cap, ask_entry_id } = decision else {
        panic!("expected Escalate, got {decision:?}");
    };
    assert_eq!((round, cap), (1, 1));

    // The ask is DURABLE: a real ledger note (source=cyan) a human must act on.
    let ask = changelist::get_entry(&conn, T, &ask_entry_id).expect("ask entry");
    assert_eq!(ask.kind, "note");
    assert_eq!(ask.source.as_deref(), Some("cyan"));
    assert_eq!(ask.params["ask"], json!("max_rounds_reached"));
    assert!(ask.intent.contains("human"), "the ask asks for a human decision");

    // The loop is parked `escalated`…
    let lp = rl::get_loop(&conn, T, BOARD, MASTER).expect("get").expect("loop");
    assert_eq!(lp.status, "escalated");

    // …and STAYS loud: a re-tick reports the SAME escalation (content-addressed
    // ask — no spam, no silence).
    let again = rl::tick(&conn, T, BOARD, MASTER).expect("re-tick");
    assert_eq!(
        again,
        LoopDecision::Escalate { round: 1, cap: 1, ask_entry_id: ask_entry_id.clone() },
        "re-tick repeats the escalation with the same durable ask"
    );
    let asks = changelist::get(&conn, T, MASTER, B)
        .expect("get")
        .entries
        .iter()
        .filter(|e| e.params.get("ask") == Some(&json!("max_rounds_reached")))
        .count();
    assert_eq!(asks, 1, "the ask dedups by content — exactly one on the ledger");
}

// ============================================================================
// 5. External approval exits the loop and ships the cut.
// ============================================================================

#[test]
fn external_approval_exits_loop_sets_outcome_shipped() {
    let conn = db();
    seed_published_round1(&conn, "file_p1");
    rl::register(&conn, T, BOARD, MASTER, B, rv::DEFAULT_MAX_ROUNDS).expect("register loop");

    // The producer approves in Frame.io — the sensor fires the AUTO transition.
    rv::version_approved(&conn, T, MASTER, B, Actor::Auto).expect("version_approved");

    let decision = rl::tick(&conn, T, BOARD, MASTER).expect("tick exit");
    assert_eq!(decision, LoopDecision::Exit { outcome: "shipped".to_string() });

    // The delivered cut carries the taste label…
    let head = changelist::get(&conn, T, MASTER, B)
        .expect("get")
        .head_version
        .expect("head");
    assert_eq!(head.outcome, "shipped", "external approval ships the head version");

    // …and the loop is closed, idempotently.
    let lp = rl::get_loop(&conn, T, BOARD, MASTER).expect("get").expect("loop");
    assert_eq!(lp.status, "exited");
    assert_eq!(lp.outcome.as_deref(), Some("shipped"));
    assert_eq!(
        rl::tick(&conn, T, BOARD, MASTER).expect("re-tick"),
        LoopDecision::Exit { outcome: "shipped".to_string() },
        "an exited loop stays exited"
    );
}

// ============================================================================
// 6. Rounds are SEQUENTIAL RUNS: a fresh run id per round, stamped with
//    `review_state.round` — never one run id reused across rounds.
// ============================================================================

#[test]
fn rounds_are_sequential_runs_with_round_stamp() {
    let conn = db();
    seed_published_round1(&conn, "file_p1");
    rl::register(&conn, T, BOARD, MASTER, B, rv::DEFAULT_MAX_ROUNDS).expect("register loop");

    // The run that published the draft round parks at round 0.
    rl::record_round_run(&conn, T, BOARD, MASTER, "run-a").expect("record run-a");

    // Round 1: notes → resume → confirm → conform → publish (round == 1).
    rl::ingest_sense_result(&conn, T, "file_p1", &round1_fixture()).expect("ingest r1");
    drive_round_to_publish(&conn, op_entry("mute", 200, Some(224), json!({})), rv::DEFAULT_MAX_ROUNDS);
    rl::record_round_run(&conn, T, BOARD, MASTER, "run-b").expect("record run-b");

    // Round 2: another pass (round == 2).
    let r2 = json!({ "data": [ { "id": "c-seq-1", "text": "fade the outro", "frame": 500 } ] });
    rl::ingest_sense_result(&conn, T, "file_p1", &r2).expect("ingest r2");
    drive_round_to_publish(&conn, op_entry("fade", 500, Some(524), json!({"dir":"out","frames":8})), rv::DEFAULT_MAX_ROUNDS);
    rl::record_round_run(&conn, T, BOARD, MASTER, "run-c").expect("record run-c");

    let runs = rl::runs_for(&conn, T, BOARD, MASTER).expect("runs");
    assert_eq!(runs.len(), 3, "three rounds = three runs");
    assert_eq!(
        runs.iter().map(|r| r.run_id.as_str()).collect::<Vec<_>>(),
        vec!["run-a", "run-b", "run-c"],
        "each round is its OWN run — a run id is never reused across rounds"
    );
    assert_eq!(
        runs.iter().map(|r| r.round).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "runs carry the review_state.round stamp, strictly sequential"
    );

    // Recording the same run twice is idempotent (no duplicate rows).
    rl::record_round_run(&conn, T, BOARD, MASTER, "run-c").expect("re-record run-c");
    assert_eq!(rl::runs_for(&conn, T, BOARD, MASTER).expect("runs").len(), 3);
}

// ============================================================================
// 7. The template instantiates a compilable loop (global-storage test — the
//    template picker → clone_to_board → compile → loop registration path).
// ============================================================================

mod template {
    use super::*;
    use std::path::Path;
    use std::sync::Once;

    static DB_INIT: Once = Once::new();

    /// Init the process-global storage once over a temp DB (same pattern as
    /// `templates_test.rs`) — `clone_to_board`/`compile_pipeline` drive it.
    fn ensure_db() {
        DB_INIT.call_once(|| {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("review_loop_template.db");
            init_base_schema(&path).expect("base schema");
            cyan_backend::storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
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

    #[test]
    fn template_instantiates_compilable_loop() {
        ensure_db();
        let (group, board) = ("rl-tpl-grp", "rl-tpl-board");
        let now = 1_700_000_000i64;
        cyan_backend::storage::group_insert_simple(group, "Loop Group", "folder.fill", "#00AEEF")
            .expect("group");
        cyan_backend::storage::workspace_insert_simple(&format!("{group}-ws"), group, "Main")
            .expect("workspace");
        cyan_backend::storage::board_insert_simple(board, &format!("{group}-ws"), "Review Loop", now)
            .expect("board");

        // The seed is in the picker for every tenant, with the @frameio.* refs
        // in its step text and the frameio plugin bound to the SENSE/PUBLISH steps.
        let seed = templates::list_templates(group)
            .into_iter()
            .find(|t| t.name == templates::SEED_FRAMEIO_REVIEW_LOOP_NAME)
            .expect("Frame.io review loop seed present");
        assert_eq!(seed.id, "builtin:frameio-review-loop");
        assert!(
            seed.steps.iter().any(|s| s.text.contains("@frameio.list_comments")),
            "the SENSE step references @frameio.list_comments"
        );
        assert!(
            seed.steps.iter().any(|s| s.text.contains("@frameio.upload")),
            "the PUBLISH steps reference @frameio.upload"
        );
        assert_eq!(
            seed.steps.iter().filter(|s| s.plugin.as_deref() == Some("frameio")).count(),
            3,
            "the three @frameio steps bind the frameio plugin"
        );
        // Both external sends are human-gated in the authored text (the
        // transition contract: external_send is ALWAYS human-fired).
        assert_eq!(
            seed.steps
                .iter()
                .filter(|s| s.text.contains("@frameio.upload") && s.text.contains("/needs-approval"))
                .count(),
            2,
            "both uploads carry the /needs-approval gate"
        );

        // Instantiate per asset: clone → real authorable step cells, in order.
        let created = templates::clone_to_board(&seed.id, board, group).expect("clone");
        assert_eq!(created.len(), seed.steps.len(), "one cell per template step");
        for cell in &created {
            assert_eq!(cell.cell_type, "step", "cloned cells are the W1 step primitive");
        }

        // The clone COMPILES — the instantiated loop is a runnable workflow.
        let plan = cyan_backend::pipeline::compile_pipeline(board).expect("compile cloned loop");
        assert_eq!(
            plan["total_cells"].as_u64(),
            Some(seed.steps.len() as u64),
            "every cloned step is in the compiled plan"
        );

        // And the loop registers on (board, asset) against the SAME global DB —
        // the controller is live for the instantiated workflow.
        {
            let lock = cyan_backend::storage::db().lock().expect("db lock");
            let lp = rl::register(&lock, group, board, "master-tpl-asset", "main", 5)
                .expect("register loop");
            assert_eq!(lp.status, "active");
            assert_eq!(
                rl::tick(&lock, group, board, "master-tpl-asset").expect("tick"),
                LoopDecision::Working { state: "DRAFT".to_string() },
                "a freshly instantiated loop sits in DRAFT (authoring)"
            );
        }
    }
}

// ============================================================================
// 8. Frame.io V4 `Comment.timestamp` is a oneOf — int FRAMES or "HH:MM:SS:FF"
//    timecode (verified against the live V4 openapi.json, 2026-07-03). String
//    timecodes resolve through the proxy's fps; anchors that are PRESENT but
//    unresolvable are COUNTED malformed — never silently pinned to frame 0
//    (the pre-fix bug). Frame.io `duration` (int FRAMES) becomes the range end.
// ============================================================================

#[test]
fn sense_parse_timestamp_oneof_int_and_timecode() {
    let result = json!({ "data": [
        { "id": "c-int",  "text": "int frames",     "timestamp": 1008 },
        { "id": "c-tc",   "text": "string timecode","timestamp": "00:00:02:12" },
        { "id": "c-none", "text": "general comment" },
        { "id": "c-null", "text": "null anchor",    "timestamp": null },
    ]});
    let (comments, malformed) = rl::parse_sense_comments(&result, Some(24.0));
    assert_eq!(malformed, 0);
    let by_id = |id: &str| comments.iter().find(|c| c.id == id).expect(id);
    assert_eq!(by_id("c-int").frame, 1008, "int variant passes through as frames");
    assert_eq!(by_id("c-tc").frame, 60, "00:00:02:12 @ 24fps = 2*24+12");
    assert_eq!(by_id("c-none").frame, 0, "no anchor = general file comment");
    assert_eq!(by_id("c-null").frame, 0, "null anchor = general file comment");
}

#[test]
fn sense_parse_timecode_ntsc_base_rounds_up() {
    // 23.976 uses the nominal integer timecode base (24) — SMPTE NDF math.
    let result = json!({ "data": [
        { "id": "c1", "text": "one second in", "timestamp": "00:00:01:00" },
    ]});
    let (comments, malformed) = rl::parse_sense_comments(&result, Some(23.976));
    assert_eq!(malformed, 0);
    assert_eq!(comments[0].frame, 24);
}

#[test]
fn sense_parse_unresolvable_anchor_is_malformed_never_frame_zero() {
    let cases = [
        // timecode string but NO fps on the proxy
        (json!({ "data": [ { "id": "c", "text": "t", "timestamp": "00:00:02:12" } ] }), None),
        // drop-frame form — unsupported, surfaced not guessed
        (json!({ "data": [ { "id": "c", "text": "t", "timestamp": "00;00;02;12" } ] }), Some(29.97)),
        // garbage string
        (json!({ "data": [ { "id": "c", "text": "t", "timestamp": "at the top" } ] }), Some(24.0)),
        // frame field out of timecode range (ff >= base)
        (json!({ "data": [ { "id": "c", "text": "t", "timestamp": "00:00:01:24" } ] }), Some(24.0)),
    ];
    for (result, fps) in cases {
        let (comments, malformed) = rl::parse_sense_comments(&result, fps);
        assert_eq!(comments.len(), 0, "unresolvable anchor never yields a comment: {result}");
        assert_eq!(malformed, 1, "unresolvable anchor is COUNTED: {result}");
    }
}

#[test]
fn sense_parse_duration_becomes_range_end() {
    // Frame.io V4: `duration` is int32 FRAMES, requires timestamp.
    let result = json!({ "data": [
        { "id": "c-range", "text": "fix this span", "timestamp": 100, "duration": 48 },
        { "id": "c-tc-range", "text": "tc span", "timestamp": "00:00:02:00", "duration": 12 },
        { "id": "c-explicit", "text": "explicit wins", "timestamp": 10, "frame_out": 20, "duration": 99 },
    ]});
    let (comments, malformed) = rl::parse_sense_comments(&result, Some(24.0));
    assert_eq!(malformed, 0);
    let by_id = |id: &str| comments.iter().find(|c| c.id == id).expect(id);
    assert_eq!(by_id("c-range").frame_out, Some(148), "frame_out = anchor + duration");
    assert_eq!(by_id("c-tc-range").frame, 48);
    assert_eq!(by_id("c-tc-range").frame_out, Some(60), "duration stacks on a timecode anchor");
    assert_eq!(by_id("c-explicit").frame_out, Some(20), "an explicit frame_out wins over duration");
}

#[test]
fn sense_ingest_string_timecode_lands_at_master_coords() {
    let conn = db();
    seed_published_round1(&conn, "file_tc1");

    // Round 1 = identity map (fixture registers the proxy at fps 24): a string
    // timecode comment must land at the SAME master frame as its int twin would.
    let result = json!({ "data": [
        { "id": "c-tc-live", "text": "music too loud here", "timestamp": "00:00:42:00" },
    ]});
    let ingest = rl::ingest_sense_result(&conn, T, "file_tc1", &result).expect("ingest");
    assert_eq!(ingest.malformed, 0);
    assert_eq!(ingest.appended.len(), 1);
    assert_eq!(ingest.appended[0].tc_in, 42 * 24, "00:00:42:00 @ 24fps → frame 1008 on the master");
}
