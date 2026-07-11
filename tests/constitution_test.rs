//! feat/notes-constitution — the MERGE RESOLVER: tenant ⊕ group ⊕ board notes →
//! the board's EFFECTIVE constitution + preferences, the exact strings that populate
//! `ProposeCtx.constitution` / `ProposeCtx.preferences` (the frozen propose_ops seam).
//!
//! Precedence: board > group > tenant. In a merged-markdown world "board wins on
//! conflict" means the board section is the MOST SPECIFIC and comes LAST (labeled),
//! so it wins in-context — mirroring the memory design's "most-specific-last".
//!
//! No live deps: pure storage, synchronous assertions.

use std::path::Path;
use std::sync::Once;

use cyan_backend::constitution;
use cyan_backend::models::dto::NoteDTO;
use cyan_backend::ops_proposer::{AssetMeta, ProposeCtx};
use cyan_backend::storage;

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("constitution.db");
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

/// Upsert a scoped note directly into the ledger (the resolver reads the store; how
/// the note ARRIVED — FFI, gossip, snapshot — is covered by the notes tests).
fn put(id: &str, anchor: &str, tenant: &str, scope: &str, kind: &str, text: &str, at: i64) {
    storage::note_upsert(&NoteDTO {
        id: id.to_string(),
        board_id: anchor.to_string(),
        tenant_id: tenant.to_string(),
        author_id: "node-x".to_string(),
        author_name: "Ada".to_string(),
        text: text.to_string(),
        created_at: at,
        updated_at: at,
        scope: scope.to_string(),
        kind: kind.to_string(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: None,
    })
    .expect("note upsert");
}

// ════════════════════════════════════════════════════════════════════════════
// 1. Precedence: tenant ⊕ group ⊕ board — every scope present, labeled, and the
//    board section LAST (most specific wins on conflict).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn precedence_board_section_is_last_and_wins() {
    ensure_db();
    let (tenant, board) = ("con-prec-t", "con-prec-board");

    put("cp-1", tenant, tenant, "tenant", "constitution", "Deliver -14 LUFS", 100);
    put("cp-2", tenant, tenant, "group", "constitution", "Cuts land on action", 101);
    put("cp-3", board, tenant, "board", "constitution", "Deliver -16 LUFS for this board", 102);

    let eff = constitution::effective_notes(tenant, Some(tenant), board).expect("resolve");

    let c = &eff.constitution;
    assert!(c.contains("Deliver -14 LUFS"), "tenant rule present:\n{c}");
    assert!(c.contains("Cuts land on action"), "group rule present:\n{c}");
    assert!(c.contains("Deliver -16 LUFS for this board"), "board rule present:\n{c}");

    let t_idx = c.find("Deliver -14 LUFS").expect("tenant idx");
    let g_idx = c.find("Cuts land on action").expect("group idx");
    let b_idx = c.find("Deliver -16 LUFS for this board").expect("board idx");
    assert!(t_idx < g_idx, "tenant before group (general → specific):\n{c}");
    assert!(g_idx < b_idx, "group before board — board is LAST so it wins:\n{c}");

    // The precedence contract is stated IN the string, so any consumer (LLM or
    // human) reads the conflict rule alongside the rules themselves.
    assert!(
        c.contains("board > group > tenant"),
        "precedence rule stated in the merged text:\n{c}"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Empties: absent scopes leave NO empty sections; nothing at all ⇒ "".
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn empty_scopes_are_skipped_and_all_empty_is_empty_string() {
    ensure_db();
    let (tenant, board) = ("con-empty-t", "con-empty-board");

    // Nothing anywhere ⇒ both strings are EMPTY (a valid, tested result — the
    // proposer ctx must not carry headers-with-no-content noise).
    let eff = constitution::effective_notes(tenant, Some(tenant), board).expect("resolve");
    assert_eq!(eff.constitution, "", "no constitution notes ⇒ empty string");
    assert_eq!(eff.preferences, "", "no preference notes ⇒ empty string");

    // Board-only ⇒ the merged text has the board rule but NO tenant/group headers.
    put("ce-1", board, tenant, "board", "constitution", "board-only rule", 10);
    let eff = constitution::effective_notes(tenant, Some(tenant), board).expect("resolve");
    assert!(eff.constitution.contains("board-only rule"));
    assert!(
        !eff.constitution.contains("## Tenant"),
        "no empty tenant section:\n{}",
        eff.constitution
    );
    assert!(
        !eff.constitution.contains("## Group"),
        "no empty group section:\n{}",
        eff.constitution
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 3. Tenant isolation: another tenant's notes NEVER leak into the effective
//    constitution, even with identical anchor ids.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn tenant_isolation_holds() {
    ensure_db();
    let (tenant_a, tenant_b) = ("con-iso-a", "con-iso-b");
    let board = "con-iso-shared-board-id";

    put("ci-a", board, tenant_a, "board", "constitution", "A's secret house rule", 1);
    put("ci-b", board, tenant_b, "board", "constitution", "B's secret house rule", 2);

    let a = constitution::effective_notes(tenant_a, Some(tenant_a), board).expect("a");
    assert!(a.constitution.contains("A's secret house rule"));
    assert!(
        !a.constitution.contains("B's secret house rule"),
        "tenant A must never see tenant B's rules"
    );

    let b = constitution::effective_notes(tenant_b, Some(tenant_b), board).expect("b");
    assert!(b.constitution.contains("B's secret house rule"));
    assert!(!b.constitution.contains("A's secret house rule"));
}

// ════════════════════════════════════════════════════════════════════════════
// 4. Kind routing: constitution → .constitution, preference → .preferences,
//    editor-note → NEITHER.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn kinds_route_to_their_own_strings() {
    ensure_db();
    let (tenant, board) = ("con-kind-t", "con-kind-board");

    put("ck-1", board, tenant, "board", "constitution", "the constitution rule", 1);
    put("ck-2", board, tenant, "board", "preference", "producer prefers J-cuts", 2);
    put("ck-3", board, tenant, "board", "editor-note", "hey, lunch at 1?", 3);

    let eff = constitution::effective_notes(tenant, Some(tenant), board).expect("resolve");

    assert!(eff.constitution.contains("the constitution rule"));
    assert!(
        !eff.constitution.contains("producer prefers J-cuts"),
        "preferences stay out of the constitution string"
    );
    assert!(eff.preferences.contains("producer prefers J-cuts"));
    assert!(
        !eff.preferences.contains("the constitution rule"),
        "constitution stays out of the preferences string"
    );
    assert!(
        !eff.constitution.contains("lunch") && !eff.preferences.contains("lunch"),
        "editor-notes feed NEITHER string"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 5. Deterministic order within a scope: created_at, then id — two resolves
//    produce byte-identical strings.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn merge_is_deterministic() {
    ensure_db();
    let (tenant, board) = ("con-det-t", "con-det-board");

    put("cd-b", board, tenant, "board", "constitution", "second rule", 200);
    put("cd-a", board, tenant, "board", "constitution", "first rule", 100);

    let one = constitution::effective_notes(tenant, Some(tenant), board).expect("one");
    let two = constitution::effective_notes(tenant, Some(tenant), board).expect("two");
    assert_eq!(one.constitution, two.constitution, "resolver is deterministic");

    let first = one.constitution.find("first rule").expect("first");
    let second = one.constitution.find("second rule").expect("second");
    assert!(first < second, "within a scope, notes order by created_at");
}

// ════════════════════════════════════════════════════════════════════════════
// 6. No group known (an un-grouped board): tenant + board still merge.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn missing_group_still_merges_tenant_and_board() {
    ensure_db();
    let (tenant, board) = ("con-nog-t", "con-nog-board");

    put("cn-1", tenant, tenant, "tenant", "constitution", "tenant-wide rule", 1);
    put("cn-2", board, tenant, "board", "constitution", "board rule", 2);

    let eff = constitution::effective_notes(tenant, None, board).expect("resolve");
    assert!(eff.constitution.contains("tenant-wide rule"));
    assert!(eff.constitution.contains("board rule"));
}

// ════════════════════════════════════════════════════════════════════════════
// 7. The resolver output IS the ProposeCtx fuel: the strings plug into the
//    frozen `propose_ops` seam with zero adaptation.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn effective_notes_feed_propose_ctx() {
    ensure_db();
    let (tenant, board) = ("con-ctx-t", "con-ctx-board");

    put("cc-1", board, tenant, "board", "constitution", "never trim the sponsor tag", 1);
    put("cc-2", board, tenant, "board", "preference", "music -20 LUFS under VO", 2);

    let eff = constitution::effective_notes(tenant, Some(tenant), board).expect("resolve");
    let asset = AssetMeta { duration_frames: Some(1440), fps: 24.0 };
    let ctx = ProposeCtx {
        constitution: &eff.constitution,
        preferences: &eff.preferences,
        asset: &asset,
        tool_schemas: "",
    };

    assert!(ctx.constitution.contains("never trim the sponsor tag"));
    assert!(ctx.preferences.contains("music -20 LUFS under VO"));
}

// ════════════════════════════════════════════════════════════════════════════
// 8. LENS_AI_NOTES P1 — the FULL scope chain: tenant ⊕ group ⊕ board ⊕ workflow
//    ⊕ producer ⊕ user, most-specific LAST (user innermost, so user wins).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn chain_merges_all_six_scopes_most_specific_last() {
    ensure_db();
    let tenant = "ch6-t";

    put("ch6-1", tenant, tenant, "tenant", "constitution", "tenant chain rule", 1);
    put("ch6-2", "ch6-g", tenant, "group", "constitution", "group chain rule", 2);
    put("ch6-3", "ch6-b", tenant, "board", "constitution", "board chain rule", 3);
    put("ch6-4", "ch6-w", tenant, "workflow", "constitution", "workflow chain rule", 4);
    put("ch6-5", "ch6-p", tenant, "producer", "constitution", "producer chain rule", 5);
    put("ch6-6", "ch6-u", tenant, "user", "constitution", "user chain rule", 6);

    let chain = constitution::ScopeChain {
        tenant_id: tenant.to_string(),
        group_id: Some("ch6-g".to_string()),
        board_id: "ch6-b".to_string(),
        workflow_id: Some("ch6-w".to_string()),
        producer_id: Some("ch6-p".to_string()),
        user_id: Some("ch6-u".to_string()),
    };
    let eff = constitution::effective_notes_chain(&chain).expect("resolve chain");
    let c = &eff.constitution;
    let idx = |s: &str| c.find(s).unwrap_or_else(|| panic!("{s:?} missing:\n{c}"));

    assert!(idx("tenant chain rule") < idx("group chain rule"), "tenant → group:\n{c}");
    assert!(idx("group chain rule") < idx("board chain rule"), "group → board:\n{c}");
    assert!(idx("board chain rule") < idx("workflow chain rule"), "board → workflow:\n{c}");
    assert!(idx("workflow chain rule") < idx("producer chain rule"), "workflow → producer:\n{c}");
    assert!(idx("producer chain rule") < idx("user chain rule"), "producer → user (user LAST, wins):\n{c}");

    // The precedence contract is stated in the merged text, covering the FULL chain
    // while preserving the 3-scope phrase existing consumers already assert on.
    assert!(c.contains("user > producer > workflow"), "chain precedence stated:\n{c}");
    assert!(c.contains("board > group > tenant"), "3-scope phrase preserved:\n{c}");
}

// ════════════════════════════════════════════════════════════════════════════
// 9. User scope is the innermost link: even against only a board rule, the user
//    section comes LAST — the sovereign, per-person override.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn chain_user_scope_is_innermost_and_wins() {
    ensure_db();
    let tenant = "chu-t";

    put("chu-1", "chu-b", tenant, "board", "constitution", "board says -16 LUFS", 1);
    put("chu-2", "chu-user", tenant, "user", "constitution", "user says -14 LUFS", 2);

    let chain = constitution::ScopeChain {
        tenant_id: tenant.to_string(),
        group_id: None,
        board_id: "chu-b".to_string(),
        workflow_id: None,
        producer_id: None,
        user_id: Some("chu-user".to_string()),
    };
    let eff = constitution::effective_notes_chain(&chain).expect("resolve");
    let c = &eff.constitution;
    let b = c.find("board says -16 LUFS").expect("board rule present");
    let u = c.find("user says -14 LUFS").expect("user rule present");
    assert!(b < u, "user section is LAST (most specific wins):\n{c}");
    assert!(c.contains("## User"), "user section labeled:\n{c}");
}

// ════════════════════════════════════════════════════════════════════════════
// 10. Chain tenant isolation: a chain for tenant A NEVER picks tenant B rows,
//     even with identical anchor ids at every link.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn chain_tenant_isolation_holds() {
    ensure_db();
    let (ta, tb) = ("chi-a", "chi-b");
    // Identical anchors for both tenants at every scope link.
    let (board, wf, prod, user) = ("chi-board", "chi-wf", "chi-prod", "chi-user");

    put("chi-a1", wf, ta, "workflow", "constitution", "A workflow secret", 1);
    put("chi-a2", prod, ta, "producer", "constitution", "A producer secret", 2);
    put("chi-a3", user, ta, "user", "constitution", "A user secret", 3);
    put("chi-b1", wf, tb, "workflow", "constitution", "B workflow secret", 1);
    put("chi-b2", prod, tb, "producer", "constitution", "B producer secret", 2);
    put("chi-b3", user, tb, "user", "constitution", "B user secret", 3);

    let chain_a = constitution::ScopeChain {
        tenant_id: ta.to_string(),
        group_id: Some(ta.to_string()),
        board_id: board.to_string(),
        workflow_id: Some(wf.to_string()),
        producer_id: Some(prod.to_string()),
        user_id: Some(user.to_string()),
    };
    let a = constitution::effective_notes_chain(&chain_a).expect("a");
    assert!(a.constitution.contains("A workflow secret"));
    assert!(a.constitution.contains("A producer secret"));
    assert!(a.constitution.contains("A user secret"));
    for leak in ["B workflow secret", "B producer secret", "B user secret"] {
        assert!(
            !a.constitution.contains(leak),
            "tenant A must never see {leak:?}:\n{}",
            a.constitution
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 11. Creative DNA (kind = "creative-dna") rides the CONSTITUTION rail as its
//     own labeled subsection per scope — never the preferences string.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn creative_dna_rides_constitution_as_labeled_subsections() {
    ensure_db();
    let tenant = "chd-t";

    put("chd-1", tenant, tenant, "tenant", "constitution", "tenant base rule", 1);
    put("chd-2", tenant, tenant, "tenant", "creative-dna", "house style: slow push-ins", 2);
    put("chd-3", "chd-u", tenant, "user", "creative-dna", "my feel: cut on breath", 3);

    let chain = constitution::ScopeChain {
        tenant_id: tenant.to_string(),
        group_id: None,
        board_id: "chd-b".to_string(),
        workflow_id: None,
        producer_id: None,
        user_id: Some("chd-u".to_string()),
    };
    let eff = constitution::effective_notes_chain(&chain).expect("resolve");
    let c = &eff.constitution;

    assert!(c.contains("## Creative DNA (Tenant)"), "tenant DNA subsection labeled:\n{c}");
    assert!(c.contains("house style: slow push-ins"), "tenant DNA content present:\n{c}");
    assert!(c.contains("## Creative DNA (User)"), "user DNA subsection labeled:\n{c}");
    assert!(c.contains("my feel: cut on breath"), "user DNA content present:\n{c}");

    // Per-scope placement: the tenant DNA rides with the tenant link (before the
    // user link), so specificity ordering holds for DNA too.
    let t_dna = c.find("house style: slow push-ins").expect("t dna");
    let u_dna = c.find("my feel: cut on breath").expect("u dna");
    assert!(t_dna < u_dna, "tenant DNA before user DNA:\n{c}");

    assert!(
        !eff.preferences.contains("slow push-ins") && !eff.preferences.contains("cut on breath"),
        "creative-dna never leaks into preferences:\n{}",
        eff.preferences
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 12. Absent chain links produce NO sections — and the legacy 3-arg resolver is
//     exactly a chain with the new links None (the frozen seam holds).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn absent_links_have_no_sections_and_three_arg_seam_is_a_none_chain() {
    ensure_db();
    let tenant = "chn-t";

    put("chn-1", tenant, tenant, "tenant", "constitution", "tenant-only rule", 1);
    put("chn-2", "chn-b", tenant, "board", "constitution", "board-only rule", 2);

    let chain = constitution::ScopeChain {
        tenant_id: tenant.to_string(),
        group_id: Some(tenant.to_string()),
        board_id: "chn-b".to_string(),
        workflow_id: None,
        producer_id: None,
        user_id: None,
    };
    let chained = constitution::effective_notes_chain(&chain).expect("chain");
    for absent in ["## Workflow", "## Producer", "## User"] {
        assert!(
            !chained.constitution.contains(absent),
            "absent link must produce no {absent:?} section:\n{}",
            chained.constitution
        );
    }

    let three_arg =
        constitution::effective_notes(tenant, Some(tenant), "chn-b").expect("3-arg");
    assert_eq!(
        three_arg, chained,
        "the 3-arg resolver IS a chain with workflow/producer/user = None"
    );
}
