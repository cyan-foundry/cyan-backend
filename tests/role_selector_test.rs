//! A3 — the role×type selector (T39-T51, T-A3-1..4, T-A3-8): builtin catalog
//! pins, Rule R2 marker discipline, the REAL deterministic compile over all
//! five roletype templates, resolve() bundles + errors + override precedence,
//! and the B3 cross-package duty/bridge interlock (ORCH-11).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Once, OnceLock},
};

use cyan_backend::{
    models::dto::{Template, NOTE_KIND_VOCAB, PRODUCTION_ROLE_VOCAB},
    role_templates::{
        self, ae_duty_catalog, builtin_roletype_templates, orchestration_bridge,
        primary_surface_for, resolve, SelectorError, SelectorResult, GATE_VOCAB,
        LIVE_BOUND_MENTIONS, TASK_STATUS_VOCAB,
    },
    storage, templates,
};

static DB_INIT: Once = Once::new();
static DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("role_selector.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        // Isolate the on-disk bundle dir (T42's bind fixture writes into it).
        let plugins = tempfile::tempdir().expect("tmp plugins dir");
        unsafe { std::env::set_var("CYAN_PLUGINS_ROOT", plugins.path()) };
        std::mem::forget(plugins);
        let _ = DB_PATH.set(path);
        std::mem::forget(dir);
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
        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, element_type TEXT NOT NULL,
            x REAL, y REAL, width REAL, height REAL, z_index INTEGER DEFAULT 0,
            style_json TEXT, content_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

/// The no-plugins-installed lookup for tests that don't exercise step 4.
fn no_lookup(_tool: &str) -> Option<String> {
    None
}

// ════════════════════════════════════════════════════════════════════════════
// T39 — the Template wire stays additive: pre-A3 JSON decodes; round-trips
// with defaults (plugins [], format_type None).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn template_wire_stays_additive() {
    let pre_a3 = serde_json::json!({
        "id": "builtin:transcode-deliver",
        "tenant_id": "",
        "name": "Old template",
        "description": "pre-A3 wire shape",
        "source": "builtin",
        "steps": [ {"text": "Transcode the master", "plugin": null} ],
        "created_at": 0,
    });
    let t: Template = serde_json::from_value(pre_a3).expect("pre-A3 template decodes");
    assert_eq!(t.format_type, None);
    assert!(t.plugins.is_empty());
    assert!(t.stages.is_empty());
    assert!(t.note_kinds.is_empty());
    assert_eq!(t.maturity, None);
    assert_eq!(t.catalog_version, None);
    assert_eq!(t.scope, None);

    // Round-trip: the defaults never serialize (skip-if-empty/None), so the
    // re-encoded wire carries no new keys for a legacy template.
    let encoded = serde_json::to_value(&t).expect("encode");
    for key in ["format_type", "stages", "note_kinds", "plugins", "maturity", "catalog_version", "scope"] {
        assert!(encoded.get(key).is_none(), "legacy round-trip must not grow key {key}");
    }
    let again: Template = serde_json::from_value(encoded).expect("round-trip decodes");
    assert_eq!(again.name, "Old template");
}

// ════════════════════════════════════════════════════════════════════════════
// T40 (CHANGED) — all five builtins, FULL, maturity "mvp", the §9b step counts
// and note_kinds lists (⊆ NOTE_KIND_VOCAB by const reference).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn roletype_builtins_cover_all_five_formats() {
    let builtins = builtin_roletype_templates();
    let by_format: HashMap<&str, &Template> =
        builtins.iter().map(|t| (t.format_type.as_deref().expect("format"), t)).collect();

    let expected_steps: [(&str, usize); 5] = [
        ("promo", 9),
        ("short_film", 11),
        ("commercial", 12),
        ("episodic", 13),
        ("feature", 16),
    ];
    assert_eq!(builtins.len(), 5, "exactly five roletype builtins");
    for (format, steps) in expected_steps {
        let t = by_format.get(format).unwrap_or_else(|| panic!("builtin for {format}"));
        assert_eq!(t.id, format!("builtin:roletype:{format}"), "id convention");
        assert_eq!(t.maturity.as_deref(), Some("mvp"), "{format} ships mvp (D-A3.1)");
        assert_eq!(t.steps.len(), steps, "{format} step count");
        assert!(!t.stages.is_empty(), "{format} carries a stage rail");
        // note_kinds ⊆ the A1 vocab BY CONST REFERENCE — never a literal count.
        for k in &t.note_kinds {
            assert!(
                NOTE_KIND_VOCAB.contains(&k.as_str()),
                "{format} note_kind {k} must be in NOTE_KIND_VOCAB"
            );
        }
    }

    // The §9b explicit lists.
    assert_eq!(
        by_format["promo"].note_kinds,
        vec!["creative-brief", "constitution", "preference", "creative-dna", "decision",
             "editor-note", "legal-clearance", "qc-report"],
    );
    assert_eq!(
        by_format["commercial"].note_kinds,
        vec!["creative-brief", "script", "shot-log", "continuity", "constitution", "preference",
             "creative-dna", "decision", "editor-note", "legal-clearance", "turnover", "qc-report"],
    );
    assert_eq!(
        by_format["short_film"].note_kinds,
        vec!["script", "lined-script", "shot-log", "continuity", "constitution", "preference",
             "creative-dna", "decision", "editor-note", "turnover", "qc-report"],
    );
    // episodic + feature adopt ALL 13 in the §3 literal order.
    for format in ["episodic", "feature"] {
        assert_eq!(
            by_format[format].note_kinds,
            NOTE_KIND_VOCAB.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
            "{format} adopts the full 13-kind vocab in §3 order"
        );
    }

    // A couple of byte-carried §9b step texts (the full lists live in the data).
    assert_eq!(by_format["promo"].steps[0].text, "Probe the source spot with @cyan-media.probe");
    assert_eq!(
        by_format["feature"].steps[11].text,
        "Conform the locked cut against the source pool with @cyan-media.conform"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T41 (CHANGED) — Rule R2 v2 marker discipline across ALL FIVE templates:
// every `@` mention ∈ LIVE_BOUND_MENTIONS (4); every non-`@` step carries
// `(manual)` OR starts `Manual:` OR matches await-sense.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn live_steps_mention_only_live_tools() {
    let is_await_sense = |c: &str| {
        let c = c.to_lowercase();
        c.contains("await") && (c.contains("note") || c.contains("review") || c.contains("feedback"))
    };
    for t in builtin_roletype_templates() {
        for step in &t.steps {
            if step.text.contains('@') {
                let mention = LIVE_BOUND_MENTIONS
                    .iter()
                    .find(|m| step.text.contains(*m));
                assert!(
                    mention.is_some(),
                    "{}: @ step must mention a LIVE_BOUND tool: {:?}",
                    t.id,
                    step.text
                );
            } else {
                assert!(
                    step.text.contains("(manual)")
                        || step.text.starts_with("Manual:")
                        || is_await_sense(&step.text),
                    "{}: non-@ step must carry a manual/await marker: {:?}",
                    t.id,
                    step.text
                );
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// T42 (CHANGED) — clone ALL FIVE to boards, run the REAL deterministic compile:
// every non-@ step's executor == "manual" (the inert qc_loudness hint on
// episodic-12 / feature-15 is EXPECTED metadata, present-and-harmless); feature
// step 12 BINDS @cyan-media.conform against a fixture registry.
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn roletype_steps_compile_manual_gates() {
    ensure_db();
    const GROUP: &str = "t42-group";
    storage::group_insert_simple(GROUP, "T42", "folder", "#00AEEF").expect("group");
    storage::workspace_insert_simple("t42-ws", GROUP, "General").expect("ws");
    // The bind fixture: a cyan-media bundle installed in THIS group's Plugins
    // workspace (objects row) + the unpacked drop-in dir with a manifest
    // carrying probe + conform (`ensure_bundle_unpacked`'s dev drop-in path).
    storage::workspace_insert_simple("t42-plugins", GROUP, "Plugins").expect("plugins ws");
    storage::file_insert_simple(
        "t42-cyan-media-bundle",
        Some(GROUP),
        Some("t42-plugins"),
        None,
        "cyan-media.cyanplugin",
        "blake3hash",
        64,
        None,
        0,
    )
    .expect("bundle row");
    let bundle_dir = storage::plugin_bundles_dir().join("cyan-media");
    std::fs::create_dir_all(&bundle_dir).expect("mkdir bundle");
    std::fs::write(bundle_dir.join("manifest.json"), CYAN_MEDIA_MANIFEST).expect("manifest");
    // The install check requires a FETCHED bundle (local_path set) — the
    // file-swarm sets this on delivery; the fixture mirrors it.
    storage::file_set_local_path(
        "t42-cyan-media-bundle",
        bundle_dir.to_str().expect("utf8 bundle path"),
    )
    .expect("set local_path");

    for (i, template) in builtin_roletype_templates().into_iter().enumerate() {
        let board = format!("t42-board-{i}");
        storage::board_insert_simple(&board, "t42-ws", &template.name, 1).expect("board");
        let created =
            templates::clone_to_board(&template.id, &board, GROUP).expect("clone roletype builtin");
        assert_eq!(created.len(), template.steps.len(), "{}: one cell per step", template.id);

        // The REAL deterministic compile, captured command channel.
        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let result = cyan_backend::pipeline::compile_via_llm(&board, &command_tx)
            .await
            .expect("compile");
        assert_eq!(
            result["applied"].as_u64(),
            Some(template.steps.len() as u64),
            "{}: every step compiles",
            template.id
        );

        let mut updates: Vec<(String, serde_json::Value)> = Vec::new();
        while let Ok(msg) = command_rx.try_recv() {
            if let cyan_backend::models::commands::CommandMsg::UpdateNotebookCell {
                content,
                metadata_json,
                ..
            } = msg
            {
                let meta: serde_json::Value =
                    serde_json::from_str(&metadata_json.expect("metadata")).expect("meta json");
                updates.push((content.unwrap_or_default(), meta));
            }
        }
        assert_eq!(updates.len(), template.steps.len(), "{}: one update per step", template.id);

        for (content, meta) in &updates {
            let executor = meta["pipeline"]["executor"].as_str().expect("executor");
            if content.contains('@') {
                assert_eq!(executor, "lens", "{}: @ step dispatches: {content:?}", template.id);
            } else {
                assert_eq!(executor, "manual", "{}: non-@ step parks: {content:?}", template.id);
            }
        }

        // The inert qc_loudness media-tool HINT on episodic-12 / feature-15 is
        // EXPECTED compiled metadata — present AND harmless (executor manual).
        if template.id.ends_with("episodic") || template.id.ends_with("feature") {
            let qc = updates
                .iter()
                .find(|(c, _)| c.starts_with("QC the"))
                .expect("the QC step compiled");
            assert_eq!(qc.1["pipeline"]["executor"], serde_json::json!("manual"));
            let hint = qc.1["mcp_tool"]["tool"].as_str().unwrap_or_default();
            assert!(
                hint == "qc_loudness" || hint == "qc_black_freeze",
                "the media-tool hint is stamped (harmless): {:?}",
                qc.1["mcp_tool"]
            );
        }

        // Feature step 12 BINDS @cyan-media.conform (Bound, not Miss).
        if template.id.ends_with("feature") {
            let conform = updates
                .iter()
                .find(|(c, _)| c.contains("@cyan-media.conform"))
                .expect("feature carries the conform step");
            assert_eq!(
                conform.1["mcp_tool"]["bound"],
                serde_json::json!(true),
                "feature step 12 binds against the fixture registry: {:?}",
                conform.1
            );
            assert_eq!(conform.1["mcp_tool"]["plugin_id"], serde_json::json!("cyan-media"));
            assert_eq!(conform.1["mcp_tool"]["tool"], serde_json::json!("conform"));
            assert!(conform.1.get("mcp_tool_miss").is_none(), "Bound, not Miss");
        }
    }
}

/// The T42 fixture manifest: cyan-media advertising probe + conform (conform's
/// only required prop is filled from upstream at dispatch — `pending`, still a
/// BIND at compile).
const CYAN_MEDIA_MANIFEST: &str = r#"{
  "name": "cyan-media",
  "version": "1.0.0",
  "runtime": "python-uv",
  "tools": [
    {
      "name": "probe",
      "when_to_use": "probe a media file",
      "io_types": { "input": ["video"], "output": ["report"] },
      "stage": "ingest",
      "side_effects": [],
      "locality": "device",
      "input_schema": {},
      "output_schema": {}
    },
    {
      "name": "conform",
      "when_to_use": "conform a cut against the source pool",
      "io_types": { "input": ["video"], "output": ["video"] },
      "stage": "conform",
      "side_effects": [],
      "locality": "device",
      "input_schema": {},
      "output_schema": {}
    }
  ]
}"#;

// ════════════════════════════════════════════════════════════════════════════
// T43 (CHANGED) — resolve(producer, promo): template id + 6 plugins + promo's 8
// note_kinds + catalog_version; producer agentification agent_does(4) /
// human_does(3); orchestration 5; ae_duties ABSENT for producer.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn resolve_promo_producer_returns_full_bundle() {
    ensure_db();
    let r = resolve(None, "producer", "promo", &no_lookup).expect("resolve");
    assert_eq!(r.catalog_version, "roletype.v1");
    assert!(!r.observe_only);
    let template = r.template.as_ref().expect("template present");
    assert_eq!(template.id, "builtin:roletype:promo");
    assert_eq!(r.plugins.len(), 6, "promo's 6 plugins with flags");
    for p in &r.plugins {
        assert!(["live", "roadmap"].contains(&p.status.as_str()));
        assert!(["device", "cloud", "both"].contains(&p.execution.as_str()));
    }
    assert_eq!(template.note_kinds.len(), 8, "promo's 8 note_kinds");

    // Producer map on promo: agent_does 4 / human_does 3 (the SUPERSEDED 3/2
    // baseline pin is gone — freeform_note_structuring joined the base map).
    assert_eq!(
        r.agentification.agent_does,
        vec![
            "brief_to_workflow_generation",
            "freeform_note_structuring",
            "version_orchestration",
            "review_loop_management"
        ],
    );
    assert_eq!(
        r.agentification.human_does,
        vec!["approvals", "legal_clearance", "freeform_note_structuring:gate"],
    );

    assert_eq!(r.orchestration.len(), 5, "the 5-row bridge rides every producing role");
    assert!(r.ae_duties.is_empty(), "ae_duties attach ONLY for assistant_editor");
    let wire = serde_json::to_value(&r).expect("encode");
    assert!(wire.get("ae_duties").is_none(), "empty array never serialized");

    // AE base for reference: 13 tasks ⇒ agent_does 13 / human_does 5 gate re-lists.
    let ae = resolve(None, "assistant_editor", "short_film", &no_lookup).expect("ae resolve");
    assert_eq!(ae.agentification.agent_does.len(), 13);
    assert_eq!(ae.agentification.human_does.len(), 5);
    assert!(ae.agentification.human_does.iter().all(|h| h.ends_with(":gate")));
}

// ════════════════════════════════════════════════════════════════════════════
// T-A3-1 — the format deltas apply to the agentification map.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn format_deltas_apply_to_agentification() {
    ensure_db();
    let tasks = |role: &str, format: &str| -> Vec<String> {
        resolve(None, role, format, &no_lookup)
            .expect("resolve")
            .agentification
            .tasks
            .iter()
            .map(|t| t.task.clone())
            .collect()
    };

    // (assistant_editor, promo) lacks sync/log_shots/stringouts/turnovers.
    let ae_promo = tasks("assistant_editor", "promo");
    for removed in ["sync_picture_sound", "log_shots", "stringouts_selects", "turnovers"] {
        assert!(!ae_promo.contains(&removed.to_string()), "promo AE drops {removed}");
    }
    // (assistant_editor, feature) includes multicam_grouping.
    assert!(tasks("assistant_editor", "feature").contains(&"multicam_grouping".to_string()));
    // (producer, episodic): + show_bible_stewardship, − version_orchestration.
    let prod_epi = tasks("producer", "episodic");
    assert!(prod_epi.contains(&"show_bible_stewardship".to_string()));
    assert!(!prod_epi.contains(&"version_orchestration".to_string()));
    // (director, feature): dcc component == avid.
    let dir_feature = resolve(None, "director", "feature", &no_lookup).expect("resolve");
    let dcc = dir_feature
        .agentification
        .tasks
        .iter()
        .find(|t| t.task == "dcc_agentic_edits")
        .expect("dcc task");
    assert_eq!(dcc.component.as_deref(), Some("avid"));
    // (assistant_editor, short_film) == the base map verbatim.
    let base: Vec<String> =
        role_templates::base_agentification("assistant_editor").iter().map(|t| t.task.clone()).collect();
    assert_eq!(tasks("assistant_editor", "short_film"), base, "short_film AE == base verbatim");

    // Key-busting evidence (T61's plan-key input): promo vs feature AE maps
    // serialize differently.
    let promo_json = serde_json::to_string(
        &resolve(None, "assistant_editor", "promo", &no_lookup).expect("r").agentification,
    )
    .expect("encode");
    let feature_json = serde_json::to_string(
        &resolve(None, "assistant_editor", "feature", &no_lookup).expect("r").agentification,
    )
    .expect("encode");
    assert_ne!(promo_json, feature_json, "agent_json differs across formats");
}

// ════════════════════════════════════════════════════════════════════════════
// T-A3-2 — the AE duty catalog, pinned (13 ids; statuses exact per §9c-bis;
// vocab discipline; duties attach ONLY for assistant_editor).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn ae_duty_catalog_pinned() {
    ensure_db();
    let catalog = ae_duty_catalog();
    assert_eq!(
        catalog.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
        vec![
            "ingest", "organize", "sync", "shot_log", "import_report", "stringouts", "qc",
            "deep_qc", "conform", "turnover", "deliver", "multicam", "heal"
        ],
        "exactly 13 duty ids — B3's 11 + multicam + heal"
    );
    let status = |id: &str| {
        catalog.iter().find(|d| d.id == id).map(|d| d.status.clone()).expect("duty")
    };
    for live in ["ingest", "qc", "conform", "turnover", "deliver", "heal"] {
        assert_eq!(status(live), "live", "{live} is live");
    }
    for build_now in ["shot_log", "import_report"] {
        assert_eq!(status(build_now), "build_now", "{build_now} is build_now");
    }
    for coming in ["organize", "sync", "stringouts", "deep_qc", "multicam"] {
        assert_eq!(status(coming), "coming", "{coming} is coming");
    }
    for d in &catalog {
        assert!(TASK_STATUS_VOCAB.contains(&d.status.as_str()));
        if let Some(g) = d.gate.as_deref() {
            assert!(GATE_VOCAB.contains(&g), "gate {g} in vocab");
        }
        if let Some(c) = d.gate_caveat.as_deref() {
            assert!(
                !GATE_VOCAB.contains(&c),
                "caveats are display strings, never gate tokens: {c}"
            );
        }
        if d.status == "live" {
            assert!(!d.component.is_empty(), "live duty {} names an op-backed rail", d.id);
        }
    }

    // Attach rule: assistant_editor only; studio_exec omits BOTH arrays.
    let ae = resolve(None, "assistant_editor", "feature", &no_lookup).expect("ae");
    assert_eq!(ae.ae_duties.len(), 13);
    let exec = resolve(None, "studio_exec", "feature", &no_lookup).expect("exec");
    assert!(exec.ae_duties.is_empty());
    assert!(exec.orchestration.is_empty());
    let exec_wire = serde_json::to_value(&exec).expect("encode");
    assert!(exec_wire.get("ae_duties").is_none());
    assert!(exec_wire.get("orchestration").is_none());
}

// ════════════════════════════════════════════════════════════════════════════
// T-A3-3 — the orchestration bridge rows, pinned.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn orchestration_bridge_rows_pinned() {
    let bridge = orchestration_bridge();
    assert_eq!(
        bridge.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(),
        vec![
            "editorial_to_sound", "editorial_to_color", "editorial_to_vfx",
            "sound_to_delivery", "color_to_delivery"
        ],
        "exactly 5 handoff ids"
    );
    for h in &bridge {
        assert!(["editorial", "sound", "color", "vfx", "delivery"].contains(&h.from_dept.as_str()));
        assert!(["editorial", "sound", "color", "vfx", "delivery"].contains(&h.to_dept.as_str()));
        assert!(TASK_STATUS_VOCAB.contains(&h.status.as_str()), "ONE status ∈ vocab");
        if let Some(g) = h.gate.as_deref() {
            assert!(GATE_VOCAB.contains(&g));
        }
        for k in &h.note_kinds {
            assert!(NOTE_KIND_VOCAB.contains(&k.as_str()), "note_kinds ⊆ the 13-vocab by const ref");
        }
        if let Some(hk) = h.handoff_kind.as_deref() {
            assert_eq!(hk, "turnover", "handoff_kind is the SHIPPED A1 kind where present");
        }
        // External effects carry external_send or human_ok.
        if h.id.starts_with("editorial_to") && h.id != "editorial_to_color" {
            assert_eq!(h.gate.as_deref(), Some("external_send"));
        }
    }
    let by_id = |id: &str| bridge.iter().find(|h| h.id == id).expect("row");
    assert_eq!(by_id("editorial_to_color").status, "live");
    assert!(
        by_id("editorial_to_color").live_rail.as_deref().unwrap_or_default().contains("conform_proxy"),
        "the §9c-bis live_rail op names are pinned"
    );
    assert_eq!(by_id("color_to_delivery").status, "live");
    assert_eq!(by_id("color_to_delivery").gate.as_deref(), Some("human_ok"));
    assert!(
        by_id("color_to_delivery").live_rail.as_deref().unwrap_or_default().contains("produce_master"),
    );
    assert_eq!(by_id("editorial_to_vfx").status, "coming");
    assert!(by_id("editorial_to_vfx").live_rail.is_none());
    assert_eq!(by_id("sound_to_delivery").status, "coming");
}

// ════════════════════════════════════════════════════════════════════════════
// T-A3-4 — SelectorResult's two REV-2 arrays are additive: absent decodes to
// defaults; present round-trips; empty never serializes.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn selector_result_new_arrays_are_additive() {
    ensure_db();
    // Old-shape JSON (no ae_duties/orchestration) decodes with defaults.
    let old = serde_json::json!({
        "catalog_version": "roletype.v1",
        "role": "editor",
        "format_type": "promo",
        "observe_only": false,
        "template": null,
        "plugins": [],
        "agentification": { "role": "editor", "tasks": [], "agent_does": [], "human_does": [] },
    });
    let decoded: SelectorResult = serde_json::from_value(old).expect("old shape decodes");
    assert!(decoded.ae_duties.is_empty());
    assert!(decoded.orchestration.is_empty());

    // With them: byte round-trip.
    let full = resolve(None, "assistant_editor", "feature", &no_lookup).expect("resolve");
    let wire = serde_json::to_string(&full).expect("encode");
    let back: SelectorResult = serde_json::from_str(&wire).expect("round-trip");
    assert_eq!(back.ae_duties.len(), 13);
    assert_eq!(back.orchestration.len(), 5);
    assert_eq!(serde_json::to_string(&back).expect("re-encode"), wire, "byte round-trip");

    // Empty arrays never serialize.
    let exec = resolve(None, "studio_exec", "promo", &no_lookup).expect("exec");
    let wire = serde_json::to_value(&exec).expect("encode");
    assert!(wire.get("ae_duties").is_none());
    assert!(wire.get("orchestration").is_none());
    assert!(wire.get("template").is_some(), "template key ALWAYS present…");
    assert!(wire["template"].is_null(), "…as JSON null for studio_exec");
}

// ════════════════════════════════════════════════════════════════════════════
// T-A3-8 — the B3 cross-package interlock (ORCH-11): B3's 11 row ids ⊆ the 13;
// per-id status identity; component claims row-matched; bridge↔queue pairs
// pinned; B3's `turnover` queue row has NO bridge counterpart. (B3-side mirror
// = T-ln-13, bound at the 2nd cross-package pass.)
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn duty_ids_match_b3_ae_queue_rows() {
    let catalog = ae_duty_catalog();
    // B3's canonical 11-row queue: ids + the shared 3-state statuses.
    let b3_rows: [(&str, &str); 11] = [
        ("ingest", "live"),
        ("organize", "coming"),
        ("sync", "coming"),
        ("shot_log", "build_now"),
        ("import_report", "build_now"),
        ("stringouts", "coming"),
        ("qc", "live"),
        ("deep_qc", "coming"),
        ("conform", "live"),
        ("turnover", "live"),
        ("deliver", "live"),
    ];
    for (id, status) in b3_rows {
        let duty = catalog
            .iter()
            .find(|d| d.id == id)
            .unwrap_or_else(|| panic!("B3 row id {id} ⊆ A3's 13 duty ids"));
        assert_eq!(duty.status, status, "status IDENTITY over the shared vocab for {id}");
        assert!(TASK_STATUS_VOCAB.contains(&duty.status.as_str()));
    }
    // The superset direction is one-way: A3 adds exactly multicam + heal.
    let extras: Vec<&str> = catalog
        .iter()
        .map(|d| d.id.as_str())
        .filter(|id| !b3_rows.iter().any(|(b, _)| b == id))
        .collect();
    assert_eq!(extras, vec!["multicam", "heal"]);

    // Component claims agree row-for-row on the two build_now flips.
    let by_id = |id: &str| catalog.iter().find(|d| d.id == id).expect("duty");
    assert!(
        by_id("shot_log").component.contains("hand entry"),
        "shot_log = hand entry (A1 kind + A4 composer)"
    );
    assert!(
        by_id("import_report").component.contains("/notes/structure/report"),
        "import_report = the A5 report lane by endpoint name (D-A3.12)"
    );
    assert!(by_id("import_report").component.contains("ReportImportView"), "+ A4's surface");

    // Bridge ↔ queue pairs (statuses must agree).
    let bridge = orchestration_bridge();
    let pair = |bridge_id: &str, duty_id: &str| {
        let b = bridge.iter().find(|h| h.id == bridge_id).expect("bridge row");
        let d = by_id(duty_id);
        assert_eq!(b.status, d.status, "{bridge_id} ↔ {duty_id} statuses agree");
    };
    pair("editorial_to_color", "conform");
    pair("color_to_delivery", "deliver");
    // B3's `turnover` row has NO bridge counterpart (a review-loop hop, not a
    // dept handoff — SR-10): no bridge row pairs with it in the pinned map.
    let pinned_pairs = [("editorial_to_color", "conform"), ("color_to_delivery", "deliver")];
    assert!(
        !pinned_pairs.iter().any(|(_, d)| *d == "turnover"),
        "the queue's turnover row is deliberately unpaired"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// T44-T48 — resolve() errors + installed checks.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn resolve_unknown_role_rejects_with_vocab() {
    ensure_db();
    let err = resolve(None, "showrunner", "promo", &no_lookup).expect_err("unknown role");
    assert_eq!(err, SelectorError::UnknownRole("showrunner".to_string()));
    let json = err.to_json();
    assert_eq!(json["error"], serde_json::json!("unknown_role"));
    assert_eq!(json["given"], serde_json::json!("showrunner"));
    assert_eq!(json["allowed"].as_array().map(Vec::len), Some(7), "allowed lists all 7");
    assert_eq!(
        json["allowed"],
        serde_json::json!(PRODUCTION_ROLE_VOCAB),
        "the ONE ordered vocab, by const reference"
    );

    let err = resolve(None, "producer", "sitcom", &no_lookup).expect_err("unknown format");
    assert_eq!(err.to_json()["error"], serde_json::json!("unknown_format_type"));
}

#[test]
fn resolve_null_tenant_is_builtin_only() {
    ensure_db();
    const TENANT: &str = "t45-tenant";
    // A valid tenant override for promo exists…
    let saved = templates::save_roletype_template(
        TENANT,
        &serde_json::json!({
            "name": "Promo Override",
            "source": "user", "tenant_id": TENANT, "created_at": 0, "id": "ignored",
            "format_type": "promo",
            "steps": [ {"text": "Custom promo step (manual)"} ],
        })
        .to_string(),
    );
    assert!(saved.get("error").is_none(), "override saves: {saved}");

    // …but tenant None ⇒ builtin ALWAYS (the override lookup is skipped).
    let r = resolve(None, "producer", "promo", &no_lookup).expect("resolve");
    assert_eq!(r.template.expect("template").id, "builtin:roletype:promo");

    // With the tenant, the override wins (T49's other half, exercised below).
    let r = resolve(Some(TENANT), "producer", "promo", &no_lookup).expect("resolve");
    assert_eq!(r.template.expect("template").name, "Promo Override");
}

#[test]
fn studio_exec_resolves_observe_only() {
    ensure_db();
    let r = resolve(None, "studio_exec", "feature", &no_lookup).expect("resolve");
    assert!(r.observe_only);
    assert!(r.template.is_none(), "template JSON null (key present — see T-A3-4)");
    assert!(r.agentification.tasks.is_empty());
    assert!(r.plugins.is_empty());
}

#[test]
fn selector_marks_uninstalled_plugins() {
    ensure_db();
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = AtomicUsize::new(0);
    let lookup = |tool: &str| -> Option<String> {
        calls.fetch_add(1, Ordering::SeqCst);
        (tool == "upload_for_review").then(|| "frameio".to_string())
    };
    let r = resolve(None, "producer", "promo", &lookup).expect("resolve");
    let by_id = |id: &str| r.plugins.iter().find(|p| p.id == id).expect("plugin");
    assert!(by_id("frameio").installed, "flagship resolved to the owning plugin");
    assert!(!by_id("cyan-media").installed, "probe → None ⇒ uninstalled");
    for roadmap in ["premiere", "after-effects", "resolve", "ailut"] {
        assert!(!by_id(roadmap).installed, "roadmap entries never installed");
    }
    // Roadmap entries never invoke the closure: calls == live-with-flagship count (2).
    assert_eq!(calls.load(Ordering::SeqCst), 2, "lookup called for the two LIVE flagships only");
}

#[test]
fn flagship_owner_mismatch_is_uninstalled() {
    ensure_db();
    let lookup = |tool: &str| -> Option<String> {
        (tool == "upload_for_review").then(|| "evil-plugin".to_string())
    };
    let r = resolve(None, "producer", "promo", &lookup).expect("resolve");
    let frameio = r.plugins.iter().find(|p| p.id == "frameio").expect("frameio");
    assert!(!frameio.installed, "a different owning plugin is NOT installed (+ obs warn)");
}

// ════════════════════════════════════════════════════════════════════════════
// T49-T51 — tenant overrides + the v2 save.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn user_override_wins_over_builtin() {
    ensure_db();
    const TENANT: &str = "t49-tenant";
    let saved = templates::save_roletype_template(
        TENANT,
        &serde_json::json!({
            "name": "House Commercial Flow",
            "source": "user", "tenant_id": TENANT, "created_at": 0, "id": "ignored",
            "format_type": "commercial",
            "maturity": "extensible",
            "steps": [ {"text": "House step one (manual)"}, {"text": "Await producer notes"} ],
            "note_kinds": ["creative-brief", "decision"],
        })
        .to_string(),
    );
    assert!(saved.get("error").is_none(), "{saved}");

    let r = resolve(Some(TENANT), "producer", "commercial", &no_lookup).expect("resolve");
    assert_eq!(r.template.expect("template").name, "House Commercial Flow");

    // Another tenant still gets the builtin.
    let other = resolve(Some("t49-other-tenant"), "producer", "commercial", &no_lookup)
        .expect("resolve");
    assert_eq!(other.template.expect("template").id, "builtin:roletype:commercial");
}

#[test]
fn legacy_user_template_excluded_from_selector() {
    ensure_db();
    const TENANT: &str = "t50-tenant";
    // A LEGACY user template (pre-A3: NULL roletype columns) via the v1 save.
    let legacy = templates::save_as_template(
        TENANT,
        "Legacy Flow",
        "pre-A3 save",
        vec![cyan_backend::models::dto::TemplateStep {
            text: "Old step".to_string(),
            plugin: None,
            stage: None,
        }],
    )
    .expect("v1 save");

    // In list_templates…
    assert!(
        templates::list_templates(TENANT).iter().any(|t| t.id == legacy.id),
        "legacy template lists"
    );
    // …but NEVER in the selector (catalog_version is NULL).
    for format in ["promo", "commercial", "short_film", "episodic", "feature"] {
        let r = resolve(Some(TENANT), "producer", format, &no_lookup).expect("resolve");
        assert_ne!(
            r.template.expect("template").id,
            legacy.id,
            "a NULL-column row never enters resolve()"
        );
    }
}

#[test]
fn save_v2_rejects_invalid_format_type() {
    ensure_db();
    const TENANT: &str = "t51-tenant";
    let before = storage::template_list_by_tenant(TENANT).expect("list").len();
    let rejected = templates::save_roletype_template(
        TENANT,
        &serde_json::json!({
            "name": "Sitcom Flow",
            "source": "user", "tenant_id": TENANT, "created_at": 0, "id": "x",
            "format_type": "sitcom",
            "steps": [ {"text": "step (manual)"} ],
        })
        .to_string(),
    );
    assert_eq!(rejected["error"], serde_json::json!("invalid_format_type"));
    assert_eq!(rejected["given"], serde_json::json!("sitcom"));
    assert_eq!(rejected["allowed"].as_array().map(Vec::len), Some(5), "allowed vocab in the error");
    assert_eq!(
        storage::template_list_by_tenant(TENANT).expect("list").len(),
        before,
        "the WHOLE save rejected — nothing persisted"
    );

    // A valid save returns the server-stamped fields.
    let saved = templates::save_roletype_template(
        TENANT,
        &serde_json::json!({
            "name": "Real Promo Flow",
            "source": "builtin", "tenant_id": "spoofed", "created_at": 999, "id": "spoofed-id",
            "format_type": "promo",
            "steps": [ {"text": "step (manual)"} ],
            "ae_duties": [{"junk": true}],
            "orchestration": "ignored",
        })
        .to_string(),
    );
    assert!(saved.get("error").is_none(), "unknown keys tolerated-and-ignored: {saved}");
    assert_eq!(saved["source"], serde_json::json!("user"), "source server-stamped");
    assert_eq!(saved["tenant_id"], serde_json::json!(TENANT), "tenant server-stamped");
    assert_eq!(saved["scope"], serde_json::json!("tenant"), "scope server-stamped");
    assert_eq!(saved["catalog_version"], serde_json::json!("roletype.v1"));
    assert_eq!(saved["id"].as_str().map(str::len), Some(64), "blake3-minted id");
    assert_ne!(saved["id"], serde_json::json!("spoofed-id"));
}

// ════════════════════════════════════════════════════════════════════════════
// ROLE→SURFACE — the additive `primary_surface` dimension (nav only; orthogonal
// to template/RBAC/author_role). Every vocab role maps to a stable surface id;
// resolve() carries it; the wire stays additive (old shape → board_wall).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn primary_surface_maps_every_vocab_role() {
    // Deterministic, total over the vocab — no role falls through to fallback
    // by accident (the fallback is reserved for genuinely-unknown roles).
    assert_eq!(primary_surface_for("studio_exec"), "board_wall");
    assert_eq!(primary_surface_for("producer"), "shows");
    assert_eq!(primary_surface_for("director"), "review_player");
    assert_eq!(primary_surface_for("editor"), "notebook");
    assert_eq!(primary_surface_for("colorist"), "notebook");
    assert_eq!(primary_surface_for("sound"), "notebook");
    assert_eq!(primary_surface_for("assistant_editor"), "ae_queue");
    // Unknown role → safe read-only landing, never a crash.
    assert_eq!(primary_surface_for("showrunner"), "board_wall");
    // Coverage: every canonical vocab role resolves to a non-empty surface.
    for role in PRODUCTION_ROLE_VOCAB {
        assert!(!primary_surface_for(role).is_empty(), "surface for {role}");
    }
}

#[test]
fn resolve_carries_primary_surface() {
    ensure_db();
    let prod = resolve(None, "producer", "promo", &no_lookup).expect("resolve");
    assert_eq!(prod.primary_surface, "shows");
    let exec = resolve(None, "studio_exec", "promo", &no_lookup).expect("resolve");
    assert_eq!(exec.primary_surface, "board_wall", "observe_only role still lands");
    let ae = resolve(None, "assistant_editor", "feature", &no_lookup).expect("resolve");
    assert_eq!(ae.primary_surface, "ae_queue");
    // Present on the wire.
    let wire = serde_json::to_value(&prod).expect("encode");
    assert_eq!(wire["primary_surface"], serde_json::json!("shows"));
}

#[test]
fn primary_surface_wire_is_additive() {
    // Old-shape JSON (predating the dimension) decodes to the safe fallback.
    let old = serde_json::json!({
        "catalog_version": "roletype.v1", "role": "editor", "format_type": "promo",
        "observe_only": false, "template": null, "plugins": [],
        "agentification": { "role": "editor", "tasks": [], "agent_does": [], "human_does": [] },
    });
    let decoded: SelectorResult = serde_json::from_value(old).expect("old shape decodes");
    assert_eq!(decoded.primary_surface, "board_wall", "absent → board_wall fallback");
}
