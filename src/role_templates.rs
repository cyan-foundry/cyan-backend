//! A3 — the role×type selector: builtin roletype catalog, agentification maps,
//! AE duty catalog, orchestration bridge, and the deterministic `resolve()`
//! (DETAILED §9b/§9c/§9c-bis/§9d). Offline, zero-LLM, pure data + one storage
//! read (the tenant override lookup).
//!
//! Vocabulary note (SYN-1): the craft-role vocabulary LIVES in
//! `crate::models::dto::PRODUCTION_ROLE_VOCAB` — A3 owns the VALUES, consumes
//! the const; never a second literal.
//!
//! Display rule (D-A3.4): plugin `roadmap` and task/duty `coming` render the
//! SAME greyed "coming to your rooms" affordance — one look, two axes;
//! `build_now` renders a distinct "in this release" chip, never greyed, never
//! presented as running today.

use serde::{Deserialize, Serialize};

use crate::models::dto::{Template, TemplatePlugin, TemplateStep, PRODUCTION_ROLE_VOCAB};

// ═══════════════════════════════════════════════════════════════════════════
// Vocabularies (§3, A3-owned)
// ═══════════════════════════════════════════════════════════════════════════

pub const FORMAT_TYPE_VOCAB: [&str; 5] = ["promo", "commercial", "short_film", "episodic", "feature"];
/// Kept for overrides; all five BUILTINS ship `"mvp"` (D-A3.1).
pub const TEMPLATE_MATURITY_VOCAB: [&str; 2] = ["mvp", "extensible"];
/// A plugin exists or doesn't — a DIFFERENT axis from task status.
pub const PLUGIN_STATUS_VOCAB: [&str; 2] = ["live", "roadmap"];
pub const PLUGIN_EXECUTION_VOCAB: [&str; 3] = ["device", "cloud", "both"];
pub const TASK_OWNER_VOCAB: [&str; 3] = ["agent", "human", "agent_gated"];

/// REV 2 — ONE 3-state status vocab, SHARED SPELLING with B3's AE-queue rows
/// (ORCH-11) — tasks, duties, and bridge rows all use it. `build_now` = a named
/// component in THIS run's build packages delivers it.
pub const TASK_STATUS_VOCAB: [&str; 3] = ["live", "build_now", "coming"];

/// REV 2 — closed gate vocab (D-A3.11). Qualifiers (legal gate, heal advisory)
/// live in `gate_caveat` display strings — never in the gate token.
pub const GATE_VOCAB: [&str; 3] = ["confirm", "external_send", "human_ok"];

pub const DEPT_VOCAB: [&str; 5] = ["editorial", "sound", "color", "vfx", "delivery"];

/// REV 2 — Rule R2 v2: the FOUR device-bindable live mentions (A3 GC1 —
/// `@cyan-media.conform` is proven bound in the shipping seed
/// `builtin:frameio-review-loop`).
pub const LIVE_BOUND_MENTIONS: [&str; 4] = [
    "@cyan-media.probe",
    "@cyan-media.conform",
    "@frameio.upload_for_review",
    "@frameio.list_comments",
];

/// Stays v1 — nothing shipped; the bump rule applies to post-ship edits (D-A3.10).
pub const ROLETYPE_CATALOG_VERSION: &str = "roletype.v1";

/// Template `scope` values (§9a): `"tenant"` live; `"user"`/`"group"`
/// stored-but-treated-as-tenant (roadmap).
pub const TEMPLATE_SCOPE_VOCAB: [&str; 3] = ["tenant", "user", "group"];

/// `source` for the merged roletype builtins. Deliberately NOT the media-seed
/// `"builtin"` literal — the frozen seed tests pin `source == "builtin"` to
/// exactly the four media seeds; the roletype catalog is its own lane.
pub const SOURCE_BUILTIN_ROLETYPE: &str = "builtin:roletype";

// ═══════════════════════════════════════════════════════════════════════════
// §9b — the FIVE builtin roletype templates (full, maturity "mvp"; D-A3.1)
// ═══════════════════════════════════════════════════════════════════════════

fn step(text: &str, plugin: Option<&str>, stage: &str) -> TemplateStep {
    TemplateStep {
        text: text.to_string(),
        plugin: plugin.map(str::to_string),
        stage: Some(stage.to_string()),
    }
}

fn plugin(id: &str, status: &str, execution: &str, flagship: Option<&str>, spec: Option<&str>) -> TemplatePlugin {
    TemplatePlugin {
        id: id.to_string(),
        status: status.to_string(),
        execution: execution.to_string(),
        flagship_tool: flagship.map(str::to_string),
        spec: spec.map(str::to_string),
    }
}

/// §9b's plugin registry rows by id (LIVE per lens builtins + proven conform;
/// ROADMAP per the five converged postprod specs; after-effects draft).
fn registry_plugin(id: &str) -> TemplatePlugin {
    match id {
        "cyan-media" => plugin("cyan-media", "live", "both", Some("probe"), None),
        "frameio" => plugin("frameio", "live", "both", Some("upload_for_review"), None),
        "after-effects" => plugin("after-effects", "roadmap", "device", None, Some("draft")),
        "premiere" | "resolve" | "ailut" | "protools" | "avid" => {
            plugin(id, "roadmap", "device", None, Some("converged"))
        }
        // final-draft, sync-agent — roadmap/device, no spec.
        _ => plugin(id, "roadmap", "device", None, None),
    }
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn builtin(
    format: &str,
    name: &str,
    description: &str,
    stages: &[&str],
    plugins: &[&str],
    note_kinds: &[&str],
    steps: Vec<TemplateStep>,
) -> Template {
    Template {
        id: format!("builtin:roletype:{format}"),
        tenant_id: String::new(),
        name: name.to_string(),
        description: description.to_string(),
        source: SOURCE_BUILTIN_ROLETYPE.to_string(),
        steps,
        created_at: 0,
        format_type: Some(format.to_string()),
        stages: strs(stages),
        note_kinds: strs(note_kinds),
        plugins: plugins.iter().map(|p| registry_plugin(p)).collect(),
        maturity: Some("mvp".to_string()),
        catalog_version: Some(ROLETYPE_CATALOG_VERSION.to_string()),
        scope: Some("tenant".to_string()),
    }
}

/// All 13 note kinds in the §3 literal order (episodic + feature adopt the full set).
fn all_note_kinds() -> Vec<&'static str> {
    crate::models::dto::NOTE_KIND_VOCAB.to_vec()
}

/// The five builtin roletype templates (§9b, byte-carried step texts).
pub fn builtin_roletype_templates() -> Vec<Template> {
    let all13 = all_note_kinds();
    vec![
        builtin(
            "promo",
            "Promo",
            "Repurpose a source spot into a promo — brand graphics, base look, review loop, delivery.",
            &["repurpose", "edit", "graphics", "color", "version", "deliver"],
            &["cyan-media", "frameio", "premiere", "after-effects", "resolve", "ailut"],
            &["creative-brief", "constitution", "preference", "creative-dna", "decision",
              "editor-note", "legal-clearance", "qc-report"],
            vec![
                step("Probe the source spot with @cyan-media.probe", Some("cyan-media"), "ingest"),
                step("Organize the repurpose pull per house rules — bins, naming (manual)", None, "editorial"),
                step("Cut the promo per the creative brief — creative cut (manual)", None, "edit"),
                step("Apply brand graphics and end-card per house rules — After Effects coming to your rooms (manual)", None, "edit"),
                step("Grade the base look per creative DNA — Resolve + AI-LUT coming to your rooms (manual)", None, "color"),
                step("Upload the cut for review with @frameio.upload_for_review", Some("frameio"), "review"),
                step("Await producer notes/review", None, "review"),
                step("Sense notes and conform with @frameio.list_comments", Some("frameio"), "review"),
                step("Version and deliver per delivery house rules — producer OKs, incl. legal clearance (manual)", None, "delivery"),
            ],
        ),
        builtin(
            "commercial",
            "Commercial",
            "Shoot-to-spot chain — dailies, agency review loop, sound turnover, grade, cutdowns.",
            &["ingest", "dailies", "edit", "review", "sound", "color", "version", "deliver"],
            &["cyan-media", "frameio", "premiere", "after-effects", "protools", "resolve", "ailut", "sync-agent"],
            &["creative-brief", "script", "shot-log", "continuity", "constitution", "preference",
              "creative-dna", "decision", "editor-note", "legal-clearance", "turnover", "qc-report"],
            vec![
                step("Ingest and probe the shoot media with @cyan-media.probe", Some("cyan-media"), "ingest"),
                step("Organize dailies into bins per house rules — bins, naming (manual)", None, "dailies"),
                step("Sync picture and sound — sync agent coming (manual)", None, "dailies"),
                step("Log shots into the structured shot log — auto-populate coming; scripty corrects rows (manual)", None, "dailies"),
                step("Assemble selects from circle takes for the agency — coming; editor picks the takes (manual)", None, "edit"),
                step("Cut the spot per the creative brief — creative cut (manual)", None, "edit"),
                step("Upload the cut for agency review with @frameio.upload_for_review", Some("frameio"), "review"),
                step("Await agency/client notes/review", None, "review"),
                step("Sense notes and conform with @frameio.list_comments", Some("frameio"), "review"),
                step("Stage the sound turnover to Pro Tools — coming; producer gates the send (manual)", None, "sound"),
                step("Grade and finish in Resolve — coming; AI-LUT base look (manual)", None, "color"),
                step("Version the cutdowns and deliver per delivery house rules — producer OKs, incl. legal clearance (manual)", None, "deliver"),
            ],
        ),
        builtin(
            "short_film",
            "Short film",
            "Script through finish — dailies, shot log, director review loop, sound turnover, finish in Resolve.",
            &["script", "shot_log", "edit", "conform", "color", "mix", "finish"],
            &["cyan-media", "frameio", "avid", "protools", "resolve", "ailut", "final-draft", "sync-agent"],
            &["script", "lined-script", "shot-log", "continuity", "constitution", "preference",
              "creative-dna", "decision", "editor-note", "turnover", "qc-report"],
            vec![
                step("Import the script — Final Draft import coming; author the script note set today (manual)", None, "editorial"),
                step("Ingest and probe dailies with @cyan-media.probe", Some("cyan-media"), "ingest"),
                step("Sync picture and sound — sync agent coming (manual)", None, "editorial"),
                step("Log shots into the structured shot log — auto-populate coming; scripty corrects rows (manual)", None, "editorial"),
                step("Assemble scene stringouts from circle/print takes — coming; editor picks the takes (manual)", None, "editorial"),
                step("Cut the film — creative cut (manual)", None, "edit"),
                step("Upload the cut for review with @frameio.upload_for_review", Some("frameio"), "review"),
                step("Await director notes/review", None, "review"),
                step("Sense notes and conform with @frameio.list_comments", Some("frameio"), "review"),
                step("Stage the sound turnover to Pro Tools — coming; producer gates the send (manual)", None, "sound"),
                step("Conform, grade and finish in Resolve — coming; render the master per delivery house rules (manual)", None, "color"),
            ],
        ),
        builtin(
            "episodic",
            "Episodic",
            "Per-episode chain; the show bible is the GROUP-scope constitution — author it once per show, every episode resolves it",
            &["script", "dailies", "edit", "review", "sound", "color", "qc", "deliver"],
            &["cyan-media", "frameio", "avid", "protools", "resolve", "ailut", "final-draft", "sync-agent"],
            &all13,
            vec![
                step("Import the episode script — Final Draft import coming; author the script note set today (manual)", None, "script"),
                step("Ingest and probe dailies with @cyan-media.probe", Some("cyan-media"), "dailies"),
                step("Sync picture and sound — sync agent coming (manual)", None, "dailies"),
                step("Log shots into the structured shot log — auto-populate coming; scripty corrects rows (manual)", None, "dailies"),
                step("Assemble scene stringouts from circle/print takes — coming; editor picks the takes (manual)", None, "edit"),
                step("Cut the episode per the show bible and creative brief — creative cut (manual)", None, "edit"),
                step("Upload the cut for review with @frameio.upload_for_review", Some("frameio"), "review"),
                step("Await showrunner/network notes/review", None, "review"),
                step("Sense notes and conform with @frameio.list_comments", Some("frameio"), "review"),
                step("Stage the sound turnover to Pro Tools — coming; producer gates the send (manual)", None, "sound"),
                step("Conform and grade in Resolve per the show LUT — coming (manual)", None, "color"),
                step("QC the episode against network delivery specs — deep QC coming; basic black/freeze + loudness runs on the rail today (manual)", None, "qc"),
                step("Package and deliver the episode per network delivery house rules — producer OKs, incl. legal clearance (manual)", None, "deliver"),
            ],
        ),
        builtin(
            "feature",
            "Feature",
            "Screenplay through delivered master — full editorial chain, conform, sound turnover, grade, QC, package.",
            &["script", "dailies", "editorial", "edit", "review", "conform", "sound", "color", "qc", "deliver"],
            &["cyan-media", "frameio", "avid", "protools", "resolve", "ailut", "final-draft", "sync-agent"],
            &all13,
            vec![
                step("Import the screenplay — Final Draft import coming; author the script note set today (manual)", None, "script"),
                step("Ingest and probe dailies with @cyan-media.probe", Some("cyan-media"), "dailies"),
                step("Sync picture and sound — sync agent coming (manual)", None, "dailies"),
                step("Log shots into the structured shot log — auto-populate coming; scripty corrects rows (manual)", None, "dailies"),
                step("Line the script against coverage — lined-script notes; import coming (manual)", None, "editorial"),
                step("Track continuity per scene and take — continuity notes; scripty authors today (manual)", None, "editorial"),
                step("Assemble scene stringouts from circle/print takes — coming; editor picks the takes (manual)", None, "editorial"),
                step("Cut the film — creative cut (manual)", None, "edit"),
                step("Upload the cut for review with @frameio.upload_for_review", Some("frameio"), "review"),
                step("Await director/producer notes/review", None, "review"),
                step("Sense notes and conform with @frameio.list_comments", Some("frameio"), "review"),
                step("Conform the locked cut against the source pool with @cyan-media.conform", Some("cyan-media"), "conform"),
                step("Stage the sound turnover to Pro Tools — coming; producer gates the send (manual)", None, "sound"),
                step("Grade the film in Resolve per creative DNA — coming; AI-LUT base look (manual)", None, "color"),
                step("QC the master against delivery house rules — deep QC coming; basic black/freeze + loudness runs on the rail today (manual)", None, "qc"),
                step("Package and deliver the master per delivery house rules — producer OKs, incl. legal clearance (manual)", None, "deliver"),
            ],
        ),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════
// §9c — agentification: base map + (role, format) delta overlay
// ═══════════════════════════════════════════════════════════════════════════

/// One agentification task row. Serialize rule (§9c): `agent_does` = tasks with
/// owner ∈ {agent, agent_gated}; `human_does` = human rows + every agent_gated
/// task re-listed `"<task>:gate"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentificationTask {
    pub task: String,
    /// `TASK_OWNER_VOCAB`.
    pub owner: String,
    /// `TASK_STATUS_VOCAB` (3-state, shared with B3 — ORCH-11).
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    /// `GATE_VOCAB`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<String>,
}

fn task(
    id: &str,
    owner: &str,
    status: &str,
    component: Option<&str>,
    gate: Option<&str>,
) -> AgentificationTask {
    AgentificationTask {
        task: id.to_string(),
        owner: owner.to_string(),
        status: status.to_string(),
        component: component.map(str::to_string),
        gate: gate.map(str::to_string),
    }
}

/// The §9c BASE map (format default) for one role. `studio_exec` ⇒ empty
/// (observe_only).
pub fn base_agentification(role: &str) -> Vec<AgentificationTask> {
    match role {
        "producer" => vec![
            task("brief_to_workflow_generation", "agent", "live", Some("generation"), None),
            task("freeform_note_structuring", "agent_gated", "build_now", Some("/notes/structure"), Some("confirm")),
            task("version_orchestration", "agent", "coming", Some("premiere"), None),
            task("review_loop_management", "agent", "live", Some("review-loop"), None),
            task("approvals", "human", "live", None, None),
            task("legal_clearance", "human", "live", None, None),
        ],
        "assistant_editor" => vec![
            task("ingest_transcode_proxy", "agent", "live", Some("ingest-rail"), None),
            task("organize_bins_naming", "agent_gated", "coming", Some("cyan-media"), Some("confirm")),
            task("sync_picture_sound", "agent", "coming", Some("sync-agent"), None),
            task("log_shots", "agent_gated", "build_now", Some("/notes/structure/report"), Some("confirm")),
            task("stringouts_selects", "agent_gated", "coming", Some("structured-notes"), Some("confirm")),
            task("qc_basic", "agent", "live", Some("cyan-media"), None),
            task("qc_against_house_rules", "agent", "coming", Some("cyan-media"), None),
            task("turnovers", "agent_gated", "coming", Some("protools"), Some("external_send")),
            task("conform_relink_versioning", "agent", "live", Some("review-loop"), None),
            task("render_deliverables", "agent", "live", Some("ingest-rail"), None),
            task("publish_deliverables", "agent_gated", "live", Some("frameio"), Some("external_send")),
            task("credential_self_heal", "agent", "live", Some("frameio-refresh"), None),
            task("failure_triage", "agent", "live", Some("lens-heal"), None),
        ],
        "editor" => vec![
            task("mechanical_version_edits", "agent", "live", Some("review-loop"), None),
            task("organized_material_handoff", "agent", "coming", Some("structured-notes"), None),
            task("creative_cut", "human", "live", None, None),
        ],
        "director" => vec![
            task("note_to_edit_loop", "agent", "live", Some("review-loop"), None),
            task("dcc_agentic_edits", "agent", "coming", Some("premiere"), None),
            task("creative_direction", "human", "live", None, None),
        ],
        "colorist" => vec![
            task("conform", "agent", "live", Some("review-loop"), None),
            task("base_look", "agent", "coming", Some("ailut"), None),
            task("render_master", "agent_gated", "coming", Some("resolve"), Some("confirm")),
            task("creative_grade", "human", "live", None, None),
        ],
        "sound" => vec![
            task("sound_turnover", "agent_gated", "coming", Some("protools"), Some("external_send")),
            task("layback", "agent", "coming", Some("protools"), None),
            task("audio_qc_loudness", "agent", "live", Some("cyan-media"), None),
            task("audio_qc_deep", "agent", "coming", Some("cyan-media"), None),
            task("the_mix", "human", "live", None, None),
        ],
        // studio_exec — observe_only, no tasks.
        _ => Vec::new(),
    }
}

/// One delta op over a role's base map (§9c).
#[derive(Debug, Clone)]
pub enum DeltaOp {
    Remove(&'static str),
    Add(AgentificationTask),
    Component(&'static str, &'static str),
}

/// One `(role, formats)` delta row.
pub struct FormatDelta {
    pub role: &'static str,
    pub formats: &'static [&'static str],
    pub op: DeltaOp,
}

/// §9c's `AGENTIFICATION_FORMAT_DELTAS` — the ONLY cells differing from the base.
pub fn agentification_format_deltas() -> Vec<FormatDelta> {
    vec![
        FormatDelta { role: "assistant_editor", formats: &["promo"], op: DeltaOp::Remove("sync_picture_sound") },
        FormatDelta { role: "assistant_editor", formats: &["promo"], op: DeltaOp::Remove("log_shots") },
        FormatDelta { role: "assistant_editor", formats: &["promo"], op: DeltaOp::Remove("stringouts_selects") },
        FormatDelta { role: "assistant_editor", formats: &["promo"], op: DeltaOp::Remove("turnovers") },
        FormatDelta {
            role: "assistant_editor",
            formats: &["feature", "episodic"],
            op: DeltaOp::Add(task("multicam_grouping", "agent", "coming", Some("sync-agent"), None)),
        },
        FormatDelta {
            role: "producer",
            formats: &["episodic"],
            op: DeltaOp::Add(task("show_bible_stewardship", "human", "live", Some("constitution"), None)),
        },
        FormatDelta {
            role: "producer",
            formats: &["short_film", "episodic", "feature"],
            op: DeltaOp::Remove("version_orchestration"),
        },
        FormatDelta { role: "editor", formats: &["promo"], op: DeltaOp::Remove("organized_material_handoff") },
        FormatDelta {
            role: "director",
            formats: &["short_film", "episodic", "feature"],
            op: DeltaOp::Component("dcc_agentic_edits", "avid"),
        },
        FormatDelta { role: "sound", formats: &["promo"], op: DeltaOp::Remove("sound_turnover") },
        FormatDelta { role: "sound", formats: &["promo"], op: DeltaOp::Remove("layback") },
    ]
}

/// Base map + format deltas (applied BEFORE serialization — §9d step 5).
/// Returns `(tasks, deltas_applied)`.
pub fn resolved_agentification(role: &str, format_type: &str) -> (Vec<AgentificationTask>, u64) {
    let mut tasks = base_agentification(role);
    let mut applied: u64 = 0;
    for delta in agentification_format_deltas() {
        if delta.role != role || !delta.formats.contains(&format_type) {
            continue;
        }
        match &delta.op {
            DeltaOp::Remove(id) => {
                let before = tasks.len();
                tasks.retain(|t| t.task != *id);
                if tasks.len() != before {
                    applied += 1;
                }
            }
            DeltaOp::Add(row) => {
                tasks.push(row.clone());
                applied += 1;
            }
            DeltaOp::Component(id, component) => {
                for t in tasks.iter_mut().filter(|t| t.task == *id) {
                    t.component = Some((*component).to_string());
                    applied += 1;
                }
            }
        }
    }
    (tasks, applied)
}

// ═══════════════════════════════════════════════════════════════════════════
// §9c-bis — AE duty catalog + orchestration bridge (deterministic data)
// ═══════════════════════════════════════════════════════════════════════════

/// One AI-assistant-editor duty (§9c-bis). Per ORCH-11, B3's ELEVEN-row set is
/// canonical: B3's 11 row ids ⊆ these 13; statuses share `TASK_STATUS_VOCAB`
/// and must agree id-for-id (T-A3-8; B3-side mirror T-ln-13).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DutyEntry {
    pub id: String,
    pub title: String,
    /// `TASK_STATUS_VOCAB` (shared with B3 — ORCH-11).
    pub status: String,
    pub component: String,
    /// `GATE_VOCAB` or None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<String>,
    /// Display qualifier, never a gate token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_caveat: Option<String>,
    /// build_now/coming rows only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_ref: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn duty(
    id: &str,
    title: &str,
    status: &str,
    component: &str,
    gate: Option<&str>,
    gate_caveat: Option<&str>,
    spec_ref: Option<&str>,
) -> DutyEntry {
    DutyEntry {
        id: id.to_string(),
        title: title.to_string(),
        status: status.to_string(),
        component: component.to_string(),
        gate: gate.map(str::to_string),
        gate_caveat: gate_caveat.map(str::to_string),
        spec_ref: spec_ref.map(str::to_string),
    }
}

/// The 13-duty catalog (B3's 11 row ids ∪ {multicam, heal}). A fn rather than a
/// `const` so the entries are owned serde types (the wire needs Deserialize on
/// the iOS mirror side; owned strings keep the round-trip total) — the DATA is
/// the §9c-bis table verbatim.
pub fn ae_duty_catalog() -> Vec<DutyEntry> {
    vec![
        duty("ingest", "Ingest, transcode, proxy", "live", "ingest-rail (scan/source/produce ops)", None, None, None),
        duty("organize", "Organize bins + naming per house rules", "coming", "cyan-media + constitution", Some("confirm"), None, Some("AI_ASSISTANT_EDITOR §map")),
        duty("sync", "Sync picture and sound", "coming", "sync-agent", None, None, Some("AI_ASSISTANT_EDITOR §open items")),
        duty("shot_log", "Structured shot log (hand entry)", "build_now", "A1 shot-log kind + A4 composer (hand entry)", Some("confirm"), None, Some("A1 §4.3")),
        duty("import_report", "Import camera/sound reports", "build_now", "A5 report lane (/notes/structure/report) + A4 ReportImportView", Some("confirm"), None, Some("A5 §9h")),
        duty("stringouts", "Scene stringouts from circle takes", "coming", "structured-notes derivation + NLE assembly", Some("confirm"), None, Some("AI_ASSISTANT_EDITOR §open items")),
        duty("qc", "Basic QC (black/freeze + loudness)", "live", "cyan-media qc_black_freeze + qc_loudness", None, None, None),
        duty("deep_qc", "Deep QC against house rules", "coming", "cyan-media + constitution", None, None, Some("AI_ASSISTANT_EDITOR §open items")),
        duty("conform", "Conform confirmed edits", "live", "review-loop conform rail", Some("confirm"), None, None),
        duty("turnover", "Publish turnover for review", "live", "frameio publish (external_send always human-fired)", Some("external_send"), None, None),
        duty("deliver", "Render + deliver the master", "live", "ingest produce_master + human OK", Some("human_ok"), Some("incl. legal-clearance gate — A5 inject_legal_gate"), None),
        duty("multicam", "Multicam grouping", "coming", "sync-agent", None, None, Some("AI_ASSISTANT_EDITOR §open items")),
        duty("heal", "Self-heal credentials + triage failures", "live", "lens SelfHealer + backend frameio_refresh", None, Some("diagnosis advisory; side-effectful fixes are human-gated proposals"), None),
    ]
}

/// One cross-department handoff (§9c-bis). Same 5 rows every format v1
/// (D-A3.7); each row carries ONE status (the DOMINANT carrier's;
/// secondary-artifact honesty lives in `live_rail`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffEntry {
    pub id: String,
    /// `DEPT_VOCAB`.
    pub from_dept: String,
    pub to_dept: String,
    /// Artifact names, display.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepares: Vec<String>,
    /// A1 wire ids (13-vocab).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub note_kinds: Vec<String>,
    /// `"turnover"` — SHIPPED, A1 §4.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_kind: Option<String>,
    /// Dispatcher ops / tools carrying it today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_rail: Option<String>,
    /// `TASK_STATUS_VOCAB` — ONE value.
    pub status: String,
    /// `GATE_VOCAB`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_caveat: Option<String>,
}

/// The 5-row bridge (editorial→sound/color/vfx, sound→delivery, color→delivery).
/// Consumers: the selector UI; B3's AE queue (bridge↔queue pairs pinned —
/// `editorial_to_color ↔ conform`, `color_to_delivery ↔ deliver`; B3's
/// `turnover` row deliberately has NO bridge counterpart — a review-loop hop,
/// not a department handoff, SR-10). NOT carried on /generate v1 (D-A3.8).
#[allow(clippy::too_many_arguments)]
fn handoff(
    id: &str,
    from_dept: &str,
    to_dept: &str,
    prepares: &[&str],
    note_kinds: &[&str],
    handoff_kind: Option<&str>,
    live_rail: Option<&str>,
    status: &str,
    gate: Option<&str>,
    gate_caveat: Option<&str>,
) -> HandoffEntry {
    HandoffEntry {
        id: id.to_string(),
        from_dept: from_dept.to_string(),
        to_dept: to_dept.to_string(),
        prepares: strs(prepares),
        note_kinds: strs(note_kinds),
        handoff_kind: handoff_kind.map(str::to_string),
        live_rail: live_rail.map(str::to_string),
        status: status.to_string(),
        gate: gate.map(str::to_string),
        gate_caveat: gate_caveat.map(str::to_string),
    }
}

pub fn orchestration_bridge() -> Vec<HandoffEntry> {
    vec![
        handoff(
            "editorial_to_sound",
            "editorial",
            "sound",
            &["AAF turnover (split-mono embedded BWF)", "cut-ref QT", "edit-notes digest"],
            &["shot-log", "decision", "editor-note", "turnover"],
            Some("turnover"),
            Some("cut-ref publish: @frameio.upload_for_review (live); AAF authoring: none (protools plugin, coming)"),
            "coming",
            Some("external_send"),
            None,
        ),
        handoff(
            "editorial_to_color",
            "editorial",
            "color",
            &["conformed cut", "cut list", "grade-notes slice"],
            &["creative-dna", "decision", "turnover"],
            Some("turnover"),
            Some("conform: review conform_proxy/conform_run/publish_proxy + changelist conform_plan/conform_map + @cyan-media.conform — the carrier, LIVE; EDL/XML export: resolve plugin, coming"),
            "live",
            Some("confirm"),
            None,
        ),
        handoff(
            "editorial_to_vfx",
            "editorial",
            "vfx",
            &["VFX pull list", "plates/change-list", "reference QT"],
            &["decision", "editor-note", "turnover"],
            Some("turnover"),
            None,
            "coming",
            Some("external_send"),
            None,
        ),
        handoff(
            "sound_to_delivery",
            "sound",
            "delivery",
            &["mixed-stems layback", "loudness QC vs house target"],
            &["constitution", "legal-clearance", "qc-report"],
            None,
            Some("layback: protools, coming — the carrier; loudness QC findings already live (qc_loudness)"),
            "coming",
            Some("confirm"),
            None,
        ),
        handoff(
            "color_to_delivery",
            "color",
            "delivery",
            &["graded master render", "QC report", "delivery package"],
            &["constitution", "legal-clearance", "decision", "qc-report", "turnover"],
            Some("turnover"),
            Some("ingest produce_master_plan/produce_master + changelist set_outcome/version freeze + basic qc — LIVE; deep QC coming"),
            "live",
            Some("human_ok"),
            Some("legal gate — A5 inject_legal_gate; Layer-2 external_send regardless"),
        ),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════
// §9d — the selector resolve
// ═══════════════════════════════════════════════════════════════════════════

/// The selector's template projection (§9d).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectorTemplate {
    pub id: String,
    pub name: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maturity: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<String>,
    pub steps: Vec<TemplateStep>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub note_kinds: Vec<String>,
}

/// One plugin row with its per-device installed flag (§9d step 4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectorPluginEntry {
    pub id: String,
    pub status: String,
    pub execution: String,
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flagship_tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<String>,
}

/// The agentification wire shape (§9c serialize rule; SYN-3: keys are
/// `agent_does`/`human_does`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectorAgentification {
    pub role: String,
    pub tasks: Vec<AgentificationTask>,
    pub agent_does: Vec<String>,
    pub human_does: Vec<String>,
}

/// The resolve result (§9d). Presence rules: Options omitted when `None` —
/// EXCEPT `template`, ALWAYS present (JSON `null` for studio_exec). The two
/// REV-2 arrays are skip-if-empty (absent, never `[]` — T-A3-4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectorResult {
    pub catalog_version: String,
    pub role: String,
    pub format_type: String,
    pub observe_only: bool,
    pub template: Option<SelectorTemplate>,
    pub plugins: Vec<SelectorPluginEntry>,
    pub agentification: SelectorAgentification,
    /// `role == "assistant_editor"` only; other roles omit the field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ae_duties: Vec<DutyEntry>,
    /// All producing roles; empty (absent) for studio_exec.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orchestration: Vec<HandoffEntry>,
}

/// Typed resolve errors — the FFI maps each to its exact §9d error payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorError {
    UnknownRole(String),
    UnknownFormatType(String),
    MissingParam(&'static str),
}

impl SelectorError {
    /// The exact §9d error JSON (`allowed` lists the full vocab).
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            SelectorError::UnknownRole(given) => serde_json::json!({
                "error": "unknown_role", "given": given, "allowed": PRODUCTION_ROLE_VOCAB,
            }),
            SelectorError::UnknownFormatType(given) => serde_json::json!({
                "error": "unknown_format_type", "given": given, "allowed": FORMAT_TYPE_VOCAB,
            }),
            SelectorError::MissingParam(p) => serde_json::json!({
                "error": "missing_param", "given": p,
            }),
        }
    }
}

fn selector_template(t: &Template) -> SelectorTemplate {
    SelectorTemplate {
        id: t.id.clone(),
        name: t.name.clone(),
        source: t.source.clone(),
        maturity: t.maturity.clone(),
        stages: t.stages.clone(),
        steps: t.steps.clone(),
        note_kinds: t.note_kinds.clone(),
    }
}

/// The §9c serialize rule: `agent_does` = owner ∈ {agent, agent_gated};
/// `human_does` = human rows + every agent_gated task re-listed `"<task>:gate"`.
fn serialize_agentification(role: &str, tasks: Vec<AgentificationTask>) -> SelectorAgentification {
    let agent_does: Vec<String> = tasks
        .iter()
        .filter(|t| t.owner == "agent" || t.owner == "agent_gated")
        .map(|t| t.task.clone())
        .collect();
    let mut human_does: Vec<String> =
        tasks.iter().filter(|t| t.owner == "human").map(|t| t.task.clone()).collect();
    human_does.extend(
        tasks.iter().filter(|t| t.owner == "agent_gated").map(|t| format!("{}:gate", t.task)),
    );
    SelectorAgentification { role: role.to_string(), tasks, agent_does, human_does }
}

/// The deterministic selector resolve (§9d steps 1-7) — offline, zero-LLM.
///
/// `tenant`: `Some` enables the tenant-override lookup (a user template whose
/// `format_type` matches AND whose `catalog_version` is non-NULL, newest wins);
/// `None` ⇒ builtin always. `installed_lookup`: bare flagship tool name IN,
/// OWNING plugin id OUT (`None` = not installed / registry error — the prod
/// closure swallows registry errors to `None` + obs `selector_registry_error`;
/// resolve never fails on lookup).
pub fn resolve(
    tenant: Option<&str>,
    role: &str,
    format_type: &str,
    installed_lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<SelectorResult, SelectorError> {
    // (1) vocab checks (F rows E1-E3).
    if role.is_empty() {
        return Err(SelectorError::MissingParam("role"));
    }
    if format_type.is_empty() {
        return Err(SelectorError::MissingParam("format_type"));
    }
    if !PRODUCTION_ROLE_VOCAB.contains(&role) {
        return Err(SelectorError::UnknownRole(role.to_string()));
    }
    if !FORMAT_TYPE_VOCAB.contains(&format_type) {
        return Err(SelectorError::UnknownFormatType(format_type.to_string()));
    }

    // (3) studio_exec ⇒ observe_only: template null, no plugins, empty map,
    // BOTH REV-2 arrays omitted.
    if role == "studio_exec" {
        let result = SelectorResult {
            catalog_version: ROLETYPE_CATALOG_VERSION.to_string(),
            role: role.to_string(),
            format_type: format_type.to_string(),
            observe_only: true,
            template: None,
            plugins: Vec::new(),
            agentification: serialize_agentification(role, Vec::new()),
            ae_duties: Vec::new(),
            orchestration: Vec::new(),
        };
        tracing::info!(
            "obs selector_resolved role={role} format_type={format_type} observe_only=true deltas_applied=0 duties=0 handoffs=0"
        );
        return Ok(result);
    }

    // (2) template pick — tenant override (format match + catalog_version
    // non-NULL, newest wins) else builtin; tenant None ⇒ builtin always.
    let builtin = builtin_roletype_templates()
        .into_iter()
        .find(|t| t.format_type.as_deref() == Some(format_type));
    let overriding = tenant.and_then(|t| {
        crate::storage::template_list_by_tenant(t)
            .unwrap_or_default()
            .into_iter()
            .filter(|tp| tp.format_type.as_deref() == Some(format_type) && tp.catalog_version.is_some())
            .max_by_key(|tp| tp.created_at)
    });
    let picked = overriding.or(builtin);
    let template = picked.as_ref().map(selector_template);

    // (4) per-plugin installed check via the flagship tool.
    let plugins: Vec<SelectorPluginEntry> = picked
        .as_ref()
        .map(|t| t.plugins.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            let installed = if p.status != "live" {
                // roadmap ⇒ never installed; lookup SKIPPED.
                false
            } else {
                match p.flagship_tool.as_deref() {
                    None => {
                        tracing::warn!(
                            "obs selector_live_plugin_without_flagship plugin={}",
                            p.id
                        );
                        false
                    }
                    Some(tool) => match installed_lookup(tool) {
                        Some(owner) if owner == p.id => true,
                        Some(owner) => {
                            tracing::warn!(
                                "obs selector_tool_owner_mismatch plugin={} tool={tool} owner={owner}",
                                p.id
                            );
                            false
                        }
                        None => false,
                    },
                }
            };
            SelectorPluginEntry {
                id: p.id,
                status: p.status,
                execution: p.execution,
                installed,
                flagship_tool: p.flagship_tool,
                spec: p.spec,
            }
        })
        .collect();

    // (5) agentification = base + format deltas (applied BEFORE serialization).
    let (tasks, deltas_applied) = resolved_agentification(role, format_type);
    let agentification = serialize_agentification(role, tasks);

    // (6) attach the REV-2 arrays: duties for assistant_editor only;
    // orchestration for every producing role.
    let ae_duties: Vec<DutyEntry> =
        if role == "assistant_editor" { ae_duty_catalog() } else { Vec::new() };
    let orchestration: Vec<HandoffEntry> = orchestration_bridge();

    // (7) obs — flat u64 fields (3.2b).
    tracing::info!(
        "obs selector_resolved role={role} format_type={format_type} observe_only=false deltas_applied={} duties={} handoffs={}",
        deltas_applied,
        ae_duties.len() as u64,
        orchestration.len() as u64
    );

    Ok(SelectorResult {
        catalog_version: ROLETYPE_CATALOG_VERSION.to_string(),
        role: role.to_string(),
        format_type: format_type.to_string(),
        observe_only: false,
        template,
        plugins,
        agentification,
        ae_duties,
        orchestration,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Storage migration (A3 template columns; wired into `storage::run_migrations`)
// ═══════════════════════════════════════════════════════════════════════════

/// Additive nullable roletype columns on `templates`. Idempotent (probe one
/// column, add all seven) — the notes payload-column recipe.
pub fn migrate(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    if conn.prepare("SELECT format_type FROM templates LIMIT 1").is_err() {
        tracing::info!("Migration: adding roletype template columns (A3)");
        for col in [
            "format_type TEXT",
            "stages_json TEXT",
            "note_kinds_json TEXT",
            "plugins_json TEXT",
            "maturity TEXT",
            "catalog_version TEXT",
            "scope TEXT",
        ] {
            let _ = conn.execute(&format!("ALTER TABLE templates ADD COLUMN {col}"), []);
        }
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// Construction invariants (E16 + plugin-consistency — verified on the printed
// data, T-A3-5) + vocab pins
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    /// Component ids that are RAILS (not plugins) — exempt from the
    /// plugin-consistency invariant.
    const RAIL_COMPONENTS: [&str; 11] = [
        "generation",
        "review-loop",
        "ingest-rail",
        "structured-notes",
        "/notes/structure",
        "/notes/structure/report",
        "constitution",
        "lens-heal",
        "frameio-refresh",
        "cyan-media",
        "frameio",
    ];

    #[test]
    fn construction_invariants_hold() {
        let templates = builtin_roletype_templates();
        // Every delta names a task id present in (or added to) the role's base map.
        for delta in agentification_format_deltas() {
            let base = base_agentification(delta.role);
            match &delta.op {
                DeltaOp::Remove(id) | DeltaOp::Component(id, _) => {
                    assert!(
                        base.iter().any(|t| t.task == *id),
                        "delta {} names unknown base task {id} for {}",
                        delta.formats.join(","),
                        delta.role
                    );
                }
                DeltaOp::Add(row) => {
                    assert!(
                        !base.iter().any(|t| t.task == row.task),
                        "delta adds a task already in {}'s base: {}",
                        delta.role,
                        row.task
                    );
                }
            }
            for f in delta.formats {
                assert!(FORMAT_TYPE_VOCAB.contains(f), "delta format {f} not in vocab");
            }
        }
        // All 35 resolved (role, format) maps: statuses/owners/gates in vocab;
        // any plugin-component task's plugin appears in the format's list.
        for role in PRODUCTION_ROLE_VOCAB {
            for format in FORMAT_TYPE_VOCAB {
                let (tasks, _) = resolved_agentification(role, format);
                let plugin_ids: Vec<String> = templates
                    .iter()
                    .find(|t| t.format_type.as_deref() == Some(format))
                    .map(|t| t.plugins.iter().map(|p| p.id.clone()).collect())
                    .unwrap_or_default();
                for t in &tasks {
                    assert!(TASK_STATUS_VOCAB.contains(&t.status.as_str()), "{role}/{format}: status {}", t.status);
                    assert!(TASK_OWNER_VOCAB.contains(&t.owner.as_str()), "{role}/{format}: owner {}", t.owner);
                    if let Some(g) = t.gate.as_deref() {
                        assert!(GATE_VOCAB.contains(&g), "{role}/{format}: gate {g}");
                    }
                    if let Some(c) = t.component.as_deref()
                        && !RAIL_COMPONENTS.contains(&c)
                    {
                        assert!(
                            plugin_ids.iter().any(|p| p == c),
                            "{role}/{format}: task {} claims plugin component {c} absent from the format's plugin list {plugin_ids:?}",
                            t.task
                        );
                    }
                }
            }
        }
        // Duty + bridge rows stay inside their vocabs.
        for d in ae_duty_catalog() {
            assert!(TASK_STATUS_VOCAB.contains(&d.status.as_str()));
            if let Some(g) = d.gate.as_deref() {
                assert!(GATE_VOCAB.contains(&g));
            }
        }
        for h in orchestration_bridge() {
            assert!(TASK_STATUS_VOCAB.contains(&h.status.as_str()));
            assert!(DEPT_VOCAB.contains(&h.from_dept.as_str()));
            assert!(DEPT_VOCAB.contains(&h.to_dept.as_str()));
            if let Some(g) = h.gate.as_deref() {
                assert!(GATE_VOCAB.contains(&g));
            }
            for k in &h.note_kinds {
                assert!(crate::models::dto::NOTE_KIND_VOCAB.contains(&k.as_str()), "bridge kind {k}");
            }
        }
    }

    #[test]
    fn base_counts_match_the_printed_map() {
        assert_eq!(base_agentification("producer").len(), 6);
        assert_eq!(base_agentification("assistant_editor").len(), 13);
        assert_eq!(base_agentification("editor").len(), 3);
        assert_eq!(base_agentification("director").len(), 3);
        assert_eq!(base_agentification("colorist").len(), 4);
        assert_eq!(base_agentification("sound").len(), 5);
        assert!(base_agentification("studio_exec").is_empty());
    }

    #[test]
    fn task_status_vocab_pinned_shared_with_b3() {
        assert_eq!(TASK_STATUS_VOCAB, ["live", "build_now", "coming"]);
        assert_eq!(GATE_VOCAB, ["confirm", "external_send", "human_ok"]);
    }
}
