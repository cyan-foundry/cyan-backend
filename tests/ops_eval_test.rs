//! feat/notes-constitution — the EVAL-ORACLE harness.
//!
//! `ops_eval::evaluate` grades a CANDIDATE `OpsProposer` against a REFERENCE on a
//! seed note corpus and reports agreement. Both sides are plain `OpsProposer`
//! trait objects; the intended reference is an Llm-backed oracle behind the `Llm`
//! seam (Claude-as-oracle today; 70B-as-oracle is a `LlmProvider` config flip) —
//! that impl is the JOIN. CI exercises the harness with scripted/fake proposers:
//! NO live model, NO keys, NO network.
//!
//! The `ScriptedLlmProposer` below is an EVAL FIXTURE proving the `Llm` seam fits
//! the proposer shape (prompt in → strict closed-vocab JSON out → never-guess on
//! anything else). It is NOT the production LLM proposer.

use std::collections::BTreeMap;
use std::sync::Arc;

use cyan_backend::llm::{Llm, LlmProvider, ScriptedLlm};
use cyan_backend::ops_eval;
use cyan_backend::ops_proposer::{OpsProposer, ProposeCtx, ProposedOp, ReviewNote};
use serde_json::json;

/// The frozen contract's closed op vocabulary (PROPOSE_OPS_CONTRACT.md).
const OP_VOCAB: [&str; 9] =
    ["trim", "delete", "level", "mute", "fade", "speed", "reframe", "color", "loudnorm"];

// ════════════════════════════════════════════════════════════════════════════
// Fixtures
// ════════════════════════════════════════════════════════════════════════════

/// A deterministic proposer: note-text substring → scripted ops. Stands in for
/// "the regex impl" / "a candidate" without touching Session A's spine.
struct StaticProposer {
    rules: BTreeMap<&'static str, Vec<(String, serde_json::Value)>>,
}

impl StaticProposer {
    fn new() -> Self {
        Self { rules: BTreeMap::new() }
    }
    fn on(mut self, needle: &'static str, op: &str, params: serde_json::Value) -> Self {
        self.rules
            .entry(needle)
            .or_default()
            .push((op.to_string(), params));
        self
    }
}

impl OpsProposer for StaticProposer {
    fn propose_ops(&self, note: &ReviewNote, _ctx: &ProposeCtx) -> Vec<ProposedOp> {
        for (needle, ops) in &self.rules {
            if note.text.contains(needle) {
                return ops
                    .iter()
                    .map(|(op, params)| ProposedOp {
                        op: op.clone(),
                        params: params.clone(),
                        tc_in: Some(0),
                        tc_out: note.tc_out,
                        confidence: Some(1.0),
                        rationale: Some(format!("matched '{needle}'")),
                    })
                    .collect();
            }
        }
        Vec::new() // never-guess: no rule ⇒ no op
    }
}

/// EVAL FIXTURE: a proposer that consults the `Llm` seam and parses a STRICT
/// closed-vocab JSON op array. Any parse failure, any op outside the vocab, any
/// LLM error ⇒ EMPTY (never-guess holds for every impl, including fixtures).
struct ScriptedLlmProposer {
    llm: Arc<dyn Llm>,
}

impl OpsProposer for ScriptedLlmProposer {
    fn propose_ops(&self, note: &ReviewNote, ctx: &ProposeCtx) -> Vec<ProposedOp> {
        let system = "You propose closed-vocab mechanical edit ops as a JSON array. \
                      Emit [] unless the note fully specifies an op.";
        let prompt = format!(
            "NOTE ({} @ {:?}): {}\nCONSTITUTION:\n{}\nPREFERENCES:\n{}\nTOOL SCHEMAS:\n{}",
            note.source, note.tc_out, note.text, ctx.constitution, ctx.preferences, ctx.tool_schemas
        );
        let Ok(raw) = self.llm.complete(system, &prompt) else {
            return Vec::new();
        };
        let Ok(vals) = serde_json::from_str::<Vec<serde_json::Value>>(&raw) else {
            return Vec::new();
        };
        let mut ops = Vec::with_capacity(vals.len());
        for v in vals {
            let Some(op) = v.get("op").and_then(|o| o.as_str()) else {
                return Vec::new();
            };
            if !OP_VOCAB.contains(&op) {
                return Vec::new(); // outside the closed vocab ⇒ the whole answer is untrusted
            }
            ops.push(ProposedOp {
                op: op.to_string(),
                params: v.get("params").cloned().unwrap_or(json!({})),
                tc_in: v.get("tc_in").and_then(|t| t.as_i64()),
                tc_out: v.get("tc_out").and_then(|t| t.as_i64()),
                confidence: v.get("confidence").and_then(|c| c.as_f64()).map(|c| c as f32),
                rationale: v.get("rationale").and_then(|r| r.as_str()).map(String::from),
            });
        }
        ops
    }
}

fn note(text: &str) -> ReviewNote {
    ReviewNote { text: text.to_string(), tc_out: Some(288), source: "frameio".to_string() }
}

/// The two mechanical rules the seed corpus's mechanical notes resolve to.
fn mechanical_proposer() -> StaticProposer {
    StaticProposer::new()
        .on("trim 12 frames off the tail", "trim", json!({"edge": "tail", "frames": 12}))
        .on("mute the second channel", "mute", json!({"track": "A2"}))
}

// ════════════════════════════════════════════════════════════════════════════
// 1. Identical proposers agree fully — including agreeing on EMPTY (never-guess
//    parity is a first-class metric, not a gap in coverage).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn identical_proposers_agree_fully_on_the_seed_corpus() {
    let corpus = ops_eval::seed_corpus();
    assert!(corpus.len() >= 8, "seed corpus is a real corpus, not a stub");

    let candidate = mechanical_proposer();
    let reference = mechanical_proposer();
    let report = ops_eval::evaluate(&candidate, &reference, &corpus);

    assert_eq!(report.total, corpus.len());
    assert_eq!(report.op_agree, report.total, "identical proposers agree everywhere");
    assert_eq!(report.exact_agree, report.total, "params agree too");
    assert!((report.agreement() - 1.0).abs() < f64::EPSILON);
    assert!(
        report.both_empty >= 2,
        "the corpus carries vague/creative notes on which never-guess means BOTH stay empty (got {})",
        report.both_empty
    );
    assert!(
        report.both_empty < report.total,
        "the corpus also carries mechanical notes that DO propose"
    );
    assert_eq!(report.candidate_only, 0);
    assert_eq!(report.reference_only, 0);
    assert_eq!(report.disagree, 0);
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Divergence is counted on the right side of the ledger.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn divergent_candidate_lowers_agreement_and_lands_in_the_right_bucket() {
    let corpus = ops_eval::seed_corpus();
    let reference = mechanical_proposer();

    // A guessing candidate: also "infers" an op from a vague creative note the
    // reference correctly leaves empty.
    let candidate = mechanical_proposer().on("feels rushed", "trim", json!({"frames": 24}));

    let report = ops_eval::evaluate(&candidate, &reference, &corpus);
    assert!(report.agreement() < 1.0, "a guess must cost agreement");
    assert!(
        report.candidate_only >= 1,
        "candidate-proposed-where-reference-was-empty lands in candidate_only"
    );
    assert_eq!(report.reference_only, 0);

    // Same op, different params: op_agree but NOT exact_agree.
    let sloppy = StaticProposer::new()
        .on("trim 12 frames off the tail", "trim", json!({"edge": "tail", "frames": 13}))
        .on("mute the second channel", "mute", json!({"track": "A2"}));
    let report = ops_eval::evaluate(&sloppy, &reference, &corpus);
    assert!(report.op_agree > report.exact_agree, "param drift splits op vs exact agreement");
}

// ════════════════════════════════════════════════════════════════════════════
// 3. The Llm-seam fixture: strict parse, closed vocab, never-guess.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn scripted_llm_proposer_never_guesses() {
    let corpus_note = note("trim 12 frames off the tail");
    let asset = cyan_backend::ops_proposer::AssetMeta { duration_frames: Some(1440), fps: 24.0 };
    let ctx = ProposeCtx { constitution: "", preferences: "", asset: &asset, tool_schemas: "" };

    // (a) Prose instead of JSON ⇒ empty.
    let llm = Arc::new(ScriptedLlm::new().default_response("I think you should trim the tail."));
    let p = ScriptedLlmProposer { llm };
    assert!(p.propose_ops(&corpus_note, &ctx).is_empty(), "prose is not an op");

    // (b) Valid JSON but an op OUTSIDE the closed vocab ⇒ empty.
    let llm = Arc::new(ScriptedLlm::new().default_response(r#"[{"op":"explode","params":{}}]"#));
    let p = ScriptedLlmProposer { llm };
    assert!(p.propose_ops(&corpus_note, &ctx).is_empty(), "closed vocab is closed");

    // (c) A fully-specified closed-vocab answer ⇒ exactly that op.
    let llm = Arc::new(ScriptedLlm::new().on(
        "trim 12 frames",
        r#"[{"op":"trim","params":{"edge":"tail","frames":12},"tc_in":0,"tc_out":288,"confidence":0.95,"rationale":"note fully specifies the trim"}]"#,
    ));
    let p = ScriptedLlmProposer { llm };
    let ops = p.propose_ops(&corpus_note, &ctx);
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].op, "trim");
    assert_eq!(ops[0].params, json!({"edge":"tail","frames":12}));
    assert_eq!(ops[0].confidence, Some(0.95));

    // (d) The Llm erroring (no rule, no default) ⇒ empty, never a panic.
    let llm = Arc::new(ScriptedLlm::new());
    let p = ScriptedLlmProposer { llm };
    assert!(p.propose_ops(&note("mute the second channel"), &ctx).is_empty());
}

// ════════════════════════════════════════════════════════════════════════════
// 4. The constitution flows through ProposeCtx into the Llm prompt — the whole
//    point of the notes foundation.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn constitution_reaches_the_llm_prompt() {
    let llm = Arc::new(ScriptedLlm::new().default_response("[]"));
    let p = ScriptedLlmProposer { llm: llm.clone() };

    let asset = cyan_backend::ops_proposer::AssetMeta { duration_frames: Some(1440), fps: 24.0 };
    let ctx = ProposeCtx {
        constitution: "## Board\nNever trim the sponsor tag",
        preferences: "producer prefers J-cuts",
        asset: &asset,
        tool_schemas: "{\"trim\":{}}",
    };
    let _ = p.propose_ops(&note("tighten the open"), &ctx);

    let calls = llm.calls();
    assert_eq!(calls.len(), 1);
    let (_, prompt) = &calls[0];
    assert!(prompt.contains("Never trim the sponsor tag"), "constitution in the prompt");
    assert!(prompt.contains("producer prefers J-cuts"), "preferences in the prompt");
    assert!(prompt.contains("tighten the open"), "the note itself in the prompt");
}

// ════════════════════════════════════════════════════════════════════════════
// 5. Harness edges + the provider config seam.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn empty_corpus_is_vacuously_agreed() {
    let a = StaticProposer::new();
    let b = StaticProposer::new();
    let report = ops_eval::evaluate(&a, &b, &[]);
    assert_eq!(report.total, 0);
    assert!((report.agreement() - 1.0).abs() < f64::EPSILON, "vacuous agreement is 1.0");
}

#[test]
fn llm_provider_is_config_never_hardcoded() {
    assert_eq!(LlmProvider::parse("claude"), Some(LlmProvider::Claude));
    assert_eq!(LlmProvider::parse("vllm"), Some(LlmProvider::Vllm));
    assert_eq!(LlmProvider::parse("edge"), Some(LlmProvider::Edge));
    assert_eq!(LlmProvider::parse("gpt-99"), None, "unknown providers are rejected, not defaulted");
    for p in [LlmProvider::Claude, LlmProvider::Vllm, LlmProvider::Edge] {
        assert_eq!(LlmProvider::parse(p.as_str()), Some(p), "as_str/parse round-trips");
    }
}

#[test]
fn eval_report_displays_the_metrics() {
    let corpus = ops_eval::seed_corpus();
    let p = mechanical_proposer();
    let r = ops_eval::evaluate(&p, &p, &corpus);
    let line = r.to_string();
    assert!(line.contains("agreement"), "report renders a human-readable summary: {line}");
    assert!(line.contains(&format!("{}", r.total)));
}

// ════════════════════════════════════════════════════════════════════════════
// 6. Seed corpus sanity: cases expose ctx the proposers can rely on.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn seed_corpus_cases_build_propose_ctx() {
    for case in ops_eval::seed_corpus() {
        let ctx: ProposeCtx = case.ctx();
        assert!(case.note.tc_out.is_some() || case.note.tc_out.is_none()); // shape holds
        assert!(ctx.asset.fps > 0.0, "every case carries a real fps");
        assert!(!case.note.text.is_empty());
    }
    // At least one case exercises a constitution-bearing ctx.
    assert!(
        ops_eval::seed_corpus().iter().any(|c| !c.constitution.is_empty()),
        "the corpus includes constitution-conditioned cases"
    );
}
