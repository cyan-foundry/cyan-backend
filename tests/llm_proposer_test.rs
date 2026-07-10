//! THE JOIN (SESSION_JOIN §3) — the PRODUCTION Llm-backed `OpsProposer`.
//!
//! `llm_proposer::LlmOpsProposer` sits behind the frozen seam and calls the
//! `Llm` trait (`provider: claude | vllm | edge` — config, NEVER hardcoded).
//! Everything here runs on the `ScriptedLlm` fake: default `cargo test` is
//! green with NO model, NO keys, NO network. The live-70B proof is the
//! `#[ignore]` test at the bottom (GPU-gated — run it only when the box's vLLM
//! serves the real model).
//!
//! The load-bearing properties:
//!   * the production prompt carries note + constitution + preferences +
//!     tool_schemas + asset meta (the ctx the notes foundation exists to feed);
//!   * the parser is STRICT: out-of-vocab, under-specified, malformed, or
//!     errored answers ⇒ EMPTY — never-guess holds for the LLM impl too;
//!   * the CONSTITUTION CONDITIONS the proposal (proved through ops_eval on the
//!     seed corpus): a house rule turns a vague note into a mechanical op that
//!     the same note WITHOUT the rule never yields;
//!   * config selects regex vs LLM (regex stays the zero-LLM default).

use std::sync::Arc;

use cyan_backend::llm::{LlmProvider, ScriptedLlm};
use cyan_backend::llm_proposer::{proposer_choice, LlmOpsProposer, ProposerChoice};
use cyan_backend::note_inference::RegexOpsProposer;
use cyan_backend::ops_eval;
use cyan_backend::ops_proposer::{AssetMeta, OpsProposer, ProposeCtx, ReviewNote};
use cyan_backend::review_loop::PROPOSER_TOOL_SCHEMAS;
use serde_json::json;

fn note(text: &str, anchor: Option<i64>) -> ReviewNote {
    ReviewNote { text: text.to_string(), tc_out: anchor, source: "frameio".to_string() }
}

fn asset() -> AssetMeta {
    AssetMeta { duration_frames: Some(1440), fps: 24.0 }
}

fn ctx<'a>(asset: &'a AssetMeta, constitution: &'a str, preferences: &'a str) -> ProposeCtx<'a> {
    ProposeCtx { constitution, preferences, asset, tool_schemas: PROPOSER_TOOL_SCHEMAS }
}

fn proposer(llm: ScriptedLlm) -> LlmOpsProposer {
    LlmOpsProposer::new(Arc::new(llm), LlmProvider::Claude)
}

// ════════════════════════════════════════════════════════════════════════════
// 1. The production prompt: everything the ctx carries reaches the model.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn prompt_carries_note_constitution_preferences_schemas_and_asset_meta() {
    let scripted = Arc::new(ScriptedLlm::new().default_response("[]"));
    let a = asset();
    let c = ctx(&a, "## Board\nVO sits -2 dB under music.", "producer prefers cuts on action");
    let p = LlmOpsProposer::new(scripted.clone(), LlmProvider::Claude);
    let _ = p.propose_ops(&note("the VO's hot at the top", Some(60)), &c);

    let calls = scripted.calls();
    assert_eq!(calls.len(), 1, "exactly one completion turn per note");
    let (system, prompt) = &calls[0];

    assert!(prompt.contains("the VO's hot at the top"), "note text in prompt");
    assert!(prompt.contains("60"), "the note's master-frame anchor in prompt");
    assert!(prompt.contains("VO sits -2 dB under music."), "constitution in prompt");
    assert!(prompt.contains("producer prefers cuts on action"), "preferences in prompt");
    assert!(prompt.contains("\"trim\""), "closed-vocab tool schemas in prompt");
    assert!(prompt.contains("1440"), "asset duration_frames in prompt");
    assert!(prompt.contains("24"), "asset fps in prompt");
    // Never-guess is IN THE CONTRACT the model is handed, not just our parser.
    let all = format!("{system}\n{prompt}");
    assert!(
        all.contains("[]") && (all.to_lowercase().contains("never") || all.to_lowercase().contains("only")),
        "the prompt states the never-guess/empty contract"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Parse: a fully-specified closed-vocab answer lands as ProposedOps.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn valid_closed_vocab_answer_parses_into_ops() {
    let llm = ScriptedLlm::new().default_response(
        r#"[{"op":"trim","params":{"edge":"tail","frames":12},"tc_in":0,"tc_out":1440,
             "confidence":0.86,"rationale":"the note names the edge and the count"}]"#,
    );
    let a = asset();
    let ops = proposer(llm).propose_ops(&note("trim 12 frames off the tail", Some(288)), &ctx(&a, "", ""));
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].op, "trim");
    assert_eq!(ops[0].params["edge"], json!("tail"));
    assert_eq!(ops[0].params["frames"], json!(12));
    assert_eq!((ops[0].tc_in, ops[0].tc_out), (Some(0), Some(1440)));
    assert!((ops[0].confidence.expect("confidence") - 0.86).abs() < 1e-6);
    assert!(ops[0].rationale.as_deref().unwrap_or("").contains("names the edge"));
}

#[test]
fn fenced_json_answer_is_tolerated() {
    // Models fence JSON constantly; a fence is formatting, not a guess.
    let llm = ScriptedLlm::new().default_response(
        "```json\n[{\"op\":\"mute\",\"params\":{},\"tc_in\":100,\"tc_out\":200,\"confidence\":0.9}]\n```",
    );
    let a = asset();
    let ops = proposer(llm).propose_ops(&note("mute that section", Some(100)), &ctx(&a, "", ""));
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].op, "mute");
}

// ════════════════════════════════════════════════════════════════════════════
// 3. STRICT: anything not a fully-specified closed-vocab answer ⇒ EMPTY.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn strict_parser_rejects_bad_answers_wholesale() {
    let a = asset();
    let n = note("do something", Some(10));
    let cases: Vec<(&str, &str)> = vec![
        ("prose", "You should trim the tail by 12 frames."),
        ("out-of-vocab op", r#"[{"op":"explode","params":{}}]"#),
        // 'loudnorm' is contract-vocab prose but NOT a ledger op — the schema
        // says so; an answer using it is untrusted.
        ("non-ledger op", r#"[{"op":"loudnorm","params":{"target_lufs":-14}}]"#),
        ("not an array", r#"{"op":"trim","params":{"edge":"tail","frames":12}}"#),
        ("op missing", r#"[{"params":{"edge":"tail","frames":12}}]"#),
        ("trim without frames", r#"[{"op":"trim","params":{"edge":"tail"},"tc_out":1440}]"#),
        ("trim bad edge", r#"[{"op":"trim","params":{"edge":"middle","frames":5}}]"#),
        ("tail trim without tc_out", r#"[{"op":"trim","params":{"edge":"tail","frames":12}}]"#),
        ("zero-frame trim", r#"[{"op":"trim","params":{"edge":"tail","frames":0},"tc_out":1440}]"#),
        ("level without gain or lufs", r#"[{"op":"level","params":{},"tc_in":0,"tc_out":100}]"#),
        ("gain level without a range", r#"[{"op":"level","params":{"gain_db":-2}}]"#),
        ("mute without a range end", r#"[{"op":"mute","params":{},"tc_in":50}]"#),
        ("fade bad dir", r#"[{"op":"fade","params":{"dir":"sideways","frames":8},"tc_in":0,"tc_out":8}]"#),
        ("speed zero ratio", r#"[{"op":"speed","params":{"ratio":0},"tc_in":0,"tc_out":100}]"#),
        ("confidence out of range", r#"[{"op":"mute","params":{},"tc_in":0,"tc_out":10,"confidence":1.7}]"#),
        (
            "one bad op poisons the batch",
            r#"[{"op":"mute","params":{},"tc_in":0,"tc_out":10},{"op":"explode","params":{}}]"#,
        ),
    ];
    for (label, answer) in cases {
        let llm = ScriptedLlm::new().default_response(answer);
        let ops = proposer(llm).propose_ops(&n, &ctx(&a, "", ""));
        assert!(ops.is_empty(), "{label}: must be rejected wholesale, got {} op(s)", ops.len());
    }
}

#[test]
fn llm_error_means_empty_never_a_panic() {
    // A ScriptedLlm with no rules and no default ERRORS on every call.
    let llm = ScriptedLlm::new();
    let a = asset();
    let ops = proposer(llm).propose_ops(&note("trim 12 frames off the tail", Some(288)), &ctx(&a, "", ""));
    assert!(ops.is_empty(), "an LLM error is an empty proposal, never a crash");
}

// ════════════════════════════════════════════════════════════════════════════
// 4. THE DELIGHTER PROOF (ops_eval + seed corpus): the constitution CHANGES the
//    proposal. The fake answers ONLY when the house rule's text is in the
//    prompt — so an op on these cases proves the real prompt assembly carried
//    the constitution, and the same note without the rule stays empty.
// ════════════════════════════════════════════════════════════════════════════

/// The fake, scripted for the seed corpus's mechanical + constitution cases.
/// Needles match on PROMPT content (note text / constitution text).
fn corpus_scripted_llm() -> ScriptedLlm {
    ScriptedLlm::new()
        // fully-specified mechanical notes — no constitution needed
        .on(
            "trim 12 frames off the tail",
            r#"[{"op":"trim","params":{"edge":"tail","frames":12},"tc_in":0,"tc_out":1440,"confidence":0.95,"rationale":"explicit edge and count"}]"#,
        )
        .on(
            "mute the second channel",
            r#"[{"op":"mute","params":{"track":"A2"},"tc_in":0,"tc_out":1440,"confidence":0.9,"rationale":"explicit channel"}]"#,
        )
        // constitution-conditioned: the needle is the HOUSE RULE, not the note —
        // the answer can only fire if the constitution reached the prompt.
        .on(
            "Deliver -14 LUFS integrated",
            r#"[{"op":"level","params":{"target_lufs":-14},"confidence":0.85,"rationale":"the house loudness spec pins the target"}]"#,
        )
        .on(
            "trim cold opens 3-5s",
            r#"[{"op":"trim","params":{"edge":"head","frames":96},"tc_in":0,"confidence":0.8,"rationale":"house rule: cold opens tighten 3-5s; 4s @ 24fps"}]"#,
        )
        // everything else (creative / under-specified / out-of-scope): EMPTY.
        .default_response("[]")
}

#[test]
fn constitution_conditions_the_proposal_on_the_seed_corpus() {
    let corpus = ops_eval::seed_corpus();
    let candidate = LlmOpsProposer::new(Arc::new(corpus_scripted_llm()), LlmProvider::Claude);
    let reference = RegexOpsProposer;

    let report = ops_eval::evaluate(&candidate, &reference, &corpus);
    assert_eq!(report.total, corpus.len());
    // Vague/creative/under-specified/out-of-scope: BOTH stay empty (never-guess
    // parity) — 6 of the 10 seed cases.
    assert_eq!(report.both_empty, 6, "never-guess parity holds on the vague cases: {report}");
    // The explicit tail trim: both propose the same op.
    assert!(report.op_agree > report.both_empty, "the explicit trim agrees beyond empties: {report}");
    // The LLM's EDGE over regex: the mute + the TWO constitution-conditioned
    // cases (house loudness spec, cold-open tighten) — ops regex cannot infer.
    assert_eq!(report.candidate_only, 3, "mute + 2 constitution-conditioned wins: {report}");
    assert_eq!(report.reference_only, 0, "the LLM misses nothing regex catches: {report}");
    assert_eq!(report.disagree, 0, "{report}");
}

#[test]
fn the_same_note_without_the_house_rule_stays_empty() {
    // "-2 dB on VO" style proof, directly: the vague note proposes ONLY when
    // the house rule is in ctx. Needle = the rule text ⇒ prompt-content proof.
    let llm = ScriptedLlm::new()
        .on(
            "VO runs -2 dB whenever a reviewer flags it hot",
            r#"[{"op":"level","params":{"gain_db":-2},"tc_in":60,"tc_out":1440,"confidence":0.82,"rationale":"house rule: hot VO drops 2 dB"}]"#,
        )
        .default_response("[]");
    let p = LlmOpsProposer::new(Arc::new(llm), LlmProvider::Claude);
    let a = asset();
    let vague = note("the VO's hot here", Some(60));

    let with_rule = p.propose_ops(
        &vague,
        &ctx(&a, "## Board\nVO runs -2 dB whenever a reviewer flags it hot.", ""),
    );
    assert_eq!(with_rule.len(), 1, "the house rule turns the vague note into a level op");
    assert_eq!(with_rule[0].op, "level");
    assert_eq!(with_rule[0].params["gain_db"], json!(-2));

    let without_rule = p.propose_ops(&vague, &ctx(&a, "", ""));
    assert!(
        without_rule.is_empty(),
        "the SAME note with no constitution stays with the human — never guessed"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// 5. Config selects the proposer — regex is the zero-LLM default; the provider
//    is parsed, never guessed.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn config_selects_regex_by_default_and_llm_by_choice() {
    assert_eq!(proposer_choice(None, None), ProposerChoice::Regex, "zero-LLM default");
    assert_eq!(proposer_choice(Some("regex"), None), ProposerChoice::Regex);
    assert_eq!(proposer_choice(Some("regex"), Some("claude")), ProposerChoice::Regex);
    assert_eq!(
        proposer_choice(Some("llm"), Some("claude")),
        ProposerChoice::Llm(LlmProvider::Claude)
    );
    assert_eq!(
        proposer_choice(Some("llm"), Some("vllm")),
        ProposerChoice::Llm(LlmProvider::Vllm)
    );
    assert_eq!(
        proposer_choice(Some("llm"), Some("edge")),
        ProposerChoice::Llm(LlmProvider::Edge)
    );
    // Unknown/missing provider is REJECTED, not defaulted: the safe fall-back
    // is the deterministic regex impl — never a silently-guessed model.
    assert_eq!(proposer_choice(Some("llm"), None), ProposerChoice::Regex);
    assert_eq!(proposer_choice(Some("llm"), Some("gpt-x")), ProposerChoice::Regex);
    assert_eq!(proposer_choice(Some("banana"), Some("claude")), ProposerChoice::Regex);
}

// ════════════════════════════════════════════════════════════════════════════
// 6. GPU-GATED (the live 70B proof). `#[ignore]` by default: run explicitly
//    with the box's vLLM up and CYAN_VLLM_URL/CYAN_VLLM_MODEL exported:
//      cargo test --test llm_proposer_test live_70b -- --ignored --nocapture
// ════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "GPU-gated: needs the box's vLLM serving the real model (CYAN_VLLM_URL + CYAN_VLLM_MODEL)"]
fn live_70b_proposes_a_trim_from_a_free_form_note() {
    let llm = cyan_backend::llm_proposer::HttpLlm::from_env(LlmProvider::Vllm)
        .expect("CYAN_VLLM_URL + CYAN_VLLM_MODEL must be exported for the live leg");
    let p = LlmOpsProposer::new(Arc::new(llm), LlmProvider::Vllm);
    let a = asset();
    let ops = p.propose_ops(
        &note("this shot overstays its welcome — lose the last 12 frames", Some(1200)),
        &ctx(&a, "", ""),
    );
    assert_eq!(ops.len(), 1, "the 70B infers exactly one op from the free-form note");
    assert_eq!(ops[0].op, "trim");
    assert_eq!(ops[0].params["edge"], json!("tail"));
    assert_eq!(ops[0].params["frames"], json!(12));
    eprintln!("live 70B proposal: {:?} rationale={:?}", ops[0].params, ops[0].rationale);
}
