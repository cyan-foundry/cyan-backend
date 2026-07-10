//! feat/notes-constitution — the EVAL-ORACLE harness (TONIGHT_RUN Part 4).
//!
//! Grade a CANDIDATE [`OpsProposer`] against a REFERENCE on a note corpus and
//! report agreement. Both sides are plain trait objects: today's reference is a
//! scripted/deterministic proposer in CI; the intended reference is an Llm-backed
//! oracle behind [`crate::llm::Llm`] (Claude-as-oracle now, 70B-as-oracle is a
//! `LlmProvider` config flip) — that impl lands with the JOIN. The harness itself
//! never touches a model, a key, or the network.
//!
//! Agreement semantics: **never-guess parity counts.** Two proposers that both
//! stay EMPTY on a vague creative note AGREE — that is the contract's most
//! important behavior, not a coverage gap.

use crate::ops_proposer::{AssetMeta, OpsProposer, ProposeCtx, ProposedOp, ReviewNote};

/// One corpus case: the note plus everything a `ProposeCtx` borrows. Owns its
/// data; [`EvalCase::ctx`] borrows it into the frozen seam shape.
pub struct EvalCase {
    pub note: ReviewNote,
    pub constitution: String,
    pub preferences: String,
    pub asset: AssetMeta,
    pub tool_schemas: String,
}

impl EvalCase {
    pub fn ctx(&self) -> ProposeCtx<'_> {
        ProposeCtx {
            constitution: &self.constitution,
            preferences: &self.preferences,
            asset: &self.asset,
            tool_schemas: &self.tool_schemas,
        }
    }
}

/// Agreement metrics over one evaluation run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvalReport {
    pub total: usize,
    /// Cases where candidate and reference emit the SAME op sequence (names, in
    /// order) — including both staying empty.
    pub op_agree: usize,
    /// Subset of `op_agree` where params and tc anchors match too.
    pub exact_agree: usize,
    /// Cases where BOTH stayed empty (never-guess parity).
    pub both_empty: usize,
    /// Candidate proposed, reference stayed empty (the candidate guessed).
    pub candidate_only: usize,
    /// Reference proposed, candidate stayed empty (the candidate missed).
    pub reference_only: usize,
    /// Both proposed, but different ops.
    pub disagree: usize,
}

impl EvalReport {
    /// Fraction of cases where the op sequences agree. An empty corpus is
    /// vacuously agreed (1.0).
    pub fn agreement(&self) -> f64 {
        if self.total == 0 {
            return 1.0;
        }
        self.op_agree as f64 / self.total as f64
    }
}

impl std::fmt::Display for EvalReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ops_eval: agreement {:.3} ({}/{} op-agree, {} exact, {} both-empty, \
             {} candidate-only, {} reference-only, {} disagree)",
            self.agreement(),
            self.op_agree,
            self.total,
            self.exact_agree,
            self.both_empty,
            self.candidate_only,
            self.reference_only,
            self.disagree
        )
    }
}

/// Run the corpus through both proposers and tally agreement.
pub fn evaluate(
    candidate: &dyn OpsProposer,
    reference: &dyn OpsProposer,
    corpus: &[EvalCase],
) -> EvalReport {
    let mut report = EvalReport { total: corpus.len(), ..Default::default() };
    for case in corpus {
        let ctx = case.ctx();
        let cand = candidate.propose_ops(&case.note, &ctx);
        let refr = reference.propose_ops(&case.note, &ctx);
        match (cand.is_empty(), refr.is_empty()) {
            (true, true) => {
                report.both_empty += 1;
                report.op_agree += 1;
                report.exact_agree += 1;
            }
            (false, true) => report.candidate_only += 1,
            (true, false) => report.reference_only += 1,
            (false, false) => {
                if same_ops(&cand, &refr) {
                    report.op_agree += 1;
                    if same_exact(&cand, &refr) {
                        report.exact_agree += 1;
                    }
                } else {
                    report.disagree += 1;
                }
            }
        }
    }
    report
}

/// Same op-name sequence, in order.
fn same_ops(a: &[ProposedOp], b: &[ProposedOp]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.op == y.op)
}

/// Same ops AND same params/tc anchors. Confidence and rationale are the
/// proposer's own voice — never part of agreement.
fn same_exact(a: &[ProposedOp], b: &[ProposedOp]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.op == y.op && x.params == y.params && x.tc_in == y.tc_in && x.tc_out == y.tc_out
        })
}

/// The seed corpus: small, deterministic, and shaped like the real review lane —
/// fully-specified mechanical notes (must propose), vague/creative notes (must
/// stay EMPTY), and constitution-conditioned cases (the ctx the notes foundation
/// exists to carry).
pub fn seed_corpus() -> Vec<EvalCase> {
    let asset = || AssetMeta { duration_frames: Some(1440), fps: 24.0 };
    let note = |text: &str, tc_out: Option<i64>| ReviewNote {
        text: text.to_string(),
        tc_out,
        source: "frameio".to_string(),
    };
    let case = |n: ReviewNote, constitution: &str, preferences: &str| EvalCase {
        note: n,
        constitution: constitution.to_string(),
        preferences: preferences.to_string(),
        asset: asset(),
        tool_schemas: r#"{"trim":{},"delete":{},"level":{},"mute":{},"fade":{},"speed":{},"reframe":{},"color":{},"loudnorm":{}}"#
            .to_string(),
    };

    vec![
        // ── fully-specified mechanical notes: a proposer SHOULD emit ──
        case(note("trim 12 frames off the tail", Some(288)), "", ""),
        case(note("mute the second channel", Some(480)), "", ""),
        // ── vague / creative notes: never-guess ⇒ EMPTY is the right answer ──
        case(note("the open feels rushed", Some(24)), "", ""),
        case(note("make it pop more", None), "", ""),
        case(note("love this cut!", Some(600)), "", ""),
        // ── ambiguous-without-context: only the constitution disambiguates ──
        case(
            note("apply the house loudness spec", Some(1200)),
            "## Tenant\nDeliver -14 LUFS integrated",
            "",
        ),
        case(
            note("standard cold-open tighten please", Some(72)),
            "## Board\ntrim cold opens 3-5s",
            "producer prefers cuts on action",
        ),
        // ── mechanical but under-specified: still EMPTY without the numbers ──
        case(note("trim the tail a bit", Some(288)), "", ""),
        case(note("bring the music down", Some(960)), "", "producer likes music low under VO"),
        // ── out-of-scope ask: no closed-vocab op exists for it ──
        case(note("regenerate the title card in French", Some(48)), "", ""),
    ]
}
