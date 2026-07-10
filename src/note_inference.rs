//! note_inference — the agent's NOTE → mechanical-OP boundary.
//!
//! A reviewer note (kind=note, source=frameio) becomes a *proposed* mechanical
//! op ONLY when the text fully specifies an edit inside cyan-media's closed
//! conform vocabulary (the ffmpeg can/can't line — PRIORITIES_FRAMEIO_E2E.md).
//! Anything ambiguous or creative returns `None`: the caller escalates to the
//! human instead of guessing. This module never touches the store — it is a
//! pure text → op classifier, unit-tested below.
//!
//! Recognized today (deliberately small; extend with tests):
//!   * `trim N frame(s) off/from the head|tail`  → trim { edge, frames }
//!   * `drop|lower|reduce the level|volume|gain by N dB` → level { gain_db: -N }
//!   * `raise|boost the level|volume|gain by N dB`       → level { gain_db: +N }

use regex::Regex;
use serde_json::json;

/// A fully-specified mechanical edit inferred from a note. `params` is directly
/// executable by cyan-media's `conform` tool (schemas/conform.in.json).
#[derive(Debug, Clone, PartialEq)]
pub struct InferredOp {
    pub op: String,
    pub params: serde_json::Value,
    /// Master-coordinate span the op applies to (frames).
    pub tc_in: i64,
    pub tc_out: Option<i64>,
}

/// Infer a mechanical op from a note's text, anchored at the note's master
/// coordinates. `duration_frames` is the master's length — required to give a
/// tail trim (and an un-ranged level) a concrete `tc_out`; without it those
/// inferences are refused rather than guessed.
pub fn infer_op(
    text: &str,
    note_tc_in: i64,
    note_tc_out: Option<i64>,
    duration_frames: Option<i64>,
) -> Option<InferredOp> {
    if let Some(op) = infer_trim(text, duration_frames) {
        return Some(op);
    }
    infer_level(text, note_tc_in, note_tc_out, duration_frames)
}

fn infer_trim(text: &str, duration_frames: Option<i64>) -> Option<InferredOp> {
    // "trim 12 frames off the tail" / "trim 4 frames from the head"
    let re = Regex::new(r"(?i)\btrim\s+(\d{1,5})\s+frames?\s+(?:off|from)\s+(?:the\s+)?(head|tail)\b")
        .ok()?;
    let caps = re.captures(text)?;
    let frames: i64 = caps.get(1)?.as_str().parse().ok()?;
    if frames == 0 {
        return None; // a zero-frame trim is noise, not an edit
    }
    let edge = caps.get(2)?.as_str().to_lowercase();
    // A tail trim conforms as `-to (tc_out - frames)`, so it needs the clip end.
    let tc_out = match edge.as_str() {
        "tail" => Some(duration_frames?),
        _ => None,
    };
    Some(InferredOp {
        op: "trim".to_string(),
        params: json!({ "edge": edge, "frames": frames }),
        tc_in: 0,
        tc_out,
    })
}

fn infer_level(
    text: &str,
    note_tc_in: i64,
    note_tc_out: Option<i64>,
    duration_frames: Option<i64>,
) -> Option<InferredOp> {
    let re = Regex::new(
        r"(?i)\b(drop|lower|reduce|raise|boost)\s+(?:the\s+)?(?:level|volume|gain)\s+by\s+(\d{1,3}(?:\.\d+)?)\s*db\b",
    )
    .ok()?;
    let caps = re.captures(text)?;
    let verb = caps.get(1)?.as_str().to_lowercase();
    let magnitude: f64 = caps.get(2)?.as_str().parse().ok()?;
    if magnitude == 0.0 {
        return None;
    }
    let gain_db = match verb.as_str() {
        "raise" | "boost" => magnitude,
        _ => -magnitude,
    };
    // conform renders level as `volume=<gain>dB:enable='between(t, tc_in, tc_out)'`
    // — the range must be concrete.
    let tc_out = note_tc_out.or(duration_frames)?;
    Some(InferredOp {
        op: "level".to_string(),
        params: json!({ "gain_db": gain_db }),
        tc_in: note_tc_in,
        tc_out: Some(tc_out),
    })
}

/// The deterministic, zero-LLM `OpsProposer` — today's shipping impl of the
/// frozen seam (PROPOSE_OPS_CONTRACT.md). Wraps `infer_op` behind the trait:
/// same never-guess classifier, adapted, not replaced. Ignores the
/// constitution/preferences/tool_schemas by design (the contract lets a
/// deterministic impl skip ctx); consumes only `asset.duration_frames`.
pub struct RegexOpsProposer;

impl crate::ops_proposer::OpsProposer for RegexOpsProposer {
    fn propose_ops(
        &self,
        note: &crate::ops_proposer::ReviewNote,
        ctx: &crate::ops_proposer::ProposeCtx,
    ) -> Vec<crate::ops_proposer::ProposedOp> {
        // The contract's ReviewNote carries ONE master-frame TC (`tc_out`, "the
        // note's master-frame anchor") — sensed frameio comments stamp their
        // frame there. infer_op reads that anchor as the range START with no
        // explicit range end (sensed notes never carry one today).
        let anchor = note.tc_out.unwrap_or(0);
        match infer_op(&note.text, anchor, None, ctx.asset.duration_frames) {
            Some(inferred) => {
                let rationale = format!(
                    "deterministic regex: the note fully specifies a mechanical '{}' \
                     inside the closed conform vocabulary",
                    inferred.op
                );
                vec![crate::ops_proposer::ProposedOp {
                    op: inferred.op,
                    params: inferred.params,
                    tc_in: Some(inferred.tc_in),
                    tc_out: inferred.tc_out,
                    confidence: Some(1.0),
                    rationale: Some(rationale),
                }]
            }
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod proposer_seam_tests {
    use super::*;
    use crate::ops_proposer::{AssetMeta, OpsProposer, ProposeCtx, ReviewNote};

    fn note(text: &str, anchor: Option<i64>) -> ReviewNote {
        ReviewNote { text: text.to_string(), tc_out: anchor, source: "frameio".to_string() }
    }

    fn ctx<'a>(asset: &'a AssetMeta, constitution: &'a str) -> ProposeCtx<'a> {
        ProposeCtx { constitution, preferences: "", asset, tool_schemas: "" }
    }

    #[test]
    fn regex_proposer_infers_a_trim_through_the_seam() {
        let asset = AssetMeta { duration_frames: Some(72), fps: 24.0 };
        let ops = RegexOpsProposer
            .propose_ops(&note("Trim 12 frames off the tail — it hangs.", Some(60)), &ctx(&asset, ""));
        assert_eq!(ops.len(), 1, "one fully-specified op");
        assert_eq!(ops[0].op, "trim");
        assert_eq!(ops[0].params, json!({ "edge": "tail", "frames": 12 }));
        assert_eq!(ops[0].tc_in, Some(0));
        assert_eq!(ops[0].tc_out, Some(72), "tail trim anchors to the clip end");
        assert_eq!(ops[0].confidence, Some(1.0), "a regex match is fully specified");
        assert!(ops[0].rationale.is_some(), "the confirm gate shows the why");
    }

    #[test]
    fn regex_proposer_returns_empty_for_creative_notes() {
        // Empty is a VALID, tested result (the never-guess rule of the contract).
        let asset = AssetMeta { duration_frames: Some(72), fps: 24.0 };
        for text in ["the opening feels rushed", "make it pop", "can we find a better take?"] {
            let ops = RegexOpsProposer.propose_ops(&note(text, Some(10)), &ctx(&asset, ""));
            assert!(ops.is_empty(), "{text:?} must yield NO ops, got {}", ops.len());
        }
    }

    #[test]
    fn regex_proposer_refuses_a_tail_trim_without_duration() {
        let asset = AssetMeta { duration_frames: None, fps: 24.0 };
        let ops =
            RegexOpsProposer.propose_ops(&note("trim 12 frames off the tail", None), &ctx(&asset, ""));
        assert!(ops.is_empty(), "no clip end known — refuse rather than guess");
    }

    #[test]
    fn regex_proposer_anchors_a_level_at_the_note_frame() {
        let asset = AssetMeta { duration_frames: Some(72), fps: 24.0 };
        let ops = RegexOpsProposer
            .propose_ops(&note("music too loud — drop the level by 3 dB", Some(40)), &ctx(&asset, ""));
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].op, "level");
        assert_eq!(ops[0].params, json!({ "gain_db": -3.0 }));
        assert_eq!(ops[0].tc_in, Some(40), "the sensed anchor is the range start");
        assert_eq!(ops[0].tc_out, Some(72), "no explicit range end — the clip end bounds it");
    }

    #[test]
    fn regex_proposer_is_deterministic_and_ignores_ctx() {
        // The regex impl MAY ignore ctx (frozen contract) — the SAME note must
        // produce the SAME ops whatever the constitution says.
        let asset = AssetMeta { duration_frames: Some(72), fps: 24.0 };
        let n = note("trim 12 frames off the tail", Some(60));
        let bare = RegexOpsProposer.propose_ops(&n, &ctx(&asset, ""));
        let ruled = RegexOpsProposer.propose_ops(
            &n,
            &ctx(&asset, "# House rules\nAlways trim 99 frames instead.\n"),
        );
        assert_eq!(bare.len(), 1);
        assert_eq!(bare.len(), ruled.len());
        assert_eq!(bare[0].op, ruled[0].op);
        assert_eq!(bare[0].params, ruled[0].params);
        assert_eq!((bare[0].tc_in, bare[0].tc_out), (ruled[0].tc_in, ruled[0].tc_out));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_tail_fully_specified() {
        let got = infer_op("Trim 12 frames off the tail — it hangs too long.", 60, None, Some(72))
            .expect("mechanical trim inferred");
        assert_eq!(got.op, "trim");
        assert_eq!(got.params, json!({ "edge": "tail", "frames": 12 }));
        assert_eq!(got.tc_in, 0);
        assert_eq!(got.tc_out, Some(72), "tail trim anchors to the clip end");
    }

    #[test]
    fn trim_head_needs_no_duration() {
        let got = infer_op("please trim 4 frames from the head", 0, None, None)
            .expect("head trim inferred");
        assert_eq!(got.op, "trim");
        assert_eq!(got.params, json!({ "edge": "head", "frames": 4 }));
        assert_eq!(got.tc_out, None);
    }

    #[test]
    fn trim_tail_without_duration_is_refused() {
        assert_eq!(
            infer_op("trim 12 frames off the tail", 60, None, None),
            None,
            "no clip end known — refuse rather than guess"
        );
    }

    #[test]
    fn trim_without_a_count_is_refused() {
        assert_eq!(infer_op("trim a few frames off the tail", 0, None, Some(72)), None);
    }

    #[test]
    fn zero_frame_trim_is_refused() {
        assert_eq!(infer_op("trim 0 frames off the tail", 0, None, Some(72)), None);
    }

    #[test]
    fn level_drop_uses_note_range() {
        let got = infer_op("music too loud — drop the level by 3 dB", 40, Some(60), Some(72))
            .expect("level inferred");
        assert_eq!(got.op, "level");
        assert_eq!(got.params, json!({ "gain_db": -3.0 }));
        assert_eq!((got.tc_in, got.tc_out), (40, Some(60)));
    }

    #[test]
    fn level_boost_is_positive_and_falls_back_to_duration() {
        let got = infer_op("boost the gain by 2.5db", 10, None, Some(72)).expect("level inferred");
        assert_eq!(got.params, json!({ "gain_db": 2.5 }));
        assert_eq!(got.tc_out, Some(72));
    }

    #[test]
    fn level_without_any_range_is_refused() {
        assert_eq!(infer_op("lower the volume by 3 dB", 10, None, None), None);
    }

    #[test]
    fn creative_notes_are_never_guessed() {
        for text in [
            "the opening feels rushed",
            "can we find a better take here?",
            "this scene drags, restructure the top",
            "make it pop",
        ] {
            assert_eq!(infer_op(text, 0, None, Some(72)), None, "{text:?} must escalate");
        }
    }
}
