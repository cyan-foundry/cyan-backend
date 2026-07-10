/// A reviewer note ready for op inference. Anchored at the note's master-frame TC.
pub struct ReviewNote {
    pub text: String,
    pub tc_out: Option<i64>,     // master-frame anchor (from the sensed comment)
    pub source: String,          // "frameio", ...
}

/// Everything a proposer MAY consider. Deterministic impls (regex) MAY ignore fields;
/// the LLM impl consumes all of it. Borrowed so producers own the data.
pub struct ProposeCtx<'a> {
    pub constitution: &'a str,   // merged tenant⊕group⊕board notes (markdown) — Session B produces
    pub preferences: &'a str,    // producer/color prefs — Session B produces
    pub asset: &'a AssetMeta,    // duration_frames, fps, ...
    pub tool_schemas: &'a str,   // the closed-vocab op vocabulary a proposer may emit
}

pub struct AssetMeta {
    pub duration_frames: Option<i64>,
    pub fps: f64,
}

/// One proposed mechanical op — shape mirrors cyan-media conform's ChangeEntry.
pub struct ProposedOp {
    pub op: String,                // closed vocab: trim|delete|level|mute|fade|speed|reframe|color|loudnorm
    pub params: serde_json::Value, // op-specific params (conform-compatible)
    pub tc_in: Option<i64>,
    pub tc_out: Option<i64>,
    pub confidence: Option<f32>,   // drives the confirm gate / batch thresholds
    pub rationale: Option<String>, // why — for the human confirm + the eval oracle
}

/// The seam. Regex today; Llm-backed later; both implement this.
pub trait OpsProposer: Send + Sync {
    /// NEVER guesses: emits an op ONLY when the note (± constitution) fully specifies one
    /// inside the closed vocab. Returns empty when nothing is confidently inferable.
    fn propose_ops(&self, note: &ReviewNote, ctx: &ProposeCtx) -> Vec<ProposedOp>;
}
