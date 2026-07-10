//! THE JOIN (SESSION_JOIN §3) — the PRODUCTION Llm-backed `OpsProposer`.
//!
//! [`LlmOpsProposer`] sits behind the frozen seam (PROPOSE_OPS_CONTRACT.md) and
//! calls the [`crate::llm::Llm`] trait. Provider selection is CONFIG
//! (`claude | vllm | edge` — Claude / the box's Llama-70B / the edge 8B), NEVER
//! hardcoded; unknown providers are rejected and the deterministic regex impl
//! runs instead. Default `cargo test` exercises everything on `ScriptedLlm`.
//!
//! **Never-guess is enforced twice**: the prompt instructs `[]` unless the note
//! (± constitution) fully specifies a closed-vocab op, and the STRICT parser
//! rejects the WHOLE answer on any out-of-vocab op, missing required param,
//! degenerate range, or malformed JSON. An empty proposal is always the safe
//! output — the note stays with the human.
//!
//! **Locks note:** `propose_from_note*` runs with the store connection held. A
//! live LLM turn under that guard parks other verbs for its duration — which is
//! why the LLM proposer is OPT-IN (`CYAN_OPS_PROPOSER=llm`), the completion has
//! a hard timeout, and the zero-LLM regex impl stays the default.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use crate::llm::{Llm, LlmProvider};
use crate::ops_proposer::{OpsProposer, ProposeCtx, ProposedOp, ReviewNote};

/// The ops an LLM answer may use: the contract vocabulary ∩ the ledger vocabulary
/// (`changelist::OP_VOCAB`). The contract spells loudness-normalize as
/// `level + target_lufs` — `loudnorm` is NOT a ledger op, and an answer using it
/// is untrusted (the schema string says so explicitly).
const LLM_OP_VOCAB: [&str; 8] =
    ["trim", "delete", "level", "mute", "fade", "speed", "reframe", "color"];

/// Hard ceiling on one completion turn (overridable via `CYAN_LLM_TIMEOUT_SECS`).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

// ════════════════════════════════════════════════════════════════════════════
// Config: which proposer runs. Regex is the zero-LLM default.
// ════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposerChoice {
    /// The deterministic zero-LLM default (`note_inference::RegexOpsProposer`).
    Regex,
    /// The Llm-backed proposer on the given provider.
    Llm(LlmProvider),
}

/// Pure config → choice. `kind` is `CYAN_OPS_PROPOSER` (`regex` default | `llm`);
/// `provider` is `CYAN_LLM_PROVIDER` (`claude | vllm | edge`). An `llm` ask with
/// a missing/unknown provider is REJECTED back to regex — a model is never
/// silently guessed.
pub fn proposer_choice(kind: Option<&str>, provider: Option<&str>) -> ProposerChoice {
    match kind {
        Some("llm") => match provider.and_then(LlmProvider::parse) {
            Some(p) => ProposerChoice::Llm(p),
            None => {
                tracing::warn!(
                    "CYAN_OPS_PROPOSER=llm but CYAN_LLM_PROVIDER is missing/unknown ({provider:?}) — \
                     falling back to the regex proposer (a provider is never guessed)"
                );
                ProposerChoice::Regex
            }
        },
        Some("regex") | None => ProposerChoice::Regex,
        Some(other) => {
            tracing::warn!("unknown CYAN_OPS_PROPOSER {other:?} — regex runs");
            ProposerChoice::Regex
        }
    }
}

/// The environment's choice (`CYAN_OPS_PROPOSER` / `CYAN_LLM_PROVIDER`).
pub fn proposer_choice_from_env() -> ProposerChoice {
    proposer_choice(
        std::env::var("CYAN_OPS_PROPOSER").ok().as_deref(),
        std::env::var("CYAN_LLM_PROVIDER").ok().as_deref(),
    )
}

// ════════════════════════════════════════════════════════════════════════════
// The proposer.
// ════════════════════════════════════════════════════════════════════════════

pub struct LlmOpsProposer {
    llm: Arc<dyn Llm>,
    provider: LlmProvider,
}

impl LlmOpsProposer {
    pub fn new(llm: Arc<dyn Llm>, provider: LlmProvider) -> Self {
        Self { llm, provider }
    }

    fn system_prompt() -> &'static str {
        "You convert ONE reviewer note into mechanical edit operations from a CLOSED \
         vocabulary, for frame-accurate video conform. You NEVER guess: reply with an \
         op ONLY when the note — read together with the constitution and preferences — \
         fully specifies the operation and every required parameter inside the closed \
         vocabulary. Under-specified, creative, ambiguous, or out-of-vocabulary asks \
         MUST get the empty answer. Reply with ONLY a JSON array (no prose, no keys \
         around it): [] for no operation, or op objects shaped \
         {\"op\", \"params\", \"tc_in\", \"tc_out\", \"confidence\", \"rationale\"}. \
         Frames are integers in MASTER coordinates. `confidence` is 0..1. `rationale` \
         is one sentence naming what specified the op (quote the house rule when it did)."
    }

    fn user_prompt(note: &ReviewNote, ctx: &ProposeCtx) -> String {
        format!(
            "TOOL SCHEMAS (the closed vocabulary — the ONLY ops you may emit):\n{}\n\n\
             CONSTITUTION (house rules; board > group > tenant):\n{}\n\n\
             PREFERENCES:\n{}\n\n\
             ASSET: duration_frames={} fps={}\n\n\
             REVIEWER NOTE (source={}, master-frame anchor={}):\n{}\n\n\
             Reply with ONLY the JSON array.",
            ctx.tool_schemas,
            if ctx.constitution.is_empty() { "(none)" } else { ctx.constitution },
            if ctx.preferences.is_empty() { "(none)" } else { ctx.preferences },
            ctx.asset
                .duration_frames
                .map(|d| d.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            ctx.asset.fps,
            note.source,
            note.tc_out.map(|t| t.to_string()).unwrap_or_else(|| "none".to_string()),
            note.text,
        )
    }
}

impl OpsProposer for LlmOpsProposer {
    fn propose_ops(&self, note: &ReviewNote, ctx: &ProposeCtx) -> Vec<ProposedOp> {
        let raw = match self.llm.complete(Self::system_prompt(), &Self::user_prompt(note, ctx)) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "llm_proposer({}): completion failed — empty proposal (never a guess): {e}",
                    self.provider.as_str()
                );
                return Vec::new();
            }
        };
        let ops = parse_strict(&raw);
        tracing::info!(
            "obs llm_propose provider={} note_source={} ops={} raw_bytes={}",
            self.provider.as_str(),
            note.source,
            ops.len(),
            raw.len()
        );
        ops
    }
}

// ════════════════════════════════════════════════════════════════════════════
// The STRICT parser. Any invalid op poisons the WHOLE answer (an answer that
// hallucinates once is untrusted everywhere).
// ════════════════════════════════════════════════════════════════════════════

/// Strip a surrounding markdown fence (``` / ```json) — formatting, not content.
fn strip_fence(raw: &str) -> &str {
    let t = raw.trim();
    let Some(rest) = t.strip_prefix("```") else { return t };
    let rest = rest.strip_prefix("json").unwrap_or(rest);
    rest.strip_suffix("```").unwrap_or(rest).trim()
}

fn parse_strict(raw: &str) -> Vec<ProposedOp> {
    let Ok(vals) = serde_json::from_str::<Vec<serde_json::Value>>(strip_fence(raw)) else {
        return Vec::new(); // not a bare JSON array ⇒ untrusted
    };
    let mut ops = Vec::with_capacity(vals.len());
    for v in &vals {
        match parse_op(v) {
            Some(op) => ops.push(op),
            None => return Vec::new(), // one bad op poisons the batch
        }
    }
    ops
}

fn parse_op(v: &serde_json::Value) -> Option<ProposedOp> {
    let op = v.get("op")?.as_str()?;
    if !LLM_OP_VOCAB.contains(&op) {
        return None;
    }
    let params = v.get("params").cloned().unwrap_or_else(|| serde_json::json!({}));
    if !params.is_object() {
        return None;
    }
    let tc_in = v.get("tc_in").and_then(|t| t.as_i64());
    let tc_out = v.get("tc_out").and_then(|t| t.as_i64());
    let confidence = match v.get("confidence") {
        None | Some(serde_json::Value::Null) => None,
        Some(c) => {
            let c = c.as_f64()?;
            if !(0.0..=1.0).contains(&c) {
                return None; // a confidence outside [0,1] marks a confused answer
            }
            Some(c as f32)
        }
    };

    // Per-op required params — under-specified ops NEVER land.
    let p = |k: &str| params.get(k);
    let pos_int = |k: &str| p(k).and_then(|x| x.as_i64()).filter(|n| *n > 0);
    let range_ok = match (tc_in, tc_out) {
        (Some(i), Some(o)) => o > i,
        _ => false,
    };
    let fully_specified = match op {
        "trim" => {
            let edge = p("edge").and_then(|e| e.as_str());
            let frames_ok = pos_int("frames").is_some();
            match edge {
                Some("head") => frames_ok,
                // A tail trim conforms as `-to (tc_out - frames)` — the clip end
                // must be IN THE ANSWER. The prompt hands the model
                // duration_frames; an answer that dropped it is sloppy ⇒ reject
                // (the regex impl fills it deterministically; an LLM must not be
                // silently repaired).
                Some("tail") => frames_ok && tc_out.filter(|o| *o > 0).is_some(),
                _ => false,
            }
        }
        "level" => {
            let gain = p("gain_db").and_then(|g| g.as_f64());
            let lufs = p("target_lufs").and_then(|l| l.as_f64());
            match (gain, lufs) {
                // conform windows a gain over [tc_in, tc_out) — the range is required.
                (Some(g), _) if g != 0.0 => range_ok,
                // target_lufs renders as a global loudnorm — no window needed.
                (None, Some(_)) => true,
                _ => false,
            }
        }
        "mute" | "delete" => range_ok,
        "fade" => {
            matches!(p("dir").and_then(|d| d.as_str()), Some("in") | Some("out"))
                && pos_int("frames").is_some()
        }
        "speed" => p("ratio").and_then(|r| r.as_f64()).filter(|r| *r > 0.0).is_some(),
        "reframe" => p("aspect").and_then(|a| a.as_str()).filter(|a| !a.is_empty()).is_some(),
        "color" => p("preset").and_then(|s| s.as_str()).filter(|s| !s.is_empty()).is_some(),
        _ => false,
    };
    if !fully_specified {
        return None;
    }

    Some(ProposedOp {
        op: op.to_string(),
        params,
        tc_in,
        tc_out,
        confidence,
        rationale: v.get("rationale").and_then(|r| r.as_str()).map(String::from),
    })
}

// ════════════════════════════════════════════════════════════════════════════
// The live `Llm` impl: one completion turn over HTTP, per provider. The whole
// request runs on its OWN OS thread (reqwest::blocking must never run inside a
// Tokio worker — the lens learned this live, 77c1435) with a hard timeout.
// ════════════════════════════════════════════════════════════════════════════

pub struct HttpLlm {
    provider: LlmProvider,
    /// claude: unused (fixed API base). vllm/edge: the OpenAI-compatible base URL.
    base_url: String,
    model: String,
    /// claude only.
    api_key: String,
    timeout: Duration,
}

impl HttpLlm {
    /// Build from the environment. `None` (with a warn) when the provider's
    /// required config is missing — the caller falls back to regex; a missing
    /// key is a configuration fact, never a panic.
    pub fn from_env(provider: LlmProvider) -> Option<Self> {
        let timeout = std::env::var("CYAN_LLM_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_TIMEOUT);
        let need = |var: &str| {
            let v = std::env::var(var).ok().filter(|v| !v.is_empty());
            if v.is_none() {
                tracing::warn!("llm_proposer: {var} unset — provider {} unavailable", provider.as_str());
            }
            v
        };
        match provider {
            LlmProvider::Claude => Some(Self {
                provider,
                base_url: std::env::var("ANTHROPIC_BASE_URL")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| "https://api.anthropic.com".to_string()),
                model: std::env::var("CYAN_LLM_MODEL")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| "claude-sonnet-5".to_string()),
                api_key: need("ANTHROPIC_API_KEY")?,
                timeout,
            }),
            LlmProvider::Vllm => Some(Self {
                provider,
                base_url: need("CYAN_VLLM_URL")?,
                model: need("CYAN_VLLM_MODEL")?,
                api_key: String::new(),
                timeout,
            }),
            LlmProvider::Edge => Some(Self {
                provider,
                base_url: need("CYAN_EDGE_URL")?,
                model: need("CYAN_EDGE_MODEL")?,
                api_key: String::new(),
                timeout,
            }),
        }
    }

    fn request_blocking(&self, system: &str, prompt: &str) -> Result<String, String> {
        let client = reqwest::blocking::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        match self.provider {
            LlmProvider::Claude => {
                let body = serde_json::json!({
                    "model": self.model,
                    "max_tokens": 1024,
                    "system": system,
                    "messages": [{ "role": "user", "content": prompt }],
                });
                let resp = client
                    .post(format!("{}/v1/messages", self.base_url.trim_end_matches('/')))
                    .header("x-api-key", &self.api_key)
                    .header("anthropic-version", "2023-06-01")
                    .json(&body)
                    .send()
                    .map_err(|e| format!("claude request: {e}"))?;
                let status = resp.status();
                let v: serde_json::Value =
                    resp.json().map_err(|e| format!("claude response body: {e}"))?;
                if !status.is_success() {
                    return Err(format!("claude {status}: {v}"));
                }
                v["content"][0]["text"]
                    .as_str()
                    .map(String::from)
                    .ok_or_else(|| format!("claude: no text content in {v}"))
            }
            LlmProvider::Vllm | LlmProvider::Edge => {
                let body = serde_json::json!({
                    "model": self.model,
                    "temperature": 0,
                    "messages": [
                        { "role": "system", "content": system },
                        { "role": "user", "content": prompt },
                    ],
                });
                let resp = client
                    .post(format!(
                        "{}/v1/chat/completions",
                        self.base_url.trim_end_matches('/')
                    ))
                    .json(&body)
                    .send()
                    .map_err(|e| format!("{} request: {e}", self.provider.as_str()))?;
                let status = resp.status();
                let v: serde_json::Value = resp
                    .json()
                    .map_err(|e| format!("{} response body: {e}", self.provider.as_str()))?;
                if !status.is_success() {
                    return Err(format!("{} {status}: {v}", self.provider.as_str()));
                }
                v["choices"][0]["message"]["content"]
                    .as_str()
                    .map(String::from)
                    .ok_or_else(|| format!("{}: no message content in {v}", self.provider.as_str()))
            }
        }
    }
}

impl Llm for HttpLlm {
    fn complete(&self, system: &str, prompt: &str) -> Result<String, String> {
        // A dedicated OS thread: safe from both sync FFI dispatch threads and
        // Tokio workers (a blocking reqwest inside a runtime worker aborts).
        let (tx, rx) = mpsc::channel();
        let system = system.to_string();
        let prompt = prompt.to_string();
        // Rebuild a shallow config clone for the thread (all fields are owned).
        let me = HttpLlm {
            provider: self.provider,
            base_url: self.base_url.clone(),
            model: self.model.clone(),
            api_key: self.api_key.clone(),
            timeout: self.timeout,
        };
        let deadline = self.timeout + Duration::from_secs(5);
        std::thread::spawn(move || {
            let _ = tx.send(me.request_blocking(&system, &prompt));
        });
        match rx.recv_timeout(deadline) {
            Ok(r) => r,
            Err(_) => Err(format!(
                "{}: completion timed out after {:?}",
                self.provider.as_str(),
                deadline
            )),
        }
    }
}

/// The env-configured proposer for the spine's default path: `None` means run
/// the regex impl (default, or an LLM ask whose provider/config is unusable).
pub fn configured_llm_proposer() -> Option<LlmOpsProposer> {
    match proposer_choice_from_env() {
        ProposerChoice::Regex => None,
        ProposerChoice::Llm(provider) => {
            let llm = HttpLlm::from_env(provider)?;
            Some(LlmOpsProposer::new(Arc::new(llm), provider))
        }
    }
}
