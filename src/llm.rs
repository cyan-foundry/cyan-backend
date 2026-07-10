//! The `Llm` seam (feat/notes-constitution; TONIGHT_RUN Part 4).
//!
//! ONE narrow completion trait every model-backed component calls — the future
//! LLM-backed `OpsProposer` and the eval oracle both sit behind it. Provider
//! selection is CONFIG (`claude` | `vllm` | `edge` — Claude / Llama-70B / edge-8B),
//! NEVER hardcoded. The live provider impls land with the LLM-proposer JOIN;
//! default `cargo test` runs entirely on [`ScriptedLlm`] (no model, no keys).

use std::sync::Mutex;

/// The configured model provider. Parsed from config strings; unknown providers
/// are REJECTED (`None`), never silently defaulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmProvider {
    /// Anthropic Claude (the oracle today).
    Claude,
    /// The box's vLLM (Llama-70B — oracle-by-config-flip).
    Vllm,
    /// The edge 8B.
    Edge,
}

impl LlmProvider {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "claude" => Some(Self::Claude),
            "vllm" => Some(Self::Vllm),
            "edge" => Some(Self::Edge),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Vllm => "vllm",
            Self::Edge => "edge",
        }
    }
}

/// The seam. One completion turn: `(system, prompt) → text`. Synchronous by
/// design — the engine's proposer/eval paths are conn-level synchronous code;
/// a live HTTP impl blocks internally (off the UI/FFI threads, per the locks
/// contract). Errors are strings across the trait object; NEVER a panic.
pub trait Llm: Send + Sync {
    fn complete(&self, system: &str, prompt: &str) -> Result<String, String>;
}

/// The scripted fake — CI's stand-in for any live model, mirroring the
/// `Scripted*` convention (`ScriptedTransport`, cyan-lens's `ScriptedLlmClient`).
///
/// `.on(needle, response)` answers any call whose prompt (or system) contains
/// `needle`; `.default_response(..)` catches the rest; with neither, `complete`
/// returns `Err` (exercising callers' no-answer paths). Every call is recorded
/// for prompt assertions via `.calls()`.
#[derive(Default)]
pub struct ScriptedLlm {
    rules: Vec<(String, String)>,
    default_response: Option<String>,
    calls: Mutex<Vec<(String, String)>>,
}

impl ScriptedLlm {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on(mut self, needle: &str, response: &str) -> Self {
        self.rules.push((needle.to_string(), response.to_string()));
        self
    }

    pub fn default_response(mut self, response: &str) -> Self {
        self.default_response = Some(response.to_string());
        self
    }

    /// Every `(system, prompt)` pair this fake has answered, in call order.
    pub fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().map(|c| c.clone()).unwrap_or_default()
    }
}

impl Llm for ScriptedLlm {
    fn complete(&self, system: &str, prompt: &str) -> Result<String, String> {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push((system.to_string(), prompt.to_string()));
        }
        for (needle, response) in &self.rules {
            if prompt.contains(needle.as_str()) || system.contains(needle.as_str()) {
                return Ok(response.clone());
            }
        }
        match &self.default_response {
            Some(r) => Ok(r.clone()),
            None => Err("ScriptedLlm: no rule matched and no default_response set".to_string()),
        }
    }
}
