//! The physical plan the backend wave-executor consumes.
//!
//! Cyan Lens is the **optimizer**: at compile time it lowers the typed DAG into a
//! durable [`PhysicalPlan`] (see cyan-lens `STATUS_LENS_WAVE_PARALLEL.md` and
//! `WORKFLOW_MATERIALIZATION.md` §1). The backend is the **executor**: it runs the
//! plan wave-concurrently and stays the sequential offline fallback when no plan is
//! present. This module is the JSON-portable mirror of that artifact — receive-only,
//! deserialized from Lens's output; nothing here panics.
//!
//! Shape (kept identical to the Lens `plan.rs` types, so the JSON round-trips):
//! ```text
//! PhysicalPlan { tenant_id, waves[], max_concurrency, max_cost_usd, total_cost_usd }
//! Wave         { index, steps[], batches[][]  }   // batches = step-id groups
//! PlannedStep  { id, placement, cache_key, cache_hit, is_gate, gate_barrier?,
//!                cost_usd, concurrency_weight }
//! ```

use serde::{Deserialize, Serialize};

/// The durable artifact the executor runs: ordered waves of independent steps, with
/// per-step placement / cache / gate-barrier and the concurrency + cost caps the plan
/// was built against. `tenant_id` flows onto every exec event and every cache lookup.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhysicalPlan {
    pub tenant_id: String,
    /// Ordered level-sets; run in `index` order.
    pub waves: Vec<Wave>,
    /// The concurrency cap the plan was batched against (informational here — the
    /// batches already encode it).
    #[serde(default)]
    pub max_concurrency: u32,
    #[serde(default)]
    pub max_cost_usd: f64,
    #[serde(default)]
    pub total_cost_usd: f64,
}

/// One level-set of mutually-independent steps. `batches` splits the wave so each
/// batch's summed `concurrency_weight` stays within `max_concurrency`; batches run
/// one after another, each batch's steps concurrently.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Wave {
    pub index: u32,
    pub steps: Vec<PlannedStep>,
    /// Step-id groups; each group runs concurrently, groups run in order.
    #[serde(default)]
    pub batches: Vec<Vec<String>>,
}

/// A single step's placement + optimizer decisions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedStep {
    pub id: String,
    /// `local | cloud | hybrid` — where the step is dispatched (data gravity).
    #[serde(default)]
    pub placement: String,
    #[serde(default)]
    pub cache_key: String,
    /// Content-addressed memoization hit: reuse the prior artifact, skip execution.
    #[serde(default)]
    pub cache_hit: bool,
    /// This step is a human-approval gate (it runs, then pauses for approval before
    /// its dependents — which carry it as their `gate_barrier` — may start).
    #[serde(default)]
    pub is_gate: bool,
    /// The nearest unapproved upstream gate this step waits behind. `None` ⇒ the
    /// step's branch has no gate and proceeds regardless. A gate **branch barrier**,
    /// never a global stall.
    #[serde(default)]
    pub gate_barrier: Option<String>,
    #[serde(default)]
    pub cost_usd: f64,
    #[serde(default = "default_weight")]
    pub concurrency_weight: u32,
}

fn default_weight() -> u32 {
    1
}

impl PhysicalPlan {
    /// Deserialize a plan from Lens JSON. Returns `None` on malformed input rather
    /// than erroring the run — a bad plan degrades to the sequential fallback.
    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }

    /// Waves in execution (`index`) order.
    pub fn ordered_waves(&self) -> Vec<&Wave> {
        let mut waves: Vec<&Wave> = self.waves.iter().collect();
        waves.sort_by_key(|w| w.index);
        waves
    }
}

impl Wave {
    /// The id-groups to run, in order. Falls back to one batch of every step (the
    /// whole wave concurrent) when the plan omitted explicit batching.
    pub fn ordered_batches(&self) -> Vec<Vec<String>> {
        if self.batches.is_empty() {
            vec![self.steps.iter().map(|s| s.id.clone()).collect()]
        } else {
            self.batches.clone()
        }
    }

    pub fn step(&self, id: &str) -> Option<&PlannedStep> {
        self.steps.iter().find(|s| s.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_lens_plan_shape() {
        let json = r#"{
            "tenant_id": "grp-1",
            "max_concurrency": 2,
            "max_cost_usd": 10.0,
            "total_cost_usd": 1.5,
            "waves": [
                { "index": 0, "steps": [{"id":"a","placement":"local","cache_key":"k","cache_hit":false,"is_gate":false,"gate_barrier":null,"cost_usd":0.5,"concurrency_weight":1}], "batches": [["a"]] },
                { "index": 1, "steps": [
                    {"id":"b","placement":"local","cache_key":"kb","cache_hit":true,"is_gate":false,"gate_barrier":null,"cost_usd":0.0,"concurrency_weight":1},
                    {"id":"c","placement":"cloud","cache_key":"kc","cache_hit":false,"is_gate":false,"gate_barrier":"a","cost_usd":1.0,"concurrency_weight":1}
                ], "batches": [["b","c"]] }
            ]
        }"#;
        let plan = PhysicalPlan::from_json(json).expect("parses");
        assert_eq!(plan.tenant_id, "grp-1");
        assert_eq!(plan.ordered_waves().len(), 2);
        let w1 = plan.ordered_waves()[1];
        assert_eq!(w1.ordered_batches(), vec![vec!["b".to_string(), "c".to_string()]]);
        assert!(w1.step("b").expect("b").cache_hit);
        assert_eq!(w1.step("c").expect("c").gate_barrier.as_deref(), Some("a"));
    }

    #[test]
    fn missing_batches_default_to_one_concurrent_batch() {
        let json = r#"{"tenant_id":"t","waves":[{"index":0,"steps":[
            {"id":"x"},{"id":"y"}
        ]}]}"#;
        let plan = PhysicalPlan::from_json(json).expect("parses with serde defaults");
        let w = plan.ordered_waves()[0];
        assert_eq!(w.ordered_batches(), vec![vec!["x".to_string(), "y".to_string()]]);
        // serde defaults fill the optimizer fields.
        assert_eq!(w.step("x").expect("x").concurrency_weight, 1);
        assert!(!w.step("y").expect("y").cache_hit);
    }

    #[test]
    fn malformed_json_is_none_not_panic() {
        assert!(PhysicalPlan::from_json("not json").is_none());
        assert!(PhysicalPlan::from_json("{}").is_none()); // missing tenant_id/waves
    }
}
