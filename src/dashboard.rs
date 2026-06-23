//! Dashboard producer — the read-model the board workflow dashboard renders.
//!
//! See `DASHBOARD_CONTRACT.md`. The dashboard is a **consumer**: it renders live
//! exec transitions (additive [`crate::models::events::SwiftEvent`] variants) plus
//! one rolled-up [`DashboardSnapshot`] the producer computes from the obs/cost rail.
//! This module owns the snapshot shape and the aggregation ([`RunObs`]); the actual
//! events are emitted from the REAL run path (`crate::pipeline::run_pipeline`).
//!
//! Everything here is flat/primitive (Parquet-safe) and tagged with
//! `tenant_id + run_id + board_id + workflow_id + stage + plugin?` so the read-model
//! can slice by workflow/stage/plugin and the bill is attributable (DASHBOARD_CONTRACT
//! §5/§7). Nothing here panics — it is reachable from FFI-driven runs.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ── Cost rail rates ────────────────────────────────────────────────────────
// Flat USD rates used by [`StepCost::est_usd`] (the cost rail, DASHBOARD_CONTRACT
// §C `est_cost_usd`). Token rates are per 1k tokens; GPU is per millisecond.
// External (plugin) cost arrives already in USD on the cost rail.
pub const USD_PER_1K_TOKENS_IN: f64 = 0.003;
pub const USD_PER_1K_TOKENS_OUT: f64 = 0.015;
pub const USD_PER_GPU_MS: f64 = 0.000_02; // ≈ $0.02 / GPU-second

/// Who performed a step — the `actor` slicing dimension (DASHBOARD_CONTRACT §A).
/// `Ai` = agentic/compute work (its wall time is `ai_minutes`); `Human` = a
/// manual/gate step (its time is `human_minutes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Actor {
    Human,
    Ai,
}

impl Actor {
    pub fn as_str(&self) -> &'static str {
        match self {
            Actor::Human => "human",
            Actor::Ai => "ai",
        }
    }
}

/// A step's lifecycle state (DASHBOARD_CONTRACT §A: `StepStateChanged.state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepState {
    Pending,
    Running,
    AwaitingApproval,
    Approved,
    Done,
    Failed,
}

impl StepState {
    pub fn as_str(&self) -> &'static str {
        match self {
            StepState::Pending => "pending",
            StepState::Running => "running",
            StepState::AwaitingApproval => "awaiting_approval",
            StepState::Approved => "approved",
            StepState::Done => "done",
            StepState::Failed => "failed",
        }
    }

    /// Whether the step reached a terminal state (counts toward `items_processed`).
    pub fn is_terminal(&self) -> bool {
        matches!(self, StepState::Approved | StepState::Done | StepState::Failed)
    }
}

/// Per-step cost inputs — the cost rail's raw obs (DASHBOARD_CONTRACT §C). Flat
/// primitives; `external_usd` is a plugin/external-call bill already in USD.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StepCost {
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub gpu_ms: u64,
    pub external_usd: f64,
}

impl StepCost {
    /// The cost-rail estimate: Σ(tokens×rate) + Σ(gpu_ms×rate) + external plugin cost.
    pub fn est_usd(&self) -> f64 {
        (self.tokens_in as f64 / 1000.0) * USD_PER_1K_TOKENS_IN
            + (self.tokens_out as f64 / 1000.0) * USD_PER_1K_TOKENS_OUT
            + (self.gpu_ms as f64) * USD_PER_GPU_MS
            + self.external_usd
    }
}

/// One step's observation, fed into [`RunObs`] as the run progresses. The
/// aggregation over these (grouped by `stage`) IS the [`DashboardSnapshot`].
#[derive(Debug, Clone)]
pub struct StepObs {
    pub step_id: String,
    pub name: String,
    pub stage: String,
    pub actor: Actor,
    pub plugin: Option<String>,
    pub state: StepState,
    /// Wall time spent in this step, milliseconds.
    pub wall_ms: u64,
    /// Approval-gate (human) time attributed to this step's stage, milliseconds.
    pub gate_ms: u64,
    pub cost: StepCost,
}

/// Aggregated read-model of one workflow run (DASHBOARD_CONTRACT §B). Computed by
/// the producer from obs; the app never aggregates — it renders this directly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    pub tenant_id: String,
    pub board_id: String,
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_label: String,
    pub updated_at: i64,
    pub current_item: Option<String>,
    pub current_stage: Option<String>,
    pub items_processed: u64,
    pub items_total: u64,
    pub totals: Totals,
    pub per_stage: Vec<StageStat>,
    /// Human/gate minutes by stage — "which stages eat human hours" (the prompt's
    /// `gate_minutes_by_stage`). Mirrors `per_stage[].human_minutes`, keyed for the UI.
    pub gate_minutes_by_stage: BTreeMap<String, f64>,
}

/// Run-wide totals (DASHBOARD_CONTRACT §B `totals`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Totals {
    pub wall_minutes: f64,
    pub human_minutes: f64,
    pub ai_minutes: f64,
    pub files_processed: u64,
    pub est_cost_usd: f64,
}

/// Per-stage breakdown (DASHBOARD_CONTRACT §B `per_stage`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StageStat {
    pub stage: String,
    pub state: String,
    pub minutes: f64,
    pub human_minutes: f64,
    pub ai_minutes: f64,
    pub cost_usd: f64,
    pub plugins: Vec<String>,
}

/// Milliseconds → minutes.
fn ms_to_min(ms: u64) -> f64 {
    ms as f64 / 60_000.0
}

/// Accumulates a run's step obs and computes the [`DashboardSnapshot`]. One per run.
#[derive(Debug, Clone)]
pub struct RunObs {
    pub tenant_id: String,
    pub board_id: String,
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_label: String,
    pub items_total: u64,
    steps: Vec<StepObs>,
}

impl RunObs {
    pub fn new(
        tenant_id: impl Into<String>,
        board_id: impl Into<String>,
        run_id: impl Into<String>,
        workflow_id: impl Into<String>,
        workflow_label: impl Into<String>,
        items_total: u64,
    ) -> Self {
        RunObs {
            tenant_id: tenant_id.into(),
            board_id: board_id.into(),
            run_id: run_id.into(),
            workflow_id: workflow_id.into(),
            workflow_label: workflow_label.into(),
            items_total,
            steps: Vec::new(),
        }
    }

    /// Record one step's obs (called once the step reaches a reportable state).
    pub fn record(&mut self, obs: StepObs) {
        self.steps.push(obs);
    }

    /// Steps that reached a terminal state (the `items_processed`/`files_processed` count).
    pub fn items_processed(&self) -> u64 {
        self.steps.iter().filter(|s| s.state.is_terminal()).count() as u64
    }

    /// Compute the snapshot as-of `updated_at`. `current_item`/`current_stage` name
    /// the step the dashboard should foreground (DASHBOARD_CONTRACT §C "latest running").
    pub fn snapshot(
        &self,
        updated_at: i64,
        current_item: Option<String>,
        current_stage: Option<String>,
    ) -> DashboardSnapshot {
        // Group by stage, preserving first-appearance order for a stable read-model.
        let mut stage_order: Vec<String> = Vec::new();
        let mut by_stage: BTreeMap<String, Vec<&StepObs>> = BTreeMap::new();
        for s in &self.steps {
            if !stage_order.contains(&s.stage) {
                stage_order.push(s.stage.clone());
            }
            by_stage.entry(s.stage.clone()).or_default().push(s);
        }

        let mut per_stage = Vec::with_capacity(stage_order.len());
        let mut gate_minutes_by_stage = BTreeMap::new();
        let (mut wall_ms, mut human_ms, mut ai_ms, mut cost_usd) = (0u64, 0u64, 0u64, 0.0f64);

        for stage in &stage_order {
            let obs = &by_stage[stage];
            let stage_wall: u64 = obs.iter().map(|s| s.wall_ms).sum();
            let stage_gate: u64 = obs.iter().map(|s| s.gate_ms).sum();
            let stage_ai: u64 = obs
                .iter()
                .filter(|s| s.actor == Actor::Ai)
                .map(|s| s.wall_ms)
                .sum();
            let stage_cost: f64 = obs.iter().map(|s| s.cost.est_usd()).sum();

            let mut plugins: Vec<String> = obs.iter().filter_map(|s| s.plugin.clone()).collect();
            plugins.sort();
            plugins.dedup();

            // The stage's state is its latest step's state.
            let state = obs
                .last()
                .map(|s| s.state.as_str().to_string())
                .unwrap_or_else(|| StepState::Pending.as_str().to_string());

            let stage_human_min = ms_to_min(stage_gate);
            gate_minutes_by_stage.insert(stage.clone(), stage_human_min);
            per_stage.push(StageStat {
                stage: stage.clone(),
                state,
                minutes: ms_to_min(stage_wall),
                human_minutes: stage_human_min,
                ai_minutes: ms_to_min(stage_ai),
                cost_usd: stage_cost,
                plugins,
            });

            wall_ms += stage_wall;
            human_ms += stage_gate;
            ai_ms += stage_ai;
            cost_usd += stage_cost;
        }

        let files_processed = self.items_processed();

        DashboardSnapshot {
            tenant_id: self.tenant_id.clone(),
            board_id: self.board_id.clone(),
            run_id: self.run_id.clone(),
            workflow_id: self.workflow_id.clone(),
            workflow_label: self.workflow_label.clone(),
            updated_at,
            current_item,
            current_stage,
            items_processed: files_processed,
            items_total: self.items_total,
            totals: Totals {
                wall_minutes: ms_to_min(wall_ms),
                human_minutes: ms_to_min(human_ms),
                ai_minutes: ms_to_min(ai_ms),
                files_processed,
                est_cost_usd: cost_usd,
            },
            per_stage,
            gate_minutes_by_stage,
        }
    }
}
