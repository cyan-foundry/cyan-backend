//! Process-global, purely-observational stress/chaos metrics.
//!
//! This module is **additive instrumentation only** — it never changes engine
//! behavior. It exposes a handful of atomic counters that the stress fabric
//! (the `cyan_node` test bin + the multi-process stress suite) reads to assert
//! the hard invariants in `STRESS_HARNESS_SPEC.md`:
//!
//! - **gossip message volume bounded vs N** ("no message storm"): every inbound
//!   gossip message that `TopicActor` actually processes bumps [`record_gossip_recv`].
//!   A quadratic blow-up shows up directly as a runaway count.
//! - **bounded gossip degree**: `NeighborUp`/`NeighborDown` adjust a live gauge
//!   ([`record_neighbor_up`] / [`record_neighbor_down`]); the stress suite reads
//!   [`gossip_degree`] to confirm the per-node neighbor count stays bounded as N
//!   grows (iroh-gossip's HyParView keeps active degree ~constant by design — this
//!   measures that it actually does).
//!
//! Counters are process-global (the engine's `storage` is process-global too), so
//! each `cyan_node` OS process reports ITS OWN numbers — the honest per-peer view
//! the spec demands. Reset is available for in-process tests that reuse a process.
//!
//! Cost: a relaxed atomic add on the gossip receive path. Behavior-neutral.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Total inbound gossip messages this node has processed (NetworkEvent +
/// NetworkCommand + swarm negotiation), excluding our own echoed messages
/// (those are dropped before this point). The "no storm" oracle.
static GOSSIP_RECV: AtomicU64 = AtomicU64::new(0);

/// Lifetime count of `NeighborUp` events (peers that became gossip neighbors).
static NEIGHBOR_UP: AtomicU64 = AtomicU64::new(0);

/// Lifetime count of `NeighborDown` events.
static NEIGHBOR_DOWN: AtomicU64 = AtomicU64::new(0);

/// Live gossip degree gauge = up − down, clamped at 0. Signed so a transient
/// reordering of up/down can never wrap a u64.
static GOSSIP_DEGREE: AtomicI64 = AtomicI64::new(0);

/// One inbound gossip message processed. Called once per delivered, non-self
/// gossip message in `TopicActor::handle_gossip_event`.
#[inline]
pub fn record_gossip_recv() {
    GOSSIP_RECV.fetch_add(1, Ordering::Relaxed);
}

/// A gossip neighbor came up.
#[inline]
pub fn record_neighbor_up() {
    NEIGHBOR_UP.fetch_add(1, Ordering::Relaxed);
    GOSSIP_DEGREE.fetch_add(1, Ordering::Relaxed);
}

/// A gossip neighbor went down.
#[inline]
pub fn record_neighbor_down() {
    NEIGHBOR_DOWN.fetch_add(1, Ordering::Relaxed);
    // Clamp at zero: never let the live gauge go negative if down races ahead.
    let prev = GOSSIP_DEGREE.fetch_sub(1, Ordering::Relaxed);
    if prev <= 0 {
        GOSSIP_DEGREE.store(0, Ordering::Relaxed);
    }
}

/// Total inbound gossip messages processed by this node.
pub fn gossip_recv() -> u64 {
    GOSSIP_RECV.load(Ordering::Relaxed)
}

/// Lifetime `NeighborUp` count.
pub fn neighbor_up() -> u64 {
    NEIGHBOR_UP.load(Ordering::Relaxed)
}

/// Lifetime `NeighborDown` count.
pub fn neighbor_down() -> u64 {
    NEIGHBOR_DOWN.load(Ordering::Relaxed)
}

/// Current live gossip degree (active neighbors) for this node. Never negative.
pub fn gossip_degree() -> u64 {
    GOSSIP_DEGREE.load(Ordering::Relaxed).max(0) as u64
}

/// Resident set size of THIS process in kilobytes, or `None` if it can't be read.
///
/// Portable best-effort: `/proc/self/statm` on Linux (the Docker rig), `ps` on
/// macOS/BSD. Used as the "bounded memory over the run" oracle — the stress suite
/// samples it before/after load and asserts no unbounded growth.
pub fn rss_kb() -> Option<u64> {
    // Linux: /proc/self/statm — second field is resident pages.
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        if let Some(pages) = statm.split_whitespace().nth(1) {
            if let Ok(pages) = pages.parse::<u64>() {
                let page_kb = 4; // 4 KiB pages on every platform we ship to.
                return Some(pages * page_kb);
            }
        }
    }
    // macOS / BSD: ask `ps` for this pid's RSS (already in KiB).
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().parse::<u64>().ok()
}

/// Reset all counters. For in-process tests that reuse a process across cases;
/// each `cyan_node` OS process starts clean anyway.
pub fn reset() {
    GOSSIP_RECV.store(0, Ordering::Relaxed);
    NEIGHBOR_UP.store(0, Ordering::Relaxed);
    NEIGHBOR_DOWN.store(0, Ordering::Relaxed);
    GOSSIP_DEGREE.store(0, Ordering::Relaxed);
}
