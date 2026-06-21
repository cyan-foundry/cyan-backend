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

/// Anti-entropy state digests this node has broadcast (one per sweep with peers
/// present). The sweep's cost is `O(1)` gossip per tick, so this grows linearly in
/// run time — the "sweep traffic is bounded, not a storm" oracle.
static AE_DIGEST_SENT: AtomicU64 = AtomicU64::new(0);

/// Anti-entropy repair pulls this node has STARTED (a snapshot merge triggered by a
/// divergent digest). Debounced to one in-flight per group, so this stays small/
/// bounded and does NOT grow with message volume — the "repair is cheap" oracle.
static AE_REPAIR: AtomicU64 = AtomicU64::new(0);

/// Snapshots this node has SERVED to other peers (join-time pulls + anti-entropy
/// repairs both land here, since they share one serve path). The "snapshot under
/// load" oracle: with concurrent cold-joiners spread multi-source across holders, no
/// single holder's served count should equal the whole joiner fleet.
static SNAPSHOT_SERVED: AtomicU64 = AtomicU64::new(0);

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

/// One anti-entropy state digest broadcast onto a group topic.
#[inline]
pub fn record_ae_digest_sent() {
    AE_DIGEST_SENT.fetch_add(1, Ordering::Relaxed);
}

/// One anti-entropy repair pull started (post-debounce).
#[inline]
pub fn record_ae_repair() {
    AE_REPAIR.fetch_add(1, Ordering::Relaxed);
}

/// One snapshot served to a peer (join-time or anti-entropy repair).
#[inline]
pub fn record_snapshot_served() {
    SNAPSHOT_SERVED.fetch_add(1, Ordering::Relaxed);
}

/// §5 incremental catch-up: snapshots SERVED as an incremental delta (`since` was set,
/// so only rows newer than the requester's high-water mark were sent). The "a returning
/// peer pulled only the delta, not a full re-snapshot" oracle.
static INCREMENTAL_SERVED: AtomicU64 = AtomicU64::new(0);

/// §5 incremental catch-up: snapshots SERVED as a FULL snapshot (`since` absent — the
/// cold-start / no-common-base fallback). Paired with [`incremental_served`] so a test
/// can prove a catch-up took the incremental path and NOT the full path.
static FULL_SERVED: AtomicU64 = AtomicU64::new(0);

/// Rows the LAST served snapshot put on the wire (sum across Structure/Content/Metadata).
/// The transfer-size oracle: an incremental catch-up serves only the missing range, so this
/// reflects the delta count, not the whole group.
static ROWS_SERVED_LAST: AtomicU64 = AtomicU64::new(0);

/// One incremental (`since`-bounded) snapshot served, carrying `rows` data rows.
#[inline]
pub fn record_incremental_served(rows: u64) {
    INCREMENTAL_SERVED.fetch_add(1, Ordering::Relaxed);
    ROWS_SERVED_LAST.store(rows, Ordering::Relaxed);
}

/// One full snapshot served, carrying `rows` data rows.
#[inline]
pub fn record_full_served(rows: u64) {
    FULL_SERVED.fetch_add(1, Ordering::Relaxed);
    ROWS_SERVED_LAST.store(rows, Ordering::Relaxed);
}

/// Incremental (delta) snapshots this node has served.
pub fn incremental_served() -> u64 {
    INCREMENTAL_SERVED.load(Ordering::Relaxed)
}

/// Full snapshots this node has served.
pub fn full_served() -> u64 {
    FULL_SERVED.load(Ordering::Relaxed)
}

/// Data rows the most recent served snapshot put on the wire.
pub fn rows_served_last() -> u64 {
    ROWS_SERVED_LAST.load(Ordering::Relaxed)
}

/// Total inbound gossip messages processed by this node.
pub fn gossip_recv() -> u64 {
    GOSSIP_RECV.load(Ordering::Relaxed)
}

/// Anti-entropy digests this node has broadcast.
pub fn ae_digest_sent() -> u64 {
    AE_DIGEST_SENT.load(Ordering::Relaxed)
}

/// Anti-entropy repair pulls this node has started.
pub fn ae_repair() -> u64 {
    AE_REPAIR.load(Ordering::Relaxed)
}

/// Snapshots this node has served to peers.
pub fn snapshot_served() -> u64 {
    SNAPSHOT_SERVED.load(Ordering::Relaxed)
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
    AE_DIGEST_SENT.store(0, Ordering::Relaxed);
    AE_REPAIR.store(0, Ordering::Relaxed);
    SNAPSHOT_SERVED.store(0, Ordering::Relaxed);
    INCREMENTAL_SERVED.store(0, Ordering::Relaxed);
    FULL_SERVED.store(0, Ordering::Relaxed);
    ROWS_SERVED_LAST.store(0, Ordering::Relaxed);
}
