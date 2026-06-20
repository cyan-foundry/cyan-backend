//! Application-level anti-entropy / delta repair for live group state.
//!
//! # Why this exists (the substrate gap the stress fabric found)
//!
//! Live deltas ride iroh-gossip, which is **best-effort by design** (HyParView +
//! PlumTree; "eventual delivery acceptable"; iroh 0.95 has no reliable-delivery
//! knob). Under many-peer load some broadcasts are dropped — the receiver sees a
//! `GossipEvent::Lagged` — and are **never re-delivered**. The join-time snapshot
//! catches up a fresh *joiner*, but nothing reconciled *missed live deltas*
//! between already-joined peers, so past N≈8 the mesh plateaued at partial,
//! divergent state (see `STATUS_STRESS_FABRIC.md`).
//!
//! # The fix — the simplest mechanism that converges
//!
//! Each peer, on a bounded + jittered sweep, gossips a **compact per-group state
//! digest**: `(item_count, blake3-hash-of-all-(id,version)-pairs)`. A peer that
//! hears a digest that differs from its own and is **not behind** the sender pulls
//! a snapshot from that sender and merges it. The snapshot serve/apply path already
//! exists (`handle_snapshot_server` + `download_snapshot`) and applies every row as
//! an idempotent upsert-by-id, so a *merge* of the full state is exactly "pull the
//! items I'm missing and apply them" — **no new transfer protocol is invented**.
//!
//! Why this converges: snapshot merges are monotonic (set union), so a peer's state
//! only grows, bounded by the finite union of all peers' state. Any peer not yet
//! equal to that union will, on the next sweep, hear a more-complete (or
//! equal-count-but-divergent) digest and pull toward the union. State strictly
//! increases until every peer equals the union ⇒ the mesh CONVERGES regardless of
//! how many live deltas `Lagged` dropped.
//!
//! Why it's cheap / bounded: the digest is `O(state)` to compute (one hash over the
//! group's rows) and `O(1)` to gossip per sweep per peer — never `O(messages)`. A
//! peer runs at most **one** repair pull at a time (debounced), so the repair
//! traffic is bounded too. `metrics::ae_digest_sent` / `metrics::ae_repair` expose
//! both so the stress suite can assert no storm.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::storage;

/// Default base interval between anti-entropy sweeps. Jittered per-tick (see
/// [`jittered_sweep`]) so peers don't all sweep in lockstep.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(2);

/// Default base window a joiner collects snapshot offers over before picking a
/// holder (see [`jittered_pick_window`]). Short — it only needs to overlap the
/// burst of offers so the random pick has more than one holder to choose from.
pub const PICK_WINDOW: Duration = Duration::from_millis(300);

/// Sweep base interval, with a test-only override via `CYAN_AE_SWEEP_MS`
/// (milliseconds; `0`/unset ⇒ [`SWEEP_INTERVAL`]). The override lets the stress
/// suite drive sweeps fast enough to observe convergence inside a bounded timeout.
pub fn sweep_base() -> Duration {
    match std::env::var("CYAN_AE_SWEEP_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(ms) if ms > 0 => Duration::from_millis(ms),
        _ => SWEEP_INTERVAL,
    }
}

/// A jittered sweep interval: `base + rand(0 ..= base/2)`. Spreads sweeps out so a
/// fleet of peers does not gossip its digest at the same instant.
pub fn jittered_sweep() -> Duration {
    use rand::Rng;
    let base = sweep_base();
    let extra = rand::thread_rng().gen_range(0..=(base.as_millis() as u64 / 2).max(1));
    base + Duration::from_millis(extra)
}

/// Snapshot-offer collection window, with a test-only override via `CYAN_AE_PICK_MS`.
pub fn pick_base() -> Duration {
    match std::env::var("CYAN_AE_PICK_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(ms) if ms > 0 => Duration::from_millis(ms),
        _ => PICK_WINDOW,
    }
}

/// A jittered offer-collection window: `base + rand(0 ..= base)`.
pub fn jittered_pick_window() -> Duration {
    use rand::Rng;
    let base = pick_base();
    let extra = rand::thread_rng().gen_range(0..=(base.as_millis() as u64).max(1));
    base + Duration::from_millis(extra)
}

/// The compact per-group state digest gossiped on each sweep. JSON-tagged `Digest`
/// — disjoint from `SwarmMessage` (`IHave`/`WhoHas`), `NetworkEvent`, and
/// `NetworkCommand`, so the topic's existing parse-dispatch routes it cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AntiEntropyMsg {
    /// "Here is the digest of everything I hold for this group." A receiver that is
    /// not behind the sender but differs pulls a merge snapshot from `node_id`.
    Digest {
        group_id: String,
        node_id: String,
        /// Number of items the sender holds for the group.
        count: u64,
        /// Blake3 hex over the sender's sorted `(kind,id,version)` lines.
        hash: String,
    },
}

/// Compute an `O(state)` content digest of everything group `group_id` holds:
/// `(item_count, blake3_hex)` over the sorted per-item `(kind, id, version)` lines.
///
/// Version columns: `created_at` for immutable rows (group/workspace/board),
/// `updated_at` for mutable rows (elements/cells), `timestamp` for chats, and the
/// content `hash` for files (files are immutable by hash). Two peers with identical
/// state produce identical `(count, hash)`; any divergence flips the hash.
///
/// This is the cheap detector the spec asks for — a hash over current rows, never a
/// scan of the message history.
pub fn group_digest(group_id: &str) -> (u64, String) {
    // Field separator that cannot appear in an id (ids are hex/uuid-like).
    const SEP: char = '\u{1}';
    let mut entries: Vec<String> = Vec::new();

    if let Ok(Some(g)) = storage::group_get(group_id) {
        entries.push(format!("g{SEP}{}{SEP}{}", g.id, g.created_at));
    }

    let workspaces = storage::workspace_list_by_group(group_id).unwrap_or_default();
    let ws_ids: Vec<String> = workspaces.iter().map(|w| w.id.clone()).collect();
    for w in &workspaces {
        entries.push(format!("w{SEP}{}{SEP}{}", w.id, w.created_at));
    }

    let boards = storage::board_list_by_workspaces(&ws_ids).unwrap_or_default();
    let board_ids: Vec<String> = boards.iter().map(|b| b.id.clone()).collect();
    for b in &boards {
        entries.push(format!("b{SEP}{}{SEP}{}", b.id, b.created_at));
    }

    for e in storage::element_list_by_boards(&board_ids).unwrap_or_default() {
        entries.push(format!("e{SEP}{}{SEP}{}", e.id, e.updated_at));
    }
    for c in storage::cell_list_by_boards(&board_ids).unwrap_or_default() {
        entries.push(format!("c{SEP}{}{SEP}{}", c.id, c.updated_at));
    }
    for ch in storage::chat_list_by_workspaces(&ws_ids).unwrap_or_default() {
        entries.push(format!("h{SEP}{}{SEP}{}", ch.id, ch.timestamp));
    }
    for f in storage::file_list_by_group(group_id).unwrap_or_default() {
        entries.push(format!("f{SEP}{}{SEP}{}", f.id, f.hash));
    }

    entries.sort_unstable();
    let count = entries.len() as u64;

    let mut hasher = blake3::Hasher::new();
    for e in &entries {
        hasher.update(e.as_bytes());
        hasher.update(b"\n");
    }
    (count, hasher.finalize().to_hex().to_string())
}
