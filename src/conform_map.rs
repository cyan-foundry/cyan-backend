// cyan-backend/src/conform_map.rs
//
// Timecode remap — proxy_tc ⇄ master_tc through a version's conform plan
// (CYAN_FORMAT_QA gap 1, "the round-2 mis-pin bug").
//
// A review proxy is the master with the version's STRUCTURAL ops applied:
// trim/delete remove master ranges, insert splices foreign frames in, speed
// retimes a range. A Frame.io comment arrives in PROXY coordinates; the ledger
// stores MASTER coordinates (entries anchor to the source asset — the spine). This
// module builds the piecewise frame map between the two from a version's ordered
// ops, plus its inverse. Non-structural ops (level/mute/fade/lift/reframe/color/
// slip/swap/markers/notes) never move frames — lift blanks its range IN PLACE
// (duration kept), matching the cyan-media renderer. A version with none of the
// structural verbs maps identity, which is why round 1 "works" without a remap and
// round 2+ silently mis-pins.
//
// Pure functions over `ConformOp` — no I/O; `for_version` is the one thin DB read
// (the version's frozen plan) so the sensor leg can remap with a version id.

use crate::changelist::{self, ConformOp};
use anyhow::{anyhow, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

/// The ops that move frames between master and proxy. Everything else in the
/// closed vocab decorates frames in place (identity for the map).
///
/// `lift` is deliberately NOT here (WOW-2 verification finding, 2026-07-08):
/// true NLE lift semantics — and the cyan-media renderer's tested behavior —
/// KEEP the timeline duration and blank the range in place (black + silence);
/// only delete/extract ripples. A map that dropped lifted frames disagreed
/// with the rendered pixels by exactly the lifted length, mis-pinning every
/// anchor after the lift point. `slip` is likewise identity: the renderer
/// refuses it (needs_manual) until real per-range slip assembly exists.
pub const STRUCTURAL_OPS: &[&str] = &["trim", "delete", "insert", "speed"];

/// One piece of the piecewise-linear map: proxy frames `[proxy_start, proxy_end)`
/// (`proxy_end == None` = the unbounded tail) cover master frames starting at
/// `master_start`, advancing `ratio` master frames per proxy frame (`ratio == 1.0`
/// outside retimes). `master_start == None` marks inserted FOREIGN media — those
/// proxy frames have no master coordinates at all.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MapSegment {
    pub proxy_start: i64,
    pub proxy_end: Option<i64>,
    pub master_start: Option<i64>,
    pub ratio: f64,
}

/// The proxy ⇄ master frame map for one version — ordered, non-overlapping
/// segments covering proxy tc 0..∞.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConformMap {
    pub segments: Vec<MapSegment>,
}

impl ConformMap {
    /// True when no structural op moved any frame — proxy tc IS master tc.
    pub fn is_identity(&self) -> bool {
        matches!(
            self.segments.as_slice(),
            [MapSegment {
                proxy_start: 0,
                proxy_end: None,
                master_start: Some(0),
                ratio,
            }] if *ratio == 1.0
        )
    }

    /// proxy frame → master frame. `None` when the proxy frame is inserted foreign
    /// media (no master coordinates) or negative.
    pub fn proxy_to_master(&self, proxy_tc: i64) -> Option<i64> {
        for s in &self.segments {
            let inside =
                proxy_tc >= s.proxy_start && s.proxy_end.is_none_or(|e| proxy_tc < e);
            if inside {
                return s
                    .master_start
                    .map(|m| m + (((proxy_tc - s.proxy_start) as f64) * s.ratio).round() as i64);
            }
        }
        None
    }

    /// master frame → proxy frame (the inverse). `None` when the master frame was
    /// removed from the proxy (trim/lift/delete) or negative. Under a retime the
    /// inverse is lossy at sub-ratio granularity (several master frames land on one
    /// proxy frame) — nearest proxy frame is returned.
    pub fn master_to_proxy(&self, master_tc: i64) -> Option<i64> {
        for s in &self.segments {
            let Some(ms) = s.master_start else { continue };
            let master_end = s
                .proxy_end
                .map(|pe| ms + (((pe - s.proxy_start) as f64) * s.ratio).round() as i64);
            if master_tc >= ms && master_end.is_none_or(|e| master_tc < e) {
                return Some(
                    s.proxy_start + (((master_tc - ms) as f64) / s.ratio).round() as i64,
                );
            }
        }
        None
    }
}

/// Build the proxy ⇄ master map from a version's ORDERED ops (the conform plan).
/// Structural ops contribute; everything else is identity. All op coordinates are
/// MASTER frames (entries anchor to the source asset), so removals/retimes/inserts
/// compose by position, not by application order.
pub fn build(ops: &[ConformOp]) -> ConformMap {
    // Collect the structural facts in master coordinates.
    let mut removals: Vec<(i64, i64)> = Vec::new(); // [start, end) dropped from the proxy
    let mut speeds: Vec<(i64, i64, f64)> = Vec::new(); // [start, end) plays at ratio
    let mut inserts: Vec<(i64, i64)> = Vec::new(); // (master position, foreign frames)

    for op in ops {
        match op.op.as_str() {
            "trim" => {
                let frames = op.params.get("frames").and_then(|v| v.as_i64()).unwrap_or(0);
                if frames <= 0 {
                    continue;
                }
                let edge = op.params.get("edge").and_then(|v| v.as_str()).unwrap_or("head");
                if edge == "tail" {
                    // Needs a range end to trim from; a malformed tail trim is a no-op.
                    if let Some(out) = op.tc_out {
                        removals.push(((out - frames).max(op.tc_in), out));
                    }
                } else {
                    removals.push((op.tc_in, op.tc_in + frames));
                }
            }
            // delete RIPPLES — the rendered proxy drops the range. lift does
            // NOT: cyan-media blanks the range IN PLACE (black + silence,
            // duration kept — true NLE lift), so lift stays identity here or
            // every anchor after the lift point mis-pins by the lift length.
            "delete" => {
                if let Some(out) = op.tc_out
                    && out > op.tc_in
                {
                    removals.push((op.tc_in, out));
                }
            }
            "insert" => {
                let at = op.params.get("at").and_then(|v| v.as_i64()).unwrap_or(op.tc_in);
                let frames = op.params.get("frames").and_then(|v| v.as_i64()).unwrap_or(0);
                if frames > 0 {
                    inserts.push((at, frames));
                }
            }
            "speed" => {
                let ratio = op.params.get("ratio").and_then(|v| v.as_f64()).unwrap_or(1.0);
                if ratio > 0.0
                    && ratio != 1.0
                    && let Some(out) = op.tc_out
                    && out > op.tc_in
                {
                    speeds.push((op.tc_in, out, ratio));
                }
            }
            _ => {} // non-structural — identity
        }
    }

    // Merge overlapping removals into disjoint ranges.
    removals.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(removals.len());
    for (a, b) in removals {
        match merged.last_mut() {
            Some((_, end)) if a <= *end => *end = (*end).max(b),
            _ => merged.push((a, b)),
        }
    }

    // Boundary points on the master axis: every place the mapping can change.
    let mut bounds: Vec<i64> = vec![0];
    bounds.extend(merged.iter().flat_map(|&(a, b)| [a, b]));
    bounds.extend(speeds.iter().flat_map(|&(a, b, _)| [a, b]));
    bounds.extend(inserts.iter().map(|&(at, _)| at));
    bounds.retain(|&b| b >= 0);
    bounds.sort_unstable();
    bounds.dedup();

    // Walk the master axis, accumulating proxy position.
    let mut segments: Vec<MapSegment> = Vec::new();
    let mut proxy = 0i64;
    for (i, &m0) in bounds.iter().enumerate() {
        // Foreign frames spliced in at this master position (in op order).
        for &(at, frames) in &inserts {
            if at == m0 {
                segments.push(MapSegment {
                    proxy_start: proxy,
                    proxy_end: Some(proxy + frames),
                    master_start: None,
                    ratio: 1.0,
                });
                proxy += frames;
            }
        }

        // Boundaries include every removal edge, so the interval [m0, m1) is fully
        // inside or fully outside each removal — membership of m0 decides.
        let m1 = bounds.get(i + 1).copied();
        if merged.iter().any(|&(a, b)| a <= m0 && m0 < b) {
            continue; // this master range is removed — no proxy frames
        }
        // Boundaries include every speed edge, so the interval is fully inside or
        // fully outside each retime; the LAST op to claim it wins.
        let ratio = speeds
            .iter()
            .rev()
            .find(|&&(a, b, _)| a <= m0 && m0 < b)
            .map(|&(_, _, r)| r)
            .unwrap_or(1.0);

        match m1 {
            Some(m1) => {
                let proxy_len = (((m1 - m0) as f64) / ratio).round() as i64;
                if proxy_len > 0 {
                    segments.push(MapSegment {
                        proxy_start: proxy,
                        proxy_end: Some(proxy + proxy_len),
                        master_start: Some(m0),
                        ratio,
                    });
                    proxy += proxy_len;
                }
            }
            None => {
                // The unbounded tail — past every op, always ratio 1.
                segments.push(MapSegment {
                    proxy_start: proxy,
                    proxy_end: None,
                    master_start: Some(m0),
                    ratio: 1.0,
                });
            }
        }
    }

    ConformMap { segments }
}

/// The map for a version's frozen conform plan — the sensor leg's entry point:
/// Frame.io file id → proxy asset → `derived_from_version` → this map → master
/// coordinates for the ledger entry.
pub fn for_version(conn: &Connection, tenant_id: &str, version_id: &str) -> Result<ConformMap> {
    Ok(build(&changelist::conform_plan(conn, tenant_id, version_id)?))
}

/// Remap an observation made in PROXY coordinates into MASTER coordinates, and
/// stamp the raw observation into `params.observed` — `{proxy_ref, tc_in[, tc_out]}`.
/// The entry is then appended with the returned master coords + params, so the
/// observation is inside the content hash (re-verifiable provenance, never lost to
/// a lifecycle move). Errors if the observed frame is inserted foreign media —
/// there ARE no master coordinates to store, which must surface, not guess.
pub fn remap_observed(
    map: &ConformMap,
    proxy_ref: &str,
    proxy_tc_in: i64,
    proxy_tc_out: Option<i64>,
    mut params: serde_json::Value,
) -> Result<(i64, Option<i64>, serde_json::Value)> {
    let master_in = map.proxy_to_master(proxy_tc_in).ok_or_else(|| {
        anyhow!("proxy tc {} has no master coordinates (inserted media)", proxy_tc_in)
    })?;
    let master_out = match proxy_tc_out {
        Some(p) => Some(map.proxy_to_master(p).ok_or_else(|| {
            anyhow!("proxy tc {} has no master coordinates (inserted media)", p)
        })?),
        None => None,
    };
    if !params.is_object() {
        params = serde_json::json!({});
    }
    let mut observed = serde_json::json!({ "proxy_ref": proxy_ref, "tc_in": proxy_tc_in });
    if let Some(out) = proxy_tc_out {
        observed["tc_out"] = serde_json::json!(out);
    }
    params["observed"] = observed;
    Ok((master_in, master_out, params))
}
