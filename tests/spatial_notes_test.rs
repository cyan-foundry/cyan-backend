// tests/spatial_notes_test.rs
//
// The REVIEW_WAIST_SPEC §5 acceptance suite, by name. This IS the contract for
// spatial notes: the `ref`/`region`/`intent_struct` groups, and the resolution
// rules that turn a referent into timeline pins on any version.
//
// No network, no real peer, no Resolve, no Frame.io: all cut knowledge arrives
// through the `FrameMapProvider` / `LineageProvider` seams, backed here by
// fixture maps (including dissolves, retimes and lineage edges).
//
// Fixture (spec §5): sources A, B, C; v1 = A[0..100] B[0..80] C[0..120];
// a region note on B@40 and a junction note at A→B, both authored on v1.

use cyan_backend::changelist::{self, compute_entry_hash, ChangeEntry};
use cyan_backend::spatial::{
    self, Boundary, CaptureCtx, DominantHit, EntryRef, ExportScope, ExtentHint, FrameMapProvider,
    IntentStruct, LineageEdge, LineageFrameMap, LineageProvider, Occurrence, RasterRef, Region,
    Resolution, Shape, SideFrame,
};
use rusqlite::Connection;

// ============================================================================
// Fixture cut model — the fake FrameMapProvider.
// ============================================================================

/// One clip on the fixture timeline. `num`/`den` are SOURCE frames per TIMELINE
/// frame: `1/2` is a 50% retime (slow-mo — one source frame occupies two
/// timeline frames). `dissolve_in` is the overlap, in frames, with the segment
/// before it.
#[derive(Debug, Clone)]
struct Segment {
    asset: &'static str,
    src_in: i64,
    src_out: i64, // exclusive
    num: i64,
    den: i64,
    dissolve_in: Option<i64>,
}

impl Segment {
    fn plain(asset: &'static str, src_in: i64, src_out: i64) -> Self {
        Segment { asset, src_in, src_out, num: 1, den: 1, dissolve_in: None }
    }
    fn retimed(asset: &'static str, src_in: i64, src_out: i64, num: i64, den: i64) -> Self {
        Segment { asset, src_in, src_out, num, den, dissolve_in: None }
    }
    fn with_dissolve(mut self, frames: i64) -> Self {
        self.dissolve_in = Some(frames);
        self
    }
    /// Timeline length: source length scaled by the retime.
    fn tl_len(&self) -> i64 {
        (self.src_out - self.src_in) * self.den / self.num
    }
}

/// A named version's cut, as an ordered segment list.
#[derive(Debug, Clone)]
struct Cut {
    segments: Vec<Segment>,
}

impl Cut {
    /// Timeline start of each segment, in order.
    fn starts(&self) -> Vec<i64> {
        let mut out = Vec::new();
        let mut t = 0;
        for s in &self.segments {
            out.push(t);
            t += s.tl_len();
        }
        out
    }
}

/// The fixture `FrameMapProvider`: a map of version id → cut.
struct FixtureFrames {
    cuts: Vec<(&'static str, Cut)>,
}

impl FixtureFrames {
    fn get(&self, version: &str) -> Option<&Cut> {
        self.cuts.iter().find(|(v, _)| *v == version).map(|(_, c)| c)
    }
}

impl FrameMapProvider for FixtureFrames {
    fn occurrences(&self, version: &str, asset_hash: &str, src_frame: i64) -> Vec<Occurrence> {
        let Some(cut) = self.get(version) else {
            return Vec::new();
        };
        let starts = cut.starts();
        let mut out = Vec::new();
        for (i, seg) in cut.segments.iter().enumerate() {
            if seg.asset != asset_hash || src_frame < seg.src_in || src_frame >= seg.src_out {
                continue;
            }
            // FIRST timeline frame of this source frame within this segment —
            // a slow-mo source frame occupies several, and the pin takes the first.
            let tl = starts[i] + (src_frame - seg.src_in) * seg.den / seg.num;
            out.push(Occurrence {
                segment_id: format!("{version}:{i}"),
                tl_frame: tl,
                src_frame,
            });
        }
        out
    }

    fn boundaries(&self, version: &str) -> Vec<Boundary> {
        let Some(cut) = self.get(version) else {
            return Vec::new();
        };
        let starts = cut.starts();
        let mut out = Vec::new();
        for (i, pair) in cut.segments.windows(2).enumerate() {
            let (prev, cur) = (&pair[0], &pair[1]);
            out.push(Boundary {
                tl_frame: starts[i + 1],
                out: SideFrame { asset_hash: prev.asset.to_string(), frame: prev.src_out - 1 },
                incoming: SideFrame { asset_hash: cur.asset.to_string(), frame: cur.src_in },
                transition_frames: cur.dissolve_in,
            });
        }
        out
    }

    fn dominant_at(&self, version: &str, tl_frame: i64) -> Option<DominantHit> {
        let cut = self.get(version)?;
        let starts = cut.starts();
        let idx = (0..cut.segments.len())
            .find(|&i| tl_frame >= starts[i] && tl_frame < starts[i] + cut.segments[i].tl_len())?;
        let seg = &cut.segments[idx];
        let into_seg = tl_frame - starts[idx];
        let src = seg.src_in + into_seg * seg.num / seg.den;

        // Inside a dissolve overlap, one timeline frame maps to TWO sources.
        // Capture anchors to the dominant-mix source: the outgoing shot holds the
        // mix until the midpoint, the incoming one after it.
        if let (Some(t), true) = (seg.dissolve_in, idx > 0)
            && into_seg < seg.dissolve_in.unwrap_or(0)
        {
            let prev = &cut.segments[idx - 1];
            // The outgoing tail runs under the head of this segment.
            let prev_src = prev.src_out - 1 - (t - 1 - into_seg);
            let incoming_dominant = into_seg * 2 >= t;
            return Some(if incoming_dominant {
                DominantHit {
                    asset_hash: seg.asset.to_string(),
                    src_frame: src,
                    other: Some(SideFrame {
                        asset_hash: prev.asset.to_string(),
                        frame: prev_src,
                    }),
                    transition_frames: Some(t),
                }
            } else {
                DominantHit {
                    asset_hash: prev.asset.to_string(),
                    src_frame: prev_src,
                    other: Some(SideFrame { asset_hash: seg.asset.to_string(), frame: src }),
                    transition_frames: Some(t),
                }
            });
        }
        Some(DominantHit {
            asset_hash: seg.asset.to_string(),
            src_frame: src,
            other: None,
            transition_frames: None,
        })
    }
}

/// The fixture `LineageProvider`: declared (old → new) essence edges.
#[derive(Default)]
struct FixtureLineage {
    edges: Vec<(&'static str, LineageEdge)>,
}

impl FixtureLineage {
    fn with(mut self, from: &'static str, to: &str, map: Option<LineageFrameMap>) -> Self {
        self.edges
            .push((from, LineageEdge { to: to.to_string(), frame_map: map }));
        self
    }
}

impl LineageProvider for FixtureLineage {
    fn children(&self, asset_hash: &str) -> Vec<LineageEdge> {
        self.edges
            .iter()
            .filter(|(f, _)| *f == asset_hash)
            .map(|(_, e)| e.clone())
            .collect()
    }
}

/// No lineage declared anywhere — the walk must never guess.
struct NoLineage;
impl LineageProvider for NoLineage {
    fn children(&self, _: &str) -> Vec<LineageEdge> {
        Vec::new()
    }
}

// ============================================================================
// The spec §5 fixture versions.
// ============================================================================

fn frames() -> FixtureFrames {
    FixtureFrames {
        cuts: vec![
            // v1 — the authored cut.
            ("v1", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::plain("B", 0, 80),
                Segment::plain("C", 0, 120),
            ]}),
            // v2 — trims 8 off A's head.
            ("v2", Cut { segments: vec![
                Segment::plain("A", 8, 100),
                Segment::plain("B", 0, 80),
                Segment::plain("C", 0, 120),
            ]}),
            // v3 — reorders to B A C.
            ("v3", Cut { segments: vec![
                Segment::plain("B", 0, 80),
                Segment::plain("A", 0, 100),
                Segment::plain("C", 0, 120),
            ]}),
            // v4 — retimes B to 50% (one source frame per two timeline frames).
            ("v4", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::retimed("B", 0, 80, 1, 2),
                Segment::plain("C", 0, 120),
            ]}),
            // v7 — composes all three: reorder, A head-trim, B at 50%.
            ("v7", Cut { segments: vec![
                Segment::retimed("B", 0, 80, 1, 2),
                Segment::plain("A", 8, 100),
                Segment::plain("C", 0, 120),
            ]}),
            // v2-swap — B replaced by the re-graded B-prime (same duration).
            ("v2swap", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::plain("Bprime", 0, 80),
                Segment::plain("C", 0, 120),
            ]}),
            // v2-retimed-rerender — B replaced by B-double-prime at 2× the frames.
            ("v2retime", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::plain("Bdprime", 0, 160),
                Segment::plain("C", 0, 120),
            ]}),
            // v2-drop — B is gone from the cut entirely.
            ("v2drop", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::plain("C", 0, 120),
            ]}),
            // v9 — B restored.
            ("v9", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::plain("B", 0, 80),
                Segment::plain("C", 0, 120),
            ]}),
            // v2-tail — A's TAIL extended by 6: the A→B cut moved +6.
            ("v2tail", Cut { segments: vec![
                Segment::plain("A", 0, 106),
                Segment::plain("B", 0, 80),
                Segment::plain("C", 0, 120),
            ]}),
            // intercut — A B A B: the A→B junction resolves at BOTH boundaries.
            ("vintercut", Cut { segments: vec![
                Segment::plain("A", 0, 50),
                Segment::plain("B", 0, 40),
                Segment::plain("A", 50, 100),
                Segment::plain("B", 40, 80),
            ]}),
            // v4-noadj — reorder breaks A→B adjacency entirely (B C A).
            ("v4noadj", Cut { segments: vec![
                Segment::plain("B", 0, 80),
                Segment::plain("C", 0, 120),
                Segment::plain("A", 0, 100),
            ]}),
            // v5 — adjacency restored.
            ("v5", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::plain("B", 0, 80),
            ]}),
            // vdissolve — a 12-frame A⤬B dissolve.
            ("vdissolve", Cut { segments: vec![
                Segment::plain("A", 0, 100),
                Segment::plain("B", 0, 80).with_dissolve(12),
            ]}),
            // vslowmo — B at 50%, so B@40 occupies two timeline frames.
            ("vslowmo", Cut { segments: vec![
                Segment::retimed("B", 0, 80, 1, 2),
            ]}),
            // vrepeat — the SAME B range used twice (repeated source range).
            ("vrepeat", Cut { segments: vec![
                Segment::plain("B", 0, 80),
                Segment::plain("C", 0, 20),
                Segment::plain("B", 0, 80),
            ]}),
        ],
    }
}

/// The authored region note: B@40, a rect drawn on a 960×540 proxy.
fn region_ref() -> EntryRef {
    EntryRef::source("B", 40, None)
}

fn a_region() -> Region {
    Region {
        key_frame: 40,
        shape: Shape {
            kind: "rect".to_string(),
            // A rect from (0.25, 0.30) to (0.55, 0.70), fixed-point ×1e6.
            points: vec![[250_000, 300_000], [550_000, 700_000]],
        },
        raster_ref: RasterRef { w: 960, h: 540 },
        extent_hint: None,
    }
}

/// The authored junction note: the A→B seam of v1.
fn junction_ref() -> EntryRef {
    EntryRef::junction(
        SideFrame { asset_hash: "A".to_string(), frame: 99 },
        SideFrame { asset_hash: "B".to_string(), frame: 0 },
        None,
    )
}

fn pins_of(r: &Resolution) -> Vec<(String, i64, i64)> {
    match r {
        Resolution::OnCut { pins } => pins
            .iter()
            .map(|p| (p.asset_hash.clone(), p.tl_frame, p.src_frame))
            .collect(),
        _ => panic!("expected OnCut, got {r:?}"),
    }
}

// ============================================================================
// T-V7-SOURCE — the B@40 note renders on B frame 40 in EVERY version.
// ============================================================================

#[test]
fn t_v7_source() {
    let fm = frames();
    let lin = NoLineage;
    let r = region_ref();

    // v1: B starts at timeline 100, so B@40 lands at 140.
    assert_eq!(
        pins_of(&spatial::resolve(&r, None, "v1", &fm, &lin)),
        vec![("B".to_string(), 140, 40)]
    );
    // v2: 8 frames trimmed off A's head shifts B back to 92 → 132.
    assert_eq!(
        pins_of(&spatial::resolve(&r, None, "v2", &fm, &lin)),
        vec![("B".to_string(), 132, 40)]
    );
    // v3: B is now FIRST — the note follows the content, not the position.
    assert_eq!(
        pins_of(&spatial::resolve(&r, None, "v3", &fm, &lin)),
        vec![("B".to_string(), 40, 40)]
    );
    // v4: B at 50% — source frame 40 is timeline frame 100 + 80 = 180. The pin
    // takes the FIRST timeline frame of the occurrence (spec §1).
    assert_eq!(
        pins_of(&spatial::resolve(&r, None, "v4", &fm, &lin)),
        vec![("B".to_string(), 180, 40)]
    );
    // v7: all three composed — B first, at 50% → 80.
    assert_eq!(
        pins_of(&spatial::resolve(&r, None, "v7", &fm, &lin)),
        vec![("B".to_string(), 80, 40)]
    );
}

#[test]
fn t_v7_source_slowmo_pins_at_first_frame_of_the_occurrence() {
    let fm = frames();
    // B@40 at 50% occupies timeline frames 80 AND 81; the pin is the first, and
    // the region is valid at both.
    assert_eq!(
        pins_of(&spatial::resolve(&region_ref(), None, "vslowmo", &fm, &NoLineage)),
        vec![("B".to_string(), 80, 40)]
    );
}

#[test]
fn t_v7_source_repeated_range_pins_at_every_occurrence() {
    let fm = frames();
    // A repeated source range pins at EVERY occurrence (spec §1).
    let r = spatial::resolve(&region_ref(), None, "vrepeat", &fm, &NoLineage);
    assert_eq!(
        pins_of(&r),
        vec![("B".to_string(), 40, 40), ("B".to_string(), 140, 40)]
    );
    // The capture-context hint marks the AUTHORED one — display only.
    let ctx = CaptureCtx { occurrence_hint: Some(1), ..Default::default() };
    let hinted = spatial::resolve(&region_ref(), Some(&ctx), "vrepeat", &fm, &NoLineage);
    match hinted {
        Resolution::OnCut { pins } => {
            assert!(!pins[0].authored, "occurrence 0 is not the authored one");
            assert!(pins[1].authored, "the hint marks occurrence 1");
        }
        other => panic!("expected OnCut, got {other:?}"),
    }
}

// ============================================================================
// T-RERENDER-FOLLOW — the lineage walk, the most common versioning event in post.
// ============================================================================

#[test]
fn t_rerender_follow() {
    let fm = frames();
    // B swapped for the re-graded B′ (new essence, NEW Blake3, same duration).
    let lin = FixtureLineage::default().with("B", "Bprime", None);
    let r = spatial::resolve(&region_ref(), None, "v2swap", &fm, &lin);
    match &r {
        Resolution::OnCut { pins } => {
            assert_eq!(pins.len(), 1);
            assert_eq!(pins[0].asset_hash, "Bprime");
            assert_eq!(pins[0].src_frame, 40, "identity frame map across a re-grade");
            assert_eq!(pins[0].tl_frame, 140);
            assert!(pins[0].via_lineage, "reached through the lineage walk");
        }
        other => panic!("the note must NOT be latent — it followed the re-grade: {other:?}"),
    }
}

#[test]
fn t_rerender_follow_retimed_rerender_uses_the_declared_frame_map() {
    let fm = frames();
    // B″ is a retimed re-render at 2× the frames — the edge declares the map.
    let lin = FixtureLineage::default().with(
        "B",
        "Bdprime",
        Some(LineageFrameMap { scale_num: 2, scale_den: 1, offset: 0 }),
    );
    let r = spatial::resolve(&region_ref(), None, "v2retime", &fm, &lin);
    match &r {
        Resolution::OnCut { pins } => {
            assert_eq!(pins[0].asset_hash, "Bdprime");
            assert_eq!(pins[0].src_frame, 80, "40 mapped through 2/1");
            assert!(pins[0].via_lineage);
        }
        other => panic!("expected OnCut through the declared map, got {other:?}"),
    }
}

#[test]
fn t_rerender_follow_without_a_lineage_edge_goes_latent() {
    let fm = frames();
    // No edge declared: the walk NEVER guesses that Bprime is B.
    assert!(
        matches!(
            spatial::resolve(&region_ref(), None, "v2swap", &fm, &NoLineage),
            Resolution::Latent { .. }
        ),
        "with no declared lineage the note must go latent, not guess"
    );
}

// ============================================================================
// T-V7-JUNCTION — the seam, not either shot.
// ============================================================================

#[test]
fn t_v7_junction() {
    let fm = frames();
    let lin = NoLineage;
    let j = junction_ref();

    // v1 — the authored boundary, zero drift.
    match spatial::resolve(&j, None, "v1", &fm, &lin) {
        Resolution::Junction { pins } => {
            assert_eq!(pins.len(), 1);
            assert_eq!(pins[0].tl_frame, 100);
            assert_eq!(pins[0].drift, 0);
        }
        other => panic!("expected Junction, got {other:?}"),
    }

    // v2tail — A's tail extended by 6: "the cut moved +6 since this note".
    match spatial::resolve(&j, None, "v2tail", &fm, &lin) {
        Resolution::Junction { pins } => {
            assert_eq!(pins.len(), 1);
            assert_eq!(pins[0].drift, 6, "drift is computed from the authored frames");
            assert_eq!(pins[0].tl_frame, 106);
        }
        other => panic!("expected Junction, got {other:?}"),
    }
}

#[test]
fn t_v7_junction_intercut_resolves_at_both_boundaries() {
    let fm = frames();
    // The A-B-A-B intercut is normal: the junction resolves at BOTH A→B seams.
    let ctx = CaptureCtx { occurrence_hint: Some(0), ..Default::default() };
    match spatial::resolve(&junction_ref(), Some(&ctx), "vintercut", &fm, &NoLineage) {
        Resolution::Junction { pins } => {
            assert_eq!(pins.len(), 2, "both A→B boundaries resolve");
            assert_eq!(pins[0].tl_frame, 50);
            assert_eq!(pins[1].tl_frame, 140);
            assert!(pins[0].authored, "the hint marks the authored boundary");
            assert!(!pins[1].authored);
        }
        other => panic!("expected two Junction pins, got {other:?}"),
    }
}

#[test]
fn t_v7_junction_broken_adjacency_goes_latent_then_resurfaces() {
    let fm = frames();
    // B C A: A and B are nowhere adjacent → latent.
    assert!(
        matches!(
            spatial::resolve(&junction_ref(), None, "v4noadj", &fm, &NoLineage),
            Resolution::Latent { .. }
        ),
        "a junction with no A→B boundary anywhere is latent"
    );
    // v5 restores adjacency → the note resurfaces. No data changed in between.
    assert!(
        matches!(
            spatial::resolve(&junction_ref(), None, "v5", &fm, &NoLineage),
            Resolution::Junction { .. }
        ),
        "restoring adjacency resurfaces the junction note"
    );
}

// ============================================================================
// T-OFFCUT-LATENT — listed, queryable, never orphaned; no data change.
// ============================================================================

#[test]
fn t_offcut_latent() {
    let conn = db();
    let fm = frames();

    let stored = append_region_note(&conn, "tenant-1");
    let hash_before = stored.entry_hash.clone();

    // v2drop removes B: the note is LATENT — listed under "notes on removed
    // material", never deleted.
    let r = spatial::resolve(&region_ref(), None, "v2drop", &fm, &NoLineage);
    assert!(matches!(r, Resolution::Latent { .. }), "expected Latent, got {r:?}");

    // Still queryable while latent — latency is a render-time answer, not a
    // storage state.
    let view = changelist::get(&conn, "tenant-1", "master-1", "main").expect("get");
    assert_eq!(view.entries.len(), 1, "the latent note is still in the list");

    // v9 restores B: on-cut again, at the same source frame.
    assert_eq!(
        pins_of(&spatial::resolve(&region_ref(), None, "v9", &fm, &NoLineage)),
        vec![("B".to_string(), 140, 40)]
    );

    // NO DATA CHANGE at any point — the entry never moved.
    let after = changelist::get_entry(&conn, "tenant-1", &stored.id).expect("entry");
    assert_eq!(after.entry_hash, hash_before, "going latent must not touch the entry");
    assert_eq!(after.referent, stored.referent);
    assert_eq!(after.region, stored.region);
}

// ============================================================================
// T-DISSOLVE-CAPTURE — the dominant-mix source, and the one-gesture flip.
// ============================================================================

#[test]
fn t_dissolve_capture() {
    let fm = frames();
    // vdissolve: A[0..100], then B with a 12-frame overlap starting at tl 100.
    // Two frames in (tl 102) the OUTGOING A still holds the mix.
    let early = fm.dominant_at("vdissolve", 102).expect("a frame inside the dissolve");
    assert_eq!(early.asset_hash, "A", "before the midpoint the outgoing shot dominates");
    assert_eq!(early.transition_frames, Some(12));
    // The flip gesture re-anchors to the OTHER source.
    let other = early.other.clone().expect("the flip target");
    assert_eq!(other.asset_hash, "B");

    // Ten frames in (tl 110), past the midpoint, the INCOMING shot dominates.
    let late = fm.dominant_at("vdissolve", 110).expect("a frame inside the dissolve");
    assert_eq!(late.asset_hash, "B", "past the midpoint the incoming shot dominates");
    assert_eq!(late.other.expect("flip target").asset_hash, "A");

    // The second flip target is the JUNCTION — usually the referent the reviewer
    // means — and it records `transition.frames`.
    let jref = EntryRef::junction(
        SideFrame { asset_hash: "A".to_string(), frame: 99 },
        SideFrame { asset_hash: "B".to_string(), frame: 0 },
        Some(12),
    );
    match spatial::resolve(&jref, None, "vdissolve", &fm, &NoLineage) {
        Resolution::Junction { pins } => {
            assert_eq!(pins.len(), 1);
            assert_eq!(pins[0].tl_frame, 100);
        }
        other => panic!("the junction flip must resolve, got {other:?}"),
    }
    assert_eq!(
        jref.junction.as_ref().and_then(|j| j.transition_frames),
        Some(12),
        "the transition duration is recorded on the ref"
    );
}

// ============================================================================
// T-REGION-RASTER — drawn on a proxy, valid on the master.
// ============================================================================

#[test]
fn t_region_raster() {
    let region = a_region();
    // Drawn on 960×540; the stored coords are normalized, so the SAME region
    // overlays correctly at 3840×2160 with no rescaling of stored data.
    let hd = region.project(&RasterRef { w: 960, h: 540 });
    let uhd = region.project(&RasterRef { w: 3840, h: 2160 });
    assert_eq!(hd, vec![(240.0, 162.0), (528.0, 378.0)]);
    assert_eq!(uhd, vec![(960.0, 648.0), (2112.0, 1512.0)]);
    // Exactly 4× in each axis — the mapping is raster-independent.
    for (a, b) in hd.iter().zip(uhd.iter()) {
        assert!((b.0 - a.0 * 4.0).abs() < 1e-9);
        assert!((b.1 - a.1 * 4.0).abs() < 1e-9);
    }
    // `raster_ref` pins what it was drawn on, so an anamorphic/letterboxed proxy
    // is distinguishable from a square-pixel one carrying the same coords.
    assert_eq!(region.raster_ref, RasterRef { w: 960, h: 540 });
    let anamorphic = Region { raster_ref: RasterRef { w: 1920, h: 817 }, ..a_region() };
    assert_ne!(
        anamorphic.raster_ref, region.raster_ref,
        "the raster the shape was drawn on is part of the datum"
    );
    // ... and it is HASHED content: the same coords on a different raster are a
    // different note, because they mean a different place.
    assert_ne!(
        compute_entry_hash(&entry_with(Some(region_ref()), Some(region.clone()), None, None)),
        compute_entry_hash(&entry_with(Some(region_ref()), Some(anamorphic), None, None)),
    );
}

// ============================================================================
// T-SEED-UNCONFIRMED — a seed, not a window. Confirmation is lifecycle.
// ============================================================================

#[test]
fn t_seed_unconfirmed() {
    let region = a_region();
    // The Resolve projection carries marker + geometry in `customData` — and
    // nothing that implies a window.
    let payload = serde_json::to_value(&region).expect("serialize");
    let obj = payload.as_object().expect("object");
    for forbidden in ["confirmed", "tracked", "motion_path", "track", "window"] {
        assert!(
            !obj.contains_key(forbidden),
            "a region must carry no `{forbidden}` — it is a seed by definition of the format"
        );
    }
    // The only temporal field is the reviewer's own scrub gesture, and it is
    // explicitly NOT a tracked extent.
    let with_hint = Region {
        extent_hint: Some(ExtentHint { frame_in: 30, frame_out: 60 }),
        ..a_region()
    };
    assert_eq!(with_hint.key_frame, 40, "exactness lives at key_frame only");

    // Confirmation exists ONLY as a later ledger fact referencing the note —
    // never as a mutation of the authored entry.
    let conn = db();
    let note = append_region_note(&conn, "tenant-1");
    let confirm = changelist::append(
        &conn,
        "master-1",
        "main",
        ChangeEntry {
            referent: Some(EntryRef::entry(&note.entry_hash)),
            intent: "colorist placed and confirmed the window".to_string(),
            ..base_entry("tenant-1", "note", None)
        },
    )
    .expect("append the confirm fact");

    // Two rows: the authored seed is untouched.
    assert_eq!(
        changelist::get_entry(&conn, "tenant-1", &note.id).expect("seed").entry_hash,
        note.entry_hash,
        "realization must never mutate the authored entry"
    );
    assert_eq!(
        confirm.referent.as_ref().and_then(|r| r.about_entry_hash.as_deref()),
        Some(note.entry_hash.as_str()),
        "the confirm fact references the seed by content hash"
    );
    assert_ne!(confirm.entry_hash, note.entry_hash);
}

// ============================================================================
// T-HASH-BACKCOMPAT — THE LAW. Every pre-region entry hashes IDENTICALLY.
// ============================================================================

/// Golden `entry_hash` values captured by running `compute_entry_hash` at commit
/// ea45a25 — BEFORE the `ref`/`region`/`intent_struct` fields existed. If any of
/// these moves, the canonical serialization changed and the format is broken:
/// every entry_hash on every shipped device, and every `list_hash`/`cut_hash`
/// built from them, would be invalidated. Do not "update" these to make a test
/// pass.
const GOLDEN: &[(&str, &str)] = &[
    ("note_plain", "fc3b517b8850f19bc6530310ceace799e8b7923c4d682ebfd717c84beaf350ea"),
    ("op_trim", "5a6bd0b8ab4c65082951c385f1122518af0e9332bfa43acaed44b64ba0e982b0"),
    ("marker", "bdc2fa639d7d3aedd09ca3754085ddec740573a4a18704ac9c9ae739d7061230"),
    ("note_no_optionals", "23c166ed31f69925210bb0171c6beee500cd238e257c496d6179ca53149d02b0"),
    ("op_with_depends", "fcf6bc7d1bd3ac33f59ecbebab4d93c3c320fdc366eabc1981f495b4bbb2bd24"),
];

/// The exact fixtures the goldens were computed from.
fn golden_fixture(name: &str) -> ChangeEntry {
    let base = |kind: &str, op: Option<&str>, tc_in: i64, intent: &str| ChangeEntry {
        id: "fixed-id-ignored".to_string(),
        entry_hash: String::new(),
        asset_hash: "asset-A-blake3".to_string(),
        tenant_id: "tenant-1".to_string(),
        branch: Some("main".to_string()),
        track: Some("V1".to_string()),
        tc_in,
        tc_out: Some(tc_in + 24),
        kind: kind.to_string(),
        op: op.map(|s| s.to_string()),
        params: serde_json::json!({"edge": "head", "frames": 8}),
        intent: intent.to_string(),
        source: Some("frameio".to_string()),
        source_ref: Some("cmt-1".to_string()),
        author: Some("u-editor".to_string()),
        role: Some("editor".to_string()),
        proposed_by: Some("human".to_string()),
        created_at: 1_700_000_000,
        state: "proposed".to_string(),
        active: true,
        approved_by: None,
        approved_at: None,
        supersedes: None,
        superseded_by: None,
        seq: 1,
        depends_on: None,
        version_ref: None,
        outcome: None,
        updated_at: 0,
        updated_by: None,
        referent: None,
        region: None,
        intent_struct: None,
        capture_ctx: None,
    };
    match name {
        "note_plain" => base("note", None, 120, "her face — warmer"),
        "op_trim" => base("op", Some("trim"), 0, "trim 8 off the head"),
        "marker" => base("marker", Some("marker"), 480, "check this"),
        "note_no_optionals" => ChangeEntry {
            track: None,
            tc_out: None,
            source: None,
            source_ref: None,
            author: None,
            role: None,
            proposed_by: None,
            params: serde_json::json!({}),
            ..base("note", None, 55, "bare")
        },
        "op_with_depends" => ChangeEntry {
            depends_on: Some("entry-xyz".to_string()),
            ..base("op", Some("speed"), 300, "retime to 50%")
        },
        other => panic!("unknown fixture {other}"),
    }
}

#[test]
fn t_hash_backcompat() {
    for (name, want) in GOLDEN {
        let got = compute_entry_hash(&golden_fixture(name));
        assert_eq!(
            &got, want,
            "T-HASH-BACKCOMPAT VIOLATED for `{name}`: the canonical serialization \
             changed. Every entry_hash on every shipped device just moved. This is \
             a format break — fix the serialization, never this constant."
        );
    }
}

#[test]
fn t_hash_backcompat_absent_groups_add_no_keys() {
    // The mechanism behind the law: with all three groups absent, the canonical
    // object gains not one key.
    let e = golden_fixture("note_plain");
    assert!(e.referent.is_none() && e.region.is_none() && e.intent_struct.is_none());
    assert!(
        spatial::canonical_extra(None, None, None).is_empty(),
        "absent = omitted entirely; null is forbidden"
    );
    // And a serialized entry emits no `ref`/`region`/`intent_struct` key at all.
    let json = serde_json::to_value(&e).expect("serialize");
    let obj = json.as_object().expect("object");
    for k in ["ref", "region", "intent_struct", "capture_ctx"] {
        assert!(!obj.contains_key(k), "absent group `{k}` must be OMITTED, not null");
    }
}

// ============================================================================
// T-HASH-CANONICAL-REGION — two writers, identical bytes.
// ============================================================================

#[test]
fn t_hash_canonical_region() {
    // Two "independent writers" building the same region note by different
    // routes: one constructs the struct directly, the other round-trips through
    // JSON with keys in a hostile order.
    let writer_a = entry_with(Some(region_ref()), Some(a_region()), None, None);

    let hostile = serde_json::json!({
        "region": {
            "raster_ref": {"h": 540, "w": 960},
            "shape": {"points": [[250000, 300000], [550000, 700000]], "type": "rect"},
            "key_frame": 40
        },
        "ref": {"src": {"frame_in": 40, "asset_hash": "B"}, "class": "source"}
    });
    let writer_b = ChangeEntry {
        referent: Some(serde_json::from_value(hostile["ref"].clone()).expect("ref")),
        region: Some(serde_json::from_value(hostile["region"].clone()).expect("region")),
        ..entry_with(None, None, None, None)
    };

    assert_eq!(
        compute_entry_hash(&writer_a),
        compute_entry_hash(&writer_b),
        "two independent writers must produce byte-identical canonical bytes"
    );

    // No float ever enters hashed content: the coords are integers.
    let v = serde_json::to_value(a_region()).expect("serialize");
    for p in v["shape"]["points"].as_array().expect("points") {
        for c in p.as_array().expect("pair") {
            assert!(c.is_i64(), "region coords must be fixed-point ints, got {c}");
        }
    }
}

#[test]
fn t_hash_canonical_region_hint_present_vs_absent_dedups_to_one_row() {
    let conn = db();
    // Peer 1 captures the note WITH capture context; peer 2 without it.
    let with_ctx = ChangeEntry {
        capture_ctx: Some(CaptureCtx {
            occurrence_hint: Some(1),
            version_id: Some("v1".to_string()),
            tl_frame: Some(140),
            proxy_raster: Some(RasterRef { w: 960, h: 540 }),
        }),
        ..entry_with(Some(region_ref()), Some(a_region()), None, None)
    };
    let without_ctx = entry_with(Some(region_ref()), Some(a_region()), None, None);

    assert_eq!(
        compute_entry_hash(&with_ctx),
        compute_entry_hash(&without_ctx),
        "capture context is display-only — it must NEVER fork identity"
    );

    let a = changelist::append(&conn, "master-1", "main", with_ctx).expect("append a");
    let b = changelist::append(&conn, "master-1", "main", without_ctx).expect("append b");
    assert_eq!(a.entry_hash, b.entry_hash);
    let view = changelist::get(&conn, "tenant-1", "master-1", "main").expect("get");
    assert_eq!(view.entries.len(), 1, "the two captures dedup to ONE row");
}

// ============================================================================
// T-PROXY-ORPHAN-GUARD — the known orphaning foot-gun.
// ============================================================================

#[test]
fn t_proxy_orphan_guard() {
    // Re-uploading a proxy inside a live round orphans every comment anchored to
    // the old one. The seam refuses it: a new proxy is a NEW ROUND.
    let verdict = spatial::check_proxy_swap("proxy-abc", "proxy-abc", 3);
    assert!(verdict.is_ok(), "the same proxy within a round is fine");

    let refused = spatial::check_proxy_swap("proxy-abc", "proxy-xyz", 3);
    assert!(
        refused.is_err(),
        "swapping the proxy mid-round must be refused at the seam"
    );
    let msg = refused.unwrap_err().to_string();
    assert!(
        msg.contains("new round"),
        "the refusal must say what to do instead, got: {msg}"
    );
}

// ============================================================================
// T-TENANT — tenant_id on every row and query; nothing crosses.
// ============================================================================

#[test]
fn t_tenant() {
    let conn = db();
    let mine = append_region_note(&conn, "tenant-1");
    let theirs = append_region_note(&conn, "tenant-2");

    // Identical CONTENT in two tenants: two distinct rows, each tenant-scoped.
    assert_ne!(mine.entry_hash, theirs.entry_hash, "tenant_id is hashed content");

    let v1 = changelist::get(&conn, "tenant-1", "master-1", "main").expect("t1");
    assert_eq!(v1.entries.len(), 1);
    assert!(v1.entries.iter().all(|e| e.tenant_id == "tenant-1"));
    assert!(
        v1.entries.iter().all(|e| e.region.is_some()),
        "the region rides the tenant-scoped read"
    );

    // The other tenant's note is not reachable through this tenant's queries.
    assert!(
        changelist::get_entry(&conn, "tenant-1", &theirs.id).is_err(),
        "a cross-tenant entry read must fail"
    );
    let all_t1 = changelist::list_entries_by_tenant(&conn, "tenant-1").expect("list");
    assert!(all_t1.iter().all(|e| e.tenant_id == "tenant-1"));
    assert_eq!(all_t1.len(), 1, "the training view is tenant-scoped, never crossing");
}

// ============================================================================
// Cross-org export gating (waist §3 / E2) — OFF by default.
// ============================================================================

#[test]
fn cross_org_export_of_spatial_groups_is_gated_off_by_default() {
    let r = region_ref();
    let g = a_region();
    let i = IntentStruct { craft: "colour".to_string(), structured: None };

    // In-tenant: unrestricted.
    let inside = spatial::project_for_export(
        Some(&r), Some(&g), Some(&i), ExportScope::InTenant, false,
    );
    assert!(inside.r#ref.is_some() && inside.region.is_some() && inside.intent_struct.is_some());
    assert!(inside.dropped.is_empty());

    // Cross-org with the gate OFF (the default): all three withheld, and the
    // loss is DECLARED — nothing silently downgrades.
    let outside = spatial::project_for_export(
        Some(&r), Some(&g), Some(&i), ExportScope::CrossOrg, false,
    );
    assert!(outside.r#ref.is_none() && outside.region.is_none() && outside.intent_struct.is_none());
    assert_eq!(outside.dropped, vec!["ref", "region", "intent_struct"]);

    // Explicitly opened.
    let opened = spatial::project_for_export(
        Some(&r), Some(&g), Some(&i), ExportScope::CrossOrg, true,
    );
    assert!(opened.region.is_some());
    assert!(opened.dropped.is_empty());
}

// ============================================================================
// Sync — the new fields ride the existing gossip/snapshot path opaquely.
// ============================================================================

#[test]
fn spatial_groups_replicate_and_union_by_content_hash() {
    let host = db();
    let peer = db();

    let authored = append_region_note(&host, "tenant-1");
    assert!(authored.region.is_some());

    // The peer receives the entry over the SAME `ChangeEntryAppended` apply path.
    let landed = changelist::apply_entry(&peer, &authored).expect("apply on the peer");
    assert_eq!(landed.entry_hash, authored.entry_hash, "content-hash identity holds");
    assert_eq!(landed.referent, authored.referent, "ref replicated");
    assert_eq!(landed.region, authored.region, "region replicated");
    assert_eq!(landed.intent_struct, authored.intent_struct, "intent_struct replicated");

    // Replay is a no-op: union by (tenant, entry_hash).
    changelist::apply_entry(&peer, &authored).expect("replay");
    let view = changelist::get(&peer, "tenant-1", "master-1", "main").expect("get");
    assert_eq!(view.entries.len(), 1, "a replayed delta unions, never duplicates");
    assert!(view.entries[0].region.is_some(), "the region survived the round trip");

    // The same note arriving from a peer that ALSO carried capture context still
    // unions to one row — capture context is not identity.
    let with_ctx = ChangeEntry {
        id: uuid_like(),
        capture_ctx: Some(CaptureCtx { occurrence_hint: Some(0), ..Default::default() }),
        ..authored.clone()
    };
    changelist::apply_entry(&peer, &with_ctx).expect("apply the hinted copy");
    let view = changelist::get(&peer, "tenant-1", "master-1", "main").expect("get");
    assert_eq!(view.entries.len(), 1, "hinted and unhinted copies dedup to one row");
}

// ============================================================================
// Validation — a malformed anchor is refused at append.
// ============================================================================

#[test]
fn malformed_spatial_groups_are_refused_at_append() {
    let conn = db();
    // A ref carrying a foreign payload.
    let bad_ref = EntryRef {
        class: "source".to_string(),
        src: None,
        junction: None,
        about_entry_hash: Some("deadbeef".to_string()),
        version: None,
    };
    assert!(changelist::append(
        &conn, "master-1", "main",
        ChangeEntry { referent: Some(bad_ref), ..base_entry("tenant-1", "note", None) },
    ).is_err());

    // A region on a JUNCTION-class ref — a seam has no aperture to normalize in.
    assert!(changelist::append(
        &conn, "master-1", "main",
        ChangeEntry {
            referent: Some(junction_ref()),
            region: Some(a_region()),
            ..base_entry("tenant-1", "note", None)
        },
    ).is_err());

    // An unknown craft.
    assert!(changelist::append(
        &conn, "master-1", "main",
        ChangeEntry {
            intent_struct: Some(IntentStruct { craft: "vibes".to_string(), structured: None }),
            ..base_entry("tenant-1", "note", None)
        },
    ).is_err());

    // `kind` gains no new values: a region note is kind=note + region.
    let ok = changelist::append(
        &conn, "master-1", "main",
        ChangeEntry {
            referent: Some(region_ref()),
            region: Some(a_region()),
            ..base_entry("tenant-1", "note", None)
        },
    );
    assert!(ok.is_ok(), "a region note is kind=note + region: {ok:?}");
}

// ============================================================================
// Helpers.
// ============================================================================

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    changelist::migrate(&conn).expect("migrate");
    conn
}

fn uuid_like() -> String {
    // A distinct local id; identity is the content hash, not this.
    format!("id-{}", std::time::SystemTime::now().elapsed().map(|d| d.as_nanos()).unwrap_or(0))
}

fn base_entry(tenant: &str, kind: &str, op: Option<&str>) -> ChangeEntry {
    ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: "master-1".to_string(),
        tenant_id: tenant.to_string(),
        branch: Some("main".to_string()),
        track: Some("V1".to_string()),
        tc_in: 140,
        tc_out: None,
        kind: kind.to_string(),
        op: op.map(|s| s.to_string()),
        params: serde_json::json!({}),
        intent: "her face — warmer".to_string(),
        source: Some("cyan".to_string()),
        source_ref: None,
        author: Some("u-reviewer".to_string()),
        role: Some("reviewer".to_string()),
        proposed_by: Some("human".to_string()),
        created_at: 1_700_000_000,
        state: "proposed".to_string(),
        active: true,
        approved_by: None,
        approved_at: None,
        supersedes: None,
        superseded_by: None,
        seq: 0,
        depends_on: None,
        version_ref: None,
        outcome: None,
        updated_at: 0,
        updated_by: None,
        referent: None,
        region: None,
        intent_struct: None,
        capture_ctx: None,
    }
}

fn entry_with(
    r: Option<EntryRef>,
    g: Option<Region>,
    i: Option<IntentStruct>,
    c: Option<CaptureCtx>,
) -> ChangeEntry {
    ChangeEntry {
        referent: r,
        region: g,
        intent_struct: i,
        capture_ctx: c,
        ..base_entry("tenant-1", "note", None)
    }
}

/// The canonical authored note of the §5 fixture: a region on B@40.
fn append_region_note(conn: &Connection, tenant: &str) -> ChangeEntry {
    changelist::append(
        conn,
        "master-1",
        "main",
        ChangeEntry {
            referent: Some(region_ref()),
            region: Some(a_region()),
            intent_struct: Some(IntentStruct {
                craft: "colour".to_string(),
                structured: Some(serde_json::json!({"direction": "warmer", "subject": "face"})),
            }),
            ..base_entry(tenant, "note", None)
        },
    )
    .expect("append the region note")
}
