// cyan-backend/src/spatial.rs
//
// Spatial notes — the `ref` / `region` / `intent_struct` field groups on
// `ChangeEntry`, and the resolution rules that turn a referent into timeline pins
// on any version. See REVIEW_WAIST_SPEC §1 (anchor model), §2 (region primitive),
// §3 (additive schema + canonical serialization), §5 (the named round-trip tests).
//
// The load-bearing property: an annotation is anchored to its REFERENT, and
// timeline position is always DERIVED, never stored as identity. A note on B@40
// renders on B frame 40 in v1 and in v7, through trims, reorders and retimes,
// because nothing about the timeline was ever written down.
//
// Everything here is additive to the shipped record. An entry with none of these
// groups serializes byte-identically to today (T-HASH-BACKCOMPAT) — enforced by
// `canonical_extra` returning no keys at all when all three are absent.

use std::collections::{HashSet, VecDeque};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

// ============================================================================
// Vocabs (closed, like OP_VOCAB — anything else is a validation error).
// ============================================================================

/// Referent classes (REVIEW_WAIST_SPEC §1 / ANNOTATION_TAXONOMY §3).
pub const REF_CLASS_VOCAB: &[&str] = &["source", "junction", "entry", "version"];

/// Shape types for a region (§2).
pub const SHAPE_VOCAB: &[&str] = &["point", "rect", "ellipse", "path"];

/// `intent_struct.craft` (§3). `compliance` is machine-authored only — never a
/// composer chip; that is a surface rule, not a format rule, so it validates here.
pub const CRAFT_VOCAB: &[&str] = &["colour", "edit", "sound", "gfx", "compliance", "general"];

/// Region coordinates are normalized to the source clean aperture and stored as
/// fixed-point integers scaled by 1e6. **No floats ever enter hashed content**
/// (§3) — float formatting is not reproducible across writers.
pub const FIXED_SCALE: i64 = 1_000_000;

/// Convert a normalized float coordinate to the hashed fixed-point form. Used at
/// the API/FFI boundary only; the stored and hashed type is always `i64`.
pub fn to_fixed(v: f64) -> i64 {
    (v * FIXED_SCALE as f64).round() as i64
}

/// Inverse of [`to_fixed`], for rendering only — never for hashing.
pub fn from_fixed(v: i64) -> f64 {
    v as f64 / FIXED_SCALE as f64
}

// ============================================================================
// `ref` — the referent (§1). Four classes; exactly one payload each.
// ============================================================================

/// One side of a junction, or a source anchor: a content hash plus a SOURCE frame.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SideFrame {
    pub asset_hash: String,
    pub frame: i64,
}

/// `class=source` — about CONTENT. Frames are SOURCE frames; fps rides the asset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SrcRef {
    pub asset_hash: String,
    pub frame_in: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_out: Option<i64>,
}

/// `class=junction` — about the EDIT: the boundary between two sources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JunctionRef {
    pub out: SideFrame,
    #[serde(rename = "in")]
    pub incoming: SideFrame,
    /// Present when the junction is a transition rather than a plain cut.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition_frames: Option<i64>,
}

/// `class=version` — about the ARTIFACT. Deliberately NON-migrating (§1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionRef {
    pub version_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tl_frame_in: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tl_frame_out: Option<i64>,
}

/// The note's identity. The legacy `anchor` fields (`asset_hash`, `tc_in/tc_out`)
/// remain the ledger key and capture context; `ref` is what the note is ABOUT.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntryRef {
    pub class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src: Option<SrcRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub junction: Option<JunctionRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about_entry_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<VersionRef>,
}

impl EntryRef {
    pub fn source(asset_hash: &str, frame_in: i64, frame_out: Option<i64>) -> Self {
        EntryRef {
            class: "source".into(),
            src: Some(SrcRef {
                asset_hash: asset_hash.into(),
                frame_in,
                frame_out,
            }),
            junction: None,
            about_entry_hash: None,
            version: None,
        }
    }

    pub fn junction(out: SideFrame, incoming: SideFrame, transition_frames: Option<i64>) -> Self {
        EntryRef {
            class: "junction".into(),
            src: None,
            junction: Some(JunctionRef {
                out,
                incoming,
                transition_frames,
            }),
            about_entry_hash: None,
            version: None,
        }
    }

    pub fn entry(about_entry_hash: &str) -> Self {
        EntryRef {
            class: "entry".into(),
            src: None,
            junction: None,
            about_entry_hash: Some(about_entry_hash.into()),
            version: None,
        }
    }

    pub fn version(version_id: &str, tl_frame_in: Option<i64>, tl_frame_out: Option<i64>) -> Self {
        EntryRef {
            class: "version".into(),
            src: None,
            junction: None,
            about_entry_hash: None,
            version: Some(VersionRef {
                version_id: version_id.into(),
                tl_frame_in,
                tl_frame_out,
            }),
        }
    }

    /// Exactly one payload, matching `class`. A malformed ref is a hard error at
    /// append — a half-anchored note is worse than no note.
    pub fn validate(&self) -> Result<()> {
        if !REF_CLASS_VOCAB.contains(&self.class.as_str()) {
            return Err(anyhow!("ref.class '{}' not in closed vocab", self.class));
        }
        let present = [
            self.src.is_some(),
            self.junction.is_some(),
            self.about_entry_hash.is_some(),
            self.version.is_some(),
        ];
        let expected = match self.class.as_str() {
            "source" => 0,
            "junction" => 1,
            "entry" => 2,
            _ => 3,
        };
        for (i, p) in present.iter().enumerate() {
            if i == expected && !p {
                return Err(anyhow!("ref.class={} requires its payload", self.class));
            }
            if i != expected && *p {
                return Err(anyhow!(
                    "ref.class={} carries a foreign payload — exactly one",
                    self.class
                ));
            }
        }
        if let Some(s) = &self.src {
            if s.asset_hash.trim().is_empty() {
                return Err(anyhow!("ref.src.asset_hash required"));
            }
            if let Some(out) = s.frame_out
                && out < s.frame_in
            {
                // Reversed segments normalize at capture (§1); a stored inverted
                // range is a writer bug, not a retime.
                return Err(anyhow!(
                    "ref.src frame_out < frame_in — normalize at capture"
                ));
            }
        }
        Ok(())
    }
}

// ============================================================================
// `region` — the seed (§2). No tracked extent, no motion path, no confirmation.
// ============================================================================

/// The shape, in normalized SOURCE clean-aperture coords as fixed-point ints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Shape {
    #[serde(rename = "type")]
    pub kind: String,
    /// `[[x, y], …]`, each scaled by [`FIXED_SCALE`].
    pub points: Vec<[i64; 2]>,
}

/// The raster the shape was drawn on — pins the aspect / clean-aperture mapping
/// so a shape drawn on a 960×540 proxy overlays correctly on a 4K master, and
/// anamorphic/letterboxed proxies do not silently skew (T-REGION-RASTER).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RasterRef {
    pub w: i64,
    pub h: i64,
}

/// The reviewer's scrub gesture. Authored content, hence hashed; consumers may
/// ignore it. This is NOT a tracked extent — see the type doc on [`Region`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtentHint {
    pub frame_in: i64,
    pub frame_out: i64,
}

/// A spatial referent that is exact at `key_frame` and raster-independent.
///
/// **This is a tracker SEED, not a power window.** A power window is a shape WITH
/// tracking; a static box drawn on frame 100 does not cover frame 150, and the
/// format does not pretend otherwise. There is deliberately no `confirmed` field:
/// flipping anything in content would fork `entry_hash`, and confirmation is
/// lifecycle. Realization arrives as NEW ledger facts referencing
/// `about_entry_hash` — never as mutation of the authored entry (§2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Region {
    /// SOURCE frame at which the shape was drawn (version-class: timeline frame).
    pub key_frame: i64,
    pub shape: Shape,
    pub raster_ref: RasterRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extent_hint: Option<ExtentHint>,
}

impl Region {
    pub fn validate(&self) -> Result<()> {
        if !SHAPE_VOCAB.contains(&self.shape.kind.as_str()) {
            return Err(anyhow!(
                "region.shape.type '{}' not in closed vocab",
                self.shape.kind
            ));
        }
        let n = self.shape.points.len();
        let ok = match self.shape.kind.as_str() {
            "point" => n == 1,
            "rect" | "ellipse" => n == 2,
            _ => n >= 2,
        };
        if !ok {
            return Err(anyhow!(
                "region.shape type={} has {} points",
                self.shape.kind,
                n
            ));
        }
        if self.raster_ref.w <= 0 || self.raster_ref.h <= 0 {
            return Err(anyhow!("region.raster_ref must be positive"));
        }
        if let Some(e) = &self.extent_hint
            && e.frame_out < e.frame_in
        {
            return Err(anyhow!("region.extent_hint inverted"));
        }
        Ok(())
    }

    /// Project the normalized shape onto a target raster, in PIXELS. The stored
    /// coords are raster-independent; `raster_ref` only matters when the source
    /// and target pixel-aspect differ (anamorphic), which the caller resolves by
    /// passing the target's clean-aperture size.
    pub fn project(&self, target: &RasterRef) -> Vec<(f64, f64)> {
        self.shape
            .points
            .iter()
            .map(|[x, y]| {
                (
                    from_fixed(*x) * target.w as f64,
                    from_fixed(*y) * target.h as f64,
                )
            })
            .collect()
    }
}

// ============================================================================
// `intent_struct` — the structured intent slot (§3).
// ============================================================================

/// `{craft, structured?}`. Routing (§4) and training weights read this; absent =
/// general. `structured` is a sorted-key JSON payload (colour-triple, op-verb,
/// sound-verb or qc-rule) — hashed like `params`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntentStruct {
    pub craft: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured: Option<serde_json::Value>,
}

impl IntentStruct {
    pub fn validate(&self) -> Result<()> {
        if !CRAFT_VOCAB.contains(&self.craft.as_str()) {
            return Err(anyhow!(
                "intent_struct.craft '{}' not in closed vocab",
                self.craft
            ));
        }
        if let Some(s) = &self.structured
            && !s.is_object()
        {
            return Err(anyhow!("intent_struct.structured must be an object"));
        }
        Ok(())
    }
}

// ============================================================================
// Capture context — display-only provenance, EXCLUDED from the hash (§3).
// ============================================================================

/// What the author was looking at when they wrote the note. **Never part of
/// identity**: two peers capturing the same note with and without a hint must
/// dedup to one row (T-HASH-CANONICAL-REGION). Kept alongside the entry as a
/// separate, unhashed column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CaptureCtx {
    /// Which occurrence of a repeated source range the author was on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurrence_hint: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    /// The timeline frame they were parked on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tl_frame: Option<i64>,
    /// The proxy raster the shape was drawn on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_raster: Option<RasterRef>,
}

impl CaptureCtx {
    pub fn is_empty(&self) -> bool {
        self == &CaptureCtx::default()
    }
}

// ============================================================================
// Canonical serialization (§3, extending CYAN_FORMAT_SPEC §4 — NORMATIVE).
// ============================================================================

/// The `ref` / `region` / `intent_struct` contribution to the canonical content
/// hash, as `(key, value)` pairs to splice into `compute_entry_hash`'s object.
///
/// **The rule that makes T-HASH-BACKCOMPAT a law:** absent = omitted entirely,
/// null is forbidden. When all three groups are absent this returns an EMPTY
/// vector, so the canonical object is byte-for-byte what it was before this
/// change existed and every pre-region `entry_hash` is unchanged.
///
/// Values are built through `serde_json::to_value`, whose `Map` is a `BTreeMap`
/// (no `preserve_order` feature) — so all nested objects are sorted-key and two
/// independent writers produce identical bytes. `skip_serializing_if` on every
/// optional sub-field is what keeps nulls out.
pub fn canonical_extra(
    r#ref: Option<&EntryRef>,
    region: Option<&Region>,
    intent_struct: Option<&IntentStruct>,
) -> Vec<(String, serde_json::Value)> {
    let mut out = Vec::new();
    if let Some(r) = r#ref
        && let Ok(v) = serde_json::to_value(r)
    {
        out.push(("ref".to_string(), v));
    }
    if let Some(g) = region
        && let Ok(v) = serde_json::to_value(g)
    {
        out.push(("region".to_string(), v));
    }
    if let Some(i) = intent_struct
        && let Ok(v) = serde_json::to_value(i)
    {
        out.push(("intent_struct".to_string(), v));
    }
    out
}

// ============================================================================
// Resolution seams (§5): FrameMapProvider · LineageProvider.
// ============================================================================

/// One timeline landing of a source frame. Slow-mo maps one source frame to many
/// timeline frames; the provider reports the FIRST timeline frame of the
/// occurrence within each segment (§1), one `Occurrence` per segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Occurrence {
    pub segment_id: String,
    pub tl_frame: i64,
    pub src_frame: i64,
}

/// An A→B boundary in a version's cut.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Boundary {
    pub tl_frame: i64,
    pub out: SideFrame,
    #[serde(rename = "in")]
    pub incoming: SideFrame,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition_frames: Option<i64>,
}

/// What the player is actually showing at a timeline frame. Inside a transition
/// overlap one timeline frame maps to two sources; capture anchors to the
/// dominant-mix source, with `other` offered for the one-gesture flip
/// (T-DISSOLVE-CAPTURE).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DominantHit {
    pub asset_hash: String,
    pub src_frame: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub other: Option<SideFrame>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition_frames: Option<i64>,
}

/// The cut structure of a version, as a frame RELATION (not a bijection).
///
/// Prod is backed by `conform_plan`; the default suite uses fixture maps
/// including dissolves and retimes. This trait is the whole seam — resolution
/// never touches the DB.
pub trait FrameMapProvider {
    /// Timeline occurrences of `(asset_hash, src_frame)` in `version`.
    fn occurrences(&self, version: &str, asset_hash: &str, src_frame: i64) -> Vec<Occurrence>;
    /// Every A→B boundary in `version`, in timeline order.
    fn boundaries(&self, version: &str) -> Vec<Boundary>;
    /// Capture-time: what is dominant at this timeline frame.
    fn dominant_at(&self, version: &str, tl_frame: i64) -> Option<DominantHit>;
}

/// A rational, integer-only frame map across a lineage edge. Identity when
/// absent (same duration — the common re-grade case). A retimed re-render
/// declares one. Kept rational so no float enters a resolution decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LineageFrameMap {
    pub scale_num: i64,
    pub scale_den: i64,
    #[serde(default)]
    pub offset: i64,
}

impl LineageFrameMap {
    pub fn apply(&self, frame: i64) -> i64 {
        if self.scale_den == 0 {
            return frame;
        }
        frame * self.scale_num / self.scale_den + self.offset
    }
}

/// An (old → new) essence edge: a re-grade, a returned VFX pull, a revised title.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LineageEdge {
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_map: Option<LineageFrameMap>,
}

/// (old → new) essence edges, from the ledger's own `swap{new_asset_hash}` ops
/// and `derived_from` facts. Without this walk every note on a re-graded shot
/// would go latent while the content sits visibly in the cut.
pub trait LineageProvider {
    fn children(&self, asset_hash: &str) -> Vec<LineageEdge>;
}

/// A lineage descendant reached by the walk, with the accumulated frame mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descendant {
    pub asset_hash: String,
    pub frame: i64,
    /// Hops from the authored asset — 0 is the asset itself.
    pub depth: usize,
}

/// Breadth-first walk of (old → new) edges from `asset_hash`, mapping `frame`
/// across each edge. The authored asset itself is always first (`depth == 0`),
/// so callers try the original before any descendant. The walk NEVER guesses: an
/// asset with no declared edge yields no descendants, and the note goes latent.
pub fn lineage_walk(
    lineage: &dyn LineageProvider,
    asset_hash: &str,
    frame: i64,
) -> Vec<Descendant> {
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut q: VecDeque<Descendant> = VecDeque::new();
    q.push_back(Descendant {
        asset_hash: asset_hash.to_string(),
        frame,
        depth: 0,
    });
    seen.insert(asset_hash.to_string());
    while let Some(cur) = q.pop_front() {
        for edge in lineage.children(&cur.asset_hash) {
            if !seen.insert(edge.to.clone()) {
                continue;
            }
            let mapped = match &edge.frame_map {
                Some(m) => m.apply(cur.frame),
                None => cur.frame, // identity: same duration
            };
            q.push_back(Descendant {
                asset_hash: edge.to.clone(),
                frame: mapped,
                depth: cur.depth + 1,
            });
        }
        out.push(cur);
    }
    out
}

// ============================================================================
// Resolution (§1) — the render-time answer for a referent on a version.
// ============================================================================

/// A resolved landing on the timeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pin {
    pub asset_hash: String,
    pub tl_frame: i64,
    pub src_frame: i64,
    /// True when the pin was reached through the lineage walk rather than the
    /// authored asset directly (a re-grade, a VFX return).
    pub via_lineage: bool,
    /// The occurrence the author was looking at, per capture context. Display
    /// only — it never forks identity.
    pub authored: bool,
}

/// A resolved junction landing, with drift against the authored frames.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JunctionPin {
    pub tl_frame: i64,
    pub out: SideFrame,
    #[serde(rename = "in")]
    pub incoming: SideFrame,
    /// `out.frame - authored out.frame` — "the cut moved +6 since this note".
    pub drift: i64,
    pub authored: bool,
}

/// The answer for one referent on one version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Resolution {
    /// The referent is in this cut. Repeated source ranges pin at EVERY
    /// occurrence; the authored one is marked.
    OnCut { pins: Vec<Pin> },
    /// A junction resolves to ALL A→B boundaries (the A-B-A-B intercut is
    /// normal); the authored one is marked.
    Junction { pins: Vec<JunctionPin> },
    /// Listed under "notes on removed material" — never deleted, resurfacing if
    /// the material returns. No data changes when a note goes latent.
    Latent { reason: String },
    /// A version-class note viewed from another version: it does NOT migrate; it
    /// lists as "authored on v_k" and pins only when viewing v_k.
    AuthoredOnOtherVersion { authored_on: String },
    /// An entry-class note — about a ledger item, resolved by the caller against
    /// the referenced entry.
    AboutEntry { entry_hash: String },
}

/// Resolve a referent onto `version`. Pure: all cut knowledge arrives through the
/// two provider seams, so the default suite runs with no DB, no network, no peer.
pub fn resolve(
    r#ref: &EntryRef,
    ctx: Option<&CaptureCtx>,
    version: &str,
    frames: &dyn FrameMapProvider,
    lineage: &dyn LineageProvider,
) -> Resolution {
    let hint = ctx.and_then(|c| c.occurrence_hint);
    match r#ref.class.as_str() {
        "source" => resolve_source(r#ref, hint, version, frames, lineage),
        "junction" => resolve_junction(r#ref, hint, version, frames, lineage),
        "entry" => match &r#ref.about_entry_hash {
            Some(h) => Resolution::AboutEntry {
                entry_hash: h.clone(),
            },
            None => Resolution::Latent {
                reason: "entry-class ref without about_entry_hash".to_string(),
            },
        },
        "version" => resolve_version(r#ref, version),
        other => Resolution::Latent {
            reason: format!("unknown ref class '{other}'"),
        },
    }
}

/// SOURCE class — try the authored asset first, then its lineage descendants;
/// only when none is on-cut does the note go latent.
fn resolve_source(
    r#ref: &EntryRef,
    hint: Option<i64>,
    version: &str,
    frames: &dyn FrameMapProvider,
    lineage: &dyn LineageProvider,
) -> Resolution {
    let Some(src) = &r#ref.src else {
        return Resolution::Latent {
            reason: "source-class ref without src payload".to_string(),
        };
    };
    // `lineage_walk` yields the authored asset at depth 0, so the original is
    // always preferred over a descendant.
    for d in lineage_walk(lineage, &src.asset_hash, src.frame_in) {
        let occ = frames.occurrences(version, &d.asset_hash, d.frame);
        if occ.is_empty() {
            continue;
        }
        // A repeated source range pins at EVERY occurrence; the capture-context
        // hint marks the authored one (display only — it never forks identity).
        let pins = occ
            .iter()
            .enumerate()
            .map(|(i, o)| Pin {
                asset_hash: d.asset_hash.clone(),
                tl_frame: o.tl_frame,
                src_frame: o.src_frame,
                via_lineage: d.depth > 0,
                authored: hint == Some(i as i64),
            })
            .collect();
        return Resolution::OnCut { pins };
    }
    Resolution::Latent {
        reason: format!(
            "no lineage descendant of {} is on-cut in {version}",
            src.asset_hash
        ),
    }
}

/// JUNCTION class — resolves to ALL A→B boundaries in the version. Each side is
/// matched against its own lineage set, so a re-graded A still forms the seam.
fn resolve_junction(
    r#ref: &EntryRef,
    hint: Option<i64>,
    version: &str,
    frames: &dyn FrameMapProvider,
    lineage: &dyn LineageProvider,
) -> Resolution {
    let Some(j) = &r#ref.junction else {
        return Resolution::Latent {
            reason: "junction-class ref without junction payload".to_string(),
        };
    };
    let side_set = |s: &SideFrame| -> HashSet<String> {
        lineage_walk(lineage, &s.asset_hash, s.frame)
            .into_iter()
            .map(|d| d.asset_hash)
            .collect()
    };
    let out_set = side_set(&j.out);
    let in_set = side_set(&j.incoming);

    let mut pins: Vec<JunctionPin> = frames
        .boundaries(version)
        .into_iter()
        .filter(|b| out_set.contains(&b.out.asset_hash) && in_set.contains(&b.incoming.asset_hash))
        .map(|b| JunctionPin {
            tl_frame: b.tl_frame,
            // Drift against the authored frames: "the cut moved +6 since this
            // note" is `out.frame - authored out.frame`.
            drift: b.out.frame - j.out.frame,
            out: b.out,
            incoming: b.incoming,
            authored: false,
        })
        .collect();

    if pins.is_empty() {
        // A and B are nowhere adjacent, post-lineage-walk on both sides.
        return Resolution::Latent {
            reason: format!(
                "{} and {} are nowhere adjacent in {version}",
                j.out.asset_hash, j.incoming.asset_hash
            ),
        };
    }
    if let Some(h) = hint
        && let Some(p) = pins.get_mut(h as usize)
    {
        p.authored = true;
    }
    Resolution::Junction { pins }
}

/// VERSION class — deliberately does NOT migrate. "v1 drags" is feedback on v1;
/// v2 either addressed it or didn't. On any other version it lists as
/// "authored on v_k" rather than pinning somewhere it does not belong.
fn resolve_version(r#ref: &EntryRef, version: &str) -> Resolution {
    let Some(v) = &r#ref.version else {
        return Resolution::Latent {
            reason: "version-class ref without version payload".to_string(),
        };
    };
    if v.version_id != version {
        return Resolution::AuthoredOnOtherVersion {
            authored_on: v.version_id.clone(),
        };
    }
    // Version-class frames are TIMELINE frames of the version itself — there is
    // no source to map through, which is the whole point of the class.
    let at = v.tl_frame_in.unwrap_or(0);
    Resolution::OnCut {
        pins: vec![Pin {
            asset_hash: v.version_id.clone(),
            tl_frame: at,
            src_frame: at,
            via_lineage: false,
            authored: true,
        }],
    }
}

// ============================================================================
// Proxy-orphan guard (T-PROXY-ORPHAN-GUARD).
// ============================================================================

/// Refuse a proxy swap inside a live review round.
///
/// Re-uploading a proxy mid-round is the known orphaning foot-gun: every comment
/// anchored to the old proxy's frames is stranded. The rule is that a new proxy
/// is a NEW ROUND — so the seam refuses the swap and says so.
pub fn check_proxy_swap(bound_proxy: &str, incoming_proxy: &str, round: i64) -> Result<()> {
    if bound_proxy == incoming_proxy {
        return Ok(());
    }
    Err(anyhow!(
        "proxy swap refused: round {round} is bound to proxy '{bound_proxy}', and \
         swapping in '{incoming_proxy}' would orphan every comment anchored to the \
         old one — publish a new round instead"
    ))
}

// ============================================================================
// Export gating (§3 / REVIEW_DECISIONS E2 — IP posture).
// ============================================================================

/// Who the projection is being written for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportScope {
    /// Same tenant: unrestricted.
    InTenant,
    /// Another org. `ref`/`region`/`intent_struct` are gated OFF by default.
    CrossOrg,
}

/// What a projection carries, and what it DECLARED it dropped (§4 — nothing
/// silently downgrades).
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub r#ref: Option<EntryRef>,
    pub region: Option<Region>,
    pub intent_struct: Option<IntentStruct>,
    pub dropped: Vec<String>,
}

/// Project the spatial groups for an export.
///
/// Cross-org export of `ref`/`region`/`intent_struct` ships **gated OFF by
/// default** (§3, REVIEW_DECISIONS E2): `allow_cross_org` must be explicitly true
/// for them to cross an org boundary, and whatever is withheld is named in
/// `dropped` rather than silently disappearing.
pub fn project_for_export(
    r#ref: Option<&EntryRef>,
    region: Option<&Region>,
    intent_struct: Option<&IntentStruct>,
    scope: ExportScope,
    allow_cross_org: bool,
) -> Projection {
    let gated = scope == ExportScope::CrossOrg && !allow_cross_org;
    let mut dropped = Vec::new();
    if gated {
        // Name only what was actually present — a `dropped` record must be true.
        if r#ref.is_some() {
            dropped.push("ref".to_string());
        }
        if region.is_some() {
            dropped.push("region".to_string());
        }
        if intent_struct.is_some() {
            dropped.push("intent_struct".to_string());
        }
        return Projection {
            r#ref: None,
            region: None,
            intent_struct: None,
            dropped,
        };
    }
    Projection {
        r#ref: r#ref.cloned(),
        region: region.cloned(),
        intent_struct: intent_struct.cloned(),
        dropped,
    }
}
