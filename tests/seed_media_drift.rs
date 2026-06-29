//! Drift guard: every clip the demo seed (`src/seed.rs`) binds to a board MUST be
//! covered by the canonical media manifest (`cyan-iac/modules/lens/media/clips.json`)
//! that the on-box provisioner (`provision-media.sh`) materializes.
//!
//! Why this matters: the seeded workflow's `qc-probe` step shells `ffprobe
//! /data/<clip>` inside the airgapped (`--network none`) cyan-media container. If a
//! board names a clip the provisioner doesn't know, NO file is materialized at
//! `/opt/cyan/media/<clip>`, probe fails, the run chain halts, and the board shows
//! `$0 / failed`. This test makes that failure mode a compile-of-the-suite failure
//! instead of a silent demo-day surprise.
//!
//! Source of truth: the manifest filenames are mirrored here as `MANIFEST_CLIPS`
//! (keep in sync with clips.json — the standalone `verify-media.sh` gate enforces the
//! other direction, that each manifest clip is actually present on the box). The seed
//! clips are extracted from `src/seed.rs` at test time via `include_str!`, so adding a
//! board clip is automatically picked up with no second list to maintain.

/// The canonical clip set, mirroring `cyan-iac/modules/lens/media/clips.json`.
/// If you add a clip to a `SeedBoard`, add it to the manifest AND to this list.
const MANIFEST_CLIPS: &[&str] = &[
    "sintel-clip.mp4",
    "tears-of-steel-clip.mp4",
    "elephants-dream-30s.mp4",
    "big-buck-bunny.mp4",
    "jellyfish-broll.mp4",
    "bars-smpte-720p-15s.mp4",
    "rgb-480p-12s.mp4",
];

/// Extract every `clip: "<name>"` string literal from the seed source.
fn seed_clips() -> Vec<String> {
    let src = include_str!("../src/seed.rs");
    let mut out = Vec::new();
    for line in src.lines() {
        // match `clip: "..."` (the SeedBoard field), tolerant of surrounding tokens.
        if let Some(idx) = line.find("clip:") {
            let rest = &line[idx + "clip:".len()..];
            if let Some(start) = rest.find('"') {
                let after = &rest[start + 1..];
                if let Some(end) = after.find('"') {
                    out.push(after[..end].to_string());
                }
            }
        }
    }
    out
}

#[test]
fn media_manifest_covers_seed_clips() {
    let clips = seed_clips();
    assert!(
        !clips.is_empty(),
        "extracted zero clips from src/seed.rs — the `clip: \"...\"` pattern changed; \
         update seed_clips() so this drift guard keeps working"
    );

    let mut uncovered: Vec<String> = clips
        .iter()
        .filter(|c| !MANIFEST_CLIPS.contains(&c.as_str()))
        .cloned()
        .collect();
    uncovered.sort();
    uncovered.dedup();

    assert!(
        uncovered.is_empty(),
        "seed clip(s) not covered by the canonical media manifest \
         (cyan-iac/modules/lens/media/clips.json): {uncovered:?}. \
         Add each to clips.json (so provision-media.sh materializes it) AND to \
         MANIFEST_CLIPS in this test — otherwise its board's run halts at the \
         cyan-media probe step ($0/failed)."
    );
}
