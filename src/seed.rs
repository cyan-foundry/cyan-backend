//! Demo scale-seed — the coherent 3-group / 10-board demo set, callable BOTH from the
//! `cyan_node` CLI (`seed_demo`) and IN-PROCESS via the `cyan_seed_demo()` FFI.
//!
//! Why in-process matters (Fix A): the CLI seed runs in a *separate* process under a
//! *different* identity and only lands in whatever db that process opened. The app then
//! has to open the exact same file for the seed to be visible, which only holds in the
//! isolated `CYAN_DATA_DIR` harness — a normal app launch (its Documents db) is never
//! seeded. Seeding in-process makes the app seed *its own* db under *its own* identity:
//! when `owner_node_id` is supplied the seeded groups are stamped with it (mirroring the
//! real `CommandMsg::CreateGroup` path) and a `group_members` roster row is written, so
//! the seeded groups are app-owned and fully manageable (rename/delete/deploy).
//!
//! NOTE on the Explorer-empty symptom: the tree query (`dump_tree_json`) is UNSCOPED, so
//! a node-seeded group with an empty `owner_node_id` is still visible — the historical
//! "no groups" symptom is a db-path / stale-orphan artifact of the two-process seed, which
//! this in-process path removes. Owner stamping below is for correct *ownership*, not
//! visibility.

use anyhow::{anyhow, Result};

use crate::storage;

/// The group ids the demo scale-seed OWNS. `seed_demo` deletes every one of these
/// (cascading workspaces/boards/cells/files) before re-seeding — idempotent, no dups —
/// and it also reaps the prior botched seed (three groups that all reused the name
/// "Post-Production": post-production / promos / trailers).
const SEED_MANAGED_GROUP_IDS: [&str; 4] = ["post-production", "promos", "trailers", "broadcast"];

struct SeedBoard {
    id: &'static str,
    name: &'static str,
    clip: &'static str,
    /// true ⇒ the clip has a real AUDIO track (verified via ffprobe on the lens box),
    /// so transcribe + loudness QC are runtime-coherent. Audioless clips (sintel /
    /// tears-of-steel / jellyfish / bars / rgb excerpts) get black/freeze QC only —
    /// a loudness/transcribe step on them would fail ffmpeg at run time.
    audio: bool,
}
struct SeedGroup {
    id: &'static str,
    name: &'static str,
    icon: &'static str,
    color: &'static str,
    boards: &'static [SeedBoard],
}

/// Coherent, idempotent demo scale-seed (items #1/#27/STEP2). Every clip below is a real
/// file staged in the lens media root with a matching thumbnail
/// (`/api/v1/media/thumbnail?asset=<clip>` ⇒ 200 image/jpeg).
///
/// `owner_node_id` — when non-empty, every seeded group is stamped with this identity
/// (mirrors `CommandMsg::CreateGroup`) and gets a `group_members` roster row, so the
/// seed is owned by the caller. Pass `""` to preserve the legacy CLI behavior (groups
/// inserted with no owner — unscoped, still visible, but not owned by any live identity).
pub fn seed_demo(owner_node_id: &str) -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    // 1) Truncate the managed groups (cascades workspaces/boards/cells/files) so any
    //    prior or duplicate seed data is gone before we re-seed → idempotent, no dups.
    for gid in SEED_MANAGED_GROUP_IDS {
        let _ = storage::group_delete(gid);
    }
    // group_delete doesn't cascade board_workflow_state — prune the orphaned deploy-state
    // rows so a re-seed leaves NO stale rows (the board-card deploy gate reads this table).
    let _ = storage::workflow_state_prune_orphans();
    // 2) The coherent set: 3 distinctly-named groups, 10 distinctly-named boards, each
    //    bound to ONE real staged clip. No two groups/boards share a name.
    let groups: [SeedGroup; 3] = [
        SeedGroup {
            id: "post-production",
            name: "Post-Production",
            icon: "film.stack",
            color: "#22D3EE",
            boards: &[
                SeedBoard { id: "pp-sintel-finish", name: "Sintel — Color & Finish", clip: "sintel-clip.mp4", audio: false },
                SeedBoard { id: "pp-tos-online", name: "Tears of Steel — Online Edit", clip: "tears-of-steel-clip.mp4", audio: false },
                SeedBoard { id: "pp-ed-dialogue", name: "Elephants Dream — Dialogue Pass", clip: "elephants-dream-30s.mp4", audio: true },
                SeedBoard { id: "pp-bbb-master", name: "Big Buck Bunny — Feature Master", clip: "big-buck-bunny.mp4", audio: true },
            ],
        },
        SeedGroup {
            id: "promos",
            name: "Trailers & Promos",
            icon: "megaphone.fill",
            color: "#A855F7",
            boards: &[
                SeedBoard { id: "pr-sintel-teaser", name: "Sintel — Teaser Cut", clip: "sintel-clip.mp4", audio: false },
                SeedBoard { id: "pr-tos-trailer", name: "Tears of Steel — Trailer", clip: "tears-of-steel-clip.mp4", audio: false },
                SeedBoard { id: "pr-jelly-broll", name: "Jellyfish — Nature B-Roll", clip: "jellyfish-broll.mp4", audio: false },
            ],
        },
        SeedGroup {
            id: "broadcast",
            name: "Broadcast Delivery",
            icon: "antenna.radiowaves.left.and.right",
            color: "#34D399",
            boards: &[
                SeedBoard { id: "bc-smpte-qc", name: "SMPTE Bars — QC Gate", clip: "bars-smpte-720p-15s.mp4", audio: false },
                SeedBoard { id: "bc-rgb-align", name: "RGB — Alignment Check", clip: "rgb-480p-12s.mp4", audio: false },
                SeedBoard { id: "bc-bbb-package", name: "Big Buck Bunny — Broadcast Package", clip: "big-buck-bunny.mp4", audio: true },
            ],
        },
    ];
    // When seeding under a real identity, provision workspaces as that owner (mirrors the
    // CreateGroup path which passes Some(&node_id)); the legacy CLI path keeps None.
    let ws_owner: Option<&str> = if owner_node_id.is_empty() { None } else { Some(owner_node_id) };
    let mut n_boards = 0usize;
    for g in &groups {
        storage::group_insert_simple(g.id, g.name, g.icon, g.color)
            .map_err(|e| anyhow!("group_insert_simple({}): {e}", g.id))?;
        // Stamp ownership + roster membership so the seeded group is owned by the caller
        // (rename/delete/deploy gates check owner_node_id). Guarded so the empty-owner CLI
        // path produces data byte-identical to the historical behavior.
        if !owner_node_id.is_empty() {
            storage::group_set_owner(g.id, owner_node_id)
                .map_err(|e| anyhow!("group_set_owner({}): {e}", g.id))?;
            storage::member_seen(g.id, owner_node_id, now)
                .map_err(|e| anyhow!("member_seen({}): {e}", g.id))?;
        }
        let (default_ws, _plugins) = storage::provision_group_workspaces(g.id, ws_owner)
            .map_err(|e| anyhow!("provision_group_workspaces({}): {e}", g.id))?;
        let ws = default_ws.id;
        for b in g.boards {
            seed_board(g.id, &ws, b, now)?;
            n_boards += 1;
        }
    }
    Ok(format!("{} groups / {} boards (no dups)", groups.len(), n_boards))
}

/// Seed one board: a deployed+pinned workflow whose EVERY step names the board's own
/// clip (so the per-step asset frame is coherent), plus the bound clip as a file asset.
fn seed_board(group_id: &str, ws: &str, b: &SeedBoard, now: i64) -> Result<()> {
    let board = b.id;
    storage::board_insert_simple(board, ws, b.name, now)
        .map_err(|e| anyhow!("board_insert_simple({board}): {e}"))?;
    let clip = b.clip;
    // (cell text, step_id, executor, depends_on) — coherent per-board clip throughout.
    // The QC step names the EXACT cyan-media tool(s) (qc_black_freeze / qc_loudness) so
    // the 8B emits the right `mcp_tool` name instead of guessing (e.g. "blackdetect").
    let qc_text = if b.audio {
        format!("QC findings: call the cyan-media tool qc_black_freeze on {clip} (bare filename) for black/freeze time ranges, then call qc_loudness on {clip} with target_lufs -14. Report the timecoded black ranges, freeze ranges, and the integrated LUFS.")
    } else {
        format!("QC findings: call the cyan-media tool qc_black_freeze on {clip} (bare filename) for black/freeze time ranges and report them. This clip has no audio track, so skip loudness.")
    };
    let mut steps: Vec<(String, &str, &str, Vec<&str>)> = vec![
        (format!("Ingest the broadcast master: the local file {clip} (in the media root)."),
         "ingest", "lens", vec![]),
        (format!("QC / probe: run the cyan-media probe tool on {clip} — pass the bare filename as input (not a URL) — and report container, video codec, resolution, and duration."),
         "qc-probe", "lens", vec!["ingest"]),
        (qc_text, "qc-findings", "lens", vec!["qc-probe"]),
    ];
    let mut last = "qc-findings";
    if b.audio {
        steps.push((
            format!("Transcribe: run the cyan-media transcribe tool on {clip} (bare filename, not a URL) to capture the spoken dialogue and subtitles."),
            "transcribe", "lens", vec!["qc-findings"],
        ));
        last = "transcribe";
    }
    steps.push((
        format!("Package: deliver {clip} at -14 LUFS and write the delivery sidecar."),
        "package", "manual", vec![last],
    ));
    for (i, (text, step_id, executor, deps)) in steps.iter().enumerate() {
        let meta = serde_json::json!({
            "pipeline": {
                "step_id": step_id,
                "depends_on": deps,
                "executor": executor,
                "model": "cyan-lens",
                "timeout_seconds": 300,
                "retry_count": 1,
                "auto_advance": false,
                "notifications": [],
                "state": { "status": "pending", "attempt": 0 }
            }
        })
        .to_string();
        storage::cell_insert_simple(
            &format!("{board}-{step_id}"), board, "markdown", i as i32,
            Some(text), None, false, None, Some(&meta), now, now,
        )
        .map_err(|e| anyhow!("cell_insert_simple({board}-{step_id}): {e}"))?;
    }
    // Mark the board DEPLOYED via the workflow API so the LOCAL deploy state is accurate
    // (the board-card living-wall reads this through the cyan_board_workflow_state FFI).
    crate::workflow::mark_deployed(board, true, now)
        .map_err(|e| anyhow!("mark_deployed({board}): {e}"))?;
    storage::board_meta_set_pinned(board, true, now)
        .map_err(|e| anyhow!("board_meta_set_pinned({board}): {e}"))?;
    // The bound clip as the board's primary asset artifact (coherent with the steps).
    storage::file_insert_simple(
        &format!("{board}-asset"), Some(group_id), Some(ws), Some(board),
        b.clip, &format!("seed-{board}"), 10_000_000, None, now,
    )
    .map_err(|e| anyhow!("file_insert_simple({board}-asset): {e}"))?;
    Ok(())
}
