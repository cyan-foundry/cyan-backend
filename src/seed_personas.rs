//! SeedTok multi-persona seed — the role→landing demo cast.
//!
//! Companion to `seed::seed_demo` (the 3-group scale-seed). Where `seed_demo` seeds
//! ONE owner's boards, this seeds a coherent CAST: six production personas, each with
//! its craft/production role, a home board, a role-stamped note (author_role provenance),
//! and — for the review-facing roles — a "v2" review version artifact. Every persona
//! maps deterministically to a PRIMARY LANDING SURFACE via
//! `role_templates::primary_surface_for`, so signing in as a persona lands on its home
//! surface populated by exactly this data.
//!
//! Gating: this is a DEMO/dev seed. The FFI entry (`cyan_seed_personas`) refuses unless
//! `CYAN_SEED_DEMO=1` — it never runs in a production build. Idempotent: the managed
//! group is truncated (cascade) before re-seed, so a re-run leaves no dups.
//!
//! Provenance discipline (guardrails): every note row carries `tenant_id`, `author_id`,
//! `author_name`, and `author_role` (the craft role). Group ownership and roster follow
//! the `seed_demo` pattern (owner_node_id stamped, `group_members` row written) so the
//! seeded group is app-owned. `post_super` is DISPLAYED as "post_super" but carries the
//! existing `studio_exec` craft role (no new `PRODUCTION_ROLE_VOCAB` entry — same
//! board-wall landing), per the role decision.

use anyhow::{anyhow, Result};
use serde::Serialize;

use crate::models::dto::NoteDTO;
use crate::role_templates::primary_surface_for;
use crate::storage;

/// The one group this seed OWNS (truncated + cascaded before every re-seed → idempotent).
const SEEDTOK_GROUP_ID: &str = "seedtok-studio";
const SEEDTOK_GROUP_NAME: &str = "SeedTok Studio";

/// One persona in the cast. `craft_role` is a real `PRODUCTION_ROLE_VOCAB` slug (the
/// author_role stamp + the surface-map key); `display_role` is what the UI shows (only
/// differs for `post_super`, which rides `studio_exec`). `review` ⇒ seed a v2 review
/// version artifact on the home board (the review-facing roles).
pub struct SeedPersona {
    /// Dev-token / author id — the `seedtok_<persona>` sign-in token.
    pub token: &'static str,
    /// author_name stamped on the persona's notes.
    pub display: &'static str,
    /// A real craft-role slug (`studio_exec` for post_super).
    pub craft_role: &'static str,
    /// The label the UI shows (== craft_role except post_super).
    pub display_role: &'static str,
    pub board_id: &'static str,
    pub board_name: &'static str,
    /// A valid `NOTE_KIND_VOCAB` kind for the persona's stamped note.
    pub note_kind: &'static str,
    pub note_text: &'static str,
    /// Seed a v2 review version artifact on this board (review-facing roles).
    pub review: bool,
}

/// The cast — order is the display order; each `token` is unique, each `craft_role` a
/// `PRODUCTION_ROLE_VOCAB` slug. Notes are craft-realistic and quote no fabricated metrics.
pub const SEED_PERSONAS: [SeedPersona; 6] = [
    SeedPersona {
        token: "seedtok_post_super",
        display: "Morgan Pierce",
        craft_role: "studio_exec",
        display_role: "post_super",
        board_id: "seedtok-postsuper-wall",
        board_name: "Post-Production — Slate Overview",
        note_kind: "decision",
        note_text: "Slate status: all shows in finishing. Broadcast package is the gating deliverable — hold the online lock until color signs off.",
        review: false,
    },
    SeedPersona {
        token: "seedtok_producer",
        display: "Dana Whitfield",
        craft_role: "producer",
        display_role: "producer",
        board_id: "seedtok-producer-show",
        board_name: "Sintel — Producer's Cut",
        note_kind: "creative-brief",
        note_text: "Brief: 90s promo, warm grade, licensed score. Deliver a review cut for notes before the client screening.",
        review: true,
    },
    SeedPersona {
        token: "seedtok_director",
        display: "Alex Rivera",
        craft_role: "director",
        display_role: "director",
        board_id: "seedtok-director-review",
        board_name: "Tears of Steel — Director Review",
        note_kind: "editor-note",
        note_text: "Review note: the second act drags on the wide — tighten the reverse and hold the actor's beat two frames longer.",
        review: true,
    },
    SeedPersona {
        token: "seedtok_editor",
        display: "Sam Okafor",
        craft_role: "editor",
        display_role: "editor",
        board_id: "seedtok-editor-notebook",
        board_name: "Elephants Dream — Assembly",
        note_kind: "editor-note",
        note_text: "Cut note: assembly locked through scene 4. Dialogue overlap on the door beat needs a J-cut; flagged for the mix.",
        review: false,
    },
    SeedPersona {
        token: "seedtok_asseditor",
        display: "Riya Nair",
        craft_role: "assistant_editor",
        display_role: "assistant_editor",
        board_id: "seedtok-ae-queue",
        board_name: "Big Buck Bunny — Turnover Prep",
        note_kind: "shot-log",
        note_text: "Turnover prep: media relinked, dupes removed, subclips named to house convention. Two shots pending source from camera.",
        review: false,
    },
    SeedPersona {
        token: "seedtok_colorist",
        display: "Noa Berger",
        craft_role: "colorist",
        display_role: "colorist",
        board_id: "seedtok-colorist-review",
        board_name: "Jellyfish — Color Pass",
        note_kind: "editor-note",
        note_text: "Color pass: lifted the shadows a touch, pulled the cyan out of the skin midtones, matched the two ocean angles. v2 posted for review.",
        review: true,
    },
];

/// One persona's landing manifest row (the FFI contract the iOS sign-in reads to route).
#[derive(Debug, Clone, Serialize)]
pub struct PersonaManifest {
    pub token: String,
    pub display: String,
    pub craft_role: String,
    pub display_role: String,
    /// From `primary_surface_for(craft_role)` — the iOS routing key.
    pub primary_surface: String,
    pub group_id: String,
    pub board_id: String,
    pub board_name: String,
}

/// Seed the whole cast under `owner_node_id` in `tenant_id`, returning the routing
/// manifest (one row per persona). Idempotent. Every note row is tenant + author +
/// author_role stamped.
pub fn seed_personas(tenant_id: &str, owner_node_id: &str) -> Result<Vec<PersonaManifest>> {
    let tenant = if tenant_id.is_empty() { "seedtok" } else { tenant_id };
    let now = chrono::Utc::now().timestamp();

    // Idempotent truncate (cascades workspaces/boards/cells/files) + orphan prune, exactly
    // as seed_demo does for its managed groups.
    let _ = storage::group_delete(SEEDTOK_GROUP_ID);
    let _ = storage::workflow_state_prune_orphans();

    // The owning group + workspace (owner-stamped + roster row when a real identity seeds).
    storage::group_insert_simple(SEEDTOK_GROUP_ID, SEEDTOK_GROUP_NAME, "person.3.fill", "#F59E0B")
        .map_err(|e| anyhow!("group_insert_simple(seedtok): {e}"))?;
    let ws_owner: Option<&str> = if owner_node_id.is_empty() { None } else { Some(owner_node_id) };
    if !owner_node_id.is_empty() {
        storage::group_set_owner(SEEDTOK_GROUP_ID, owner_node_id)
            .map_err(|e| anyhow!("group_set_owner(seedtok): {e}"))?;
        storage::member_seen(SEEDTOK_GROUP_ID, owner_node_id, now)
            .map_err(|e| anyhow!("member_seen(seedtok): {e}"))?;
    }
    let (default_ws, _plugins) = storage::provision_group_workspaces(SEEDTOK_GROUP_ID, ws_owner)
        .map_err(|e| anyhow!("provision_group_workspaces(seedtok): {e}"))?;
    let ws = default_ws.id;

    let mut manifest = Vec::with_capacity(SEED_PERSONAS.len());
    for p in &SEED_PERSONAS {
        seed_persona_board(&ws, tenant, p, now)?;
        let surface = primary_surface_for(p.craft_role).to_string();
        tracing::info!(
            "obs seedtok_persona token={} craft_role={} display_role={} primary_surface={} board={} tenant={}",
            p.token, p.craft_role, p.display_role, surface, p.board_id, tenant
        );
        manifest.push(PersonaManifest {
            token: p.token.to_string(),
            display: p.display.to_string(),
            craft_role: p.craft_role.to_string(),
            display_role: p.display_role.to_string(),
            primary_surface: surface,
            group_id: SEEDTOK_GROUP_ID.to_string(),
            board_id: p.board_id.to_string(),
            board_name: p.board_name.to_string(),
        });
    }
    tracing::info!(
        "obs seedtok_seeded personas={} group={} tenant={} owner={}",
        manifest.len(), SEEDTOK_GROUP_ID, tenant, if owner_node_id.is_empty() { "-" } else { owner_node_id }
    );
    Ok(manifest)
}

/// Seed one persona's home board: a tiny deployed+pinned workflow, the craft-stamped note,
/// the bound clip asset, and (review roles) a v2 review version artifact.
fn seed_persona_board(ws: &str, tenant: &str, p: &SeedPersona, now: i64) -> Result<()> {
    let board = p.board_id;
    storage::board_insert_simple(board, ws, p.board_name, now)
        .map_err(|e| anyhow!("board_insert_simple({board}): {e}"))?;

    // A minimal, coherent 3-step workflow so the board is a real deployed workflow.
    let clip = "big-buck-bunny.mp4";
    let steps: [(&str, &str, &str, &[&str]); 3] = [
        ("Ingest the source master into the workspace.", "ingest", "lens", &[]),
        ("QC / probe: run the cyan-media probe tool on the source and report container, codec, resolution, and duration.", "qc-probe", "lens", &["ingest"]),
        ("Package: assemble the delivery and write the sidecar.", "package", "manual", &["qc-probe"]),
    ];
    for (i, (text, step_id, executor, deps)) in steps.iter().enumerate() {
        let meta = serde_json::json!({
            "pipeline": {
                "step_id": step_id, "depends_on": deps, "executor": executor,
                "model": "cyan-lens", "timeout_seconds": 300, "retry_count": 1,
                "auto_advance": false, "notifications": [],
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
    crate::workflow::mark_deployed(board, true, now)
        .map_err(|e| anyhow!("mark_deployed({board}): {e}"))?;
    storage::board_meta_set_pinned(board, true, now)
        .map_err(|e| anyhow!("board_meta_set_pinned({board}): {e}"))?;

    // The bound clip as the board's primary asset.
    storage::file_insert_simple(
        &format!("{board}-asset"), Some(SEEDTOK_GROUP_ID), Some(ws), Some(board),
        clip, &format!("seed-{board}"), 10_000_000, None, now,
    )
    .map_err(|e| anyhow!("file_insert_simple({board}-asset): {e}"))?;

    // The craft-stamped note — tenant + author + author_role provenance on every field.
    let note = NoteDTO {
        id: format!("{board}-note"),
        board_id: board.to_string(),
        tenant_id: tenant.to_string(),
        author_id: p.token.to_string(),
        author_name: p.display.to_string(),
        text: p.note_text.to_string(),
        created_at: now,
        updated_at: now,
        scope: "board".to_string(),
        kind: p.note_kind.to_string(),
        anchor_kind: None,
        anchor_id: None,
        origin_ref: None,
        payload: None,
        author_role: Some(p.craft_role.to_string()),
    };
    storage::note_upsert(&note).map_err(|e| anyhow!("note_upsert({board}-note): {e}"))?;

    // Review-facing roles: a v2 review version artifact + a decision note referencing it.
    if p.review {
        storage::file_insert_simple(
            &format!("{board}-v2"), Some(SEEDTOK_GROUP_ID), Some(ws), Some(board),
            &format!("{}-v2.mp4", board), &format!("seed-{board}-v2"), 12_000_000, None, now,
        )
        .map_err(|e| anyhow!("file_insert_simple({board}-v2): {e}"))?;
        let review_note = NoteDTO {
            id: format!("{board}-review"),
            board_id: board.to_string(),
            tenant_id: tenant.to_string(),
            author_id: p.token.to_string(),
            author_name: p.display.to_string(),
            text: "Review: v2 posted for approval — see the comments on the latest version.".to_string(),
            created_at: now,
            updated_at: now,
            scope: "board".to_string(),
            kind: "decision".to_string(),
            anchor_kind: None,
            anchor_id: None,
            origin_ref: None,
            payload: None,
            author_role: Some(p.craft_role.to_string()),
        };
        storage::note_upsert(&review_note)
            .map_err(|e| anyhow!("note_upsert({board}-review): {e}"))?;
    }
    Ok(())
}
