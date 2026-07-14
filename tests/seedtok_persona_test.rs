//! ROLE→SEEDTOK — the multi-persona seed. Proves the cast seeds coherently, every
//! persona maps to its `primary_surface_for` landing, and every seeded note carries
//! tenant + author + author_role provenance (the guardrail).

use std::path::Path;
use std::sync::Once;

use cyan_backend::role_templates::primary_surface_for;
use cyan_backend::seed_personas::{seed_personas, SEED_PERSONAS};
use cyan_backend::{models::dto::production_role_valid, storage};

static DB_INIT: Once = Once::new();

fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("seedtok_persona.db");
        init_base_schema(&path).expect("base schema");
        storage::init_db(path.to_str().expect("utf8 db path")).expect("init_db");
        std::mem::forget(dir);
        // A hermetic media root with a stand-in playable asset (board_video_media only
        // STATS the file, never decodes), so the review-media bind is deterministic and
        // independent of the machine's ~/.cyan-phase3/media. Process-global on purpose.
        let media = tempfile::tempdir().expect("media tempdir");
        std::fs::write(media.path().join("sig_source.mp4"), b"\x00\x00seedtok-fake-mp4")
            .expect("write fake asset");
        unsafe { std::env::set_var("CYAN_MEDIA_ROOT", media.path()) };
        std::mem::forget(media);
    });
}

fn init_base_schema(db_path: &Path) -> Result<(), rusqlite::Error> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, icon TEXT, color TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY, group_id TEXT NOT NULL, name TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY, workspace_id TEXT, group_id TEXT, board_id TEXT,
            type TEXT NOT NULL, name TEXT NOT NULL, hash TEXT, data TEXT, size INTEGER,
            source_peer TEXT, local_path TEXT, created_at INTEGER NOT NULL,
            board_mode TEXT DEFAULT 'canvas'
        );
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL, content TEXT, output TEXT,
            collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

#[test]
fn persona_cast_shape_and_surface_mapping() {
    // Every persona rides a real craft-role slug; display_role differs only for post_super.
    assert_eq!(SEED_PERSONAS.len(), 6);
    for p in &SEED_PERSONAS {
        assert!(production_role_valid(p.craft_role), "{} craft role in vocab", p.token);
        assert!(p.token.starts_with("seedtok_"), "token convention");
        if p.token == "seedtok_post_super" {
            assert_eq!(p.craft_role, "studio_exec", "post_super rides studio_exec");
            assert_eq!(p.display_role, "post_super", "but is DISPLAYED as post_super");
        } else {
            assert_eq!(p.craft_role, p.display_role, "display == craft for {}", p.token);
        }
    }
    // Tokens unique.
    let mut toks: Vec<&str> = SEED_PERSONAS.iter().map(|p| p.token).collect();
    toks.sort_unstable();
    toks.dedup();
    assert_eq!(toks.len(), 6, "tokens are unique");
}

#[test]
fn seed_is_provenance_stamped_and_maps_surfaces() {
    ensure_db();
    let tenant = "seedtok";
    let manifest = seed_personas(tenant, "owner-node-abc").expect("seed");
    assert_eq!(manifest.len(), 6);

    let expected_surfaces = [
        ("seedtok_post_super", "board_wall"),
        ("seedtok_producer", "shows"),
        ("seedtok_director", "review_player"),
        ("seedtok_editor", "notebook"),
        ("seedtok_asseditor", "ae_queue"),
        ("seedtok_colorist", "notebook"),
    ];
    for (token, surface) in expected_surfaces {
        let row = manifest.iter().find(|m| m.token == token).expect("persona present");
        assert_eq!(row.primary_surface, surface, "{token} lands on {surface}");
        // The surface is exactly what the deterministic map yields for the craft role.
        assert_eq!(row.primary_surface, primary_surface_for(&row.craft_role));

        // The persona's stamped note carries tenant + author + author_role provenance.
        let notes = storage::note_list_by_board(&row.board_id, tenant).expect("notes");
        assert!(!notes.is_empty(), "{token} board has a seeded note");
        let craft = notes.iter().find(|n| n.id.ends_with("-note")).expect("craft note");
        assert_eq!(craft.tenant_id, tenant, "tenant stamped");
        assert_eq!(craft.author_id, token, "author stamped");
        assert_eq!(
            craft.author_role.as_deref(),
            Some(row.craft_role.as_str()),
            "{token} author_role provenance == craft role"
        );
    }

    // colorist carries colorist author_role provenance specifically.
    let colorist = manifest.iter().find(|m| m.token == "seedtok_colorist").unwrap();
    let cnotes = storage::note_list_by_board(&colorist.board_id, tenant).expect("notes");
    assert!(
        cnotes.iter().any(|n| n.author_role.as_deref() == Some("colorist")),
        "colorist provenance present"
    );

    // Review-facing roles seeded a v2 review note; editor/AE/post_super did not.
    for token in ["seedtok_producer", "seedtok_director", "seedtok_colorist"] {
        let row = manifest.iter().find(|m| m.token == token).unwrap();
        let notes = storage::note_list_by_board(&row.board_id, tenant).unwrap();
        assert!(notes.iter().any(|n| n.id.ends_with("-review")), "{token} has a review version note");
    }
    for token in ["seedtok_editor", "seedtok_asseditor", "seedtok_post_super"] {
        let row = manifest.iter().find(|m| m.token == token).unwrap();
        let notes = storage::note_list_by_board(&row.board_id, tenant).unwrap();
        assert!(!notes.iter().any(|n| n.id.ends_with("-review")), "{token} has NO review note");
    }
}

#[test]
fn review_boards_bind_a_playable_master() {
    ensure_db();
    let manifest = seed_personas("seedtok", "owner-media").expect("seed");

    // Every review-facing board resolves a master that ACTUALLY EXISTS on disk — the real
    // review-loop asset — through the SAME path the app's Video-face detection calls, so the
    // face materializes AND plays (director → review player, not a black poster).
    for token in ["seedtok_director", "seedtok_producer", "seedtok_colorist"] {
        let row = manifest.iter().find(|m| m.token == token).unwrap();
        let media = cyan_backend::pipeline_executor::board_video_media(&row.board_id);
        let master = media.get("master_uri").and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("{token} board has a master_uri: {media}"));
        assert!(master.starts_with('/'), "{token} master is an absolute path: {master}");
        assert!(master.ends_with("sig_source.mp4"), "reuses the review-loop asset: {master}");
        assert!(std::path::Path::new(master).is_file(), "{token} master is a REAL file: {master}");
    }

    // The editor board (non-review) binds no real master: its only media reference is the
    // bare seed clip, which does NOT exist on disk → the player would have nothing to play,
    // so it correctly stays on its notebook landing (no materialized Video face in prod,
    // where no CYAN_MEDIA_ROOT join fabricates a phantom path).
    let editor = manifest.iter().find(|m| m.token == "seedtok_editor").unwrap();
    let em = cyan_backend::pipeline_executor::board_video_media(&editor.board_id);
    let em_master = em.get("master_uri").and_then(|v| v.as_str());
    assert!(
        em_master.map(|m| !std::path::Path::new(m).is_file()).unwrap_or(true),
        "editor board has no REAL playable master: {em}"
    );
}

#[test]
fn seed_is_idempotent() {
    ensure_db();
    let a = seed_personas("seedtok", "owner-x").expect("seed 1");
    let b = seed_personas("seedtok", "owner-x").expect("seed 2 (re-run)");
    assert_eq!(a.len(), b.len(), "re-seed yields the same cast, no dups");
    // Each board still has exactly its expected note count (no accumulation).
    let ed = b.iter().find(|m| m.token == "seedtok_editor").unwrap();
    let notes = storage::note_list_by_board(&ed.board_id, "seedtok").unwrap();
    assert_eq!(notes.len(), 1, "editor board has exactly its one craft note after re-seed");
}
