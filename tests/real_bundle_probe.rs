//! Probe: the REAL forge frameio bundle unpacks + lists tools in autocomplete.
use std::sync::Once;
use cyan_backend::models::core::Group;
use cyan_backend::{storage, workflow};
static DB_INIT: Once = Once::new();
fn ensure_db() {
    DB_INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("probe.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            cyan_backend::ensure_schema(&conn).expect("engine schema");
        }
        storage::init_db(path.to_str().expect("utf8")).expect("init_db");
        let plugins = tempfile::tempdir().expect("plugins dir");
        unsafe { std::env::set_var("CYAN_PLUGINS_ROOT", plugins.path()) };
        std::mem::forget(plugins);
        std::mem::forget(dir);
    });
}
#[test]
fn real_frameio_bundle_lists_tools() {
    ensure_db();
    let Ok(bytes) = std::fs::read("/tmp/fb.cyanplugin") else {
        eprintln!("SKIP: no live bundle at /tmp/fb.cyanplugin (fetch via the marketplace first)");
        return;
    };
    let g = Group { id: "gp".into(), name: "GP".into(), icon: "i".into(), color: "#0FF".into(),
                    created_at: 0 };
    storage::group_insert(&g).expect("group");
    let (ws, _) = storage::provision_group_workspaces("gp", None).expect("ws");
    storage::board_insert("bp", &ws.id, "B", 0).expect("board");
    storage::install_plugin_bundle("gp", "frameio", &bytes).expect("install");
    let unpacked = storage::ensure_bundle_unpacked("frameio");
    eprintln!("unpacked: {unpacked:?}");
    let idx = workflow::filter_autocomplete("bp", "use @frameio.");
    let vals: Vec<&str> = idx.plugins.iter().map(|e| e.value.as_str()).collect();
    eprintln!("values: {vals:?}");
    assert!(vals.contains(&"frameio.upload_file"), "got {vals:?}");
    assert!(vals.contains(&"frameio.list_comments"), "got {vals:?}");
}

/// The picker shows 6 rows: the CURATED verbs must occupy the front of the
/// list — machine-generated endpoint names rank last (found live: upload_file
/// was 27th and never rendered).
#[test]
fn curated_tools_rank_before_generated_names() {
    ensure_db();
    // Reuses the install from the first test when it ran first; make it
    // self-sufficient either way.
    let Ok(bytes) = std::fs::read("/tmp/fb.cyanplugin") else {
        eprintln!("SKIP: no live bundle at /tmp/fb.cyanplugin (fetch via the marketplace first)");
        return;
    };
    let g = Group { id: "gp2".into(), name: "GP2".into(), icon: "i".into(), color: "#0FF".into(),
                    created_at: 0 };
    let _ = storage::group_insert(&g);
    let (ws, _) = storage::provision_group_workspaces("gp2", None).expect("ws");
    storage::board_insert("bp2", &ws.id, "B2", 0).expect("board");
    storage::install_plugin_bundle("gp2", "frameio", &bytes).expect("install");
    let idx = workflow::filter_autocomplete("bp2", "use @frameio.");
    let first6: Vec<&str> = idx.plugins.iter().take(6).map(|e| e.value.as_str()).collect();
    for flagship in ["frameio.upload_file", "frameio.list_comments", "frameio.create_comment"] {
        assert!(
            first6.contains(&flagship),
            "{flagship} must be in the picker's visible 6; got {first6:?}"
        );
    }
}
