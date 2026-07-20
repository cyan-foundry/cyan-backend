//! A5 device piece (PLAN 3.7) — `cyan_plugin_catalog` (T53, T54): the exact §9e
//! JSON over the installed-bundle root; bad manifests skipped; an empty root is
//! an empty list, never an error.

use std::ffi::CStr;

use cyan_backend::ffi::core as ffi;

fn call_catalog() -> serde_json::Value {
    let out = ffi::cyan_plugin_catalog();
    assert!(!out.is_null());
    let s = unsafe { CStr::from_ptr(out) }.to_string_lossy().to_string();
    ffi::cyan_free_string(out);
    serde_json::from_str(&s).expect("catalog is JSON")
}

const MEDIA_MANIFEST: &str = r#"{
  "name": "cyan-media",
  "version": "1.2.0",
  "runtime": "python-uv",
  "tools": [
    {
      "name": "probe",
      "when_to_use": "probe a media file",
      "io_types": { "input": ["video"], "output": ["report"] },
      "stage": "ingest",
      "side_effects": [],
      "locality": "device",
      "input_schema": {},
      "output_schema": {}
    },
    {
      "name": "deliver_master",
      "when_to_use": "send the master out",
      "io_types": { "input": ["video"], "output": ["video"] },
      "stage": "deliver",
      "side_effects": ["external_send"],
      "locality": "device",
      "input_schema": {},
      "output_schema": {}
    }
  ]
}"#;

// The two tests mutate the SAME process env (CYAN_PLUGINS_ROOT), so they run as
// one serialized fn.
#[test]
fn plugin_catalog_lists_installed_manifests_and_tolerates_bad_and_empty_roots() {
    // ── T54 first — a MISSING root is {"plugins":[]}, never an error ──
    let missing = tempfile::tempdir().expect("tempdir");
    let missing_path = missing.path().join("never-created");
    unsafe { std::env::set_var("CYAN_PLUGINS_ROOT", &missing_path) };
    assert_eq!(call_catalog(), serde_json::json!({ "plugins": [] }), "missing root ⇒ empty list");

    // An EXISTING-but-empty root too.
    let empty = tempfile::tempdir().expect("tempdir");
    unsafe { std::env::set_var("CYAN_PLUGINS_ROOT", empty.path()) };
    assert_eq!(call_catalog(), serde_json::json!({ "plugins": [] }), "empty root ⇒ empty list");

    // ── T53 — a fixture bundle dir (manifest, version + 2 tools, one
    // external_send) yields the exact §9e JSON, sorted by id then tool name;
    // a bad-manifest bundle is skipped ──
    let root = tempfile::tempdir().expect("tempdir");
    let media = root.path().join("cyan-media");
    std::fs::create_dir_all(&media).expect("mkdir");
    std::fs::write(media.join("manifest.json"), MEDIA_MANIFEST).expect("manifest");
    let broken = root.path().join("broken-plugin");
    std::fs::create_dir_all(&broken).expect("mkdir");
    std::fs::write(broken.join("manifest.json"), "{ not json").expect("bad manifest");
    unsafe { std::env::set_var("CYAN_PLUGINS_ROOT", root.path()) };

    let catalog = call_catalog();
    assert_eq!(
        catalog,
        serde_json::json!({
            "plugins": [
                {
                    "id": "cyan-media",
                    "version": "1.2.0",
                    "tools": [
                        { "name": "deliver_master", "side_effects": ["external_send"] },
                        { "name": "probe", "side_effects": [] },
                    ],
                }
            ]
        }),
        "the exact §9e shape — bad bundle skipped, tools sorted by name"
    );
}
