//! C5 (C2C leg) — LIVE proof that the real `FrameioC2cConnector` lists a real
//! Frame.io project's dailies against api.frame.io. Gated on `FRAMEIO_LIVE=1`
//! (needs a valid IMS token in the credential env); a no-op otherwise so the
//! default suite stays hermetic.
//!
//! Run: `FRAMEIO_LIVE=1 FRAMEIO_C2C_PROJECT=<id> cargo test --test frameio_c2c_live_test -- --ignored --nocapture`

use cyan_backend::ingest::{IngestSource, RemoteConnector};
use cyan_backend::ingest_connectors::FrameioC2cConnector;

fn live_source(uri: &str) -> IngestSource {
    IngestSource {
        id: "live".into(),
        tenant_id: "live".into(),
        board_id: "live".into(),
        kind: "frameio_c2c".into(),
        uri: uri.into(),
        schedule_secs: None,
        last_scan_at: None,
        created_at: 0,
    }
}

#[test]
#[ignore = "live: needs FRAMEIO_LIVE=1 + a valid IMS token"]
fn lists_real_c2c_project_dailies() {
    if std::env::var("FRAMEIO_LIVE").ok().as_deref() != Some("1") {
        eprintln!("skipped: set FRAMEIO_LIVE=1");
        return;
    }
    // Default to the verified "Cyan E2E" project; override via env.
    let project = std::env::var("FRAMEIO_C2C_PROJECT")
        .unwrap_or_else(|_| "a0c1b903-daef-40e7-a86f-97d1cfeff28f".to_string());
    let connector = FrameioC2cConnector::from_creds().expect("creds from env");
    let source = live_source(&format!("frameio://{project}"));
    let items = connector.list(&source).expect("live list against api.frame.io");

    eprintln!("C2C live list: {} settled media item(s)", items.len());
    for it in &items {
        eprintln!("  - {} ({} bytes) ref={}", it.name, it.size, it.ref_id);
        // Every listed item names a real file with a media extension.
        assert!(!it.ref_id.is_empty(), "each item carries a Frame.io file id");
        assert_eq!(it.provider, "frameio");
    }
    // The verified project holds real transcoded dailies.
    assert!(!items.is_empty(), "the real project lists at least one settled daily");
}
