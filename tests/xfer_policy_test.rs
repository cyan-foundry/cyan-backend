//! Relay policy for large media (G8 hardening fix 5): the small relay is signaling,
//! not a media plane. Above the size threshold a RELAY-ONLY path is refused with a
//! typed error unless the operator opts in; direct/LAN paths are never affected.
//!
//! The decision is the pure `check_relay_path` — fixtured here across the whole
//! (size × path × opt-in) matrix, exactly the RelayPolicy combinations the docker
//! relay rig exercises end-to-end (relay rungs stay out of in-process scope per
//! SUBSTRATE_TEST_SPEC).

use cyan_backend::xfer_policy::{check_relay_path, RelayRefused, DEFAULT_RELAY_MAX_BYTES};

const GB: u64 = 1024 * 1024 * 1024;

#[test]
fn large_transfer_refuses_relay_only_path() {
    let threshold = DEFAULT_RELAY_MAX_BYTES; // 256 MB default

    // The refusal case: big media, relay-only, no opt-in → typed error naming both sizes.
    let err = check_relay_path(4 * GB, true, threshold, false)
        .expect_err("4 GB over a relay-only path must be refused");
    assert_eq!(
        err,
        RelayRefused { size: 4 * GB, threshold },
        "the typed error carries the size and the ceiling"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("direct/LAN required for large media"),
        "refusal must tell the operator what to do, got: {msg}"
    );
    assert!(
        msg.contains("CYAN_XFER_RELAY_OK"),
        "refusal must name the opt-in knob, got: {msg}"
    );

    // Exactly at the threshold passes (the policy is 'above', not 'at').
    check_relay_path(threshold, true, threshold, false)
        .expect("a transfer at the threshold rides the relay");
    // One byte above is refused.
    check_relay_path(threshold + 1, true, threshold, false)
        .expect_err("one byte above the threshold is refused on relay-only");

    // Small transfers ride the relay freely — it IS the signaling/fallback plane.
    check_relay_path(64 * 1024 * 1024, true, threshold, false)
        .expect("small transfers are unaffected");

    // Direct/mDNS-LAN paths are NEVER policy-refused, whatever the size.
    check_relay_path(20 * GB * 4, false, threshold, false)
        .expect("direct/LAN paths carry any size");

    // Operator opt-in (CYAN_XFER_RELAY_OK=1 at the call site) lifts the refusal.
    check_relay_path(4 * GB, true, threshold, true)
        .expect("explicit opt-in sends large media over the relay");

    // Unknown size (0) passes preflight — the check re-runs once a header names the
    // real size, so nothing large slips through.
    check_relay_path(0, true, threshold, false).expect("unknown size defers to the header check");

    // A configured (smaller) threshold is honored.
    let small = 8 * 1024 * 1024;
    let err = check_relay_path(small + 1, true, small, false)
        .expect_err("configured threshold gates instead of the default");
    assert_eq!(err.threshold, small);
}
