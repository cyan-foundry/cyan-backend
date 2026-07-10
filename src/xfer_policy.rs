// cyan-backend/src/xfer_policy.rs
//
// Relay policy for large media (G8 hardening, fix 5): the relay is SIGNALING
// infrastructure, not a media plane. A multi-GB dailies transfer over the small
// relay would saturate it for every user; above a size threshold a transfer whose
// only path to the peer is the relay is REFUSED with a typed error, unless the
// operator explicitly opts in (`CYAN_XFER_RELAY_OK=1`). Direct and mDNS-LAN
// connections are never affected — the check keys on the CONNECTION PATH, not on
// the relay mode configured at bind time.
//
// The decision is a pure function (`check_relay_path`) so tests fixture it
// directly; `enforce` is the env-wired form the transfer paths call.

use std::fmt;

/// Default size above which a relay-only path is refused (256 MB).
pub const DEFAULT_RELAY_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Typed refusal — surfaces to the app as a clean `FileDownloadFailed`, never a
/// mid-transfer stall on a saturated relay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayRefused {
    pub size: u64,
    pub threshold: u64,
}

impl fmt::Display for RelayRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "transfer of {} bytes exceeds the relay ceiling ({} bytes): \
             direct/LAN required for large media; relay opt-in via CYAN_XFER_RELAY_OK=1",
            self.size, self.threshold
        )
    }
}

impl std::error::Error for RelayRefused {}

/// The size threshold in effect: `CYAN_XFER_RELAY_MAX` (bytes) or the 256 MB default.
pub fn relay_max_bytes() -> u64 {
    std::env::var("CYAN_XFER_RELAY_MAX")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_RELAY_MAX_BYTES)
}

/// Whether the operator opted large transfers into the relay (`CYAN_XFER_RELAY_OK=1`).
pub fn relay_opt_in() -> bool {
    std::env::var("CYAN_XFER_RELAY_OK").ok().as_deref() == Some("1")
}

/// The PURE policy decision: refuse iff the transfer is above `threshold`, the only
/// path is the relay, and the operator has not opted in. Size 0 (unknown) passes —
/// the preflight can't refuse what it can't measure; the path check re-runs when a
/// header reveals the size.
pub fn check_relay_path(
    total_size: u64,
    relay_only: bool,
    threshold: u64,
    opt_in: bool,
) -> Result<(), RelayRefused> {
    if relay_only && !opt_in && total_size > threshold {
        return Err(RelayRefused {
            size: total_size,
            threshold,
        });
    }
    Ok(())
}

/// The env-wired form the transfer paths call.
pub fn enforce(total_size: u64, relay_only: bool) -> Result<(), RelayRefused> {
    check_relay_path(total_size, relay_only, relay_max_bytes(), relay_opt_in())
}
