// src/models/node_config.rs
//
// Per-node network configuration — the seam that lets multiple NetworkActors run
// in one process with independent relay/discovery settings (the substrate test
// harness needs this; see SUBSTRATE_TEST_SPEC §1).
//
// SHIPPING BEHAVIOR IS UNCHANGED: the production FFI init site builds a
// `NodeConfig` from the existing `RELAY_URL`/`DISCOVERY_KEY`/`BOOTSTRAP_NODE_ID`
// globals, so `relay_mode_for` reproduces the exact RelayMode the engine used
// before this config was threaded in.

use std::str::FromStr;

use iroh::{RelayMap, RelayMode, RelayUrl};

/// How a node reaches peers when a direct path is unavailable.
#[derive(Clone, Debug)]
pub enum RelayPolicy {
    /// No relay at all — `RelayMode::Disabled`. LAN/offline (G2-LAN, G9).
    Disabled,
    /// A specific relay URL — `RelayMode::Custom`. Falls back to `Default` if the
    /// URL fails to parse (preserves the engine's prior behavior).
    Url(String),
    /// n0 public relays — `RelayMode::Default`.
    Default,
}

/// How a node discovers peers.
#[derive(Clone, Debug)]
pub enum DiscoveryPolicy {
    /// mDNS only — same-LAN/loopback discovery, no bootstrap (offline-friendly).
    MdnsOnly,
    /// Dial a known bootstrap node id (hex) for gossip discovery.
    Bootstrap(String),
}

/// One node's full network config, handed to `NetworkActor::new`.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub relay: RelayPolicy,
    pub discovery: DiscoveryPolicy,
    pub discovery_key: String,
}

/// Pure mapping from a `RelayPolicy` to an iroh `RelayMode`.
///
/// `Url` parses the string; on a parse error it falls back to `RelayMode::Default`,
/// exactly as the engine did when it read `RELAY_URL` directly. No side effects so
/// it is unit-testable in isolation.
pub fn relay_mode_for(policy: &RelayPolicy) -> RelayMode {
    match policy {
        RelayPolicy::Disabled => RelayMode::Disabled,
        RelayPolicy::Url(url_str) => match RelayUrl::from_str(url_str) {
            Ok(url) => RelayMode::Custom(RelayMap::from(url)),
            Err(_) => RelayMode::Default,
        },
        RelayPolicy::Default => RelayMode::Default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_maps_to_disabled() {
        assert!(matches!(
            relay_mode_for(&RelayPolicy::Disabled),
            RelayMode::Disabled
        ));
    }

    #[test]
    fn valid_url_maps_to_custom() {
        let policy = RelayPolicy::Url("https://relay.example.com".to_string());
        assert!(matches!(relay_mode_for(&policy), RelayMode::Custom(_)));
    }

    #[test]
    fn invalid_url_falls_back_to_default() {
        // Preserves the engine's prior "warn + fall back to Default" behavior.
        let policy = RelayPolicy::Url("not a url".to_string());
        assert!(matches!(relay_mode_for(&policy), RelayMode::Default));
    }

    #[test]
    fn default_maps_to_default() {
        assert!(matches!(
            relay_mode_for(&RelayPolicy::Default),
            RelayMode::Default
        ));
    }
}
