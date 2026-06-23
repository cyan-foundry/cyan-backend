//! Substrate W11 / X-CUT — LAN/local P2P is NEVER gated by licensing.
//!
//! The headline graceful-offline property: with the internet down AND the
//! license expired, two same-LAN (loopback) peers still discover each other and
//! sync a delta end-to-end. Local collaboration must never depend on a reachable
//! license server or a live entitlement — only genuinely-cloud paid surfaces
//! gate.
//!
//! We install an EXPIRED `LicenseGate` process-wide, prove it DENIES a cloud
//! surface (so the gate is real and active), then run the offline discovery +
//! delta-sync slice and assert it converges anyway. The sync path never consults
//! the gate, so an expired/absent license can never break LAN use.
//!
//! `RelayPolicy::Disabled` + `DiscoveryPolicy::MdnsOnly`, bounded waits, iroh 0.95.

mod support;

use cyan_backend::licensing::{install_gate, CloudAction, LicenseGate};
use cyan_backend::models::events::NetworkEvent;
use cyan_identity::{Entitlement, Features, Meter, Plan};
use support::{
    meet, serial, spawn_mesh, unique_discovery_key, unique_group_id, DiscoveryPolicy, NodeCfg,
    RelayPolicy, SYNC_TIMEOUT,
};

fn offline_cfg() -> NodeCfg {
    NodeCfg {
        relay: RelayPolicy::Disabled,
        discovery: DiscoveryPolicy::MdnsOnly,
        discovery_key: unique_discovery_key(),
    }
}

/// An expired trial entitlement: every paid feature granted, but the trial clock
/// ran out long ago — so cloud surfaces gate while local read stays allowed.
fn expired_trial() -> Entitlement {
    Entitlement {
        tenant: "acme".to_string(),
        plan: Plan::Trial,
        seats: 5,
        features: Features::all(),
        // Trial expiry in the distant past — definitively lapsed.
        trial_expiry: Some(1),
        meter: Meter {
            included_minutes: 1_000,
            rate_cents_per_minute: 5,
        },
    }
}

#[tokio::test]
async fn lan_collab_not_gated_offline() {
    let _serial = serial().await;

    // Install an EXPIRED license process-wide BEFORE any mesh work.
    install_gate(LicenseGate::new(expired_trial()));

    // The gate is real and active: a genuinely-cloud paid surface is DENIED.
    assert!(
        cyan_backend::licensing::gate_cloud_action(CloudAction::RunWorkflow).is_err(),
        "an expired license must gate the cloud Lens run"
    );

    // …yet LAN/local P2P collaboration is fully unaffected. No internet
    // (relay disabled, mDNS-only) + an expired license, two loopback peers:
    let cfg = offline_cfg();
    assert!(matches!(cfg.relay, RelayPolicy::Disabled));
    let nodes = spawn_mesh(2, cfg).await.expect("offline mesh spawns");
    let group = unique_group_id();
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("nodes discover each other offline despite the expired license");

    // A delta propagates end-to-end with the license expired and no internet.
    nodes[0].broadcast(
        &group,
        NetworkEvent::WhiteboardElementAdded {
            id: "gated-offline-elem-1".to_string(),
            board_id: "board-1".to_string(),
            element_type: "rectangle".to_string(),
            x: 1.0,
            y: 1.0,
            width: 10.0,
            height: 10.0,
            z_index: 1,
            style_json: None,
            content_json: None,
            created_at: 1,
            updated_at: 1,
        },
    );
    nodes[1]
        .wait_network(
            |e| matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if id == "gated-offline-elem-1"),
            SYNC_TIMEOUT,
        )
        .await
        .expect("delta propagates on the LAN despite the expired license");

    for n in nodes {
        n.shutdown().await;
    }
}
