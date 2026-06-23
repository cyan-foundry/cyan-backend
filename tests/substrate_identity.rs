//! Mesh-write enforcement (identity/RBAC mesh half) — the in-process substrate rung.
//!
//! IDENTITY_RBAC_SPEC: "a peer presenting no/invalid grant is REFUSED at the write/admin/
//! serve-snapshot path. Assert on the RECEIVER's own state." Here the receiver's own state is its
//! per-node [`MeshAuthorizer`] (a fresh `Arc` per node, like `peers_per_group`) plus the absence of
//! the write on its event channel — both unaffected by the harness's shared SQLite DB.
//!
//! Two real cyan-backend nodes meet over loopback gossip; the receiver enforces grants for the
//! group. The negative test proves an un-granted peer's write is dropped at the receiver's
//! `TopicActor` write path; the positive test proves that once the receiver has verified that
//! peer's signed grant, the same write is accepted. The grant verification is the real
//! `MeshAuthorizer`/`GrantVerifier` code; only the grant-presentation *transport* (a peer
//! broadcasting its grant over group gossip) is shortcut via the exposed authorizer — that
//! gossip verb + its additive `cyan_*` FFI is the iOS-facing follow-up (see STATUS_IDENTITY_GRANTS).

mod support;

use std::time::Duration;

use cyan_backend::identity::{pubkey_hex, DenyReason, Grant, GroupRoster, Role, WriteDecision};
use cyan_backend::models::events::NetworkEvent;

use support::{meet, serial, spawn_mesh, unique_discovery_key, unique_group_id, NodeCfg, SYNC_TIMEOUT};

/// Build a board-write delta with a known id (the per-node event-channel oracle keys on this id).
fn board_write(id: &str) -> NetworkEvent {
    NetworkEvent::WhiteboardElementAdded {
        id: id.to_string(),
        board_id: "board-enforce".to_string(),
        element_type: "rectangle".to_string(),
        x: 1.0,
        y: 2.0,
        width: 3.0,
        height: 4.0,
        z_index: 0,
        style_json: None,
        content_json: None,
        created_at: 1,
        updated_at: 1,
    }
}

#[tokio::test]
async fn mesh_write_rejected_without_valid_grant() {
    let _guard = serial().await;
    let group = unique_group_id();
    let cfg = NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    };

    let nodes = spawn_mesh(2, cfg).await.expect("spawn 2-node mesh");
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("nodes form the group topic and deliver");

    let sender = &nodes[0];
    let receiver = &nodes[1];

    // Receiver turns ON grant enforcement for this group. The sender holds/presents no grant.
    receiver
        .authorizer()
        .lock()
        .expect("authorizer lock")
        .enforce_group(&group);

    // Sender broadcasts a normal board write.
    let elem_id = format!("{group}-unauthorized");
    sender.broadcast(&group, board_write(&elem_id));

    // RECEIVER-SIDE assertion #1: its authorizer denies the sender (no grant recorded).
    let decision = receiver
        .authorizer()
        .lock()
        .expect("authorizer lock")
        .authorize_write(&group, &sender.node_id);
    assert_eq!(
        decision,
        WriteDecision::Deny(DenyReason::NoGrant),
        "receiver must deny writes from a peer that presented no grant"
    );

    // RECEIVER-SIDE assertion #2: the refused write NEVER surfaces on the receiver's event
    // channel (a bounded wait that must time out — the write was dropped before persist/forward).
    let want = elem_id.clone();
    let surfaced = receiver
        .wait_network(
            move |e| matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if *id == want),
            Duration::from_secs(3),
        )
        .await;
    assert!(
        surfaced.is_err(),
        "receiver must NOT surface a mesh write from an un-granted peer (it was refused), got {surfaced:?}"
    );
}

#[tokio::test]
async fn mesh_write_allowed_with_valid_grant() {
    let _guard = serial().await;
    let group = unique_group_id();
    let cfg = NodeCfg {
        discovery_key: unique_discovery_key(),
        ..NodeCfg::default()
    };

    let nodes = spawn_mesh(2, cfg).await.expect("spawn 2-node mesh");
    meet(&nodes, &group, SYNC_TIMEOUT)
        .await
        .expect("nodes form the group topic and deliver");

    let sender = &nodes[0];
    let receiver = &nodes[1];

    // A group admin (XaeroID keypair) issues the sender a Member capability grant.
    let admin_secret = [7u8; 32];
    let admin_pk = pubkey_hex(&admin_secret);
    let mut issue_roster = GroupRoster::new();
    issue_roster.set_role(&group, &admin_pk, Role::Admin);
    let grant = Grant::issue(
        &group,
        Role::Member,
        &admin_secret,
        0,         // issued_at (irrelevant to verify)
        u64::MAX,  // never expires (clock-independent test)
        &format!("{group}-nonce-pos"),
        &issue_roster,
    )
    .expect("admin issues a member grant");

    // Receiver enforces the group, trusts the admin, and verifies+records the sender's grant.
    // (In production the sender broadcasts this grant over the group gossip; the receiver runs the
    // exact same `present_grant` verification — only that transport is shortcut here.)
    {
        let authz = receiver.authorizer();
        let mut a = authz.lock().expect("authorizer lock");
        a.enforce_group(&group);
        a.set_admin(&group, &admin_pk, Role::Admin);
        let role = a
            .present_grant(&sender.node_id, &grant)
            .expect("receiver verifies the sender's signed grant");
        assert_eq!(role, Role::Member);
    }

    // Now the sender's write must be accepted at the receiver.
    let elem_id = format!("{group}-authorized");
    sender.broadcast(&group, board_write(&elem_id));

    // RECEIVER-SIDE assertion #1: its authorizer allows the now-granted sender.
    assert!(
        receiver
            .authorizer()
            .lock()
            .expect("authorizer lock")
            .authorize_write(&group, &sender.node_id)
            .is_allowed(),
        "receiver must allow writes from a peer holding a valid write-capable grant"
    );

    // RECEIVER-SIDE assertion #2: the write DOES surface on the receiver's event channel.
    let want = elem_id.clone();
    receiver
        .wait_network(
            move |e| matches!(e, NetworkEvent::WhiteboardElementAdded { id, .. } if *id == want),
            SYNC_TIMEOUT,
        )
        .await
        .expect("receiver surfaces a mesh write from a validly-granted peer");
}
