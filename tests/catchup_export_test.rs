//! MESH_HARDENING §5 (incremental catch-up) + §11 (portable Group Export) — unit tests.
//!
//! These exercise the engine primitives DIRECTLY against the process-global `storage`, with
//! NO network: the snapshot frame builder ([`snapshot::build_snapshot_frames`]) is the honest
//! "pulled only the delta, not a full re-snapshot" oracle, and the bundle export/import is a
//! pure verify/decrypt/apply path. Every wait is immediate (no timers); the only shared state
//! is the DB, which we isolate by using a fresh, unique group id per test.
//!
//! What is NOT here: a real netem partition / relay rung — that is the Docker rig's job (the
//! in-process substrate suite cannot honestly isolate per-node storage; see `tests/support`).
//! The over-the-wire catch-up is covered by `tests/substrate_catchup.rs`.

mod support;

use cyan_backend::group_bundle::{self, ImportError};
use cyan_backend::identity::{Grant, Role};
use cyan_backend::models::core::Workspace;
use cyan_backend::models::protocol::SnapshotFrame;
use cyan_backend::{snapshot, storage};
use std::collections::HashSet;
use support::{ensure_db, seed_group_fixture, unique_group_id};

/// A valid, signed Member grant for `group_id` issued by `issuer_secret`.
fn grant_for(group_id: &str, issuer_secret: &[u8; 32]) -> Grant {
    Grant::issue_unchecked(group_id, Role::Member, issuer_secret, 1_000, 9_999_999_999, "nonce-1")
}

/// Add a content element with explicit version timestamps (so a `since` filter can see it).
fn add_element(board: &str, id: &str, version: i64) {
    let _ = storage::element_insert_simple(
        id, board, "rectangle", 1.0, 2.0, 3.0, 4.0, 0,
        Some("{\"fill\":\"#fff\"}"), Some("{\"text\":\"x\"}"), version, version,
    );
}

/// A group/workspace/board fixture with FULLY controlled `created_at = 1` (unlike
/// `seed_group_fixture`, whose `workspace_insert_simple` stamps `now()`), so a `since` filter
/// over the structure is deterministic. Returns the board id.
fn seed_controlled(group: &str) -> String {
    let ws = format!("{group}-ws");
    let board = format!("{group}-board");
    let _ = storage::group_insert_simple(group, "Fixture", "folder.fill", "#00AEEF");
    let _ = storage::workspace_insert(&Workspace {
        id: ws.clone(),
        group_id: group.to_string(),
        name: "Main".to_string(),
        created_at: 1,
        system: false,
    });
    let _ = storage::board_insert_simple(&board, &ws, "Canvas", 1);
    board
}

// ════════════════════════════════════════════════════════════════════════════
// §5 — incremental catch-up
// ════════════════════════════════════════════════════════════════════════════

/// A returning peer pulls ONLY the missing range (rows newer than its high-water mark), not the
/// whole group. Oracle: the frame builder yields exactly the delta rows for `since`, far fewer
/// than the full snapshot — and the full path still carries everything (the fallback intact).
#[test]
fn reconnect_pulls_only_delta_not_full_snapshot() {
    ensure_db();
    let group = unique_group_id();
    // Baseline: a controlled fixture (created_at=1) plus 5 old elements at version 1.
    let board = seed_controlled(&group);
    for i in 0..5 {
        add_element(&board, &format!("{group}-elem-{i}"), 1);
    }

    // After the peer went offline, three NEW elements are authored at a later version.
    add_element(&board, &format!("{group}-new-a"), 200);
    add_element(&board, &format!("{group}-new-b"), 200);
    add_element(&board, &format!("{group}-new-c"), 200);

    // Full snapshot (the cold-start / no-common-base fallback) carries the ENTIRE group.
    let full = snapshot::build_snapshot_frames(&group, None).expect("full frames");
    let full_rows = snapshot::frames_row_count(&full);

    // Incremental catch-up from a high-water mark of 150 carries ONLY the 3 new elements.
    let delta = snapshot::build_snapshot_frames(&group, Some(150)).expect("delta frames");
    let delta_rows = snapshot::frames_row_count(&delta);

    assert_eq!(delta_rows, 3, "incremental serves exactly the 3 missing rows, got {delta_rows}");
    assert!(
        full_rows >= 8,
        "full snapshot still carries the whole group (5 old + 3 new + structure), got {full_rows}"
    );
    assert!(delta_rows < full_rows, "the delta must be strictly smaller than the full snapshot");

    // And the delta really is just those three, by id — no stale rows leaked in.
    let delta_ids: HashSet<String> = delta
        .iter()
        .filter_map(|f| match f {
            SnapshotFrame::Content { elements, .. } => Some(elements),
            _ => None,
        })
        .flatten()
        .map(|e| e.id.clone())
        .collect();
    assert_eq!(delta_ids.len(), 3);
    assert!(delta_ids.contains(&format!("{group}-new-a")));
    assert!(!delta_ids.contains(&format!("{group}-elem-0")), "old rows must not be in the delta");
}

/// The closest reachable holder is preferred for catch-up: a direct LAN/mesh neighbor beats a
/// remoter device holder, and the configured super-peer is only the last resort.
#[test]
fn closest_holder_preferred_for_catchup() {
    // "peer-z" is the LAN/direct neighbor; "peer-a" is a remoter holder that sorts first.
    let offers: Vec<String> = vec!["peer-z".into(), "peer-a".into()];
    let lan: HashSet<String> = ["peer-z".to_string()].into_iter().collect();

    // The LAN offerer is chosen even though it is NOT the lexicographically-first offer — LAN wins.
    let pick = snapshot::pick_catchup_holder(&offers, &lan, Some("super-peer"));
    assert_eq!(pick.as_deref(), Some("peer-z"), "a direct LAN neighbor is preferred");

    // No LAN offerer ⇒ fall back to an available device holder (deterministic min), NOT the super-peer.
    let none_lan: HashSet<String> = HashSet::new();
    let pick = snapshot::pick_catchup_holder(&offers, &none_lan, Some("super-peer"));
    assert_eq!(pick.as_deref(), Some("peer-a"));

    // No offers at all ⇒ the configured super-peer is the last-resort holder.
    let pick = snapshot::pick_catchup_holder(&[], &none_lan, Some("super-peer"));
    assert_eq!(pick.as_deref(), Some("super-peer"));

    // Nothing reachable at all ⇒ no pick.
    assert_eq!(snapshot::pick_catchup_holder(&[], &none_lan, None), None);
}

// ════════════════════════════════════════════════════════════════════════════
// §11 — portable Group Export bundle
// ════════════════════════════════════════════════════════════════════════════

/// An export is XaeroID-signed, strictly scoped to the invitee's grant, encrypted to the
/// invitee, and carries NO media bytes (files are metadata-only). The honest invitee imports it.
#[test]
fn export_bundle_is_signed_and_grant_scoped() {
    ensure_db();
    let group = unique_group_id();
    let (_ws, board) = seed_group_fixture(&group, 4, 1);
    // A staged file: its bytes must NEVER appear in the bundle (hash-only metadata).
    let secret_bytes = b"TOP-SECRET-FILE-BYTES-DO-NOT-EXPORT";
    let file_hash = blake3::hash(secret_bytes).to_hex().to_string();
    let _ = storage::file_insert_simple(
        &format!("{group}-bigfile"), Some(&group), None, Some(&board),
        "secret.bin", &file_hash,
        secret_bytes.len() as u64, None, 1,
    );

    let exporter: [u8; 32] = [7u8; 32];
    let invitee: [u8; 32] = [8u8; 32];
    let invitee_pub = group_bundle::invitee_pubkey_hex(&invitee);
    let grant = grant_for(&group, &exporter);

    let bundle =
        group_bundle::export_group(&group, &grant, &invitee_pub, &exporter, 5_000).expect("export");

    // Signed by the exporter, scoped to exactly this group, watermark stamped.
    assert_eq!(bundle.group_id, group);
    assert_eq!(bundle.grant.group_id, group, "embedded grant scopes the same group");
    assert_eq!(bundle.synced_as_of, 5_000);
    assert!(!bundle.signature.is_empty());

    // No media bytes leak: the secret file content must not be anywhere in the serialized bundle.
    let wire = bundle.to_json();
    assert!(
        !wire.contains("TOP-SECRET-FILE-BYTES"),
        "bundle must never contain media bytes, only hash metadata"
    );
    // The snapshot payload is sealed (ciphertext), so plaintext field values aren't in the clear.
    assert!(!wire.contains("\"rectangle\""), "snapshot payload must be encrypted, not plaintext");

    // The honest invitee verifies + decrypts; the recovered frames carry exactly this group.
    let opened = group_bundle::verify_and_open(&bundle, &invitee).expect("invitee opens bundle");
    assert_eq!(opened.group_id, group);
    assert!(opened
        .frames
        .iter()
        .any(|f| matches!(f, SnapshotFrame::Structure { group: g, .. } if g.id == group)));

    // Exporting under a grant for a DIFFERENT group is refused at the source (never over-share).
    let foreign_grant = grant_for("some-other-group-id", &exporter);
    assert!(group_bundle::export_group(&group, &foreign_grant, &invitee_pub, &exporter, 5_000).is_err());
}

/// Import rejects an unsigned/forged bundle AND a validly-signed-but-out-of-scope bundle.
#[test]
fn import_rejects_unsigned_or_out_of_scope_bundle() {
    ensure_db();
    let group = unique_group_id();
    let _ = seed_group_fixture(&group, 2, 0);

    let exporter: [u8; 32] = [7u8; 32];
    let invitee: [u8; 32] = [8u8; 32];
    let invitee_pub = group_bundle::invitee_pubkey_hex(&invitee);
    let grant = grant_for(&group, &exporter);
    let good = group_bundle::export_group(&group, &grant, &invitee_pub, &exporter, 5_000).expect("export");

    // 1. Forged/tampered signature → BadSignature.
    let mut forged = good.clone();
    forged.signature = "00".repeat(64);
    assert_eq!(
        group_bundle::verify_and_open(&forged, &invitee).unwrap_err(),
        ImportError::BadSignature
    );

    // 2. Tampered ciphertext (signature no longer covers it) → BadSignature, payload never opened.
    let mut tampered = good.clone();
    tampered.sealed = format!("ff{}", &good.sealed[2..]);
    assert_eq!(
        group_bundle::verify_and_open(&tampered, &invitee).unwrap_err(),
        ImportError::BadSignature
    );

    // 3. Validly-signed but the embedded grant is for ANOTHER group → OutOfScope (no over-share).
    let mut out_of_scope = good.clone();
    out_of_scope.grant = grant_for("a-totally-different-group", &exporter);
    out_of_scope.sign(&exporter); // re-sign so the OUTER signature is valid…
    assert_eq!(
        group_bundle::verify_and_open(&out_of_scope, &invitee).unwrap_err(),
        ImportError::OutOfScope, // …yet scope enforcement still refuses it.
    );

    // 4. The wrong recipient cannot decrypt even a perfectly-signed bundle.
    let stranger: [u8; 32] = [42u8; 32];
    assert_eq!(
        group_bundle::verify_and_open(&good, &stranger).unwrap_err(),
        ImportError::Undecryptable
    );
}

/// Import seeds the baseline into storage and stamps "synced as of T", so §5 catch-up will
/// reconcile the gap on first online contact. We prove the seam by removing the group locally,
/// importing the bundle, and asserting the group + content are back AND the watermark is set.
#[test]
fn import_seeds_baseline_then_reconciles_on_reconnect() {
    ensure_db();
    let group = unique_group_id();
    let (_ws, board) = seed_group_fixture(&group, 3, 0);
    add_element(&board, &format!("{group}-keep"), 50);

    let exporter: [u8; 32] = [7u8; 32];
    let invitee: [u8; 32] = [8u8; 32];
    let invitee_pub = group_bundle::invitee_pubkey_hex(&invitee);
    let grant = grant_for(&group, &exporter);
    let bundle =
        group_bundle::export_group(&group, &grant, &invitee_pub, &exporter, 7_777).expect("export");

    // Wipe the group locally — the invitee's device does NOT have it before import.
    let _ = storage::group_delete(&group);
    assert!(storage::group_get(&group).expect("get").is_none(), "group is gone pre-import");

    // Air-gapped import re-seeds the baseline …
    let imported = group_bundle::import_group(&bundle, &invitee).expect("import");
    assert_eq!(imported, group);
    assert!(storage::group_get(&group).expect("get").is_some(), "group is seeded by import");
    let ws_ids: Vec<String> = storage::workspace_list_by_group(&group)
        .unwrap_or_default()
        .into_iter()
        .map(|w| w.id)
        .collect();
    let boards = storage::board_list_by_workspaces(&ws_ids).unwrap_or_default();
    let board_ids: Vec<String> = boards.iter().map(|b| b.id.clone()).collect();
    let elems = storage::element_list_by_boards(&board_ids).unwrap_or_default();
    assert!(elems.iter().any(|e| e.id == format!("{group}-keep")), "content re-seeded");

    // … and stamps the watermark that drives the §5 catch-up on reconnect.
    assert_eq!(
        storage::group_sync_state_get(&group),
        Some(7_777),
        "synced-as-of watermark recorded for reconnect reconciliation"
    );
}

/// The whole import path runs with NO network: no `NetworkActor`, no endpoint, no relay — just
/// storage + crypto. (This test never constructs any network object; its success IS the proof.)
#[test]
fn airgapped_import_works_with_no_network() {
    ensure_db();
    let group = unique_group_id();
    let _ = seed_group_fixture(&group, 2, 1);

    let exporter: [u8; 32] = [7u8; 32];
    let invitee: [u8; 32] = [8u8; 32];
    let invitee_pub = group_bundle::invitee_pubkey_hex(&invitee);
    let grant = grant_for(&group, &exporter);
    let bundle =
        group_bundle::export_group(&group, &grant, &invitee_pub, &exporter, 4_242).expect("export");

    // Round-trip through the on-disk `.cyangroup` JSON body, then import — fully offline.
    let body = bundle.to_json();
    let parsed = group_bundle::GroupBundle::from_json(&body).expect("parse");
    let _ = storage::group_delete(&group);
    let imported = group_bundle::import_group(&parsed, &invitee).expect("offline import");
    assert_eq!(imported, group);
    assert!(storage::group_get(&group).expect("get").is_some());
    assert_eq!(storage::group_sync_state_get(&group), Some(4_242));
}
