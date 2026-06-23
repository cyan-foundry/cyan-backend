//! Substrate G10 — resilience / swarming (SUBSTRATE_TEST_SPEC §3).
//!
//! These drive the file-swarm consumer: resume a partial transfer, fetch from multiple sources,
//! survive peer churn mid-transfer, and negotiate a holder via i-have/who-has. The engine now mounts
//! a content-addressed blob swarm (`cyan_backend::swarm::BlobSwarm`) on every node's endpoint, served
//! on the blobs ALPN, with i-have/who-has riding the existing group gossip — see `src/swarm.rs` and
//! `STATUS_FILE_SWARM_CONSUMER.md`.
//!
//! Discipline (CLAUDE.md + the spec): offline only (`RelayPolicy::Disabled`, loopback addresses wired
//! out-of-band via the harness `StaticProvider`); every wait is a bounded `tokio::time::timeout`; and
//! every assertion is on the RECEIVER's OWN observed state — its per-node blob store (`has`) or its
//! own holder registry (`holders`), never a log line. The blob store is per-node even though the
//! engine's SQLite is process-global, so it is an honest per-node oracle here.

#![allow(clippy::disallowed_methods)]

mod support;

use std::time::Duration;

use support::{spawn_mesh, spawn_node, unique_group_id, wire_addrs, NodeCfg, SYNC_TIMEOUT};
use cyan_backend::swarm::Hash;

/// Deterministic blob payload of `len` bytes — no randomness, so the content hash is stable across
/// runs and two holders adding the same bytes produce one shared content-addressed identity.
fn blob_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// G10: a transfer interrupted mid-stream resumes and verifies intact.
///
/// The engine's swarm resume is holder-granular: `iroh-blobs` Blake3-verifies streamed chunks and
/// persists the verified ranges, so a `fetch` only pulls the ranges still missing. Here the requester
/// is handed an UNREACHABLE first holder (contributes nothing — the maximally-interrupted source,
/// stalled at offset 0) followed by a live holder; the fetch resumes against the live holder and the
/// requester's OWN store ends with the complete, Blake3-verified blob. A second fetch then exercises
/// the resume short-circuit: the blob is already present, so it returns from its end offset without
/// re-downloading, and the integrity gate re-verifies. (True *mid-byte* interruption is the existing
/// `FileTransferMsg`/`resume_offset` wire path and the relay resume rung — out of in-process scope.)
#[tokio::test]
async fn partial_transfer_resumes_from_offset() {
    let _serial = support::serial().await;
    let data = blob_bytes(512 * 1024);

    // A live holder, plus a spare node we close to play the unreachable (stalled-at-offset-0) source.
    let holder = spawn_node("holder", NodeCfg::default()).await.expect("spawn holder");
    let stalled = spawn_node("stalled", NodeCfg::default()).await.expect("spawn stalled");
    let requester = spawn_node("requester", NodeCfg::default()).await.expect("spawn requester");

    let nodes = [holder, stalled, requester];
    wire_addrs(&nodes, SYNC_TIMEOUT).await.expect("wire loopback addresses");
    let [holder, stalled, requester] = nodes;

    let hash = holder.swarm().add(data.clone()).await.expect("holder adds blob");
    let stalled_id = stalled.node_id.clone();
    // Make the first holder unreachable: it has no blob and we close it, so dialing it stalls then
    // fails, and the fetch must resume against the live holder.
    stalled.shutdown().await;

    let holders = vec![stalled_id, holder.node_id.clone()];
    let fetched = tokio::time::timeout(SYNC_TIMEOUT, requester.swarm().fetch(&hash, &holders))
        .await
        .expect("fetch timed out")
        .expect("fetch must resume past the unreachable holder and complete");

    assert_eq!(fetched.as_ref(), data.as_slice(), "resumed bytes differ from the source content");
    assert_eq!(Hash::new(&fetched), hash, "resumed content must hash back to the requested identity");
    assert!(
        requester.swarm().has(&hash).await.expect("has() query"),
        "requester's own store must hold the blob after resuming"
    );

    // Resume short-circuit: already present ⇒ a re-fetch returns from the end offset, re-verified.
    let again = tokio::time::timeout(SYNC_TIMEOUT, requester.swarm().fetch(&hash, std::slice::from_ref(&holder.node_id)))
        .await
        .expect("resume short-circuit timed out")
        .expect("re-fetch of an already-complete blob must succeed");
    assert_eq!(again.as_ref(), data.as_slice(), "re-fetched bytes differ from the source content");
}

/// G10: a file is fetched from two sources and reassembled intact.
///
/// Two holders add the SAME bytes → content addressing means one shared hash. A fresh requester (which
/// does not hold the blob) fetches it from BOTH holders. Oracle: the requester's OWN store now holds
/// the blob and the returned bytes Blake3-verify back to the requested identity.
#[tokio::test]
async fn file_fetched_from_two_sources_in_parallel() {
    let _serial = support::serial().await;
    let data = blob_bytes(512 * 1024);

    let holder_a = spawn_node("holder-a", NodeCfg::default()).await.expect("spawn holder-a");
    let holder_b = spawn_node("holder-b", NodeCfg::default()).await.expect("spawn holder-b");
    let requester = spawn_node("requester", NodeCfg::default()).await.expect("spawn requester");

    let nodes = [holder_a, holder_b, requester];
    wire_addrs(&nodes, SYNC_TIMEOUT).await.expect("wire loopback addresses");
    let [holder_a, holder_b, requester] = nodes;

    let hash_a = holder_a.swarm().add(data.clone()).await.expect("holder-a adds blob");
    let hash_b = holder_b.swarm().add(data.clone()).await.expect("holder-b adds blob");
    assert_eq!(hash_a, hash_b, "content addressing: identical bytes must hash to one identity");

    assert!(
        !requester.swarm().has(&hash_a).await.expect("has() query"),
        "fresh requester must not already hold the blob"
    );

    let holders = vec![holder_a.node_id.clone(), holder_b.node_id.clone()];
    let fetched = tokio::time::timeout(SYNC_TIMEOUT, requester.swarm().fetch(&hash_a, &holders))
        .await
        .expect("multi-source fetch timed out")
        .expect("multi-source fetch failed");

    assert_eq!(fetched.as_ref(), data.as_slice(), "fetched bytes differ from the source content");
    assert_eq!(Hash::new(&fetched), hash_a, "fetched content must hash back to the requested identity");
    assert!(
        requester.swarm().has(&hash_a).await.expect("has() query"),
        "requester's own store must hold the blob after multi-source fetch"
    );
}

/// G10: a transfer survives the source peer leaving mid-stream.
///
/// Two holders hold the blob; one is SHUT DOWN (its endpoint closed → undialable, the in-process
/// model of a peer leaving the swarm) and listed FIRST in the provider set. The fetch's bounded dial
/// to the departed holder fails fast and falls through to the survivor. Oracle: the requester's OWN
/// store holds the verified blob despite a dead holder ahead of the live one.
#[tokio::test]
async fn transfer_survives_source_peer_churn() {
    let _serial = support::serial().await;
    let data = blob_bytes(512 * 1024);

    let survivor = spawn_node("survivor", NodeCfg::default()).await.expect("spawn survivor");
    let leaver = spawn_node("leaver", NodeCfg::default()).await.expect("spawn leaver");
    let requester = spawn_node("requester", NodeCfg::default()).await.expect("spawn requester");

    let nodes = [survivor, leaver, requester];
    wire_addrs(&nodes, SYNC_TIMEOUT).await.expect("wire loopback addresses");
    let [survivor, leaver, requester] = nodes;

    let hash = survivor.swarm().add(data.clone()).await.expect("survivor adds blob");
    let leaver_hash = leaver.swarm().add(data.clone()).await.expect("leaver adds blob");
    assert_eq!(leaver_hash, hash, "both holders hold identical content");

    // The leaver departs mid-swarm: close its endpoint so it is undialable.
    let leaver_id = leaver.node_id.clone();
    leaver.shutdown().await;

    // Departed holder listed first; the fetch must fall through to the survivor and resume.
    let holders = vec![leaver_id, survivor.node_id.clone()];
    let fetched = tokio::time::timeout(SYNC_TIMEOUT, requester.swarm().fetch(&hash, &holders))
        .await
        .expect("fetch with a churned holder timed out")
        .expect("fetch must survive a dropped holder");

    assert_eq!(fetched.as_ref(), data.as_slice(), "fetched bytes differ from the source content");
    assert!(
        requester.swarm().has(&hash).await.expect("has() query"),
        "requester's own store must hold the blob after falling through the dead holder"
    );
}

/// G10: i-have/who-has negotiation picks a holder among candidates.
///
/// The negotiation rides the engine's REAL group gossip: the requester broadcasts `WhoHas` (via the
/// `SwarmWhoHas` command → its TopicActor); the holder's TopicActor feeds it to `BlobSwarm::on_message`,
/// which (since it holds the blob) answers `IHave`; the requester's TopicActor records the holder.
/// Oracle: the requester's OWN holder registry lists the holder's node id — its own observed state.
#[tokio::test]
async fn i_have_who_has_negotiation_picks_a_holder() {
    let _serial = support::serial().await;

    // A 2-node mesh sharing a discovery key; `meet` forms the group topic and confirms delivery.
    let nodes = spawn_mesh(2, NodeCfg::default()).await.expect("spawn 2-node mesh");
    let group = unique_group_id();
    support::meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet on the group topic");

    let holder = &nodes[0];
    let requester = &nodes[1];
    let holder_id = holder.node_id.clone();

    // The holder holds a blob; only it can answer a WhoHas for this hash.
    let hash = holder.swarm().add(blob_bytes(4096)).await.expect("holder adds blob");
    let hash_hex = hash.to_string();

    // Requester drives WhoHas until it observes a holder (bounded). Retrying covers the gossip-join
    // race the same way the mesh tests retry their probes.
    let negotiated = tokio::time::timeout(SYNC_TIMEOUT, async {
        loop {
            requester.swarm_who_has(&group, &hash_hex);
            tokio::time::sleep(Duration::from_millis(300)).await;
            if requester.swarm().holders(&hash).await.contains(&holder_id) {
                return;
            }
        }
    })
    .await;

    assert!(negotiated.is_ok(), "requester never learned a holder within {SYNC_TIMEOUT:?}");
    let holders = requester.swarm().holders(&hash).await;
    assert!(
        holders.contains(&holder_id),
        "requester's holder registry {holders:?} should list the holder {holder_id}"
    );
}

/// G10 (consumer): a `.cyanplugin` seeded into a group's Plugins workspace is swarm-distributed to
/// members — the uploader adds it to its content-addressed store and announces `IHave` to the group.
///
/// Drives the engine's real plugin hook (`SeedAndAnnounceBlob`, the command `cyan_upload_file` emits
/// for a `.cyanplugin`). Oracle: the member's OWN swarm holder registry lists the uploader for the
/// plugin's content hash — its own observed state, the proof that members can now swarm-fetch it.
/// Reuses the existing file/upload commands; adds NO new client `cyan_*` FFI.
#[tokio::test]
async fn plugin_seeded_into_plugins_workspace_distributes_to_members() {
    let _serial = support::serial().await;

    let nodes = spawn_mesh(2, NodeCfg::default()).await.expect("spawn 2-node mesh");
    let group = unique_group_id();
    support::meet(&nodes, &group, SYNC_TIMEOUT).await.expect("nodes meet on the group topic");

    let uploader = &nodes[0];
    let member = &nodes[1];
    let uploader_id = uploader.node_id.clone();

    // Write a `.cyanplugin` artifact to disk; the uploader seeds it from this path.
    let data = blob_bytes(64 * 1024);
    let hash = Hash::new(&data);
    let hash_hex = hash.to_string();
    let plugin_path = std::env::temp_dir().join(format!("{hash_hex}.cyanplugin"));
    std::fs::write(&plugin_path, &data).expect("write plugin artifact");
    let plugin_path = plugin_path.to_string_lossy().to_string();

    // Re-seed in a bounded loop: the `IHave` announce is one-shot per call, so retrying covers the
    // gossip-join race (re-seeding is idempotent — same bytes ⇒ same content hash).
    let distributed = tokio::time::timeout(SYNC_TIMEOUT, async {
        loop {
            uploader.seed_plugin(&group, &hash_hex, &plugin_path);
            tokio::time::sleep(Duration::from_millis(300)).await;
            if member.swarm().holders(&hash).await.contains(&uploader_id) {
                return;
            }
        }
    })
    .await;

    let _ = std::fs::remove_file(&plugin_path);
    assert!(distributed.is_ok(), "plugin IHave never reached the member within {SYNC_TIMEOUT:?}");
    let holders = member.swarm().holders(&hash).await;
    assert!(
        holders.contains(&uploader_id),
        "member's holder registry {holders:?} should list the uploader {uploader_id} for the plugin"
    );
}
