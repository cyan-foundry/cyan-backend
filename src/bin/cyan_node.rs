//! cyan_node — a TEST-ONLY peer binary for the multi-process substrate rig.
//!
//! Each `cyan_node` process boots a real `NetworkActor` against its OWN SQLite
//! database (`NODE_DB`), giving the substrate suite *genuine per-node storage* —
//! which the in-process harness cannot have (the engine's `storage` is a
//! process-global singleton). This is the honest path for snapshot/storage-truth
//! assertions like `late_joiner_gets_full_snapshot`: the joiner runs in its own
//! process, so counting rows in its DB really proves it received the data.
//!
//! It uses ONLY the crate's PUBLIC API (`cyan_backend::{actors, storage, models,
//! DATA_DIR}`) — no FFI, no engine edits. It is driven over a tiny line protocol
//! on stdin/stdout (one request per line; one response line per request, tagged
//! with the `@@CYAN@@` sentinel so engine stderr/stdout noise can never be mistaken
//! for a response).
//!
//! ## Environment (read once at boot)
//! - `NODE_DB`            — path to this process's SQLite DB (required).
//! - `DISCOVERY_KEY`      — gossip discovery key (default `cyan-test`).
//! - `RELAY`              — `disabled` (default) or a relay URL.
//! - `BOOTSTRAP_NODE_ID`  — optional hex node id ⇒ `DiscoveryPolicy::Bootstrap`
//!   (absent ⇒ `DiscoveryPolicy::MdnsOnly`).
//! - `DATA_DIR`           — optional download dir; defaults to `<NODE_DB dir>/data`.
//!
//! ## Control protocol (stdin → stdout, line oriented)
//! Request (one per line)                 Response (`@@CYAN@@ ` prefixed)
//! - `node_id`                            `@@CYAN@@ node_id <hex>`
//! - `addr`                               `@@CYAN@@ addr <endpoint-addr-json>`
//! - `add_peer <endpoint-addr-json>`      `@@CYAN@@ ok add_peer`
//! - `seed_empty_group <gid>`             `@@CYAN@@ ok seed_empty_group`
//! - `seed_fixture <gid>`                 `@@CYAN@@ ok seed_fixture`
//! - `join_group <gid> [bootstrap_hex]`   `@@CYAN@@ ok join_group`
//! - `wait_sync <gid> <timeout_ms>`       `@@CYAN@@ ok wait_sync` | `@@CYAN@@ timeout wait_sync`
//! - `count <kind> <gid>`                 `@@CYAN@@ count <kind> <n>`
//!   kinds: groups|workspaces|boards|elements|cells|chats|files
//! - `admin_pubkey`                       `@@CYAN@@ admin_pubkey <hex>`
//! - `enforce_group <gid>`                `@@CYAN@@ ok enforce_group` (enforce + self=Owner-admin)
//! - `set_admin <gid> <pubkey> [role]`    `@@CYAN@@ ok set_admin`
//! - `issue_grant <gid> <role> [ttl]`     `@@CYAN@@ grant <nonce> <qr>` (ttl secs; negative ⇒ expired)
//! - `revoke_grant <gid> <nonce>`         `@@CYAN@@ ok revoke_grant`
//! - `join_group_grant <gid> <boot|-> <qr>`  `@@CYAN@@ ok join_group_grant`
//!
//! ## Stress / chaos fabric verbs (Round 7)
//! - `post_edits <gid> <n> [board]`       `@@CYAN@@ ok post_edits <n>` (local insert + gossip broadcast)
//! - `seed_blob <gid> <size> [name]`      `@@CYAN@@ blob <file_id> <hash>` (hold + announce)
//! - `fetch_blob <gid> <fid> <hash> <src> <size> <to_ms>`  `@@CYAN@@ fetched <path>` | `timeout fetch_blob`
//! - `verify_blob <fid> <hash>`           `@@CYAN@@ verify ok|mismatch|missing` (blake3 re-check)
//! - `tier <peer_hex>`                    `@@CYAN@@ tier direct|relay|mixed|none|unknown`
//! - `metrics`                            `@@CYAN@@ metrics <json>` (rss_kb, gossip_recv, degree…)
//! - `quit`                               (process exits 0)
//!
//! Every wait is bounded; the binary never blocks unboundedly.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use iroh::{EndpointAddr, PublicKey, SecretKey};
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Notify;

use cyan_backend::actors::NetworkActor;
use cyan_backend::identity::{pubkey_hex, Grant, MeshAuthorizer, Role};
use cyan_backend::models::commands::NetworkCommand;
use cyan_backend::models::events::{NetworkEvent, SwiftEvent};
use cyan_backend::models::node_config::{DiscoveryPolicy, NodeConfig, RelayPolicy};
use cyan_backend::storage;

/// Response sentinel — the harness scans stdout for lines starting with this, so
/// engine logging (which goes to stderr anyway) can never be parsed as a response.
const SENTINEL: &str = "@@CYAN@@";

/// The fixed fixture shape (mirrors `network_actor_test::create_test_data`): the
/// host seeds exactly these counts; a synced joiner must end with the same.
const FIXTURE_ELEMENTS: usize = 5;
const FIXTURE_CELLS: usize = 3;
const FIXTURE_CHATS: usize = 3;
const FIXTURE_FILES: usize = 1;

#[tokio::main]
async fn main() -> Result<()> {
    // The engine logs to stderr (eprintln!); leave stdout pristine for the protocol.
    // Opt-in iroh/gossip tracing to STDERR when RUST_LOG is set (debugging the rig only;
    // stdout — the control channel — is never touched).
    if std::env::var("RUST_LOG").is_ok() {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }

    let db_path = std::env::var("NODE_DB").context("NODE_DB env required")?;
    let discovery_key = std::env::var("DISCOVERY_KEY").unwrap_or_else(|_| "cyan-test".to_string());
    let relay = match std::env::var("RELAY").as_deref() {
        Ok("disabled") | Err(_) => RelayPolicy::Disabled,
        Ok(url) => RelayPolicy::Url(url.to_string()),
    };
    let discovery = match std::env::var("BOOTSTRAP_NODE_ID") {
        Ok(id) if !id.is_empty() => DiscoveryPolicy::Bootstrap(id),
        _ => DiscoveryPolicy::MdnsOnly,
    };

    // Per-process storage: create base tables, then init the global DB at NODE_DB.
    init_base_schema(&db_path)?;
    storage::init_db(&db_path).map_err(|e| anyhow!("storage::init_db({db_path}): {e}"))?;

    // Optionally seed the host fixture BEFORE the actor starts. This is deliberate:
    // the engine's startup group-load auto-spawns a group TopicActor (which blocks on
    // `gossip.subscribe_and_join(..).await` until a neighbor connects). For the HOST we
    // WANT that — it makes the host host the group topic deterministically and wait for
    // the joiner. A late joiner, by contrast, must start with an EMPTY db so its command
    // loop is reachable to process `JoinGroup` (with the reachable host as a peer).
    if let Ok(gid) = std::env::var("SEED_FIXTURE")
        && !gid.is_empty()
    {
        seed_fixture(&gid)?;
    }

    // Downloads land under DATA_DIR (process-global OnceCell in the engine).
    let data_dir = std::env::var("DATA_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        PathBuf::from(&db_path)
            .parent()
            .map(|p| p.join("data"))
            .unwrap_or_else(|| PathBuf::from("data"))
    });
    std::fs::create_dir_all(&data_dir).ok();
    let _ = cyan_backend::DATA_DIR.set(data_dir);

    // Identity + channels.
    let mut rng = ChaCha8Rng::from_os_rng();
    let secret_key = SecretKey::generate(&mut rng);
    let node_id = secret_key.public().to_string();

    // Grant (capability) keypair for this node — a distinct Ed25519 keypair seeded from the
    // same 32-byte secret, used to sign/verify capability grants in the multi-process identity
    // tests. The node's `MeshAuthorizer` (grabbed below) is the honest per-process oracle.
    let grant_secret: [u8; 32] = secret_key.to_bytes();
    let admin_pubkey = pubkey_hex(&grant_secret);

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<NetworkCommand>();
    let peers_per_group = Arc::new(Mutex::new(HashMap::<String, HashSet<PublicKey>>::new()));

    let cfg = NodeConfig {
        relay,
        discovery,
        discovery_key,
    };
    let actor = NetworkActor::new(secret_key, event_tx, peers_per_group, cfg)
        .await
        .map_err(|e| anyhow!("NetworkActor::new: {e}"))?;

    // Test-support seams grabbed before the actor moves into its task.
    let endpoint = actor.endpoint();
    let static_discovery = actor.static_discovery();
    let authorizer = actor.authorizer();

    tokio::spawn(async move {
        actor.start(cmd_rx).await;
    });

    // Drain events into shared state so `wait_sync`/`fetch_blob` can observe the
    // completion event even if it arrives before the verb is issued.
    let synced: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    // file_id -> local_path for completed downloads (the fetch_blob oracle).
    let downloaded: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    {
        let synced = synced.clone();
        let downloaded = downloaded.clone();
        let notify = notify.clone();
        tokio::spawn(async move {
            while let Some(ev) = event_rx.recv().await {
                match ev {
                    SwiftEvent::SyncComplete { group_id } => {
                        if let Ok(mut s) = synced.lock() {
                            s.insert(group_id);
                        }
                        notify.notify_waiters();
                    }
                    SwiftEvent::FileDownloaded { file_id, local_path } => {
                        if let Ok(mut d) = downloaded.lock() {
                            d.insert(file_id, local_path);
                        }
                        notify.notify_waiters();
                    }
                    _ => {}
                }
            }
        });
    }

    // Control loop: read requests on stdin, write tagged responses on stdout.
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let verb = parts.next().unwrap_or("");
        let rest: Vec<&str> = parts.collect();

        if verb == "quit" {
            break;
        }

        let resp = handle_verb(
            verb,
            &rest,
            &node_id,
            &endpoint,
            &static_discovery,
            &cmd_tx,
            &synced,
            &downloaded,
            &notify,
            &authorizer,
            &grant_secret,
            &admin_pubkey,
        )
        .await
        .unwrap_or_else(|e| format!("err {verb} {e}"));

        stdout
            .write_all(format!("{SENTINEL} {resp}\n").as_bytes())
            .await?;
        stdout.flush().await?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_verb(
    verb: &str,
    rest: &[&str],
    node_id: &str,
    endpoint: &iroh::Endpoint,
    static_discovery: &iroh::discovery::static_provider::StaticProvider,
    cmd_tx: &UnboundedSender<NetworkCommand>,
    synced: &Arc<Mutex<HashSet<String>>>,
    downloaded: &Arc<Mutex<HashMap<String, String>>>,
    notify: &Arc<Notify>,
    authorizer: &Arc<std::sync::Mutex<MeshAuthorizer>>,
    grant_secret: &[u8; 32],
    admin_pubkey: &str,
) -> Result<String> {
    match verb {
        "node_id" => Ok(format!("node_id {node_id}")),

        // ── Identity / RBAC verbs (multi-process grant tests) ─────────────────────────────
        // This node's capability-grant (admin) Ed25519 pubkey hex.
        "admin_pubkey" => Ok(format!("admin_pubkey {admin_pubkey}")),

        // Turn ON grant enforcement for a group AND register this node as its Owner-admin, so
        // it can both issue grants and verify presented grants against itself.
        "enforce_group" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let mut auth = authorizer.lock().map_err(|_| anyhow!("authorizer poisoned"))?;
            auth.enforce_group(gid);
            auth.set_admin(gid, admin_pubkey, Role::Owner);
            Ok("ok enforce_group".to_string())
        }

        // Register an external admin pubkey for a group in this node's roster.
        // `set_admin <gid> <pubkey_hex> [role]` (role default: owner).
        "set_admin" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let pk = rest.get(1).ok_or_else(|| anyhow!("pubkey required"))?;
            let role = rest
                .get(2)
                .and_then(|s| Role::parse(s))
                .unwrap_or(Role::Owner);
            authorizer
                .lock()
                .map_err(|_| anyhow!("authorizer poisoned"))?
                .set_admin(gid, pk, role);
            Ok("ok set_admin".to_string())
        }

        // Issue (sign) a capability grant for a group, signed by THIS node's admin key.
        // `issue_grant <gid> <role> [ttl_secs]`; ttl_secs may be negative to mint an already
        // expired grant for negative tests (default 3600). Replies `grant <nonce> <qr>` — the
        // qr is compact JSON (no spaces), so it travels as a single token.
        "issue_grant" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let role = rest
                .get(1)
                .and_then(|s| Role::parse(s))
                .unwrap_or(Role::Member);
            let ttl: i64 = rest.get(2).and_then(|s| s.parse().ok()).unwrap_or(3600);
            let now = unix_now();
            let expiry = (now as i64 + ttl).max(0) as u64;
            // Unique nonce per issue (no Math.random in scripts; mix node + gid + time).
            let nonce = format!("{}-{}-{}", &admin_pubkey[..8], gid, now.wrapping_add(ttl as u64));
            // `issue_unchecked`: this node legitimately is the group admin (registered via
            // `enforce_group`); the receiving holder re-checks issuer-is-admin against its own
            // roster at verify time, so authority is still enforced where it matters.
            let grant = Grant::issue_unchecked(gid, role, grant_secret, now, expiry, &nonce);
            Ok(format!("grant {} {}", grant.nonce, grant.to_qr_payload()))
        }

        // Revoke a grant by (group_id, nonce) in this node's authorizer.
        "revoke_grant" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let nonce = rest.get(1).ok_or_else(|| anyhow!("nonce required"))?;
            authorizer
                .lock()
                .map_err(|_| anyhow!("authorizer poisoned"))?
                .revoke(gid, nonce);
            Ok("ok revoke_grant".to_string())
        }

        // Join a group presenting a signed grant QR. `join_group_grant <gid> <bootstrap_hex> <qr>`.
        "join_group_grant" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?.to_string();
            // `-` is the explicit "no bootstrap" sentinel (see MpNode::join_group_with_grant).
            let bootstrap_peer = rest
                .get(1)
                .filter(|s| **s != "-")
                .map(|s| s.to_string());
            let grant = rest.get(2).map(|s| s.to_string());
            cmd_tx
                .send(NetworkCommand::JoinGroup {
                    group_id: gid,
                    bootstrap_peer,
                    grant,
                })
                .map_err(|e| anyhow!("send JoinGroup: {e}"))?;
            Ok("ok join_group_grant".to_string())
        }

        "addr" => {
            let addr = await_direct_addr(endpoint, Duration::from_secs(10)).await?;
            let json = serde_json::to_string(&addr).context("serialize EndpointAddr")?;
            Ok(format!("addr {json}"))
        }

        "add_peer" => {
            // The JSON may contain spaces? No — serde_json output has none for this
            // type, but rejoin defensively in case of any whitespace.
            let json = rest.join(" ");
            let addr: EndpointAddr =
                serde_json::from_str(&json).context("deserialize EndpointAddr")?;
            static_discovery.add_endpoint_info(addr);
            Ok("ok add_peer".to_string())
        }

        "seed_empty_group" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            storage::group_insert_simple(gid, "Invited Group", "folder.fill", "#FF6B6B")
                .map_err(|e| anyhow!("group_insert_simple: {e}"))?;
            Ok("ok seed_empty_group".to_string())
        }

        "seed_fixture" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            seed_fixture(gid)?;
            Ok("ok seed_fixture".to_string())
        }

        "join_group" => {
            let gid = rest
                .first()
                .ok_or_else(|| anyhow!("group_id required"))?
                .to_string();
            let bootstrap_peer = rest.get(1).map(|s| s.to_string());
            cmd_tx
                .send(NetworkCommand::JoinGroup {
                    group_id: gid,
                    bootstrap_peer,
                    grant: None,
                })
                .map_err(|e| anyhow!("send JoinGroup: {e}"))?;
            Ok("ok join_group".to_string())
        }

        "wait_sync" => {
            let gid = rest
                .first()
                .ok_or_else(|| anyhow!("group_id required"))?
                .to_string();
            let timeout_ms: u64 = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(60_000);
            let ok = wait_sync(synced, notify, &gid, Duration::from_millis(timeout_ms)).await;
            if ok {
                Ok("ok wait_sync".to_string())
            } else {
                // NOT an `err ` response: a timeout is an expected, non-error outcome the rig
                // maps to `Ok(false)` (e.g. grant-refused-snapshot tests). An `err ` prefix
                // would be read as a hard control error instead.
                Ok("timeout wait_sync".to_string())
            }
        }

        "count" => {
            let kind = rest.first().ok_or_else(|| anyhow!("kind required"))?;
            let gid = rest.get(1).ok_or_else(|| anyhow!("group_id required"))?;
            let n = count_kind(kind, gid)?;
            Ok(format!("count {kind} {n}"))
        }

        // ── Stress / chaos fabric verbs (Round 7) ─────────────────────────────────────────
        // Post N live whiteboard-element edits to a group: insert each into THIS node's storage
        // AND broadcast it over the group gossip, exactly as the app's create-element path does.
        // Element ids are namespaced by this node's id so concurrent multi-source posting never
        // collides — convergence is then "every peer ends with the same total, no duplicates".
        // `post_edits <gid> <n> [board_id]`.
        "post_edits" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let n: usize = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("count required"))?;
            // Default to the fixture board so the elements are countable via `count elements`.
            let board = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-board"));
            let tag = &node_id[..8.min(node_id.len())];
            let now = chrono::Utc::now().timestamp();
            for i in 0..n {
                let id = format!("{gid}-e-{tag}-{i:06}");
                storage::element_insert_simple(
                    &id, &board, "rectangle",
                    (i % 1000) as f64, (i % 700) as f64, 80.0, 40.0, i as i32,
                    Some("{\"fill\":\"#00AEEF\"}"),
                    Some(&format!("{{\"text\":\"edit {i} by {tag}\"}}")),
                    now, now,
                )
                .map_err(|e| anyhow!("element_insert_simple: {e}"))?;
                let event = NetworkEvent::WhiteboardElementAdded {
                    id,
                    board_id: board.clone(),
                    element_type: "rectangle".to_string(),
                    x: (i % 1000) as f64,
                    y: (i % 700) as f64,
                    width: 80.0,
                    height: 40.0,
                    z_index: i as i32,
                    style_json: Some("{\"fill\":\"#00AEEF\"}".to_string()),
                    content_json: Some(format!("{{\"text\":\"edit {i} by {tag}\"}}")),
                    created_at: now,
                    updated_at: now,
                };
                cmd_tx
                    .send(NetworkCommand::Broadcast {
                        group_id: gid.to_string(),
                        event,
                    })
                    .map_err(|e| anyhow!("send Broadcast: {e}"))?;
            }
            Ok(format!("ok post_edits {n}"))
        }

        // Generate a deterministic blob of <size> bytes, content-address it, hold it in this node's
        // swarm store, and announce it to the group so peers can swarm-fetch. Also writes a file
        // metadata row + local_path so `count files` and direct transfer both see it.
        // `seed_blob <gid> <size_bytes> [name]` → `blob <file_id> <hash>`.
        "seed_blob" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let size: usize = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("size_bytes required"))?;
            let tag = &node_id[..8.min(node_id.len())];
            let name = rest.get(2).map(|s| s.to_string()).unwrap_or_else(|| format!("blob-{tag}.bin"));
            let file_id = format!("{gid}-blob-{tag}-{size}");

            // Deterministic, non-trivially-compressible payload keyed by file_id (so different
            // seeds differ and integrity checks are meaningful).
            let bytes = gen_blob(&file_id, size);
            let hash = blake3::hash(&bytes).to_hex().to_string();

            // Land it on disk and register it.
            let data_dir = cyan_backend::DATA_DIR
                .get()
                .cloned()
                .unwrap_or_else(|| PathBuf::from("data"));
            std::fs::create_dir_all(&data_dir).ok();
            let path = data_dir.join(&file_id);
            std::fs::write(&path, &bytes).map_err(|e| anyhow!("write blob: {e}"))?;

            let now = chrono::Utc::now().timestamp();
            storage::file_insert_simple(
                &file_id, Some(gid), None, None, &name, &hash, size as u64, Some(node_id), now,
            )
            .map_err(|e| anyhow!("file_insert_simple: {e}"))?;
            storage::file_set_local_path(&file_id, path.to_str().unwrap_or_default())
                .map_err(|e| anyhow!("file_set_local_path: {e}"))?;

            cmd_tx
                .send(NetworkCommand::SeedAndAnnounceBlob {
                    group_id: gid.to_string(),
                    hash: hash.clone(),
                    path: path.to_string_lossy().to_string(),
                })
                .map_err(|e| anyhow!("send SeedAndAnnounceBlob: {e}"))?;
            Ok(format!("blob {file_id} {hash}"))
        }

        // Fetch a blob a peer is holding. Registers the file metadata row (so the engine's
        // local_path update has a row to write), requests the download, and waits — bounded — for
        // the `FileDownloaded` event. `fetch_blob <gid> <file_id> <hash> <source_peer> <size> <timeout_ms>`
        // → `fetched <local_path>` | `timeout fetch_blob`.
        "fetch_blob" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let file_id = rest.get(1).ok_or_else(|| anyhow!("file_id required"))?.to_string();
            let hash = rest.get(2).ok_or_else(|| anyhow!("hash required"))?.to_string();
            let source = rest.get(3).ok_or_else(|| anyhow!("source_peer required"))?.to_string();
            let size: u64 = rest.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            let timeout_ms: u64 = rest.get(5).and_then(|s| s.parse().ok()).unwrap_or(60_000);

            // Insert the metadata row if absent (a real joiner would learn it via snapshot/FileAvailable).
            let now = chrono::Utc::now().timestamp();
            let _ = storage::file_insert_simple(
                &file_id, Some(gid), None, None,
                &format!("{file_id}.bin"), &hash, size, Some(&source), now,
            );

            cmd_tx
                .send(NetworkCommand::RequestFileDownload {
                    file_id: file_id.clone(),
                    hash,
                    source_peer: source,
                    resume_offset: 0,
                })
                .map_err(|e| anyhow!("send RequestFileDownload: {e}"))?;

            match wait_download(downloaded, notify, &file_id, Duration::from_millis(timeout_ms)).await {
                Some(path) => Ok(format!("fetched {path}")),
                None => Ok("timeout fetch_blob".to_string()),
            }
        }

        // Recompute the blake3 of a downloaded file and compare to the expected hash — the honest,
        // independent integrity oracle (the engine verifies too; this proves it on the receiver).
        // `verify_blob <file_id> <expected_hash>` → `verify ok` | `verify mismatch` | `verify missing`.
        "verify_blob" => {
            let file_id = rest.first().ok_or_else(|| anyhow!("file_id required"))?;
            let expected = rest.get(1).ok_or_else(|| anyhow!("expected_hash required"))?;
            match storage::file_get_local_path(file_id).filter(|p| !p.is_empty()) {
                Some(path) => match std::fs::read(&path) {
                    Ok(bytes) => {
                        let got = blake3::hash(&bytes).to_hex().to_string();
                        if &got == expected {
                            Ok("verify ok".to_string())
                        } else {
                            Ok("verify mismatch".to_string())
                        }
                    }
                    Err(_) => Ok("verify missing".to_string()),
                },
                None => Ok("verify missing".to_string()),
            }
        }

        // This node's connection tier to a peer: Direct / Relay / Mixed / None. The topology-intent
        // oracle (direct vs relay vs ws). `tier <peer_id_hex>`.
        "tier" => {
            let peer = rest.first().ok_or_else(|| anyhow!("peer_id required"))?;
            let pk: PublicKey = peer.parse().map_err(|e| anyhow!("parse peer id: {e}"))?;
            let t = match endpoint.conn_type(pk) {
                Some(mut w) => match iroh::Watcher::get(&mut w) {
                    iroh::endpoint::ConnectionType::Direct(_) => "direct",
                    iroh::endpoint::ConnectionType::Relay(_) => "relay",
                    iroh::endpoint::ConnectionType::Mixed(_, _) => "mixed",
                    iroh::endpoint::ConnectionType::None => "none",
                },
                None => "unknown",
            };
            Ok(format!("tier {t}"))
        }

        // Process-level state + metrics as compact JSON (no spaces ⇒ single token): node id, RSS,
        // and the gossip counters from the additive `metrics` module. Storage counts stay on `count`
        // (they are group-scoped). The "no message storm" + "bounded memory" + "bounded degree" oracles.
        "metrics" => {
            let rss = cyan_backend::metrics::rss_kb().unwrap_or(0);
            let json = serde_json::json!({
                "node_id": node_id,
                "rss_kb": rss,
                "gossip_recv": cyan_backend::metrics::gossip_recv(),
                "neighbor_up": cyan_backend::metrics::neighbor_up(),
                "neighbor_down": cyan_backend::metrics::neighbor_down(),
                "gossip_degree": cyan_backend::metrics::gossip_degree(),
            });
            Ok(format!("metrics {}", serde_json::to_string(&json)?))
        }

        other => Err(anyhow!("unknown verb '{other}'")),
    }
}

/// Poll the endpoint until it has at least one direct (loopback) address. Bounded.
/// Current unix time in seconds (for grant issued_at/expiry).
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn await_direct_addr(endpoint: &iroh::Endpoint, timeout: Duration) -> Result<EndpointAddr> {
    tokio::time::timeout(timeout, async {
        loop {
            let a = endpoint.addr();
            if a.ip_addrs().next().is_some() {
                return a;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("no direct address within {:?}", timeout))
}

/// Bounded wait for `SyncComplete` of `group_id`. The 50ms re-poll tick guards against
/// a lost wakeup between the set check and `notified()`; the real signal is the set,
/// and the whole wait is bounded by `timeout` — never an unbounded block.
async fn wait_sync(
    synced: &Arc<Mutex<HashSet<String>>>,
    notify: &Arc<Notify>,
    group_id: &str,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if synced
                .lock()
                .map(|s| s.contains(group_id))
                .unwrap_or(false)
            {
                return;
            }
            tokio::select! {
                _ = notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    })
    .await
    .is_ok()
}

/// Deterministic pseudo-random blob of `size` bytes keyed by `seed`. A tiny xorshift keeps the
/// payload incompressible-ish and unique per seed, so blake3 integrity assertions are meaningful
/// without pulling in an RNG dependency or `Math.random`-style nondeterminism.
fn gen_blob(seed: &str, size: usize) -> Vec<u8> {
    let mut state: u64 = 0xcbf29ce484222325;
    for b in seed.as_bytes() {
        state ^= *b as u64;
        state = state.wrapping_mul(0x100000001b3); // FNV-1a mix for the seed.
    }
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state & 0xff) as u8);
    }
    out
}

/// Bounded wait for a `FileDownloaded` of `file_id`, returning its local_path. Same lost-wakeup
/// guard as `wait_sync`: the shared map is the truth, the whole wait is bounded by `timeout`.
async fn wait_download(
    downloaded: &Arc<Mutex<HashMap<String, String>>>,
    notify: &Arc<Notify>,
    file_id: &str,
    timeout: Duration,
) -> Option<String> {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(p) = downloaded.lock().ok().and_then(|d| d.get(file_id).cloned()) {
                return p;
            }
            tokio::select! {
                _ = notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    })
    .await
    .ok()
}

/// Count rows of `kind` scoped to `group_id`, reading THIS process's storage.
fn count_kind(kind: &str, group_id: &str) -> Result<usize> {
    let ws_ids = storage::workspace_list_ids_by_group(group_id);
    let n = match kind {
        "groups" => storage::group_list()
            .map_err(|e| anyhow!("group_list: {e}"))?
            .iter()
            .filter(|g| g.id == group_id)
            .count(),
        "workspaces" => storage::workspace_list_by_group(group_id)
            .map_err(|e| anyhow!("workspace_list_by_group: {e}"))?
            .len(),
        "boards" => storage::board_list_by_workspaces(&ws_ids)
            .map_err(|e| anyhow!("board_list_by_workspaces: {e}"))?
            .len(),
        "elements" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::element_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("element_list_by_boards: {e}"))?
                .len()
        }
        "cells" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::cell_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("cell_list_by_boards: {e}"))?
                .len()
        }
        "chats" => storage::chat_list_by_workspaces(&ws_ids)
            .map_err(|e| anyhow!("chat_list_by_workspaces: {e}"))?
            .len(),
        "files" => storage::file_list_by_group(group_id)
            .map_err(|e| anyhow!("file_list_by_group: {e}"))?
            .len(),
        other => return Err(anyhow!("unknown count kind '{other}'")),
    };
    Ok(n)
}

fn board_ids(ws_ids: &[String]) -> Result<Vec<String>> {
    Ok(storage::board_list_by_workspaces(ws_ids)
        .map_err(|e| anyhow!("board_list_by_workspaces: {e}"))?
        .iter()
        .map(|b| b.id.clone())
        .collect())
}

/// Seed the full host fixture into this process's DB: a group with one workspace,
/// one board, 5 elements, 3 cells, 3 chats, and one file-meta record. Mirrors the
/// proven `network_actor_test::create_test_data` shape.
fn seed_fixture(group_id: &str) -> Result<()> {
    let ws = format!("{group_id}-ws");
    let board = format!("{group_id}-board");
    let now = chrono::Utc::now().timestamp();

    storage::group_insert_simple(group_id, "Fixture Group", "folder.fill", "#00AEEF")
        .map_err(|e| anyhow!("group_insert_simple: {e}"))?;
    storage::workspace_insert_simple(&ws, group_id, "Main Workspace")
        .map_err(|e| anyhow!("workspace_insert_simple: {e}"))?;
    storage::board_insert_simple(&board, &ws, "Test Canvas", now)
        .map_err(|e| anyhow!("board_insert_simple: {e}"))?;

    for i in 0..FIXTURE_ELEMENTS {
        storage::element_insert_simple(
            &format!("{group_id}-elem-{i:03}"),
            &board,
            "rectangle",
            (i * 100) as f64,
            (i * 50) as f64,
            200.0,
            100.0,
            i as i32,
            Some("{\"fill\":\"#00AEEF\"}"),
            Some(&format!("{{\"text\":\"Element {i}\"}}")),
            now,
            now,
        )
        .map_err(|e| anyhow!("element_insert_simple: {e}"))?;
    }

    for i in 0..FIXTURE_CELLS {
        storage::cell_insert_simple(
            &format!("{group_id}-cell-{i:03}"),
            &board,
            "code",
            i as i32,
            Some(&format!("# Cell {i}\nprint('hello')")),
            Some("hello"),
            false,
            Some(100.0),
            None,
            now,
            now,
        )
        .map_err(|e| anyhow!("cell_insert_simple: {e}"))?;
    }

    for i in 0..FIXTURE_CHATS {
        storage::chat_insert_simple(
            &format!("{group_id}-chat-{i:03}"),
            &ws,
            &format!("Test message {i}"),
            "test-author",
            None,
            now,
        )
        .map_err(|e| anyhow!("chat_insert_simple: {e}"))?;
    }

    for i in 0..FIXTURE_FILES {
        storage::file_insert_simple(
            &format!("{group_id}-file-{i:03}"),
            Some(group_id),
            Some(&ws),
            Some(&board),
            "test-document.pdf",
            "abc123def456",
            1_024_000,
            None,
            now,
        )
        .map_err(|e| anyhow!("file_insert_simple: {e}"))?;
    }

    Ok(())
}

/// Create the base tables the migrations assume exist (mirrors the in-process
/// harness's `init_base_schema` and the multi-process bins' `init_test_schema`).
fn init_base_schema(db_path: &str) -> Result<()> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, icon TEXT, color TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY, group_id TEXT NOT NULL, name TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS objects (
            id TEXT PRIMARY KEY, workspace_id TEXT, group_id TEXT, board_id TEXT,
            type TEXT NOT NULL, name TEXT NOT NULL, hash TEXT, data TEXT, size INTEGER,
            source_peer TEXT, local_path TEXT, created_at INTEGER NOT NULL,
            board_mode TEXT DEFAULT 'canvas'
        );
        CREATE TABLE IF NOT EXISTS whiteboard_elements (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, element_type TEXT NOT NULL,
            x REAL NOT NULL, y REAL NOT NULL, width REAL NOT NULL, height REAL NOT NULL,
            z_index INTEGER NOT NULL, style_json TEXT, content_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, cell_id TEXT
        );
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL, content TEXT, output TEXT,
            collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}
