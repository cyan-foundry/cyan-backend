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
//! - `bootstrap_id`                        `@@CYAN@@ bootstrap_id <hex>`  (resolved bootstrap id)
//! - `addr`                               `@@CYAN@@ addr <endpoint-addr-json>`
//! - `add_peer <endpoint-addr-json>`      `@@CYAN@@ ok add_peer`
//! - `seed_empty_group <gid>`             `@@CYAN@@ ok seed_empty_group`
//! - `seed_fixture <gid>`                 `@@CYAN@@ ok seed_fixture`
//! - `seed_postprod <gid>`                `@@CYAN@@ ok seed_postprod <board>`  (S5 named demo
//!   scenario: Post-Production group → default ws → Broadcast Delivery board → English
//!   workflow (deployed+pinned) → asset artifacts; idempotent on re-run)
//! - `join_group <gid> [bootstrap_hex]`   `@@CYAN@@ ok join_group`
//! - `wait_sync <gid> <timeout_ms>`       `@@CYAN@@ ok wait_sync` | `@@CYAN@@ timeout wait_sync`
//! - `count <kind> <gid>`                 `@@CYAN@@ count <kind> <n>`
//!   kinds: groups|workspaces|system_workspaces|boards|elements|cells|chats|notes|files
//! - `read_deltas <gid> <since_cursor>`   `@@CYAN@@ deltas <json>` (§1 incremental catch-up SERVE
//!   side: this node's events for `gid` newer than `since_cursor`, group-scoped; json =
//!   `{group_id,since,high_water,count,frames:[SnapshotFrame...]}` — what a holder serves a late
//!   joiner so the Lens replica can do incremental catch-up, not just a full snapshot)
//! - `admin_pubkey`                       `@@CYAN@@ admin_pubkey <hex>`
//! - `enforce_group <gid>`                `@@CYAN@@ ok enforce_group` (enforce + self=Owner-admin)
//! - `set_admin <gid> <pubkey> [role]`    `@@CYAN@@ ok set_admin`
//! - `issue_grant <gid> <role> [ttl]`     `@@CYAN@@ grant <nonce> <qr>` (ttl secs; negative ⇒ expired)
//! - `revoke_grant <gid> <nonce>`         `@@CYAN@@ ok revoke_grant`
//! - `join_group_grant <gid> <boot|-> <qr>`  `@@CYAN@@ ok join_group_grant`
//!
//! ## Mesh-hardening verbs (MESH_HARDENING_SPEC §2/§5/§11 — the Docker/netem e2e rig)
//! - `seed_peer <gid> <endpoint-addr-json>`  `@@CYAN@@ ok seed_peer` (§2 seed pipeline →
//!   `SeedGroupPeer`: make a peer resolvable + route it into the group topic so `NeighborUp`
//!   fires with NO relay/bootstrap — the LAN/no-infra mesh-formation path)
//! - `catch_up <gid> <source_peer_hex> [since]`  `@@CYAN@@ ok catch_up` (§5 incremental
//!   catch-up → `CatchUp`: pull ONLY the range since `since` from a holder; `since` omitted ⇒
//!   the persisted import/high-water mark)
//! - `count members <gid>`                  `@@CYAN@@ count members <n>` (§3 persisted roster)
//! - `bundle_pubkey`                         `@@CYAN@@ bundle_pubkey <x25519-hex>` (§11 the
//!   device's sealed-box recipient key an inviter exports to)
//! - `export_group <gid> <invitee_x_pub_hex>` `@@CYAN@@ bundle <bundle-json>` (§11 signed,
//!   grant-scoped, invitee-encrypted `.cyangroup` payload — single-line JSON, travels
//!   out-of-band over the harness like email/AirDrop/USB)
//! - `import_group <bundle-json>`            `@@CYAN@@ ok import_group <gid>` (§11 air-gapped
//!   import: verify + scope + decrypt + seed + stamp watermark; touches NO network)
//!
//! ## Stress / chaos fabric verbs (Round 7)
//! - `post_edits <gid> <n> [board]`       `@@CYAN@@ ok post_edits <n>` (local insert + gossip broadcast)
//! - `post_chat <gid> <n> [ws]`           `@@CYAN@@ ok post_chat <n>` (local insert + ChatSent broadcast)
//! - `post_workflow <gid> [steps] [ws]`   `@@CYAN@@ ok post_workflow <board> <steps>` (board+cells+pin broadcast)
//!
//! ## Distributed workflow RUN verbs (Round 10 — run-execution + run-state propagation)
//! - `wf_author <gid> <shape>`            `@@CYAN@@ ok wf_author <board>` (author a RUNNABLE workflow:
//!   board + step cells carrying pipeline configs, broadcast Added+Updated so peers get the configs;
//!   shape ∈ linear|diamond|gated)
//! - `wf_run <board> [wave|seq]`          `@@CYAN@@ ok wf_run <json>` (RUN the pipeline — wave-concurrent
//!   over a level-set plan, or sequential fallback; <json> summarizes the exec events that fired:
//!   started/finished/finished_state/stats/progress + running/done/failed/awaiting/pending step lists
//!   + mode/peak/run_id. Run-state rides the SAME cell-update gossip path to every peer.)
//! - `wf_state <board> <step_id>`         `@@CYAN@@ state <step_id> <status>` (THIS peer's run-state for
//!   the step, read from its OWN notebook_cells metadata — the convergence oracle; `absent` if missing)
//! - `wf_approve <board> <step_id>`       `@@CYAN@@ ok wf_approve` (approve a human gate on THIS peer;
//!   broadcasts the approval so the run unblocks for every peer)
//! - `post_notes <gid> <n> [board]`       `@@CYAN@@ ok post_notes <n>` (local note insert, NO broadcast; digest-converge)
//! - `set_pin <gid> <0|1> [board]`        `@@CYAN@@ ok set_pin <pinned>` (local pin, NO broadcast; digest-converge)
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
use cyan_backend::models::commands::{CommandMsg, NetworkCommand};
use cyan_backend::models::dto::NotebookCellDTO;
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

    // §5 discoverable bootstrap config. An explicit `BOOTSTRAP_NODE_ID` env pins the global FIRST
    // (`OnceCell::set` is first-wins) so the discovery topic AND every group topic agree on ONE
    // live bootstrap — not the bundled hardcode (the bug that made cross-net meshes race-dependent).
    if let Ok(id) = std::env::var("BOOTSTRAP_NODE_ID")
        && !id.is_empty()
    {
        let _ = cyan_backend::BOOTSTRAP_NODE_ID.set(id);
    }
    // When `CYAN_RENDEZVOUS_URL` is set, fetch the org-signed (or self-signed bootstrap) rendezvous
    // config, verify it against the pinned `CYAN_ORG_PUBKEY`, and adopt the LIVE bootstrap id /
    // discovery_key / relay it carries — filling only what the env above didn't pin. This is the
    // real discover→verify→pin path the iOS app uses. Untouched (no network) when no URL is set, so
    // the offline / LAN / explicit-id paths behave exactly as before §5.
    let rdv_source = if std::env::var("CYAN_RENDEZVOUS_URL").is_ok_and(|s| !s.is_empty()) {
        Some(cyan_backend::rendezvous::fetch_and_apply_if_configured())
    } else {
        None
    };
    eprintln!(
        "🧭 [cyan_node] rendezvous source={:?} resolved bootstrap={}",
        rdv_source,
        cyan_backend::bootstrap_node_id()
    );

    // discovery_key: explicit env wins; else a config-resolved value; else the test default.
    let discovery_key = std::env::var("DISCOVERY_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| cyan_backend::DISCOVERY_KEY.get().cloned())
        .unwrap_or_else(|| "cyan-test".to_string());
    // relay: explicit RELAY env wins; else a config-resolved RELAY_URL; else disabled.
    let relay = match std::env::var("RELAY").as_deref() {
        Ok("disabled") => RelayPolicy::Disabled,
        Ok(url) => RelayPolicy::Url(url.to_string()),
        Err(_) => match cyan_backend::RELAY_URL.get() {
            Some(u) if !u.is_empty() => RelayPolicy::Url(u.clone()),
            _ => RelayPolicy::Disabled,
        },
    };
    // The resolved bootstrap id now drives discovery (whether it came from the env or the verified
    // config); absent ⇒ MdnsOnly. `bootstrap_node_id()` returns the bundled fallback when nothing
    // resolved, but we only switch to Bootstrap policy when a value was actually pinned/verified.
    let discovery = match cyan_backend::BOOTSTRAP_NODE_ID.get() {
        Some(id) if !id.is_empty() => DiscoveryPolicy::Bootstrap(id.clone()),
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

    // ROUND8 §W3: optionally provision a group the way the create path does — a group
    // record plus its two auto-seeded workspaces (default + system "Plugins") — BEFORE
    // the actor starts, so the engine auto-hosts the group topic (the host role).
    if let Ok(gid) = std::env::var("PROVISION_GROUP")
        && !gid.is_empty()
    {
        provision_group(&gid)?;
    }

    // Downloads land under DATA_DIR (process-global OnceCell in the engine).
    let data_dir = std::env::var("DATA_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        PathBuf::from(&db_path)
            .parent()
            .map(|p| p.join("data"))
            .unwrap_or_else(|| PathBuf::from("data"))
    });
    std::fs::create_dir_all(&data_dir).ok();
    let _ = cyan_backend::DATA_DIR.set(data_dir.clone());

    // Point the on-device MCP plugin root at an EMPTY dir (unless the caller set one),
    // so a workflow's `local` mcp_tool steps resolve "not installed" and fail FAST +
    // deterministically offline — the test-only local step the run-execution harness
    // drives (real local/MCP execution is out of substrate scope; see CLAUDE.md). No
    // network, no spawn, no backoff: `resolve_installed_tool` returns None → a surfaced
    // error, which is enough to drive pending→running→terminal and the exec events.
    if std::env::var("CYAN_PLUGINS_ROOT").is_err() {
        let proot = data_dir.join("plugins-empty");
        std::fs::create_dir_all(&proot).ok();
        // SAFETY: set once at boot, before any actor/run task reads it.
        unsafe {
            std::env::set_var("CYAN_PLUGINS_ROOT", &proot);
        }
    }

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

    // ── Workflow-run bridge (Round 10) ────────────────────────────────────────────
    // `pipeline::run_pipeline_*` drives step-state changes through a `CommandMsg`
    // channel (the app's CommandActor seam). cyan_node has no CommandActor, so this
    // task IS the seam for the one command a run emits — `UpdateNotebookCell`: apply
    // it to THIS node's storage AND broadcast `NotebookCellUpdated` over the group, so
    // the run-state (each step's pipeline `state.status`) rides the SAME gossip path
    // that carries chat/boards and converges on every peer. Mirrors the engine's
    // CommandActor arm in `lib.rs`. Other CommandMsg variants are local-only here.
    let (cmd_msg_tx, mut cmd_msg_rx) = mpsc::unbounded_channel::<CommandMsg>();
    {
        let net_tx = cmd_tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = cmd_msg_rx.recv().await {
                if let CommandMsg::UpdateNotebookCell {
                    id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json,
                } = msg
                {
                    let dto = NotebookCellDTO {
                        id: id.clone(),
                        board_id: board_id.clone(),
                        cell_type: cell_type.clone(),
                        cell_order,
                        content: content.clone(),
                        output: output.clone(),
                        collapsed,
                        height,
                        metadata_json: metadata_json.clone(),
                        created_at: 0,
                        updated_at: 0,
                    };
                    let _ = storage::cell_update(&dto);
                    if let Some(gid) = storage::board_get_group_id(&board_id) {
                        let _ = net_tx.send(NetworkCommand::Broadcast {
                            group_id: gid,
                            event: NetworkEvent::NotebookCellUpdated {
                                id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json,
                            },
                        });
                    }
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
            &cmd_msg_tx,
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
    cmd_msg_tx: &UnboundedSender<CommandMsg>,
    synced: &Arc<Mutex<HashSet<String>>>,
    downloaded: &Arc<Mutex<HashMap<String, String>>>,
    notify: &Arc<Notify>,
    authorizer: &Arc<std::sync::Mutex<MeshAuthorizer>>,
    grant_secret: &[u8; 32],
    admin_pubkey: &str,
) -> Result<String> {
    match verb {
        "node_id" => Ok(format!("node_id {node_id}")),

        // The bootstrap id this node RESOLVED at startup (from `BOOTSTRAP_NODE_ID`, a verified
        // rendezvous config, or the bundled fallback). Lets a test assert positively that a peer
        // adopted the LIVE published id — or fell back to bundled when the config was tampered.
        "bootstrap_id" => Ok(format!("bootstrap_id {}", cyan_backend::bootstrap_node_id())),

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

        // S5 demo seed: a NAMED, broadcast-friendly "Post-Production" group → default
        // workspace → "Broadcast Delivery" board → a sample English workflow (deployed +
        // pinned, so the board's Dashboard FACE has real content) → a few asset artifacts.
        // Idempotent (no-op once the group already has a board), so every bring-up converges.
        // `seed_postprod <gid>` → `ok seed_postprod <board>`.
        "seed_postprod" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let board = seed_postprod(gid)?;
            Ok(format!("ok seed_postprod {board}"))
        }

        // Coherent, IDEMPOTENT demo scale-seed: 3 distinctly-NAMED groups, 10 boards,
        // each board bound to ONE real staged clip (clip + thumbnail both on the lens
        // box, verified 200 on /api/v1/media/thumbnail). Every step in a board names
        // THAT board's own clip → the per-step asset frame is coherent (item #1) and no
        // two groups/boards share a name (item #27 / STEP2). Idempotent by truncate-then-
        // seed of the managed group ids: re-running yields EXACTLY this set, zero dups.
        "seed_demo" => {
            let summary = seed_demo()?;
            Ok(format!("ok seed_demo {summary}"))
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

        // ── §1 read_deltas — incremental catch-up SERVE side (SUPER_PEER_COMPLETION §1) ──────
        // Read THIS node's storage for the events of ONE group whose version is strictly newer
        // than `since_cursor`, and return them — so a holder (e.g. the Lens EmbeddedReplica) can
        // SERVE incremental catch-up to a late/returning peer instead of a full re-snapshot. It is
        // the read-only HOLDER counterpart to the `catch_up` REQUESTER verb. Reuses the engine's
        // `snapshot::build_snapshot_frames(group, Some(since))` (the same since-bounded path the live
        // `CatchUp` command uses) so a delta served here and a delta pulled there are identical.
        //
        // STRICTLY group-scoped: the frames are built solely from `group_id`'s own rows, so another
        // group's events can never appear. The response is single-line JSON (no whitespace) carrying:
        //   {group_id, since, high_water, count, frames:[SnapshotFrame...]}
        // `count` is the number of DATA rows newer than the cursor (EXCLUDING the always-present
        // group row in the Structure frame — so a caught-up reader sees count=0). `high_water` is
        // this holder's current max version (the cursor a caller sends next to stay current).
        // `read_deltas <group_id> <since_cursor>` → `deltas <json>`.
        "read_deltas" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let since: i64 = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("since_cursor required (i64)"))?;
            let frames = cyan_backend::snapshot::build_snapshot_frames(gid, Some(since))
                .map_err(|e| anyhow!("build_snapshot_frames: {e}"))?;
            let count = cyan_backend::snapshot::frames_row_count(&frames);
            let high_water = cyan_backend::snapshot::group_high_water(gid);
            let json = serde_json::to_string(&serde_json::json!({
                "group_id": gid,
                "since": since,
                "high_water": high_water,
                "count": count,
                "frames": frames,
            }))?;
            Ok(format!("deltas {json}"))
        }

        // ── Mesh-hardening verbs (MESH_HARDENING §2/§5/§11) ──────────────────────────────
        // §2 seed pipeline: turn a resolvable EndpointAddr into a present peer in ONE group's
        // gossip topic. This is the no-relay/no-bootstrap formation path — the engine makes the
        // addr resolvable (`add_endpoint_info`), persists it for rejoin, and routes it into the
        // topic so `NeighborUp` fires. In the rig this stands in for an mDNS-discovered peer
        // (Docker bridges don't carry multicast reliably; the seed pipeline it feeds is the same).
        // `seed_peer <gid> <endpoint-addr-json>`.
        "seed_peer" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?.to_string();
            // The addr JSON has no whitespace from serde, but rejoin defensively.
            let addr_json = rest[1..].join(" ");
            if addr_json.is_empty() {
                return Err(anyhow!("addr_json required"));
            }
            cmd_tx
                .send(NetworkCommand::SeedGroupPeer { group_id: gid, addr_json })
                .map_err(|e| anyhow!("send SeedGroupPeer: {e}"))?;
            Ok("ok seed_peer".to_string())
        }

        // §5 incremental catch-up: pull ONLY the missing range for a group from a holder, since
        // the requester's high-water mark. `since` omitted ⇒ the engine uses the persisted
        // import/high-water mark. Used by a peer returning after a partition/offline window to
        // reconcile WITHOUT a full re-snapshot. `catch_up <gid> <source_peer_hex> [since]`.
        "catch_up" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?.to_string();
            let source = rest.get(1).ok_or_else(|| anyhow!("source_peer required"))?.to_string();
            let since: Option<i64> = rest.get(2).and_then(|s| s.parse().ok());
            cmd_tx
                .send(NetworkCommand::CatchUp { group_id: gid, source_peer: source, since })
                .map_err(|e| anyhow!("send CatchUp: {e}"))?;
            Ok("ok catch_up".to_string())
        }

        // §11 this device's X25519 sealed-box recipient key — what an inviter exports a bundle
        // TO so only this device can open it. Derived from the node's Ed25519 identity.
        "bundle_pubkey" => {
            let hex = cyan_backend::group_bundle::invitee_pubkey_hex(grant_secret);
            Ok(format!("bundle_pubkey {hex}"))
        }

        // §11 export a signed, grant-scoped, invitee-encrypted `.cyangroup` bundle of THIS node's
        // current group state. This node issues a Member grant for the group (signed by its own
        // admin key — it is the producer), then seals the snapshot to the invitee's X25519 key.
        // The bundle JSON is returned on the wire (single line, no whitespace) so it can be
        // handed out-of-band to an air-gapped importer — exactly the email/AirDrop/USB delivery
        // §11 describes. `export_group <gid> <invitee_x_pub_hex>` → `bundle <json>`.
        "export_group" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let invitee_x_pub = rest.get(1).ok_or_else(|| anyhow!("invitee_x_pub_hex required"))?;
            let now = unix_now();
            let expiry = now + 3600;
            let nonce = format!("{}-{}-export", &admin_pubkey[..8], gid);
            let grant = Grant::issue_unchecked(gid, Role::Member, grant_secret, now, expiry, &nonce);
            let bundle =
                cyan_backend::group_bundle::export_group(gid, &grant, invitee_x_pub, grant_secret, now as i64)
                    .map_err(|e| anyhow!("export_group: {e}"))?;
            Ok(format!("bundle {}", bundle.to_json()))
        }

        // §11 air-gapped import: verify (outer sig · grant sig · scope) + decrypt + seed storage +
        // stamp the "synced as of T" watermark §5 catch-up reconciles from. Touches NO network —
        // the honest cold-start path. `import_group <bundle-json>` → `ok import_group <gid>`.
        "import_group" => {
            let json = rest.join(" ");
            if json.is_empty() {
                return Err(anyhow!("bundle json required"));
            }
            let bundle = cyan_backend::group_bundle::GroupBundle::from_json(&json)
                .map_err(|e| anyhow!("parse bundle: {e}"))?;
            let gid = cyan_backend::group_bundle::import_group(&bundle, grant_secret)
                .map_err(|e| anyhow!("import_group: {e}"))?;
            Ok(format!("ok import_group {gid}"))
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

        // Insert `n` live edits into THIS node's storage but DO NOT broadcast them — a deterministic
        // stand-in for live deltas whose gossip was dropped (`Lagged`) so no other peer ever saw
        // them. Without anti-entropy these never reach the rest of the mesh; with the sweep they are
        // detected (digest mismatch) and pulled. `post_local <gid> <n> [board]` → `ok post_local <n>`.
        "post_local" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let n: usize = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("count required"))?;
            let board = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-board"));
            let tag = &node_id[..8.min(node_id.len())];
            let now = chrono::Utc::now().timestamp();
            for i in 0..n {
                // Distinct id namespace ("le" = local edit) so these never collide with `post_edits`.
                let id = format!("{gid}-le-{tag}-{i:06}");
                storage::element_insert_simple(
                    &id, &board, "rectangle",
                    (i % 1000) as f64, (i % 700) as f64, 80.0, 40.0, i as i32,
                    Some("{\"fill\":\"#FF8800\"}"),
                    Some(&format!("{{\"text\":\"local edit {i} by {tag}\"}}")),
                    now, now,
                )
                .map_err(|e| anyhow!("element_insert_simple: {e}"))?;
            }
            Ok(format!("ok post_local {n}"))
        }

        // Author `n` notes (ROUND8 §W2) into THIS node's storage WITHOUT broadcasting —
        // the deterministic stand-in for "the live NoteAdded never reached the peer".
        // Note ids are namespaced by this node's id so concurrent multi-source posting
        // never collides; with notes in the digest + snapshot, ONLY the anti-entropy
        // sweep can reconcile them, so "every peer ends with the exact union" is the
        // digest-convergence proof. `post_notes <gid> <n> [board]` → `ok post_notes <n>`.
        "post_notes" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let n: usize = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("count required"))?;
            let board = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-board"));
            let tag = &node_id[..8.min(node_id.len())];
            let now = chrono::Utc::now().timestamp();
            for i in 0..n {
                let note = cyan_backend::models::dto::NoteDTO {
                    id: format!("{gid}-n-{tag}-{i:06}"),
                    board_id: board.clone(),
                    tenant_id: gid.to_string(),
                    author_id: node_id.to_string(),
                    author_name: format!("peer-{tag}"),
                    text: format!("note {i} by {tag}"),
                    created_at: now,
                    updated_at: now,
                };
                storage::note_upsert(&note).map_err(|e| anyhow!("note_upsert: {e}"))?;
            }
            Ok(format!("ok post_notes {n}"))
        }

        // Set the fixture board's pinned-workflow state (ROUND8 §W4) into THIS node's
        // storage WITHOUT broadcasting — the deterministic stand-in for "the live
        // PinSet never reached the peer". With pins in the digest + snapshot, ONLY the
        // anti-entropy sweep can reconcile it, so "the joiner ends pinned too" is the
        // convergence proof. `set_pin <gid> <0|1> [board]` → `ok set_pin <pinned>`.
        "set_pin" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let pinned = rest
                .get(1)
                .map(|s| *s == "1" || s.eq_ignore_ascii_case("true"))
                .ok_or_else(|| anyhow!("pinned flag required"))?;
            let board = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-board"));
            let now = chrono::Utc::now().timestamp();
            let pin = cyan_backend::models::dto::PinDTO {
                board_id: board,
                tenant_id: gid.to_string(),
                pinned,
                updated_at: now,
            };
            storage::pin_upsert(&pin).map_err(|e| anyhow!("pin_upsert: {e}"))?;
            Ok(format!("ok set_pin {}", pinned as i32))
        }

        // R12 C3: set the BOARD-PIN lane (`board_metadata.is_pinned` + `pin_updated_at`, the C1/C2
        // convergent delta — distinct from the ROUND8 workflow-pin `set_pin` above) into THIS node's
        // storage WITHOUT broadcasting — the deterministic stand-in for a dropped `BoardPinned`. With
        // the board-pin lane now in the digest + snapshot, ONLY anti-entropy can reconcile it. The
        // optional explicit `clock` lets a test assert LWW (a stale clock must not clobber a newer pin).
        // `set_board_pin <gid> <0|1> [board] [clock]` → `ok set_board_pin <is_pinned> <clock>`.
        "set_board_pin" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let pinned = rest
                .get(1)
                .map(|s| *s == "1" || s.eq_ignore_ascii_case("true"))
                .ok_or_else(|| anyhow!("pinned flag required"))?;
            let board = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-board"));
            let clock = rest
                .get(3)
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            storage::board_meta_set_pinned(&board, pinned, clock)
                .map_err(|e| anyhow!("board_meta_set_pinned: {e}"))?;
            Ok(format!("ok set_board_pin {} {clock}", pinned as i32))
        }

        // R12 C3 (D2/E1 lane): deploy a workflow (`board_workflow_state` deployed/dashboard/locked)
        // into THIS node's storage WITHOUT broadcasting — the stand-in for a deploy a peer missed.
        // With workflow-state now in the digest + snapshot, anti-entropy reconciles it on the next
        // sweep / cold-join. `deploy_local <gid> <0|1 dashboard> [board] [clock]` → `ok deploy_local <clock>`.
        "deploy_local" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let dashboard = rest
                .get(1)
                .map(|s| *s == "1" || s.eq_ignore_ascii_case("true"))
                .ok_or_else(|| anyhow!("dashboard flag required"))?;
            let board = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-board"));
            let clock = rest
                .get(3)
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            storage::workflow_state_set_deployed(&board, dashboard, clock)
                .map_err(|e| anyhow!("workflow_state_set_deployed: {e}"))?;
            Ok(format!("ok deploy_local {clock}"))
        }

        // ── Live-harness behaviors (ROUND8 harness) ───────────────────────────────────────
        // Post `n` live board-chat messages to a group: insert each into THIS node's storage AND
        // broadcast a `ChatSent` over the group gossip, exactly as the app's send-chat path does
        // (the receiver persists it via the same `ChatSent` apply arm). Chat ids are namespaced by
        // this node's id, so concurrent multi-source posting never collides — convergence is then
        // "every peer ends with the same chat total, no dupes, no loss".
        // `post_chat <gid> <n> [workspace_id]` → `ok post_chat <n>`.
        "post_chat" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let n: usize = rest
                .get(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("count required"))?;
            // Default to the fixture workspace so the chats are countable via `count chats`.
            let ws = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-ws"));
            let tag = &node_id[..8.min(node_id.len())];
            let now = chrono::Utc::now().timestamp();
            for i in 0..n {
                let id = format!("{gid}-msg-{tag}-{i:06}");
                let message = format!("msg {i} from {tag}");
                // R11 §1: chat is board-scoped. This dev bin keys its load-gen chats to a
                // deterministic board id derived from the workspace (the workspace's default
                // board), kept consistent between sender and receiver assertions.
                let board = format!("{ws}-board");
                storage::chat_insert(&id, &board, &ws, &message, node_id, None, now)
                    .map_err(|e| anyhow!("chat_insert: {e}"))?;
                let event = NetworkEvent::ChatSent {
                    id,
                    board_id: board,
                    workspace_id: ws.clone(),
                    message,
                    author: node_id.to_string(),
                    parent_id: None,
                    timestamp: now,
                };
                cmd_tx
                    .send(NetworkCommand::Broadcast {
                        group_id: gid.to_string(),
                        event,
                    })
                    .map_err(|e| anyhow!("send Broadcast: {e}"))?;
            }
            Ok(format!("ok post_chat {n}"))
        }

        // Author a local-placement workflow on THIS node and REPLICATE its authoring over the mesh:
        // create a workflow board, add `steps` notebook cells (the steps), and PIN it (the gate),
        // broadcasting `BoardCreated` + `NotebookCellAdded` + `PinSet` so every peer persists them
        // via the same apply arms. Execution / wave-placement is LOCAL/MCP and out of substrate
        // scope (CLAUDE.md) — what the MESH carries, and what peers must converge on, is the
        // authored board + steps + pinned-gate. `post_workflow <gid> [steps] [ws]`
        // → `ok post_workflow <board_id> <steps>`.
        "post_workflow" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let steps: usize = rest.get(1).and_then(|s| s.parse().ok()).unwrap_or(3);
            let ws = rest
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{gid}-ws"));
            let tag = &node_id[..8.min(node_id.len())];
            let board = format!("{gid}-wf-{tag}");
            let now = chrono::Utc::now().timestamp();

            // 1) the workflow board itself.
            storage::board_insert(&board, &ws, "Workflow", now)
                .map_err(|e| anyhow!("board_insert: {e}"))?;
            cmd_tx
                .send(NetworkCommand::Broadcast {
                    group_id: gid.to_string(),
                    event: NetworkEvent::BoardCreated {
                        id: board.clone(),
                        workspace_id: ws.clone(),
                        name: "Workflow".to_string(),
                        created_at: now,
                    },
                })
                .map_err(|e| anyhow!("send Broadcast BoardCreated: {e}"))?;

            // 2) the steps, as ordered notebook cells.
            for i in 0..steps {
                let id = format!("{board}-step-{i:03}");
                let content = format!("step {i}: local-placement task");
                storage::cell_insert(&id, &board, "code", i as i32, Some(&content))
                    .map_err(|e| anyhow!("cell_insert: {e}"))?;
                cmd_tx
                    .send(NetworkCommand::Broadcast {
                        group_id: gid.to_string(),
                        event: NetworkEvent::NotebookCellAdded {
                            id,
                            board_id: board.clone(),
                            cell_type: "code".to_string(),
                            cell_order: i as i32,
                            content: Some(content),
                        },
                    })
                    .map_err(|e| anyhow!("send Broadcast NotebookCellAdded: {e}"))?;
            }

            // 3) pin it — the workflow's activation gate.
            let pin = cyan_backend::models::dto::PinDTO {
                board_id: board.clone(),
                tenant_id: gid.to_string(),
                pinned: true,
                updated_at: now,
            };
            storage::pin_upsert(&pin).map_err(|e| anyhow!("pin_upsert: {e}"))?;
            cmd_tx
                .send(NetworkCommand::Broadcast {
                    group_id: gid.to_string(),
                    event: NetworkEvent::PinSet {
                        board_id: board.clone(),
                        tenant_id: gid.to_string(),
                        pinned: true,
                        updated_at: now,
                    },
                })
                .map_err(|e| anyhow!("send Broadcast PinSet: {e}"))?;

            Ok(format!("ok post_workflow {board} {steps}"))
        }

        // ── Distributed workflow RUN verbs (Round 10) ─────────────────────────────────────
        // Author a RUNNABLE workflow: a board + step cells whose metadata carries the pipeline
        // configs (DAG shape), broadcast (Added then Updated-with-metadata) so every peer gets
        // the configs and can read/approve. `wf_author <gid> <shape>` (shape ∈ linear|diamond|gated).
        "wf_author" => {
            let gid = rest.first().ok_or_else(|| anyhow!("group_id required"))?;
            let shape = rest.get(1).copied().unwrap_or("linear");
            let board = author_workflow(gid, shape, node_id, cmd_tx)?;
            Ok(format!("ok wf_author {board}"))
        }

        // RUN the authored workflow through the real wave executor (`run_pipeline_with_plan`):
        // `wave` (default) runs a level-set physical plan WAVE-CONCURRENTLY; `seq` runs the
        // sequential toposort fallback. Returns a JSON summary of the exec events that fired
        // (DASHBOARD_CONTRACT §A) — the run-execution oracle. Step run-state rides the cell-update
        // gossip path to every peer (assert with `wf_state`). `wf_run <board> [wave|seq]`.
        "wf_run" => {
            let board = rest.first().ok_or_else(|| anyhow!("board_id required"))?;
            let mode = rest.get(1).copied().unwrap_or("wave");
            let json = run_workflow(board, mode, cmd_msg_tx).await?;
            Ok(format!("ok wf_run {json}"))
        }

        // THIS peer's run-state for a step, read from its OWN notebook_cells metadata — the
        // convergence oracle (never a log line). `wf_state <board> <step_id>` → `state <id> <status>`.
        "wf_state" => {
            let board = rest.first().ok_or_else(|| anyhow!("board_id required"))?;
            let step = rest.get(1).ok_or_else(|| anyhow!("step_id required"))?;
            let status = workflow_state(board, step)?;
            Ok(format!("state {step} {status}"))
        }

        // Approve a human gate on THIS peer and broadcast the approval over the mesh, so the run
        // unblocks for EVERY peer (branch-barrier release). `wf_approve <board> <step_id>`.
        "wf_approve" => {
            let board = rest.first().ok_or_else(|| anyhow!("board_id required"))?;
            let step = rest.get(1).ok_or_else(|| anyhow!("step_id required"))?;
            cyan_backend::pipeline::approve_step(board, step, Some("harness"), cmd_msg_tx, None)
                .map_err(|e| anyhow!("approve_step: {e}"))?;
            Ok("ok wf_approve".to_string())
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
                "ae_digest_sent": cyan_backend::metrics::ae_digest_sent(),
                "ae_repair": cyan_backend::metrics::ae_repair(),
                "snapshot_served": cyan_backend::metrics::snapshot_served(),
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

// ── Distributed workflow RUN helpers (Round 10) ────────────────────────────────────────────

/// The DAG shape of a runnable workflow: `(step_id, depends_on, executor)`. `local` steps run
/// (and, with the empty plugin root, deterministically fail "not installed" — the test-only local
/// step); `manual` steps are human-approval gates. Three shapes exercise the executor:
///   linear  — s0→s1→s2 (a chain; every step its own wave)
///   diamond — a→{b,c}→d (b,c independent ⇒ one wave ⇒ run CONCURRENTLY)
///   gated   — g(gate) ; b depends on g (gated branch) ; x independent (branch barrier)
fn workflow_shape(shape: &str) -> Result<Vec<(&'static str, Vec<&'static str>, &'static str)>> {
    let specs = match shape {
        "linear" => vec![
            ("s0", vec![], "local"),
            ("s1", vec!["s0"], "local"),
            ("s2", vec!["s1"], "local"),
        ],
        "diamond" => vec![
            ("a", vec![], "local"),
            ("b", vec!["a"], "local"),
            ("c", vec!["a"], "local"),
            ("d", vec!["b", "c"], "local"),
        ],
        "gated" => vec![
            ("g", vec![], "manual"),
            ("b", vec!["g"], "local"),
            ("x", vec![], "local"),
        ],
        other => return Err(anyhow!("unknown workflow shape '{other}' (linear|diamond|gated)")),
    };
    Ok(specs)
}

/// Build a `PipelineStepConfig` for one authored step (everything but id/deps/executor default).
fn make_step_config(
    step_id: &str,
    depends_on: &[&str],
    executor: &str,
) -> cyan_backend::pipeline::PipelineStepConfig {
    cyan_backend::pipeline::PipelineStepConfig {
        step_id: step_id.to_string(),
        depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
        stage: Some(step_id.to_string()),
        executor: executor.to_string(),
        model: None,
        model_config: None,
        tools: vec![],
        output_format: "markdown".to_string(),
        command: None,
        timeout_seconds: Some(5),
        retry_count: Some(0),
        auto_advance: false,
        notifications: vec![],
        state: cyan_backend::pipeline::PipelineStepState::default(),
    }
}

/// A step cell's metadata JSON: the pipeline config plus, for executable (non-gate) steps, an
/// `mcp_tool` spec pointing at a NON-installed plugin so the run fails fast + offline (the
/// deterministic test-only local step). Single line, no whitespace — travels as one gossip field.
fn step_metadata(config: &cyan_backend::pipeline::PipelineStepConfig) -> Result<String> {
    let mut meta = serde_json::Map::new();
    meta.insert("pipeline".to_string(), serde_json::to_value(config)?);
    if config.executor != "manual" {
        meta.insert(
            "mcp_tool".to_string(),
            serde_json::json!({ "plugin_id": "nope", "tool": "nope", "args": {} }),
        );
    }
    Ok(serde_json::Value::Object(meta).to_string())
}

/// Author a runnable workflow on THIS node and replicate it: a board + one step cell per shape
/// step, each carrying its pipeline config. Broadcast `BoardCreated`, then per cell
/// `NotebookCellAdded` (creates the row on peers) + `NotebookCellUpdated` (writes the metadata on
/// peers) — so every peer holds the configs and can read run-state / approve gates. Returns the
/// board id.
fn author_workflow(
    gid: &str,
    shape: &str,
    node_id: &str,
    cmd_tx: &UnboundedSender<NetworkCommand>,
) -> Result<String> {
    let specs = workflow_shape(shape)?;
    let tag = &node_id[..8.min(node_id.len())];
    let ws = format!("{gid}-ws");
    let board = format!("{gid}-wfr-{shape}-{tag}");
    let now = chrono::Utc::now().timestamp();

    storage::board_insert(&board, &ws, "Workflow Run", now)
        .map_err(|e| anyhow!("board_insert: {e}"))?;
    cmd_tx
        .send(NetworkCommand::Broadcast {
            group_id: gid.to_string(),
            event: NetworkEvent::BoardCreated {
                id: board.clone(),
                workspace_id: ws.clone(),
                name: "Workflow Run".to_string(),
                created_at: now,
            },
        })
        .map_err(|e| anyhow!("send BoardCreated: {e}"))?;

    for (i, (sid, deps, exec)) in specs.iter().enumerate() {
        let config = make_step_config(sid, deps, exec);
        let meta = step_metadata(&config)?;
        let cell_id = format!("{board}-{sid}");
        let content = format!("step {sid}");
        let order = i as i32;

        storage::cell_insert(&cell_id, &board, "code", order, Some(&content))
            .map_err(|e| anyhow!("cell_insert: {e}"))?;
        let dto = NotebookCellDTO {
            id: cell_id.clone(),
            board_id: board.clone(),
            cell_type: "code".to_string(),
            cell_order: order,
            content: Some(content.clone()),
            output: None,
            collapsed: false,
            height: None,
            metadata_json: Some(meta.clone()),
            created_at: now,
            updated_at: now,
        };
        storage::cell_update(&dto).map_err(|e| anyhow!("cell_update: {e}"))?;

        cmd_tx
            .send(NetworkCommand::Broadcast {
                group_id: gid.to_string(),
                event: NetworkEvent::NotebookCellAdded {
                    id: cell_id.clone(),
                    board_id: board.clone(),
                    cell_type: "code".to_string(),
                    cell_order: order,
                    content: Some(content.clone()),
                },
            })
            .map_err(|e| anyhow!("send NotebookCellAdded: {e}"))?;
        cmd_tx
            .send(NetworkCommand::Broadcast {
                group_id: gid.to_string(),
                event: NetworkEvent::NotebookCellUpdated {
                    id: cell_id,
                    board_id: board.clone(),
                    cell_type: "code".to_string(),
                    cell_order: order,
                    content: Some(content),
                    output: None,
                    collapsed: false,
                    height: None,
                    metadata_json: Some(meta),
                },
            })
            .map_err(|e| anyhow!("send NotebookCellUpdated: {e}"))?;
    }

    Ok(board)
}

/// Reload the board's step configs from THIS node's storage (parsed from each cell's
/// `metadata_json.pipeline`), ordered by `cell_order`.
fn load_step_configs(board_id: &str) -> Result<Vec<cyan_backend::pipeline::PipelineStepConfig>> {
    let conn = storage::db().lock().map_err(|e| anyhow!("db lock: {e}"))?;
    let mut stmt = conn.prepare(
        "SELECT metadata_json FROM notebook_cells WHERE board_id = ?1 ORDER BY cell_order",
    )?;
    let rows = stmt.query_map(rusqlite::params![board_id], |r| r.get::<_, Option<String>>(0))?;
    let mut out = Vec::new();
    for row in rows {
        let Some(mj) = row? else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&mj) else { continue };
        let Some(p) = v.get("pipeline") else { continue };
        if let Ok(cfg) =
            serde_json::from_value::<cyan_backend::pipeline::PipelineStepConfig>(p.clone())
        {
            out.push(cfg);
        }
    }
    Ok(out)
}

/// Compute the level-set physical plan from the authored configs — the minimal materializer
/// (WORKFLOW_MATERIALIZATION §1): wave index = longest-path depth, each wave is one concurrent
/// batch of its independent steps; `manual` steps are gates, and a step depending on a gate carries
/// it as its `gate_barrier`. This is what Lens would emit; here the harness emits it so the run is
/// wave-concurrent. tenant = the board's group (matches `tenant = group_id`).
fn build_level_set_plan(
    board_id: &str,
    configs: &[cyan_backend::pipeline::PipelineStepConfig],
) -> cyan_backend::exec_plan::PhysicalPlan {
    use cyan_backend::exec_plan::{PhysicalPlan, PlannedStep, Wave};
    use std::collections::{BTreeMap, HashSet};

    let tenant = storage::board_get_group_id(board_id).unwrap_or_else(|| "device".to_string());
    let ids: HashSet<&str> = configs.iter().map(|c| c.step_id.as_str()).collect();
    let gate_ids: HashSet<&str> = configs
        .iter()
        .filter(|c| c.executor == "manual")
        .map(|c| c.step_id.as_str())
        .collect();

    // Fixpoint over a (small, acyclic) DAG: level = 1 + max(dep levels).
    let mut level: BTreeMap<String, usize> =
        configs.iter().map(|c| (c.step_id.clone(), 0usize)).collect();
    loop {
        let mut changed = false;
        for c in configs {
            let want = c
                .depends_on
                .iter()
                .filter(|d| ids.contains(d.as_str()))
                .map(|d| level.get(d).copied().unwrap_or(0) + 1)
                .max()
                .unwrap_or(0);
            if want > level[&c.step_id] {
                level.insert(c.step_id.clone(), want);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Group steps by level into waves; each wave is one concurrent batch.
    let mut by_level: BTreeMap<usize, Vec<PlannedStep>> = BTreeMap::new();
    let mut batch_by_level: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for c in configs {
        let lvl = level[&c.step_id];
        let gate_barrier = c
            .depends_on
            .iter()
            .find(|d| gate_ids.contains(d.as_str()))
            .cloned();
        by_level.entry(lvl).or_default().push(PlannedStep {
            id: c.step_id.clone(),
            placement: "local".to_string(),
            cache_key: String::new(),
            cache_hit: false,
            is_gate: c.executor == "manual",
            gate_barrier,
            cost_usd: 0.0,
            concurrency_weight: 1,
        });
        batch_by_level.entry(lvl).or_default().push(c.step_id.clone());
    }

    let max_concurrency = by_level.values().map(|s| s.len()).max().unwrap_or(1) as u32;
    let waves: Vec<Wave> = by_level
        .into_iter()
        .map(|(lvl, steps)| Wave {
            index: lvl as u32,
            steps,
            batches: vec![batch_by_level.remove(&lvl).unwrap_or_default()],
        })
        .collect();

    PhysicalPlan {
        tenant_id: tenant,
        waves,
        max_concurrency,
        max_cost_usd: 0.0,
        total_cost_usd: 0.0,
    }
}

/// Run the workflow through the real executor and summarize the exec events that fired into a
/// single-line JSON (the run-execution oracle). `mode = "seq"` ⇒ sequential fallback (no plan).
async fn run_workflow(
    board_id: &str,
    mode: &str,
    cmd_msg_tx: &UnboundedSender<CommandMsg>,
) -> Result<String> {
    let configs = load_step_configs(board_id)?;
    if configs.is_empty() {
        return Err(anyhow!("no pipeline steps on board {board_id} (author first)"));
    }
    let plan = if mode == "seq" {
        None
    } else {
        Some(build_level_set_plan(board_id, &configs))
    };

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<SwiftEvent>();
    let result = cyan_backend::pipeline::run_pipeline_with_plan(board_id, plan, cmd_msg_tx, &ev_tx)
        .await
        .map_err(|e| anyhow!("run_pipeline_with_plan: {e}"))?;

    // Drain the run's exec events (the channel is closed once `run_pipeline_with_plan` returns,
    // so this is bounded — no live wait) and bucket the step-state transitions.
    use std::collections::BTreeSet;
    let (mut started, mut finished, mut stats, mut progress) = (0u32, 0u32, 0u32, 0u32);
    let mut finished_state = String::new();
    let mut by_state: std::collections::BTreeMap<String, BTreeSet<String>> = Default::default();
    while let Ok(ev) = ev_rx.try_recv() {
        match ev {
            SwiftEvent::WorkflowRunStarted { .. } => started += 1,
            SwiftEvent::WorkflowRunFinished { state, .. } => {
                finished += 1;
                finished_state = state;
            }
            SwiftEvent::WorkflowStatsUpdated { .. } => stats += 1,
            SwiftEvent::StepProgress { .. } => progress += 1,
            SwiftEvent::StepStateChanged { step_id, state, .. } => {
                by_state.entry(state).or_default().insert(step_id);
            }
            _ => {}
        }
    }
    let bucket = |k: &str| -> Vec<String> {
        by_state.get(k).map(|s| s.iter().cloned().collect()).unwrap_or_default()
    };

    let summary = serde_json::json!({
        "run_id": result.get("run_id").and_then(|v| v.as_str()).unwrap_or(""),
        "mode": result.get("mode").and_then(|v| v.as_str()).unwrap_or(""),
        "peak": result.get("peak_concurrency").and_then(|v| v.as_u64()).unwrap_or(0),
        "started": started,
        "finished": finished,
        "finished_state": finished_state,
        "stats": stats,
        "progress": progress,
        "running": bucket("running"),
        "done": bucket("done"),
        "failed": bucket("failed"),
        "awaiting": bucket("awaiting_approval"),
        "pending": bucket("pending"),
        "approved": bucket("approved"),
    });
    Ok(serde_json::to_string(&summary)?)
}

/// THIS node's run-state for one step: the cell's `pipeline.state.status`, or `absent`.
fn workflow_state(board_id: &str, step_id: &str) -> Result<String> {
    let conn = storage::db().lock().map_err(|e| anyhow!("db lock: {e}"))?;
    let mut stmt = conn.prepare(
        "SELECT json_extract(metadata_json, '$.pipeline.state.status') \
         FROM notebook_cells \
         WHERE board_id = ?1 AND json_extract(metadata_json, '$.pipeline.step_id') = ?2",
    )?;
    let status = stmt
        .query_row(rusqlite::params![board_id, step_id], |r| {
            r.get::<_, Option<String>>(0)
        })
        .ok()
        .flatten();
    Ok(status.unwrap_or_else(|| "absent".to_string()))
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
        // ROUND8 §W3: count only the system (Plugins) workspaces — proves the `system`
        // flag replicated to a joiner, not just the workspace rows.
        "system_workspaces" => storage::workspace_list_by_group(group_id)
            .map_err(|e| anyhow!("workspace_list_by_group: {e}"))?
            .iter()
            .filter(|w| w.system)
            .count(),
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
        "notes" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::note_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("note_list_by_boards: {e}"))?
                .len()
        }
        // ROUND8 §W4: count only the PINNED boards — proves the pinned state (not just
        // a pin row) replicated to a joiner via the digest + snapshot.
        "pins" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::pin_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("pin_list_by_boards: {e}"))?
                .iter()
                .filter(|p| p.pinned)
                .count()
        }
        "files" => storage::file_list_by_group(group_id)
            .map_err(|e| anyhow!("file_list_by_group: {e}"))?
            .len(),
        // R12 C3: count boards PINNED via the board-pin lane (`board_metadata.is_pinned`, the C1/C2
        // convergent delta) — the per-node oracle that a dropped `BoardPinned` was reconciled by
        // anti-entropy. Distinct from the ROUND8 workflow-pin `pins` kind above.
        "board_pins" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::board_metadata_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("board_metadata_list_by_boards: {e}"))?
                .iter()
                .filter(|m| m.is_pinned)
                .count()
        }
        // R12 C3 (D2/E1 lane): count DEPLOYED boards (`board_workflow_state.deployed`) — the oracle
        // that a workflow deploy a peer missed was reconciled via the digest + snapshot repair.
        "deployed" => {
            let board_ids = board_ids(&ws_ids)?;
            storage::workflow_state_list_by_boards(&board_ids)
                .map_err(|e| anyhow!("workflow_state_list_by_boards: {e}"))?
                .iter()
                .filter(|s| s.deployed)
                .count()
        }
        // MESH_HARDENING §3: the PERSISTENT roster — every peer ever seen over the mesh,
        // online or not (the row is never deleted). The honest per-node oracle for "an offline
        // peer keeps its cached row" and "the roster survives reconnect".
        "members" => storage::group_members_list(group_id).len(),
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
            &board,
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

/// S5 demo seed (dev only — explicit external action, NOT engine auto-seed, which is
/// deliberately removed per R10FB §D). Builds the named broadcast scenario Rick walks:
/// a "Post-Production" group + its create-path workspaces, a "Broadcast Delivery" board
/// carrying a sample English workflow (markdown step cells), deployed + pinned so the
/// board's Dashboard FACE renders the running workflow, plus a few asset artifacts.
/// Uses ONLY the public storage API. Idempotent: a no-op once the group has a board, so
/// re-running on every bring-up converges to the same state. Returns the board id.
fn seed_postprod(group_id: &str) -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    let board = format!("{group_id}-broadcast-delivery");
    // Idempotent guard: already seeded if the group has any board.
    if count_kind("boards", group_id).unwrap_or(0) > 0 {
        return Ok(board);
    }
    storage::group_insert_simple(group_id, "Post-Production", "film.stack", "#22D3EE")
        .map_err(|e| anyhow!("group_insert_simple: {e}"))?;
    // The create-path workspaces (default landing + system "Plugins"); the default is the
    // board's home. `provision_group_workspaces` is INSERT OR IGNORE → idempotent.
    let (default_ws, _plugins) = storage::provision_group_workspaces(group_id, None)
        .map_err(|e| anyhow!("provision_group_workspaces: {e}"))?;
    let ws = default_ws.id;
    storage::board_insert_simple(&board, &ws, "Broadcast Delivery", now)
        .map_err(|e| anyhow!("board_insert_simple: {e}"))?;
    // The sample English workflow — the steps in plain English (the Notebook face authors
    // these; compile/decompose turns them into the DAG). Markdown cells so they read as prose.
    // Each cell carries a BOUND pipeline config in metadata_json (`{"pipeline":{…}}`) so
    // the Dashboard shows real steps immediately (no "0 steps") AND Run executes them
    // deterministically. executor="lens" routes to the cloud orchestrator → cyan-media on
    // the box (probe/transcribe over the staged clips); the asset is bound in the cell text
    // (the bare filename) so the 8B doesn't author a path. "manual" = a human approval gate.
    // (cell text, step_id, executor, depends_on)
    let steps: [(&str, &str, &str, &[&str]); 4] = [
        ("Ingest the broadcast master: the local file big-buck-bunny.mp4 (in the media root).",
         "ingest", "lens", &[]),
        ("QC / probe: run the cyan-media probe tool on big-buck-bunny.mp4 — pass the bare filename as input (not a URL) — and report container, video codec, resolution, and duration.",
         "qc-probe", "lens", &["ingest"]),
        ("Transcribe: run the cyan-media transcribe tool on elephants-dream-30s.mp4 (bare filename, not a URL) to capture the spoken dialogue and subtitles.",
         "transcribe", "lens", &["qc-probe"]),
        ("Package: deliver the master at -14 LUFS and write the delivery sidecar.",
         "package", "manual", &["transcribe"]),
    ];
    for (i, (text, step_id, executor, deps)) in steps.iter().enumerate() {
        let meta = serde_json::json!({
            "pipeline": {
                "step_id": step_id,
                "depends_on": deps,
                "executor": executor,
                "model": "cyan-lens",
                "timeout_seconds": 300,
                "retry_count": 1,
                "auto_advance": false,
                "notifications": [],
                "state": { "status": "pending", "attempt": 0 }
            }
        })
        .to_string();
        storage::cell_insert_simple(
            &format!("{board}-{step_id}"), &board, "markdown", i as i32,
            Some(text), None, false, None, Some(&meta), now, now,
        )
        .map_err(|e| anyhow!("cell_insert_simple: {e}"))?;
    }
    // Deploy (dashboard=true) + pin so the board's Dashboard FACE shows the running workflow.
    storage::workflow_state_set_deployed(&board, true, now)
        .map_err(|e| anyhow!("workflow_state_set_deployed: {e}"))?;
    storage::board_meta_set_pinned(&board, true, now)
        .map_err(|e| anyhow!("board_meta_set_pinned: {e}"))?;
    // A few asset artifacts — the staged demo clips, attached to the board.
    for (i, name) in ["big-buck-bunny.mp4", "bars-smpte-720p-15s.mp4", "rgb-480p-12s.mp4"]
        .iter()
        .enumerate()
    {
        storage::file_insert_simple(
            &format!("{board}-asset-{i:02}"), Some(group_id), Some(&ws), Some(&board),
            name, &format!("seed-asset-{i}"), 10_000_000, None, now,
        )
        .map_err(|e| anyhow!("file_insert_simple: {e}"))?;
    }
    Ok(board)
}

/// The group ids the demo scale-seed OWNS. `seed_demo` deletes every one of these
/// before re-creating its set, so a re-seed converges to EXACTLY the intended groups
/// with zero duplicates — and it also reaps the prior botched seed (three groups that
/// all reused the name "Post-Production": post-production / promos / trailers).
const SEED_MANAGED_GROUP_IDS: [&str; 4] = ["post-production", "promos", "trailers", "broadcast"];

struct SeedBoard {
    id: &'static str,
    name: &'static str,
    clip: &'static str,
    /// true ⇒ the clip has a real AUDIO track (verified via ffprobe on the lens box),
    /// so transcribe + loudness QC are runtime-coherent. Audioless clips (sintel /
    /// tears-of-steel / jellyfish / bars / rgb excerpts) get black/freeze QC only —
    /// a loudness/transcribe step on them would fail ffmpeg at run time.
    audio: bool,
}
struct SeedGroup {
    id: &'static str,
    name: &'static str,
    icon: &'static str,
    color: &'static str,
    boards: &'static [SeedBoard],
}

/// Coherent, idempotent demo scale-seed (items #1/#27/STEP2). See the `seed_demo`
/// dispatch comment. Every clip below is a real file staged in the lens media root with
/// a matching thumbnail (`/api/v1/media/thumbnail?asset=<clip>` ⇒ 200 image/jpeg).
fn seed_demo() -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    // 1) Truncate the managed groups (cascades workspaces/boards/cells/files) so any
    //    prior or duplicate seed data is gone before we re-seed → idempotent, no dups.
    for gid in SEED_MANAGED_GROUP_IDS {
        let _ = storage::group_delete(gid);
    }
    // group_delete doesn't cascade board_workflow_state — prune the orphaned deploy-state
    // rows so a re-seed leaves NO stale rows (the board-card deploy gate reads this table).
    let _ = storage::workflow_state_prune_orphans();
    // 2) The coherent set: 3 distinctly-named groups, 10 distinctly-named boards, each
    //    bound to ONE real staged clip. No two groups/boards share a name.
    let groups: [SeedGroup; 3] = [
        SeedGroup {
            id: "post-production",
            name: "Post-Production",
            icon: "film.stack",
            color: "#22D3EE",
            boards: &[
                SeedBoard { id: "pp-sintel-finish", name: "Sintel — Color & Finish", clip: "sintel-clip.mp4", audio: false },
                SeedBoard { id: "pp-tos-online", name: "Tears of Steel — Online Edit", clip: "tears-of-steel-clip.mp4", audio: false },
                SeedBoard { id: "pp-ed-dialogue", name: "Elephants Dream — Dialogue Pass", clip: "elephants-dream-30s.mp4", audio: true },
                SeedBoard { id: "pp-bbb-master", name: "Big Buck Bunny — Feature Master", clip: "big-buck-bunny.mp4", audio: true },
            ],
        },
        SeedGroup {
            id: "promos",
            name: "Trailers & Promos",
            icon: "megaphone.fill",
            color: "#A855F7",
            boards: &[
                SeedBoard { id: "pr-sintel-teaser", name: "Sintel — Teaser Cut", clip: "sintel-clip.mp4", audio: false },
                SeedBoard { id: "pr-tos-trailer", name: "Tears of Steel — Trailer", clip: "tears-of-steel-clip.mp4", audio: false },
                SeedBoard { id: "pr-jelly-broll", name: "Jellyfish — Nature B-Roll", clip: "jellyfish-broll.mp4", audio: false },
            ],
        },
        SeedGroup {
            id: "broadcast",
            name: "Broadcast Delivery",
            icon: "antenna.radiowaves.left.and.right",
            color: "#34D399",
            boards: &[
                SeedBoard { id: "bc-smpte-qc", name: "SMPTE Bars — QC Gate", clip: "bars-smpte-720p-15s.mp4", audio: false },
                SeedBoard { id: "bc-rgb-align", name: "RGB — Alignment Check", clip: "rgb-480p-12s.mp4", audio: false },
                SeedBoard { id: "bc-bbb-package", name: "Big Buck Bunny — Broadcast Package", clip: "big-buck-bunny.mp4", audio: true },
            ],
        },
    ];
    let mut n_boards = 0usize;
    for g in &groups {
        storage::group_insert_simple(g.id, g.name, g.icon, g.color)
            .map_err(|e| anyhow!("group_insert_simple({}): {e}", g.id))?;
        let (default_ws, _plugins) = storage::provision_group_workspaces(g.id, None)
            .map_err(|e| anyhow!("provision_group_workspaces({}): {e}", g.id))?;
        let ws = default_ws.id;
        for b in g.boards {
            seed_board(g.id, &ws, b, now)?;
            n_boards += 1;
        }
    }
    Ok(format!("{} groups / {} boards (no dups)", groups.len(), n_boards))
}

/// Seed one board: a deployed+pinned workflow whose EVERY step names the board's own
/// clip (so the per-step asset frame is coherent), plus the bound clip as a file asset.
fn seed_board(group_id: &str, ws: &str, b: &SeedBoard, now: i64) -> Result<()> {
    let board = b.id;
    storage::board_insert_simple(board, ws, b.name, now)
        .map_err(|e| anyhow!("board_insert_simple({board}): {e}"))?;
    let clip = b.clip;
    // (cell text, step_id, executor, depends_on) — coherent per-board clip throughout.
    // The QC step names the EXACT cyan-media tool(s) (qc_black_freeze / qc_loudness) so
    // the 8B emits the right `mcp_tool` name instead of guessing (e.g. "blackdetect").
    let qc_text = if b.audio {
        format!("QC findings: call the cyan-media tool qc_black_freeze on {clip} (bare filename) for black/freeze time ranges, then call qc_loudness on {clip} with target_lufs -14. Report the timecoded black ranges, freeze ranges, and the integrated LUFS.")
    } else {
        format!("QC findings: call the cyan-media tool qc_black_freeze on {clip} (bare filename) for black/freeze time ranges and report them. This clip has no audio track, so skip loudness.")
    };
    let mut steps: Vec<(String, &str, &str, Vec<&str>)> = vec![
        (format!("Ingest the broadcast master: the local file {clip} (in the media root)."),
         "ingest", "lens", vec![]),
        (format!("QC / probe: run the cyan-media probe tool on {clip} — pass the bare filename as input (not a URL) — and report container, video codec, resolution, and duration."),
         "qc-probe", "lens", vec!["ingest"]),
        (qc_text, "qc-findings", "lens", vec!["qc-probe"]),
    ];
    let mut last = "qc-findings";
    if b.audio {
        steps.push((
            format!("Transcribe: run the cyan-media transcribe tool on {clip} (bare filename, not a URL) to capture the spoken dialogue and subtitles."),
            "transcribe", "lens", vec!["qc-findings"],
        ));
        last = "transcribe";
    }
    steps.push((
        format!("Package: deliver {clip} at -14 LUFS and write the delivery sidecar."),
        "package", "manual", vec![last],
    ));
    for (i, (text, step_id, executor, deps)) in steps.iter().enumerate() {
        let meta = serde_json::json!({
            "pipeline": {
                "step_id": step_id,
                "depends_on": deps,
                "executor": executor,
                "model": "cyan-lens",
                "timeout_seconds": 300,
                "retry_count": 1,
                "auto_advance": false,
                "notifications": [],
                "state": { "status": "pending", "attempt": 0 }
            }
        })
        .to_string();
        storage::cell_insert_simple(
            &format!("{board}-{step_id}"), board, "markdown", i as i32,
            Some(text), None, false, None, Some(&meta), now, now,
        )
        .map_err(|e| anyhow!("cell_insert_simple({board}-{step_id}): {e}"))?;
    }
    // Mark the board DEPLOYED via the workflow API so the LOCAL deploy state is accurate
    // (the board-card living-wall reads this through the cyan_board_workflow_state FFI).
    cyan_backend::workflow::mark_deployed(board, true, now)
        .map_err(|e| anyhow!("mark_deployed({board}): {e}"))?;
    storage::board_meta_set_pinned(board, true, now)
        .map_err(|e| anyhow!("board_meta_set_pinned({board}): {e}"))?;
    // The bound clip as the board's primary asset artifact (coherent with the steps).
    storage::file_insert_simple(
        &format!("{board}-asset"), Some(group_id), Some(ws), Some(board),
        b.clip, &format!("seed-{board}"), 10_000_000, None, now,
    )
    .map_err(|e| anyhow!("file_insert_simple({board}-asset): {e}"))?;
    Ok(())
}

/// ROUND8 §W3: provision a group the way the create path does — a group record plus
/// its two auto-seeded workspaces (default landing + system "Plugins"). Mirrors what
/// `CommandActor::CreateGroup` seeds, so the snapshot the host serves carries both.
fn provision_group(group_id: &str) -> Result<()> {
    storage::group_insert_simple(group_id, "Provisioned Group", "folder.fill", "#00AEEF")
        .map_err(|e| anyhow!("group_insert_simple: {e}"))?;
    storage::provision_group_workspaces(group_id, None)
        .map_err(|e| anyhow!("provision_group_workspaces: {e}"))?;
    Ok(())
}

/// Create the base tables the migrations assume exist (mirrors the in-process
/// harness's `init_base_schema` and the multi-process bins' `init_test_schema`).
fn init_base_schema(db_path: &str) -> Result<()> {
    // Share the engine's hardened open path: creates the parent dir and returns a
    // typed error instead of panicking when the data dir does not exist yet.
    let conn = storage::open_db(std::path::Path::new(db_path))?;
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
