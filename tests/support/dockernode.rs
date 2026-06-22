//! Docker rig driver — spawn `cyan_node` peers as CONTAINERS on Docker networks so the
//! connectivity ladder (LAN / relay-only / WebSocket-only / offline) can be forced for
//! real, then drive them over the SAME stdin/stdout line protocol the in-process
//! multi-process rig uses (`tests/support/multiprocess.rs`). Responses are `@@CYAN@@`
//! tagged; every wait is bounded by a `tokio::time::timeout`.
//!
//! This module shells out to `docker` only — it has NO dependency on the engine crate, so
//! it compiles in a plain `cargo test` even though the rungs that use it are `#[ignore]`d
//! and gated behind `CYAN_RIG=1` (see `tests/substrate_relay.rs`). Nothing here runs unless
//! a rung test is invoked with `--ignored` and the rig env is present.

#![allow(dead_code)]

use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Response sentinel — must match `src/bin/cyan_node.rs`.
const SENTINEL: &str = "@@CYAN@@";

/// Default per-request timeout for control verbs.
pub const REQ_TIMEOUT: Duration = Duration::from_secs(45);

/// The cyan_node image, overridable via env (Makefile sets `CYAN_NODE_IMAGE`).
pub fn node_image() -> String {
    std::env::var("CYAN_NODE_IMAGE").unwrap_or_else(|_| "cyan/node:rig".to_string())
}

/// The xaeroflux bootstrap image (the discovery-rendezvous node), overridable via env.
pub fn bootstrap_image() -> String {
    std::env::var("CYAN_BOOTSTRAP_IMAGE").unwrap_or_else(|_| "cyan/bootstrap:rig".to_string())
}

/// The relay URL peers use on the relay/WebSocket rungs (Makefile sets `CYAN_RELAY_URL`).
pub fn relay_url() -> String {
    std::env::var("CYAN_RELAY_URL").unwrap_or_else(|_| "http://cyan-rig-relay:3340".to_string())
}

/// Relay policy passed to a peer container via the `RELAY` env the bin reads.
#[derive(Clone, Debug)]
pub enum Relay {
    Disabled,
    Url(String),
}

/// One `cyan_node` CONTAINER, driven over its stdin/stdout line protocol.
pub struct DockerNode {
    pub name: String,
    pub node_id: String,
    /// The primary Docker network this container was launched on (the one `partition`/`heal`
    /// detach and re-attach to emulate a node going offline/coming back without killing it).
    pub network: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    /// Path to the captured container stderr (engine + iroh tracing) — the relay oracle
    /// reads `home is now relay` / connection-type lines from here.
    pub log_path: std::path::PathBuf,
}

impl Drop for DockerNode {
    fn drop(&mut self) {
        // Force-remove the container (it is `--rm`, but kill it deterministically too).
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.child.start_kill();
    }
}

/// Everything needed to launch one peer container.
pub struct Spec<'a> {
    pub name: &'a str,
    /// The single Docker network this peer attaches to (LAN/offline: shared; relay/ws:
    /// its own mesh island so the only common reachability is the relay).
    pub network: &'a str,
    pub relay: Relay,
    pub discovery_key: &'a str,
    pub bootstrap_node_id: Option<&'a str>,
    pub seed_fixture_group: Option<&'a str>,
    /// WebSocket-only rung: use ws-entrypoint.sh to black-hole outbound UDP first.
    pub block_udp: bool,
}

impl DockerNode {
    /// `docker rm -f` any stale container with this name (best effort).
    fn pre_clean(name: &str) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    /// Launch a peer container and resolve its node id (also confirms the actor booted).
    pub async fn spawn(spec: Spec<'_>) -> Result<DockerNode> {
        Self::spawn_with_env(spec, &[]).await
    }

    /// `spawn` plus extra `-e KEY=VALUE` env pairs — used to drive the §5 discover-via-published-
    /// config path (`CYAN_RENDEZVOUS_URL` + `CYAN_ORG_PUBKEY`) the explicit `Spec` fields don't cover.
    pub async fn spawn_with_env(spec: Spec<'_>, extra_env: &[(&str, &str)]) -> Result<DockerNode> {
        Self::pre_clean(spec.name);

        // Capture container stderr (engine eprintln + iroh tracing) to a file so the relay
        // oracle can read iroh's own connection-type/home-relay lines from it. Defaults to
        // the temp dir; override with CYAN_RIG_LOG_DIR to keep logs for post-mortem.
        let log_dir = std::env::var("CYAN_RIG_LOG_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        std::fs::create_dir_all(&log_dir).ok();
        let log_path = log_dir.join(format!("cyan-rig-{}.log", spec.name));
        let log_file = std::fs::File::create(&log_path)
            .with_context(|| format!("create log file {log_path:?}"))?;

        let relay_env = match &spec.relay {
            Relay::Disabled => "disabled".to_string(),
            Relay::Url(u) => u.clone(),
        };

        let mut args: Vec<String> = vec![
            "run".into(),
            "-i".into(),
            "--rm".into(),
            "--name".into(),
            spec.name.into(),
            "--cap-add".into(),
            "NET_ADMIN".into(),
            "--network".into(),
            spec.network.into(),
            "-e".into(),
            "NODE_DB=/data/node.db".into(),
            "-e".into(),
            "DATA_DIR=/data/data".into(),
            "-e".into(),
            format!("DISCOVERY_KEY={}", spec.discovery_key),
            "-e".into(),
            format!("RELAY={relay_env}"),
            // RUST_LOG turns on iroh tracing to stderr inside the bin — the oracle source.
            "-e".into(),
            "RUST_LOG=iroh::magicsock=debug,iroh_relay=info,info".into(),
        ];
        if let Some(b) = spec.bootstrap_node_id {
            args.push("-e".into());
            args.push(format!("BOOTSTRAP_NODE_ID={b}"));
        }
        if let Some(g) = spec.seed_fixture_group {
            args.push("-e".into());
            args.push(format!("SEED_FIXTURE={g}"));
        }
        if spec.block_udp {
            args.push("--entrypoint".into());
            args.push("/usr/local/bin/ws-entrypoint.sh".into());
        }
        for (k, v) in extra_env {
            args.push("-e".into());
            args.push(format!("{k}={v}"));
        }
        args.push(node_image());

        let mut child = Command::new("docker")
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(log_file))
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("docker run cyan_node ({})", spec.name))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("{}: no child stdin", spec.name))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("{}: no child stdout", spec.name))?;

        let mut node = DockerNode {
            name: spec.name.to_string(),
            node_id: String::new(),
            network: spec.network.to_string(),
            child,
            stdin,
            stdout: BufReader::new(stdout),
            log_path,
        };

        // First image pull + container start can be slow; give node_id extra slack.
        let resp = node.request("node_id", Duration::from_secs(120)).await?;
        node.node_id = resp
            .strip_prefix("node_id ")
            .ok_or_else(|| anyhow!("{}: unexpected node_id response: {resp}", node.name))?
            .to_string();
        Ok(node)
    }

    async fn request(&mut self, line: &str, timeout: Duration) -> Result<String> {
        self.stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .with_context(|| format!("{}: write '{line}'", self.name))?;
        self.stdin.flush().await.ok();
        self.read_resp(timeout).await
    }

    /// Read stdout until a `@@CYAN@@`-tagged line; ignore any other output. Bounded.
    async fn read_resp(&mut self, timeout: Duration) -> Result<String> {
        let name = self.name.clone();
        let stdout = &mut self.stdout;
        tokio::time::timeout(timeout, async {
            let mut line = String::new();
            loop {
                line.clear();
                let n = stdout.read_line(&mut line).await?;
                if n == 0 {
                    return Err(anyhow!("{name}: child stdout closed"));
                }
                let trimmed = line.trim_end();
                if let Some(payload) = trimmed.strip_prefix(SENTINEL) {
                    let payload = payload.trim();
                    if let Some(err) = payload.strip_prefix("err ") {
                        return Err(anyhow!("{name}: control error: {err}"));
                    }
                    return Ok(payload.to_string());
                }
            }
        })
        .await
        .map_err(|_| anyhow!("{}: timeout after {:?} reading response", self.name, timeout))?
    }

    /// The bootstrap id this node RESOLVED at startup — from an explicit `BOOTSTRAP_NODE_ID`, a
    /// verified rendezvous config (`CYAN_RENDEZVOUS_URL`), or the bundled fallback. Lets a test
    /// assert positively that a peer adopted the LIVE published id (not the hardcode), or fell back.
    pub async fn bootstrap_id(&mut self) -> Result<String> {
        let resp = self.request("bootstrap_id", REQ_TIMEOUT).await?;
        resp.strip_prefix("bootstrap_id ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected bootstrap_id response: {resp}", self.name))
    }

    /// This node's serialized `EndpointAddr` (JSON) — `{"id":..,"addrs":[{"Relay"|"Ip"..}]}`.
    pub async fn addr(&mut self) -> Result<String> {
        let resp = self.request("addr", REQ_TIMEOUT).await?;
        resp.strip_prefix("addr ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected addr response: {resp}", self.name))
    }

    /// Poll `addr` until it advertises a `{"Relay": ...}` entry (the node has homed to the
    /// relay), bounded. Required before exchanging addrs on the relay/WebSocket rungs so
    /// the peer can fall back to the relay when the direct `Ip` is non-routable.
    pub async fn await_relay_addr(&mut self, timeout: Duration) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let a = self.addr().await?;
            if a.contains("\"Relay\"") {
                return Ok(a);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "{}: no relay entry in addr within {:?} (last: {a})",
                    self.name,
                    timeout
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn add_peer(&mut self, addr_json: &str) -> Result<()> {
        self.request(&format!("add_peer {addr_json}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    pub async fn seed_fixture(&mut self, group_id: &str) -> Result<()> {
        self.request(&format!("seed_fixture {group_id}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    pub async fn join_group(&mut self, group_id: &str, bootstrap: Option<&str>) -> Result<()> {
        let line = match bootstrap {
            Some(b) => format!("join_group {group_id} {b}"),
            None => format!("join_group {group_id}"),
        };
        self.request(&line, REQ_TIMEOUT).await.map(|_| ())
    }

    pub async fn wait_sync(&mut self, group_id: &str, timeout: Duration) -> Result<bool> {
        let ms = timeout.as_millis() as u64;
        let resp = self
            .request(
                &format!("wait_sync {group_id} {ms}"),
                timeout + Duration::from_secs(10),
            )
            .await?;
        Ok(resp == "ok wait_sync")
    }

    pub async fn count(&mut self, kind: &str, group_id: &str) -> Result<usize> {
        let resp = self
            .request(&format!("count {kind} {group_id}"), REQ_TIMEOUT)
            .await?;
        resp.strip_prefix(&format!("count {kind} "))
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| anyhow!("{}: unexpected count response: {resp}", self.name))
    }

    // ── Mesh-hardening verbs (MESH_HARDENING §2/§3/§5/§11) ──────────────────────────────

    /// §2 seed pipeline: feed a resolvable `EndpointAddr` (JSON) into ONE group's gossip topic
    /// so `NeighborUp` fires with no relay/bootstrap. The no-infra mesh-formation path (the rig
    /// stand-in for an mDNS-discovered peer, since Docker bridges don't carry multicast).
    pub async fn seed_peer(&mut self, group_id: &str, addr_json: &str) -> Result<()> {
        self.request(&format!("seed_peer {group_id} {addr_json}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// §5 incremental catch-up: pull only the range since `since` from `source_peer` (a holder).
    /// `since == None` ⇒ the engine uses the persisted import/high-water mark. Returning peers
    /// reconcile a partition/offline gap with this instead of a full re-snapshot.
    pub async fn catch_up(
        &mut self,
        group_id: &str,
        source_peer: &str,
        since: Option<i64>,
    ) -> Result<()> {
        let line = match since {
            Some(s) => format!("catch_up {group_id} {source_peer} {s}"),
            None => format!("catch_up {group_id} {source_peer}"),
        };
        self.request(&line, REQ_TIMEOUT).await.map(|_| ())
    }

    /// §3 persisted roster size for a group (every peer ever seen, online or not).
    pub async fn count_members(&mut self, group_id: &str) -> Result<usize> {
        self.count("members", group_id).await
    }

    /// §11 this device's X25519 sealed-box recipient key (hex) — an inviter exports a bundle TO it.
    pub async fn bundle_pubkey(&mut self) -> Result<String> {
        let resp = self.request("bundle_pubkey", REQ_TIMEOUT).await?;
        resp.strip_prefix("bundle_pubkey ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected bundle_pubkey response: {resp}", self.name))
    }

    /// §11 export a signed, grant-scoped, invitee-encrypted `.cyangroup` bundle (JSON on the wire).
    /// The bundle is handed out-of-band to an importer — it never crosses the mesh.
    pub async fn export_group(&mut self, group_id: &str, invitee_x_pub_hex: &str) -> Result<String> {
        let resp = self
            .request(&format!("export_group {group_id} {invitee_x_pub_hex}"), REQ_TIMEOUT)
            .await?;
        resp.strip_prefix("bundle ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected export_group response: {resp}", self.name))
    }

    /// §11 air-gapped import: verify + decrypt + seed + stamp watermark, touching NO network.
    /// Returns the imported group id.
    pub async fn import_group(&mut self, bundle_json: &str) -> Result<String> {
        let resp = self
            .request(&format!("import_group {bundle_json}"), REQ_TIMEOUT)
            .await?;
        resp.strip_prefix("ok import_group ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected import_group response: {resp}", self.name))
    }

    /// Post `n` live whiteboard-element edits (local insert + gossip broadcast) — §12 continuous
    /// delta-sync driver. Returns the count the node acknowledged.
    pub async fn post_edits(&mut self, group_id: &str, n: usize) -> Result<usize> {
        let resp = self.request(&format!("post_edits {group_id} {n}"), REQ_TIMEOUT).await?;
        resp.strip_prefix("ok post_edits ")
            .and_then(|s| s.trim().parse::<usize>().ok())
            .ok_or_else(|| anyhow!("{}: unexpected post_edits response: {resp}", self.name))
    }

    /// Post `n` live board-chat messages (local insert + `ChatSent` broadcast) — §12 chat-live driver.
    pub async fn post_chat(&mut self, group_id: &str, n: usize) -> Result<usize> {
        let resp = self.request(&format!("post_chat {group_id} {n}"), REQ_TIMEOUT).await?;
        resp.strip_prefix("ok post_chat ")
            .and_then(|s| s.trim().parse::<usize>().ok())
            .ok_or_else(|| anyhow!("{}: unexpected post_chat response: {resp}", self.name))
    }

    // ── netem / chaos: emulate offline, partition, and a degraded link (host-side docker) ──────
    //
    // These shell out to the host `docker` CLI against this container by name — they do NOT use
    // the stdin/stdout control channel, so they work even while the node is partitioned/paused.

    /// Take the node OFFLINE without killing it: `docker pause` freezes the process (its TCP/QUIC
    /// state goes dark). The engine state + DB survive, so `unpause` is a faithful "came back".
    pub async fn pause(&self) -> Result<()> {
        docker(&["pause", &self.name]).await
    }

    /// Bring a paused node back online.
    pub async fn unpause(&self) -> Result<()> {
        docker(&["unpause", &self.name]).await
    }

    /// Partition this node from its primary network (detach the bridge) — the process keeps
    /// running but loses all reachability to peers on that network. Heal with [`heal`].
    pub async fn partition(&self) -> Result<()> {
        docker(&["network", "disconnect", &self.network, &self.name]).await
    }

    /// Re-attach this node to its primary network after a [`partition`].
    pub async fn heal(&self) -> Result<()> {
        docker(&["network", "connect", &self.network, &self.name]).await
    }

    /// Attach this container to an ADDITIONAL network (e.g. so a durable holder is reachable from
    /// two otherwise-isolated mesh islands, like the relay is).
    pub async fn connect_network(&self, network: &str) -> Result<()> {
        docker(&["network", "connect", network, &self.name]).await
    }

    /// Apply a fixed egress latency to `eth0` via `tc netem` (needs `iproute2` + `NET_ADMIN`,
    /// both present in the rig image). Idempotent: replaces any prior qdisc.
    pub async fn set_latency(&self, delay_ms: u32) -> Result<()> {
        // `del` first so re-applying is idempotent; ignore its failure on a clean iface.
        let _ = docker(&["exec", &self.name, "tc", "qdisc", "del", "dev", "eth0", "root"]).await;
        let delay = format!("{delay_ms}ms");
        docker(&[
            "exec", &self.name, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay",
            &delay,
        ])
        .await
    }

    /// Remove any `tc netem` shaping from `eth0`.
    pub async fn clear_latency(&self) -> Result<()> {
        let _ = docker(&["exec", &self.name, "tc", "qdisc", "del", "dev", "eth0", "root"]).await;
        Ok(())
    }

    pub async fn quit(&mut self) -> Result<()> {
        self.stdin.write_all(b"quit\n").await.ok();
        self.stdin.flush().await.ok();
        Ok(())
    }

    /// Read this node's captured stderr (engine + iroh tracing) for oracle assertions.
    pub fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// iroh connection-type oracle: did this node home to the relay (i.e. the relay path is
    /// live)? Combined with the topological isolation of the rung (peers on split bridges
    /// with no direct route), a true here means traffic to the peer can only have used the
    /// relay. Reads iroh's OWN tracing — no custom engine meter.
    pub fn homed_to_relay(&self) -> bool {
        let log = self.read_log();
        log.contains("home is now relay")
    }

    /// True if iroh ever reported a DIRECT connection type to a remote (LAN rung oracle).
    pub fn observed_direct_conn(&self) -> bool {
        let log = self.read_log();
        log.contains("ConnectionType::Direct")
            || log.contains("conn_type=Direct")
            || log.contains("connection type: Direct")
    }
}

/// Run a host-side `docker <args>` command (chaos verbs: pause/unpause/network/exec tc), failing
/// with the captured stderr. Used for impairments that must work even while a node is unreachable.
async fn docker(args: &[&str]) -> Result<()> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .await
        .with_context(|| format!("spawn docker {:?}", args))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(anyhow!(
            "docker {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// One-directional address injection: teach `learner` how to reach `target` (so `learner` can
/// dial it directly where routable, via relay otherwise). Bounded by `addr`/`add_peer`.
pub async fn wire_into(learner: &mut DockerNode, target: &mut DockerNode) -> Result<()> {
    let target_addr = target.addr().await?;
    learner.add_peer(&target_addr).await
}

/// Exchange both nodes' serialized addresses into each other's StaticProvider so either can
/// dial the other (direct where routable, via relay otherwise). Bounded by `addr`/`add_peer`.
pub async fn wire_pair(a: &mut DockerNode, b: &mut DockerNode) -> Result<()> {
    let a_addr = a.addr().await?;
    let b_addr = b.addr().await?;
    a.add_peer(&b_addr).await?;
    b.add_peer(&a_addr).await?;
    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════════════════════
// BootstrapNode — the REAL xaeroflux discovery-rendezvous container (NOT a cyan_node)
// ══════════════════════════════════════════════════════════════════════════════════════════════
//
// The bootstrap is `xaeroflux_bootstrap` (xaeroflux/src/bin/xaeroflux_bootstrap.rs), built into
// `cyan/bootstrap:rig` (harness/Dockerfile.bootstrap). It does NOT speak the `@@CYAN@@` stdin/stdout
// control protocol — it runs detached and serves the iroh-gossip discovery mesh, acting as the
// cross-network peer-introducer / gossip relay. On startup it SELF-PUBLISHES a signed rendezvous
// config (its own node_id + bound direct addrs + discovery_key + relay) to a file inside the
// container, which we read back over `docker exec cat` — the same JSON a deploy would serve at the
// well-known URL and peers would fetch. This is the LIVE source of the bootstrap id (no hardcode).

/// The named Docker volume the bootstrap writes its rendezvous config into and the [`ConfigServer`]
/// serves from — the rig stand-in for the object store / well-known URL a deploy would use.
pub const RDV_VOLUME: &str = "cyan-rig-rdv";

/// One running `xaeroflux_bootstrap` CONTAINER, plus the signed rendezvous config it published.
pub struct BootstrapNode {
    pub name: String,
    /// The bootstrap's node_id (== its ed25519 public key, hex) — read from the published config,
    /// NOT hardcoded. This is what peers pin/dial.
    pub node_id: String,
    /// The dialable direct socket addrs (`ip:port`) the bootstrap published — filtered to the
    /// private/Docker-bridge addresses peers on the isolation networks can actually reach.
    pub addrs: Vec<String>,
    /// The raw bytes of the signed rendezvous config the bootstrap self-published (xaeroflux's
    /// `SignedRendezvousConfig` JSON: `{config:{...}, signer, signature}`). This is exactly what a
    /// static server would serve and `rendezvous::fetch_and_apply_if_configured` would fetch.
    pub published_config: Vec<u8>,
}

impl Drop for BootstrapNode {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl BootstrapNode {
    /// Launch the bootstrap on `networks` (attaches to all of them so it can bridge isolated
    /// islands like a real rendezvous), wait until it self-publishes its rendezvous config, and
    /// read back the LIVE node_id + dialable addrs. Iggy + n0 DNS are disabled (no broker / no
    /// public DNS in the rig), so the only thing it does is discovery/gossip — exactly its role.
    pub async fn spawn(name: &str, networks: &[&str], discovery_key: &str) -> Result<BootstrapNode> {
        Self::spawn_inner(name, networks, discovery_key, true, None).await
    }

    /// `spawn` with control over whether the shared rendezvous volume is recreated fresh (true for
    /// a brand-new bootstrap, false for a redeploy onto the same volume so the [`ConfigServer`]
    /// mount stays valid) and an optional `exclude_id` to wait PAST (so a redeploy resolves the
    /// NEW published id, not the stale one still on the volume until the new node overwrites it).
    async fn spawn_inner(
        name: &str,
        networks: &[&str],
        discovery_key: &str,
        fresh_volume: bool,
        exclude_id: Option<&str>,
    ) -> Result<BootstrapNode> {
        // Best-effort clean of a stale container with this name.
        let _ = docker(&["rm", "-f", name]).await;

        // The bootstrap writes its rendezvous config into RDV_VOLUME so the ConfigServer can serve
        // it. A brand-new bootstrap recreates the volume so no prior run's config lingers.
        if fresh_volume {
            let _ = docker(&["volume", "rm", "-f", RDV_VOLUME]).await;
            docker(&["volume", "create", RDV_VOLUME]).await?;
        }

        let primary = *networks
            .first()
            .ok_or_else(|| anyhow!("bootstrap needs at least one network"))?;
        let args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name.into(),
            "--network".into(),
            primary.into(),
            "-v".into(),
            format!("{RDV_VOLUME}:/opt/cyan/data"),
            "-e".into(),
            format!("DISCOVERY_KEY={discovery_key}"),
            "-e".into(),
            "IGGY_ENABLED=0".into(),
            "-e".into(),
            "NO_N0=1".into(),
            "-e".into(),
            "XAEROFLUX_ENV=rig".into(),
            "-e".into(),
            "RUST_LOG=info".into(),
            bootstrap_image(),
        ];
        let out = Command::new("docker")
            .args(&args)
            .output()
            .await
            .with_context(|| format!("docker run bootstrap ({name})"))?;
        if !out.status.success() {
            return Err(anyhow!(
                "docker run bootstrap {name} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }

        // Attach to the remaining networks so the bootstrap is reachable from each isolated island.
        for net in &networks[1..] {
            docker(&["network", "connect", net, name]).await?;
        }

        // Poll for the self-published rendezvous config (with at least one dialable direct addr).
        // Bounded — a bootstrap that never publishes a dialable addr is a real finding, not a hang.
        let mut node = BootstrapNode {
            name: name.to_string(),
            node_id: String::new(),
            addrs: Vec::new(),
            published_config: Vec::new(),
        };
        node.refresh_published_config(Duration::from_secs(60), exclude_id).await?;
        Ok(node)
    }

    /// Read the bootstrap's currently-published rendezvous config out of the container, parse the
    /// LIVE node_id + dialable addrs, and store the raw signed bytes. Polls (bounded) until the
    /// file exists, carries ≥1 private/dialable addr, and (if `exclude_id` is set) advertises an id
    /// DIFFERENT from it (used after a redeploy to skip the stale config still on the volume).
    pub async fn refresh_published_config(
        &mut self,
        timeout: Duration,
        exclude_id: Option<&str>,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(bytes) = self.read_config_file().await
                && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes)
            {
                let node_id = v["config"]["bootstrap"]["node_id"].as_str().unwrap_or("");
                let addrs: Vec<String> = v["config"]["bootstrap"]["addr"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| s.as_str())
                            .filter(|s| is_dialable_in_rig(s))
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                let fresh = exclude_id.map(|x| x != node_id).unwrap_or(true);
                if !node_id.is_empty() && !addrs.is_empty() && fresh {
                    self.node_id = node_id.to_string();
                    self.addrs = addrs;
                    self.published_config = bytes;
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "{}: bootstrap did not publish a fresh dialable rendezvous config within {:?}",
                    self.name,
                    timeout
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// `docker exec <name> cat /opt/cyan/data/rendezvous.json` — the published signed config.
    async fn read_config_file(&self) -> Result<Vec<u8>> {
        let out = Command::new("docker")
            .args(["exec", &self.name, "cat", "/opt/cyan/data/rendezvous.json"])
            .output()
            .await
            .with_context(|| format!("docker exec cat rendezvous ({})", self.name))?;
        if !out.status.success() {
            return Err(anyhow!("rendezvous.json not readable yet"));
        }
        Ok(out.stdout)
    }

    /// The bootstrap's iroh `EndpointAddr` in the JSON form `add_peer` accepts
    /// (`{"id":..,"addrs":[{"Ip":"ip:port"}]}`) — built from the LIVE published node_id + addrs.
    /// This is the ONLY address material peers are given: they learn how to reach the BOOTSTRAP,
    /// never each other (cross-peer discovery is the bootstrap's job to prove).
    pub fn endpoint_addr_json(&self) -> String {
        let addrs: Vec<serde_json::Value> = self
            .addrs
            .iter()
            .map(|a| serde_json::json!({ "Ip": a }))
            .collect();
        serde_json::json!({ "id": self.node_id, "addrs": addrs }).to_string()
    }

    /// Restart the bootstrap container so it generates a FRESH node identity and republishes — the
    /// redeploy scenario (a new node.key ⇒ a new node_id). Because the container is `--rm`-free here
    /// we remove + relaunch; a fresh ephemeral DB dir means a new key, hence a new id.
    pub async fn redeploy(&mut self, networks: &[&str], discovery_key: &str) -> Result<()> {
        let name = self.name.clone();
        let old_id = self.node_id.clone();
        let _ = docker(&["rm", "-f", &name]).await;
        // An identity-rotating redeploy: wipe the persisted `node.key` (and the stale config) from
        // the SHARED volume so the new node generates a FRESH id — exactly the case §5 must survive
        // with no app retune. The volume itself is kept so the ConfigServer mount stays valid.
        let _ = docker(&[
            "run", "--rm", "-v", &format!("{RDV_VOLUME}:/v"), "busybox", "sh", "-c",
            "rm -f /v/node.key /v/rendezvous.json",
        ])
        .await;
        // Reuse the SAME volume and wait until the published id has CHANGED from the old one — i.e.
        // the new node has written a fresh rendezvous.json.
        let replacement =
            BootstrapNode::spawn_inner(&name, networks, discovery_key, false, Some(&old_id)).await?;
        self.node_id = replacement.node_id.clone();
        self.addrs = replacement.addrs.clone();
        self.published_config = replacement.published_config.clone();
        std::mem::forget(replacement); // same container name; avoid the old Drop rm-f'ing it
        Ok(())
    }

    /// Corrupt the SERVED rendezvous config so its signature no longer covers the bytes (flip a
    /// value inside the signed `config`). A peer fetching it must reject it and fall back — proving
    /// no false bootstrap is adopted from a tampered doc. Modifies the shared volume in place.
    pub async fn tamper_served_config(&self) -> Result<()> {
        docker(&[
            "run", "--rm", "-v", &format!("{RDV_VOLUME}:/v"), "busybox", "sh", "-c",
            // Change the discovery_key inside the signed config; signer/signature are untouched, so
            // verification fails. (The string appears in both config and is safe to flip for this.)
            "sed -i 's/cyan-rig/evil-mesh/' /v/rendezvous.json",
        ])
        .await
    }

    pub fn read_log(&self) -> String {
        std::process::Command::new("docker")
            .args(["logs", &self.name])
            .output()
            .map(|o| {
                let mut s = String::from_utf8_lossy(&o.stdout).to_string();
                s.push_str(&String::from_utf8_lossy(&o.stderr));
                s
            })
            .unwrap_or_default()
    }
}

/// A socket addr the rig's containers can actually dial: a private (RFC1918) Docker-bridge address
/// or loopback. The bootstrap also publishes its public egress IP (from STUN/QAD), which no peer on
/// an isolated bridge can reach — filtering those out keeps dial attempts fast and meaningful.
fn is_dialable_in_rig(addr: &str) -> bool {
    let host = match addr.rsplit_once(':') {
        Some((h, _)) => h,
        None => addr,
    };
    host.starts_with("172.")
        || host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.starts_with("127.")
}

// ══════════════════════════════════════════════════════════════════════════════════════════════
// ConfigServer — the "well-known URL" that serves the bootstrap's self-published rendezvous config
// ══════════════════════════════════════════════════════════════════════════════════════════════
//
// A tiny `busybox httpd` container that serves [`RDV_VOLUME`] (the volume the bootstrap writes its
// rendezvous.json into) over HTTP. This is the rig stand-in for the object store / CloudFront path
// a deploy uploads the config to. Peers fetch `http://<server>:8080/rendezvous.json` via
// `CYAN_RENDEZVOUS_URL` and verify it — exactly the iOS-app discover→pin path, with NO hardcoded id.

pub struct ConfigServer {
    pub name: String,
    /// The URL peers reach (resolvable on the shared Docker networks via the container name).
    pub url: String,
}

impl Drop for ConfigServer {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl ConfigServer {
    /// Serve `RDV_VOLUME` over HTTP on `networks` (attach to all islands the peers live on). The
    /// served file is whatever the bootstrap last published; start this only AFTER the bootstrap's
    /// config is present so a peer never fetches an empty/partial doc.
    pub async fn spawn(name: &str, networks: &[&str]) -> Result<ConfigServer> {
        let _ = docker(&["rm", "-f", name]).await;
        let primary = *networks
            .first()
            .ok_or_else(|| anyhow!("config server needs at least one network"))?;
        // busybox httpd: foreground (-f), port 8080, webroot the mounted (read-only) volume.
        let args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name.into(),
            "--network".into(),
            primary.into(),
            "-v".into(),
            format!("{RDV_VOLUME}:/web:ro"),
            "busybox".into(),
            "httpd".into(),
            "-f".into(),
            "-p".into(),
            "8080".into(),
            "-h".into(),
            "/web".into(),
        ];
        let out = Command::new("docker")
            .args(&args)
            .output()
            .await
            .with_context(|| format!("docker run config server ({name})"))?;
        if !out.status.success() {
            return Err(anyhow!(
                "docker run config server {name} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        for net in &networks[1..] {
            docker(&["network", "connect", net, name]).await?;
        }
        Ok(ConfigServer {
            name: name.to_string(),
            url: format!("http://{name}:8080/rendezvous.json"),
        })
    }
}
