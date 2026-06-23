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
