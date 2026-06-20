//! Multi-process rig — spawn `cyan_node` peers in SEPARATE OS PROCESSES so each has
//! its OWN global SQLite DB, making per-node storage assertions honest (the in-process
//! harness shares one process-global DB; see `support` module docs).
//!
//! Each `MpNode` wraps a child `cyan_node` process driven over its stdin/stdout line
//! protocol (responses tagged `@@CYAN@@`). Relay is disabled; nodes dial each other
//! directly over loopback after exchanging serialized `EndpointAddr`s (JSON) into each
//! other's `StaticProvider`. Every request is bounded by a timeout — never an unbounded
//! read.

#![allow(dead_code)]

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Response sentinel — must match `src/bin/cyan_node.rs`.
const SENTINEL: &str = "@@CYAN@@";

/// Process-level state + metrics reported by `cyan_node`'s `metrics` verb (the stress oracles:
/// "no message storm" = `gossip_recv` bounded vs N; "bounded memory" = `rss_kb` over the run;
/// "bounded degree" = `gossip_degree`).
#[derive(Debug, Clone, Deserialize)]
pub struct NodeMetrics {
    pub node_id: String,
    pub rss_kb: u64,
    pub gossip_recv: u64,
    pub neighbor_up: u64,
    pub neighbor_down: u64,
    pub gossip_degree: u64,
    /// Anti-entropy state digests this peer has broadcast (sweep traffic = `O(1)`/tick).
    #[serde(default)]
    pub ae_digest_sent: u64,
    /// Anti-entropy repair pulls this peer has started (debounced; bounded, not per-message).
    #[serde(default)]
    pub ae_repair: u64,
    /// Snapshots this peer has served to others (the "no single-host overload" oracle).
    #[serde(default)]
    pub snapshot_served: u64,
}

/// Default per-request timeout for control verbs (boot/addr/count are fast).
pub const REQ_TIMEOUT: Duration = Duration::from_secs(30);

/// A `cyan_node` child process with its own DB, driven over a line protocol.
pub struct MpNode {
    pub name: String,
    pub node_id: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    // Holds the per-node temp dir (NODE_DB/DATA_DIR) alive for the node's lifetime.
    _tmp: tempfile::TempDir,
}

impl Drop for MpNode {
    fn drop(&mut self) {
        // Best-effort terminate; the child also exits on stdin close / `quit`.
        let _ = self.child.start_kill();
    }
}

impl MpNode {
    /// Spawn a `cyan_node` process with a fresh temp DB, relay disabled, the given
    /// discovery key, and an optional bootstrap node id. If `seed_fixture_group` is set,
    /// the process seeds the full host fixture for that group BEFORE its actor starts,
    /// so the engine's startup auto-spawns and hosts the group topic (the host role).
    /// Resolves its `node_id`.
    pub async fn spawn(
        name: &str,
        discovery_key: &str,
        bootstrap_node_id: Option<&str>,
        seed_fixture_group: Option<&str>,
    ) -> Result<MpNode> {
        Self::spawn_with_env(name, discovery_key, bootstrap_node_id, seed_fixture_group, &[]).await
    }

    /// Like [`MpNode::spawn`] but injects extra `(key, value)` environment variables into the child
    /// (e.g. `CYAN_AE_SWEEP_MS` to drive the anti-entropy sweep fast enough to observe convergence
    /// inside a bounded test timeout). Each child gets its OWN env — no process-global `set_var`.
    pub async fn spawn_with_env(
        name: &str,
        discovery_key: &str,
        bootstrap_node_id: Option<&str>,
        seed_fixture_group: Option<&str>,
        extra_env: &[(&str, &str)],
    ) -> Result<MpNode> {
        let tmp = tempfile::tempdir().context("create per-node tempdir")?;
        let db_path = tmp.path().join("node.db");
        let data_dir = tmp.path().join("data");

        let exe = env!("CARGO_BIN_EXE_cyan_node");
        let mut cmd = Command::new(exe);
        // Engine logs (stderr) → inherit by default; if CYAN_MP_LOG_DIR is set, route
        // each node's stderr to a file there for post-mortem debugging of the rig.
        let stderr = match std::env::var("CYAN_MP_LOG_DIR") {
            Ok(dir) if !dir.is_empty() => {
                let path = std::path::Path::new(&dir).join(format!("{name}.err.log"));
                match std::fs::File::create(&path) {
                    Ok(f) => std::process::Stdio::from(f),
                    Err(_) => std::process::Stdio::inherit(),
                }
            }
            _ => std::process::Stdio::inherit(),
        };
        cmd.env("NODE_DB", &db_path)
            .env("DATA_DIR", &data_dir)
            .env("DISCOVERY_KEY", discovery_key)
            .env("RELAY", "disabled")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(stderr)
            .kill_on_drop(true);
        if let Some(b) = bootstrap_node_id {
            cmd.env("BOOTSTRAP_NODE_ID", b);
        }
        if let Some(g) = seed_fixture_group {
            cmd.env("SEED_FIXTURE", g);
        }
        for (k, v) in extra_env {
            cmd.env(k, v);
        }

        let mut child = cmd.spawn().with_context(|| format!("spawn cyan_node ({name})"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("{name}: no child stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("{name}: no child stdout"))?;

        let mut node = MpNode {
            name: name.to_string(),
            node_id: String::new(),
            child,
            stdin,
            stdout: BufReader::new(stdout),
            _tmp: tmp,
        };

        // Resolve node id (also confirms the process booted and the actor is up).
        let resp = node.request("node_id", REQ_TIMEOUT).await?;
        node.node_id = resp
            .strip_prefix("node_id ")
            .ok_or_else(|| anyhow!("{name}: unexpected node_id response: {resp}"))?
            .to_string();
        Ok(node)
    }

    /// Send one request line and read exactly one tagged response, bounded by `timeout`.
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
                // Non-sentinel line on stdout (shouldn't happen): ignore.
            }
        })
        .await
        .map_err(|_| anyhow!("{}: timeout after {:?} reading response to a request", self.name, timeout))?
    }

    /// This node's serialized `EndpointAddr` (JSON), once it has a direct address.
    pub async fn addr(&mut self) -> Result<String> {
        let resp = self.request("addr", REQ_TIMEOUT).await?;
        resp.strip_prefix("addr ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected addr response: {resp}", self.name))
    }

    /// Inject a peer's serialized `EndpointAddr` (JSON) into this node's StaticProvider.
    pub async fn add_peer(&mut self, addr_json: &str) -> Result<()> {
        self.request(&format!("add_peer {addr_json}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// Create an empty group record (the "invite"), so this node hosts a topic to sync into.
    pub async fn seed_empty_group(&mut self, group_id: &str) -> Result<()> {
        self.request(&format!("seed_empty_group {group_id}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// Seed the full host fixture (group + ws + board + 5 elements + 3 cells + 3 chats + 1 file).
    pub async fn seed_fixture(&mut self, group_id: &str) -> Result<()> {
        self.request(&format!("seed_fixture {group_id}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// Join `group_id`, optionally seeded with a bootstrap peer's node id.
    pub async fn join_group(&mut self, group_id: &str, bootstrap: Option<&str>) -> Result<()> {
        let line = match bootstrap {
            Some(b) => format!("join_group {group_id} {b}"),
            None => format!("join_group {group_id}"),
        };
        self.request(&line, REQ_TIMEOUT).await.map(|_| ())
    }

    // ── Identity / RBAC (grant-gated snapshot tests) ──────────────────────────────────────

    /// This node's capability-grant (admin) Ed25519 pubkey hex.
    pub async fn admin_pubkey(&mut self) -> Result<String> {
        let resp = self.request("admin_pubkey", REQ_TIMEOUT).await?;
        resp.strip_prefix("admin_pubkey ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected admin_pubkey response: {resp}", self.name))
    }

    /// Turn ON grant enforcement for `group_id` and register this node as its Owner-admin.
    pub async fn enforce_group(&mut self, group_id: &str) -> Result<()> {
        self.request(&format!("enforce_group {group_id}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// Register an external admin `pubkey_hex` (Owner) for `group_id` in this node's roster.
    pub async fn set_admin(&mut self, group_id: &str, pubkey_hex: &str) -> Result<()> {
        self.request(&format!("set_admin {group_id} {pubkey_hex}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// Issue (sign) a grant for `group_id` at `role`, valid for `ttl_secs` (may be negative for an
    /// already-expired grant). Returns `(nonce, qr_payload)`.
    pub async fn issue_grant(
        &mut self,
        group_id: &str,
        role: &str,
        ttl_secs: i64,
    ) -> Result<(String, String)> {
        let resp = self
            .request(&format!("issue_grant {group_id} {role} {ttl_secs}"), REQ_TIMEOUT)
            .await?;
        let payload = resp
            .strip_prefix("grant ")
            .ok_or_else(|| anyhow!("{}: unexpected issue_grant response: {resp}", self.name))?;
        let (nonce, qr) = payload
            .split_once(' ')
            .ok_or_else(|| anyhow!("{}: malformed grant response: {resp}", self.name))?;
        Ok((nonce.to_string(), qr.to_string()))
    }

    /// Revoke a grant by `(group_id, nonce)` in this node's authorizer.
    pub async fn revoke_grant(&mut self, group_id: &str, nonce: &str) -> Result<()> {
        self.request(&format!("revoke_grant {group_id} {nonce}"), REQ_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// Join `group_id` presenting a signed grant QR payload (the invite). `bootstrap` is the
    /// holder's node id; `grant` is `None` to deliberately join with no grant (rejection tests).
    pub async fn join_group_with_grant(
        &mut self,
        group_id: &str,
        bootstrap: Option<&str>,
        grant: Option<&str>,
    ) -> Result<()> {
        // `-` is the explicit "no bootstrap" sentinel so the grant token never slides into the
        // bootstrap slot when bootstrap is absent (whitespace is collapsed child-side).
        let b = bootstrap.unwrap_or("-");
        let line = match grant {
            Some(g) => format!("join_group_grant {group_id} {b} {g}"),
            None => format!("join_group_grant {group_id} {b}"),
        };
        self.request(&line, REQ_TIMEOUT).await.map(|_| ())
    }

    /// Wait (in the child) for `SyncComplete` of `group_id`, bounded by `timeout`.
    pub async fn wait_sync(&mut self, group_id: &str, timeout: Duration) -> Result<bool> {
        // Give the control read a little slack beyond the child's own wait.
        let ms = timeout.as_millis() as u64;
        let resp = self
            .request(
                &format!("wait_sync {group_id} {ms}"),
                timeout + Duration::from_secs(5),
            )
            .await?;
        Ok(resp == "ok wait_sync")
    }

    /// Count rows of `kind` (groups|workspaces|boards|elements|cells|chats|notes|files)
    /// in THIS process's storage, scoped to `group_id`.
    pub async fn count(&mut self, kind: &str, group_id: &str) -> Result<usize> {
        let resp = self
            .request(&format!("count {kind} {group_id}"), REQ_TIMEOUT)
            .await?;
        let n = resp
            .strip_prefix(&format!("count {kind} "))
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| anyhow!("{}: unexpected count response: {resp}", self.name))?;
        Ok(n)
    }

    // ── Stress / chaos fabric (Round 7) ───────────────────────────────────────────────────

    /// Post `n` live whiteboard-element edits to `group_id`: each is inserted into this node's
    /// own storage AND broadcast over the group gossip. Edit ids are namespaced by node id, so
    /// concurrent posting from many peers never collides.
    pub async fn post_edits(&mut self, group_id: &str, n: usize) -> Result<()> {
        // Posting + broadcasting n events can take a moment for large n; scale the timeout.
        let timeout = REQ_TIMEOUT + Duration::from_millis(n as u64 * 5);
        self.request(&format!("post_edits {group_id} {n}"), timeout)
            .await
            .map(|_| ())
    }

    /// Insert `n` edits into THIS node's storage WITHOUT broadcasting them — a deterministic
    /// stand-in for live deltas whose gossip was dropped, so no other peer ever received them.
    /// Anti-entropy must detect + repair them on the next sweep.
    pub async fn post_local(&mut self, group_id: &str, n: usize) -> Result<()> {
        let timeout = REQ_TIMEOUT + Duration::from_millis(n as u64 * 5);
        self.request(&format!("post_local {group_id} {n}"), timeout)
            .await
            .map(|_| ())
    }

    /// Author `n` notes into THIS node's storage WITHOUT broadcasting them (ROUND8 §W2).
    /// Note ids are node-namespaced, so two peers' note sets are disjoint and only the
    /// anti-entropy digest+snapshot path can reconcile them — the digest-convergence proof.
    pub async fn post_notes(&mut self, group_id: &str, n: usize) -> Result<()> {
        let timeout = REQ_TIMEOUT + Duration::from_millis(n as u64 * 5);
        self.request(&format!("post_notes {group_id} {n}"), timeout)
            .await
            .map(|_| ())
    }

    /// Seed (hold + announce) a deterministic blob of `size` bytes into `group_id`'s swarm.
    /// Returns `(file_id, blake3_hex)`.
    pub async fn seed_blob(&mut self, group_id: &str, size: usize) -> Result<(String, String)> {
        let timeout = REQ_TIMEOUT + Duration::from_millis((size / 1_000_000) as u64 * 1000);
        let resp = self
            .request(&format!("seed_blob {group_id} {size}"), timeout)
            .await?;
        let payload = resp
            .strip_prefix("blob ")
            .ok_or_else(|| anyhow!("{}: unexpected seed_blob response: {resp}", self.name))?;
        let (file_id, hash) = payload
            .split_once(' ')
            .ok_or_else(|| anyhow!("{}: malformed blob response: {resp}", self.name))?;
        Ok((file_id.to_string(), hash.to_string()))
    }

    /// Fetch a blob `source_peer` holds, waiting (bounded) for completion. Returns the local
    /// path on success, or `None` on timeout.
    pub async fn fetch_blob(
        &mut self,
        group_id: &str,
        file_id: &str,
        hash: &str,
        source_peer: &str,
        size: u64,
        timeout: Duration,
    ) -> Result<Option<String>> {
        let ms = timeout.as_millis() as u64;
        let resp = self
            .request(
                &format!("fetch_blob {group_id} {file_id} {hash} {source_peer} {size} {ms}"),
                timeout + Duration::from_secs(5),
            )
            .await?;
        Ok(resp.strip_prefix("fetched ").map(|p| p.to_string()))
    }

    /// Independently re-verify a downloaded blob's blake3 against `expected_hash`.
    /// Returns true only on `verify ok`.
    pub async fn verify_blob(&mut self, file_id: &str, expected_hash: &str) -> Result<bool> {
        let resp = self
            .request(&format!("verify_blob {file_id} {expected_hash}"), REQ_TIMEOUT)
            .await?;
        Ok(resp == "verify ok")
    }

    /// This node's connection tier to `peer_id` (`direct`/`relay`/`mixed`/`none`/`unknown`).
    pub async fn tier(&mut self, peer_id: &str) -> Result<String> {
        let resp = self.request(&format!("tier {peer_id}"), REQ_TIMEOUT).await?;
        resp.strip_prefix("tier ")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{}: unexpected tier response: {resp}", self.name))
    }

    /// This node's process-level state + metrics (RSS, gossip counters, degree).
    pub async fn metrics(&mut self) -> Result<NodeMetrics> {
        let resp = self.request("metrics", REQ_TIMEOUT).await?;
        let json = resp
            .strip_prefix("metrics ")
            .ok_or_else(|| anyhow!("{}: unexpected metrics response: {resp}", self.name))?;
        serde_json::from_str(json)
            .with_context(|| format!("{}: parse metrics json: {json}", self.name))
    }

    /// Ask the child to exit cleanly.
    pub async fn quit(&mut self) -> Result<()> {
        self.stdin.write_all(b"quit\n").await.ok();
        self.stdin.flush().await.ok();
        Ok(())
    }

    /// Quit AND fully reap the child process, bounded. Critical when many tests run in one binary:
    /// `quit()`/`Drop` only *start* termination, so without an explicit reap the OS processes pile
    /// up and starve the next test's nodes. Consumes self so the handle can't be reused after exit.
    pub async fn shutdown(mut self) {
        let _ = self.stdin.write_all(b"quit\n").await;
        let _ = self.stdin.flush().await;
        // Wait for real exit; if it dawdles, kill_on_drop (set at spawn) finishes the job.
        if tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(2), self.child.wait()).await;
        }
    }
}

/// Wire two nodes so each can dial the other directly over loopback: exchange their
/// serialized `EndpointAddr`s into each other's StaticProvider. Bounded by `addr`/`add_peer`.
pub async fn wire_pair(a: &mut MpNode, b: &mut MpNode) -> Result<()> {
    let a_addr = a.addr().await?;
    let b_addr = b.addr().await?;
    a.add_peer(&b_addr).await?;
    b.add_peer(&a_addr).await?;
    Ok(())
}

/// Full-mesh wiring for the stress fabric: every node learns every other node's loopback
/// `EndpointAddr`, so any peer can dial any peer and the gossip overlay forms freely among
/// all N. (Relay is disabled; `StaticProvider` is the only discovery, so peers can dial
/// only addrs we hand them — full mesh is the in-process stand-in for "everyone reachable".)
/// Two bounded passes: collect all addrs, then inject each into the others.
pub async fn wire_mesh(nodes: &mut [MpNode]) -> Result<()> {
    let mut addrs = Vec::with_capacity(nodes.len());
    for n in nodes.iter_mut() {
        addrs.push(n.addr().await?);
    }
    for (i, n) in nodes.iter_mut().enumerate() {
        for (j, addr) in addrs.iter().enumerate() {
            if i != j {
                n.add_peer(addr).await?;
            }
        }
    }
    Ok(())
}
