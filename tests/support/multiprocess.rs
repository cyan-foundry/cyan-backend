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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Response sentinel — must match `src/bin/cyan_node.rs`.
const SENTINEL: &str = "@@CYAN@@";

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

    /// Count rows of `kind` (groups|workspaces|boards|elements|cells|chats|files) in
    /// THIS process's storage, scoped to `group_id`.
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

    /// Ask the child to exit cleanly.
    pub async fn quit(&mut self) -> Result<()> {
        self.stdin.write_all(b"quit\n").await.ok();
        self.stdin.flush().await.ok();
        Ok(())
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
