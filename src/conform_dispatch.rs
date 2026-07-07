//! conform_dispatch — the REAL `ConformDispatch` into cyan-media.
//!
//! Spawns cyan-media's stdio JSON-RPC host (`cyan-media-mcp`, the cyan-mcp
//! dialect: newline-delimited `initialize` → `tools/call`) and runs the
//! `conform` tool. This is the production seam `review_loop::conform_proxy`
//! renders through when no plugin host is in play (the phase-3 reverse-loop
//! harness); `FakeConform` in tests fakes exactly this trait.
//!
//! The `input` the loop passes is the OPAQUE Frame.io file ref of the proxy
//! under review; cyan-media only accepts media-root-relative paths (path
//! confinement). The caller therefore provides `input_rel` — the resolved,
//! media-root-relative source the ref maps to — and this dispatch rewrites
//! `args.input` before the call, mirroring `HostConformDispatch`'s URI
//! resolution.

use crate::review_loop::ConformDispatch;
use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub struct StdioConformDispatch {
    /// The host command line, argv[0] first — e.g.
    /// `["uv", "run", "--project", "<cyan-media dir>", "cyan-media-mcp"]`.
    pub command: Vec<String>,
    /// Exported to the child as `CYAN_MEDIA_ROOT` (path confinement root and
    /// the parent of the `.cyan-derived/conform/` output tree).
    pub media_root: PathBuf,
    /// Media-root-relative input the opaque proxy ref resolves to.
    pub input_rel: String,
    /// Bound on the whole call (spawn + render). Never waits unbounded.
    pub timeout: Duration,
}

impl ConformDispatch for StdioConformDispatch {
    fn conform(&self, mut args: Value) -> Result<Value> {
        if let Some(obj) = args.as_object_mut() {
            obj.insert("input".to_string(), json!(self.input_rel));
        }
        let (argv0, rest) = self
            .command
            .split_first()
            .ok_or_else(|| anyhow!("conform dispatch: empty host command"))?;
        let mut child = Command::new(argv0)
            .args(rest)
            .env("CYAN_MEDIA_ROOT", &self.media_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit()) // obs lines; never carries secrets
            .spawn()
            .with_context(|| format!("spawn cyan-media host {:?}", self.command))?;

        let outcome = self.drive(&mut child, args);
        let _ = child.kill();
        let _ = child.wait();
        outcome
    }
}

impl StdioConformDispatch {
    fn drive(&self, child: &mut Child, args: Value) -> Result<Value> {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("conform host: no stdin pipe"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("conform host: no stdout pipe"))?;

        let init = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} });
        let call = json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": { "name": "conform", "arguments": args }
        });
        writeln!(stdin, "{init}").context("write initialize")?;
        writeln!(stdin, "{call}").context("write tools/call conform")?;
        drop(stdin); // EOF: the host processes both lines and exits

        // A reader thread feeds a channel so every wait is bounded.
        let (tx, rx) = mpsc::channel::<std::io::Result<String>>();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });

        let deadline = Instant::now() + self.timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| anyhow!("conform timed out after {:?}", self.timeout))?;
            let line = match rx.recv_timeout(remaining) {
                Ok(Ok(line)) => line,
                Ok(Err(e)) => bail!("conform host: stdout read failed: {e}"),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    bail!("conform timed out after {:?}", self.timeout)
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("conform host closed stdout before answering the tools/call")
                }
            };
            // stdout is JSON-RPC-only by contract; skip anything unparseable.
            let Ok(v) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if v.get("id").and_then(Value::as_i64) != Some(2) {
                continue;
            }
            if let Some(err) = v.get("error") {
                bail!("conform rpc error: {err}");
            }
            let result = v
                .get("result")
                .cloned()
                .ok_or_else(|| anyhow!("conform reply has neither result nor error: {v}"))?;
            if let Some(tool_err) = result.get("error") {
                bail!("conform tool failed: {tool_err}");
            }
            return Ok(result);
        }
    }
}
