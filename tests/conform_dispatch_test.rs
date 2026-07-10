//! StdioConformDispatch — the REAL ConformDispatch seam, proven against a
//! scripted stand-in for cyan-media's stdio JSON-RPC host (`cyan-media-mcp`,
//! newline-delimited JSON-RPC: initialize → tools/call conform). The fake here
//! only fakes the CHILD PROCESS TRANSPORT so the protocol framing, input
//! rewrite, error mapping, and process hygiene are testable hermetically; the
//! production target is the real cyan-media host (exercised by the phase-3
//! harness / gate).

use cyan_backend::conform_dispatch::StdioConformDispatch;
use cyan_backend::review_loop::ConformDispatch;
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

/// Write an executable fake host: copies every stdin line to $CAPTURE, then
/// emits the given stdout lines (newline-delimited JSON-RPC responses).
fn fake_host(dir: &Path, capture: &Path, responses: &[&str]) -> String {
    let script = dir.join("fake-cyan-media-mcp.sh");
    let mut body = String::from("#!/bin/sh\n");
    body.push_str(&format!("tee '{}' >/dev/null\n", capture.display()));
    for r in responses {
        body.push_str(&format!("printf '%s\\n' '{}'\n", r));
    }
    fs::write(&script, body).expect("write fake host");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");
    script.display().to_string()
}

fn dispatch_for(script: String, media_root: &Path) -> StdioConformDispatch {
    StdioConformDispatch {
        command: vec![script],
        media_root: media_root.to_path_buf(),
        input_rel: "master/sig_source.mp4".to_string(),
        timeout: Duration::from_secs(10),
    }
}

#[test]
fn conform_speaks_the_dialect_and_rewrites_input() {
    let tmp = tempfile::tempdir().expect("tmp");
    let capture = tmp.path().join("capture.jsonl");
    let script = fake_host(
        tmp.path(),
        &capture,
        &[
            r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"1.0","capabilities":{}}}"#,
            r#"{"jsonrpc":"2.0","id":2,"result":{"output_path":"conform/abc123.mp4","applied":[{"op":"trim"}],"needs_manual":[]}}"#,
        ],
    );
    let d = dispatch_for(script, tmp.path());

    let out = d
        .conform(json!({
            "input": "frameio-file-id-opaque",
            "fps": 24.0,
            "ops": [{ "op": "trim", "tc_in": 0, "tc_out": 72, "params": {"edge":"tail","frames":12} }]
        }))
        .expect("conform result");
    assert_eq!(out["output_path"], json!("conform/abc123.mp4"));
    assert_eq!(out["applied"][0]["op"], json!("trim"));

    // The wire: line 1 = initialize, line 2 = tools/call conform with the
    // OPAQUE Frame.io ref rewritten to the media-root-relative input path.
    let wire = fs::read_to_string(&capture).expect("capture");
    let lines: Vec<Value> = wire
        .lines()
        .map(|l| serde_json::from_str(l).expect("wire line is JSON"))
        .collect();
    assert_eq!(lines.len(), 2, "exactly initialize + tools/call");
    assert_eq!(lines[0]["method"], json!("initialize"));
    assert_eq!(lines[1]["method"], json!("tools/call"));
    assert_eq!(lines[1]["params"]["name"], json!("conform"));
    let args = &lines[1]["params"]["arguments"];
    assert_eq!(
        args["input"],
        json!("master/sig_source.mp4"),
        "opaque proxy ref must be rewritten to the media-root-relative input"
    );
    assert_eq!(args["fps"], json!(24.0));
    assert_eq!(args["ops"][0]["op"], json!("trim"));
}

#[test]
fn tool_level_error_surfaces_as_err() {
    let tmp = tempfile::tempdir().expect("tmp");
    let capture = tmp.path().join("capture.jsonl");
    let script = fake_host(
        tmp.path(),
        &capture,
        &[
            r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"1.0"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"result":{"error":{"error_class":"ToolFailed","message":"ffmpeg exploded"}}}"#,
        ],
    );
    let err = dispatch_for(script, tmp.path())
        .conform(json!({"input":"x","fps":24.0,"ops":[]}))
        .expect_err("tool error must not be swallowed");
    assert!(err.to_string().contains("ffmpeg exploded"), "got: {err}");
}

#[test]
fn rpc_level_error_surfaces_as_err() {
    let tmp = tempfile::tempdir().expect("tmp");
    let capture = tmp.path().join("capture.jsonl");
    let script = fake_host(
        tmp.path(),
        &capture,
        &[
            r#"{"jsonrpc":"2.0","id":1,"result":{}}"#,
            r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"unknown tool"}}"#,
        ],
    );
    let err = dispatch_for(script, tmp.path())
        .conform(json!({"input":"x","fps":24.0,"ops":[]}))
        .expect_err("rpc error must not be swallowed");
    assert!(err.to_string().contains("unknown tool"), "got: {err}");
}

#[test]
fn silent_host_times_out_bounded() {
    let tmp = tempfile::tempdir().expect("tmp");
    let capture = tmp.path().join("capture.jsonl");
    // Emits nothing for id 2 — the dispatch must give up within its timeout,
    // not hang the loop.
    let script = fake_host(
        tmp.path(),
        &capture,
        &[r#"{"jsonrpc":"2.0","id":1,"result":{}}"#],
    );
    let mut d = dispatch_for(script, tmp.path());
    d.timeout = Duration::from_millis(500);
    let err = d
        .conform(json!({"input":"x","fps":24.0,"ops":[]}))
        .expect_err("silent host must time out");
    let msg = err.to_string();
    assert!(
        msg.contains("timed out") || msg.contains("closed stdout"),
        "got: {msg}"
    );
}
