//! Consumer-integration tests for the LOCAL `McpTool` dispatch path (Wave 2
//! keystone, device side). cyan-backend is the LOCAL DEVICE HOST: a pipeline step
//! with `executor = local` that names a plugin tool runs the plugin ON-DEVICE via
//! the supervised cyan-mcp host — no cloud round-trip. This mirrors the cyan-lens
//! cloud-host `McpTool` contract (same `{ plugin_id, tool, args }` shape, same
//! cost-isolation: the plugin's cost is an EXTERNAL line, never our LLM tokens).
//!
//! These drive cyan-mcp's `ScriptedTransport` — NO real subprocess — so they are
//! deterministic with no unbounded wait. The real device spawn (`StdioTransport`
//! from a `.cyanplugin` bundle) is the prod lifecycle wired in
//! `pipeline_executor.rs`; the dispatch LOGIC (initialize → call_tool → thread the
//! result, the cost rail, the approval gate) is what these prove.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cyan_backend::mcp_host::{
    requires_approval, McpDispatch, McpTool, PluginHost, RunCostLedger, RunScope,
};
use cyan_backend::storage;

use cyan_mcp::{
    BackoffPolicy, Clock, Emitter, EventSink, PluginTransport, RecordingEmitter, RecordingSink,
    ScriptedTransport, SystemClock,
};
use serde_json::Value;

const TENANT: &str = "tenant-test";

/// Parse a JSON literal into a `Value`. We avoid the `json!` macro because it
/// expands to `unwrap()`, which the workspace lint rejects even in tests; `expect`
/// on a static literal is allowed and equally safe.
fn jval(s: &str) -> Value {
    serde_json::from_str(s).expect("valid JSON literal in test")
}

fn test_host() -> PluginHost {
    PluginHost::new(
        Arc::new(RecordingSink::new()) as Arc<dyn EventSink>,
        Arc::new(RecordingEmitter::new()) as Arc<dyn Emitter>,
        Arc::new(SystemClock::new()) as Arc<dyn Clock>,
        BackoffPolicy {
            base: Duration::from_millis(100),
            max: Duration::from_secs(5),
            max_restarts: 3,
        },
        TENANT.to_string(),
    )
}

/// A scripted transport that answers `initialize` (id=1) then one `tools/call`
/// (id=2) with `result`. The Client correlates replies by id, so these are the
/// two requests a single tool dispatch sends.
fn scripted_call(result_json: &str) -> Box<dyn PluginTransport> {
    let mut t = ScriptedTransport::new();
    t.push_reply(jval(r#"{ "jsonrpc": "2.0", "id": 1, "result": {} }"#));
    t.push_reply(jval(&format!(
        r#"{{ "jsonrpc": "2.0", "id": 2, "result": {result_json} }}"#
    )));
    Box::new(t)
}

/// A pipeline `McpTool` step dispatches via the local host; the scripted plugin
/// result threads back into the step output (so the next reasoning step can use
/// it). No cloud round-trip — the plugin ran on-device.
#[test]
fn pipeline_step_invokes_local_plugin_tool() {
    let host = test_host();
    let scope = RunScope {
        tenant_id: TENANT.to_string(),
        run_id: "run-invoke".to_string(),
    };
    let step = McpTool {
        plugin_id: "transcoder".to_string(),
        tool: "transcode".to_string(),
        args: jval(r#"{ "src": "a.mov", "profile": "h264_hd" }"#),
    };
    let ledger = RunCostLedger::new();

    let dispatch = host
        .dispatch_mcp_tool(&scope, &step, &[], false, &ledger, || {
            Ok(scripted_call(r#"{ "status": "ok", "output_uri": "out.mp4" }"#))
        })
        .expect("local dispatch succeeds");

    match dispatch {
        McpDispatch::Ran(result) => {
            // The plugin's JSON result threads into the step output verbatim.
            assert_eq!(result.result["status"], "ok");
            assert_eq!(result.result["output_uri"], "out.mp4");
        }
        McpDispatch::Gated { .. } => panic!("a pure tool must run, not gate"),
    }
}

/// A local/partner plugin tool call records its cost on the EXTERNAL rail and adds
/// ZERO to our LLM token tally. Mirrors the lens cost-isolation property: their
/// compute bills externally; our reasoning tokens are untouched.
#[test]
fn local_mcp_tool_cost_is_external_not_tokens() {
    let host = test_host();
    let scope = RunScope {
        tenant_id: TENANT.to_string(),
        run_id: "run-cost".to_string(),
    };
    let step = McpTool {
        plugin_id: "planetcast".to_string(),
        tool: "qc".to_string(),
        args: jval("{}"),
    };
    let ledger = RunCostLedger::new();
    // Baseline: our reasoning has already spent some vLLM tokens this run.
    ledger.record_llm(120, 80);

    host.dispatch_mcp_tool(&scope, &step, &[], false, &ledger, || {
        // The partner reports its OWN billing as `cost_usd` (external pass-through).
        Ok(scripted_call(r#"{ "report": "pass", "cost_usd": 0.42 }"#))
    })
    .expect("local dispatch succeeds");

    // Our LLM tally is UNCHANGED — the partner's compute added zero of our tokens.
    assert_eq!(
        ledger.llm_tokens(),
        (120, 80),
        "an external plugin tool adds zero LLM tokens"
    );

    // The cost lands on the external rail as a flat `tool_called` obs line.
    let external = ledger.external_tool_calls();
    assert_eq!(external.len(), 1, "exactly one external tool obs");
    let obs = &external[0];
    assert_eq!(obs.source, "external");
    assert_eq!(obs.tenant_id, TENANT);
    assert_eq!(obs.run_id, "run-cost");
    assert_eq!(obs.plugin_id, "planetcast");
    assert_eq!(obs.tool, "qc");
    assert_eq!(obs.cost_usd, Some(0.42));
}

/// An installed `.cyanplugin` bundle in the group's "Plugins" workspace is picked
/// up (file-swarm view via `storage`) and its tool is found via cyan-mcp's
/// `Registry` — making the tool available to a local pipeline step.
#[test]
fn plugin_tool_from_plugins_workspace_is_discoverable() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Init the (process-global) DB against a temp file — this test binary owns it.
    // `run_migrations` (inside `init_db`) does not create the base group/workspace/
    // object tables (the FFI/app owns those), so create them first, exactly as the
    // substrate harness does.
    let db_path = tmp.path().join("disco.db");
    init_base_schema(&db_path).expect("base schema");
    storage::init_db(db_path.to_str().expect("utf8 db path")).expect("init_db");

    // The file-swarm fetches a `.cyanplugin` into a bundle dir under a plugins root
    // (one subdir per plugin, named by plugin_id). It carries a cyan-forge manifest.
    let plugins_root = tmp.path().join("plugins");
    let bundle_dir = plugins_root.join("media-plugin");
    std::fs::create_dir_all(&bundle_dir).expect("mkdir bundle");
    std::fs::write(bundle_dir.join("manifest.json"), MANIFEST_JSON).expect("write manifest");

    // Install == a bundle file appears in the group's "Plugins" workspace, fetched
    // (local_path set) by the swarm. Reuse the real schema + helpers.
    let group_id = "group-disco";
    let ws_id = "ws-plugins";
    let file_id = "file-media-bundle";
    storage::group_insert_simple(group_id, "Disco", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(ws_id, group_id, "Plugins").expect("workspace");
    storage::file_insert_simple(
        file_id,
        Some(group_id),
        Some(ws_id),
        None,
        "media.cyanplugin",
        "blake3hash",
        128,
        None,
        0,
    )
    .expect("file");
    storage::file_set_local_path(file_id, bundle_dir.to_str().expect("utf8 bundle path"))
        .expect("set local_path");

    let host = test_host();

    // Workspace pickup: the installed bundle is visible to the local host.
    let bundles = host.discover_bundles(group_id).expect("discover bundles");
    assert_eq!(bundles.len(), 1, "one installed bundle");
    assert_eq!(bundles[0].name, "media.cyanplugin");
    assert_eq!(bundles[0].local_path, bundle_dir.to_str().expect("utf8"));

    // Registry → tools: the bundle's tool is resolvable for a local pipeline step,
    // and its declared side_effects come along (so the gate can read them).
    let resolved = host
        .resolve_installed_tool(Path::new(bundles[0].local_path.as_str()).parent().expect("root"), "transcode")
        .expect("registry index ok")
        .expect("tool found via registry");
    assert_eq!(resolved.0, "media-plugin");
    assert_eq!(resolved.1.name, "transcode");
    assert_eq!(resolved.1.side_effects, vec!["external_send".to_string()]);
}

/// A side-effecting tool (`external_send`/`delete`) is NEVER auto-executed: it
/// requires the human-approval gate. Until approved, dispatch returns `Gated`
/// WITHOUT ever opening a transport (in prod: without spawning the process).
#[test]
fn local_mcp_tool_external_send_requires_approval_gate() {
    let host = test_host();
    let scope = RunScope {
        tenant_id: TENANT.to_string(),
        run_id: "run-gate".to_string(),
    };
    let step = McpTool {
        plugin_id: "deliver".to_string(),
        tool: "push_to_platform".to_string(),
        args: jval(r#"{ "platform": "JioStar" }"#),
    };
    let side_effects = vec!["external_send".to_string()];
    assert!(requires_approval(&side_effects));

    let ledger = RunCostLedger::new();

    // Unapproved: must gate and NOT open a transport (the closure would panic).
    let gated = host
        .dispatch_mcp_tool(&scope, &step, &side_effects, false, &ledger, || {
            panic!("a gated tool must not open a transport / spawn the plugin")
        })
        .expect("gate decision is not an error");
    match gated {
        McpDispatch::Gated { side_effects } => {
            assert_eq!(side_effects, vec!["external_send".to_string()]);
        }
        McpDispatch::Ran(_) => panic!("an unapproved side-effecting tool must gate"),
    }
    // Nothing ran, so nothing billed.
    assert!(ledger.external_tool_calls().is_empty());

    // Approved (the human-approval path flipped the gate): now it runs.
    let ran = host
        .dispatch_mcp_tool(&scope, &step, &side_effects, true, &ledger, || {
            Ok(scripted_call(r#"{ "delivered": true }"#))
        })
        .expect("approved dispatch succeeds");
    match ran {
        McpDispatch::Ran(result) => assert_eq!(result.result["delivered"], true),
        McpDispatch::Gated { .. } => panic!("an approved tool must run"),
    }
    assert_eq!(ledger.external_tool_calls().len(), 1, "approved call is billed");
}

/// Create the base group/workspace/object tables the helpers + `plugin_bundles_in_group`
/// rely on (mirrors the substrate harness; run once before `storage::init_db`).
fn init_base_schema(db_path: &Path) -> Result<(), rusqlite::Error> {
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
        "#,
    )?;
    Ok(())
}

/// A minimal cyan-forge manifest exposing one side-effecting tool.
const MANIFEST_JSON: &str = r#"{
  "name": "media-plugin",
  "version": "1.0.0",
  "runtime": "python-uv",
  "tools": [
    {
      "name": "transcode",
      "when_to_use": "convert an asset to a delivery profile",
      "io_types": { "input": ["video"], "output": ["video"] },
      "stage": "transcode",
      "side_effects": ["external_send"],
      "locality": "device",
      "input_schema": {},
      "output_schema": {}
    }
  ]
}"#;
