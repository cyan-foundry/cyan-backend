//! Loopback proof for the LOCAL side of the mesh-transport sovereignty model
//! (PRODUCTION_HARDENING_SET §4). The cyan-lens `MeshTransport` (branch
//! feat/mesh-transport) dispatches a tool call over the mesh; here we prove the
//! receiving end: `RemoteInvokeHandler` decodes a `ToolCallRequest`, runs the
//! LOCAL plugin via the existing cyan-mcp host (`PluginHost::dispatch_mcp_tool`),
//! and returns a `ToolCallResponse` — all with NO real subprocess and NO network
//! (a scripted echo transport stands in for the local plugin).
//!
//! Named tests:
//!   - remote_invoke_runs_local_plugin_and_returns_result
//!   - remote_invoke_is_tenant_scoped
//!   - remote_invoke_carrier_event_round_trips
//!   - remote_invoke_unresolvable_plugin_is_clean_error (no hang/panic)
//!   - remote_invoke_side_effecting_tool_is_gated (fail-closed on device)
//!
//! The protocol structs mirror cyan-lens `mesh_transport::{ToolCallRequest,
//! ToolCallResponse}` field-for-field; the serialized JSON is the cross-repo wire
//! contract. `protocol_wire_shape_is_stable` locks that shape.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use cyan_backend::mcp_host::PluginHost;
use cyan_backend::mesh_invoke::{
    RemoteInvokeHandler, RemoteToolConnector, ToolCallRequest, ToolCallResponse,
};
use cyan_backend::models::events::NetworkEvent;

use cyan_mcp::{
    BackoffPolicy, Clock, Emitter, EventSink, PluginTransport, RecordingEmitter, RecordingSink,
    ScriptedTransport, SystemClock,
};
use serde_json::Value;

/// Parse a JSON literal (the backend lint forbids the `json!` macro / `unwrap`).
fn jval(s: &str) -> Value {
    serde_json::from_str(s).expect("valid JSON literal in test")
}

fn test_host() -> Arc<PluginHost> {
    Arc::new(PluginHost::new(
        Arc::new(RecordingSink::new()) as Arc<dyn EventSink>,
        Arc::new(RecordingEmitter::new()) as Arc<dyn Emitter>,
        Arc::new(SystemClock::new()) as Arc<dyn Clock>,
        BackoffPolicy {
            base: Duration::from_millis(10),
            max: Duration::from_secs(1),
            max_restarts: 3,
        },
        "unused-default-tenant".to_string(),
    ))
}

/// A connector whose `connect` returns a scripted transport pre-loaded with the
/// MCP `initialize` (id=1) + `tools/call` (id=2) replies — an in-memory echo
/// "plugin". Records every (tenant, plugin) it opened, for scope assertions.
struct EchoConnector {
    /// The `tools/call` result the fake plugin returns (echoed back).
    result: Value,
    side_effects: Vec<String>,
    opened: Mutex<Vec<(String, String)>>,
}

impl EchoConnector {
    fn new(result: Value, side_effects: Vec<String>) -> Self {
        EchoConnector {
            result,
            side_effects,
            opened: Mutex::new(Vec::new()),
        }
    }

    fn opened(&self) -> Vec<(String, String)> {
        self.opened.lock().expect("lock").clone()
    }
}

impl RemoteToolConnector for EchoConnector {
    fn side_effects(&self, _plugin_id: &str, _tool: &str) -> Vec<String> {
        self.side_effects.clone()
    }

    fn connect(&self, tenant_id: &str, plugin_id: &str) -> Result<Box<dyn PluginTransport>> {
        self.opened
            .lock()
            .map_err(|e| anyhow!("opened lock poisoned: {e}"))?
            .push((tenant_id.to_string(), plugin_id.to_string()));
        let mut t = ScriptedTransport::new();
        // initialize reply (the Client's first request, id=1)
        t.push_reply(jval(r#"{ "jsonrpc": "2.0", "id": 1, "result": { "protocolVersion": "1.0" } }"#));
        // tools/call reply (id=2) — the echo plugin's result.
        t.push_reply(serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": self.result.clone(),
        }))
        .expect("valid reply"));
        Ok(Box::new(t))
    }
}

/// A connector that cannot reach its plugin (resolution/spawn failure). `connect`
/// errors cleanly; the handler must surface that as an error response, not panic.
struct UnreachableConnector;

impl RemoteToolConnector for UnreachableConnector {
    fn side_effects(&self, _plugin_id: &str, _tool: &str) -> Vec<String> {
        vec![]
    }
    fn connect(&self, _tenant_id: &str, _plugin_id: &str) -> Result<Box<dyn PluginTransport>> {
        Err(anyhow!("plugin 'contido' is not installed on this host"))
    }
}

fn request(tenant: &str) -> ToolCallRequest {
    ToolCallRequest {
        tenant_id: tenant.to_string(),
        plugin_id: "contido".to_string(),
        tool: "render".to_string(),
        args_json: jval(r#"{ "clip": "intro.mp4" }"#),
        corr_id: "corr-1".to_string(),
    }
}

#[test]
fn remote_invoke_runs_local_plugin_and_returns_result() {
    let connector = Arc::new(EchoConnector::new(
        jval(r#"{ "rendered": "intro.out", "cost_usd": 0.5 }"#),
        vec![],
    ));
    let handler = RemoteInvokeHandler::new(test_host(), connector.clone());

    let resp = handler.handle(&request("tenant-a"));

    assert_eq!(resp.corr_id, "corr-1");
    assert!(resp.error.is_none(), "no error on the happy path");
    let result = resp.result_json.expect("a result on success");
    assert_eq!(result["rendered"], "intro.out");

    // The local plugin was actually opened, tenant-scoped.
    assert_eq!(connector.opened(), vec![("tenant-a".to_string(), "contido".to_string())]);
}

#[test]
fn remote_invoke_is_tenant_scoped() {
    let connector = Arc::new(EchoConnector::new(jval(r#"{ "ok": true }"#), vec![]));
    let handler = RemoteInvokeHandler::new(test_host(), connector.clone());

    let _ = handler.handle(&request("tenant-a"));
    let _ = handler.handle(&request("tenant-b"));

    // Each call opened its plugin under the SAME tenant that dispatched it.
    let opened = connector.opened();
    assert_eq!(opened[0].0, "tenant-a");
    assert_eq!(opened[1].0, "tenant-b");
}

#[test]
fn remote_invoke_carrier_event_round_trips() {
    let connector = Arc::new(EchoConnector::new(jval(r#"{ "ok": 1 }"#), vec![]));
    let handler = RemoteInvokeHandler::new(test_host(), connector);

    // Drive through the NetworkEvent mesh carrier (RemoteToolCall → RemoteToolResult).
    let call_event = request("tenant-a").to_event();
    assert!(matches!(call_event, NetworkEvent::RemoteToolCall { .. }));

    let result_event = handler
        .handle_event(&call_event)
        .expect("a RemoteToolCall yields a result event");
    let resp = ToolCallResponse::from_event(&result_event).expect("RemoteToolResult");
    assert_eq!(resp.corr_id, "corr-1");
    assert_eq!(resp.result_json.expect("result")["ok"], 1);

    // A non-call event is ignored by the handler.
    let other = NetworkEvent::RemoteToolResult {
        corr_id: "x".to_string(),
        result: None,
        error: None,
    };
    assert!(handler.handle_event(&other).is_none());
}

#[test]
fn remote_invoke_unresolvable_plugin_is_clean_error() {
    let handler = RemoteInvokeHandler::new(test_host(), Arc::new(UnreachableConnector));

    let resp = handler.handle(&request("tenant-a"));

    assert_eq!(resp.corr_id, "corr-1");
    assert!(resp.result_json.is_none());
    let err = resp.error.expect("an unresolvable plugin is a clean error");
    assert!(err.contains("not installed"), "got: {err}");
}

#[test]
fn remote_invoke_side_effecting_tool_is_gated() {
    // A side-effecting tool reaching the device WITHOUT cloud approval fails
    // closed — the handler never opens the transport.
    let connector = Arc::new(EchoConnector::new(
        jval(r#"{ "sent": true }"#),
        vec!["external_send".to_string()],
    ));
    let handler = RemoteInvokeHandler::new(test_host(), connector.clone());

    let resp = handler.handle(&request("tenant-a"));

    assert!(resp.result_json.is_none(), "gated tool must not run");
    let err = resp.error.expect("a gate is reported as an error");
    assert!(err.contains("gated"), "got: {err}");
    // Fail-closed: the plugin transport was never opened.
    assert!(connector.opened().is_empty(), "a gated tool opens no transport");
}

/// The wire protocol is stable JSON — the exact shape the cyan-lens MeshTransport
/// produces. Locks field names so a rename can't silently break the contract.
#[test]
fn protocol_wire_shape_is_stable() {
    let req = request("t");
    let v = serde_json::to_value(&req).expect("serialize req");
    assert_eq!(v["tenant_id"], "t");
    assert_eq!(v["plugin_id"], "contido");
    assert_eq!(v["tool"], "render");
    assert_eq!(v["args_json"], jval(r#"{ "clip": "intro.mp4" }"#));
    assert_eq!(v["corr_id"], "corr-1");

    let ok = ToolCallResponse::ok("c1", jval(r#"{ "r": 2 }"#));
    let okv = serde_json::to_value(&ok).expect("serialize ok");
    assert_eq!(okv, jval(r#"{ "corr_id": "c1", "result_json": { "r": 2 } }"#));

    let err = ToolCallResponse::err("c1", "boom");
    let errv = serde_json::to_value(&err).expect("serialize err");
    assert_eq!(errv, jval(r#"{ "corr_id": "c1", "error": "boom" }"#));
}
