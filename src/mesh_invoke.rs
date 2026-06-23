// src/mesh_invoke.rs
//
// The LOCAL side of the "Contido local, Lens on AWS" sovereignty model
// (PRODUCTION_HARDENING_SET §4). The cloud Lens `MeshTransport` dispatches a
// plugin tool call OVER THE MESH to this host; `RemoteInvokeHandler` receives it,
// runs the LOCAL plugin against LOCAL data via the EXISTING cyan-mcp host
// machinery ([`crate::mcp_host::PluginHost::dispatch_mcp_tool`]), and returns the
// result. Plugin + data never leave the device; Lens only orchestrates.
//
// This module is ADDITIVE: it adds the wire protocol + a handler that REUSES the
// plugin host. It does not change the FFI contract or the StdioTransport path.
//
// The wire protocol ([`ToolCallRequest`] / [`ToolCallResponse`]) mirrors the
// cyan-lens `mesh_transport` structs field-for-field — the serialized JSON is the
// cross-repo contract. They also convert to/from the additive
// [`NetworkEvent::RemoteToolCall`] / [`NetworkEvent::RemoteToolResult`] carriers
// that ride the existing gossip mesh.
//
// SCOPED OUT (deferred, see STATUS_MESH_TRANSPORT.md): wiring the network actor to
// route an inbound `RemoteToolCall` off gossip into this handler and dial the
// `RemoteToolResult` back to the originating Lens peer. The handler + protocol are
// proven over loopback here; the gossip/QUIC plumbing is the next bounded step.

use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use cyan_mcp::PluginTransport;

use crate::mcp_host::{McpDispatch, McpTool, PluginHost, RunCostLedger, RunScope};
use crate::models::events::NetworkEvent;

// ============================================================================
// The wire protocol (mirrors cyan-lens mesh_transport::{ToolCallRequest,...})
// ============================================================================

/// A request to run ONE plugin tool on this host over the mesh. `tenant_id` rides
/// every hop so the call runs tenant-scoped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRequest {
    /// Tenant the call is scoped to.
    pub tenant_id: String,
    /// Locally-installed plugin that exposes the tool.
    pub plugin_id: String,
    /// Tool name to invoke.
    pub tool: String,
    /// JSON arguments for the call.
    pub args_json: Value,
    /// Correlation id matching the response back to this request.
    pub corr_id: String,
}

/// This host's reply to a [`ToolCallRequest`]. Exactly one of `result_json` (the
/// tool ran) or `error` (it could not) is set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallResponse {
    /// Correlation id echoing the originating [`ToolCallRequest::corr_id`].
    pub corr_id: String,
    /// The tool's JSON result, if it ran successfully.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_json: Option<Value>,
    /// A human-readable error, if the host could not run the tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ToolCallRequest {
    /// Decode a request from its mesh carrier, if the event is a `RemoteToolCall`.
    pub fn from_event(event: &NetworkEvent) -> Option<Self> {
        match event {
            NetworkEvent::RemoteToolCall {
                corr_id,
                tenant_id,
                plugin_id,
                tool,
                args,
            } => Some(ToolCallRequest {
                tenant_id: tenant_id.clone(),
                plugin_id: plugin_id.clone(),
                tool: tool.clone(),
                args_json: args.clone(),
                corr_id: corr_id.clone(),
            }),
            _ => None,
        }
    }

    /// Encode this request as its mesh carrier event.
    pub fn to_event(&self) -> NetworkEvent {
        NetworkEvent::RemoteToolCall {
            corr_id: self.corr_id.clone(),
            tenant_id: self.tenant_id.clone(),
            plugin_id: self.plugin_id.clone(),
            tool: self.tool.clone(),
            args: self.args_json.clone(),
        }
    }
}

impl ToolCallResponse {
    /// A success response carrying the tool result.
    pub fn ok(corr_id: impl Into<String>, result: Value) -> Self {
        ToolCallResponse {
            corr_id: corr_id.into(),
            result_json: Some(result),
            error: None,
        }
    }

    /// A failure response carrying an error message.
    pub fn err(corr_id: impl Into<String>, error: impl Into<String>) -> Self {
        ToolCallResponse {
            corr_id: corr_id.into(),
            result_json: None,
            error: Some(error.into()),
        }
    }

    /// Decode a response from its mesh carrier, if the event is a
    /// `RemoteToolResult`.
    pub fn from_event(event: &NetworkEvent) -> Option<Self> {
        match event {
            NetworkEvent::RemoteToolResult {
                corr_id,
                result,
                error,
            } => Some(ToolCallResponse {
                corr_id: corr_id.clone(),
                result_json: result.clone(),
                error: error.clone(),
            }),
            _ => None,
        }
    }

    /// Encode this response as its mesh carrier event.
    pub fn to_event(&self) -> NetworkEvent {
        NetworkEvent::RemoteToolResult {
            corr_id: self.corr_id.clone(),
            result: self.result_json.clone(),
            error: self.error.clone(),
        }
    }
}

// ============================================================================
// The connector seam: how the handler opens a transport to a local plugin
// ============================================================================

/// How the remote-invoke handler reaches a locally-installed plugin: it reads the
/// tool's declared `side_effects` (for the device-side gate) and opens a live
/// transport for one call. The prod impl resolves the installed bundle and spawns
/// a `cyan_mcp::StdioTransport`; tests pass a scripted echo transport.
pub trait RemoteToolConnector: Send + Sync {
    /// Side effects the tool declares (read from its manifest). Read-only = `[]`.
    fn side_effects(&self, plugin_id: &str, tool: &str) -> Vec<String>;

    /// Open a live transport to the (local) plugin for one tool call.
    fn connect(&self, tenant_id: &str, plugin_id: &str) -> Result<Box<dyn PluginTransport>>;
}

// ============================================================================
// The remote-invoke handler
// ============================================================================

/// Receives a [`ToolCallRequest`] from the mesh and runs the local plugin tool
/// via the existing [`PluginHost`], returning a [`ToolCallResponse`]. Tenant scope
/// rides from the request through `RunScope` into the host and onto the external
/// cost rail.
///
/// Approval note: the device fails CLOSED. A read-only tool (empty
/// `side_effects`) runs; a side-effecting tool (`external_send` / `delete`) is
/// `Gated` and never opens a transport here, because the current protocol carries
/// no approval token. The cloud Lens `Enforcer` is the approval authority, but
/// until the protocol carries that decision a stray/un-approved side-effecting
/// call can never auto-run on the device. Carrying an explicit approval token so
/// approved side-effecting remote calls can run is a deferred refinement — see
/// STATUS_MESH_TRANSPORT.md.
pub struct RemoteInvokeHandler {
    host: Arc<PluginHost>,
    connector: Arc<dyn RemoteToolConnector>,
}

impl RemoteInvokeHandler {
    /// Build a handler over the device plugin host + a connector seam.
    pub fn new(host: Arc<PluginHost>, connector: Arc<dyn RemoteToolConnector>) -> Self {
        RemoteInvokeHandler { host, connector }
    }

    /// Run one mesh tool call locally and return the response (never panics; any
    /// failure becomes a `ToolCallResponse::err`, so an unreachable plugin or a
    /// bad tool surfaces as a clean error to the cloud Lens, not a hang/crash).
    pub fn handle(&self, req: &ToolCallRequest) -> ToolCallResponse {
        let scope = RunScope {
            tenant_id: req.tenant_id.clone(),
            run_id: req.corr_id.clone(),
        };
        let step = McpTool {
            plugin_id: req.plugin_id.clone(),
            tool: req.tool.clone(),
            args: req.args_json.clone(),
        };
        let side_effects = self.connector.side_effects(&req.plugin_id, &req.tool);
        let ledger = RunCostLedger::new();
        let connector = self.connector.clone();
        let tenant = req.tenant_id.clone();
        let plugin = req.plugin_id.clone();

        // Fail closed: a side-effecting tool is gated on the device (the protocol
        // carries no approval token yet — see the type doc). A read-only tool runs.
        let dispatched = self.host.dispatch_mcp_tool(
            &scope,
            &step,
            &side_effects,
            /* approved = */ false,
            &ledger,
            || connector.connect(&tenant, &plugin),
        );

        match dispatched {
            Ok(McpDispatch::Ran(result)) => ToolCallResponse::ok(&req.corr_id, result.result),
            Ok(McpDispatch::Gated { side_effects }) => ToolCallResponse::err(
                &req.corr_id,
                format!("tool gated: side effects {side_effects:?} require approval"),
            ),
            Err(e) => ToolCallResponse::err(&req.corr_id, e.to_string()),
        }
    }

    /// Convenience: handle a mesh carrier event end-to-end, returning the result
    /// carrier event. `None` if the event is not a `RemoteToolCall`.
    pub fn handle_event(&self, event: &NetworkEvent) -> Option<NetworkEvent> {
        let req = ToolCallRequest::from_event(event)?;
        Some(self.handle(&req).to_event())
    }
}
