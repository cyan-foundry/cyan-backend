// src/mcp_host.rs
//
// cyan-backend is the LOCAL DEVICE HOST for MCP plugins. This module composes the
// shared `cyan-mcp` crate (Supervisor + Client + transport + obs) into the device
// engine, and supplies the one seam that is backend-specific: where a plugin's
// relayed output goes.
//
// On the device there is NO local Iggy. A plugin's relayed events reach Lens by
// being broadcast INTO the group mesh/gossip; the super-peer (a Lens replica)
// picks them off gossip and feeds its Iggy/enricher. `MeshRelaySink` is that
// bridge. It lives behind `cyan-mcp`'s `EventSink` trait so tests swap in
// `RecordingSink` and never touch the network.
//
// The app never sees any of this: plugins are just files in a "Plugins" workspace
// (see `storage::plugin_bundles_in_group`), so there is NO new `cyan_*` FFI.

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

use cyan_mcp::{
    BackoffPolicy, Clock, Emitter, EventSink, PluginEvent, PluginId, PluginTransport, Supervisor,
    TenantId,
};

use crate::models::commands::NetworkCommand;
use crate::models::events::NetworkEvent;

/// The default name of the per-group workspace that holds installed plugin
/// bundles. A `.cyanplugin` file landing here is picked up by the host.
pub const PLUGINS_WORKSPACE_NAME: &str = "Plugins";

/// File-name suffix that marks a file in the Plugins workspace as an installable
/// MCP plugin bundle.
pub const PLUGIN_BUNDLE_SUFFIX: &str = ".cyanplugin";

/// `EventSink` that relays a plugin's output INTO the group mesh/gossip.
///
/// `deliver` is infallible by contract (the trait returns `()`), so a closed
/// network channel is dropped rather than propagated — the supervision loop must
/// not die because the mesh is momentarily unavailable. This is the prod sink;
/// tests use `cyan_mcp::RecordingSink`.
pub struct MeshRelaySink {
    /// Group whose mesh the relayed events are broadcast into.
    group_id: String,
    /// The engine's network command channel (same one the actors drive).
    network_tx: UnboundedSender<NetworkCommand>,
}

impl MeshRelaySink {
    /// Wire a relay sink for one group onto the engine's network command channel.
    pub fn new(group_id: String, network_tx: UnboundedSender<NetworkCommand>) -> Self {
        MeshRelaySink {
            group_id,
            network_tx,
        }
    }
}

impl EventSink for MeshRelaySink {
    fn deliver(&self, event: PluginEvent) {
        // Carry the relayed event over gossip as an opaque payload. Lens (the
        // super-peer) parses `payload`; mesh peers treat it as pass-through.
        let payload = event.params.to_string();
        let relay = NetworkEvent::PluginRelay {
            plugin_id: event.plugin_id,
            method: event.method,
            payload,
        };
        // Best-effort: if the network channel is closed (engine shutting down),
        // drop the event. The trait gives us no way to report it, and a dead
        // channel must never crash the plugin supervision loop.
        if let Err(e) = self.network_tx.send(NetworkCommand::Broadcast {
            group_id: self.group_id.clone(),
            event: relay,
        }) {
            tracing::warn!("MeshRelaySink: dropped plugin relay (network channel closed): {e}");
        }
    }
}

/// The local device plugin host. Holds the shared seams (sink, obs emitter,
/// clock, backoff policy, tenant) and builds a supervised plugin from a
/// transport. One host per device; one `Supervisor` per plugin process.
///
/// The host is transport-agnostic: prod passes a `cyan_mcp::StdioTransport`
/// (real child process), tests pass a `cyan_mcp::ScriptedTransport`. The sink is
/// likewise swappable — `MeshRelaySink` in prod, `RecordingSink` in tests.
pub struct PluginHost {
    sink: Arc<dyn EventSink>,
    emitter: Arc<dyn Emitter>,
    clock: Arc<dyn Clock>,
    backoff: BackoffPolicy,
    tenant_id: TenantId,
}

impl PluginHost {
    /// Build a host from its seams. `sink` is where every supervised plugin's
    /// relayed events flow (prod: [`MeshRelaySink`]).
    pub fn new(
        sink: Arc<dyn EventSink>,
        emitter: Arc<dyn Emitter>,
        clock: Arc<dyn Clock>,
        backoff: BackoffPolicy,
        tenant_id: TenantId,
    ) -> Self {
        PluginHost {
            sink,
            emitter,
            clock,
            backoff,
            tenant_id,
        }
    }

    /// Build a `Supervisor` for one plugin over the given transport, wired to this
    /// host's sink/emitter/clock. The caller drives it (`start` then a
    /// `supervise_once` loop); prod runs that loop on a dedicated blocking thread
    /// because the transport's `recv` blocks.
    ///
    /// Note (finding): `cyan-mcp`'s `Supervisor` and `Client` each *own* a
    /// `Box<dyn PluginTransport>`, so they cannot share one transport instance.
    /// For the device host's two load-bearing behaviors — relaying events to the
    /// sink and surviving crashes — the `Supervisor` is the composition point; the
    /// `Client` (request/response tool calls) is the Lens-side ReAct concern and
    /// gets its own transport when that lands.
    pub fn supervise(
        &self,
        transport: Box<dyn PluginTransport>,
        plugin_id: impl Into<PluginId>,
    ) -> Supervisor {
        Supervisor::new(
            transport,
            self.clock.clone(),
            self.emitter.clone(),
            self.sink.clone(),
            self.backoff.clone(),
            self.tenant_id.clone(),
            plugin_id.into(),
        )
    }

    /// Discover installed plugin bundles for a group: the `.cyanplugin` files the
    /// file-swarm has fetched into the group's "Plugins" workspace. This is the
    /// "registry = files" pickup — install == a bundle file appears here. Turning
    /// a bundle into a runnable [`cyan_mcp::SpawnConfig`] (unpack → manifest
    /// runtime via `cyan_mcp::Registry`/`Manifest`) is the next step on top of
    /// this detection.
    pub fn discover_bundles(
        &self,
        group_id: &str,
    ) -> anyhow::Result<Vec<crate::storage::PluginBundleFile>> {
        crate::storage::plugin_bundles_in_group(
            group_id,
            PLUGINS_WORKSPACE_NAME,
            PLUGIN_BUNDLE_SUFFIX,
        )
    }
}
