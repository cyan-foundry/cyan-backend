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

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

use cyan_mcp::{
    BackoffPolicy, Client, Clock, Emitter, EventSink, Obs, PluginEvent, PluginId, PluginTransport,
    RecordingSink, Registry, Supervisor, TenantId, ToolBlock,
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

    /// Resolve a tool name to `(plugin_id, ToolBlock)` by indexing the group's
    /// installed plugin bundles under `plugins_root` (the file-swarm unpacks each
    /// `.cyanplugin` into a subdir there) via cyan-mcp's `Registry`. This is the
    /// "registry = files → tools" wiring: it turns an installed bundle into a tool
    /// a local pipeline step can dispatch, and surfaces the tool's manifest
    /// `side_effects` so the dispatcher can gate it. `Ok(None)` = no such tool
    /// installed; a bad bundle is skipped by the registry, not fatal.
    pub fn resolve_installed_tool(
        &self,
        plugins_root: &Path,
        tool: &str,
    ) -> anyhow::Result<Option<(String, ToolBlock)>> {
        let mut registry = Registry::new();
        registry
            .index(plugins_root)
            .map_err(|e| anyhow!("index plugins registry {}: {e}", plugins_root.display()))?;
        Ok(registry.lookup_by_tool(tool).and_then(|entry| {
            entry
                .manifest
                .tools
                .iter()
                .find(|t| t.name == tool)
                .map(|tb| (entry.plugin_id.clone(), tb.clone()))
        }))
    }

    /// Dispatch one `McpTool` step on-device through the supervised cyan-mcp host
    /// — the local mirror of the lens cloud `McpTool` path (same contract). The
    /// plugin runs locally; no cloud round-trip.
    ///
    /// `connect` produces a *live* transport for this call (prod: a spawned
    /// `StdioTransport`; tests: a pre-scripted `ScriptedTransport`). It is a
    /// closure, not an eager argument, so a GATED tool never opens a transport —
    /// in prod that means a side-effecting plugin process is never even spawned
    /// before approval.
    ///
    /// Cost isolation (matches lens): the plugin reports its own billing as
    /// `cost_usd` in the tool result; we record that on the EXTERNAL rail
    /// (`ledger.record_external_tool`) and NEVER against our LLM token tally.
    /// cyan-mcp's plugin-internal `tool_called` obs is discarded (a
    /// `DiscardEmitter`) so cost is counted exactly once, on our rail.
    pub fn dispatch_mcp_tool<F>(
        &self,
        scope: &RunScope,
        step: &McpTool,
        side_effects: &[String],
        approved: bool,
        ledger: &RunCostLedger,
        connect: F,
    ) -> anyhow::Result<McpDispatch>
    where
        F: FnOnce() -> anyhow::Result<Box<dyn PluginTransport>>,
    {
        // Gate: a side-effecting tool (external_send / delete) is NEVER
        // auto-executed. Return without opening a transport — the human-approval
        // path (pipeline.rs::approve_step) flips `approved` before a re-dispatch.
        if requires_approval(side_effects) && !approved {
            return Ok(McpDispatch::Gated {
                side_effects: side_effects.to_vec(),
            });
        }

        let transport = connect()?;
        // Request/response tool call: own transport (cyan-mcp's Supervisor and
        // Client cannot share one — see `supervise`). Relayed events during the
        // call go to a throwaway sink; the tool *result* is what threads back.
        let mut client = Client::new(
            transport,
            Arc::new(RecordingSink::new()) as Arc<dyn EventSink>,
            Arc::new(DiscardEmitter) as Arc<dyn Emitter>,
            self.clock.clone(),
            scope.tenant_id.clone(),
            step.plugin_id.clone(),
        );
        client
            .initialize()
            .map_err(|e| anyhow!("mcp initialize {}: {e}", step.plugin_id))?;

        // THE ADVERTISED-vs-REGISTERED CONTRACT (mcp_tool_test.rs): the bound
        // tool must be one the plugin PROCESS actually registers. A stale or
        // mis-curated bundle can advertise a tool (e.g. the raw
        // `post_…_files_local_upload` twin) the process never registers — that
        // died as a bare "Unknown tool" mid-run (live 2026-07-09). Reconcile at
        // spawn time and refuse LOUDLY, naming both sides, before any
        // tools/call fires.
        let registered = client
            .list_tool_names()
            .map_err(|e| anyhow!("mcp tools/list {}: {e}", step.plugin_id))?;
        if !registered.iter().any(|n| n == &step.tool) {
            return Err(anyhow!(
                "plugin '{}' contract violation: the installed manifest advertises tool '{}' \
                 but the plugin process registers [{}] — the bundle is stale or mis-curated; \
                 reinstall the plugin (or pin an exact registered tool name)",
                step.plugin_id,
                step.tool,
                registered.join(", ")
            ));
        }

        let start = self.clock.now();
        let result = client
            .call_tool(&step.tool, step.args.clone())
            .map_err(|e| anyhow!("mcp call_tool {}.{}: {e}", step.plugin_id, step.tool))?;
        let duration_ms = self.clock.now().saturating_sub(start).as_millis() as u64;

        // Convention (mirrors lens): a partner/plugin tool reports its own billing
        // as `cost_usd`. EXTERNAL rail only — never our vLLM tokens.
        let cost_usd = result.get("cost_usd").and_then(Value::as_f64);
        ledger.record_external_tool(ToolCalledObs {
            tenant_id: scope.tenant_id.clone(),
            run_id: scope.run_id.clone(),
            plugin_id: step.plugin_id.clone(),
            tool: step.tool.clone(),
            duration_ms,
            cost_usd,
            source: "external".to_string(),
        });

        Ok(McpDispatch::Ran(McpToolResult {
            result,
            duration_ms,
            cost_usd,
        }))
    }
}

/// The env var a manifest credential resolves from, by convention (kept
/// IDENTICAL to the lens host so a bundle behaves the same on device and
/// cloud): `<PLUGIN>_<PROVIDER-LAST-SEGMENT>_TOKEN`, uppercased. The frameio
/// manifest's `oauth2`/`adobe_ims` credential resolves `FRAMEIO_IMS_TOKEN`.
pub fn cred_env_var(plugin_name: &str, provider: &str) -> String {
    let segment = provider.rsplit(['_', '-']).next().unwrap_or(provider);
    format!("{}_{}_TOKEN", env_token(plugin_name), env_token(segment))
}

/// Uppercase + squash non-alphanumerics to `_` (env-var-safe).
pub(crate) fn env_token(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Build the `SpawnConfig` for an unpacked bundle dir: the manifest's declared
/// runtime picks the launch command (a forge `python-uv` bundle ships
/// `src/plugin.py` + `uv.lock`, so `uv run` inside the bundle dir is the whole
/// launch — same recipe as the lens host), and its declared credentials resolve
/// from the DEVICE process env at spawn ("inject at plugin spawn" — the value
/// never lives in the bundle). A missing credential refuses the spawn with a
/// clear error, never a half-started process. A bundle with a legacy `run`
/// entrypoint keeps working.
pub fn bundle_spawn_config(
    plugin_id: &str,
    bundle_dir: &Path,
    tenant_id: &str,
) -> anyhow::Result<cyan_mcp::SpawnConfig> {
    let manifest = cyan_mcp::Manifest::from_bundle(bundle_dir)
        .map_err(|e| anyhow!("bundle manifest {}: {e}", bundle_dir.display()))?;

    let mut creds = vec![
        (
            "CYAN_TENANT_ID".to_string(),
            cyan_mcp::SecretString::new(tenant_id.to_string()),
        ),
        // The plugin must confine paths to the SAME root the host stages
        // attachments into (media_staging). Injected explicitly so a plugin
        // never falls back to its own cwd-relative default when the app
        // process env lacks CYAN_MEDIA_ROOT — that mismatch is unfixable
        // from inside the plugin. (Not a secret; rides the env-inject rail.)
        (
            "CYAN_MEDIA_ROOT".to_string(),
            cyan_mcp::SecretString::new(
                crate::media_staging::effective_media_root()
                    .to_string_lossy()
                    .into_owned(),
            ),
        ),
    ];
    for c in manifest
        .credentials
        .iter()
        .chain(manifest.extra_credentials.iter())
    {
        let env_var = cred_env_var(&manifest.name, &c.provider);
        let val = resolve_credential(&manifest.name, &c.provider, tenant_id, &env_var)
            .ok_or_else(|| {
                anyhow!(
                    "plugin '{plugin_id}' requires credential '{env_var}' and none is available \
                     (vault key '{}', cred file {}, process env all empty): spawn refused",
                    crate::device_vault::plugin_cred_key(&manifest.name, &c.provider, tenant_id),
                    cred_env_file().display(),
                )
            })?;
        creds.push((env_var, val));
    }

    let run_entry = bundle_dir.join("run");
    let (command, args) = match manifest.runtime.as_deref() {
        Some("python-uv") => (
            "uv".to_string(),
            vec![
                "run".to_string(),
                "--directory".to_string(),
                bundle_dir.to_string_lossy().into_owned(),
                "src/plugin.py".to_string(),
            ],
        ),
        _ if run_entry.is_file() => (run_entry.to_string_lossy().into_owned(), vec![]),
        other => {
            return Err(anyhow!(
                "plugin '{plugin_id}' declares runtime {other:?}, which the device host cannot spawn (supported: python-uv, or a bundled `run` entrypoint)"
            ))
        }
    };

    Ok(cyan_mcp::SpawnConfig {
        plugin_id: plugin_id.to_string(),
        command,
        args,
        creds,
    })
}

/// Resolve one declared plugin credential FRESH at spawn time
/// (PLUGIN_CREDENTIAL_ONBOARDING §C), most-authoritative first:
///
/// 1. the device PLUGIN VAULT (`plugin_cred_key(plugin, provider, tenant)`) —
///    the real per-install custody once connect-time capture writes it;
/// 2. the credential dotenv file (`CYAN_CRED_ENV_FILE`, default
///    `~/.frameio.env`) read FRESH — the auto-refreshing loader rewrites that
///    file hourly, and re-reading per spawn (instead of trusting the app
///    process env, a launch-time SNAPSHOT) is what fixes the 401-mid-session
///    bug without touching any plugin;
/// 3. the process env — the demo stopgap, last.
fn resolve_credential(
    plugin: &str,
    provider: &str,
    tenant_id: &str,
    env_var: &str,
) -> Option<cyan_mcp::SecretString> {
    let key = crate::device_vault::plugin_cred_key(plugin, provider, tenant_id);
    if let Ok(Some(secret)) = crate::device_vault::plugin_cred_vault().load(&key) {
        use secrecy::ExposeSecret;
        return Some(cyan_mcp::SecretString::new(secret.expose_secret().to_string()));
    }
    if let Some(v) = dotenv_lookup(&cred_env_file(), env_var) {
        return Some(cyan_mcp::SecretString::new(v));
    }
    std::env::var(env_var)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(cyan_mcp::SecretString::new)
}

/// The credential dotenv file consulted at spawn (fresh read). Overridable for
/// tests/deploys via `CYAN_CRED_ENV_FILE`; defaults to the auto-refreshed
/// `~/.frameio.env` the demo loader maintains.
pub(crate) fn cred_env_file() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("CYAN_CRED_ENV_FILE")
        && !p.trim().is_empty()
    {
        return std::path::PathBuf::from(p.trim());
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".frameio.env")
}

/// Minimal dotenv lookup: `KEY=VALUE` lines, tolerating a leading `export `,
/// surrounding quotes, and comment/blank lines. Returns the LAST match (the
/// refresher appends/rewrites; last write wins).
pub(crate) fn dotenv_lookup(path: &std::path::Path, key: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut found = None;
    for line in text.lines() {
        let line = line.trim();
        let line = line.strip_prefix("export ").unwrap_or(line);
        if let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            let v = v.trim().trim_matches(|c| c == '"' || c == '\'');
            if !v.is_empty() {
                found = Some(v.to_string());
            }
        }
    }
    found
}

/// Side effect that requires the human-approval gate before a tool runs.
pub const SIDE_EFFECT_EXTERNAL_SEND: &str = "external_send";
/// Side effect that requires the human-approval gate before a tool runs.
pub const SIDE_EFFECT_DELETE: &str = "delete";

/// Whether a tool's declared `side_effects` require human approval before it may
/// auto-execute. `external_send` / `delete` do; a pure/read-only tool does not.
pub fn requires_approval(side_effects: &[String]) -> bool {
    side_effects
        .iter()
        .any(|s| s == SIDE_EFFECT_EXTERNAL_SEND || s == SIDE_EFFECT_DELETE)
}

/// Run scope carried on every external cost obs line: which tenant + pipeline run
/// a tool call belongs to.
#[derive(Debug, Clone)]
pub struct RunScope {
    /// Tenant that owns the run.
    pub tenant_id: String,
    /// Pipeline run id (the `op_id` on the obs rail).
    pub run_id: String,
}

/// One `McpTool` pipeline step: dispatch `tool` on `plugin_id` with `args`.
/// Mirrors the lens cloud contract `McpTool { plugin_id, tool, args }`
/// (WORKFLOW_MATERIALIZATION §2). The backend uses the canonical field name
/// `tool` — lens had to call it `tool_name` only because its enum's serde tag
/// already occupied `tool`; there is no such collision here.
#[derive(Debug, Clone)]
pub struct McpTool {
    /// Plugin that exposes the tool.
    pub plugin_id: String,
    /// Tool name to call.
    pub tool: String,
    /// JSON arguments for the call.
    pub args: Value,
}

/// The result of a non-gated tool dispatch.
#[derive(Debug, Clone)]
pub struct McpToolResult {
    /// The plugin tool's JSON result (threads back into the step output).
    pub result: Value,
    /// Wall-clock duration of the call.
    pub duration_ms: u64,
    /// External/plugin cost in USD (partner billing pass-through), if reported.
    pub cost_usd: Option<f64>,
}

/// Outcome of dispatching an `McpTool` step locally.
#[derive(Debug, Clone)]
pub enum McpDispatch {
    /// The tool ran; its result is threaded into the step output.
    Ran(McpToolResult),
    /// The tool is side-effecting and unapproved — it was NOT executed and
    /// requires the human-approval gate. No transport was opened / process spawned.
    Gated {
        /// The declared side effects that triggered the gate.
        side_effects: Vec<String>,
    },
}

/// One flat, tenant-scoped EXTERNAL cost obs line — a plugin/partner tool call.
/// `source` is always `"external"`: this rail is for THEIR billing
/// (manifest cost-locality), kept separate from our LLM token tally.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCalledObs {
    /// Tenant the call is scoped to.
    pub tenant_id: String,
    /// Pipeline run the call belongs to.
    pub run_id: String,
    /// Plugin that was called.
    pub plugin_id: String,
    /// Tool that was called.
    pub tool: String,
    /// Wall-clock duration of the call.
    pub duration_ms: u64,
    /// External/plugin cost in USD, if the tool reported it.
    pub cost_usd: Option<f64>,
    /// Cost rail discriminator — always `"external"`.
    pub source: String,
}

/// Per-run cost ledger with two SEPARATE rails: our LLM token tally (our vLLM
/// reasoning) and external plugin/partner tool costs. The cost-isolation
/// invariant: an `McpTool` call only ever touches the external rail — it adds
/// ZERO to the LLM tally (proven by `local_mcp_tool_cost_is_external_not_tokens`).
#[derive(Default)]
pub struct RunCostLedger {
    llm_tokens_in: AtomicU64,
    llm_tokens_out: AtomicU64,
    external: Mutex<Vec<ToolCalledObs>>,
}

impl RunCostLedger {
    /// A fresh, empty ledger.
    pub fn new() -> Self {
        RunCostLedger::default()
    }

    /// Record tokens our own vLLM reasoning spent this run (the LLM rail).
    pub fn record_llm(&self, tokens_in: u64, tokens_out: u64) {
        self.llm_tokens_in.fetch_add(tokens_in, Ordering::Relaxed);
        self.llm_tokens_out.fetch_add(tokens_out, Ordering::Relaxed);
    }

    /// Our LLM token tally so far as `(tokens_in, tokens_out)`.
    pub fn llm_tokens(&self) -> (u64, u64) {
        (
            self.llm_tokens_in.load(Ordering::Relaxed),
            self.llm_tokens_out.load(Ordering::Relaxed),
        )
    }

    /// Record one external plugin/partner tool call (the external rail). Also
    /// emits a flat obs line on the `"obs"` target for prod observability.
    pub fn record_external_tool(&self, obs: ToolCalledObs) {
        tracing::info!(
            target: "obs",
            event = "tool_called",
            source = "external",
            tenant_id = %obs.tenant_id,
            run_id = %obs.run_id,
            plugin_id = %obs.plugin_id,
            tool = %obs.tool,
            duration_ms = obs.duration_ms,
            cost_usd = ?obs.cost_usd,
        );
        if let Ok(mut g) = self.external.lock() {
            g.push(obs);
        }
    }

    /// Snapshot of every external tool call recorded. Poison-safe (no panic).
    pub fn external_tool_calls(&self) -> Vec<ToolCalledObs> {
        self.external.lock().map(|g| g.clone()).unwrap_or_default()
    }
}

/// A cyan-mcp `Emitter` that drops the plugin-internal obs. The external
/// cost-isolation obs (`tool_called` / `source=external`) is emitted by the host
/// on the backend rail (`RunCostLedger`), not from inside cyan-mcp's client — so a
/// tool call is counted exactly once.
#[derive(Default)]
struct DiscardEmitter;

impl Emitter for DiscardEmitter {
    fn emit(&self, _obs: &Obs) {}
}
