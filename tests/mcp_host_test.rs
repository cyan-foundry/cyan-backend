//! Consumer-integration tests for the cyan-backend MCP local host (CYAN_MCP_SPEC
//! §"Consumer-integration contracts"). cyan-backend is the LOCAL DEVICE HOST.
//!
//! These drive `cyan-mcp`'s `ScriptedTransport` — NO real subprocess — and the
//! fakes `RecordingSink` / `RecordingEmitter` / `FakeClock`. They prove the
//! backend's composition of `cyan_mcp::Supervisor` + sink:
//!   - a plugin's relayed output is forwarded to the host's `EventSink`
//!     (prod: `MeshRelaySink` → mesh → super-peer → Iggy; here: `RecordingSink`),
//!   - the supervisor restarts a crashed plugin with backoff and refuses a
//!     duplicate start.
//!
//! `cyan-mcp` is synchronous and `ScriptedTransport` never blocks (an empty queue
//! returns `Err` immediately, modelling EOF), so these are deterministic with no
//! unbounded wait — no timeout plumbing required.

use std::sync::Arc;
use std::time::Duration;

use cyan_backend::mcp_host::{MeshRelaySink, PluginHost};
use cyan_backend::models::commands::NetworkCommand;
use cyan_backend::models::events::NetworkEvent;

use cyan_mcp::{
    BackoffPolicy, Clock, Emitter, EventSink, FakeClock, ObsEvent, PluginEvent, RecordingEmitter,
    RecordingSink, ScriptedTransport, SpawnConfig, SystemClock,
};
use serde_json::Value;

const TENANT: &str = "tenant-test";

/// Parse a JSON literal into a `Value`. We avoid the `json!` macro because it
/// expands to `unwrap()`, which the workspace's `disallowed_methods` lint rejects
/// even in tests; `expect` on a static literal is allowed and equally safe.
fn jval(s: &str) -> Value {
    serde_json::from_str(s).expect("valid JSON literal in test")
}

fn spawn_config(plugin_id: &str) -> SpawnConfig {
    SpawnConfig {
        plugin_id: plugin_id.to_string(),
        command: "scripted".to_string(),
        args: vec![],
        creds: vec![],
    }
}

fn test_backoff() -> BackoffPolicy {
    BackoffPolicy {
        base: Duration::from_millis(100),
        max: Duration::from_secs(5),
        max_restarts: 3,
    }
}

/// A scripted plugin pushes a relayed event → the backend host forwards it to its
/// `EventSink`. In prod the sink is `MeshRelaySink` (relays into the group mesh so
/// the super-peer feeds Iggy); here we swap in `RecordingSink` and assert it
/// received the event verbatim.
#[test]
fn plugin_event_forwarded_to_iggy() {
    let sink = Arc::new(RecordingSink::new());
    let emitter = Arc::new(RecordingEmitter::new());

    let host = PluginHost::new(
        sink.clone() as Arc<dyn EventSink>,
        emitter as Arc<dyn Emitter>,
        Arc::new(SystemClock::new()) as Arc<dyn Clock>,
        test_backoff(),
        TENANT.to_string(),
    );

    // The plugin "pushes" a server-initiated relayed event (no id → notification).
    let mut scripted = ScriptedTransport::new();
    scripted.push_event(jval(
        r#"{ "jsonrpc": "2.0", "method": "message.relay",
             "params": { "text": "hello from plugin", "channel": "general" } }"#,
    ));

    let mut supervisor = host.supervise(Box::new(scripted), "echo-plugin");
    supervisor.start(&spawn_config("echo-plugin")).expect("start");
    // One supervision step reads the pushed event and routes it to the sink.
    supervisor.supervise_once().expect("supervise_once routes the relayed event");

    let events = sink.events();
    assert_eq!(events.len(), 1, "exactly one relayed event reaches the sink");
    let ev = &events[0];
    assert_eq!(ev.plugin_id, "echo-plugin");
    assert_eq!(ev.tenant_id, TENANT);
    assert_eq!(ev.method, "message.relay");
    assert_eq!(ev.params["text"], "hello from plugin");
    assert_eq!(ev.params["channel"], "general");
}

/// A plugin crashes (transport EOF) → the supervisor backs off and restarts it,
/// and refuses a duplicate start while it is running. No duplicate spawn.
#[test]
fn plugin_supervised_across_crash() {
    let sink = Arc::new(RecordingSink::new());
    let emitter = Arc::new(RecordingEmitter::new());
    let clock = Arc::new(FakeClock::new());

    let host = PluginHost::new(
        sink as Arc<dyn EventSink>,
        emitter.clone() as Arc<dyn Emitter>,
        clock.clone() as Arc<dyn Clock>,
        test_backoff(),
        TENANT.to_string(),
    );

    // One relayed event, then the queue is empty → next recv is Err == crash/EOF.
    let mut scripted = ScriptedTransport::new();
    scripted.push_event(jval(r#"{ "jsonrpc": "2.0", "method": "ping", "params": {} }"#));
    let log = scripted.log();

    let mut supervisor = host.supervise(Box::new(scripted), "crashy-plugin");
    supervisor.start(&spawn_config("crashy-plugin")).expect("initial start");
    assert!(supervisor.is_running());
    assert_eq!(log.spawn_count(), 1, "one spawn after start");

    // No duplicate start while running.
    assert!(
        supervisor.start(&spawn_config("crashy-plugin")).is_err(),
        "a second start while running must be rejected (no duplicate spawn)"
    );
    assert_eq!(log.spawn_count(), 1, "rejected start must not spawn again");

    // Step 1: the relayed event is consumed (still running).
    supervisor.supervise_once().expect("first step consumes the relayed event");
    assert!(supervisor.is_running());

    // Step 2: recv hits the empty queue (EOF) → crash → backoff → restart.
    supervisor.supervise_once().expect("crash is handled by restart");
    assert!(supervisor.is_running(), "supervisor restarted the plugin");
    assert_eq!(log.spawn_count(), 2, "exactly one restart spawn after the crash");

    // Backoff was applied via the clock: first restart sleeps `base`.
    let sleeps = clock.sleeps();
    assert_eq!(sleeps.len(), 1, "one backoff sleep on the first restart");
    assert_eq!(sleeps[0], Duration::from_millis(100), "first backoff == base");

    // Obs tells the crash/restart story, tenant- and plugin-scoped.
    let obs = emitter.events();
    assert!(obs.iter().any(|o| matches!(o.event, ObsEvent::PluginStarted)));
    assert!(obs.iter().any(|o| matches!(o.event, ObsEvent::PluginCrashed { .. })));
    assert!(
        obs.iter()
            .any(|o| matches!(o.event, ObsEvent::PluginRestarted { attempt: 1 })),
        "a restart attempt #1 is emitted"
    );
    assert!(
        obs.iter().all(|o| o.tenant_id == TENANT && o.plugin_id == "crashy-plugin"),
        "every obs line is tenant- and plugin-scoped"
    );
}

/// The prod sink relays a plugin event INTO the group mesh: `MeshRelaySink::deliver`
/// emits a `NetworkCommand::Broadcast` carrying a `NetworkEvent::PluginRelay` on the
/// engine's network channel (which the super-peer picks off gossip for Iggy).
#[test]
fn mesh_relay_sink_broadcasts_plugin_event_into_group() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<NetworkCommand>();
    let sink = MeshRelaySink::new("group-42".to_string(), tx);

    sink.deliver(PluginEvent {
        plugin_id: "slack".to_string(),
        tenant_id: TENANT.to_string(),
        method: "message.relay".to_string(),
        params: jval(r#"{ "text": "standup at 10" }"#),
    });

    let cmd = rx.try_recv().expect("a broadcast command was queued");
    match cmd {
        NetworkCommand::Broadcast { group_id, event } => {
            assert_eq!(group_id, "group-42");
            match event {
                NetworkEvent::PluginRelay { plugin_id, method, payload } => {
                    assert_eq!(plugin_id, "slack");
                    assert_eq!(method, "message.relay");
                    // payload is the params JSON serialized as a string.
                    let parsed: serde_json::Value =
                        serde_json::from_str(&payload).expect("payload is JSON");
                    assert_eq!(parsed["text"], "standup at 10");
                }
                other => panic!("expected PluginRelay, got {other:?}"),
            }
        }
        other => panic!("expected Broadcast, got {other:?}"),
    }
}
