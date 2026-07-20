//! LIVE crux smoke — the Round-5 integration centerpiece, gated on `CRUX_REAL=1`.
//!
//! Unlike every other suite here this one exercises the REAL wires, not fakes:
//!
//!   create group → workspace → board
//!     → COMPILE a notebook workflow over a real HTTP round-trip to a vLLM stub
//!       (`pipeline::compile_via_llm` → POST /v1/chat/completions), and
//!     → RUN the pipeline (`pipeline::run_pipeline`) with ONE `McpTool` step that
//!       is dispatched ON-DEVICE through the supervised cyan-mcp host, spawning a
//!       REAL plugin subprocess (`StdioTransport` → `scripts/crux_plugin_run.py`).
//!
//! It asserts the pipeline produces a result/finding, emits the dashboard exec
//! events (`step_started`/`step_completed`/`finding_created`), and that the
//! plugin's cost lands on the EXTERNAL cost rail (cost isolation) — proving
//! backend↔HTTP + the real MCP-tool path + compile/execute end to end.
//!
//! Default `cargo test` SKIPS this (no `CRUX_REAL=1`) so the suite needs no live
//! deps. Boot the stack with `scripts/dev_stack.sh` (or just have `python3` on
//! PATH — the test starts its own compile-aware vLLM stub if `CYAN_VLLM_URL` is
//! unset). Every wait is bounded.

use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cyan_backend::models::commands::CommandMsg;
use cyan_backend::models::events::SwiftEvent;
use cyan_backend::pipeline::{self, PipelineStepConfig, PipelineStepState};
use cyan_backend::storage;
use serde_json::Value;
use tokio::sync::mpsc;

const GATE: &str = "CRUX_REAL";
const PLUGIN_ID: &str = "media-probe";
const TOOL: &str = "probe_asset";

/// Parse a JSON literal into a `Value`. We avoid the `json!` macro because it
/// expands to `unwrap()`, which the workspace lint rejects even in tests;
/// `expect` on a static/formatted literal is allowed and equally safe.
fn jval(s: &str) -> Value {
    serde_json::from_str(s).expect("valid JSON literal in test")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crux_pipeline_runs_live_mcp_tool_end_to_end() {
    if std::env::var(GATE).ok().as_deref() != Some("1") {
        eprintln!("crux smoke SKIPPED — set {GATE}=1 (with the dev stack up) to run it");
        return;
    }

    // ── obs capture: record the `obs`-target tracing rail so we can prove the
    //    external cost line fired (cost isolation). Set once for this binary. ──
    let obs_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    install_obs_capture(obs_buf.clone());

    let repo_root = env!("CARGO_MANIFEST_DIR");

    // ── temp device state: a fresh SQLite DB + an installed-plugins root ──
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("crux.db");
    init_base_schema(&db_path).expect("base schema");
    storage::init_db(db_path.to_str().expect("utf8 db")).expect("init_db");

    let plugins_root = tmp.path().join("plugins");
    install_plugin_bundle(&plugins_root, repo_root);
    // SAFETY: single-threaded test setup; these globals scope the device host.
    unsafe {
        std::env::set_var("CYAN_PLUGINS_ROOT", &plugins_root);
        std::env::set_var("CYAN_TENANT_ID", "crux-tenant");
    }

    // ── a real vLLM endpoint for COMPILE: prefer the stack's (CYAN_VLLM_URL),
    //    else start the bundled compile-aware stub on a free port. ──
    let mut stub = StubProc::ensure(repo_root);

    // ── channels: keep both senders alive for the whole run. Commands are not
    //    drained (no actor here), which is fine — `run_pipeline` reads step
    //    configs straight from the DB we seed. ──
    let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<CommandMsg>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SwiftEvent>();

    // ── create group → workspace → board ──
    let now = chrono::Utc::now().timestamp();
    let group = "crux-group";
    let ws = "crux-ws";
    let board = "crux-board-0001"; // ≥8 chars: run_pipeline slices board_id[..8]
    storage::group_insert_simple(group, "Crux", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(ws, group, "Main").expect("workspace");
    storage::board_insert_simple(board, ws, "Pipeline", now).expect("board");

    // An English step cell — the COMPILE input (real HTTP round-trip).
    storage::cell_insert_simple(
        "cell-english", board, "markdown", 0,
        Some("Analyze the asset notes and summarize the codec issues"),
        None, false, None, None, now, now,
    )
    .expect("english cell");

    // ── WIRE A: compile English → step configs over real HTTP to the vLLM stub ──
    let compiled = pipeline::compile_via_llm(board, &cmd_tx)
        .await
        .expect("compile_via_llm over real HTTP");
    assert_eq!(compiled["success"].as_bool(), Some(true), "compile reported success");
    let n = compiled["steps_compiled"].as_u64().unwrap_or(0);
    assert!(n >= 1, "compile applied ≥1 step config (got {n}) — HTTP wire OK");
    assert!(
        compiled["configs"].as_array().map(|a| !a.is_empty()).unwrap_or(false),
        "compile parsed a non-empty config array from the model"
    );

    // ── The McpTool step: a `local` step whose metadata names an installed
    //    plugin tool. Seeded directly into the DB (commands aren't drained here),
    //    exactly as a materialized plan that maps a stage to an installed plugin. ──
    let config = PipelineStepConfig {
        step_id: "probe".to_string(),
        depends_on: vec![],
        stage: None,
        executor: "local".to_string(),
        model: None,
        model_config: None,
        tools: vec![],
        output_format: "markdown".to_string(),
        command: None,
        timeout_seconds: Some(30),
        retry_count: Some(1),
        auto_advance: false,
        review_hold: false,
        waiting_on: None,
        notifications: vec![],
        state: PipelineStepState::default(),
    };
    let mut meta = serde_json::Map::new();
    meta.insert("pipeline".to_string(), serde_json::to_value(&config).expect("config json"));
    meta.insert(
        "mcp_tool".to_string(),
        jval(&format!(
            r#"{{ "plugin_id": "{PLUGIN_ID}", "tool": "{TOOL}", "args": {{ "src": "asset.mov" }} }}"#
        )),
    );
    let metadata = Value::Object(meta);
    storage::cell_insert_simple(
        "cell-probe", board, "markdown", 1,
        Some("Probe the asset with the media-probe plugin"),
        None, false, None, Some(&metadata.to_string()), now, now,
    )
    .expect("probe cell");

    // ── WIRE B: run the pipeline → the McpTool step dispatches on-device through
    //    the real cyan-mcp host, spawning the real plugin subprocess. ──
    let result = pipeline::run_pipeline(board, &cmd_tx, &event_tx)
        .await
        .expect("run_pipeline");

    // The step produced a result (ai_complete with non-empty output).
    let results = result["results"].as_array().cloned().unwrap_or_default();
    let probe = results
        .iter()
        .find(|r| r["step_id"].as_str() == Some("probe"))
        .unwrap_or_else(|| panic!("no 'probe' step in results: {result}"));
    assert_eq!(
        probe["status"].as_str(),
        Some("ai_complete"),
        "the McpTool step completed (got {probe})"
    );
    assert!(
        probe["output_length"].as_u64().unwrap_or(0) > 0,
        "the plugin result threaded into the step output"
    );

    // ── dashboard exec events fired for the on-device step ──
    let mut events = Vec::new();
    while let Ok(ev) = event_rx.try_recv() {
        if let SwiftEvent::StatusUpdate { message } = ev {
            events.push(message);
        }
    }
    let saw = |needle: &str| events.iter().any(|m| m.contains(needle));
    assert!(saw("pipeline.step_started"), "step_started exec event: {events:?}");
    assert!(saw("pipeline.step_completed"), "step_completed exec event: {events:?}");
    assert!(saw("pipeline.finding_created"), "finding_created exec event: {events:?}");
    assert!(saw("plugin complete"), "plugin-complete status: {events:?}");

    // ── cost isolation: the plugin's cost is on the EXTERNAL rail, tagged
    //    source=external + plugin/tool, with the cost_usd the plugin reported. ──
    let obs = String::from_utf8_lossy(&obs_buf.lock().expect("obs lock")).to_string();
    assert!(obs.contains("tool_called"), "external tool_called obs: {obs}");
    assert!(obs.contains("source") && obs.contains("external"), "obs on external rail: {obs}");
    assert!(obs.contains(PLUGIN_ID) && obs.contains(TOOL), "obs tagged plugin/tool: {obs}");
    assert!(obs.contains("0.07"), "obs carries the plugin's external cost_usd: {obs}");

    stub.kill();
    eprintln!("crux smoke PASSED — compile(HTTP)+run(local MCP) end to end; {} exec events", events.len());
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Install a tracing subscriber that captures ONLY the `obs` rail into `buf`.
fn install_obs_capture(buf: Arc<Mutex<Vec<u8>>>) {
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::EnvFilter;

    #[derive(Clone)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for BufWriter {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            if let Ok(mut g) = self.0.lock() {
                g.extend_from_slice(b);
            }
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("off,obs=info"))
        .with_writer(BufWriter(buf))
        .without_time()
        .try_init();
}

/// Lay down a real `.cyanplugin` bundle: `manifest.json` + an executable `run`
/// entrypoint (the committed `scripts/crux_plugin_run.py`) the host can spawn.
fn install_plugin_bundle(plugins_root: &Path, repo_root: &str) {
    let bundle = plugins_root.join(PLUGIN_ID);
    std::fs::create_dir_all(&bundle).expect("mkdir bundle");

    let manifest = format!(
        r#"{{
          "name": "{PLUGIN_ID}",
          "version": "1.0.0",
          "runtime": "python-uv",
          "tools": [{{
            "name": "{TOOL}",
            "when_to_use": "probe an asset for codec/resolution",
            "io_types": {{ "input": ["video"], "output": ["json"] }},
            "stage": "ingest",
            "side_effects": [],
            "locality": "device",
            "input_schema": {{}},
            "output_schema": {{}}
          }}]
        }}"#
    );
    std::fs::write(bundle.join("manifest.json"), manifest).expect("write manifest");

    let src = PathBuf::from(repo_root).join("scripts").join("crux_plugin_run.py");
    let body = std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
    let run = bundle.join("run");
    std::fs::write(&run, body).expect("write run");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&run).expect("stat run").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&run, perms).expect("chmod run");
    }
}

/// The compile vLLM endpoint. If `CYAN_VLLM_URL` is already set (dev stack), use
/// it; otherwise spawn the bundled compile-aware stub on a free port and point
/// `CYAN_VLLM_URL` at it. Bounded readiness poll.
struct StubProc(Option<std::process::Child>);

impl StubProc {
    fn ensure(repo_root: &str) -> Self {
        if std::env::var("CYAN_VLLM_URL").is_ok() {
            return StubProc(None);
        }
        let port = free_port();
        let script = PathBuf::from(repo_root).join("scripts").join("crux_vllm_stub.py");
        let child = std::process::Command::new("python3")
            .arg(&script)
            .arg(port.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn vLLM stub ({}): {e}", script.display()));
        let url = format!("http://127.0.0.1:{port}");
        // SAFETY: single-threaded test setup before any pipeline call.
        unsafe {
            std::env::set_var("CYAN_VLLM_URL", &url);
        }
        wait_listening(port);
        StubProc(Some(child))
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for StubProc {
    fn drop(&mut self) {
        self.kill();
    }
}

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

/// Poll until the stub's TCP port accepts a connection, bounded to 10s.
fn wait_listening(port: u16) {
    use std::net::{SocketAddr, TcpStream};
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("vLLM stub on 127.0.0.1:{port} not listening within 10s");
}

/// Base tables the engine migrations assume exist (mirrors the substrate harness
/// + `cyan_node`'s `init_base_schema`). Run once before `storage::init_db`.
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
        CREATE TABLE IF NOT EXISTS notebook_cells (
            id TEXT PRIMARY KEY, board_id TEXT NOT NULL, cell_type TEXT NOT NULL,
            cell_order INTEGER NOT NULL, content TEXT, output TEXT,
            collapsed INTEGER DEFAULT 0, height REAL, metadata_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}
