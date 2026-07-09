//! phase3_reverse_loop — the Phase-3 REVERSE-loop harness.
//!
//! Drives the REAL verbs end-to-end against the persistent ledger at
//! `$CYAN_DATA_DIR/cyan.db` (the same DB the gates assert with sqlite3):
//!
//!   sense    register the phase-3 master (the frame-signature fixture) + the
//!            published v1 proxy (remote_refs.frameio = the REAL file id), post
//!            a reviewer comment WITH a timecode on that Frame.io file if it is
//!            not already there (IMS-authed, real User-Agent), fetch the real
//!            `list_comments` envelope, and ingest it via the `sense_ingest`
//!            verb (review_state::command).
//!   propose  infer a mechanical op from the ingested note (note_inference —
//!            never guessed) and propose it via the `propose_op` verb (AGENT).
//!   confirm  approve the proposal via `confirm_op` and advance NOTES_IN →
//!            CONFORMING via `confirm_notes` (both HUMAN-gated verbs; the
//!            harness acts AS the human actor — it exercises the gate, it does
//!            not remove it).
//!   conform  run `review_loop::conform_proxy` through the REAL
//!            `StdioConformDispatch` into cyan-media (stdio JSON-RPC `conform`
//!            tool), freezing a new ChangeVersion and registering the rendered
//!            output as a DERIVED proxy.
//!   all      the four in order (default). Every step is idempotent/resumable:
//!            appends dedup on entry_hash, transitions are checked before they
//!            fire, and the Frame.io comment is looked up before it is posted.
//!
//! Secrets: `FRAMEIO_IMS_TOKEN` is held as a `SecretString` and only exposed
//! into the Authorization header — never printed, never logged.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use cyan_backend::conform_dispatch::StdioConformDispatch;
use cyan_backend::{asset_registry, changelist, note_inference, review_loop, review_state, storage};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

const USER_AGENT: &str = "cyan-frameio/1.0";
/// The reviewer note the loop runs on — fully mechanical (a tail trim), so the
/// agent can infer an executable op and the conformed proxy keeps frame N's
/// signature == N (a tail trim never shifts head frame indices).
const NOTE_TEXT: &str = "Trim 12 frames off the tail — it hangs too long.";
const NOTE_FRAME: i64 = 60;

#[derive(Parser)]
#[command(about = "Phase-3 reverse-loop harness (sense → propose → confirm → conform)")]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand, Clone, Copy)]
enum Cmd {
    Sense,
    Propose,
    Confirm,
    Conform,
    All,
}

struct Env {
    tenant: String,
    branch: String,
    file_id: String,
    fixture: PathBuf,
    media_root: PathBuf,
    media_dir: PathBuf,
    account_id: Option<String>,
    token: Option<SecretString>,
}

fn read_env() -> Result<Env> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let media_root = PathBuf::from(
        std::env::var("CYAN_MEDIA_ROOT").unwrap_or_else(|_| format!("{home}/.cyan-phase3/media")),
    );
    let fixture = PathBuf::from(std::env::var("PHASE3_FIXTURE").unwrap_or_else(|_| {
        media_root.join("master/sig_source.mp4").display().to_string()
    }));
    Ok(Env {
        tenant: std::env::var("PHASE3_TENANT").unwrap_or_else(|_| "phase3".to_string()),
        branch: "main".to_string(),
        file_id: std::env::var("PHASE3_FRAMEIO_FILE_ID").context(
            "PHASE3_FRAMEIO_FILE_ID not set — the Frame.io file id of the uploaded phase-3 master",
        )?,
        fixture,
        media_root,
        media_dir: PathBuf::from(
            std::env::var("CYAN_MEDIA_DIR").unwrap_or_else(|_| format!("{home}/cyan-media")),
        ),
        account_id: std::env::var("FRAMEIO_ACCOUNT_ID").ok(),
        token: std::env::var("FRAMEIO_IMS_TOKEN").ok().map(SecretString::from),
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let env = read_env()?;
    let db_path = storage::resolve_db_path("");
    let db_str = db_path
        .to_str()
        .ok_or_else(|| anyhow!("non-utf8 db path {}", db_path.display()))?;
    storage::init_db(db_str).with_context(|| format!("init ledger at {db_str}"))?;
    println!("ledger: {db_str}");

    match cli.cmd.unwrap_or(Cmd::All) {
        Cmd::Sense => sense(&env),
        Cmd::Propose => propose(&env),
        Cmd::Confirm => confirm(&env),
        Cmd::Conform => conform(&env),
        Cmd::All => {
            sense(&env)?;
            propose(&env)?;
            confirm(&env)?;
            conform(&env)
        }
    }
}

/// Run a review verb through the JSON command surface; `{"error": ...}` → Err.
fn verb(cmd: Value) -> Result<Value> {
    let out = review_state::command(&cmd.to_string());
    let v: Value = serde_json::from_str(&out).context("verb reply not JSON")?;
    if let Some(e) = v.get("error") {
        bail!("verb {} failed: {}", cmd["op"], e);
    }
    Ok(v)
}

/// ffprobe the fixture → (fps, total frames).
fn probe_fixture(fixture: &PathBuf) -> Result<(f64, i64)> {
    let out = std::process::Command::new("ffprobe")
        .args([
            "-v", "error", "-select_streams", "v:0", "-count_frames",
            "-show_entries", "stream=r_frame_rate,nb_read_frames", "-of", "json",
        ])
        .arg(fixture)
        .output()
        .context("run ffprobe")?;
    if !out.status.success() {
        bail!("ffprobe failed on {}: {}", fixture.display(), String::from_utf8_lossy(&out.stderr));
    }
    let v: Value = serde_json::from_slice(&out.stdout).context("ffprobe JSON")?;
    let stream = v["streams"]
        .get(0)
        .ok_or_else(|| anyhow!("no video stream in {}", fixture.display()))?;
    let rate = stream["r_frame_rate"]
        .as_str()
        .ok_or_else(|| anyhow!("no r_frame_rate"))?;
    let (num, den) = rate.split_once('/').unwrap_or((rate, "1"));
    let fps = num.parse::<f64>().context("fps num")? / den.parse::<f64>().context("fps den")?;
    if fps <= 0.0 {
        bail!("non-positive fps {rate}");
    }
    let frames: i64 = stream["nb_read_frames"]
        .as_str()
        .ok_or_else(|| anyhow!("no nb_read_frames"))?
        .parse()
        .context("frame count")?;
    Ok((fps, frames))
}

fn master_hash(env: &Env) -> Result<String> {
    let bytes = std::fs::read(&env.fixture)
        .with_context(|| format!("read fixture {}", env.fixture.display()))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

// ── Frame.io (v4, IMS-authed, real User-Agent — WAF requirement) ────────────

fn fio_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()
        .context("build http client")
}

fn fio(env: &Env, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value> {
    let token = env
        .token
        .as_ref()
        .ok_or_else(|| anyhow!("FRAMEIO_IMS_TOKEN not set (expired tokens 401 — re-mint)"))?;
    let url = format!("https://api.frame.io/v4{path}");
    let mut req = fio_client()?
        .request(method.clone(), &url)
        .bearer_auth(token.expose_secret());
    if let Some(b) = body {
        req = req.json(&b);
    }
    let resp = req.send().with_context(|| format!("{method} {url}"))?;
    let status = resp.status();
    let text = resp.text().context("read body")?;
    if !status.is_success() {
        bail!("{method} {path} → {status}: {}", text.chars().take(300).collect::<String>());
    }
    serde_json::from_str(&text).with_context(|| format!("{path} reply not JSON"))
}

fn account(env: &Env) -> Result<&str> {
    env.account_id
        .as_deref()
        .ok_or_else(|| anyhow!("FRAMEIO_ACCOUNT_ID not set"))
}

// ── sense ───────────────────────────────────────────────────────────────────

fn sense(env: &Env) -> Result<()> {
    let (fps, frames) = probe_fixture(&env.fixture)?;
    let duration_ms = ((frames as f64 / fps) * 1000.0).round() as i64;
    let master = master_hash(env)?;
    println!("master: {master} ({frames} frames @ {fps} fps)");

    register_master(env, &master, fps, duration_ms)?;
    publish_v1(env, &master)?;
    let v1 = head_version(env, &master)?;
    register_v1_proxy(env, &master, &v1, fps, duration_ms)?;

    let acct = account(env)?.to_string();
    let comments_path = format!("/accounts/{acct}/files/{}/comments", env.file_id);
    let mut envelope = fio(env, reqwest::Method::GET, &comments_path, None)?;
    let have = envelope["data"]
        .as_array()
        .is_some_and(|a| a.iter().any(|c| c["text"].as_str() == Some(NOTE_TEXT)));
    if !have {
        let created = fio(
            env,
            reqwest::Method::POST,
            &comments_path,
            Some(json!({ "data": { "text": NOTE_TEXT, "timestamp": NOTE_FRAME } })),
        )?;
        println!(
            "posted reviewer comment {} at frame {NOTE_FRAME}",
            created["data"]["id"].as_str().unwrap_or("?")
        );
        envelope = fio(env, reqwest::Method::GET, &comments_path, None)?;
    } else {
        println!("reviewer comment already present on file {}", env.file_id);
    }

    let ingest = verb(json!({
        "op": "sense_ingest",
        "tenant_id": env.tenant,
        "proxy_ref": env.file_id,
        "result": envelope,
    }))?;
    println!(
        "sense_ingest: appended={} deduped={} own_refs_skipped={} unmappable={} malformed={} state={}",
        ingest["appended"].as_array().map_or(0, Vec::len),
        ingest["deduped"],
        ingest["own_refs_skipped"],
        ingest["unmappable"],
        ingest["malformed"],
        ingest["state"]["state"]
    );
    Ok(())
}

fn register_master(env: &Env, master: &str, fps: f64, duration_ms: i64) -> Result<()> {
    let lock = storage::db().lock().map_err(|e| anyhow!("db lock: {e}"))?;
    asset_registry::upsert(
        &lock,
        &asset_registry::Asset {
            hash: master.to_string(),
            tenant_id: env.tenant.clone(),
            kind: Some("master".to_string()),
            fps: Some(fps),
            duration_ms: Some(duration_ms),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({ "path": env.fixture.display().to_string() }),
            render_profile: None,
            created_at: 0,
        },
    )?;
    Ok(())
}

/// DRAFT → IN_REVIEW (freezes v1 — the untouched master, an empty change list).
fn publish_v1(env: &Env, master: &str) -> Result<()> {
    let state = verb(json!({
        "op": "start_draft", "tenant_id": env.tenant,
        "asset_hash": master, "branch": env.branch,
    }))?;
    if state["state"].as_str() == Some("DRAFT") {
        let st = verb(json!({
            "op": "publish", "tenant_id": env.tenant,
            "asset_hash": master, "branch": env.branch, "actor": "human",
        }))?;
        println!("published v1: state={}", st["state"]);
    } else {
        println!("review already at {}", state["state"]);
    }
    Ok(())
}

fn head_version(env: &Env, master: &str) -> Result<changelist::ChangeVersion> {
    let lock = storage::db().lock().map_err(|e| anyhow!("db lock: {e}"))?;
    changelist::get(&lock, &env.tenant, master, &env.branch)?
        .head_version
        .ok_or_else(|| anyhow!("no head version — publish did not freeze v1"))
}

fn register_v1_proxy(
    env: &Env,
    master: &str,
    v1: &changelist::ChangeVersion,
    fps: f64,
    duration_ms: i64,
) -> Result<()> {
    let proxy_hash = blake3::hash(format!("{master}:proxy:v{}", v1.version_no).as_bytes())
        .to_hex()
        .to_string();
    let lock = storage::db().lock().map_err(|e| anyhow!("db lock: {e}"))?;
    asset_registry::upsert(
        &lock,
        &asset_registry::Asset {
            hash: proxy_hash.clone(),
            tenant_id: env.tenant.clone(),
            kind: Some("proxy".to_string()),
            fps: Some(fps),
            duration_ms: Some(duration_ms),
            derived_from_asset: None,
            derived_from_version: None,
            remote_refs: json!({}),
            profile_json: json!({}),
            render_profile: Some("phase3-signature".to_string()),
            created_at: 0,
        },
    )?;
    asset_registry::set_derivation(&lock, &env.tenant, &proxy_hash, master, &v1.version_id)?;
    asset_registry::set_remote_ref(&lock, &env.tenant, &proxy_hash, "frameio", &env.file_id)?;
    println!("v1 proxy {proxy_hash} → frameio {}", env.file_id);
    Ok(())
}

// ── propose ─────────────────────────────────────────────────────────────────

fn propose(env: &Env) -> Result<()> {
    let master = master_hash(env)?;
    let (note, existing, duration_frames) = {
        let lock = storage::db().lock().map_err(|e| anyhow!("db lock: {e}"))?;
        let view = changelist::get(&lock, &env.tenant, &master, &env.branch)?;
        let note = view
            .entries
            .iter()
            .filter(|e| e.kind == "note" && e.source.as_deref() == Some("frameio") && e.active)
            .max_by_key(|e| (e.created_at, e.seq))
            .cloned()
            .ok_or_else(|| anyhow!("no frameio note in the ledger — run `sense` first"))?;
        let existing = view
            .entries
            .iter()
            .find(|e| {
                e.kind == "op"
                    && e.proposed_by.as_deref() == Some("agent")
                    && e.active
                    && (e.state == "proposed" || e.state == "approved")
            })
            .cloned();
        let asset = asset_registry::get(&lock, &env.tenant, &master)?;
        let frames = match (asset.duration_ms, asset.fps) {
            (Some(ms), Some(fps)) => Some(((ms as f64 / 1000.0) * fps).round() as i64),
            _ => None,
        };
        (note, existing, frames)
    };
    if let Some(op) = existing {
        println!("agent op already proposed: {} ({}, {})", op.id, op.op.as_deref().unwrap_or("?"), op.state);
        return Ok(());
    }

    let inferred = note_inference::infer_op(&note.intent, note.tc_in, note.tc_out, duration_frames)
        .ok_or_else(|| {
            anyhow!(
                "note {:?} is not a fully-specified mechanical edit — escalate to the human",
                note.intent
            )
        })?;
    let entry = changelist::ChangeEntry {
        id: String::new(),
        entry_hash: String::new(),
        asset_hash: master.clone(),
        tenant_id: env.tenant.clone(),
        branch: None,
        track: Some("V1".to_string()),
        tc_in: inferred.tc_in,
        tc_out: inferred.tc_out,
        kind: "op".to_string(),
        op: Some(inferred.op.clone()),
        params: inferred.params.clone(),
        intent: format!("{}: {}", inferred.op, note.intent),
        source: Some("cyan".to_string()),
        source_ref: None,
        author: Some("cyan-agent".to_string()),
        role: Some("agent".to_string()),
        proposed_by: None, // forced to agent by propose_op
        created_at: 0,
        state: String::new(),
        active: true,
        approved_by: None,
        approved_at: None,
        supersedes: None,
        superseded_by: None,
        seq: 0,
        depends_on: Some(note.id.clone()),
        version_ref: None,
        outcome: None,
        updated_at: 0,
        updated_by: None,
    };
    let proposed = verb(json!({
        "op": "propose_op",
        "asset_hash": master,
        "branch": env.branch,
        "actor": "agent",
        "entry": serde_json::to_value(&entry)?,
    }))?;
    println!(
        "proposed op {} = {} {} (from note {})",
        proposed["id"], proposed["op"], proposed["params"], note.id
    );
    Ok(())
}

// ── confirm ─────────────────────────────────────────────────────────────────

fn confirm(env: &Env) -> Result<()> {
    let master = master_hash(env)?;
    let pending = {
        let lock = storage::db().lock().map_err(|e| anyhow!("db lock: {e}"))?;
        let view = changelist::get(&lock, &env.tenant, &master, &env.branch)?;
        view.entries
            .iter()
            .filter(|e| e.kind == "op" && e.proposed_by.as_deref() == Some("agent") && e.active)
            .max_by_key(|e| (e.created_at, e.seq))
            .cloned()
    };
    let op = pending.ok_or_else(|| anyhow!("no agent op to confirm — run `propose` first"))?;
    if op.state == "proposed" {
        let approved = verb(json!({
            "op": "confirm_op", "tenant_id": env.tenant,
            "entry_id": op.id, "decision": "approve", "actor": "human",
        }))?;
        println!(
            "human approved op {}: state={} approved_by={}",
            op.id, approved["state"], approved["approved_by"]
        );
    } else {
        println!("op {} already {}", op.id, op.state);
    }

    let state = verb(json!({
        "op": "get", "tenant_id": env.tenant, "asset_hash": master, "branch": env.branch,
    }))?;
    match state["state"].as_str() {
        Some("NOTES_IN") => {
            let st = verb(json!({
                "op": "confirm_notes", "tenant_id": env.tenant,
                "asset_hash": master, "branch": env.branch, "actor": "human",
            }))?;
            println!("confirm_notes: state={}", st["state"]);
        }
        Some("CONFORMING") => println!("already CONFORMING"),
        other => bail!("unexpected review state {other:?} before conform"),
    }
    Ok(())
}

// ── conform ─────────────────────────────────────────────────────────────────

fn conform(env: &Env) -> Result<()> {
    let input_rel = env
        .fixture
        .strip_prefix(&env.media_root)
        .map_err(|_| {
            anyhow!(
                "fixture {} is outside CYAN_MEDIA_ROOT {} — cyan-media confines inputs to its root",
                env.fixture.display(),
                env.media_root.display()
            )
        })?
        .to_string_lossy()
        .to_string();
    let command = match std::env::var("CYAN_MEDIA_MCP_CMD") {
        Ok(cmd) => cmd.split_whitespace().map(str::to_string).collect(),
        Err(_) => vec![
            "uv".to_string(),
            "run".to_string(),
            "--project".to_string(),
            env.media_dir.display().to_string(),
            "cyan-media-mcp".to_string(),
        ],
    };
    let dispatch = StdioConformDispatch {
        command,
        media_root: env.media_root.clone(),
        input_rel,
        timeout: Duration::from_secs(600),
    };

    // Global variant: the render runs with the store lock RELEASED.
    let outcome = review_loop::conform_proxy_global(&env.tenant, &env.file_id, None, &dispatch)?;
    let out_abs = env.media_root.join(&outcome.output_path);
    if !out_abs.is_file() {
        bail!(
            "conform reported {} but no file exists at {}",
            outcome.output_path,
            out_abs.display()
        );
    }
    println!(
        "conform: ops={} output={} new_proxy={} version={} needs_manual={} asks={}",
        outcome.sent_ops.len(),
        out_abs.display(),
        outcome.new_proxy_hash,
        outcome.new_version_id,
        outcome.needs_manual.len(),
        outcome.escalated_asks.len(),
    );
    println!("review state: {:?}", outcome.state.map(|s| s.state));
    Ok(())
}
