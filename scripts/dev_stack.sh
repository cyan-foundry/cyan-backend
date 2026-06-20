#!/usr/bin/env bash
#
# dev_stack.sh — ONE command to boot the local Cyan stack for the Round-5 crux.
#
# cyan-backend is the HUB: FFI to the app, mesh to peers, HTTP to lens. This boots
# the pieces the LIVE crux smoke needs and prints exactly how to drive them:
#
#   1. lens infra        — `make -C $CYAN_LENS_DIR up` (postgres + iggy +
#                          vllm-stub:8000 + sample mcp-plugin:8077), per the lens
#                          repo's runbook. We RUN the lens repo; we never edit it.
#   2. compile vLLM stub — scripts/crux_vllm_stub.py on a free port, exported as
#                          CYAN_VLLM_URL. (The lens e2e vllm-stub returns
#                          enrichment/query JSON, NOT the pipeline-config ARRAY the
#                          backend compile path expects — a real shape mismatch, so
#                          the crux compile points here. See STATUS_DEV_STACK.md.)
#   3. cyan_node peer    — the test/host backend bin; prints its node id + the
#                          LENS_URL it targets.
#
# Ctrl-C / exit tears everything down cleanly.
#
# Usage:
#   scripts/dev_stack.sh                 # infra + stub + a backend peer
#   LENS_SERVER=1 scripts/dev_stack.sh   # ALSO `cargo run` the lens HTTP API (8080)
#   CYAN_LENS_DIR=/path scripts/dev_stack.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CYAN_LENS_DIR="${CYAN_LENS_DIR:-$HOME/cyan-lens}"
STATE="$(mktemp -d -t cyan-dev-stack.XXXXXX)"
PIDS=()
LENS_UP=0
NODE_FIFO=""

log()  { printf '\033[36m[dev-stack]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[dev-stack]\033[0m %s\n' "$*"; }
err()  { printf '\033[31m[dev-stack]\033[0m %s\n' "$*" >&2; }

cleanup() {
  echo
  log "tearing down…"
  # Close the cyan_node stdin fifo (the node exits its read loop on EOF).
  exec 3>&- 2>/dev/null || true
  for pid in "${PIDS[@]:-}"; do
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null || true
  done
  if [ "$LENS_UP" = "1" ]; then
    log "stopping lens infra (make down)…"
    make -C "$CYAN_LENS_DIR" down >/dev/null 2>&1 || true
  fi
  [ -n "$NODE_FIFO" ] && rm -f "$NODE_FIFO" 2>/dev/null || true
  rm -rf "$STATE" 2>/dev/null || true
  log "done."
}
trap cleanup EXIT INT TERM

# ── 1. lens infra ────────────────────────────────────────────────────────────
if [ ! -d "$CYAN_LENS_DIR" ]; then
  warn "lens repo not found at $CYAN_LENS_DIR — skipping lens infra."
  warn "  HANDOFF: clone cyan-lens there and run 'make up' (per its STATUS_LENS_CUTOVER / Makefile)."
  warn "  The crux compile only needs the vLLM stub below; lens infra is for the full stack."
elif ! command -v docker >/dev/null 2>&1; then
  warn "docker not found — skipping lens infra (start Docker Desktop / colima to enable it)."
else
  log "starting lens infra: make -C $CYAN_LENS_DIR up"
  if make -C "$CYAN_LENS_DIR" up; then
    LENS_UP=1
    log "lens infra up: postgres=5432 iggy=8090 vllm-stub=8000 mcp-plugin=8077"
  else
    warn "lens infra failed to start (continuing — the crux compile uses the stub below)."
  fi
fi

# ── optional: the lens HTTP API server (axum, :8080) ──────────────────────────
LENS_URL="${LENS_URL:-http://127.0.0.1:8080}"
if [ "${LENS_SERVER:-0}" = "1" ] && [ "$LENS_UP" = "1" ]; then
  log "starting lens HTTP API (cargo run in $CYAN_LENS_DIR; first build is slow)…"
  ( cd "$CYAN_LENS_DIR" && API_ADDR=0.0.0.0:8080 cargo run --quiet ) \
    >"$STATE/lens.log" 2>&1 &
  PIDS+=("$!")
  log "lens API launching → $LENS_URL (tail: $STATE/lens.log)"
else
  log "lens HTTP API not started (set LENS_SERVER=1 to run it). The crux smoke does"
  log "  not need it — compile uses the vLLM stub; the run step is the LOCAL MCP host."
fi

# ── 2. compile-aware vLLM stub ────────────────────────────────────────────────
PY="$(command -v python3 || true)"
if [ -z "$PY" ]; then
  err "python3 not found — needed for the compile vLLM stub. Install python3 and re-run."
  exit 1
fi
VLLM_PORT="$( "$PY" -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()' )"
"$PY" "$REPO_ROOT/scripts/crux_vllm_stub.py" "$VLLM_PORT" >"$STATE/vllm_stub.log" 2>&1 &
PIDS+=("$!")
export CYAN_VLLM_URL="http://127.0.0.1:$VLLM_PORT"
# bounded readiness wait
for _ in $(seq 1 50); do
  "$PY" - "$VLLM_PORT" <<'PYEOF' && break || sleep 0.2
import socket, sys
s = socket.socket()
s.settimeout(0.3)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); print("ok")
except OSError:
    sys.exit(1)
PYEOF
done
log "compile vLLM stub up → CYAN_VLLM_URL=$CYAN_VLLM_URL"

# ── 3. a cyan_node backend peer ───────────────────────────────────────────────
log "building cyan_node…"
( cd "$REPO_ROOT" && cargo build --quiet --bin cyan_node )

NODE_DB="$STATE/node.db"
NODE_DATA="$STATE/node-data"
NODE_LOG="$STATE/node.log"
mkdir -p "$NODE_DATA"
NODE_FIFO="$STATE/node.stdin"
mkfifo "$NODE_FIFO"

log "starting cyan_node peer…"
( cd "$REPO_ROOT" && \
  NODE_DB="$NODE_DB" DATA_DIR="$NODE_DATA" RELAY=disabled DISCOVERY_KEY=cyan-dev \
  ./target/debug/cyan_node <"$NODE_FIFO" >"$NODE_LOG" 2>"$STATE/node.err" ) &
PIDS+=("$!")
# Hold the fifo open so the node's stdin never EOFs (fd 3 closed on teardown).
exec 3>"$NODE_FIFO"
echo "node_id" >&3

NODE_ID=""
for _ in $(seq 1 50); do
  # `|| true`: under `set -e`, a no-match grep (exit 1) must not abort the poll.
  NODE_ID="$(grep -m1 '@@CYAN@@ node_id' "$NODE_LOG" 2>/dev/null | awk '{print $3}' || true)"
  [ -n "$NODE_ID" ] && break
  sleep 0.2
done
[ -z "$NODE_ID" ] && NODE_ID="(unavailable — see $STATE/node.err)"

# ── summary + how to drive it ─────────────────────────────────────────────────
cat <<EOF

════════════════════════ Cyan dev stack is up ════════════════════════
  lens infra      : $( [ "$LENS_UP" = 1 ] && echo 'up (pg/iggy/vllm-stub/mcp-plugin)' || echo 'not started' )
  lens HTTP API   : $( [ "${LENS_SERVER:-0}" = 1 ] && [ "$LENS_UP" = 1 ] && echo "$LENS_URL" || echo 'not started (LENS_SERVER=1 to run)' )
  compile vLLM    : $CYAN_VLLM_URL
  backend peer    : cyan_node  node_id=$NODE_ID
                    NODE_DB=$NODE_DB  RELAY=disabled  DISCOVERY_KEY=cyan-dev
  LENS_URL target : $LENS_URL   (backend default CYAN_LENS_URL is :9080 — set it if you run the lens API)

  ── run the LIVE crux smoke (a real compile + a real local MCP-tool step) ──
     CRUX_REAL=1 CYAN_VLLM_URL=$CYAN_VLLM_URL \\
       cargo test --test crux_smoke -- --nocapture

  ── launch the iOS app against this peer ──
     The app drives the engine over FFI in-process (it embeds the static lib), so
     it does not connect to cyan_node over a socket. To exercise the SAME backend↔
     lens HTTP path the crux proves, launch the app with:
       CYAN_VLLM_URL=$CYAN_VLLM_URL CYAN_LENS_URL=$LENS_URL  open the Xcode scheme
     (set these in the scheme's Run → Environment, or your shell before `open`),
     then: create a Group → Workspace → notebook Board, write a step, and run
     /pipeline compile then /pipeline run.

  Logs: $STATE/   (vllm_stub.log, node.log, node.err$( [ "${LENS_SERVER:-0}" = 1 ] && echo ', lens.log' ))
  Ctrl-C to tear everything down.
══════════════════════════════════════════════════════════════════════
EOF

# Keep running until interrupted.
while true; do sleep 3600; done
