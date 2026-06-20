#!/usr/bin/env bash
# live.sh — the FRIENDLY front door to the Cyan live-test rig (ROUND 8).
#
# ONE command, human toggles: spin up an N-account live test in seconds. Headless `cyan_node`
# peers SELF-IDENTIFY (auto-generated identity per peer from its own data dir) — NO login, NO
# SSO, NO real accounts — so `--peers` can be large; you are NOT limited by how many logins you
# have. Each peer acts and ALL peers must converge; we assert on each peer's OWN storage / OWN
# blob-verify (never on log lines), with bounded waits. Exit code is the verdict (0 = PASS).
#
# This is a thin front door over the EXISTING rig: the macos tier drives the multi-process
# orchestrator (tests/substrate_live.rs); the corp / docker tiers reuse the relay-WebSocket
# Docker rungs (Makefile / docker-compose.yml / ws-entrypoint.sh). It does not duplicate
# stress.sh — that is the chaos/scale driver; this is the friendly N-account live test.
#
# Usage:  ./live.sh [--peers N] [--mode macos|docker] [--net home|corp|offline]
#                   [--scenario sync|files|chat|workflow|all] [--keep] [--app N]
#
# Toggles (sane defaults):
#   --peers N      (8)      N headless peers, each its own data dir + auto identity (no login).
#   --mode M       (macos)  macos = N native cyan_node processes on this Mac (real LAN/loopback);
#                           docker = the isolation-network rig (full network control).
#   --net N        (home)   home    = direct + hole-punch allowed (normal path).
#                           corp    = SIMULATE a corporate firewall: block direct UDP/QUIC so peers
#                                     MUST fall back to relay / WebSocket (rigorous via Docker).
#                           offline = no internet / no relay / no bootstrap: mDNS-LAN only.
#   --scenario S   (all)    sync | files | chat | workflow | all  — behaviors to exercise + ASSERT.
#   --keep                  leave peers/relay running for manual poking (Docker tier; see notes).
#   --app N                 ALSO launch up to N real Cyan app instances for a manual UX pass
#                           (separate from the asserted headless run; capped to available logins).
#   --help                  this text.
#
# What each scenario ASSERTS (on every peer's own storage):
#   sync     — each peer creates whiteboard objects → every peer's element count == the exact union.
#   chat     — each peer sends board chat → every peer's chat count == the exact union (dedupe by id).
#   files    — each peer uploads a blob → every peer fetches + blake3-verifies every other peer's blob.
#   workflow — one peer authors+lays out a local-placement workflow → every peer sees the steps
#              (cells) and the pinned gate (pins). Execution/placement is local/MCP, out of scope.
set -uo pipefail

HARNESS_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$HARNESS_DIR/.." && pwd)"

PEERS=8
MODE=macos
NET=home
SCENARIO=all
KEEP=0
APP=0

note()  { printf '\033[36m[live]\033[0m %s\n'   "$*"; }
pass()  { printf '\033[32m[PASS]\033[0m %s\n'   "$*"; }
fail()  { printf '\033[31m[FAIL]\033[0m %s\n'   "$*"; }
gated() { printf '\033[33m[GATED]\033[0m %s\n'  "$*"; }

usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; }

# ── Parse toggles ─────────────────────────────────────────────────────────────────────────
while [ $# -gt 0 ]; do
  case "$1" in
    --peers)    PEERS="${2:?--peers needs N}"; shift 2 ;;
    --mode)     MODE="${2:?--mode needs macos|docker}"; shift 2 ;;
    --net)      NET="${2:?--net needs home|corp|offline}"; shift 2 ;;
    --scenario) SCENARIO="${2:?--scenario needs sync|files|chat|workflow|all}"; shift 2 ;;
    --keep)     KEEP=1; shift ;;
    --app)      APP="${2:?--app needs N}"; shift 2 ;;
    --help|-h)  usage; exit 0 ;;
    *) fail "unknown flag '$1'"; echo; usage; exit 2 ;;
  esac
done

case "$MODE" in macos|docker) ;; *) fail "--mode must be macos|docker (got '$MODE')"; exit 2 ;; esac
case "$NET" in home|corp|offline) ;; *) fail "--net must be home|corp|offline (got '$NET')"; exit 2 ;; esac
case "$SCENARIO" in sync|files|chat|workflow|all) ;; *) fail "--scenario invalid (got '$SCENARIO')"; exit 2 ;; esac
if ! [ "$PEERS" -ge 2 ] 2>/dev/null; then fail "--peers must be an integer >= 2 (got '$PEERS')"; exit 2; fi

note "peers=$PEERS mode=$MODE net=$NET scenario=$SCENARIO keep=$KEEP app=$APP"

# Optional manual UX pass with REAL app logins — separate from the asserted headless run.
if [ "$APP" -gt 0 ] 2>/dev/null; then
  gated "--app $APP: real Cyan app instances drive a MANUAL UX pass with real logins — that is the"
  gated "separate app path (run_multi), capped to available logins, NOT the asserted headless run."
  gated "Headless peers below need no login; continuing with the asserted scenarios."
fi

docker_ready() { command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; }
require_docker() {
  docker_ready && return 0
  gated "$1 needs Docker (real UDP blocking / network isolation). Docker is not available here."
  gated "Bring it up on a Docker host, then re-run.  See harness/README.md."
  exit 3
}

# ── corp / docker tiers: reuse the proven relay-WebSocket Docker rungs ───────────────────────
# Blocking direct UDP per-process is not something the macos in-process tier can do honestly, so
# the corporate-firewall (relay/WebSocket fallback) proof is the Docker rig's job — the SAME rungs
# the substrate suite already greens (Makefile test-relay / test-ws, ws-entrypoint.sh). These are
# 2-peer topology proofs; the N-peer SCALE proof is the macos tier. We route, we don't fake.
run_docker_rungs() {
  local targets="$1" label="$2"
  require_docker "$label"
  note "docker tier → $label : make up build-node $targets"
  make -C "$HARNESS_DIR" up build-node || { fail "$label: rig bring-up failed"; exit 1; }
  local rc=0
  # shellcheck disable=SC2086
  for t in $targets; do
    note "docker tier → make $t"
    make -C "$HARNESS_DIR" "$t" || rc=1
  done
  if [ "$KEEP" -eq 0 ]; then make -C "$HARNESS_DIR" down >/dev/null 2>&1 || true
  else note "--keep: relay + networks left up (make -C harness clean to tear down)."; fi
  if [ "$rc" -eq 0 ]; then pass "$label green"; else fail "$label had failures"; fi
  return "$rc"
}

if [ "$NET" = "corp" ]; then
  # Corporate firewall: peers MUST fall back to relay, then relay-over-WebSocket when UDP is fully
  # dropped. Both rungs are the rigorous corp proof. (macos pf-based blocking is best-effort and
  # NOT wired — Docker is the rigorous path, by design; see STATUS_ROUND8_HARNESS.md.)
  [ "$MODE" = "macos" ] && note "corp sim uses the Docker rig regardless of --mode (rigorous UDP block)."
  run_docker_rungs "test-relay test-ws" "corp firewall (relay + WebSocket fallback)"
  exit $?
fi

if [ "$MODE" = "docker" ]; then
  # home/offline on Docker → the direct/LAN rung with relay disabled (the engine's offline proof).
  run_docker_rungs "test-lan" "docker LAN/offline (direct QUIC, relay disabled)"
  exit $?
fi

# ── macos tier: N native cyan_node peers, asserted per-peer (the friendly default) ──────────
[ "$KEEP" -eq 1 ] && note "--keep is a Docker-tier knob; the macos tier owns peers as test children (ephemeral)."

OUT="$(mktemp -t cyan-live.XXXXXX)"
trap 'rm -f "$OUT" "$OUT.err"' EXIT

note "booting $PEERS headless peers (auto identity, no login) → group → scenario '$SCENARIO'…"
( cd "$REPO_DIR"
  CYAN_LIVE=1 CYAN_LIVE_N="$PEERS" CYAN_LIVE_SCENARIO="$SCENARIO" CYAN_LIVE_NET="$NET" \
    cargo test --test substrate_live live_run -- --exact --nocapture
) 2>"$OUT.err" | tee "$OUT" | grep -E '^\[live\]|info ' >/dev/null || true

# ── Render the per-scenario, per-peer PASS/FAIL table from the machine lines ────────────────
echo
printf '  %-10s %-8s %-6s %s\n' "SCENARIO" "PEER" "RESULT" "DETAIL"
printf '  %-10s %-8s %-6s %s\n' "--------" "----" "------" "------"
fails=0
while IFS= read -r line; do
  s=$(sed -n 's/.*scenario=\([^ ]*\).*/\1/p' <<<"$line")
  p=$(sed -n 's/.*peer=\([^ ]*\).*/\1/p' <<<"$line")
  r=$(sed -n 's/.*result=\([^ ]*\).*/\1/p' <<<"$line")
  d=$(sed -n 's/.*detail=\([^ ]*\).*/\1/p' <<<"$line")
  if [ "$r" = "PASS" ]; then col=32; else col=31; fails=$((fails+1)); fi
  printf '  %-10s %-8s \033[%sm%-6s\033[0m %s\n' "$s" "$p" "$col" "$r" "$d"
done < <(grep -E '^@@LIVE@@ scenario=.* result=' "$OUT")

verdict=$(grep -E '^@@LIVE@@ verdict=' "$OUT" | tail -1)
echo
if grep -q 'verdict=PASS' <<<"$verdict" && [ "$fails" -eq 0 ]; then
  pass "all $PEERS peers converged on every scenario  (net=$NET, mode=macos)"
  exit 0
else
  fail "one or more peers failed to converge — see the table + $OUT.err"
  [ -z "$verdict" ] && fail "no verdict line: the run may have crashed (check $OUT.err)"
  exit 1
fi
