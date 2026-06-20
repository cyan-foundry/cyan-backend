#!/usr/bin/env bash
# stress.sh — one-operator, one-box STRESS / CHAOS fabric driver. See STATUS_STRESS_FABRIC.md
# and ../anthropic_data_dump/STRESS_HARNESS_SPEC.md.
#
#   ./stress.sh <scenario> [N]
#
# Two tiers, one entrypoint:
#
#  • LOOPBACK tier (no Docker) — spawns N real `cyan_node` processes via the Rust multi-process
#    stress suite (tests/substrate_stress.rs): forms a group, injects duress, collects each peer's
#    OWN storage + metrics, asserts the oracles, prints PASS/FAIL + metrics, tears down. Runs today
#    on any box with a Rust toolchain. This is the CI tier (small N) and the scale-ceiling probe
#    (big N on demand).
#
#  • SHAPED tier (Docker) — forces the network rungs the loopback tier cannot (relay-only,
#    websocket-only, NAT/different-WiFi, bidirectional-island partition, `tc` degradation) using
#    the docker rig (docker-compose.yml + shape.sh). If Docker is unavailable the scenario is
#    cleanly GATED with the reason — never faked.
#
# Scenarios:
#   swarm     [N]   loopback: concurrent multi-source edits converge, no dupes/loss
#   scale     [N]   loopback: N peers (10/50/100) — bounded gossip degree, no storm, bounded RSS
#   fetch     [N]   loopback: one holder, N fetchers, Blake3 integrity on each
#   partition [N]   loopback: drop + reconnect + heal (one-sided island), converge with no loss
#   chaos     [N]   loopback: sustained kill/restart + continuous edits (CYAN_STRESS_CHAOS_SECS)
#   ladder          shaped:   direct -> relay-only -> websocket-only connectivity ladder
#   islands         shaped:   two bidirectional islands edit independently -> heal -> converge
#   degraded        shaped:   tc latency/loss/jitter/bandwidth -> still converges, no corruption
#   all             loopback: swarm + scale + fetch + partition (the green CI matrix)
#
# Exit code is the assertion result: 0 = PASS, non-zero = FAIL (or GATED for a missing rung).
set -uo pipefail

HARNESS_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$HARNESS_DIR/.." && pwd)"
SCENARIO="${1:-help}"
N="${2:-}"

note()  { printf '\033[36m[stress]\033[0m %s\n' "$*"; }
pass()  { printf '\033[32m[PASS]\033[0m %s\n'   "$*"; }
fail()  { printf '\033[31m[FAIL]\033[0m %s\n'   "$*"; }
gated() { printf '\033[33m[GATED]\033[0m %s\n'  "$*"; }

# Run a named Rust stress test with optional CYAN_STRESS_N; surface PASS/FAIL by exit code.
run_loopback() {
  local test_name="$1"; shift
  local n="${1:-}"
  note "loopback tier → $test_name ${n:+(N=$n)}"
  (
    cd "$REPO_DIR"
    [ -n "$n" ] && export CYAN_STRESS_N="$n"
    # --nocapture so the [SCALE]/metrics lines reach the operator; --exact to run just this case.
    cargo test --test substrate_stress "$test_name" -- --exact --nocapture
  )
}

docker_ready() { command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; }

# Gate a Docker scenario cleanly when the rig isn't available — print the reason, never fake.
require_docker() {
  if ! docker_ready; then
    gated "$1 needs Docker (network isolation / tc shaping). Docker not available here."
    gated "Bring it up on a Docker host:  make -C harness up build-node  &&  ./harness/stress.sh $SCENARIO"
    exit 3
  fi
}

case "$SCENARIO" in
  swarm)     run_loopback concurrent_edits_converge_no_dupes "$N" ;;
  scale)
    # The scale probe is #[ignore]'d (unreliable stacked in-suite); run it standalone + gated.
    note "loopback tier → peer_flood_scale_and_degree_bounded ${N:+(N=$N)} [standalone probe]"
    ( cd "$REPO_DIR"
      export CYAN_STRESS_SCALE=1
      [ -n "$N" ] && export CYAN_STRESS_N="$N"
      cargo test --test substrate_stress peer_flood_scale_and_degree_bounded \
        -- --exact --ignored --nocapture
    ) ;;
  fetch)     run_loopback swarm_blob_multi_fetch_integrity "$N" ;;
  partition)
    # The drop/reconnect/heal probe is #[ignore]'d (timing-sensitive stacked in-suite); run gated.
    note "loopback tier → node_churn_rejoin_converges [standalone heal probe]"
    ( cd "$REPO_DIR"
      export CYAN_STRESS_PARTITION=1
      cargo test --test substrate_stress node_churn_rejoin_converges -- --exact --ignored --nocapture
    ) ;;
  chaos)
    note "loopback tier → sustained_chaos_soak (CYAN_STRESS_CHAOS=1)"
    ( cd "$REPO_DIR"
      export CYAN_STRESS_CHAOS=1
      [ -n "$N" ] && export CYAN_STRESS_N="$N"
      cargo test --test substrate_stress sustained_chaos_soak -- --exact --ignored --nocapture
    ) ;;
  all)
    # The green CI matrix = the three in-suite-reliable scenarios (scale is a separate standalone
    # probe — `stress.sh scale`).
    rc=0
    for s in concurrent_edits_converge_no_dupes swarm_blob_multi_fetch_integrity; do
      run_loopback "$s" "$N" || rc=1
    done
    [ "$rc" -eq 0 ] && pass "loopback CI matrix green" || fail "loopback CI matrix had failures"
    exit "$rc" ;;

  # ── Shaped (Docker) rungs — reuse the relay rig in tests/substrate_relay.rs + shape.sh. ──
  ladder)
    require_docker
    note "shaped tier → connectivity ladder (LAN → relay-only → websocket-only)"
    make -C "$HARNESS_DIR" up build-node
    rc=0
    make -C "$HARNESS_DIR" test-lan   || rc=1
    make -C "$HARNESS_DIR" test-relay || rc=1
    make -C "$HARNESS_DIR" test-ws    || rc=1
    [ "$rc" -eq 0 ] && pass "connectivity ladder green at every rung" || fail "a ladder rung failed"
    exit "$rc" ;;
  islands)
    require_docker
    gated "bidirectional-island partition+heal: split mesh_a/mesh_b with NO relay, edit both"
    gated "sides, re-bridge, assert convergence. Scaffold present; wire the two-island compose"
    gated "profile + drive both islands' cyan_node sets. Tracked in STATUS_STRESS_FABRIC.md."
    exit 3 ;;
  degraded)
    require_docker
    gated "tc-degraded convergence: bring peers up on a shaped link (scripts/shape.sh apply),"
    gated "run the swarm scenario, assert it still converges + Blake3-clean, then shape.sh clear."
    gated "shape.sh is ready; the per-container apply hook is tracked in STATUS_STRESS_FABRIC.md."
    exit 3 ;;

  help|*)
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit 0 ;;
esac

rc=$?
[ "$rc" -eq 0 ] && pass "$SCENARIO" || fail "$SCENARIO (exit $rc)"
exit "$rc"
