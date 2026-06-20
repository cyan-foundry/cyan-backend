#!/bin/sh
# shape.sh — per-link network degradation via `tc netem` (latency / jitter / loss / bandwidth).
#
# The stress fabric's "degraded network" knob: apply controlled impairment to a container's
# egress interface so the convergence/integrity oracles run over a realistically bad link. Used
# from inside a container that has `--cap-add NET_ADMIN` (the same cap the WebSocket rung needs),
# or on the host against a veth. Idempotent: `clear` removes any rule first.
#
# Usage:
#   shape.sh apply [iface] [delay_ms] [jitter_ms] [loss_pct] [rate]
#   shape.sh clear [iface]
#   shape.sh show  [iface]
#
# Defaults model a poor mobile link: 120ms ± 30ms, 3% loss, 5mbit. All args optional.
set -e

CMD="${1:-apply}"
IFACE="${2:-eth0}"
DELAY="${3:-120}"
JITTER="${4:-30}"
LOSS="${5:-3}"
RATE="${6:-5mbit}"

case "$CMD" in
  apply)
    # Remove any prior qdisc (ignore "no such file" on a clean iface), then add netem.
    tc qdisc del dev "$IFACE" root 2>/dev/null || true
    tc qdisc add dev "$IFACE" root netem \
        delay "${DELAY}ms" "${JITTER}ms" distribution normal \
        loss "${LOSS}%" \
        rate "$RATE"
    echo "[shape] $IFACE: delay=${DELAY}±${JITTER}ms loss=${LOSS}% rate=${RATE}" >&2
    ;;
  clear)
    tc qdisc del dev "$IFACE" root 2>/dev/null || true
    echo "[shape] $IFACE: cleared" >&2
    ;;
  show)
    tc qdisc show dev "$IFACE"
    ;;
  *)
    echo "usage: shape.sh {apply|clear|show} [iface] [delay_ms] [jitter_ms] [loss_pct] [rate]" >&2
    exit 2
    ;;
esac
