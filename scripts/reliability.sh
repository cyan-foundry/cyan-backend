#!/usr/bin/env bash
#
# reliability.sh — stress the green substrate test binaries by re-running each one
# N times, failing on the FIRST red, and printing a pass/fail tally per binary.
#
# This proves the in-process substrate suite is not flaky and survives repetition.
# It is strictly bounded (a fixed N, no infinite loops) and additive (runs the
# existing test binaries; touches no source).
#
#   Usage:   ./scripts/reliability.sh
#            RELIABILITY_N=5 ./scripts/reliability.sh      # fewer iterations
#
# Exit code 0 iff every binary passed all N iterations; non-zero on the first red.

set -u  # (intentionally NOT -e: we handle test failures explicitly to print tallies)

N="${RELIABILITY_N:-20}"

# The green, in-process substrate binaries (per STATUS.md / OVERNIGHT_RUN PHASE 0).
# Ignored tests inside each binary stay ignored — we only stress what is green.
BINARIES=(
  substrate_discovery
  substrate_sync
  substrate_chat
  substrate_files
  substrate_offline
)

cd "$(dirname "$0")/.." || exit 2

echo "=== substrate reliability run: N=${N} per binary ==="
echo "binaries: ${BINARIES[*]}"
echo

# Build once up front so per-iteration timing reflects test execution, not compilation,
# and so a compile error fails fast before the loop.
echo "--- building test binaries (once) ---"
if ! cargo build --tests 2>&1 | tail -3; then
  echo "BUILD FAILED — aborting reliability run" >&2
  exit 2
fi
echo

overall_rc=0
for bin in "${BINARIES[@]}"; do
  echo "--- ${bin}: ${N} iterations ---"
  pass=0
  for i in $(seq 1 "$N"); do
    if cargo test --test "$bin" --quiet >/tmp/reliability_${bin}.log 2>&1; then
      pass=$((pass + 1))
      printf "  [%2d/%2d] PASS\n" "$i" "$N"
    else
      printf "  [%2d/%2d] FAIL  <-- first red; tally for %s: %d/%d passed before failure\n" \
        "$i" "$N" "$bin" "$pass" "$N"
      echo "  ---- last 40 lines of /tmp/reliability_${bin}.log ----"
      tail -40 "/tmp/reliability_${bin}.log"
      echo "  ------------------------------------------------------"
      overall_rc=1
      break
    fi
  done
  if [ "$overall_rc" -ne 0 ]; then
    echo "STOPPING on first red in ${bin}." >&2
    break
  fi
  echo "  ${bin}: ${pass}/${N} passed"
  echo
done

echo "=== reliability summary ==="
if [ "$overall_rc" -eq 0 ]; then
  echo "ALL GREEN — every binary passed ${N}/${N} iterations."
else
  echo "RED — a binary failed; see the tally above."
fi
exit "$overall_rc"
