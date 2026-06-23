#!/usr/bin/env bash
# verify_merge.sh — confirm the three merged agents all work after merging
#   feat/offline-startup-fix + feat/chat-attachment + feat/docker-rig into feat/substrate-e2e.
#
# Run from the repo root:  bash verify_merge.sh
#   RELIABILITY=1 bash verify_merge.sh   # also run the 20x stress loop (slow)
#
# Notes:
# - We run the substrate suite PER TEST BINARY (cargo test --test X), NOT a bare
#   `cargo test`, on purpose: a bare run executes the lib unit tests first and
#   fail-fasts on the pre-existing, unrelated `diagram_gen` failure. Per-binary
#   skips the lib unit tests entirely.
# - The docker rig (Agent 3) needs Docker running; if it isn't, that step is
#   SKIPPED (not failed) with a clear note.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2   # repo root: harness/ and scripts/ are resolved from here

PASS=(); FAIL=(); SKIP=()
step() {                      # step "Label" cmd...
  local label="$1"; shift
  echo; echo "──────────────────────────────────────────────────────────────"
  echo "▶  $label"
  echo "   \$ $*"
  if "$@"; then PASS+=("$label"); echo "✅ PASS — $label"
  else FAIL+=("$label"); echo "❌ FAIL — $label"; fi
}

echo "branch: $(git rev-parse --abbrev-ref HEAD)   (expected: feat/substrate-e2e)"

# 0 — it all compiles
step "build (lib + bins + tests)" cargo build --tests

# 1 — Agent 1: offline-startup fix. The whole resilience binary (contains the
#     now-un-ignored node_with_group_offline_startup_does_not_block regression guard).
step "Agent 1 — offline-startup fix (tests/substrate_resilience.rs)" \
  cargo test --test substrate_resilience -- --nocapture

# 2 — Agent 2: chat-attachment / G7 (contains chat_with_attachment_shares_file_into_scope).
step "Agent 2 — chat-attachment / G7 (tests/substrate_chat.rs)" \
  cargo test --test substrate_chat -- --nocapture

# 3 — Regression: the rest of the in-process substrate suite must stay green.
for t in substrate_discovery substrate_sync substrate_files substrate_offline substrate_snapshot_mp; do
  step "Regression — $t" cargo test --test "$t"
done

# 3b — optional reliability stress loop
if [ "${RELIABILITY:-0}" = "1" ] && [ -x scripts/reliability.sh ]; then
  step "Reliability — 20x stress loop" bash scripts/reliability.sh
fi

# 4 — Agent 3: docker rig relay/WebSocket rungs (needs Docker).
if docker info >/dev/null 2>&1; then
  echo; echo "▶  Agent 3 — docker rig (builds relay + cyan_node images, ~minutes the first time)"
  step "Agent 3 — relay rig: LAN + relay-only + WebSocket-only rungs" make -C harness test-all
  make -C harness clean >/dev/null 2>&1 || true
else
  SKIP+=("Agent 3 — docker rig (Docker not running)")
  echo; echo "⚠️  SKIP — Docker is not running. Start Docker Desktop, then:  make -C harness test-all"
fi

# ── summary ───────────────────────────────────────────────────────────────────
echo; echo "════════════════════════════ SUMMARY ════════════════════════════"
for x in "${PASS[@]:-}"; do [ -n "$x" ] && echo "  ✅ $x"; done
for x in "${SKIP[@]:-}"; do [ -n "$x" ] && echo "  ⚠️  $x"; done
for x in "${FAIL[@]:-}"; do [ -n "$x" ] && echo "  ❌ $x"; done
echo "──────────────────────────────────────────────────────────────"
if [ "${#FAIL[@]}" -eq 0 ]; then
  echo "🎉 ALL GREEN — the three agents are good on the merged tree."
  [ "${#SKIP[@]}" -ne 0 ] && echo "   (one or more steps skipped — see ⚠️ above)"
  exit 0
else
  echo "🚨 ${#FAIL[@]} step(s) FAILED — do not merge to main yet; inspect the ❌ above."
  exit 1
fi
