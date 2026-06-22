#!/usr/bin/env bash
# Assemble a MINIMAL Docker build context for the xaeroflux BOOTSTRAP image.
#
# The bootstrap is the discovery-rendezvous node (xaeroflux `src/bin/xaeroflux_bootstrap.rs`).
# xaeroflux is SELF-CONTAINED (no sibling path-deps — confirmed in its Cargo.toml), so unlike
# the cyan_node context we only copy the one crate's build inputs (Cargo.toml/lock, src/, tests/)
# into harness/.ctx-bootstrap/xaeroflux. xaeroflux itself is NEVER modified (per spec) — we only
# read it.
#
# Output: harness/.ctx-bootstrap/xaeroflux/{Cargo.toml,Cargo.lock,src,...}
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO_DIR="$(cd "$HARNESS_DIR/.." && pwd)"
PARENT_DIR="$(cd "$REPO_DIR/.." && pwd)"
XF_SRC="${XAEROFLUX_DIR:-$PARENT_DIR/xaeroflux}"
CTX="$HARNESS_DIR/.ctx-bootstrap"

if [ ! -f "$XF_SRC/src/bin/xaeroflux_bootstrap.rs" ]; then
  echo "[assemble-bootstrap] ERROR: xaeroflux not found at $XF_SRC (set XAEROFLUX_DIR)" >&2
  exit 1
fi

rm -rf "$CTX"
mkdir -p "$CTX/xaeroflux"

echo "[assemble-bootstrap] xaeroflux <- $XF_SRC"
for item in Cargo.toml Cargo.lock src tests build.rs; do
  if [ -e "$XF_SRC/$item" ]; then
    rsync -a --exclude target --exclude .git "$XF_SRC/$item" "$CTX/xaeroflux/"
  fi
done

echo "[assemble-bootstrap] context ready at $CTX"
du -sh "$CTX" 2>/dev/null || true
