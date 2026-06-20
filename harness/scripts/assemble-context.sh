#!/usr/bin/env bash
# Assemble a MINIMAL Docker build context for the cyan_node image.
#
# cyan_node links cyan-backend, which has three sibling path-deps:
#   ../xaeroai  ../xaeroid  ../cyan-backend-integrations
# The sibling dirs on disk are huge (xaeroai alone is ~25G with models/target). We copy
# ONLY what cargo needs to build (Cargo.toml, Cargo.lock, src/, tests/) into harness/.ctx,
# and crucially lowercase `xaeroID` -> `xaeroid` so the case-sensitive Linux FS in the
# container resolves the `../xaeroid` path dep (it only works on macOS by accident of the
# case-insensitive APFS).
#
# Output: harness/.ctx/{cyan-backend,xaeroai,xaeroid,cyan-backend-integrations,ws-entrypoint.sh}
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO_DIR="$(cd "$HARNESS_DIR/.." && pwd)"
PARENT_DIR="$(cd "$REPO_DIR/.." && pwd)"
CTX="$HARNESS_DIR/.ctx"

rm -rf "$CTX"
mkdir -p "$CTX"

# Per-crate copy of just the build inputs. rsync with an allowlist keeps it tiny.
copy_crate() { # <src-dir> <dest-name>
  local src="$1" dest="$2"
  mkdir -p "$CTX/$dest"
  for item in Cargo.toml Cargo.lock src tests build.rs benches examples; do
    if [ -e "$src/$item" ]; then
      rsync -a --exclude target --exclude .git "$src/$item" "$CTX/$dest/"
    fi
  done
}

echo "[assemble] cyan-backend  <- $REPO_DIR"
copy_crate "$REPO_DIR" "cyan-backend"
# xaeroID on disk, but the path dep + package name are lowercase `xaeroid`.
echo "[assemble] xaeroid       <- $PARENT_DIR/xaeroID  (lowercased for Linux)"
copy_crate "$PARENT_DIR/xaeroID" "xaeroid"
echo "[assemble] integrations  <- $PARENT_DIR/cyan-backend-integrations"
copy_crate "$PARENT_DIR/cyan-backend-integrations" "cyan-backend-integrations"
# Sibling path-deps the engine grew (cyan-backend/Cargo.toml: `../cyan-mcp`, `../cyan-identity`).
# Both are lowercase on disk and have no further path-deps, so a flat copy next to cyan-backend
# resolves them. (Without these the Linux build fails at `failed to read ../cyan-identity`.)
echo "[assemble] cyan-mcp      <- $PARENT_DIR/cyan-mcp"
copy_crate "$PARENT_DIR/cyan-mcp" "cyan-mcp"
echo "[assemble] cyan-identity <- $PARENT_DIR/cyan-identity"
copy_crate "$PARENT_DIR/cyan-identity" "cyan-identity"

cp "$HARNESS_DIR/scripts/ws-entrypoint.sh" "$CTX/ws-entrypoint.sh"

echo "[assemble] context ready at $CTX"
du -sh "$CTX" 2>/dev/null || true
