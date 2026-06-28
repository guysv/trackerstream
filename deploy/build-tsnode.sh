#!/usr/bin/env bash
#
# Cross-compile the tsnode binary for the deploy targets and drop them in deploy/dist/.
# The server cutover needs linux-amd64; the desktop sidecar (Phase C) builds the per-OS
# triples. Run from the repo root or anywhere — paths are resolved from this script.
#
#   bash deploy/build-tsnode.sh                 # all targets
#   TARGETS="linux/amd64" bash deploy/build-tsnode.sh   # just the server
#
# Stripped (-s -w) for size; tsnode links only the boxo components we use (~28MB).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NODE_DIR="$ROOT/node"
OUT="$ROOT/deploy/dist"
mkdir -p "$OUT"

# OS/arch triples → output filename. The server uses linux-amd64; the rest are the desktop
# sidecar matrix (Tauri externalBin renames to <name>-<rust-triple> at bundle time).
TARGETS="${TARGETS:-linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64}"

for t in $TARGETS; do
  goos="${t%/*}"; goarch="${t#*/}"
  ext=""; [ "$goos" = "windows" ] && ext=".exe"
  out="$OUT/tsnode-${goos}-${goarch}${ext}"
  echo "building $out"
  ( cd "$NODE_DIR" && CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
      go build -trimpath -ldflags "-s -w" -o "$out" ./cmd/tsnode )
done

echo "done:"
ls -lh "$OUT"/tsnode-* | awk '{print "  " $5, $9}'
