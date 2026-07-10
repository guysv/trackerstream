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

# Build-time version stamp for the libp2p UserAgent + RPC /version (node/config.go Version).
# --always guarantees output (falls back to an abbreviated commit hash); "dev" only if git is
# unavailable. Overridable: VERSION=... bash deploy/build-tsnode.sh
VERSION="${VERSION:-$(git -C "$ROOT" describe --tags --always 2>/dev/null || echo dev)}"
LDFLAGS="-s -w -X github.com/trackerstream/tsnode.Version=$VERSION"

# OS/arch triples → output filename. The server uses linux-amd64; the rest are the desktop
# sidecar matrix (Tauri externalBin renames to <name>-<rust-triple> at bundle time).
TARGETS="${TARGETS:-linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64}"

# Binaries built per target: tsnode (the node) + tsmon (the seed-monitor TUI). tsmon is a
# read-only dashboard over the node's loopback RPC — scp the linux-amd64 one next to tsnode on
# the seed box, or run it locally through an ssh -L tunnel. Set CMDS to build a subset.
CMDS="${CMDS:-tsnode tsmon}"

for t in $TARGETS; do
  goos="${t%/*}"; goarch="${t#*/}"
  ext=""; [ "$goos" = "windows" ] && ext=".exe"
  for cmd in $CMDS; do
    out="$OUT/${cmd}-${goos}-${goarch}${ext}"
    echo "building $out"
    ( cd "$NODE_DIR" && CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
        go build -trimpath -ldflags "$LDFLAGS" -o "$out" "./cmd/${cmd}" )
  done
done

echo "done:"
ls -lh "$OUT"/tsnode-* "$OUT"/tsmon-* 2>/dev/null | awk '{print "  " $5, $9}'
