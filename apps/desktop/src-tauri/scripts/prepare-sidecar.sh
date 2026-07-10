#!/usr/bin/env bash
#
# Build the tsnode sidecar for the CURRENT host's Rust target triple and place it where
# Tauri's `bundle.externalBin: ["binaries/tsnode"]` expects it:
#     src-tauri/binaries/tsnode-<rust-target-triple>[.exe]
# Tauri picks the file matching the build/run target and renames it to plain `tsnode` inside
# the app. Replaces the old prepare-ipfs.sh (which vendored+patched rust-ipfs); the in-process
# node is gone, the node is now this external Go binary.
#
# Idempotent: a stamp newer than node/ source skips the rebuild (so `tauri dev` is fast).
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"          # …/src-tauri
ROOT="$(cd "$HERE/../../.." && pwd)"               # repo root
NODE_DIR="$ROOT/node"
OUT_DIR="$HERE/binaries"
mkdir -p "$OUT_DIR"

# Host Rust triple (what Tauri appends). Fall back to uname if rustc is unavailable.
TRIPLE="${TS_TARGET_TRIPLE:-$(rustc -Vv 2>/dev/null | sed -n 's/^host: //p')}"
if [ -z "$TRIPLE" ]; then
  echo "could not determine the Rust host triple (set TS_TARGET_TRIPLE)" >&2; exit 1
fi

# Rust triple → Go GOOS/GOARCH.
case "$TRIPLE" in
  x86_64-apple-darwin)       GOOS=darwin  GOARCH=amd64 ;;
  aarch64-apple-darwin)      GOOS=darwin  GOARCH=arm64 ;;
  x86_64-unknown-linux-gnu)  GOOS=linux   GOARCH=amd64 ;;
  aarch64-unknown-linux-gnu) GOOS=linux   GOARCH=arm64 ;;
  x86_64-pc-windows-msvc)    GOOS=windows GOARCH=amd64 ;;
  *) echo "unmapped target triple: $TRIPLE (add it to prepare-sidecar.sh)" >&2; exit 1 ;;
esac

EXT=""; [ "$GOOS" = "windows" ] && EXT=".exe"
OUT="$OUT_DIR/tsnode-$TRIPLE$EXT"
STAMP="$OUT_DIR/.tsnode-$TRIPLE.stamp"

# Freshness guard: skip if the binary + stamp are newer than every Go source file.
if [ -f "$OUT" ] && [ -f "$STAMP" ] && [ -z "$(find "$NODE_DIR" -name '*.go' -newer "$STAMP" -print -quit)" ]; then
  echo "sidecar already built for $TRIPLE — skipping"
  exit 0
fi

echo "building tsnode → $OUT ($GOOS/$GOARCH)"
# Stamp the version (node/config.go Version) for the UserAgent + RPC /version; --always
# falls back to a commit hash, "dev" only if git is unavailable.
VERSION="${VERSION:-$(git -C "$ROOT" describe --tags --always 2>/dev/null || echo dev)}"
( cd "$NODE_DIR" && CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" \
    go build -trimpath -ldflags "-s -w -X github.com/trackerstream/tsnode.Version=$VERSION" -o "$OUT" ./cmd/tsnode )
chmod +x "$OUT"
touch "$STAMP"
echo "sidecar ready: $OUT"

# In dev, the app spawns the externalBin copy Tauri drops next to the dev executable
# (target/<profile>/tsnode), not $OUT. Tauri only re-copies during a Rust rebuild, so a
# Go-only change would rebuild $OUT but leave the spawned copy stale. Refresh it here —
# atomically (temp + mv) so a currently-running node keeps its open binary and only the
# NEXT spawn picks up the new one. Skip when the target dir doesn't exist yet.
TARGET_DIR="$HERE/target"
for profile in debug release; do
  dst="$TARGET_DIR/$profile/tsnode$EXT"
  [ -d "$TARGET_DIR/$profile" ] || continue
  cp "$OUT" "$dst.new" && chmod +x "$dst.new" && mv -f "$dst.new" "$dst"
  echo "refreshed dev spawn copy: $dst"
done
