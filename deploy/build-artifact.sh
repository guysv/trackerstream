#!/usr/bin/env bash
#
# Build the deployable server artifact: a versioned tarball of the control plane
# (API + ingest + repack/config + the prebuilt libopenmpt WASM) plus the install
# script + systemd units. Excludes node_modules (installed on target) and the
# WASM build intermediates. Run from the repo root (or anywhere — it cd's).
#
#   bash deploy/build-artifact.sh            # -> deploy/dist/trackerstream-server-<ver>.tar.gz
set -euo pipefail
cd "$(dirname "$0")/.."
REPO="$(pwd)"

VERSION="${VERSION:-$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || date +%Y%m%d)}"
NAME="trackerstream-server-${VERSION}"
OUT="$REPO/deploy/dist"
STAGE="$(mktemp -d)/$NAME"
trap 'rm -rf "$(dirname "$STAGE")"' EXIT

# The prebuilt WASM must exist (server metadata extraction needs it).
if [ ! -f "$REPO/packages/wasm/dist/libopenmpt.js" ]; then
  echo "ERROR: packages/wasm/dist/libopenmpt.js missing — run 'pnpm --filter @trackerstream/wasm build' first." >&2
  exit 1
fi

mkdir -p "$STAGE"
# Root workspace files.
cp "$REPO/package.json" "$REPO/pnpm-workspace.yaml" "$STAGE/"
# Packages the server needs (config, repack, wasm dist only — not the build/).
mkdir -p "$STAGE/packages/wasm"
cp -R "$REPO/packages/config" "$STAGE/packages/config"
cp -R "$REPO/packages/repack" "$STAGE/packages/repack"
cp "$REPO/packages/wasm/package.json" "$REPO/packages/wasm/libopenmpt.d.ts" "$STAGE/packages/wasm/"
cp -R "$REPO/packages/wasm/dist" "$STAGE/packages/wasm/dist"
# The server app + deploy scripts/units.
mkdir -p "$STAGE/apps"
cp -R "$REPO/apps/server" "$STAGE/apps/server"
cp -R "$REPO/deploy" "$STAGE/deploy"
# Scrub anything stray.
find "$STAGE" -name node_modules -type d -prune -exec rm -rf {} + 2>/dev/null || true
rm -rf "$STAGE/deploy/dist"
echo "$VERSION" > "$STAGE/VERSION"

mkdir -p "$OUT"
tar -C "$(dirname "$STAGE")" -czf "$OUT/$NAME.tar.gz" "$NAME"
echo "built $OUT/$NAME.tar.gz ($(du -h "$OUT/$NAME.tar.gz" | cut -f1))"
