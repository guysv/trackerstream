#!/usr/bin/env bash
# Materialize a PATCHED copy of rust-ipfs for `[patch.crates-io]`, mirroring the
# libopenmpt vendor-patch workflow (packages/wasm/build.sh): take the EXACT pinned
# crates.io source, drop it in a gitignored vendor/ tree, and apply the committed
# patches/*.patch on top. The committed diff stays tiny + reviewable; the full
# source tree is never committed. Re-run is idempotent.
#
# Authoring/refreshing a patch (the dev-clone half of the workflow):
#   1. cp -R <pristine rust-ipfs-0.15.0> /tmp/ri && cd /tmp/ri && git init -q && git add -A && git commit -qm base
#   2. edit, then:  git diff > <repo>/apps/desktop/src-tauri/patches/0001-bitswap-peer-attribution.patch
#   3. re-run this script; `cargo build` picks up the patched vendor copy.
set -euo pipefail

VERSION=0.15.0
CRATE="rust-ipfs-$VERSION"
HERE="$(cd "$(dirname "$0")/.." && pwd)"   # …/src-tauri
VENDOR="$HERE/vendor/rust-ipfs"
STAMP="$VENDOR/.ts-prepared"
URL="https://static.crates.io/crates/rust-ipfs/$CRATE.crate"

# Freshness guard: skip if the vendored tree is newer than every patch + this
# script (so a normal `tauri dev` doesn't re-extract the crate each launch).
if [ -f "$STAMP" ]; then
  newest="$(ls -t "$HERE"/patches/*.patch "$0" 2>/dev/null | head -1 || true)"
  if [ -z "$newest" ] || [ "$STAMP" -nt "$newest" ]; then
    echo "rust-ipfs $VERSION already prepared (up to date) — skipping"
    exit 0
  fi
fi

mkdir -p "$HERE/vendor"
rm -rf "$VENDOR"

# Pristine source: prefer the local cargo registry copy (offline, already fetched);
# fall back to downloading the pinned .crate tarball from crates.io.
CACHE="$(find "$HOME/.cargo/registry/src" -maxdepth 2 -type d -name "$CRATE" 2>/dev/null | head -1 || true)"
if [ -n "$CACHE" ]; then
  echo "vendoring rust-ipfs $VERSION from cargo registry cache"
  cp -R "$CACHE" "$VENDOR"
  chmod -R u+w "$VENDOR"   # registry cache is read-only
else
  echo "downloading $CRATE.crate from crates.io"
  tmp="$(mktemp -d)"
  curl -fSL -o "$tmp/$CRATE.crate" "$URL"
  tar xzf "$tmp/$CRATE.crate" -C "$tmp"
  mv "$tmp/$CRATE" "$VENDOR"
  rm -rf "$tmp"
fi

shopt -s nullglob
for p in "$HERE"/patches/*.patch; do
  echo "applying patch: $(basename "$p")"
  patch -d "$VENDOR" -p1 < "$p"
done
shopt -u nullglob

touch "$STAMP"
echo "rust-ipfs $VERSION prepared + patched at vendor/rust-ipfs"
