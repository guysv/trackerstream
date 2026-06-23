#!/usr/bin/env bash
#
# Interop test: the embedded rust-ipfs CLIENT (Tauri backend) fetches a module
# DAG from a kubo MASTER over Bitswap and reassembles exact bytes. Proves the
# in-process Rust node interops with kubo and the Rust reassembler is correct —
# the same data-plane loop the desktop app uses, with no second process on the
# client side.
#
#   bash test/rust-interop.sh [module-file]
set -euo pipefail
cd "$(dirname "$0")/.."
REPACK_DIR="$(pwd)"
TAURI_DIR="$REPACK_DIR/../../apps/desktop/src-tauri"

MODULE="${1:-$HOME/tmp/somemods/beyond_the_network.it}"
ROOT_DIR=/tmp/ts-interop
M="$ROOT_DIR/master"
M_API=5201; M_SWARM=5211; M_GW=5281
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

echo "=== start kubo master ==="
rm -rf "$ROOT_DIR"; mkdir -p "$M"
export IPFS_PATH="$M"
ipfs init >/dev/null 2>&1
ipfs config Addresses.API "/ip4/127.0.0.1/tcp/$M_API" >/dev/null
ipfs config Addresses.Gateway "/ip4/127.0.0.1/tcp/$M_GW" >/dev/null
ipfs config --json Addresses.Swarm "[\"/ip4/127.0.0.1/tcp/$M_SWARM\"]" >/dev/null
ipfs config --json Swarm.AddrFilters "[]" >/dev/null
ipfs config --json Bootstrap "[]" >/dev/null
IPFS_PATH="$M" ipfs daemon --routing=none >"$M/daemon.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
  curl -s -X POST "http://127.0.0.1:$M_API/api/v0/id" 2>/dev/null | grep -q '"ID"' && break || sleep 0.3
done

echo "=== load + pin DAG on master ==="
ROOT=$(node test/load-one.ts "$MODULE" "http://127.0.0.1:$M_API" | sed -n 's/^ROOT=//p')
echo "root: $ROOT"

# Master dialable address (peer id + 127.0.0.1 tcp swarm addr).
PEER=$(curl -s -X POST "http://127.0.0.1:$M_API/api/v0/id" | sed -n 's/.*"ID":"\([^"]*\)".*/\1/p')
MASTER_ADDR="/ip4/127.0.0.1/tcp/$M_SWARM/p2p/$PEER"
echo "master: $MASTER_ADDR"

echo "=== run embedded rust-ipfs client (fetch + verify + reassemble) ==="
cd "$TAURI_DIR"
MASTER_ADDR="$MASTER_ADDR" ROOT_CID="$ROOT" ORIG_FILE="$MODULE" \
  cargo run --quiet --example fetch-check
