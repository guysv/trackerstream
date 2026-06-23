#!/usr/bin/env bash
#
# Two-node kubo Bitswap swarm test. Node A ("master") pins a module's CID-DAG;
# node B ("client", separate repo, no local blocks) fetches the root over
# Bitswap, verifies every block against its CID, reassembles the EXACT original
# bytes, and plays — proving the data-plane loop with a real libp2p transport.
#
# Localhost only: this proves the mechanism (fetch-from-peer + verify), NOT
# real-network latency. Cross-NAT numbers need the droplet + a second machine
# (Phase 1 task #10).
#
#   bash test/swarm.sh [module-file]
set -euo pipefail
cd "$(dirname "$0")/.."

MODULE="${1:-$HOME/tmp/somemods/beyond_the_network.it}"
ROOT=/tmp/ts-swarm
A="$ROOT/a"
B="$ROOT/b"
A_API=5101; A_SWARM=5111; A_GW=5181
B_API=5102; B_SWARM=5112; B_GW=5182
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

init_repo() {
  local repo="$1" api="$2" swarm="$3" gw="$4"
  export IPFS_PATH="$repo"
  mkdir -p "$repo"
  # Default profile (NOT server — that adds Swarm.AddrFilters blocking 127/8).
  ipfs init >/dev/null 2>&1
  ipfs config Addresses.API "/ip4/127.0.0.1/tcp/$api" >/dev/null
  ipfs config Addresses.Gateway "/ip4/127.0.0.1/tcp/$gw" >/dev/null
  ipfs config --json Addresses.Swarm "[\"/ip4/127.0.0.1/tcp/$swarm\"]" >/dev/null
  ipfs config --json Addresses.Announce "[\"/ip4/127.0.0.1/tcp/$swarm\"]" >/dev/null
  ipfs config --json Swarm.AddrFilters "[]" >/dev/null      # allow localhost dials
  ipfs config --json Bootstrap "[]" >/dev/null              # isolated swarm
  ipfs config --json Swarm.DisableNatPortMap true >/dev/null
}

start_daemon() {
  local repo="$1"
  IPFS_PATH="$repo" ipfs daemon --routing=none >"$repo/daemon.log" 2>&1 &
  PIDS+=($!)
}

wait_api() {
  local api="$1"
  for _ in $(seq 1 100); do
    if curl -s -X POST "http://127.0.0.1:$api/api/v0/id" 2>/dev/null | grep -q '"ID"'; then
      return 0
    fi
    sleep 0.3
  done
  echo "ERROR: API on $api never came up" >&2; exit 1
}

echo "=== init isolated repos ==="
rm -rf "$ROOT"
init_repo "$A" "$A_API" "$A_SWARM" "$A_GW"
init_repo "$B" "$B_API" "$B_SWARM" "$B_GW"

echo "=== start daemons (routing=none, isolated) ==="
start_daemon "$A"; start_daemon "$B"
wait_api "$A_API"; wait_api "$B_API"

echo "=== run swarm step ==="
TS_A="http://127.0.0.1:$A_API" TS_B="http://127.0.0.1:$B_API" \
  node test/swarm-step.ts "$MODULE"
