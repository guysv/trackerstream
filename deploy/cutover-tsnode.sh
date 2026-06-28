#!/usr/bin/env bash
#
# Cut the trackerstream master over from stock kubo to the custom tsnode node — or roll
# back. Run as root on the box, AFTER deploy/install.sh has placed /usr/local/bin/tsnode and
# the trackerstream-node.service unit.
#
#   sudo bash deploy/cutover-tsnode.sh prepare    # set up tsnode repo + import kubo keys (no traffic flip)
#   sudo bash deploy/cutover-tsnode.sh cutover    # stop kubo, start tsnode, re-ingest, verify
#   sudo bash deploy/cutover-tsnode.sh rollback   # stop tsnode, restart kubo (warm standby)
#   sudo bash deploy/cutover-tsnode.sh status
#
# kubo is never uninstalled — its repo (/srv/trackerstream/ipfs, incl. keystore) is the
# rollback identity. tsnode gets its OWN repo (/srv/trackerstream/tsnode) and a re-ingested
# blockstore (CID parity), so the two never share a datastore.
set -euo pipefail
[ "$(id -u)" = "0" ] || { echo "run as root" >&2; exit 1; }

DATA=/srv/trackerstream
KUBO_REPO="$DATA/ipfs"
TS_REPO="$DATA/tsnode"
RPC="127.0.0.1:5001"
MASTER_PEER_ID="${MASTER_PEER_ID:-12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL}"
CATALOG_IPNS_KEY="${CATALOG_IPNS_KEY:-12D3KooWDb53qFZvANj5kDCr3riMhT2HJG32i5xqFKhvBtzh7wPC}"
PREFIX="$(cd "$(dirname "$0")/.." && pwd)"

# import the kubo swarm identity (config Identity.PrivKey, base64 → libp2p protobuf) and the
# catalog IPNS key (ipfs key export) into the tsnode repo, asserting the PeerIds are preserved.
prepare() {
  command -v tsnode >/dev/null || { echo "tsnode not installed — run deploy/install.sh" >&2; exit 1; }
  mkdir -p "$TS_REPO"
  local tmp; tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' RETURN

  if [ -f "$TS_REPO/identity.key" ]; then
    echo "tsnode identity already present — skipping swarm-key import"
  else
    echo "importing swarm identity from kubo config…"
    jq -r '.Identity.PrivKey' "$KUBO_REPO/config" | base64 -d > "$tmp/self.key"
    tsnode import-key --repo "$TS_REPO" --name self --file "$tmp/self.key" --expect "$MASTER_PEER_ID"
  fi

  if [ -f "$TS_REPO/keystore/catalog" ]; then
    echo "tsnode catalog key already present — skipping"
  elif sudo -u trackerstream env IPFS_PATH="$KUBO_REPO" ipfs key export catalog -o "$tmp/catalog.key" 2>/dev/null; then
    tsnode import-key --repo "$TS_REPO" --name catalog --file "$tmp/catalog.key" --expect "$CATALOG_IPNS_KEY"
  else
    echo "NOTE: kubo has no 'catalog' key yet (born on first ingest). tsnode will mint one on"
    echo "      first publish; if its name != $CATALOG_IPNS_KEY, update packages/config + clients."
  fi
  chown -R trackerstream:trackerstream "$TS_REPO"
  echo "tsnode repo ready at $TS_REPO"
}

wait_rpc() {
  for _ in $(seq 1 30); do
    curl -fsS -X POST "http://$RPC/api/v0/id" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "tsnode RPC did not come up on $RPC" >&2; return 1
}

cutover() {
  prepare
  echo "stopping kubo (warm standby stays installed/disabled)…"
  systemctl disable --now trackerstream-ipfs.service 2>/dev/null || true
  echo "starting tsnode…"
  systemctl enable --now trackerstream-node.service
  wait_rpc
  local id; id="$(curl -fsS -X POST "http://$RPC/api/v0/id" | jq -r .ID)"
  [ "$id" = "$MASTER_PEER_ID" ] || { echo "FATAL: tsnode PeerId $id != $MASTER_PEER_ID — rolling back"; rollback; exit 1; }
  echo "tsnode live, PeerId preserved ($id)"

  echo "re-ingesting corpus into tsnode blockstore (CID parity)…"
  systemctl start trackerstream-ingest.service
  echo "waiting for ingest to finish…"; systemctl is-active --quiet trackerstream-ingest.service && sleep 2
  bash "$PREFIX/deploy/verify-pinset.sh" || { echo "pinset verify FAILED — investigate before trusting cutover" >&2; exit 1; }
  echo "CUTOVER COMPLETE. Rollback any time: sudo bash deploy/cutover-tsnode.sh rollback"
}

rollback() {
  echo "stopping tsnode, restoring kubo warm standby…"
  systemctl disable --now trackerstream-node.service 2>/dev/null || true
  systemctl enable --now trackerstream-ipfs.service
  echo "rolled back to kubo. tsnode repo left intact at $TS_REPO for a retry."
}

status() {
  echo "--- units ---"
  systemctl --no-pager --lines=0 status trackerstream-node trackerstream-ipfs 2>/dev/null | grep -E "●|Active:" || true
  echo "--- live RPC id ---"
  curl -fsS -X POST "http://$RPC/api/v0/id" 2>/dev/null | jq -r '.ID // "rpc down"' || echo "rpc down"
  echo "--- node status ---"
  curl -fsS -X POST "http://$RPC/api/v0/node/status" 2>/dev/null || true
}

case "${1:-}" in
  prepare) prepare ;;
  cutover) cutover ;;
  rollback) rollback ;;
  status) status ;;
  *) echo "usage: $0 {prepare|cutover|rollback|status}" >&2; exit 2 ;;
esac
