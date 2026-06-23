#!/usr/bin/env bash
#
# trackerstream server install — turns a base-provisioned Debian 13 droplet (see
# deploy/provision.sh) into a running control plane: HTTP API + master kubo
# (DHT bootstrap + provider + circuit-relay) + coturn STUN/TURN, all under
# systemd. Idempotent; run as root from the extracted artifact root.
#
#   ssh trackerstream-server 'cd /opt/trackerstream && sudo bash deploy/install.sh'
#
# Then: sync the corpus (deploy/sync-archive.sh from the workstation) and run the
# ingest (systemctl start trackerstream-ingest).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

[ "$(id -u)" = "0" ] || { echo "run as root" >&2; exit 1; }

PREFIX="$(cd "$(dirname "$0")/.." && pwd)"   # artifact root (e.g. /opt/trackerstream)
DATA=/srv/trackerstream
NODE_MAJOR="${NODE_MAJOR:-24}"
KUBO_VERSION="${KUBO_VERSION:-0.42.0}"
PUBLIC_IP="${PUBLIC_IP:-165.227.155.138}"
PUBLIC_IP6="${PUBLIC_IP6:-2a03:b0c0:3:f0:0:2:959c:b000}"
API_PORT="${API_PORT:-8080}"
SWARM_PORT=4001
STUN_PORT=3478

echo "=== [1/8] toolchain ==="
if ! command -v node >/dev/null || [ "$(node -p 'process.versions.node.split(".")[0]')" -lt 23 ]; then
  curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | bash -
  apt-get install -y -qq nodejs
fi
command -v pnpm >/dev/null || npm install -g pnpm >/dev/null
apt-get install -y -qq coturn >/dev/null
echo "node $(node -v), pnpm $(pnpm -v), $(turnserver -h >/dev/null 2>&1 && echo coturn ok)"

echo "=== [2/8] kubo ${KUBO_VERSION} ==="
if [ "$(ipfs version -n 2>/dev/null || true)" != "$KUBO_VERSION" ]; then
  tmp="$(mktemp -d)"
  curl -fSL "https://dist.ipfs.tech/kubo/v${KUBO_VERSION}/kubo_v${KUBO_VERSION}_linux-amd64.tar.gz" \
    | tar -xz -C "$tmp"
  install -m755 "$tmp/kubo/ipfs" /usr/local/bin/ipfs
  rm -rf "$tmp"
fi
echo "ipfs $(ipfs version -n)"

echo "=== [3/8] install server tree -> /opt/trackerstream ==="
if [ "$PREFIX" != "/opt/trackerstream" ]; then
  mkdir -p /opt/trackerstream
  cp -R "$PREFIX/." /opt/trackerstream/
  PREFIX=/opt/trackerstream
fi
( cd "$PREFIX" && pnpm install --prod --config.confirmModulesPurge=false >/dev/null 2>&1 || pnpm install --prod )
echo "deps installed"

echo "=== [4/8] master kubo config ($DATA/ipfs) ==="
export IPFS_PATH="$DATA/ipfs"
if [ ! -f "$IPFS_PATH/config" ]; then
  sudo -u trackerstream env IPFS_PATH="$IPFS_PATH" ipfs init --profile=server >/dev/null
fi
cfg() { sudo -u trackerstream env IPFS_PATH="$IPFS_PATH" ipfs config "$@"; }
cfg Addresses.API "/ip4/127.0.0.1/tcp/5001"
cfg Addresses.Gateway "/ip4/127.0.0.1/tcp/8081"
cfg --json Addresses.Swarm "[\"/ip4/0.0.0.0/tcp/$SWARM_PORT\",\"/ip6/::/tcp/$SWARM_PORT\",\"/ip4/0.0.0.0/udp/$SWARM_PORT/quic-v1\",\"/ip6/::/udp/$SWARM_PORT/quic-v1\"]"
cfg --json Addresses.Announce "[\"/ip4/$PUBLIC_IP/tcp/$SWARM_PORT\",\"/ip6/$PUBLIC_IP6/tcp/$SWARM_PORT\",\"/ip4/$PUBLIC_IP/udp/$SWARM_PORT/quic-v1\",\"/ip6/$PUBLIC_IP6/udp/$SWARM_PORT/quic-v1\"]"
cfg Routing.Type dht                                  # full DHT server (bootstrap peer)
# Provide only pinned ROOTS to the DHT, not every leaf block (MVP-FOLLOWUP A2):
# a per-block DHT provide throttled bulk ingest to ~1.3 modules/s. Clients always
# bootstrap to this always-on master and Bitswap-fetch every block directly from
# it, so only roots need DHT provider records (for warm-cache peer discovery).
cfg Provide.Strategy roots                            # was: all (per-block provide = ingest bottleneck)
cfg Provide.DHT.Interval 22h                          # (kubo 0.42 renamed Reprovider.* -> Provide.*)
cfg --json Swarm.RelayService.Enabled true            # circuit-relay v2 (NAT fallback for peers)
cfg --json Swarm.AddrFilters '[]'
# Drop the deprecated Reprovider block if an older init created one (0.42 is fatal on it).
F="$IPFS_PATH/config"; jq 'del(.Reprovider)' "$F" > "$F.tmp" && mv "$F.tmp" "$F"
chown -R trackerstream:trackerstream "$DATA/ipfs"
echo "kubo configured (DHT server, provider=all, relay v2)"

echo "=== [5/8] coturn STUN/TURN ==="
secret_file=/etc/trackerstream/turn.secret
mkdir -p /etc/trackerstream
[ -f "$secret_file" ] || { openssl rand -hex 24 > "$secret_file"; chmod 600 "$secret_file"; }
TURN_SECRET="$(cat "$secret_file")"
sed -e "s/__PUBLIC_IP__/$PUBLIC_IP/" -e "s/__TURN_SECRET__/$TURN_SECRET/" \
  "$PREFIX/deploy/turnserver.conf" > /etc/turnserver.conf
echo 'TURNSERVER_ENABLED=1' > /etc/default/coturn
systemctl enable --now coturn >/dev/null 2>&1 || systemctl restart coturn
echo "coturn STUN on :$STUN_PORT (TURN realm trackerstream)"

echo "=== [6/8] server config + systemd units ==="
[ -f /etc/trackerstream/server.env ] || cp "$PREFIX/deploy/server.env.sample" /etc/trackerstream/server.env
mkdir -p "$DATA/catalog"; chown trackerstream:trackerstream "$DATA/catalog"
install -m644 "$PREFIX"/deploy/systemd/*.service "$PREFIX"/deploy/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now trackerstream-ipfs.service
systemctl enable --now trackerstream-api.service
systemctl enable --now trackerstream-ingest.timer

echo "=== [7/8] firewall (ufw) ==="
ufw allow OpenSSH >/dev/null 2>&1 || true
ufw allow "$SWARM_PORT"/tcp >/dev/null 2>&1 || true
ufw allow "$SWARM_PORT"/udp >/dev/null 2>&1 || true
ufw allow "$STUN_PORT" >/dev/null 2>&1 || true
ufw allow "$API_PORT"/tcp >/dev/null 2>&1 || true
yes | ufw enable >/dev/null 2>&1 || true

echo "=== [8/8] status ==="
systemctl --no-pager --lines=0 status trackerstream-ipfs trackerstream-api coturn 2>/dev/null | grep -E "●|Active:" || true
echo
echo "DONE. Master peer id:"
sudo -u trackerstream env IPFS_PATH="$IPFS_PATH" ipfs id -f '<id>\n' 2>/dev/null || echo "(start trackerstream-ipfs first)"
echo
echo "Next: sync corpus from workstation (deploy/sync-archive.sh), then:"
echo "  systemctl start trackerstream-ingest   # build CID-DAGs, pin, catalog"
echo "  curl localhost:$API_PORT/healthz"
