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
PUBLIC_IP="${PUBLIC_IP:-5.75.131.145}"
PUBLIC_IP6="${PUBLIC_IP6:-2a01:4f8:1c1f:9120::1}"
SWARM_PORT=4001
STUN_PORT=3478
# Stable identities the custom node must reproduce (packages/config). The cutover imports
# these from the kubo warm-standby so already-shipped clients keep resolving.
MASTER_PEER_ID="${MASTER_PEER_ID:-12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL}"
CATALOG_IPNS_KEY="${CATALOG_IPNS_KEY:-12D3KooWDb53qFZvANj5kDCr3riMhT2HJG32i5xqFKhvBtzh7wPC}"

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
echo "ipfs $(ipfs version -n)  (warm-standby; the live master is tsnode)"

echo "=== [2b/8] tsnode (custom go-libp2p+boxo node) ==="
# Prefer the prebuilt static binary shipped in the artifact (deploy/build-tsnode.sh);
# fall back to building from source if a Go toolchain is present. The cutover
# (deploy/cutover-tsnode.sh) wires it as the live master; install.sh only places it.
TSNODE_BIN="$(dirname "$0")/dist/tsnode-linux-amd64"
if [ -f "$TSNODE_BIN" ]; then
  install -m755 "$TSNODE_BIN" /usr/local/bin/tsnode
elif command -v go >/dev/null; then
  ( cd "$(dirname "$0")/../node" && go build -trimpath -ldflags "-s -w" -o /usr/local/bin/tsnode ./cmd/tsnode )
else
  echo "WARN: no deploy/dist/tsnode-linux-amd64 and no Go toolchain — run deploy/build-tsnode.sh first" >&2
fi
command -v tsnode >/dev/null && echo "tsnode installed at $(command -v tsnode)"

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
# circuit-relay v2 (NAT fallback for peers), clamped to HANDSHAKE-ONLY (P2P-NEXT-STEPS
# Phase 0): the master may broker a DCUtR hole-punch but must NOT carry bulk data. This
# kills the "relay-bytes trap" — offload audio silently flowing through the master's relay
# instead of peer↔peer — and makes any fake-offload fail loudly. 128KiB / 30s is ample for
# signaling, far below an audio stream. (go-libp2p RelayLimit: Data=int64 bytes, Duration.)
# Set the WHOLE RelayService object in one call: kubo 0.42's `ipfs config --json` refuses
# to create the nested `.Limit` key when it doesn't exist yet ("RelayService.Limit not
# found"), so the per-leaf form fails on a fresh box.
cfg --json Swarm.RelayService '{"Enabled":true,"Limit":{"ConnectionDataLimit":131072,"ConnectionDurationLimit":"30s"}}'
cfg --json Swarm.AddrFilters '[]'

# --- Inbound capacity: absorb connection STORMS, not just steady-state churn -------
# The master is an always-on bootstrap + provider, so a launch flash-crowd (many
# distinct clients dialing at once) must not get trimmed or shed. Two levers:
#  1) ConnMgr — defaults trim above HighWater=900, which would drop real clients
#     mid-stream during a crowd. Raise the watermarks so the master HOLDS a big warm
#     peer set. This is the lever that actually bit (the box already runs ~260 peers).
#  2) ResourceMgr scopes — keep the RM ENABLED (don't drop DoS protection); raise the
#     System/Transient/Peer count ceilings well above defaults so connection COUNT
#     isn't the limiter (the per-scope Memory budget, ~half RAM, stays the backstop —
#     the 8GB box bounds the true ceiling regardless).
# NOTE: a burst from a SINGLE source IP is separately capped by go-libp2p's per-subnet
# ConnLimiter, which kubo 0.42 does not expose in config — so this does NOT lift the
# single-host benchmark cap (keep client sweeps to low --jobs). It lifts real,
# many-IP storms. Verify after restart with `ipfs swarm resources`.
cfg --json Swarm.ConnMgr.Type '"basic"'
cfg --json Swarm.ConnMgr.LowWater 1000                # hold a large warm peer set
cfg --json Swarm.ConnMgr.HighWater 3000               # before trimming kicks in (was 900)
cfg --json Swarm.ConnMgr.GracePeriod '"60s"'          # don't trim a peer in its first minute
cfg --json Swarm.ResourceMgr.Enabled true             # keep DoS protection ON, just looser
# Per-scope COUNT overrides. NB: kubo removed the `Swarm.ResourceMgr.Limits` config
# key in 0.19 — overrides now live in this side-car file (merged over the memory-scaled
# defaults). Memory/FD omitted = keep defaults, so the per-scope Memory budget (~half
# RAM) stays the real backstop; we only lift the conn/stream count ceilings.
sudo -u trackerstream tee "$IPFS_PATH/libp2p-resource-limit-overrides.json" >/dev/null <<'JSON'
{
  "System":      {"Conns":16384,"ConnsInbound":8192,"ConnsOutbound":16384,"Streams":65536,"StreamsInbound":32768,"StreamsOutbound":65536},
  "Transient":   {"Conns":4096,"ConnsInbound":2048,"ConnsOutbound":4096,"Streams":16384,"StreamsInbound":8192,"StreamsOutbound":16384},
  "PeerDefault": {"Conns":64,"ConnsInbound":32,"ConnsOutbound":64,"Streams":4096,"StreamsInbound":2048,"StreamsOutbound":4096}
}
JSON

# Drop the deprecated Reprovider block if an older init created one (0.42 is fatal on it).
F="$IPFS_PATH/config"; jq 'del(.Reprovider)' "$F" > "$F.tmp" && mv "$F.tmp" "$F"
chown -R trackerstream:trackerstream "$DATA/ipfs"
echo "kubo configured (DHT server, provider=roots, relay v2 handshake-only, storm-tolerant rcmgr/connmgr)"

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
# journald retention drop-in (explicit size/time bounds for all our services).
mkdir -p /etc/systemd/journald.conf.d
install -m644 "$PREFIX/deploy/journald-trackerstream.conf" /etc/systemd/journald.conf.d/trackerstream.conf
systemctl restart systemd-journald
systemctl daemon-reload
systemctl enable --now trackerstream-ipfs.service
systemctl enable --now trackerstream-ingest.timer
# Scheduled ops (D3): daily backup + ~1-min node/status metrics export.
systemctl enable --now trackerstream-backup.timer
systemctl enable --now trackerstream-metrics.timer

echo "=== [7/8] firewall (ufw) ==="
ufw allow OpenSSH >/dev/null 2>&1 || true
ufw allow "$SWARM_PORT"/tcp >/dev/null 2>&1 || true
ufw allow "$SWARM_PORT"/udp >/dev/null 2>&1 || true
ufw allow "$STUN_PORT" >/dev/null 2>&1 || true
yes | ufw enable >/dev/null 2>&1 || true

echo "=== [8/8] status ==="
systemctl --no-pager --lines=0 status trackerstream-ipfs coturn 2>/dev/null | grep -E "●|Active:" || true
echo
echo "DONE. Master peer id:"
sudo -u trackerstream env IPFS_PATH="$IPFS_PATH" ipfs id -f '<id>\n' 2>/dev/null || echo "(start trackerstream-ipfs first)"
echo
echo "Next: sync corpus from workstation (deploy/sync-archive.sh), then:"
echo "  systemctl start trackerstream-ingest   # build CID-DAGs, pin, catalog"
echo "  curl -X POST localhost:5001/api/v0/node/status   # node liveness"
