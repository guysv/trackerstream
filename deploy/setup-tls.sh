#!/usr/bin/env bash
#
# trackerstream — stand up TLS for the control-plane HTTP API (MVP-FOLLOWUP A1).
#
# Installs Caddy, fronts the API on https://$TS_DOMAIN (automatic Let's Encrypt
# cert + renewal), opens 80/443, and locks the raw API port (8080) so the API is
# reachable ONLY through Caddy. Idempotent; run as root on the droplet:
#
#   ssh trackerstream-server 'cd /opt/trackerstream && sudo TS_DOMAIN=trackerstream.xyz bash deploy/setup-tls.sh'
#
# Prereqs: $TS_DOMAIN must already resolve (A/AAAA) to this droplet so ACME
# HTTP-01 can validate. The API (serve.ts) should bind 127.0.0.1 — Caddy proxies
# to 127.0.0.1:8080 regardless, but this script also `ufw deny 8080` so external
# direct access is closed. Set LOCK_8080=0 to keep :8080 open during migration.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

[ "$(id -u)" = "0" ] || { echo "run as root" >&2; exit 1; }
TS_DOMAIN="${TS_DOMAIN:-trackerstream.xyz}"
LOCK_8080="${LOCK_8080:-1}"
PREFIX="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== [1/6] install Caddy ==="
if ! command -v caddy >/dev/null; then
  apt-get install -y -qq debian-keyring debian-archive-keyring apt-transport-https curl gnupg >/dev/null
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
    | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
    > /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq
  apt-get install -y -qq caddy >/dev/null
fi
echo "caddy $(caddy version | head -1)"

echo "=== [2/6] render Caddyfile for $TS_DOMAIN ==="
TS_DOMAIN="$TS_DOMAIN" envsubst '${TS_DOMAIN}' < "$PREFIX/deploy/Caddyfile" > /etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile

echo "=== [3/6] firewall: open 80,443 ==="
ufw allow 80/tcp  >/dev/null 2>&1 || true
ufw allow 443/tcp >/dev/null 2>&1 || true
ufw allow 443/udp >/dev/null 2>&1 || true   # HTTP/3 (QUIC)

echo "=== [4/6] start Caddy ==="
systemctl enable caddy >/dev/null 2>&1 || true
systemctl reload caddy 2>/dev/null || systemctl restart caddy
sleep 2

echo "=== [5/6] verify HTTPS (waiting for cert issuance) ==="
ok=0
for i in $(seq 1 30); do
  if curl -fsS -m 5 "https://$TS_DOMAIN/healthz" >/dev/null 2>&1; then ok=1; break; fi
  sleep 2
done
if [ "$ok" = 1 ]; then
  echo "HTTPS OK: $(curl -fsS -m 5 "https://$TS_DOMAIN/healthz")"
else
  echo "!! HTTPS not reachable yet — check: journalctl -u caddy -n 50" >&2
  exit 1
fi

echo "=== [6/6] lock the raw API port (8080) ==="
if [ "$LOCK_8080" = 1 ]; then
  ufw delete allow 8080/tcp >/dev/null 2>&1 || true
  ufw deny 8080/tcp >/dev/null 2>&1 || true
  echo "ufw: :8080 denied (API reachable only via Caddy/443)"
else
  echo "LOCK_8080=0 — leaving :8080 open"
fi

echo
echo "DONE. API now fronted at https://$TS_DOMAIN"
ufw status | grep -E "80|443|8080" || true
