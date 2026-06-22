#!/usr/bin/env bash
#
# trackerstream — base provisioning for the single master-node server.
#
# Stack-agnostic host setup: brings a fresh Debian 13 (trixie) DigitalOcean
# droplet with an attached Block Storage Volume to a state where the deploy
# artifact can be installed. Does NOT install the app toolchain (Node/Rust,
# kubo/libp2p) — that is chosen with the Phase 1 stack pick — and does NOT
# enable a firewall (the libp2p/STUN ports are not fixed yet).
#
# Idempotent: safe to re-run. Run as root on the droplet.
#   ssh trackerstream-server 'bash -s' < deploy/provision.sh
#
# Target droplet: Basic Regular, 4 vCPU / 8 GB, region fra1, 250 GB volume.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# The DigitalOcean volume auto-mount path. Override if the volume differs.
VOL="${TRACKERSTREAM_VOLUME:-/mnt/volume_fra1_1782162987280}"
VOL_DEV="${TRACKERSTREAM_VOLUME_DEV:-/dev/sda}"

if [ ! -d "$VOL" ]; then
  echo "ERROR: volume mount $VOL not found (set TRACKERSTREAM_VOLUME)." >&2
  exit 1
fi

echo "=== [1/6] base packages ==="
apt-get update -qq
apt-get install -y -qq build-essential git curl ca-certificates pkg-config \
  libssl-dev sqlite3 libsqlite3-dev unzip jq htop rsync ufw >/dev/null
echo "installed."

echo "=== [2/6] swap (4G, swappiness=10) ==="
if [ ! -f /swapfile ]; then
  fallocate -l 4G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile >/dev/null
  swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
  echo 'vm.swappiness=10' > /etc/sysctl.d/99-swappiness.conf
  sysctl -q -w vm.swappiness=10
  echo "swap created."
else
  echo "swap already present."
fi

echo "=== [3/6] persist volume in fstab (by UUID, nofail) ==="
UUID=$(blkid -s UUID -o value "$VOL_DEV")
if ! grep -q "$UUID" /etc/fstab; then
  echo "UUID=$UUID $VOL ext4 defaults,nofail,noatime,discard 0 2" >> /etc/fstab
  echo "added UUID=$UUID -> $VOL"
else
  echo "volume already in fstab."
fi

echo "=== [4/6] service user ==="
if ! id trackerstream >/dev/null 2>&1; then
  useradd --system --create-home --home-dir /var/lib/trackerstream \
    --shell /usr/sbin/nologin trackerstream
  echo "created user trackerstream."
else
  echo "user exists."
fi

echo "=== [5/6] data dir layout on volume ==="
DATA="$VOL/trackerstream"
mkdir -p "$DATA"/{archive,ipfs,catalog,artifacts}
chown -R trackerstream:trackerstream "$DATA"
ln -sfn "$DATA" /srv/trackerstream   # stable data root
echo "layout:"; ls -la /srv/trackerstream/

echo "=== [6/6] summary ==="
echo "swap:"; swapon --show
echo "fstab (relevant):"; grep -E "swapfile|$UUID" /etc/fstab
echo
echo "PENDING (later phases):"
echo "  - app toolchain (Node/Rust, kubo/libp2p) — Phase 1 stack pick"
echo "  - firewall: 'ufw allow OpenSSH' then libp2p/STUN ports, then 'ufw enable'"
echo "DONE."
