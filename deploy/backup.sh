#!/usr/bin/env bash
#
# Back up the trackerstream control-plane state: the SQLite catalog (modules,
# playlists, accounts) and the kubo pinset (the list of pinned root CIDs — the
# blocks themselves are large + re-pinnable from the archive, so we snapshot the
# pin LIST, not the datastore). For full block-store durability, also take a
# DigitalOcean volume snapshot (the data dir is the mounted volume).
#
#   bash deploy/backup.sh            # -> /srv/trackerstream/backups/<ts>/
# Run on the droplet (or via ssh). Restore: see deploy/RESTORE.md.
set -euo pipefail
DATA=/srv/trackerstream
TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$DATA/backups/$TS"
mkdir -p "$OUT"

echo "=== catalog (online-safe backup via sqlite .backup) ==="
sqlite3 "$DATA/catalog/catalog.db" ".backup '$OUT/catalog.db'"

echo "=== pinset (recursive root CIDs) ==="
IPFS_PATH="$DATA/ipfs" ipfs pin ls --type=recursive --quiet > "$OUT/pinset.txt" 2>/dev/null || true
wc -l < "$OUT/pinset.txt" | xargs echo "pinned roots:"

echo "=== config ==="
cp /etc/trackerstream/server.env "$OUT/" 2>/dev/null || true

echo "backup written to $OUT"
echo "TIP: also snapshot the block volume:  doctl compute volume-action snapshot <volume-id>"
# Retain the last 14 backups.
ls -1dt "$DATA"/backups/*/ 2>/dev/null | tail -n +15 | xargs -r rm -rf
