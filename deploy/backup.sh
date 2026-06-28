#!/usr/bin/env bash
#
# Back up the trackerstream control-plane state: the SQLite catalog (modules,
# playlists, accounts) and the kubo pinset (the list of pinned root CIDs — the
# blocks themselves are large + re-pinnable from the archive, so we snapshot the
# pin LIST, not the datastore). For full block-store durability we ALSO take a
# DigitalOcean Block Storage volume snapshot (the data dir IS the mounted
# volume), if `doctl` is installed + authed.
#
#   bash deploy/backup.sh            # -> /srv/trackerstream/backups/<ts>/
#
# Non-interactive + idempotent — safe to run from a systemd timer
# (trackerstream-backup.timer, daily). Rotates local backups (keeps the last
# KEEP_LOCAL) and, when doctl is available, prunes DO volume snapshots (keeps the
# last KEEP_SNAPSHOTS). Every doctl call is guarded so the script still succeeds
# without doctl (the local catalog + pinset backup always runs).
#
# Restore: see deploy/RESTORE.md.
set -euo pipefail

DATA="${DATA:-/srv/trackerstream}"
KEEP_LOCAL="${KEEP_LOCAL:-14}"           # local catalog/pinset backups to retain
KEEP_SNAPSHOTS="${KEEP_SNAPSHOTS:-7}"    # DO volume snapshots to retain
# Volume to snapshot. If unset, we try to discover the volume backing $DATA by
# name (DO_VOLUME_NAME, default "trackerstream"). Either can be set in
# /etc/trackerstream/server.env or the environment.
DO_VOLUME_ID="${DO_VOLUME_ID:-}"
DO_VOLUME_NAME="${DO_VOLUME_NAME:-trackerstream}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$DATA/backups/$TS"
mkdir -p "$OUT"

echo "=== catalog (online-safe backup via sqlite .backup) ==="
sqlite3 "$DATA/catalog/catalog.db" ".backup '$OUT/catalog.db'"

echo "=== pinset (recursive root CIDs) ==="
# Prefer the live master's RPC pin/ls (tsnode or kubo daemon); fall back to the kubo CLI for
# the warm-standby repo. The pinset is just root CIDs — blocks are re-pinnable from the
# archive — so either source is equivalent for restore.
RPC="${TS_RPC:-127.0.0.1:5001}"
if curl -fsS -X POST "http://$RPC/api/v0/pin/ls?type=recursive" 2>/dev/null \
     | jq -r '.Keys // {} | keys[]' 2>/dev/null > "$OUT/pinset.txt" && [ -s "$OUT/pinset.txt" ]; then
  :
else
  IPFS_PATH="$DATA/ipfs" ipfs pin ls --type=recursive --quiet > "$OUT/pinset.txt" 2>/dev/null || true
fi
wc -l < "$OUT/pinset.txt" | xargs echo "pinned roots:"

echo "=== config ==="
cp /etc/trackerstream/server.env "$OUT/" 2>/dev/null || true

echo "backup written to $OUT"

# ---- rotate local backups (keep last KEEP_LOCAL) --------------------------
ls -1dt "$DATA"/backups/*/ 2>/dev/null | tail -n "+$((KEEP_LOCAL + 1))" | xargs -r rm -rf
echo "local backups retained: up to $KEEP_LOCAL (older pruned)"

# ---- DO volume snapshot (optional; degrades gracefully) -------------------
# `doctl` may be absent or unauthenticated on the droplet — in that case we
# print a hint and exit 0 so the timer reports success.
if ! command -v doctl >/dev/null 2>&1; then
  echo "doctl not installed — skipping volume snapshot."
  echo "TIP: install+auth doctl to enable block-volume snapshots, or snapshot manually:"
  echo "     doctl compute volume-action snapshot <volume-id> --snapshot-name trackerstream-$TS"
  exit 0
fi

# Verify auth without failing the script.
if ! doctl account get >/dev/null 2>&1; then
  echo "doctl present but not authenticated (run: doctl auth init) — skipping volume snapshot."
  exit 0
fi

# Resolve the volume id if not given explicitly.
if [ -z "$DO_VOLUME_ID" ]; then
  DO_VOLUME_ID="$(doctl compute volume list --format ID,Name --no-header 2>/dev/null \
    | awk -v n="$DO_VOLUME_NAME" '$2 == n {print $1; exit}')" || true
fi
if [ -z "$DO_VOLUME_ID" ]; then
  echo "could not resolve DO volume (name='$DO_VOLUME_NAME'); set DO_VOLUME_ID — skipping snapshot."
  exit 0
fi

SNAP_NAME="trackerstream-$TS"
echo "=== DO volume snapshot ($DO_VOLUME_ID -> $SNAP_NAME) ==="
if doctl compute volume-action snapshot "$DO_VOLUME_ID" --snapshot-name "$SNAP_NAME" >/dev/null 2>&1; then
  echo "snapshot requested: $SNAP_NAME"
else
  echo "snapshot request failed (continuing; local backup is intact)."
  exit 0
fi

# ---- prune old volume snapshots (keep last KEEP_SNAPSHOTS) -----------------
# List trackerstream-* volume snapshots oldest-first, drop all but the newest
# KEEP_SNAPSHOTS. We only ever touch snapshots WE created (name prefix), so this
# never deletes unrelated snapshots.
mapfile -t OLD_SNAPS < <(
  doctl compute snapshot list --resource volume \
    --format ID,Name,CreatedAt --no-header 2>/dev/null \
    | awk '$2 ~ /^trackerstream-/ {print $3"\t"$1}' \
    | sort \
    | head -n "-$KEEP_SNAPSHOTS" \
    | cut -f2
) || true

if [ "${#OLD_SNAPS[@]}" -gt 0 ]; then
  for id in "${OLD_SNAPS[@]}"; do
    [ -n "$id" ] || continue
    doctl compute snapshot delete "$id" --force >/dev/null 2>&1 \
      && echo "pruned old snapshot $id" \
      || echo "could not prune snapshot $id (continuing)."
  done
else
  echo "snapshots retained: up to $KEEP_SNAPSHOTS (none to prune)"
fi
