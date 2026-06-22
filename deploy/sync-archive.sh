#!/usr/bin/env bash
#
# trackerstream — sync the Mod Archive corpus to the droplet.
#
# Pushes the local offline corpus (~52 GB, 122k+ modules) to the droplet's
# block volume, where the Phase 2 ingest pipeline reads it. rsync over SSH:
# resumable (survives a dropped link), incremental (re-runs send only changes),
# and verifiable. Run from a workstation that holds the corpus.
#
#   ./deploy/sync-archive.sh              # sync
#   DRYRUN=1 ./deploy/sync-archive.sh     # preview only, transfer nothing
#   BWLIMIT=20m ./deploy/sync-archive.sh  # cap upload bandwidth
#
# Overridable via env:
#   SRC       local corpus dir   (default ~/tmp/modarchive  — the lab convention)
#   HOST      ssh target         (default trackerstream-server)
#   DEST      remote archive dir (default /srv/trackerstream/archive)
#   OWNER     remote owner       (default trackerstream)
set -euo pipefail

SRC="${SRC:-$HOME/tmp/modarchive}"
HOST="${HOST:-trackerstream-server}"
DEST="${DEST:-/srv/trackerstream/archive}"
OWNER="${OWNER:-trackerstream}"

# Trailing slash on SRC => copy the *contents* into DEST (not a nested dir).
SRC="${SRC%/}/"

if [ ! -d "$SRC" ]; then
  echo "ERROR: local corpus dir not found: $SRC (set SRC=...)." >&2
  exit 1
fi

RSYNC_OPTS=(-a --partial --human-readable --info=progress2
            --no-perms --no-owner --no-group)   # remote chown handled after
[ -n "${BWLIMIT:-}" ] && RSYNC_OPTS+=("--bwlimit=$BWLIMIT")
[ -n "${DRYRUN:-}" ]  && RSYNC_OPTS+=(--dry-run)

echo "=== source ==="; du -sh "$SRC" 2>/dev/null || true
find "$SRC" -type f | wc -l | xargs echo "local file count:"

echo "=== ensuring remote dir ==="
ssh -o BatchMode=yes "$HOST" "mkdir -p '$DEST'"

echo "=== rsync -> $HOST:$DEST ${DRYRUN:+(DRY RUN)} ==="
rsync "${RSYNC_OPTS[@]}" "$SRC" "$HOST:$DEST/"

if [ -z "${DRYRUN:-}" ]; then
  echo "=== fixing ownership + verifying ==="
  ssh -o BatchMode=yes "$HOST" "chown -R $OWNER:$OWNER '$DEST'; \
    echo -n 'remote file count: '; find '$DEST' -type f | wc -l; \
    echo -n 'remote size: '; du -sh '$DEST'"
fi
echo "DONE."
