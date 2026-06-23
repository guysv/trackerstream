#!/usr/bin/env bash
#
# Confirm the running kubo pinset matches the catalog: every module root CID in
# the catalog must be pinned (availability floor), and there should be no
# orphaned recursive pins not in the catalog. Run on the droplet.
#
#   bash deploy/verify-pinset.sh
set -euo pipefail
DATA=/srv/trackerstream
export IPFS_PATH="$DATA/ipfs"

CATALOG=$(mktemp); PINNED=$(mktemp)
trap 'rm -f "$CATALOG" "$PINNED"' EXIT

sqlite3 "$DATA/catalog/catalog.db" "SELECT DISTINCT root_cid FROM modules;" | sort -u > "$CATALOG"
ipfs pin ls --type=recursive --quiet | sort -u > "$PINNED"

CAT_N=$(wc -l < "$CATALOG"); PIN_N=$(wc -l < "$PINNED")
MISSING=$(comm -23 "$CATALOG" "$PINNED" | wc -l | tr -d ' ')   # in catalog, not pinned
ORPHAN=$(comm -13 "$CATALOG" "$PINNED" | wc -l | tr -d ' ')    # pinned, not in catalog

echo "catalog roots: $CAT_N    pinned (recursive): $PIN_N"
echo "MISSING (catalog root not pinned): $MISSING"
echo "ORPHAN  (pinned root not in catalog): $ORPHAN"
if [ "$MISSING" -eq 0 ]; then
  echo "OK: every catalog module is pinned (availability floor intact)."
else
  echo "WARN: re-run ingest to re-pin missing roots." >&2
fi
# Orphans are expected if other content was pinned manually; not an error.
[ "$MISSING" -eq 0 ]
