#!/usr/bin/env bash
#
# Confirm the running master's pinset matches the catalog: every module root CID in
# the catalog must be pinned (availability floor), and there should be no orphaned
# recursive pins not in the catalog. Works against the live master whichever it is —
# tsnode over its kubo-compatible RPC (pin/ls), or the kubo warm-standby CLI. Run on the box.
#
#   bash deploy/verify-pinset.sh
set -euo pipefail
DATA=/srv/trackerstream
RPC="${TS_RPC:-127.0.0.1:5001}"

CATALOG=$(mktemp); PINNED=$(mktemp)
trap 'rm -f "$CATALOG" "$PINNED"' EXIT

sqlite3 "$DATA/catalog/catalog.db" "SELECT DISTINCT root_cid FROM modules;" | sort -u > "$CATALOG"

# Prefer the RPC pin/ls (tsnode or kubo daemon both serve it); fall back to the kubo CLI.
if curl -fsS -X POST "http://$RPC/api/v0/pin/ls?type=recursive" 2>/dev/null \
     | jq -r '.Keys // {} | keys[]' 2>/dev/null | sort -u > "$PINNED" && [ -s "$PINNED" ]; then
  echo "pinset source: RPC $RPC"
else
  echo "pinset source: kubo CLI ($DATA/ipfs)"
  IPFS_PATH="$DATA/ipfs" ipfs pin ls --type=recursive --quiet | sort -u > "$PINNED"
fi

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
