#!/usr/bin/env bash
#
# Export trackerstream node liveness as Prometheus textfile metrics — NO app changes.
# Polls the local tsnode RPC (node/status) and atomically writes a .prom file that the
# Prometheus node_exporter "textfile" collector picks up. Dependency-light: bash + curl + jq.
#
#   bash deploy/metrics-export.sh        # writes $TEXTFILE
#
# Run on a short interval via trackerstream-metrics.timer (every ~1 min).
#
# The Node.js API (/healthz) was retired with the Go cutover; liveness now comes from the
# tsnode node/status RPC (the same endpoint the client status pane reads).
#
# --- Wiring node_exporter ---------------------------------------------------
# Install node_exporter and point its textfile collector at $TEXTFILE_DIR:
#     apt-get install -y prometheus-node-exporter
#     # ensure it runs with:  --collector.textfile.directory=/var/lib/node_exporter/textfile_collector
#     # (Debian's default ExecStart already uses ARGS from /etc/default/prometheus-node-exporter;
#     #  add:  ARGS="--collector.textfile.directory=/var/lib/node_exporter/textfile_collector" )
# Prometheus then scrapes node_exporter (:9100) and these gauges appear as
# trackerstream_up, trackerstream_peers, etc.
#
# --- Wiring a simple uptime/health check (no Prometheus) --------------------
# An external uptime monitor can hit the RPC directly:
#     curl -fsS -X POST http://127.0.0.1:5001/api/v0/node/status   (HTTP 200 = up)
# Or alert on `trackerstream_up 0` / stale file mtime if scraping node_exporter.
set -euo pipefail

# tsnode RPC is POST-only (kubo-compatible /api/v0). Default mirrors the node unit's TS_RPC.
STATUS_URL="${STATUS_URL:-http://127.0.0.1:5001/api/v0/node/status}"
TEXTFILE_DIR="${TEXTFILE_DIR:-/var/lib/node_exporter/textfile_collector}"
TEXTFILE="${TEXTFILE:-$TEXTFILE_DIR/trackerstream.prom}"
CURL_TIMEOUT="${CURL_TIMEOUT:-5}"

mkdir -p "$TEXTFILE_DIR"

# Atomic write: build into a temp file in the same dir, then mv into place so the
# collector never reads a half-written file.
TMP="$(mktemp "$TEXTFILE_DIR/.trackerstream.prom.XXXXXX")"
trap 'rm -f "$TMP"' EXIT

emit() {
  # emit <metric> <help> <value>
  printf '# HELP %s %s\n# TYPE %s gauge\n%s %s\n' "$1" "$2" "$1" "$1" "$3" >> "$TMP"
}

# Scrape node/status. On any failure (down, timeout, bad JSON) we still emit a
# valid file with trackerstream_up 0 so "down" is observable, not just absent.
if json="$(curl -fsS -X POST --max-time "$CURL_TIMEOUT" "$STATUS_URL" 2>/dev/null)" \
   && echo "$json" | jq -e . >/dev/null 2>&1; then
  # Pull counts defensively: missing keys -> 0. node/status shape:
  #   { "Peers": N, "CatalogPeers": N, "Pins": N, "TotalIn": N, "TotalOut": N, ... }
  peers="$(echo "$json"         | jq -r '.Peers        // 0 | floor' 2>/dev/null || echo 0)"
  catalog_peers="$(echo "$json" | jq -r '.CatalogPeers // 0 | floor' 2>/dev/null || echo 0)"
  pins="$(echo "$json"          | jq -r '.Pins         // 0 | floor' 2>/dev/null || echo 0)"
  emit trackerstream_up           "1 if the trackerstream node RPC responded with valid JSON" 1
  emit trackerstream_peers        "Connected libp2p peers"                "$peers"
  emit trackerstream_catalog_peers "Peers subscribed to the catalog topic" "$catalog_peers"
  emit trackerstream_pins         "Pinned roots (corpus + catalog)"        "$pins"
else
  emit trackerstream_up "1 if the trackerstream node RPC responded with valid JSON" 0
fi

mv "$TMP" "$TEXTFILE"
trap - EXIT
chmod 644 "$TEXTFILE"
echo "wrote $TEXTFILE"
