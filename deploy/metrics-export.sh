#!/usr/bin/env bash
#
# Export trackerstream /healthz as Prometheus textfile metrics — NO app changes.
# Polls the local API and atomically writes a .prom file that the Prometheus
# node_exporter "textfile" collector picks up. Dependency-light: bash + curl + jq.
#
#   bash deploy/metrics-export.sh        # writes $TEXTFILE
#
# Run on a short interval via trackerstream-metrics.timer (every ~1 min).
#
# --- Wiring node_exporter ---------------------------------------------------
# Install node_exporter and point its textfile collector at $TEXTFILE_DIR:
#     apt-get install -y prometheus-node-exporter
#     # ensure it runs with:  --collector.textfile.directory=/var/lib/node_exporter/textfile_collector
#     # (Debian's default ExecStart already uses ARGS from /etc/default/prometheus-node-exporter;
#     #  add:  ARGS="--collector.textfile.directory=/var/lib/node_exporter/textfile_collector" )
# Prometheus then scrapes node_exporter (:9100) and these gauges appear as
# trackerstream_up, trackerstream_modules, etc.
#
# --- Wiring a simple uptime/health check (no Prometheus) --------------------
# An external uptime monitor (UptimeRobot, Healthchecks.io, a cron curl) can hit
# the same endpoint directly:  curl -fsS http://<host>:8080/healthz  (HTTP 200 =
# up). Or alert on `trackerstream_up 0` / stale file mtime if scraping node_exporter.
set -euo pipefail

HEALTHZ_URL="${HEALTHZ_URL:-http://127.0.0.1:8080/healthz}"
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

# Scrape /healthz. On any failure (down, timeout, bad JSON) we still emit a
# valid file with trackerstream_up 0 so "down" is observable, not just absent.
if json="$(curl -fsS --max-time "$CURL_TIMEOUT" "$HEALTHZ_URL" 2>/dev/null)" \
   && echo "$json" | jq -e . >/dev/null 2>&1; then
  # Pull counts defensively: missing keys -> 0. /healthz shape:
  #   { "modules": N, "playlists": N, "users": N, "uptimeSeconds": N, ... }
  modules="$(echo "$json"   | jq -r '.modules       // 0 | floor' 2>/dev/null || echo 0)"
  playlists="$(echo "$json" | jq -r '.playlists     // 0 | floor' 2>/dev/null || echo 0)"
  users="$(echo "$json"     | jq -r '.users         // 0 | floor' 2>/dev/null || echo 0)"
  uptime="$(echo "$json"    | jq -r '.uptimeSeconds // 0 | floor' 2>/dev/null || echo 0)"
  emit trackerstream_up            "1 if the trackerstream API /healthz responded with valid JSON" 1
  emit trackerstream_modules       "Number of catalog modules"   "$modules"
  emit trackerstream_playlists     "Number of playlists"         "$playlists"
  emit trackerstream_users         "Number of user accounts"     "$users"
  emit trackerstream_uptime_seconds "API process uptime in seconds" "$uptime"
else
  emit trackerstream_up "1 if the trackerstream API /healthz responded with valid JSON" 0
fi

mv "$TMP" "$TEXTFILE"
trap - EXIT
chmod 644 "$TEXTFILE"
echo "wrote $TEXTFILE"
