#!/usr/bin/env bash
# Sweep The Mod Archive's XML API for genre-classified modules and save raw XML dumps.
#
# Genre is a browse axis on TMA: request=search&type=genre&query=<genreid> returns every
# module of that genre, 40 per page, each carrying its module id + md5 hash. So one request
# yields 40 already-classified modules — ~230x cheaper than a per-module lookup. The whole
# archive (~29,638 genred modules across 77 genres) sweeps in ~780 requests.
#
# Usage:
#   scripts/tma-genre-sweep.sh <genreid> [<genreid> ...]   # sweep specific genres
#   scripts/tma-genre-sweep.sh --all                       # sweep every genre in view_genres
#
# Raw XML is written to ./api-dumps/genre-<id>-page-<n>.xml (resumable: existing pages are
# skipped, so a re-run only fetches what's missing). The genre index is cached at
# ./api-dumps/view_genres.xml.
#
# Env overrides:
#   MODARCHIVE_KEY   API key (default: contents of ~/tmp/MODARCHIVETOK)
#   TMA_DELAY        seconds to sleep between requests (default: 1.0)
#   OUT_DIR          output directory (default: ./api-dumps)
set -euo pipefail

KEY="${MODARCHIVE_KEY:-$(tr -d '[:space:]' < "$HOME/tmp/MODARCHIVETOK")}"
[ -n "$KEY" ] || { echo "no API key (set MODARCHIVE_KEY or ~/tmp/MODARCHIVETOK)" >&2; exit 1; }

DELAY="${TMA_DELAY:-1.0}"
OUT_DIR="${OUT_DIR:-./api-dumps}"
BASE="https://modarchive.org/data/xml-tools.php?key=$KEY"
mkdir -p "$OUT_DIR"

# fetch <url> <destfile> — skip if dest already exists (resume), sleep after a real fetch.
fetch() {
  local url="$1" dest="$2"
  if [ -s "$dest" ]; then echo "  skip (cached) $(basename "$dest")"; return; fi
  curl -sS --fail-with-body "$url" -o "$dest"
  sleep "$DELAY"
}

# extract the integer inside the first <tag>...</tag>
xml_int() { grep -o "<$2>[0-9]*</$2>" "$1" | head -1 | grep -o '[0-9]*'; }

sweep_genre() {
  local gid="$1"
  echo "genre $gid:"
  local p1="$OUT_DIR/genre-$gid-page-1.xml"
  fetch "$BASE&request=search&type=genre&query=$gid&page=1" "$p1"
  if grep -q '<error>' "$p1"; then echo "  no results / error"; return; fi
  local pages; pages="$(xml_int "$p1" totalpages)"; pages="${pages:-1}"
  [ "$pages" -lt 1 ] && pages=1
  echo "  totalpages=$pages"
  local n
  for ((n=2; n<=pages; n++)); do
    fetch "$BASE&request=search&type=genre&query=$gid&page=$n" "$OUT_DIR/genre-$gid-page-$n.xml"
  done
}

# --all: pull the genre index and sweep every child genre id it lists.
if [ "${1:-}" = "--all" ]; then
  gx="$OUT_DIR/view_genres.xml"
  [ -s "$gx" ] || { curl -sS --fail-with-body "$BASE&request=view_genres" -o "$gx"; sleep "$DELAY"; }
  # child genres carry the real file counts; parents are umbrella categories.
  # (read loop instead of mapfile — macOS ships bash 3.2, which has no mapfile.)
  GENRES=()
  while IFS= read -r gid; do [ -n "$gid" ] && GENRES+=("$gid"); done < <(
    python3 -c "import re; print('\n'.join(re.findall(r'<child>\s*<text>.*?</text>\s*<id>(\d+)</id>', open('$gx').read(), re.S)))")
  set -- "${GENRES[@]}"
  echo "sweeping ${#GENRES[@]} genres from $gx"
fi

[ "$#" -ge 1 ] || { echo "usage: $0 <genreid> [...] | --all" >&2; exit 1; }

for gid in "$@"; do sweep_genre "$gid"; done

echo "---"
echo "pages on disk: $(ls "$OUT_DIR"/genre-*-page-*.xml 2>/dev/null | wc -l | tr -d ' ')"
echo "quota: $(curl -sS "$BASE&request=view_requests" | grep -o '<current>[0-9]*</current>' | grep -o '[0-9]*')/1500 used this month (view_requests is free)"
