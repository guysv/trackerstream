#!/usr/bin/env bash
# Clear local desktop-client storage while preserving the peer identity.
#
# The client's data lives under the Tauri app-data dir (identifier xyz.trackerstream):
#   tsnode/datastore/   LevelDB: cached blocks + pins + DHT/IPNS records + catalog VFS
#                       pages   -> this is "stored storage", safe to wipe
#   tsnode/identity.key ed25519 peer id                 -> PRESERVED (never touched)
#   tsnode/keystore/    named IPNS signing keys          -> PRESERVED
#   held_roots.json     mirrors which roots are held     -> cleared with the datastore
#   ipns_cache.json     cached IPNS resolutions          -> cleared with the datastore
#   playlists.db        your playlists (user data)       -> only with --playlists
#
# Two independent operations, each behind a confirmation prompt:
#   blocks    -> remove tsnode/datastore + the two bookkeeping caches (keeps the id)
#   playlists -> remove playlists.db (+ its -wal/-shm)
#
# Usage:
#   scripts/client_clear.sh                # interactive: asks about each
#   scripts/client_clear.sh --blocks       # only clear the block cache
#   scripts/client_clear.sh --playlists    # only clear playlists
#   scripts/client_clear.sh --all          # both
#   scripts/client_clear.sh --all --yes    # both, no prompts (for automation)
# Env override:
#   TS_APP_DATA   app-data dir (default: OS-specific path for xyz.trackerstream)
#
# The app MUST be quit first: tsnode holds a LevelDB lock on the datastore while it
# runs, so wiping it under a live node would corrupt the store. The script refuses to
# run if a tsnode/trackerstream process is alive.
set -euo pipefail

ID="xyz.trackerstream"

# Resolve the Tauri app_data_dir for this platform (override with TS_APP_DATA).
default_app_data() {
  case "$(uname -s)" in
    Darwin) printf '%s/Library/Application Support/%s' "$HOME" "$ID" ;;
    Linux)  printf '%s/%s' "${XDG_DATA_HOME:-$HOME/.local/share}" "$ID" ;;
    *)      return 1 ;;
  esac
}
APP="${TS_APP_DATA:-$(default_app_data)}" || { echo "unsupported OS; set TS_APP_DATA" >&2; exit 1; }

do_blocks=false
do_playlists=false
assume_yes=false
for a in "$@"; do
  case "$a" in
    --blocks)    do_blocks=true ;;
    --playlists) do_playlists=true ;;
    --all)       do_blocks=true; do_playlists=true ;;
    --yes|-y)    assume_yes=true ;;
    -h|--help)   sed -n '2,30p' "$0"; exit 0 ;;
    *)           echo "unknown arg: $a (try --help)" >&2; exit 1 ;;
  esac
done
# No operation flags -> interactive mode: offer each in turn.
interactive=false
if ! $do_blocks && ! $do_playlists; then interactive=true; fi

[ -d "$APP" ] || { echo "no client data dir at: $APP" >&2; exit 1; }

# Refuse to touch the store while the node is live (holds the datastore LOCK).
if pgrep -x tsnode >/dev/null 2>&1 || pgrep -x trackerstream >/dev/null 2>&1; then
  echo "refusing: the client (tsnode/trackerstream) is running — quit the app first." >&2
  exit 1
fi

# Human-readable size of a path, "-" if absent.
sizeof() { [ -e "$1" ] && du -sh "$1" 2>/dev/null | cut -f1 || echo "-"; }

# Prompt unless --yes. Returns 0 to proceed, 1 to skip.
confirm() {
  $assume_yes && return 0
  local reply
  read -r -p "$1 [y/N] " reply </dev/tty
  [[ "$reply" =~ ^[Yy]$ ]]
}

echo "client data dir: $APP"
echo

clear_blocks() {
  local ds="$APP/tsnode/datastore"
  echo "-- blocks --"
  echo "  datastore:       $(sizeof "$ds")   ($ds)"
  echo "  held_roots.json: $(sizeof "$APP/held_roots.json")"
  echo "  ipns_cache.json: $(sizeof "$APP/ipns_cache.json")"
  echo "  KEEPS identity.key + keystore/ (peer id unchanged)"
  if confirm "clear the block cache?"; then
    rm -rf "$ds"
    rm -f "$APP/held_roots.json" "$APP/ipns_cache.json"
    echo "  cleared blocks."
  else
    echo "  skipped blocks."
  fi
  echo
}

clear_playlists() {
  echo "-- playlists --"
  echo "  playlists.db:    $(sizeof "$APP/playlists.db")   (user data — playlists/likes)"
  echo "  KEEPS the playlist signing key in tsnode/keystore/"
  if confirm "clear playlists (irreversible)?"; then
    rm -f "$APP/playlists.db" "$APP/playlists.db-wal" "$APP/playlists.db-shm"
    echo "  cleared playlists."
  else
    echo "  skipped playlists."
  fi
  echo
}

if $interactive; then
  clear_blocks
  clear_playlists
else
  $do_blocks && clear_blocks
  $do_playlists && clear_playlists
fi

echo "done."
