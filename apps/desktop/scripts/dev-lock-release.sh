#!/usr/bin/env bash
# Stop hook: fires when Claude finishes a turn. If the dev tree is locked
# (Claude edited Rust/Go this session, via dev-lock-acquire.sh), release the
# lock ONLY if the code compiles — which triggers exactly one clean rebuild in
# dev-locked.mjs. Broken or mid-batch code stays locked so the running app keeps
# its last good build. Never blocks the turn.
DESKTOP="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$DESKTOP/../.." && pwd)"
LOCK="$DESKTOP/.dev-lock"

[ -f "$LOCK" ] || exit 0   # tree not locked → Claude didn't touch Rust/Go → nothing to do

ok=1
( cd "$DESKTOP/src-tauri" && cargo check --quiet ) >/dev/null 2>&1 || ok=0
[ "$ok" = 1 ] && { ( cd "$REPO/node" && go build ./... ) >/dev/null 2>&1 || ok=0; }

if [ "$ok" = 1 ]; then
  rm -f "$LOCK"
  echo '{"systemMessage":"✅ dev tree unlocked — app rebuilding with your changes"}'
else
  echo '{"systemMessage":"🔒 dev tree still locked — Rust/Go does not compile yet (app kept on last good build)"}'
fi
exit 0
