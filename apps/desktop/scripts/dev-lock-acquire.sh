#!/usr/bin/env bash
# PreToolUse(Edit|Write) hook: lock the dev tree the instant Claude edits a
# Rust or Go file, so dev-locked.mjs defers rebuilds until the batch is released
# (by dev-lock-release.sh, once it compiles). Idempotent, never blocks.
#
# stdin: {"tool_input":{"file_path":"…"}, …}
DESKTOP="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK="$DESKTOP/.dev-lock"

path="$(jq -r '.tool_input.file_path // empty' 2>/dev/null)"
case "$path" in
  *.rs|*.go) touch "$LOCK" ;;
esac
exit 0
