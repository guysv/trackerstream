#!/usr/bin/env bash
# Sync the current repo tree to the LAN dev peer over SSH using git: push the current
# branch straight into the peer's checkout and fast-forward its working tree (via
# receive.denyCurrentBranch=updateInstead). No GitHub round-trip — LAN only.
#
# Reads the peer IP from DEV_PEER_ADDR (written by the find-lan-peer skill). Env overrides:
#   DEV_PEER_USER    ssh user on the peer      (default: $USER)
#   DEV_PEER_PATH    repo path on the peer     (default: same absolute path as here)
#   DEV_PEER_SSH_KEY identity file             (default: ~/.ssh/id_ed25519)
#
# The peer's working tree must be clean and on the same branch for the fast-forward to
# apply; otherwise the push is rejected and nothing changes.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
ADDR_FILE="$REPO_ROOT/DEV_PEER_ADDR"
[ -f "$ADDR_FILE" ] || { echo "missing $ADDR_FILE — run the find-lan-peer skill first" >&2; exit 1; }

ADDR="$(tr -d '[:space:]' < "$ADDR_FILE")"
[ -n "$ADDR" ] || { echo "$ADDR_FILE is empty" >&2; exit 1; }

PEER_USER="${DEV_PEER_USER:-$USER}"
PEER_PATH="${DEV_PEER_PATH:-$REPO_ROOT}"
SSH_KEY="${DEV_PEER_SSH_KEY:-$HOME/.ssh/id_ed25519}"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
REMOTE="$PEER_USER@$ADDR"

# Force a single identity: a loaded agent with many keys trips "too many auth failures".
SSH="ssh -o IdentitiesOnly=yes -i $SSH_KEY"
export GIT_SSH_COMMAND="$SSH"

echo "syncing $BRANCH -> $REMOTE:$PEER_PATH"

# Let the peer accept a push to its checked-out branch and update its worktree.
$SSH "$REMOTE" "git -C '$PEER_PATH' config receive.denyCurrentBranch updateInstead"

git push "ssh://$REMOTE$PEER_PATH" "$BRANCH:$BRANCH"

echo -n "peer HEAD now: "
$SSH "$REMOTE" "git -C '$PEER_PATH' rev-parse --short HEAD"
