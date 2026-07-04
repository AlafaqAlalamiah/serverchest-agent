#!/bin/bash
# ServerChest agent release: publish 0.0.2 → main (self-update channel), wait for
# GitHub's raw CDN to serve it, then optionally trigger update on given servers.
#
# Usage:
#   ./deploy-agent.sh "commit message"            # commit staged work, push, publish to main
#   ./deploy-agent.sh "msg" --update 2            # …and trigger update_agent on server id 2
#   ./deploy-agent.sh --update 2                  # publish current HEAD + trigger (no new commit)
set -euo pipefail
cd "$(dirname "$0")"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
ok(){ echo -e "${GREEN}✓${NC} $*"; }; info(){ echo -e "${CYAN}→${NC} $*"; }
warn(){ echo -e "${YELLOW}⚠${NC} $*"; }; die(){ echo -e "${RED}✗ $*${NC}"; exit 1; }

RAW="https://raw.githubusercontent.com/AlafaqAlalamiah/serverchest-agent/main/agent.py"
MSG=""; UPDATE_IDS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --update) shift; UPDATE_IDS+=("$1") ;;
    *) MSG="$1" ;;
  esac
  shift
done

[[ "$(git rev-parse --abbrev-ref HEAD)" == "0.0.2" ]] || die "Not on branch 0.0.2 (on $(git rev-parse --abbrev-ref HEAD))."

# 1. Commit staged/all changes if a message was given.
if [[ -n "$MSG" ]]; then
  git add -A
  git commit -m "$MSG" || warn "nothing to commit"
elif ! git diff-index --quiet HEAD --; then
  die "Uncommitted changes and no commit message given. Pass a message or commit first."
fi

# 2. Push 0.0.2.
info "Pushing 0.0.2…"; git push -q origin 0.0.2; ok "0.0.2 pushed"

# 3. Publish to main, keeping it byte-identical to 0.0.2 (histories diverged).
info "Publishing to main…"
git fetch -q origin main
git branch -qD _release 2>/dev/null || true
git checkout -q -b _release origin/main
git merge -q 0.0.2 -m "Merge 0.0.2: ${MSG:-release}" || die "merge conflict — resolve manually"
if [[ -n "$(git diff _release 0.0.2)" ]]; then
  git checkout -q 0.0.2; git branch -qD _release
  die "main would differ from 0.0.2 after merge — inspect manually, not pushing."
fi
git push -q origin _release:main
git checkout -q 0.0.2; git branch -qD _release
ok "main published (identical to 0.0.2)"

# 4. Wait for GitHub raw CDN to serve the new agent.py (compare content hashes).
LOCAL=$(shasum -a 256 agent.py | awk '{print $1}')
info "Waiting for CDN to serve the new agent.py…"
FRESH=0
for i in $(seq 1 10); do
  REMOTE=$(curl -s -H 'Cache-Control: no-cache' "$RAW" | shasum -a 256 | awk '{print $1}')
  [[ "$LOCAL" == "$REMOTE" ]] && { FRESH=1; break; }
  sleep 30
done
[[ "$FRESH" == 1 ]] && ok "CDN serving the new version" || warn "CDN not yet fresh after ~5m (agents will still get it once cached)"

# 5. Trigger update_agent on requested servers via the relay (on timeman).
for id in "${UPDATE_IDS[@]}"; do
  info "Triggering update_agent on server $id…"
  ssh timeman "SECRET=\$(pm2 env 4 | awk -F': ' '/RELAY_INTERNAL_SECRET/{print \$2}'); \
    curl -s -X POST -H \"x-relay-secret: \$SECRET\" -H 'Content-Type: application/json' \
      -d '{\"action\":\"update_agent\",\"params\":{},\"timeout\":60000}' http://127.0.0.1:3006/send/$id | head -c 60; \
    echo; sleep 8; \
    echo -n 'reconnected: '; curl -s -H \"x-relay-secret: \$SECRET\" http://127.0.0.1:3006/status/$id"
  echo
done

ok "Agent release complete."
