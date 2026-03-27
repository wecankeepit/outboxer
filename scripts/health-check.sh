#!/bin/bash
# Outboxer — Health check for all services
# Usage: npm run health

set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $*"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $*"; }
fail() { echo -e "  ${RED}✗${NC} $*"; }

check_launchd() {
  local LABEL="$1"
  if launchctl list "$LABEL" >/dev/null 2>&1; then
    local INFO
    INFO=$(launchctl list "$LABEL" 2>/dev/null)
    local PID
    PID=$(echo "$INFO" | grep '"PID"' | grep -o '[0-9]*')
    if [[ -n "$PID" ]]; then
      ok "launchd loaded, PID $PID"
    else
      local EXIT_CODE
      EXIT_CODE=$(echo "$INFO" | grep 'LastExitStatus' | grep -o '[0-9]*')
      warn "launchd loaded but process not running (last exit: ${EXIT_CODE:-unknown})"
    fi
  else
    fail "NOT loaded in launchd"
  fi
}

echo "Outboxer Health Check"
echo "═══════════════════════════════════════"

# ── Chat Viewer ──
echo ""
echo "Chat Viewer (port 3847):"
if lsof -ti :3847 >/dev/null 2>&1; then
  ok "Running on port 3847"
else
  fail "NOT running — start with: npm run viewer"
fi
check_launchd "com.outboxer.chat-viewer"

# ── Google OAuth (gog) ──
echo ""
echo "Google OAuth (gog):"
if command -v gog &>/dev/null; then
  ok "gog CLI installed ($(which gog))"
  if gog auth tokens export your-email@gmail.com --out /tmp/outboxer-hc-$$.json --overwrite &>/dev/null; then
    ok "gog token for your-email@gmail.com is valid"
    rm -f /tmp/outboxer-hc-$$.json
  else
    fail "gog token export failed — re-auth with: gog auth add your-email@gmail.com --services gmail,contacts"
  fi
else
  fail "gog CLI not found — install gogcli"
fi

# ── Gmail Sync ──
echo ""
echo "Gmail Sync Daemon:"
check_launchd "com.outboxer.gmail-sync"
if tail -5 "$HOME/.outboxer/logs/sync-gmail.log" 2>/dev/null | grep -q "AUTH FAILED\|GoogleAuthError"; then
  fail "Recent auth failure in logs — check gog auth"
else
  ok "No recent auth failures"
fi

# ── Contacts Sync ──
echo ""
echo "Contacts Sync Daemon:"
check_launchd "com.outboxer.contacts-sync"
if tail -5 "$HOME/.outboxer/logs/sync-contacts.log" 2>/dev/null | grep -q "AUTH FAILED\|GoogleAuthError"; then
  fail "Recent auth failure in logs — check gog auth"
else
  ok "No recent auth failures"
fi

# ── Google Messages Bridge ──
echo ""
echo "Google Messages Bridge (Docker):"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q bridge-gmessages; then
  ok "mautrix-gmessages container running"
else
  fail "NOT running — start with: npm run bridge:up"
fi
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q bridge-synapse; then
  ok "Synapse container running"
else
  fail "Synapse NOT running"
fi
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q bridge-postgres; then
  ok "PostgreSQL container running"
else
  fail "PostgreSQL NOT running"
fi

# ── GMessages Sync ──
echo ""
echo "GMessages Sync Daemon:"
check_launchd "com.outboxer.gmessages-sync"
if [[ -f "$HOME/.outboxer/matrix-token" ]]; then
  ok "Matrix token present"
else
  fail "No Matrix token — bridge may not be set up"
fi

# ── Database ──
echo ""
echo "Database:"
DB="$HOME/.outboxer/takeout/gmail.db"
if [[ -f "$DB" ]]; then
  SIZE=$(du -h "$DB" | awk '{print $1}')
  ok "Exists ($SIZE)"
  node -e "
    const {DatabaseSync}=require('node:sqlite');
    const db=new DatabaseSync('$DB');
    const r=db.prepare('SELECT source,COUNT(*) as c FROM messages GROUP BY source ORDER BY c DESC').all();
    for(const s of r) console.log('    ' + s.source + ': ' + s.c.toLocaleString() + ' messages');
    const total=db.prepare('SELECT COUNT(*) as c FROM messages').get();
    console.log('    Total: ' + total.c.toLocaleString() + ' messages');
    const latest=db.prepare('SELECT MAX(date) as d FROM messages').get();
    console.log('    Latest: ' + (latest.d||'').slice(0,19));
  " 2>/dev/null
else
  fail "Database not found at $DB"
fi

echo ""
echo "═══════════════════════════════════════"
