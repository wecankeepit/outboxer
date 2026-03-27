#!/bin/bash
# Outboxer — Google Contacts sync daemon
# Runs the contacts sync in daemon mode (every 15 minutes).
# Managed by launchd (com.outboxer.contacts-sync)

set -euo pipefail

LOG_DIR="$HOME/.outboxer/logs"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_DIR/sync-contacts.log"
}

log "Contacts sync daemon starting..."

# Run the sync in daemon mode
cd /path/to/your/outboxer
exec /opt/homebrew/bin/node --import tsx src/sync-google-contacts.ts --daemon \
  >> "$LOG_DIR/sync-contacts.log" 2>&1
