#!/bin/bash
# Outboxer — Gmail sync daemon
# Runs the Gmail sync in daemon mode (every 15 minutes).
# Managed by launchd (com.outboxer.gmail-sync)

set -euo pipefail

LOG_DIR="$HOME/.outboxer/logs"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_DIR/sync-gmail.log"
}

log "Gmail sync daemon starting..."

# Run the sync in daemon mode
cd /path/to/your/outboxer
exec /opt/homebrew/bin/node --import tsx src/sync-gmail.ts --daemon \
  >> "$LOG_DIR/sync-gmail.log" 2>&1
