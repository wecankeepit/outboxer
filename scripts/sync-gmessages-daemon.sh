#!/bin/bash
# Outboxer — Google Messages sync daemon
# Polls the local Matrix bridge for new messages every 15 seconds.
# Managed by launchd (com.outboxer.gmessages-sync)

set -euo pipefail

LOG_DIR="$HOME/.outboxer/logs"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_DIR/sync-gmessages.log"
}

log "Google Messages sync daemon starting..."

cd /path/to/your/outboxer
exec /opt/homebrew/bin/node --import tsx src/sync-gmessages.ts --daemon \
  >> "$LOG_DIR/sync-gmessages.log" 2>&1
