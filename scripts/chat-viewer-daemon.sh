#!/bin/bash
# Outboxer — Chat viewer daemon
# Runs the chat viewer web server on port 3847.
# Managed by launchd (com.outboxer.chat-viewer)

set -euo pipefail

LOG_DIR="$HOME/.outboxer/logs"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_DIR/chat-viewer.log"
}

log "Chat viewer starting..."

cd /path/to/your/outboxer
# Served under IonClaw Vite + Cloudflare tunnel at https://your-domain.com/outboxer/
export BASE_PATH=/outboxer
exec /opt/homebrew/bin/node --import tsx src/chat-viewer.ts \
  >> "$LOG_DIR/chat-viewer.log" 2>&1
