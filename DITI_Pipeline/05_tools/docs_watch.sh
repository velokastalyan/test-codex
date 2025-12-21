#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT_DIR/04_logs"
LOG_FILE="$LOG_DIR/docs_watch_current.log"
EXPORT_SCRIPT="$SCRIPT_DIR/docs_export_to_automat.sh"
PACK_SCRIPT="$SCRIPT_DIR/docs_pack_for_chatbot.sh"

mkdir -p "$LOG_DIR"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  printf '%s %s\n' "$(timestamp)" "$*" >> "$LOG_FILE"
}

run_sync() {
  log "Change detected. Running export + pack."
  "$EXPORT_SCRIPT" >> "$LOG_FILE" 2>&1
  "$PACK_SCRIPT" >> "$LOG_FILE" 2>&1
  log "Sync complete."
}

watch_paths=(
  "$ROOT_DIR/docs"
  "$ROOT_DIR/README.md"
  "$ROOT_DIR/CHANGELOG.md"
)

log "Starting docs watch."

if command -v fswatch >/dev/null 2>&1; then
  log "Using fswatch with 2s latency."
  fswatch -o -l 2 "${watch_paths[@]}" | while read -r _; do
    run_sync
  done
else
  log "fswatch not found; falling back to polling every 5s."
  prev_sum=""
  while true; do
    current_sum=$(find "${watch_paths[@]}" -type f -name "*.md" -print0 | \
      xargs -0 shasum | shasum | awk '{print $1}')
    if [ "$current_sum" != "$prev_sum" ]; then
      prev_sum="$current_sum"
      run_sync
    fi
    sleep 5
  done
fi
