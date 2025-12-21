#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT_DIR/04_logs"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/docs_export_${TS}.log"

mkdir -p "$LOG_DIR"

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"
}

log "Starting docs export to Automat."

SOURCES=()
while IFS= read -r line; do
  SOURCES+=("$line")
done < <(
  {
    find "$ROOT_DIR/docs" -type f -name "*.md"
    printf '%s\n' "$ROOT_DIR/README.md" "$ROOT_DIR/CHANGELOG.md"
  } | sort
)

for SRC in "${SOURCES[@]}"; do
  if [ ! -f "$SRC" ]; then
    log "Skipping missing file: $SRC"
    continue
  fi
  REL_PATH="${SRC#$ROOT_DIR/}"
  OUT_PATH="$ROOT_DIR/Automat/${REL_PATH%.md}.rtf"
  OUT_DIR="$(dirname "$OUT_PATH")"
  mkdir -p "$OUT_DIR"

  log "Converting $REL_PATH -> Automat/${REL_PATH%.md}.rtf"
  textutil -convert txt -stdout "$SRC" \
    | textutil -convert rtf -stdin -stdout \
    > "$OUT_PATH"

done

log "Docs export completed."
