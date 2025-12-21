#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT_DIR/04_logs"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/docs_pack_${TS}.log"
UPLOAD_DIR="$ROOT_DIR/Automat/UPLOAD_MD"

mkdir -p "$LOG_DIR"

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"
}

log "Rebuilding $UPLOAD_DIR"
rm -rf "$UPLOAD_DIR"
mkdir -p "$UPLOAD_DIR/docs"

cp "$ROOT_DIR/README.md" "$UPLOAD_DIR/README.md"
cp "$ROOT_DIR/CHANGELOG.md" "$UPLOAD_DIR/CHANGELOG.md"

while IFS= read -r file; do
  rel_path="${file#$ROOT_DIR/docs/}"
  dest="$UPLOAD_DIR/docs/$rel_path"
  mkdir -p "$(dirname "$dest")"
  cp "$file" "$dest"
done < <(find "$ROOT_DIR/docs" -type f -name "*.md")

manifest="$UPLOAD_DIR/MANIFEST.txt"
{
  printf 'Pack Timestamp: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')"
  printf '\nFiles:\n'
  (cd "$UPLOAD_DIR" && find . -type f -name "*.md" | sort)
} > "$manifest"

log "Wrote manifest to $manifest"

log "Opening Finder on $UPLOAD_DIR"
open "$UPLOAD_DIR" || log "Finder open failed (non-macOS environment?)"

log "Docs pack complete."
