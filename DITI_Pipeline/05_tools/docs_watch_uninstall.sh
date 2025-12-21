#!/bin/zsh
set -euo pipefail

PLIST_PATH="$HOME/Library/LaunchAgents/com.diti.pipeline.docswatch.plist"
AGENT_LABEL="com.diti.pipeline.docswatch"

if [ -f "$PLIST_PATH" ]; then
  launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
  rm -f "$PLIST_PATH"
fi

printf '\nRemoved LaunchAgent %s\n' "$AGENT_LABEL"
printf '\nVerify with:\n'
printf '  launchctl print gui/%s/%s\n' "$(id -u)" "$AGENT_LABEL"
printf '  tail -n 50 "$HOME/Foto/DITI_Pipeline/04_logs/docs_watch_current.log"\n'
printf '  touch "$HOME/Foto/DITI_Pipeline/docs/ARCHITECTURE.md"\n'
