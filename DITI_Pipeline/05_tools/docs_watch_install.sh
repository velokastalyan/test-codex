#!/bin/zsh
set -euo pipefail

PLIST_PATH="$HOME/Library/LaunchAgents/com.diti.pipeline.docswatch.plist"
AGENT_LABEL="com.diti.pipeline.docswatch"
RUN_CMD="$HOME/Foto/DITI_Pipeline/05_tools/docs_watch.sh"

mkdir -p "$HOME/Library/LaunchAgents"

cat <<PLIST > "$PLIST_PATH"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${AGENT_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>${RUN_CMD}</string>
  </array>
  <key>KeepAlive</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"

printf '\nInstalled LaunchAgent at %s\n' "$PLIST_PATH"
printf '\nVerify with:\n'
printf '  launchctl print gui/%s/%s\n' "$(id -u)" "$AGENT_LABEL"
printf '  tail -n 50 "$HOME/Foto/DITI_Pipeline/04_logs/docs_watch_current.log"\n'
printf '  touch "$HOME/Foto/DITI_Pipeline/docs/ARCHITECTURE.md"\n'
