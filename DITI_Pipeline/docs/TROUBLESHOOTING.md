# Troubleshooting

## RTF Not Updating

- Run `./05_tools/docs_export_to_automat.sh` manually.
- Check `04_logs/docs_export_<TS>.log` for `textutil` errors.

## Upload Pack Missing Files

- Rebuild via `./05_tools/docs_pack_for_chatbot.sh`.
- Confirm Markdown files are under `docs/` and end in `.md`.

## Watcher Not Running

- Verify the LaunchAgent with `launchctl print`.
- Reinstall using `./05_tools/docs_watch_install.sh`.
