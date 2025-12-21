# Debugging

## Common Checks

- Confirm logs in `DITI_Pipeline/04_logs/`.
- Ensure `textutil` is available (`/usr/bin/textutil`).
- Confirm `fswatch` is installed if you want low-latency watching.

## Watcher Verification

```sh
launchctl print gui/$(id -u)/com.diti.pipeline.docswatch
```
