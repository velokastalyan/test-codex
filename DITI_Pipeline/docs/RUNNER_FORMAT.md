# Runner Format

The docs runner is the watcher process installed as a LaunchAgent. It invokes:

```
/bin/zsh -lc "$HOME/Foto/DITI_Pipeline/05_tools/docs_watch.sh"
```

This runner is designed for macOS and should run in a user GUI session.
