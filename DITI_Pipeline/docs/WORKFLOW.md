# Workflow

## Daily Flow

1. Edit Markdown docs inside `DITI_Pipeline/docs/`.
2. The watcher runs `docs_export_to_automat.sh` and `docs_pack_for_chatbot.sh`.
3. RTF mirrors appear in `Automat/` and upload-ready Markdown appears in `Automat/UPLOAD_MD/`.

## Manual Runs

```sh
./05_tools/docs_export_to_automat.sh
./05_tools/docs_pack_for_chatbot.sh
```
