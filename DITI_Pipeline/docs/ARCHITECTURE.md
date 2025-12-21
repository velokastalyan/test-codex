# Architecture

## Overview

DITI_Pipeline treats Markdown as the single source of truth and generates two derivative outputs:

- `Automat/*.rtf` — readable RTF mirrors generated from Markdown.
- `Automat/UPLOAD_MD/` — a clean, upload-ready Markdown bundle.

## Key Paths

- Source: `DITI_Pipeline/docs/**/*.md`, `DITI_Pipeline/README.md`, `DITI_Pipeline/CHANGELOG.md`
- RTF output: `DITI_Pipeline/Automat/` (mirrors the docs structure)
- Upload bundle: `DITI_Pipeline/Automat/UPLOAD_MD/`
- Logs: `DITI_Pipeline/04_logs/`
