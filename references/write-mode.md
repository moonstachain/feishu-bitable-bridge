# Write Mode

This skill uses two layers:

1. Browser login session
   - used to open the Feishu link and recover table metadata plus existing records
2. Optional Feishu OpenAPI credentials
   - used only for `--apply` to create or update rows

## Why Reads And Writes Differ

The browser session is already proven to recover:

- base token
- table id
- view id
- fields
- records

The actual write submission path is kept strict and explicit. `--apply` requires:

- a clean dry-run preview
- explicit user confirmation
- `FEISHU_APP_ID` and `FEISHU_APP_SECRET`, unless alternate env names are provided

## Constraints

- No delete support
- No hidden automatic fallback
- No write without preview
- No silent match heuristics beyond the resolved primary field
