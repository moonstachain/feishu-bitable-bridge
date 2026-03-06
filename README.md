# Feishu Bitable Bridge

## What it is
Feishu Bitable Bridge is a Codex skill for inspecting Feishu wiki or base links, extracting bitable schema and records, and safely syncing structured data back into Feishu after a dry-run preview.

## Who it's for
This repo is for operators who manage Feishu bitables from Codex workflows and need a reliable way to inspect tables, export strategy-library materials, or upsert records without manual clicking.

## Quick start
```bash
python3 scripts/feishu_bitable_bridge.py inspect-link --link 'https://h52xu4gwob.feishu.cn/wiki/...' --limit 5
```

## Inputs
- A Feishu `wiki` or `base` link.
- A logged-in browser session that can access the target Feishu page.
- Optional payload JSON when running upserts.
- Optional app token when exporting or mirroring a Feishu strategy library.

## Outputs
- Table schema and visible records exported into `artifacts/feishu-bitable-bridge/`.
- Library blueprint and seed files for strategy-library restructuring.
- Safe upsert previews before any write happens.
- Optional GitHub-ready mirror files under `exports/feishu-strategy-library/`.

## Constraints
- Writes must always be previewed first with `--dry-run`.
- This skill does not delete Feishu records.
- If the primary field is ambiguous or missing, the workflow must stop.
- The workflow depends on a valid Feishu browser session.

## Example
Use this skill when you have a Feishu strategy library and want to inspect the schema, generate normalized seed files, and then upsert cleaned records back into the main table after reviewing a preview JSON.

## Project structure
- `scripts/`: Feishu inspection, export, sync, and upsert entrypoints.
- `references/`: payload format, write-mode, and strategy-library design notes.
- `agents/`: Codex interface metadata.
- `state/`: persistent browser profile data for Feishu login reuse.

