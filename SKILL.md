---
name: feishu-bitable-bridge
description: >
  飞书多维表格（Bitable）的读写桥接层，支持建表、读记录、写记录、更新和批量操作。
  当需要操作飞书多维表数据、创建新表、批量写入数据、或同步数据到飞书时使用。
  当用户说"写入飞书"、"更新多维表"、"建一张新表"、"同步到飞书"时使用。
  NOT for 飞书文档或知识库操作（用 mcp-feishu-read-doc 或 mcp-feishu-read-wiki）。
---

# Feishu Bitable Bridge

Use this skill to take over a Feishu bitable from a wiki/base link with the user's browser login session, keep the Feishu strategy library clean, and export a GitHub mirror.

## When To Use

- The user gives a Feishu `wiki` or `base` link and wants the table schema or records.
- The user wants to inspect field names, primary field, table id, view id, or visible records.
- The user wants to upsert rows safely into a Feishu bitable after reviewing a dry-run preview.
- The user wants to restructure a Feishu strategy library, fill empty strategy fields, or mirror the current library to GitHub.

## Workflow

1. Reuse the persistent Chrome profile stored in `state/browser-profile`.
2. Open the Feishu link. If the session is not logged in, let the user finish login in the browser window.
3. Read `client_vars` and the page runtime to recover:
   - `obj_token`
   - `table_id`
   - `view_id`
   - fields
   - records
4. For writes, always run `upsert-records --dry-run` first.
5. Only run `--apply` after the user explicitly confirms the preview.

## Commands

Inspect a link and export schema plus records:

```bash
python3 /Users/liming/.codex/skills/feishu-bitable-bridge/scripts/feishu_bitable_bridge.py inspect-link --link 'https://h52xu4gwob.feishu.cn/wiki/...' --limit 5
```

Generate the 5-table restructuring blueprint from existing exports:

```bash
python3 /Users/liming/.codex/skills/feishu-bitable-bridge/scripts/feishu_bitable_bridge.py build-library-blueprint --materials-file materials-inspect.json --github-file github-inspect.json
```

Generate seed payloads for `策略主表`、`任务场景子表`、`GitHub仓库总表`、`Skill Pattern子表`、`素材/逐字稿子表`:

```bash
python3 /Users/liming/.codex/skills/feishu-bitable-bridge/scripts/feishu_bitable_bridge.py build-library-seeds --materials-file materials-inspect.json --github-file github-inspect.json
```

The strategy seed now includes:
- `策略名称-CN`
- `适用输入`
- `预期输出`
- `不适用条件`

These fields are generated as machine suggestions. Sync keeps strong fact fields current, fills empty human fields conservatively, and does not overwrite `策略负责人`.

Mirror the current 5-table library into structured files for GitHub review:

```bash
python3 /Users/liming/.codex/skills/feishu-bitable-bridge/scripts/feishu_bitable_bridge.py export-library-mirror --app-token 'Q7vvbO8rHauu1asY4J4cZAEcn6e'
```

Publish that mirror into a git repo and commit it:

```bash
python3 /Users/liming/.codex/skills/feishu-bitable-bridge/scripts/feishu_bitable_bridge.py publish-library-mirror --mirror-dir exports/feishu-strategy-library --repo-dir /path/to/repo --commit-only
```

Generate a safe preview for upsert:

```bash
python3 /Users/liming/.codex/skills/feishu-bitable-bridge/scripts/feishu_bitable_bridge.py upsert-records --link 'https://h52xu4gwob.feishu.cn/wiki/...' --primary-field '文本' --payload-file records.json --dry-run
```

Apply the upsert after confirmation:

```bash
python3 /Users/liming/.codex/skills/feishu-bitable-bridge/scripts/feishu_bitable_bridge.py upsert-records --link 'https://h52xu4gwob.feishu.cn/wiki/...' --primary-field '文本' --payload-file records.json --apply
```

## Safety Rules

- Never write before showing a preview.
- Treat `--apply` as a second explicit step, not a default.
- Do not delete records in this skill.
- If the primary field cannot be resolved, stop and report the issue.
- If duplicate primary values make matching ambiguous, stop and report the issue.

## Expected Outputs

- Inspect output: `artifacts/feishu-bitable-bridge/inspect-*.json`
- Raw probe output: `artifacts/feishu-bitable-bridge/probe-*.json`
- Library blueprint: `artifacts/feishu-bitable-bridge/library-blueprint-*.json` and `.md`
- Library seeds: `artifacts/feishu-bitable-bridge/library-seeds-*/`
- Library mirror export: `exports/feishu-strategy-library/` plus `artifacts/feishu-bitable-bridge/library-mirror-export-*.json`
- Mirror publish result: `artifacts/feishu-bitable-bridge/library-mirror-publish-*.json`
- Upsert preview: `artifacts/feishu-bitable-bridge/upsert-preview-*.json`
- Apply result: `artifacts/feishu-bitable-bridge/upsert-apply-*.json`

## Notes

- Read `references/payload-format.md` for payload rules and preview semantics.
- Read `references/write-mode.md` when the user asks how writes are committed or what constraints apply.
- Read `references/library-restructure.md` when the user wants to redesign a Feishu strategy library around strategy, GitHub, skill pattern, and transcript tables.
- Read `references/strategy-name-overrides.json` when the user wants to fine-tune Chinese strategy names without editing code.
