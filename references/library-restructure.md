# Library Restructure

Use this reference when the user wants to turn a mixed Feishu base into a cleaner strategy system.

## Target Shape

The recommended target is a 5-table layout:

1. `策略主表`
2. `任务场景子表`
3. `GitHub仓库总表`
4. `Skill Pattern子表`
5. `素材/逐字稿子表`

## Roles

- `策略主表`: the only decision entry for complex tasks.
- `任务场景子表`: controlled vocabulary for routing and default recommendations.
- `GitHub仓库总表`: fact source for repo, skill, install, and verification metadata.
- `Skill Pattern子表`: implementation-layer pattern registry derived from repos and skills.
- `素材/逐字稿子表`: raw evidence, transcripts, meeting notes, and source links.

## Flow

- `GitHub -> GitHub仓库总表 / Skill Pattern子表 -> 候选策略审核 -> 策略主表`
- `素材/逐字稿子表 -> 提炼策略候选 -> 人工确认 -> 策略主表`

## Guardrails

- Do not put full transcripts into `策略主表`.
- Do not let GitHub sync overwrite human judgment fields in `策略主表`.
- Allow duplicate material titles, but require stable IDs such as `素材ID`.
- Always dry-run writes before `--apply`.
