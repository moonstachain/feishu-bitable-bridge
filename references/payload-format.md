# Payload Format

`upsert-records` reads one JSON object or one JSON array from `--payload-file`.

## Accepted Shapes

Single record:

```json
{
  "文本": "业务赋能工具及私教规划",
  "标题": "新的标题",
  "摘要": "新的摘要"
}
```

Multiple records:

```json
[
  {
    "文本": "业务赋能工具及私教规划",
    "标题": "新的标题"
  },
  {
    "文本": "新的主键",
    "标题": "新增记录"
  }
]
```

## Matching Rules

- `primary_field` is used to match existing records.
- If `primary_field` is omitted, the script tries the table primary field.
- If the incoming record does not include the match field, the preview reports an error.
- Duplicate match values in existing records or in the incoming payload are treated as errors.

## Preview Semantics

- `creates`: records that do not match an existing row
- `updates`: records that match an existing row and contain changed fields
- `unchanged`: matched records with no effective field changes
- `errors`: anything that blocks `--apply`

## Field Support

First version support is optimized for text-like values that can be represented as strings.

- plain text
- text derived from rich-text cells
- mention/link text flattened into readable strings for preview

Do not rely on this version for attachments, images, or complex formula fields.
