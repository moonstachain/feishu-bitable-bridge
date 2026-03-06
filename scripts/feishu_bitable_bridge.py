#!/usr/bin/env python3
import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib import error, parse, request

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
LOGIN_HINTS = ("accounts.feishu.cn", "passport.feishu.cn", "login")
SKILL_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STATE_DIR = SKILL_ROOT / "state" / "browser-profile"
DEFAULT_ARTIFACT_DIR = Path.cwd() / "artifacts" / "feishu-bitable-bridge"
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_WAIT_SECONDS = 10
FEISHU_OPENAPI_BASE = "https://open.feishu.cn"


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def looks_like_login(url: str) -> bool:
    lowered = url.lower()
    return any(token in lowered for token in LOGIN_HINTS)


def flatten_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict):
                if item.get("type") == "text":
                    parts.append(str(item.get("text", "")))
                elif item.get("type") == "mention":
                    parts.append(str(item.get("text") or item.get("link") or item.get("token") or ""))
                else:
                    parts.append(str(item.get("text") or item.get("link") or json.dumps(item, ensure_ascii=False)))
            else:
                parts.append(str(item))
        return "".join(parts)
    if isinstance(value, dict):
        return str(value.get("text") or value.get("link") or json.dumps(value, ensure_ascii=False))
    return str(value)


def normalize_compare(value: Any) -> str:
    return flatten_value(value).strip()


def load_payload(payload_file: Path) -> list[dict[str, Any]]:
    payload = json.loads(payload_file.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError("payload-file must contain a JSON object or an array of JSON objects")
    return payload


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_id(prefix: str, seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}-{digest}"


def is_truthy_text(value: Any) -> bool:
    return normalize_compare(value).lower() in {"true", "1", "yes", "y", "是", "已安装", "ready"}


def split_scene_values(value: Any) -> list[str]:
    text = flatten_value(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"[\n,，;/；、|]+", text) if part.strip()]


def value_or_blank(row: dict[str, Any], key: str) -> str:
    return flatten_value(row.get(key))


def safe_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "-", name)


def parse_link_params(link: str) -> dict[str, Optional[str]]:
    parsed = parse.urlparse(link)
    params = parse.parse_qs(parsed.query)
    return {
        "table_id": params.get("table", [None])[0],
        "view_id": params.get("view", [None])[0],
    }


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def json_request(url: str, method: str, payload: Optional[dict[str, Any]], headers: dict[str, str]) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Accept", "application/json")
    for key, value in headers.items():
        req.add_header(key, value)
    try:
        with request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: {exc.code} {detail}") from exc
    return json.loads(raw) if raw else {}


def fetch_tenant_access_token(app_id: str, app_secret: str) -> str:
    payload = {"app_id": app_id, "app_secret": app_secret}
    data = json_request(
        f"{FEISHU_OPENAPI_BASE}/open-apis/auth/v3/tenant_access_token/internal",
        "POST",
        payload,
        {},
    )
    token = data.get("tenant_access_token")
    if not token:
        raise RuntimeError(f"Failed to obtain tenant_access_token: {json.dumps(data, ensure_ascii=False)}")
    return token


@dataclass
class FeishuSummary:
    base_name: str
    obj_token: str
    table_id: str
    view_id: str
    primary_field_id: Optional[str]
    primary_field_name: Optional[str]
    fields: list[dict[str, str]]
    records: list[dict[str, Any]]
    record_count: int
    raw_probe_path: str


LIBRARY_TABLE_BLUEPRINTS = [
    {
        "table_name": "策略主表",
        "role": "复杂任务的唯一决策入口",
        "primary_field": "策略ID",
        "fields": [
            "策略名称",
            "策略ID",
            "策略摘要",
            "任务场景",
            "适用输入",
            "预期输出",
            "触发条件",
            "不适用条件",
            "执行步骤摘要",
            "优先级",
            "当前状态",
            "关联Skill",
            "关联仓库",
            "关联素材",
            "最近验证时间",
            "策略负责人",
            "备注",
        ],
    },
    {
        "table_name": "任务场景子表",
        "role": "按场景选策略的稳定受控词表",
        "primary_field": "场景ID",
        "fields": [
            "场景名称",
            "场景ID",
            "场景描述",
            "典型输入",
            "典型输出",
            "默认推荐策略",
            "备选策略",
            "禁用策略",
            "备注",
        ],
    },
    {
        "table_name": "GitHub仓库总表",
        "role": "承接 GitHub 仓库事实元数据和扫描结果",
        "primary_field": "完整名称",
        "fields": [
            "仓库名",
            "完整名称",
            "可见性",
            "是否私有",
            "是否Fork",
            "主要语言",
            "仓库说明",
            "仓库主页",
            "一键安装链接",
            "是否Skill Pattern",
            "Pattern类型",
            "Pattern置信度",
            "Skill名称",
            "Skill摘要",
            "适用场景",
            "入口文件/结构",
            "是否含SKILL.md",
            "是否含agents配置",
            "是否安装就绪",
            "最近更新时间",
            "本次扫描时间",
            "备注",
        ],
    },
    {
        "table_name": "Skill Pattern子表",
        "role": "描述仓库或 skill 是否已经沉淀成可调用模式",
        "primary_field": "Pattern名称",
        "fields": [
            "Pattern名称",
            "Pattern类型",
            "关联仓库",
            "关联Skill",
            "入口命令",
            "安装方式",
            "触发说明",
            "输入要求",
            "输出约定",
            "成熟度",
            "最近验证时间",
        ],
    },
    {
        "table_name": "素材/逐字稿子表",
        "role": "沉淀原始纪要、摘要、逐字稿和证据素材",
        "primary_field": "素材ID",
        "fields": [
            "素材标题",
            "素材ID",
            "素材类型",
            "来源链接",
            "摘要",
            "逐字稿链接",
            "逐字稿全文",
            "提炼策略候选",
            "同步时间",
            "关联策略",
        ],
    },
]


def build_summary_from_schema(schema: dict[str, Any], probe: dict[str, Any], *, limit: int) -> FeishuSummary:
    table = schema["data"]["table"]
    resolved_view_id = probe["resolved_view_id"] or table["views"][0]
    view = table["viewMap"][resolved_view_id]
    field_map = table["fieldMap"]
    record_map = schema["data"]["recordMap"]

    field_ids = view["property"]["fields"]
    record_ids = view["property"]["records"]
    primary_field_id = table.get("primaryKey")
    primary_field_name = field_map.get(primary_field_id, {}).get("name") if primary_field_id else None

    fields = [{"field_id": field_id, "name": field_map[field_id]["name"]} for field_id in field_ids]
    records = []
    for record_id in record_ids[:limit]:
        record_data = record_map[record_id]
        row = {"record_id": record_id}
        for field_id in field_ids:
            cell = record_data.get(field_id) or {}
            row[field_map[field_id]["name"]] = flatten_value(cell.get("value"))
        records.append(row)

    return FeishuSummary(
        base_name=schema["base"]["name"],
        obj_token=schema["base"]["token"],
        table_id=probe["resolved_table_id"],
        view_id=resolved_view_id,
        primary_field_id=primary_field_id,
        primary_field_name=primary_field_name,
        fields=fields,
        records=records,
        record_count=table["meta"]["recordsNum"],
        raw_probe_path="",
    )


def load_summary_from_file(path: Path, *, limit: Optional[int] = None) -> FeishuSummary:
    payload = load_json(path)
    if "client_vars" in payload:
        client_vars_payload = json.loads(payload["client_vars"]["text"])
        gzip_schema = client_vars_payload["data"]["gzipSchema"]
        schema = json.loads(gzip.decompress(base64.b64decode(gzip_schema)).decode("utf-8"))
        summary = build_summary_from_schema(schema, payload, limit=limit or 100000)
        summary.raw_probe_path = str(path)
        return summary
    if {"base", "table", "fields", "records"}.issubset(payload.keys()):
        table_payload = payload["table"]
        view_payload = payload.get("view", {})
        return FeishuSummary(
            base_name=payload["base"]["name"],
            obj_token=payload["base"]["obj_token"],
            table_id=table_payload["table_id"],
            view_id=view_payload.get("view_id") or table_payload.get("view_id") or "",
            primary_field_id=table_payload.get("primary_field_id"),
            primary_field_name=table_payload.get("primary_field_name"),
            fields=payload["fields"],
            records=(payload["records"][:limit] if limit is not None else payload["records"]),
            record_count=table_payload.get("record_count", len(payload["records"])),
            raw_probe_path=payload.get("raw_probe_path", str(path)),
        )
    raise ValueError(f"Unsupported Feishu export format: {path}")


def normalize_material_records(summary: FeishuSummary) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in summary.records:
        title = value_or_blank(record, "标题") or value_or_blank(record, "文本") or record["record_id"]
        source_link = value_or_blank(record, "妙记链接") or value_or_blank(record, "来源链接")
        transcript_link = value_or_blank(record, "逐字稿链接")
        transcript_text = value_or_blank(record, "逐字稿全文")
        rows.append(
            {
                "素材标题": title,
                "素材ID": stable_id("mat", f"{summary.table_id}:{record['record_id']}"),
                "素材类型": "逐字稿" if transcript_text or transcript_link else "纪要",
                "来源链接": source_link,
                "摘要": value_or_blank(record, "摘要"),
                "逐字稿链接": transcript_link,
                "逐字稿全文": transcript_text,
                "提炼策略候选": value_or_blank(record, "提炼策略候选"),
                "同步时间": value_or_blank(record, "同步时间"),
                "关联策略": "",
            }
        )
    return rows


def normalize_github_records(summary: FeishuSummary) -> list[dict[str, Any]]:
    field_names = [field["name"] for field in summary.fields]
    rows: list[dict[str, Any]] = []
    for record in summary.records:
        row = {field_name: value_or_blank(record, field_name) for field_name in field_names}
        if not row.get("完整名称"):
            row["完整名称"] = value_or_blank(record, "完整名称") or value_or_blank(record, "仓库名")
        rows.append(row)
    return rows


def derive_task_scene_rows(github_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scenes: dict[str, dict[str, Any]] = {}
    for row in github_rows:
        for scene in split_scene_values(row.get("适用场景")):
            scenes.setdefault(
                scene,
                {
                    "场景名称": scene,
                    "场景ID": stable_id("scene", scene),
                    "场景描述": "",
                    "典型输入": "",
                    "典型输出": "",
                    "默认推荐策略": "",
                    "备选策略": "",
                    "禁用策略": "",
                    "备注": "由 GitHub仓库总表 自动归纳",
                },
            )
    return sorted(scenes.values(), key=lambda item: item["场景名称"])


def derive_skill_pattern_rows(github_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in github_rows:
        if not (is_truthy_text(row.get("是否Skill Pattern")) or value_or_blank(row, "Skill名称")):
            continue
        repo_name = value_or_blank(row, "完整名称") or value_or_blank(row, "仓库名")
        skill_name = value_or_blank(row, "Skill名称")
        rows.append(
            {
                "Pattern名称": skill_name or repo_name,
                "Pattern类型": value_or_blank(row, "Pattern类型") or "Skill",
                "关联仓库": repo_name,
                "关联Skill": skill_name,
                "入口命令": value_or_blank(row, "入口文件/结构"),
                "安装方式": value_or_blank(row, "一键安装链接") or "GitHub clone/install",
                "触发说明": value_or_blank(row, "Skill摘要"),
                "输入要求": "",
                "输出约定": "",
                "成熟度": "已验证" if is_truthy_text(row.get("是否安装就绪")) else "候选",
                "最近验证时间": value_or_blank(row, "最近更新时间") or value_or_blank(row, "本次扫描时间"),
            }
        )
    return rows


def derive_strategy_rows(github_rows: list[dict[str, Any]], material_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    material_titles = [row["素材标题"] for row in material_rows[:3]]
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in github_rows:
        strategy_name = value_or_blank(row, "Skill名称") or value_or_blank(row, "仓库名")
        if not strategy_name:
            continue
        strategy_id = stable_id("strategy", strategy_name)
        if strategy_id in seen_ids:
            continue
        seen_ids.add(strategy_id)
        rows.append(
            {
                "策略名称": strategy_name,
                "策略ID": strategy_id,
                "策略摘要": value_or_blank(row, "Skill摘要") or value_or_blank(row, "仓库说明"),
                "任务场景": " | ".join(split_scene_values(row.get("适用场景"))),
                "适用输入": "",
                "预期输出": "",
                "触发条件": value_or_blank(row, "Skill摘要"),
                "不适用条件": "",
                "执行步骤摘要": value_or_blank(row, "入口文件/结构"),
                "优先级": "P1" if is_truthy_text(row.get("是否安装就绪")) else "P2",
                "当前状态": "已验证" if is_truthy_text(row.get("是否安装就绪")) else "候选",
                "关联Skill": value_or_blank(row, "Skill名称"),
                "关联仓库": value_or_blank(row, "完整名称") or value_or_blank(row, "仓库名"),
                "关联素材": " | ".join(material_titles),
                "最近验证时间": value_or_blank(row, "最近更新时间") or value_or_blank(row, "本次扫描时间"),
                "策略负责人": "",
                "备注": "由 GitHub仓库总表 自动提炼为策略候选",
            }
        )
    return rows


def build_library_blueprint(materials_summary: FeishuSummary, github_summary: FeishuSummary) -> dict[str, Any]:
    return {
        "summary": {
            "recommended_entry_table": "策略主表",
            "sync_direction": "GitHub -> GitHub仓库总表 / Skill Pattern子表 -> 候选策略审核 -> 策略主表",
            "material_flow": "素材/逐字稿子表 -> 策略候选 -> 人工确认 -> 策略主表",
        },
        "current_tables": [
            {
                "table_id": materials_summary.table_id,
                "table_role": "素材/逐字稿子表",
                "base_name": materials_summary.base_name,
                "primary_field": materials_summary.primary_field_name,
                "field_names": [field["name"] for field in materials_summary.fields],
                "record_count": materials_summary.record_count,
            },
            {
                "table_id": github_summary.table_id,
                "table_role": "GitHub仓库总表",
                "base_name": github_summary.base_name,
                "primary_field": github_summary.primary_field_name,
                "field_names": [field["name"] for field in github_summary.fields],
                "record_count": github_summary.record_count,
            },
        ],
        "target_tables": LIBRARY_TABLE_BLUEPRINTS,
        "rules": [
            "策略主表只存可执行策略单元，不存原始逐字稿全文。",
            "GitHub仓库总表是事实源，负责承接仓库与 skill 扫描结果。",
            "Skill Pattern子表是实现层编排，不直接替代策略决策。",
            "素材/逐字稿子表允许重复标题，但必须使用 素材ID 作为稳定主键。",
            "所有写入先 dry-run，再由用户确认 apply。",
        ],
    }


def render_library_blueprint_markdown(blueprint: dict[str, Any]) -> str:
    lines = [
        "# 专家策略库重构蓝图",
        "",
        "## 总体方向",
        "",
        f"- 决策入口: `{blueprint['summary']['recommended_entry_table']}`",
        f"- 同步方向: `{blueprint['summary']['sync_direction']}`",
        f"- 素材流转: `{blueprint['summary']['material_flow']}`",
        "",
        "## 当前表角色映射",
        "",
    ]
    for table in blueprint["current_tables"]:
        lines.append(f"- `{table['table_id']}` -> `{table['table_role']}`")
        lines.append(f"  - 主字段: `{table['primary_field']}`")
        lines.append(f"  - 记录数: `{table['record_count']}`")
        lines.append(f"  - 字段: {', '.join(table['field_names'])}")
    lines.extend(["", "## 目标 5 表", ""])
    for table in blueprint["target_tables"]:
        lines.extend(
            [
                f"### {table['table_name']}",
                "",
                f"- 角色: {table['role']}",
                f"- 主字段: `{table['primary_field']}`",
                f"- 字段: {', '.join(table['fields'])}",
                "",
            ]
        )
    lines.extend(["## 规则", ""])
    for rule in blueprint["rules"]:
        lines.append(f"- {rule}")
    lines.append("")
    return "\n".join(lines)


def build_library_seed_bundle(materials_summary: FeishuSummary, github_summary: FeishuSummary) -> dict[str, Any]:
    material_rows = normalize_material_records(materials_summary)
    github_rows = normalize_github_records(github_summary)
    return {
        "策略主表": derive_strategy_rows(github_rows, material_rows),
        "任务场景子表": derive_task_scene_rows(github_rows),
        "GitHub仓库总表": github_rows,
        "Skill Pattern子表": derive_skill_pattern_rows(github_rows),
        "素材/逐字稿子表": material_rows,
    }


class FeishuBridge:
    def __init__(self, *, state_dir: Path, artifact_dir: Path) -> None:
        self.state_dir = ensure_dir(state_dir)
        self.artifact_dir = ensure_dir(artifact_dir)

    def inspect_link(
        self,
        *,
        link: str,
        table_id: Optional[str],
        view_id: Optional[str],
        limit: int,
        timeout_seconds: int,
    ) -> tuple[FeishuSummary, Path]:
        summary, raw_probe = self._inspect_internal(
            link=link,
            table_id=table_id,
            view_id=view_id,
            limit=limit,
            timeout_seconds=timeout_seconds,
        )
        output_path = self.artifact_dir / f"inspect-{now_stamp()}.json"
        payload = {
            "base": {
                "name": summary.base_name,
                "obj_token": summary.obj_token,
            },
            "table": {
                "table_id": summary.table_id,
                "record_count": summary.record_count,
                "primary_field_id": summary.primary_field_id,
                "primary_field_name": summary.primary_field_name,
            },
            "view": {
                "view_id": summary.view_id,
            },
            "fields": summary.fields,
            "records": summary.records,
            "raw_probe_path": str(raw_probe),
        }
        write_json(output_path, payload)
        return summary, output_path

    def build_upsert_preview(
        self,
        *,
        link: str,
        payload_file: Path,
        primary_field: Optional[str],
        table_id: Optional[str],
        view_id: Optional[str],
        timeout_seconds: int,
    ) -> tuple[dict[str, Any], Path]:
        summary, _ = self._inspect_internal(
            link=link,
            table_id=table_id,
            view_id=view_id,
            limit=100000,
            timeout_seconds=timeout_seconds,
        )
        payload_rows = load_payload(payload_file)
        resolved_primary = primary_field or summary.primary_field_name
        if not resolved_primary:
            raise RuntimeError("Could not resolve primary_field from arguments or table metadata")
        field_names = {field["name"] for field in summary.fields}
        if resolved_primary not in field_names:
            raise RuntimeError(f"primary_field '{resolved_primary}' was not found in table fields")

        existing_by_key: dict[str, dict[str, Any]] = {}
        duplicate_existing: set[str] = set()
        for record in summary.records:
            key = normalize_compare(record.get(resolved_primary))
            if not key:
                continue
            if key in existing_by_key:
                duplicate_existing.add(key)
            existing_by_key[key] = record

        errors: list[dict[str, Any]] = []
        creates: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        unchanged: list[dict[str, Any]] = []
        incoming_seen: set[str] = set()

        for index, row in enumerate(payload_rows):
            row_errors: list[str] = []
            unknown_fields = sorted(set(row) - field_names)
            if unknown_fields:
                row_errors.append(f"Unknown fields: {', '.join(unknown_fields)}")
            match_value = normalize_compare(row.get(resolved_primary))
            if not match_value:
                row_errors.append(f"Missing primary field '{resolved_primary}'")
            if match_value in incoming_seen:
                row_errors.append(f"Duplicate incoming primary value '{match_value}'")
            if match_value in duplicate_existing:
                row_errors.append(f"Existing records contain duplicate primary value '{match_value}'")

            if row_errors:
                errors.append({"index": index, "messages": row_errors, "row": row})
                continue

            incoming_seen.add(match_value)
            existing = existing_by_key.get(match_value)
            filtered_row = {key: row[key] for key in row if key in field_names}

            if not existing:
                creates.append(
                    {
                        "index": index,
                        "match_value": match_value,
                        "fields": filtered_row,
                    }
                )
                continue

            changes = []
            for field_name, new_value in filtered_row.items():
                old_value = existing.get(field_name, "")
                if normalize_compare(old_value) != normalize_compare(new_value):
                    changes.append(
                        {
                            "field": field_name,
                            "old": old_value,
                            "new": new_value,
                        }
                    )

            if changes:
                updates.append(
                    {
                        "index": index,
                        "record_id": existing["record_id"],
                        "match_value": match_value,
                        "changes": changes,
                        "fields": filtered_row,
                    }
                )
            else:
                unchanged.append(
                    {
                        "index": index,
                        "record_id": existing["record_id"],
                        "match_value": match_value,
                    }
                )

        preview = {
            "base": {
                "name": summary.base_name,
                "obj_token": summary.obj_token,
            },
            "table": {
                "table_id": summary.table_id,
                "view_id": summary.view_id,
                "primary_field": resolved_primary,
            },
            "fields": summary.fields,
            "source_payload_file": str(payload_file.resolve()),
            "summary": {
                "creates": len(creates),
                "updates": len(updates),
                "unchanged": len(unchanged),
                "errors": len(errors),
                "can_apply": len(errors) == 0,
            },
            "creates": creates,
            "updates": updates,
            "unchanged": unchanged,
            "errors": errors,
            "raw_probe_path": summary.raw_probe_path,
        }

        output_path = self.artifact_dir / f"upsert-preview-{now_stamp()}.json"
        write_json(output_path, preview)
        return preview, output_path

    def apply_upsert(
        self,
        *,
        preview: dict[str, Any],
        app_id: str,
        app_secret: str,
    ) -> tuple[dict[str, Any], Path]:
        if preview["summary"]["errors"] != 0:
            raise RuntimeError("Preview contains blocking errors; resolve them before --apply")

        token = fetch_tenant_access_token(app_id, app_secret)
        headers = {"Authorization": f"Bearer {token}"}
        app_token = preview["base"]["obj_token"]
        table_id = preview["table"]["table_id"]

        creates_result = []
        updates_result = []
        for item in preview["creates"]:
            payload = {"fields": item["fields"]}
            response = json_request(
                f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                "POST",
                payload,
                headers,
            )
            creates_result.append(
                {
                    "match_value": item["match_value"],
                    "response": response,
                }
            )

        for item in preview["updates"]:
            payload = {"fields": item["fields"]}
            response = json_request(
                f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{item['record_id']}",
                "PUT",
                payload,
                headers,
            )
            updates_result.append(
                {
                    "match_value": item["match_value"],
                    "record_id": item["record_id"],
                    "response": response,
                }
            )

        result = {
            "base": preview["base"],
            "table": preview["table"],
            "summary": {
                "creates_applied": len(creates_result),
                "updates_applied": len(updates_result),
            },
            "creates": creates_result,
            "updates": updates_result,
        }
        output_path = self.artifact_dir / f"upsert-apply-{now_stamp()}.json"
        write_json(output_path, result)
        return result, output_path

    def build_library_blueprint_from_files(
        self,
        *,
        materials_file: Path,
        github_file: Path,
    ) -> tuple[dict[str, Any], Path, Path]:
        materials_summary = load_summary_from_file(materials_file)
        github_summary = load_summary_from_file(github_file)
        blueprint = build_library_blueprint(materials_summary, github_summary)
        stamp = now_stamp()
        json_path = self.artifact_dir / f"library-blueprint-{stamp}.json"
        md_path = self.artifact_dir / f"library-blueprint-{stamp}.md"
        write_json(json_path, blueprint)
        md_path.write_text(render_library_blueprint_markdown(blueprint), encoding="utf-8")
        return blueprint, json_path, md_path

    def build_library_seed_bundle_from_files(
        self,
        *,
        materials_file: Path,
        github_file: Path,
    ) -> tuple[dict[str, Any], Path]:
        materials_summary = load_summary_from_file(materials_file)
        github_summary = load_summary_from_file(github_file)
        bundle = build_library_seed_bundle(materials_summary, github_summary)
        bundle_dir = ensure_dir(self.artifact_dir / f"library-seeds-{now_stamp()}")
        manifest: dict[str, Any] = {
            "source": {
                "materials_file": str(materials_file.resolve()),
                "github_file": str(github_file.resolve()),
            },
            "tables": {},
        }
        for table_name, rows in bundle.items():
            table_path = bundle_dir / f"{safe_filename(table_name)}.json"
            table_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            manifest["tables"][table_name] = {
                "row_count": len(rows),
                "payload_file": str(table_path),
            }
        manifest_path = bundle_dir / "manifest.json"
        write_json(manifest_path, manifest)
        return manifest, manifest_path

    def _inspect_internal(
        self,
        *,
        link: str,
        table_id: Optional[str],
        view_id: Optional[str],
        limit: int,
        timeout_seconds: int,
    ) -> tuple[FeishuSummary, Path]:
        probe = self._probe_link(
            link=link,
            table_id=table_id,
            view_id=view_id,
            timeout_seconds=timeout_seconds,
        )
        raw_probe_path = self.artifact_dir / f"probe-{now_stamp()}.json"
        write_json(raw_probe_path, probe)

        client_vars_text = probe["client_vars"]["text"]
        client_vars_payload = json.loads(client_vars_text)
        gzip_schema = client_vars_payload["data"]["gzipSchema"]
        schema = json.loads(gzip.decompress(base64.b64decode(gzip_schema)).decode("utf-8"))
        summary = build_summary_from_schema(schema, probe, limit=limit)
        summary.raw_probe_path = str(raw_probe_path)
        return summary, raw_probe_path

    def _probe_link(
        self,
        *,
        link: str,
        table_id: Optional[str],
        view_id: Optional[str],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(self.state_dir),
                executable_path=CHROME_PATH,
                headless=False,
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                page.goto(link, wait_until="domcontentloaded", timeout=60000)
                start = time.time()
                while time.time() - start < timeout_seconds:
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=5000)
                    except PlaywrightTimeoutError:
                        pass
                    current_url = page.url
                    if not looks_like_login(current_url):
                        break
                    time.sleep(5)
                if looks_like_login(page.url):
                    raise RuntimeError("Feishu is still on the login page; complete login and retry")

                link_params = parse_link_params(page.url)
                resolved_table_id = table_id or link_params["table_id"]
                resolved_view_id = view_id or link_params["view_id"]

                page.wait_for_timeout(DEFAULT_WAIT_SECONDS * 1000)
                runtime = page.evaluate(
                    """async (args) => {
                        const currentSpaceWiki = window.current_space_wiki || null;
                        const baseFirstBlockInfo = window.baseFirstBlockInfo || null;
                        const url = new URL(window.location.href);
                        const resolvedTableId = args.tableId || url.searchParams.get('table') || baseFirstBlockInfo?.id || null;
                        const resolvedViewId = args.viewId || url.searchParams.get('view') || null;
                        const objToken = currentSpaceWiki?.obj_token || null;
                        if (!objToken) {
                            throw new Error('Could not resolve obj_token from page runtime');
                        }
                        const clientVarsUrl = resolvedTableId && resolvedViewId
                            ? `/space/api/bitable/${objToken}/client_vars?table=${resolvedTableId}&view=${resolvedViewId}`
                            : `/space/api/bitable/${objToken}/client_vars`;
                        const response = await fetch(clientVarsUrl, { credentials: 'include' });
                        const text = await response.text();
                        return {
                            title: document.title,
                            current_url: window.location.href,
                            obj_token: objToken,
                            base_first_block_info: baseFirstBlockInfo,
                            current_space_wiki: currentSpaceWiki,
                            resolved_table_id: resolvedTableId,
                            resolved_view_id: resolvedViewId,
                            client_vars: {
                                url: clientVarsUrl,
                                status: response.status,
                                text,
                            },
                        };
                    }""",
                    {"tableId": resolved_table_id, "viewId": resolved_view_id},
                )
                if runtime["client_vars"]["status"] != 200:
                    raise RuntimeError(
                        f"client_vars fetch failed: {runtime['client_vars']['status']} {runtime['client_vars']['url']}"
                    )
                if not runtime["resolved_table_id"]:
                    first_block = runtime.get("base_first_block_info") or {}
                    runtime["resolved_table_id"] = first_block.get("id")
                if not runtime["resolved_view_id"]:
                    runtime["resolved_view_id"] = None
                if not runtime["resolved_table_id"]:
                    raise RuntimeError("Could not resolve table_id from link or runtime")
                return runtime
            finally:
                context.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and safely upsert Feishu bitables from wiki/base links.")
    parser.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR), help="Persistent Chrome profile directory.")
    parser.add_argument("--artifact-dir", default=str(DEFAULT_ARTIFACT_DIR), help="Directory for JSON outputs.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_cmd = subparsers.add_parser("inspect-link", help="Inspect a Feishu link and export schema plus records.")
    inspect_cmd.add_argument("--link", required=True)
    inspect_cmd.add_argument("--table-id")
    inspect_cmd.add_argument("--view-id")
    inspect_cmd.add_argument("--limit", type=int, default=5)
    inspect_cmd.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)

    blueprint_cmd = subparsers.add_parser(
        "build-library-blueprint",
        help="Generate the 5-table restructuring blueprint from materials and GitHub table exports.",
    )
    blueprint_cmd.add_argument("--materials-file", required=True)
    blueprint_cmd.add_argument("--github-file", required=True)

    seeds_cmd = subparsers.add_parser(
        "build-library-seeds",
        help="Generate per-table seed payloads for the 5-table strategy library architecture.",
    )
    seeds_cmd.add_argument("--materials-file", required=True)
    seeds_cmd.add_argument("--github-file", required=True)

    upsert_cmd = subparsers.add_parser("upsert-records", help="Preview or apply record upserts from JSON payload.")
    upsert_cmd.add_argument("--link", required=True)
    upsert_cmd.add_argument("--payload-file", required=True)
    upsert_cmd.add_argument("--primary-field")
    upsert_cmd.add_argument("--table-id")
    upsert_cmd.add_argument("--view-id")
    upsert_cmd.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    mode = upsert_cmd.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    upsert_cmd.add_argument("--app-id-env", default="FEISHU_APP_ID")
    upsert_cmd.add_argument("--app-secret-env", default="FEISHU_APP_SECRET")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    bridge = FeishuBridge(
        state_dir=Path(args.state_dir).expanduser().resolve(),
        artifact_dir=Path(args.artifact_dir).expanduser().resolve(),
    )

    if args.command == "inspect-link":
        summary, output_path = bridge.inspect_link(
            link=args.link,
            table_id=args.table_id,
            view_id=args.view_id,
            limit=args.limit,
            timeout_seconds=args.timeout_seconds,
        )
        payload = {
            "base": {
                "name": summary.base_name,
                "obj_token": summary.obj_token,
            },
            "table": {
                "table_id": summary.table_id,
                "view_id": summary.view_id,
                "primary_field_id": summary.primary_field_id,
                "primary_field_name": summary.primary_field_name,
                "record_count": summary.record_count,
            },
            "fields": summary.fields,
            "records": summary.records,
            "raw_probe_path": summary.raw_probe_path,
            "output_path": str(output_path),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-library-blueprint":
        blueprint, json_path, md_path = bridge.build_library_blueprint_from_files(
            materials_file=Path(args.materials_file).expanduser().resolve(),
            github_file=Path(args.github_file).expanduser().resolve(),
        )
        blueprint["json_path"] = str(json_path)
        blueprint["markdown_path"] = str(md_path)
        print(json.dumps(blueprint, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-library-seeds":
        manifest, manifest_path = bridge.build_library_seed_bundle_from_files(
            materials_file=Path(args.materials_file).expanduser().resolve(),
            github_file=Path(args.github_file).expanduser().resolve(),
        )
        manifest["manifest_path"] = str(manifest_path)
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return 0

    preview, preview_path = bridge.build_upsert_preview(
        link=args.link,
        payload_file=Path(args.payload_file).expanduser().resolve(),
        primary_field=args.primary_field,
        table_id=args.table_id,
        view_id=args.view_id,
        timeout_seconds=args.timeout_seconds,
    )

    if args.dry_run:
        preview["output_path"] = str(preview_path)
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        return 0

    app_id = os.environ.get(args.app_id_env)
    app_secret = os.environ.get(args.app_secret_env)
    if not app_id or not app_secret:
        raise RuntimeError(
            f"--apply requires {args.app_id_env} and {args.app_secret_env} in the environment"
        )
    result, result_path = bridge.apply_upsert(
        preview=preview,
        app_id=app_id,
        app_secret=app_secret,
    )
    result["output_path"] = str(result_path)
    result["preview_path"] = str(preview_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
