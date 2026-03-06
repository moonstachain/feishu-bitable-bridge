#!/usr/bin/env python3
import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
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
FEISHU_TEXT_FIELD_TYPE = 1
MIRROR_ROOT_NAME = "feishu-strategy-library"
STRATEGY_AUTO_NOTE = "适用输入/预期输出/不适用条件为机器建议，人工字段默认不覆盖"
MISSING_FROM_GITHUB_NOTE = "未在本次 GitHub 同步中命中，保留历史记录"

DEFAULT_STRATEGY_CN_NAME_OVERRIDES = {
    "youquant-backtest": "优宽回测策略执行",
    "get-biji-transcript": "Get笔记逐字稿提取",
    "Get笔记逐字稿": "Get笔记逐字稿提取",
    "quant-factor-dashboard": "量化因子策略看板",
    "ai-news-aggregator": "AI 资讯聚合器",
    "obsidian-image-hosting": "Obsidian 图床托管",
    "quant-workspace": "量化研究工作台",
    "CAUSAL_AI_MVP_1.1": "因果智能最小可行产品 1.1",
    "ai-news-feishu-collector": "AI 资讯飞书采集器",
    "ruoyi-vue-pro-raysystem": "若依 Vue Pro Ray 管理系统",
    "typescript-sdk": "TypeScript 开发套件",
    "bolt.diy": "Bolt DIY 全栈开发平台",
    "reflex-llm-examples": "Reflex 大模型示例集",
    "Resume-Matcher": "简历匹配器",
}

TOKEN_CN_OVERRIDES = {
    "ai": "AI",
    "news": "资讯",
    "aggregator": "聚合器",
    "collector": "采集器",
    "feishu": "飞书",
    "quant": "量化",
    "factor": "因子",
    "dashboard": "看板",
    "workspace": "工作台",
    "obsidian": "Obsidian",
    "image": "图像",
    "hosting": "托管",
    "host": "托管",
    "typescript": "TypeScript",
    "sdk": "开发套件",
    "resume": "简历",
    "matcher": "匹配器",
    "examples": "示例集",
    "example": "示例",
    "llm": "大模型",
    "diy": "DIY",
    "backtest": "回测",
    "youquant": "优宽",
    "causal": "因果",
    "mvp": "最小可行产品",
    "ray": "Ray",
    "system": "系统",
    "systems": "系统",
    "pro": "Pro",
    "vue": "Vue",
    "ruoyi": "若依",
    "bolt": "Bolt",
    "reflex": "Reflex",
}

STRATEGY_FIELD_OVERRIDES = {
    "youquant-backtest": {
        "适用输入": "量价数据 | 回测参数 | 策略信号或脚本定义",
        "预期输出": "回测结果 | 策略执行结论 | 量化自动化入口",
        "不适用条件": "缺少优宽环境、历史行情数据或策略信号定义时不适用",
    },
    "get-biji-transcript": {
        "适用输入": "视频或网页链接 | 音视频文件 | Get笔记登录态",
        "预期输出": "完整逐字稿 | AI 笔记 | 原文或分享链接",
        "不适用条件": "未登录 Get笔记、链接平台不受支持或任务只需纯离线转写时不适用",
    },
    "Get笔记逐字稿": {
        "适用输入": "视频或网页链接 | 音视频文件 | Get笔记登录态",
        "预期输出": "完整逐字稿 | AI 笔记 | 原文或分享链接",
        "不适用条件": "未登录 Get笔记、链接平台不受支持或任务只需纯离线转写时不适用",
    },
    "quant-factor-dashboard": {
        "适用输入": "因子指标数据 | 策略表现数据 | 看板筛选条件",
        "预期输出": "因子分析视图 | 策略看板 | 研究决策线索",
        "不适用条件": "私有仓库未开放或当前任务需要现成 Skill 自动化入口时不适用",
    },
    "ai-news-aggregator": {
        "适用输入": "资讯源链接 | RSS 或 API 数据源 | 抓取关键词",
        "预期输出": "结构化新闻流 | 聚合结果 | 后续处理输入",
        "不适用条件": "缺少数据源配置或需要即插即用的 Codex Skill 时不适用",
    },
    "obsidian-image-hosting": {
        "适用输入": "图片资源 | Obsidian 笔记内容 | 图床配置",
        "预期输出": "图片托管链接 | 笔记可引用资源 | 内容基础设施",
        "不适用条件": "当前任务不处理图片或笔记资源，或需要通用自动化编排时不适用",
    },
    "quant-workspace": {
        "适用输入": "研究课题 | 因子脚本 | 策略配置与数据文件",
        "预期输出": "研究工作台 | 因子分析结果 | 策略实验环境",
        "不适用条件": "缺少研究数据或必须走标准化 Skill 入口时不适用",
    },
    "CAUSAL_AI_MVP_1.1": {
        "适用输入": "业务需求 | 自动化流程设想 | 应用原型约束",
        "预期输出": "原型仓库参考 | 自动化方案骨架 | MVP 落地线索",
        "不适用条件": "私有仓库不可访问或当前任务需要现成可执行 Skill 时不适用",
    },
    "ai-news-feishu-collector": {
        "适用输入": "资讯源链接 | 飞书表或文档链接 | 同步规则",
        "预期输出": "飞书内资讯记录 | 同步结果 | 数据采集工作流",
        "不适用条件": "私有仓库不可访问或当前任务不涉及飞书同步时不适用",
    },
    "ruoyi-vue-pro-raysystem": {
        "适用输入": "业务流程需求 | 后台管理页面需求 | 二次开发目标",
        "预期输出": "后台系统改造参考 | 应用仓库骨架 | 业务流程实现线索",
        "不适用条件": "仅需原生 Skill 自动化，或无法接受 fork 二次辨别成本时不适用",
    },
    "typescript-sdk": {
        "适用输入": "TypeScript 项目需求 | SDK 集成目标 | API 或工具链配置",
        "预期输出": "SDK 集成方案 | 开发脚手架参考 | 自动化模板线索",
        "不适用条件": "非 TypeScript 技术栈，或不接受 fork 模板改造时不适用",
    },
    "bolt.diy": {
        "适用输入": "全栈应用需求 | 环境变量配置 | 前后端集成约束",
        "预期输出": "全栈开发骨架 | 集成模板 | 快速原型入口",
        "不适用条件": "不接受 fork 模板改造，或需要已验证安装完成的现成技能时不适用",
    },
    "reflex-llm-examples": {
        "适用输入": "示例需求 | LLM 应用设想 | Python 或浏览器自动化环境",
        "预期输出": "示例项目骨架 | LLM 工作流参考 | 原型实验起点",
        "不适用条件": "只接受稳定生产技能而不接受示例仓二次改造时不适用",
    },
    "Resume-Matcher": {
        "适用输入": "简历文本 | JD 或岗位要求 | 匹配规则",
        "预期输出": "匹配结果 | 筛选结论 | 可执行工作流入口",
        "不适用条件": "岗位要求不明确，或不希望基于 fork 工作流继续改造时不适用",
    },
}

STRATEGY_STRONG_FACT_FIELDS = [
    "策略名称",
    "策略名称-CN",
    "策略ID",
    "策略摘要",
    "任务场景",
    "执行步骤摘要",
    "优先级",
    "当前状态",
    "关联Skill",
    "关联仓库",
    "最近验证时间",
    "备注",
]
STRATEGY_INFERRED_FIELDS = ["适用输入", "预期输出", "不适用条件"]
STRATEGY_MANUAL_FIELDS = ["策略负责人", "关联素材"]

TABLE_EXPORT_FILE_MAP = {
    "策略主表": "strategy-master",
    "任务场景子表": "task-scenes",
    "GitHub仓库总表": "github-repos",
    "Skill Pattern子表": "skill-patterns",
    "素材/逐字稿子表": "materials",
}


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


def load_override_mapping(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Override file must contain a JSON object: {path}")
    return {str(key): str(value) for key, value in payload.items()}


STRATEGY_CN_NAME_OVERRIDES = {
    **DEFAULT_STRATEGY_CN_NAME_OVERRIDES,
    **load_override_mapping(SKILL_ROOT / "references" / "strategy-name-overrides.json"),
}


def to_strategy_cn_name(strategy_name: str) -> str:
    if strategy_name in STRATEGY_CN_NAME_OVERRIDES:
        return STRATEGY_CN_NAME_OVERRIDES[strategy_name]
    normalized = strategy_name.replace(".", "-").replace("_", "-")
    parts = [part for part in re.split(r"[-\s]+", normalized) if part]
    translated_parts: list[str] = []
    for part in parts:
        translated_parts.append(TOKEN_CN_OVERRIDES.get(part.lower(), part))
    return " ".join(translated_parts)


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


def write_text(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def unique_join(parts: list[str], *, separator: str = " | ") -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        value = part.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return separator.join(ordered)


def append_note_once(note: Any, extra: str) -> str:
    base = flatten_value(note).strip()
    if not base:
        return extra
    if extra in base:
        return base
    return f"{base}；{extra}"


def parse_summary_sections(text: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if "：" not in line:
            continue
        key, value = line.split("：", 1)
        key = key.strip()
        if key in {"用途", "输入/入口", "输出/结果", "适合复用的模式", "限制或注意事项"}:
            sections[key] = value.strip()
    return sections


def infer_strategy_fields(strategy_name: str, row: dict[str, Any]) -> dict[str, str]:
    if strategy_name in STRATEGY_FIELD_OVERRIDES:
        return STRATEGY_FIELD_OVERRIDES[strategy_name]

    summary = value_or_blank(row, "Skill摘要")
    entry = value_or_blank(row, "入口文件/结构")
    scenes = split_scene_values(row.get("适用场景"))
    parsed = parse_summary_sections(summary)
    lowered = " ".join(
        [
            strategy_name.lower(),
            value_or_blank(row, "仓库名").lower(),
            value_or_blank(row, "完整名称").lower(),
            summary.lower(),
            entry.lower(),
            " ".join(scene.lower() for scene in scenes),
        ]
    )

    input_candidates: list[str] = []
    output_candidates: list[str] = []
    unsuitable: list[str] = []

    if "quant" in lowered or "backtest" in lowered or "因子" in summary:
        input_candidates.extend(["量化数据", "回测参数", "策略配置"])
        output_candidates.extend(["分析结果", "策略执行结论"])
    if "get笔记" in summary or "transcript" in lowered or "逐字稿" in summary:
        input_candidates.extend(["链接或音视频文件", "登录态"])
        output_candidates.extend(["逐字稿", "结构化笔记"])
        unsuitable.append("未登录相关平台时不适用")
    if "news" in lowered or "资讯" in summary or "collector" in lowered or "aggregator" in lowered:
        input_candidates.extend(["数据源链接", "关键词", "同步配置"])
        output_candidates.extend(["结构化资讯流", "同步结果"])
    if "feishu" in lowered or "飞书" in summary:
        input_candidates.extend(["飞书链接", "表格或文档对象"])
        output_candidates.extend(["飞书记录", "自动化同步结果"])
    if "obsidian" in lowered or "image" in lowered:
        input_candidates.extend(["图片资源", "笔记内容"])
        output_candidates.extend(["托管链接", "内容基础设施"])
    if "sdk" in lowered or "typescript" in lowered:
        input_candidates.extend(["项目集成需求", "SDK 配置"])
        output_candidates.extend(["集成方案", "开发模板"])
    if "resume" in lowered or "matcher" in lowered:
        input_candidates.extend(["简历文本", "岗位要求"])
        output_candidates.extend(["匹配结果", "筛选结论"])
    if "template" in lowered or "examples" in lowered or "example" in lowered:
        output_candidates.extend(["示例骨架", "复用模板"])
        unsuitable.append("若仅接受生产级现成技能则不适用")

    if is_truthy_text(row.get("是否私有")):
        unsuitable.append("私有仓库未开放访问时不适用")
    if is_truthy_text(row.get("是否Fork")):
        unsuitable.append("Fork 仓库需先区分原始能力与二次改造")
    if not is_truthy_text(row.get("是否Skill Pattern")):
        unsuitable.append("当前未沉淀为稳定 Skill Pattern，更适合参考或二次改造")
    if not is_truthy_text(row.get("是否安装就绪")):
        unsuitable.append("未验证安装就绪前不建议直接用于生产任务")

    if parsed.get("输入/入口"):
        input_candidates.append(parsed["输入/入口"])
    if parsed.get("输出/结果"):
        output_candidates.append(parsed["输出/结果"])
    if parsed.get("限制或注意事项"):
        unsuitable.append(parsed["限制或注意事项"])
    if scenes:
        output_candidates.extend([f"{scene}相关产出" for scene in scenes[:2]])

    return {
        "适用输入": unique_join(input_candidates) or "任务描述 | 仓库入口 | 配置参数",
        "预期输出": unique_join(output_candidates) or "结构化结果 | 可复用执行线索",
        "不适用条件": unique_join(unsuitable) or "缺少上下文或访问条件时不适用",
    }


def to_yaml_text(value: Any, indent: int = 0) -> str:
    prefix = "  " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}{key}:")
                lines.append(to_yaml_text(item, indent + 1))
            elif isinstance(item, str) and "\n" in item:
                lines.append(f"{prefix}{key}: |-")
                for line in item.splitlines():
                    lines.append(f"{prefix}  {line}")
            else:
                lines.append(f"{prefix}{key}: {json.dumps(item, ensure_ascii=False)}")
        return "\n".join(lines)
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}-")
                lines.append(to_yaml_text(item, indent + 1))
            elif isinstance(item, str) and "\n" in item:
                lines.append(f"{prefix}- |-")
                for line in item.splitlines():
                    lines.append(f"{prefix}  {line}")
            else:
                lines.append(f"{prefix}- {json.dumps(item, ensure_ascii=False)}")
        return "\n".join(lines)
    return f"{prefix}{json.dumps(value, ensure_ascii=False)}"


def run_git(args: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


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


def openapi_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def require_openapi_success(data: dict[str, Any], context: str) -> dict[str, Any]:
    if data.get("code", 0) not in (0, None):
        raise RuntimeError(f"{context} failed: {json.dumps(data, ensure_ascii=False)}")
    return data.get("data") or {}


def paged_get(url: str, headers: dict[str, str], *, item_key: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        parsed = parse.urlparse(url)
        params = parse.parse_qs(parsed.query)
        if page_token:
            params["page_token"] = [page_token]
        params.setdefault("page_size", ["500"])
        encoded = parse.urlencode({key: values[0] for key, values in params.items()})
        page_url = parse.urlunparse(parsed._replace(query=encoded))
        data = require_openapi_success(json_request(page_url, "GET", None, headers), f"GET {page_url}")
        items.extend(data.get(item_key) or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
        if not page_token:
            break
    return items


def list_bitable_tables(app_token: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    return paged_get(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables",
        headers,
        item_key="items",
    )


def create_bitable_table(app_token: str, table_name: str, headers: dict[str, str]) -> dict[str, Any]:
    data = json_request(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables",
        "POST",
        {"table": {"name": table_name}},
        headers,
    )
    payload = require_openapi_success(data, f"create table {table_name}")
    return payload.get("table") or payload


def list_bitable_fields(app_token: str, table_id: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    return paged_get(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
        headers,
        item_key="items",
    )


def create_bitable_field(app_token: str, table_id: str, field_name: str, headers: dict[str, str]) -> dict[str, Any]:
    data = json_request(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
        "POST",
        {"field_name": field_name, "type": FEISHU_TEXT_FIELD_TYPE},
        headers,
    )
    return require_openapi_success(data, f"create field {field_name}")


def update_bitable_field(
    app_token: str,
    table_id: str,
    field_id: str,
    *,
    field_name: str,
    headers: dict[str, str],
) -> dict[str, Any]:
    data = json_request(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}",
        "PUT",
        {"field_name": field_name},
        headers,
    )
    return require_openapi_success(data, f"update field {field_name}")


def list_bitable_records(app_token: str, table_id: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    return paged_get(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        headers,
        item_key="items",
    )


def batch_create_bitable_records(
    app_token: str,
    table_id: str,
    rows: list[dict[str, Any]],
    headers: dict[str, str],
) -> dict[str, Any]:
    payload = {"records": [{"fields": row} for row in rows]}
    data = json_request(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
        "POST",
        payload,
        headers,
    )
    return require_openapi_success(data, f"batch create records for {table_id}")


def batch_update_bitable_records(
    app_token: str,
    table_id: str,
    rows: list[dict[str, Any]],
    headers: dict[str, str],
) -> dict[str, Any]:
    payload = {
        "records": [{"record_id": row["record_id"], "fields": row["fields"]} for row in rows],
    }
    data = json_request(
        f"{FEISHU_OPENAPI_BASE}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update",
        "POST",
        payload,
        headers,
    )
    return require_openapi_success(data, f"batch update records for {table_id}")


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
            "策略名称-CN",
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


def build_material_candidates(material_rows: list[dict[str, Any]], *, limit: int = 3) -> str:
    titles = [row["素材标题"] for row in material_rows if row.get("素材标题")]
    return unique_join(titles[:limit])


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


def derive_strategy_rows(
    github_rows: list[dict[str, Any]],
    material_rows: list[dict[str, Any]],
    skill_pattern_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    material_candidates = build_material_candidates(material_rows)
    pattern_by_repo = {
        value_or_blank(row, "关联仓库"): row
        for row in skill_pattern_rows
        if value_or_blank(row, "关联仓库")
    }
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
        repo_name = value_or_blank(row, "完整名称") or value_or_blank(row, "仓库名")
        pattern_row = pattern_by_repo.get(repo_name, {})
        inferred = infer_strategy_fields(strategy_name, row)
        note = append_note_once("由 GitHub仓库总表 自动提炼为策略候选", STRATEGY_AUTO_NOTE)
        rows.append(
            {
                "策略名称": strategy_name,
                "策略名称-CN": to_strategy_cn_name(strategy_name),
                "策略ID": strategy_id,
                "策略摘要": value_or_blank(row, "Skill摘要") or value_or_blank(row, "仓库说明"),
                "任务场景": " | ".join(split_scene_values(row.get("适用场景"))),
                "适用输入": inferred["适用输入"],
                "预期输出": inferred["预期输出"],
                "触发条件": value_or_blank(row, "Skill摘要"),
                "不适用条件": inferred["不适用条件"],
                "执行步骤摘要": value_or_blank(row, "入口文件/结构"),
                "优先级": "P1" if is_truthy_text(row.get("是否安装就绪")) else "P2",
                "当前状态": "已验证" if is_truthy_text(row.get("是否安装就绪")) else "候选",
                "关联Skill": value_or_blank(row, "Skill名称") or value_or_blank(pattern_row, "关联Skill"),
                "关联仓库": repo_name,
                "关联素材": material_candidates,
                "最近验证时间": value_or_blank(row, "最近更新时间") or value_or_blank(row, "本次扫描时间"),
                "策略负责人": "",
                "备注": note,
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
    skill_pattern_rows = derive_skill_pattern_rows(github_rows)
    return {
        "策略主表": derive_strategy_rows(github_rows, material_rows, skill_pattern_rows),
        "任务场景子表": derive_task_scene_rows(github_rows),
        "GitHub仓库总表": github_rows,
        "Skill Pattern子表": skill_pattern_rows,
        "素材/逐字稿子表": material_rows,
    }


def merge_strategy_payload_rows(
    payload_rows: list[dict[str, Any]],
    existing_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    existing_by_key, _ = rows_to_existing_index(existing_rows, "策略ID")
    merged_rows: list[dict[str, Any]] = []
    incoming_keys: set[str] = set()
    preserved_fields: list[dict[str, Any]] = []
    stale_updates: list[dict[str, Any]] = []

    for row in payload_rows:
        key = normalize_compare(row.get("策略ID"))
        existing = existing_by_key.get(key)
        merged = dict(row)
        incoming_keys.add(key)

        if existing:
            for field in STRATEGY_STRONG_FACT_FIELDS:
                if not normalize_compare(merged.get(field)) and normalize_compare(existing.get(field)):
                    merged[field] = existing[field]
            for field in STRATEGY_INFERRED_FIELDS:
                if normalize_compare(existing.get(field)):
                    if not normalize_compare(merged.get(field)):
                        merged[field] = existing[field]
                        preserved_fields.append(
                            {"策略ID": key, "field": field, "reason": "existing_non_empty"}
                        )
            for field in STRATEGY_MANUAL_FIELDS:
                if normalize_compare(existing.get(field)):
                    if normalize_compare(merged.get(field)) != normalize_compare(existing.get(field)):
                        preserved_fields.append(
                            {"策略ID": key, "field": field, "reason": "manual_field_preserved"}
                        )
                    merged[field] = existing[field]
        merged["备注"] = append_note_once(merged.get("备注"), STRATEGY_AUTO_NOTE)
        merged_rows.append(merged)

    for key, existing in existing_by_key.items():
        if key in incoming_keys:
            continue
        stale_note = append_note_once(existing.get("备注"), MISSING_FROM_GITHUB_NOTE)
        stale_updates.append(
            {
                "策略ID": existing.get("策略ID", ""),
                "备注": stale_note,
            }
        )

    return merged_rows + stale_updates, {
        "preserved_fields": preserved_fields,
        "stale_records": stale_updates,
    }


def rows_to_existing_index(rows: list[dict[str, Any]], primary_field: str) -> tuple[dict[str, dict[str, Any]], set[str]]:
    existing_by_key: dict[str, dict[str, Any]] = {}
    duplicate_existing: set[str] = set()
    for row in rows:
        key = normalize_compare(row.get(primary_field))
        if not key:
            continue
        if key in existing_by_key:
            duplicate_existing.add(key)
        existing_by_key[key] = row
    return existing_by_key, duplicate_existing


def build_upsert_preview_from_rows(
    *,
    table_name: str,
    app_token: str,
    table_id: str,
    field_names: list[str],
    existing_rows: list[dict[str, Any]],
    payload_rows: list[dict[str, Any]],
    primary_field: str,
) -> dict[str, Any]:
    existing_by_key, duplicate_existing = rows_to_existing_index(existing_rows, primary_field)
    known_fields = set(field_names)
    errors: list[dict[str, Any]] = []
    creates: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    incoming_seen: set[str] = set()

    for index, row in enumerate(payload_rows):
        row_errors: list[str] = []
        unknown_fields = sorted(set(row) - known_fields)
        if unknown_fields:
            row_errors.append(f"Unknown fields: {', '.join(unknown_fields)}")
        match_value = normalize_compare(row.get(primary_field))
        if not match_value:
            row_errors.append(f"Missing primary field '{primary_field}'")
        if match_value in incoming_seen:
            row_errors.append(f"Duplicate incoming primary value '{match_value}'")
        if match_value in duplicate_existing:
            row_errors.append(f"Existing records contain duplicate primary value '{match_value}'")
        if row_errors:
            errors.append({"index": index, "messages": row_errors, "row": row})
            continue

        incoming_seen.add(match_value)
        existing = existing_by_key.get(match_value)
        filtered_row = {key: row[key] for key in row if key in known_fields}
        if not existing:
            creates.append({"index": index, "match_value": match_value, "fields": filtered_row})
            continue

        changes = []
        for field_name, new_value in filtered_row.items():
            old_value = existing.get(field_name, "")
            if normalize_compare(old_value) != normalize_compare(new_value):
                changes.append({"field": field_name, "old": old_value, "new": new_value})
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

    return {
        "table_name": table_name,
        "base": {"obj_token": app_token},
        "table": {"table_id": table_id, "primary_field": primary_field},
        "summary": {
            "creates": len(creates),
            "updates": len(updates),
            "unchanged": len(unchanged),
            "errors": len(errors),
            "can_apply": len(errors) == 0,
        },
        "fields": [{"name": name} for name in field_names],
        "creates": creates,
        "updates": updates,
        "unchanged": unchanged,
        "errors": errors,
    }


def render_strategy_markdown(records: list[dict[str, Any]]) -> str:
    lines = [
        "# 策略主表镜像",
        "",
        "| 策略名称 | 策略名称-CN | 任务场景 | 适用输入 | 预期输出 | 不适用条件 | 关联Skill | 关联仓库 | 最近验证时间 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in records:
        values = [
            value_or_blank(row, "策略名称"),
            value_or_blank(row, "策略名称-CN"),
            value_or_blank(row, "任务场景"),
            value_or_blank(row, "适用输入"),
            value_or_blank(row, "预期输出"),
            value_or_blank(row, "不适用条件"),
            value_or_blank(row, "关联Skill"),
            value_or_blank(row, "关联仓库"),
            value_or_blank(row, "最近验证时间"),
        ]
        sanitized = [value.replace("\n", "<br>").replace("|", " / ") or "-" for value in values]
        lines.append(f"| {' | '.join(sanitized)} |")
    lines.append("")
    return "\n".join(lines)


def render_mirror_readme(table_payloads: dict[str, dict[str, Any]]) -> str:
    lines = [
        "# Feishu Strategy Library Mirror",
        "",
        "该目录是飞书“专家策略库”的结构化镜像导出。",
        "",
        "## 表清单",
        "",
    ]
    for table_name, payload in table_payloads.items():
        lines.append(f"- `{table_name}`: {payload['record_count']} 条记录")
    lines.extend(
        [
            "",
            "## 同步原则",
            "",
            "- 飞书是事实主源，GitHub 只保存镜像与审阅结果。",
            "- `策略负责人` 等人工字段默认不被自动覆盖。",
            "- `适用输入`、`预期输出`、`不适用条件` 为机器建议，可在飞书中人工修订。",
            "",
        ]
    )
    return "\n".join(lines)


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

    def export_library_mirror(
        self,
        *,
        app_token: str,
        app_id: str,
        app_secret: str,
        output_dir: Optional[Path] = None,
    ) -> tuple[dict[str, Any], Path]:
        token = fetch_tenant_access_token(app_id, app_secret)
        headers = openapi_headers(token)
        tables = list_bitable_tables(app_token, headers)
        tables_by_name = {table.get("name"): table for table in tables if table.get("name")}
        mirror_dir = ensure_dir(output_dir or (Path.cwd() / "exports" / MIRROR_ROOT_NAME))
        exported: dict[str, dict[str, Any]] = {}

        for blueprint in LIBRARY_TABLE_BLUEPRINTS:
            table_name = blueprint["table_name"]
            table = tables_by_name.get(table_name)
            if not table:
                continue
            table_id = table["table_id"]
            fields = list_bitable_fields(app_token, table_id, headers)
            records_raw = list_bitable_records(app_token, table_id, headers)
            field_names = [field.get("field_name") or field.get("name") for field in fields]
            records: list[dict[str, Any]] = []
            for record in records_raw:
                row = {"record_id": record["record_id"]}
                for field_name, value in (record.get("fields") or {}).items():
                    row[field_name] = flatten_value(value)
                records.append(row)
            payload = {
                "table_name": table_name,
                "table_id": table_id,
                "primary_field": blueprint["primary_field"],
                "field_names": field_names,
                "record_count": len(records),
                "records": records,
            }
            exported[table_name] = payload
            file_base = TABLE_EXPORT_FILE_MAP[table_name]
            write_json(mirror_dir / f"{file_base}.json", payload)
            write_text(mirror_dir / f"{file_base}.yaml", to_yaml_text(payload) + "\n")

        strategy_payload = exported.get("策略主表", {"records": []})
        write_text(mirror_dir / "strategy-master.md", render_strategy_markdown(strategy_payload["records"]))
        write_text(mirror_dir / "README.md", render_mirror_readme(exported))

        manifest = {
            "app_token": app_token,
            "mirror_dir": str(mirror_dir),
            "tables": {
                table_name: {
                    "table_id": payload["table_id"],
                    "record_count": payload["record_count"],
                    "json_file": str((mirror_dir / f"{TABLE_EXPORT_FILE_MAP[table_name]}.json").resolve()),
                    "yaml_file": str((mirror_dir / f"{TABLE_EXPORT_FILE_MAP[table_name]}.yaml").resolve()),
                }
                for table_name, payload in exported.items()
            },
            "strategy_markdown": str((mirror_dir / "strategy-master.md").resolve()),
            "readme": str((mirror_dir / "README.md").resolve()),
        }
        manifest_path = self.artifact_dir / f"library-mirror-export-{now_stamp()}.json"
        write_json(manifest_path, manifest)
        return manifest, manifest_path

    def publish_library_mirror(
        self,
        *,
        mirror_dir: Path,
        repo_dir: Path,
        commit_message: str,
        remote_url: Optional[str],
        push: bool,
    ) -> tuple[dict[str, Any], Path]:
        mirror_dir = mirror_dir.expanduser().resolve()
        repo_dir = ensure_dir(repo_dir.expanduser().resolve())
        export_target = repo_dir / "exports" / MIRROR_ROOT_NAME
        ensure_dir(export_target.parent)

        if not (repo_dir / ".git").exists():
            run_git(["init", "-b", "main"], repo_dir)

        if remote_url:
            remotes = run_git(["remote"], repo_dir).splitlines()
            if "origin" in remotes:
                run_git(["remote", "set-url", "origin", remote_url], repo_dir)
            else:
                run_git(["remote", "add", "origin", remote_url], repo_dir)

        if export_target.exists():
            shutil.rmtree(export_target)
        shutil.copytree(mirror_dir, export_target)

        status_before = run_git(["status", "--short"], repo_dir).splitlines()
        run_git(["add", "exports"], repo_dir)
        staged_status = run_git(["status", "--short"], repo_dir).splitlines()
        committed = False
        pushed = False
        if staged_status:
            try:
                run_git(["commit", "-m", commit_message], repo_dir)
            except subprocess.CalledProcessError:
                run_git(["config", "user.name", "Codex"], repo_dir)
                run_git(["config", "user.email", "codex@local.invalid"], repo_dir)
                run_git(["commit", "-m", commit_message], repo_dir)
            committed = True
        if push:
            remotes = run_git(["remote"], repo_dir).splitlines()
            if "origin" not in remotes:
                raise RuntimeError("publish-library-mirror --push requires a configured origin remote or --remote-url")
            run_git(["push", "origin", "main"], repo_dir)
            pushed = True

        result = {
            "mirror_dir": str(mirror_dir),
            "repo_dir": str(repo_dir),
            "export_target": str(export_target),
            "remote_url": remote_url or "",
            "status_before": status_before,
            "staged_status": staged_status,
            "committed": committed,
            "pushed": pushed,
            "commit_message": commit_message,
        }
        output_path = self.artifact_dir / f"library-mirror-publish-{now_stamp()}.json"
        write_json(output_path, result)
        return result, output_path

    def sync_library_seeds(
        self,
        *,
        manifest_file: Path,
        app_token: str,
        app_id: str,
        app_secret: str,
        apply_changes: bool,
    ) -> tuple[dict[str, Any], Path]:
        manifest = load_json(manifest_file)
        token = fetch_tenant_access_token(app_id, app_secret)
        headers = openapi_headers(token)
        existing_tables = list_bitable_tables(app_token, headers)
        existing_by_name = {table.get("name"): table for table in existing_tables if table.get("name")}
        sync_result: dict[str, Any] = {
            "app_token": app_token,
            "mode": "apply" if apply_changes else "dry-run",
            "source_manifest": str(manifest_file.resolve()),
            "tables": [],
        }

        for blueprint in LIBRARY_TABLE_BLUEPRINTS:
            table_name = blueprint["table_name"]
            primary_field = blueprint["primary_field"]
            payload_meta = manifest["tables"].get(table_name)
            if not payload_meta:
                sync_result["tables"].append(
                    {"table_name": table_name, "status": "skipped", "reason": "missing from manifest"}
                )
                continue
            payload_rows = load_payload(Path(payload_meta["payload_file"]))
            desired_fields = list(blueprint["fields"])
            table = existing_by_name.get(table_name)
            table_created = False
            if not table and apply_changes:
                table = create_bitable_table(app_token, table_name, headers)
                table_created = True
                existing_by_name[table_name] = table

            if not table:
                sync_result["tables"].append(
                    {
                        "table_name": table_name,
                        "status": "missing",
                        "would_create": True,
                        "desired_primary_field": primary_field,
                        "desired_fields": desired_fields,
                        "preview": {
                            "summary": {
                                "creates": len(payload_rows),
                                "updates": 0,
                                "unchanged": 0,
                                "errors": 0,
                                "can_apply": True,
                            }
                        },
                    }
                )
                continue

            table_id = table["table_id"]
            fields = list_bitable_fields(app_token, table_id, headers)
            created_fields: list[str] = []
            renamed_primary_field = False
            current_field_names = {field.get("field_name") or field.get("name"): field for field in fields}

            primary_exists = primary_field in current_field_names
            if not primary_exists and fields:
                default_field = fields[0]
                default_name = default_field.get("field_name") or default_field.get("name")
                if table_created and len(fields) == 1 and default_name != primary_field:
                    if apply_changes:
                        update_bitable_field(
                            app_token,
                            table_id,
                            default_field["field_id"],
                            field_name=primary_field,
                            headers=headers,
                        )
                    renamed_primary_field = True

            if apply_changes and (table_created or renamed_primary_field):
                fields = list_bitable_fields(app_token, table_id, headers)
                current_field_names = {field.get("field_name") or field.get("name"): field for field in fields}

            missing_fields = [field_name for field_name in desired_fields if field_name not in current_field_names]
            if apply_changes:
                for field_name in missing_fields:
                    create_bitable_field(app_token, table_id, field_name, headers)
                    created_fields.append(field_name)
                if missing_fields:
                    fields = list_bitable_fields(app_token, table_id, headers)

            field_names = [field.get("field_name") or field.get("name") for field in fields]
            field_names_for_preview = list(dict.fromkeys(field_names + desired_fields))
            existing_records_raw = list_bitable_records(app_token, table_id, headers)
            existing_rows = []
            for record in existing_records_raw:
                flattened = {"record_id": record["record_id"]}
                for field_name, value in (record.get("fields") or {}).items():
                    flattened[field_name] = flatten_value(value)
                existing_rows.append(flattened)

            merge_meta = None
            if table_name == "策略主表":
                payload_rows, merge_meta = merge_strategy_payload_rows(payload_rows, existing_rows)

            preview = build_upsert_preview_from_rows(
                table_name=table_name,
                app_token=app_token,
                table_id=table_id,
                field_names=field_names_for_preview,
                existing_rows=existing_rows,
                payload_rows=payload_rows,
                primary_field=primary_field,
            )

            create_batches: list[dict[str, Any]] = []
            update_batches: list[dict[str, Any]] = []
            if apply_changes and preview["summary"]["can_apply"]:
                for start in range(0, len(preview["creates"]), 500):
                    batch = preview["creates"][start : start + 500]
                    if batch:
                        response = batch_create_bitable_records(
                            app_token,
                            table_id,
                            [item["fields"] for item in batch],
                            headers,
                        )
                        create_batches.append({"count": len(batch), "response": response})
                for start in range(0, len(preview["updates"]), 500):
                    batch = preview["updates"][start : start + 500]
                    if batch:
                        response = batch_update_bitable_records(app_token, table_id, batch, headers)
                        update_batches.append({"count": len(batch), "response": response})

            sync_result["tables"].append(
                {
                    "table_name": table_name,
                    "table_id": table_id,
                    "status": "applied" if apply_changes and preview["summary"]["can_apply"] else "planned",
                    "table_created": table_created,
                    "renamed_primary_field": renamed_primary_field,
                    "created_fields": created_fields if apply_changes else missing_fields,
                    "preview": preview,
                    "merge_meta": merge_meta,
                    "apply_result": {
                        "create_batches": create_batches,
                        "update_batches": update_batches,
                    }
                    if apply_changes and preview["summary"]["can_apply"]
                    else None,
                }
            )

        output_path = self.artifact_dir / f"library-sync-{now_stamp()}.json"
        write_json(output_path, sync_result)
        return sync_result, output_path

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

    sync_cmd = subparsers.add_parser(
        "sync-library-seeds",
        help="Dry-run or apply the 5-table library seed bundle to a Feishu bitable base via OpenAPI.",
    )
    sync_cmd.add_argument("--manifest-file", required=True)
    sync_cmd.add_argument("--app-token", required=True)
    sync_cmd.add_argument("--app-id-env", default="FEISHU_APP_ID")
    sync_cmd.add_argument("--app-secret-env", default="FEISHU_APP_SECRET")
    sync_mode = sync_cmd.add_mutually_exclusive_group(required=True)
    sync_mode.add_argument("--dry-run", action="store_true")
    sync_mode.add_argument("--apply", action="store_true")

    export_cmd = subparsers.add_parser(
        "export-library-mirror",
        help="Export the current 5-table Feishu strategy library into JSON/YAML/Markdown mirror files.",
    )
    export_cmd.add_argument("--app-token", required=True)
    export_cmd.add_argument("--output-dir")
    export_cmd.add_argument("--app-id-env", default="FEISHU_APP_ID")
    export_cmd.add_argument("--app-secret-env", default="FEISHU_APP_SECRET")

    publish_cmd = subparsers.add_parser(
        "publish-library-mirror",
        help="Copy the exported library mirror into a git repo, commit it, and optionally push.",
    )
    publish_cmd.add_argument("--mirror-dir", required=True)
    publish_cmd.add_argument("--repo-dir", required=True)
    publish_cmd.add_argument("--remote-url")
    publish_cmd.add_argument("--commit-message", default="Update Feishu strategy library mirror")
    publish_mode = publish_cmd.add_mutually_exclusive_group(required=True)
    publish_mode.add_argument("--commit-only", action="store_true")
    publish_mode.add_argument("--push", action="store_true")

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

    if args.command == "sync-library-seeds":
        app_id = os.environ.get(args.app_id_env)
        app_secret = os.environ.get(args.app_secret_env)
        if not app_id or not app_secret:
            raise RuntimeError(
                f"sync-library-seeds requires {args.app_id_env} and {args.app_secret_env} in the environment"
            )
        result, output_path = bridge.sync_library_seeds(
            manifest_file=Path(args.manifest_file).expanduser().resolve(),
            app_token=args.app_token,
            app_id=app_id,
            app_secret=app_secret,
            apply_changes=args.apply,
        )
        result["output_path"] = str(output_path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "export-library-mirror":
        app_id = os.environ.get(args.app_id_env)
        app_secret = os.environ.get(args.app_secret_env)
        if not app_id or not app_secret:
            raise RuntimeError(
                f"export-library-mirror requires {args.app_id_env} and {args.app_secret_env} in the environment"
            )
        manifest, output_path = bridge.export_library_mirror(
            app_token=args.app_token,
            app_id=app_id,
            app_secret=app_secret,
            output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
        )
        manifest["output_path"] = str(output_path)
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return 0

    if args.command == "publish-library-mirror":
        result, output_path = bridge.publish_library_mirror(
            mirror_dir=Path(args.mirror_dir),
            repo_dir=Path(args.repo_dir),
            remote_url=args.remote_url,
            commit_message=args.commit_message,
            push=args.push,
        )
        result["output_path"] = str(output_path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
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
