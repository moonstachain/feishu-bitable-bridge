#!/usr/bin/env python3
import argparse
import base64
import gzip
import json
import os
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
                row[field_map[field_id]["name"]] = flatten_value(record_data.get(field_id, {}).get("value"))
            records.append(row)

        return (
            FeishuSummary(
                base_name=schema["base"]["name"],
                obj_token=schema["base"]["token"],
                table_id=probe["resolved_table_id"],
                view_id=resolved_view_id,
                primary_field_id=primary_field_id,
                primary_field_name=primary_field_name,
                fields=fields,
                records=records,
                record_count=table["meta"]["recordsNum"],
                raw_probe_path=str(raw_probe_path),
            ),
            raw_probe_path,
        )

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
