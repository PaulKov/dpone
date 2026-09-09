#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MANIFEST = "examples/batch/landing_yandex_webmaster_api.batch.yaml"
DEFAULT_SELECTOR = "app.host_metrics_daily"
DEFAULT_RESOURCE = "host_metrics_daily"
DEFAULT_VAULT_PATH = "api/yandex_webmaster"
DEFAULT_HOST_ID = "https:travel.example.com:443"
DEFAULT_DAYS_BACK = 3
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 1
DEFAULT_RATE_LIMIT_DELAY = 0.5
DEFAULT_PAGES_IN_SEARCH_DAILY_AGG = "last"
YWM_ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "SELECTORS": ("DPONE_IT_YANDEX_WEBMASTER_SELECTORS", "DPONE_IT_YWM_SELECTORS"),
    "RESOURCE": ("DPONE_IT_YANDEX_WEBMASTER_RESOURCE", "DPONE_IT_YWM_RESOURCE"),
    "HOST_ID": ("DPONE_IT_YANDEX_WEBMASTER_HOST_ID", "DPONE_IT_YWM_HOST_ID"),
    "HOST_URL": ("DPONE_IT_YANDEX_WEBMASTER_HOST_URL", "DPONE_IT_YWM_HOST_URL"),
    "USER_ID": ("DPONE_IT_YANDEX_WEBMASTER_USER_ID", "DPONE_IT_YWM_USER_ID"),
    "DAYS_BACK": ("DPONE_IT_YANDEX_WEBMASTER_DAYS_BACK", "DPONE_IT_YWM_DAYS_BACK"),
    "VAULT_PATH": ("DPONE_IT_YANDEX_WEBMASTER_VAULT_PATH", "DPONE_IT_YWM_VAULT_PATH"),
    "MANIFEST": ("DPONE_IT_YANDEX_WEBMASTER_MANIFEST", "DPONE_IT_YWM_MANIFEST"),
    "DEVICE_TYPES": ("DPONE_IT_YANDEX_WEBMASTER_DEVICE_TYPES", "DPONE_IT_YWM_DEVICE_TYPES"),
    "REGION_IDS": ("DPONE_IT_YANDEX_WEBMASTER_REGION_IDS", "DPONE_IT_YWM_REGION_IDS"),
    "DAY": ("DPONE_IT_YANDEX_WEBMASTER_DAY", "DPONE_IT_YWM_DAY"),
    "DATE_FROM": ("DPONE_IT_YANDEX_WEBMASTER_DATE_FROM", "DPONE_IT_YWM_DATE_FROM"),
    "DATE_TO": ("DPONE_IT_YANDEX_WEBMASTER_DATE_TO", "DPONE_IT_YWM_DATE_TO"),
    "TIMEOUT": ("DPONE_IT_YANDEX_WEBMASTER_TIMEOUT", "DPONE_IT_YWM_TIMEOUT"),
    "MAX_RETRIES": ("DPONE_IT_YANDEX_WEBMASTER_MAX_RETRIES", "DPONE_IT_YWM_MAX_RETRIES"),
    "RATE_LIMIT_DELAY": (
        "DPONE_IT_YANDEX_WEBMASTER_RATE_LIMIT_DELAY",
        "DPONE_IT_YWM_RATE_LIMIT_DELAY",
    ),
    "PAGES_IN_SEARCH_DAILY_AGG": (
        "DPONE_IT_YANDEX_WEBMASTER_PAGES_IN_SEARCH_DAILY_AGG",
        "DPONE_IT_YWM_PAGES_IN_SEARCH_DAILY_AGG",
    ),
}


@dataclass(frozen=True)
class YandexWebmasterSmokeSettings:
    vault_path: str
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    user_id: int | None
    host_id: str | None
    host_url: str | None
    device_types: tuple[str, ...]
    region_ids: tuple[int, ...]
    day: str | None
    date_from: str | None
    date_to: str | None
    days_back: int
    timeout: int
    max_retries: int
    rate_limit_delay: float
    pages_in_search_daily_agg: str


def _parse_csv_list(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _normalize_selector(selector: str) -> str:
    value = selector.strip()
    if not value:
        raise ValueError("selector must not be empty")
    return value


def _table_from_selector(selector: str) -> str:
    value = _normalize_selector(selector)
    if "." in value:
        return value.split(".", 1)[1]
    return value


def _resource_from_selectors(selectors: tuple[str, ...]) -> str:
    if not selectors:
        return DEFAULT_RESOURCE
    return _table_from_selector(selectors[0])


def _default_days_back_for_resource(resource: str) -> int:
    if resource in {"search_queries_history_daily", "query_analytics_by_region_daily"}:
        return 14
    return DEFAULT_DAYS_BACK


def _parse_int_csv_list(value: str | None) -> tuple[int, ...]:
    if not value:
        return ()
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _getenv_alias(key: str) -> str | None:
    for env_name in YWM_ENV_ALIASES[key]:
        value = os.getenv(env_name)
        if value is not None:
            return value
    return None


def _resolve_window(
    day: str | None, date_from: str | None, date_to: str | None, days_back: int
) -> tuple[str | None, str | None, str | None]:
    if day:
        return day, None, None
    if date_from or date_to:
        start = date_from or date_to
        end = date_to or date_from
        return None, start, end
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(days_back - 1, 0))
    return None, start_date.isoformat(), end_date.isoformat()


def read_settings(args: argparse.Namespace) -> YandexWebmasterSmokeSettings:
    selectors = tuple(args.selector) if args.selector else _parse_csv_list(_getenv_alias("SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)
    resolved_resource = args.resource or _getenv_alias("RESOURCE") or _resource_from_selectors(selectors)

    host_id = args.host_id or _getenv_alias("HOST_ID") or DEFAULT_HOST_ID
    host_url = args.host_url or _getenv_alias("HOST_URL") or None

    user_id_raw = (str(args.user_id) if args.user_id is not None else (_getenv_alias("USER_ID") or "")).strip()
    days_back_raw = str(args.days_back) if args.days_back is not None else str(_getenv_alias("DAYS_BACK") or "").strip()

    return YandexWebmasterSmokeSettings(
        vault_path=args.vault_path or _getenv_alias("VAULT_PATH") or DEFAULT_VAULT_PATH,
        resource=resolved_resource,
        selectors=selectors,
        manifest=args.manifest or _getenv_alias("MANIFEST") or DEFAULT_MANIFEST,
        user_id=int(user_id_raw) if user_id_raw else None,
        host_id=host_id,
        host_url=host_url,
        device_types=_parse_csv_list(args.device_types or _getenv_alias("DEVICE_TYPES")),
        region_ids=_parse_int_csv_list(args.region_ids or _getenv_alias("REGION_IDS")),
        day=args.day or _getenv_alias("DAY") or None,
        date_from=args.date_from or _getenv_alias("DATE_FROM") or None,
        date_to=args.date_to or _getenv_alias("DATE_TO") or None,
        days_back=int(days_back_raw or str(_default_days_back_for_resource(resolved_resource))),
        timeout=int(args.timeout if args.timeout is not None else (_getenv_alias("TIMEOUT") or DEFAULT_TIMEOUT)),
        max_retries=int(
            args.max_retries if args.max_retries is not None else (_getenv_alias("MAX_RETRIES") or DEFAULT_MAX_RETRIES)
        ),
        rate_limit_delay=float(
            args.rate_limit_delay
            if args.rate_limit_delay is not None
            else (_getenv_alias("RATE_LIMIT_DELAY") or DEFAULT_RATE_LIMIT_DELAY)
        ),
        pages_in_search_daily_agg=args.pages_in_search_daily_agg
        or _getenv_alias("PAGES_IN_SEARCH_DAILY_AGG")
        or DEFAULT_PAGES_IN_SEARCH_DAILY_AGG,
    )


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"Yandex Webmaster manifest not found: {candidate}")


def run_connector_smoke(settings: YandexWebmasterSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.yandex_webmaster import YandexWebmasterConnector
    from dpone.runtime.sources.strategies.api.yandex_webmaster.common import (
        build_yandex_webmaster_rows,
        parse_device_types,
    )

    connector = YandexWebmasterConnector.from_vault(
        vault_path=settings.vault_path,
        timeout=settings.timeout,
        max_retries=settings.max_retries,
        rate_limit_delay=settings.rate_limit_delay,
    )
    day, date_from, date_to = _resolve_window(settings.day, settings.date_from, settings.date_to, settings.days_back)
    resolved_day = day or date_from
    rows = build_yandex_webmaster_rows(
        connector=connector,
        resource=settings.resource,
        user_id=settings.user_id,
        host_id=settings.host_id,
        host_url=settings.host_url,
        date_from=date.fromisoformat(str(resolved_day)),
        date_to=date.fromisoformat(str(day or date_to)),
        pages_in_search_daily_agg=settings.pages_in_search_daily_agg,
        device_types=parse_device_types(settings.device_types),
        region_ids=settings.region_ids or None,
    )
    payload: dict[str, Any] = {
        "mode": "connector",
        "resource": settings.resource,
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = rows[0]
    return payload


def _patch_manifest_for_live_run(
    source: Path, settings: YandexWebmasterSmokeSettings
) -> tuple[Path, tuple[str, ...], dict[str, Any]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}
    day, date_from, date_to = _resolve_window(settings.day, settings.date_from, settings.date_to, settings.days_back)

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    source_cfg["vault_path"] = settings.vault_path
    options = source_cfg.setdefault("options", {})
    options["resource"] = settings.resource
    options["user_id"] = settings.user_id
    options["host_id"] = settings.host_id
    options["host_url"] = settings.host_url
    if settings.device_types:
        options["device_types"] = ",".join(settings.device_types)
    if settings.region_ids:
        options["region_ids"] = ",".join(str(item) for item in settings.region_ids)
    options["day"] = day
    options["start_date"] = date_from
    options["end_date"] = date_to
    options["lookback_days"] = settings.days_back
    options["timeout"] = settings.timeout
    options["max_retries"] = settings.max_retries
    options["rate_limit_delay"] = settings.rate_limit_delay
    options["pages_in_search_daily_agg"] = settings.pages_in_search_daily_agg

    schemas = data.get("schemas") or {}
    app_schema = schemas.get("app")
    if not isinstance(app_schema, dict):
        raise ValueError("Expected schema 'app' in Yandex Webmaster batch manifest")
    tables = app_schema.get("tables") or []
    filtered_tables: list[Any] = []
    present_tables: set[str] = set()
    for entry in tables:
        if isinstance(entry, str):
            table_name = entry
            entry_payload: Any = {"table": table_name}
        elif isinstance(entry, dict):
            table_name = str(entry.get("table") or "")
            entry_payload = copy.deepcopy(entry)
        else:
            continue
        if table_name in selected_tables:
            overrides = entry_payload.setdefault("overrides", {})
            source_override = overrides.setdefault("source", {})
            source_options = source_override.setdefault("options", {})
            source_options["resource"] = settings.resource if len(selected_tables) == 1 else table_name
            if settings.device_types:
                source_options["device_types"] = ",".join(settings.device_types)
            if settings.region_ids:
                source_options["region_ids"] = ",".join(str(item) for item in settings.region_ids)
            filtered_tables.append(entry_payload)
            present_tables.add(table_name)
    missing = selected_tables - present_tables
    if missing:
        raise ValueError(f"Selected Yandex Webmaster selectors are not present in manifest: {sorted(missing)}")
    app_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-yandex-webmaster-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors, {"day": day, "start_date": date_from, "end_date": date_to}


def run_manifest_smoke(settings: YandexWebmasterSmokeSettings, *, project_root: Path) -> dict[str, Any]:
    from dpone.contracts.run_context import RunContext
    from dpone.dag.config import ETLProcessConfig
    from dpone.dag.loader import ConfigLoader
    from dpone.dag.process import ETLProcess

    manifest_path = _resolve_manifest_path(project_root, settings.manifest)
    temp_manifest, selectors, manifest_options = _patch_manifest_for_live_run(manifest_path, settings)
    os.environ.setdefault("DPONE_PROJECT_DIR", str(project_root.resolve()))

    loader = ConfigLoader(base_path=manifest_path.parent)
    manifest = loader.get_manifest(temp_manifest, metadata_only=True)
    task_count = len(manifest.processes) if manifest else 0

    results: list[dict[str, Any]] = []
    for selector in selectors:
        ref = f"{temp_manifest}#{selector}"
        config = ETLProcessConfig.from_yaml(ref)
        process = ETLProcess(config=config, config_path=ref)
        result = process.run(context=RunContext(run_id=f"yandex-webmaster-live-{selector}"))
        results.append(
            {
                "selector": selector,
                "status": result.status,
                "inserted_rows": result.inserted_rows,
                "updated_rows": result.updated_rows,
                "final_rows": result.final_rows,
                "extracted_rows": result.extracted_rows,
                "duration_seconds": result.duration_seconds,
            }
        )

    return {
        "mode": "manifest-run",
        "manifest": str(manifest_path),
        "patched_manifest": str(temp_manifest),
        "selectors": list(selectors),
        "task_count": task_count,
        "manifest_options": manifest_options,
        "results": results,
    }


def _dump_payload(payload: Mapping[str, Any], *, fmt: str) -> str:
    if fmt == "json":
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    lines = []
    for key, value in payload.items():
        if isinstance(value, dict | list):
            rendered = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)
            lines.append(f"{key}:\n{rendered}")
        else:
            lines.append(f"{key}: {value}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run manual/live Yandex Webmaster smoke checks against dpone runtime.")
    parser.add_argument("--project-root", default=".", help="Repository checkout root used to resolve manifests")
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path for manifest-run")
    parser.add_argument("--resource", default=None)
    parser.add_argument("--selector", action="append", default=[], help="Batch selector to run/filter")
    parser.add_argument("--vault-path", default=None)
    parser.add_argument("--user-id", type=int, default=None)
    parser.add_argument("--host-id", default=None)
    parser.add_argument("--host-url", default=None)
    parser.add_argument("--device-types", default=None)
    parser.add_argument("--region-ids", default=None)
    parser.add_argument("--day", default=None)
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--days-back", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--rate-limit-delay", type=float, default=None)
    parser.add_argument("--pages-in-search-daily-agg", default=None)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--dry-run", action="store_true", help="Only print resolved plan without execution")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = read_settings(args)
    project_root = Path(args.project_root).resolve()
    payload: dict[str, Any] = {
        "settings": asdict(settings),
        "project_root": str(project_root),
        "mode": args.mode,
    }
    if args.dry_run:
        print(_dump_payload(payload, fmt=args.format))
        return 0

    if args.mode == "connector":
        payload["result"] = run_connector_smoke(settings)
    else:
        payload["result"] = run_manifest_smoke(settings, project_root=project_root)
    print(_dump_payload(payload, fmt=args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
