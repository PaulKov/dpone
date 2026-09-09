#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MANIFEST = "examples/batch/landing_appsflyer_api.batch.yaml"
DEFAULT_SELECTOR = "app.installs_report"
DEFAULT_RESOURCE = "installs_report"
DEFAULT_VAULT_PATH = "api/appsflyer"
DEFAULT_TIMEZONE = "Europe/Moscow"
DEFAULT_MAXIMUM_ROWS = 10000


@dataclass(frozen=True)
class AppsflyerSmokeSettings:
    vault_path: str
    app_ids: tuple[str, ...]
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    timezone: str
    date_from: str
    date_to: str
    maximum_rows: int

    @property
    def default_app_id(self) -> str:
        return self.app_ids[0]


def _parse_csv_list(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _resolve_window(date_from: str | None, date_to: str | None, days_back: int) -> tuple[str, str]:
    if date_from and date_to:
        return date_from, date_to
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(days_back - 1, 0))
    return start_date.isoformat(), end_date.isoformat()


def read_settings(args: argparse.Namespace) -> AppsflyerSmokeSettings:
    app_ids = tuple(args.app_id) if args.app_id else _parse_csv_list(os.getenv("DPONE_IT_AF_APP_IDS"))
    if not app_ids:
        raise SystemExit("Appsflyer live smoke requires at least one app id via --app-id or DPONE_IT_AF_APP_IDS")

    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_AF_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)

    date_from, date_to = _resolve_window(
        args.date_from or os.getenv("DPONE_IT_AF_DATE_FROM"),
        args.date_to or os.getenv("DPONE_IT_AF_DATE_TO"),
        int(args.days_back if args.days_back is not None else (os.getenv("DPONE_IT_AF_DAYS_BACK") or 2)),
    )

    resource = args.resource or os.getenv("DPONE_IT_AF_RESOURCE") or DEFAULT_RESOURCE
    manifest = args.manifest or os.getenv("DPONE_IT_AF_MANIFEST") or DEFAULT_MANIFEST
    vault_path = args.vault_path or os.getenv("DPONE_IT_AF_VAULT_PATH") or DEFAULT_VAULT_PATH
    timezone = args.timezone or os.getenv("DPONE_IT_AF_TIMEZONE") or DEFAULT_TIMEZONE
    maximum_rows = int(args.maximum_rows or os.getenv("DPONE_IT_AF_MAXIMUM_ROWS") or DEFAULT_MAXIMUM_ROWS)

    return AppsflyerSmokeSettings(
        vault_path=vault_path,
        app_ids=app_ids,
        resource=resource,
        selectors=selectors,
        manifest=manifest,
        timezone=timezone,
        date_from=date_from,
        date_to=date_to,
        maximum_rows=maximum_rows,
    )


def run_connector_smoke(settings: AppsflyerSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.appsflyer import AppsflyerConnector, AppsflyerQuotaExceededError

    connector = AppsflyerConnector.from_vault(
        vault_path=settings.vault_path,
        default_app_id=settings.default_app_id,
        max_retries=1,
        timeout=60,
    )
    try:
        rows = connector.fetch_resource_rows(
            resource_name=settings.resource,
            app_id=settings.default_app_id,
            from_value=settings.date_from,
            to_value=settings.date_to,
            timezone_name=settings.timezone,
            maximum_rows=settings.maximum_rows,
        )
    except AppsflyerQuotaExceededError as exc:
        return {
            "mode": "connector",
            "skipped": True,
            "skip_reason": str(exc),
            "resource": settings.resource,
            "app_id": settings.default_app_id,
            "date_from": settings.date_from,
            "date_to": settings.date_to,
        }
    payload: dict[str, Any] = {
        "mode": "connector",
        "resource": settings.resource,
        "app_id": settings.default_app_id,
        "date_from": settings.date_from,
        "date_to": settings.date_to,
        "skipped": False,
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        first = rows[0]
        payload["sample"] = {key: first[key] for key in ("source_app_id", "resource_name", "date") if key in first}
    return payload


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


def _patch_manifest_for_live_run(source: Path, settings: AppsflyerSmokeSettings) -> tuple[Path, tuple[str, ...]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    options = source_cfg.setdefault("options", {})
    options["app_ids"] = list(settings.app_ids)
    options["timezone"] = settings.timezone
    options["date_from"] = settings.date_from
    options["date_to"] = settings.date_to
    options["maximum_rows"] = settings.maximum_rows

    schemas = data.get("schemas") or {}
    app_schema = schemas.get("app")
    if not isinstance(app_schema, dict):
        raise ValueError("Expected schema 'app' in AppsFlyer batch manifest")
    tables = app_schema.get("tables") or []
    filtered_tables: list[Any] = []
    present_tables: set[str] = set()
    for entry in tables:
        if isinstance(entry, str):
            table_name = entry
        elif isinstance(entry, dict):
            table_name = str(entry.get("table") or "")
        else:
            continue
        if table_name in selected_tables:
            filtered_tables.append(entry)
            present_tables.add(table_name)
    missing = selected_tables - present_tables
    if missing:
        raise ValueError(f"Selected AppsFlyer selectors are not present in manifest: {sorted(missing)}")
    app_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-appsflyer-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"AppsFlyer manifest not found: {candidate}")


def run_manifest_smoke(settings: AppsflyerSmokeSettings, *, project_root: Path) -> dict[str, Any]:
    from dpone.contracts.run_context import RunContext
    from dpone.dag.config import ETLProcessConfig
    from dpone.dag.loader import ConfigLoader
    from dpone.dag.process import ETLProcess

    manifest_path = _resolve_manifest_path(project_root, settings.manifest)

    temp_manifest, selectors = _patch_manifest_for_live_run(manifest_path, settings)
    os.environ.setdefault("DPONE_PROJECT_DIR", str(project_root.resolve()))

    loader = ConfigLoader(base_path=manifest_path.parent)
    manifest = loader.get_manifest(temp_manifest, metadata_only=True)
    task_count = len(manifest.processes) if manifest else 0

    results: list[dict[str, Any]] = []
    for selector in selectors:
        ref = f"{temp_manifest}#{selector}"
        config = ETLProcessConfig.from_yaml(ref)
        process = ETLProcess(config=config, config_path=ref)
        result = process.run(context=RunContext(run_id=f"appsflyer-live-{selector}"))
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
        "date_from": settings.date_from,
        "date_to": settings.date_to,
        "app_ids": list(settings.app_ids),
        "results": results,
    }


def _dump_payload(payload: Mapping[str, Any], *, fmt: str) -> str:
    if fmt == "json":
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    lines = []
    for key, value in payload.items():
        if isinstance(value, dict | list):
            rendered = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
            lines.append(f"{key}:\n{rendered}")
        else:
            lines.append(f"{key}: {value}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run live/manual AppsFlyer smoke checks against dpone runtime.")
    parser.add_argument(
        "--project-root", default=".", help="Repository checkout root used to resolve example manifests"
    )
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument(
        "--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path used for manifest-run mode"
    )
    parser.add_argument("--resource", default=None, help="AppsFlyer resource used for connector mode")
    parser.add_argument("--selector", action="append", default=[], help="Selector(s) to run in manifest-run mode")
    parser.add_argument("--app-id", action="append", default=[], help="Repeatable AppsFlyer app id")
    parser.add_argument("--vault-path", default=None, help="Vault path for AppsFlyer credentials")
    parser.add_argument("--timezone", default=None)
    parser.add_argument("--date-from", default=None, help="Explicit YYYY-MM-DD start date")
    parser.add_argument("--date-to", default=None, help="Explicit YYYY-MM-DD end date")
    parser.add_argument("--days-back", type=int, default=None, help="Used when date range is not specified")
    parser.add_argument("--maximum-rows", type=int, default=None)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument(
        "--dry-run", action="store_true", help="Only print resolved plan without network or runtime execution"
    )
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
