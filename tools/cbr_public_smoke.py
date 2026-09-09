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

DEFAULT_MANIFEST = "examples/batch/landing_cbr_api.batch.yaml"
DEFAULT_SELECTOR = "app.xml_daily_asp"
DEFAULT_RESOURCE = "xml_daily_asp"


@dataclass(frozen=True)
class CbrSmokeSettings:
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    day: str | None
    date_from: str | None
    date_to: str | None
    days_back: int
    timeout: int
    retries: int
    retry_delay: float


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


def _resolve_request(day: str | None, date_from: str | None, date_to: str | None, days_back: int) -> dict[str, str]:
    if day:
        return {"day": day}
    if date_from or date_to:
        payload: dict[str, str] = {}
        if date_from:
            payload["start_date"] = date_from
        if date_to:
            payload["end_date"] = date_to
        return payload
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(days_back - 1, 0))
    return {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}


def read_settings(args: argparse.Namespace) -> CbrSmokeSettings:
    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_CBR_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)
    return CbrSmokeSettings(
        resource=args.resource or os.getenv("DPONE_IT_CBR_RESOURCE") or DEFAULT_RESOURCE,
        selectors=selectors,
        manifest=args.manifest or os.getenv("DPONE_IT_CBR_MANIFEST") or DEFAULT_MANIFEST,
        day=args.day or os.getenv("DPONE_IT_CBR_DAY") or None,
        date_from=args.date_from or os.getenv("DPONE_IT_CBR_DATE_FROM") or None,
        date_to=args.date_to or os.getenv("DPONE_IT_CBR_DATE_TO") or None,
        days_back=int(args.days_back if args.days_back is not None else (os.getenv("DPONE_IT_CBR_DAYS_BACK") or 3)),
        timeout=int(args.timeout if args.timeout is not None else (os.getenv("DPONE_IT_CBR_TIMEOUT") or 60)),
        retries=int(args.retries if args.retries is not None else (os.getenv("DPONE_IT_CBR_RETRIES") or 3)),
        retry_delay=float(
            args.retry_delay if args.retry_delay is not None else (os.getenv("DPONE_IT_CBR_RETRY_DELAY") or 2.0)
        ),
    )


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"CBR manifest not found: {candidate}")


def run_connector_smoke(settings: CbrSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.cbr import CbrConnector

    connector = CbrConnector(timeout=settings.timeout, retries=settings.retries, retry_delay=settings.retry_delay)
    request = _resolve_request(settings.day, settings.date_from, settings.date_to, settings.days_back)
    rows = list(connector.get_resources(settings.resource, filters=request))
    payload: dict[str, Any] = {
        "mode": "connector",
        "health_check": connector.health_check(),
        "resource": settings.resource,
        "request": request,
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = rows[0]
    return payload


def _patch_manifest_for_live_run(
    source: Path, settings: CbrSmokeSettings
) -> tuple[Path, tuple[str, ...], dict[str, str]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}
    request = _resolve_request(settings.day, settings.date_from, settings.date_to, settings.days_back)

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    options = source_cfg.setdefault("options", {})
    options["resource"] = settings.resource
    for key in ("day", "start_date", "end_date"):
        options.pop(key, None)
    options.update(request)

    schemas = data.get("schemas") or {}
    app_schema = schemas.get("app")
    if not isinstance(app_schema, dict):
        raise ValueError("Expected schema 'app' in CBR batch manifest")
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
        raise ValueError(f"Selected CBR selectors are not present in manifest: {sorted(missing)}")
    app_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-cbr-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors, request


def run_manifest_smoke(settings: CbrSmokeSettings, *, project_root: Path) -> dict[str, Any]:
    from dpone.contracts.run_context import RunContext
    from dpone.dag.config import ETLProcessConfig
    from dpone.dag.loader import ConfigLoader
    from dpone.dag.process import ETLProcess

    manifest_path = _resolve_manifest_path(project_root, settings.manifest)
    temp_manifest, selectors, request = _patch_manifest_for_live_run(manifest_path, settings)
    os.environ.setdefault("DPONE_PROJECT_DIR", str(project_root.resolve()))

    loader = ConfigLoader(base_path=manifest_path.parent)
    manifest = loader.get_manifest(temp_manifest, metadata_only=True)
    task_count = len(manifest.processes) if manifest else 0

    results: list[dict[str, Any]] = []
    for selector in selectors:
        ref = f"{temp_manifest}#{selector}"
        config = ETLProcessConfig.from_yaml(ref)
        process = ETLProcess(config=config, config_path=ref)
        result = process.run(context=RunContext(run_id=f"cbr-live-{selector}"))
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
        "request": request,
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
    parser = argparse.ArgumentParser(description="Run public/manual CBR smoke checks against dpone runtime.")
    parser.add_argument(
        "--project-root", default=".", help="Repository checkout root used to resolve example manifests"
    )
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument(
        "--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path used for manifest-run mode"
    )
    parser.add_argument(
        "--selector", action="append", default=[], help="Batch selector to run/filter, e.g. app.xml_daily_asp"
    )
    parser.add_argument("--resource", default=None, help="CBR resource used for connector or manifest mode")
    parser.add_argument("--day", default=None, help="Single requested day (YYYY-MM-DD)")
    parser.add_argument("--date-from", default=None, help="Requested date range lower bound (YYYY-MM-DD)")
    parser.add_argument("--date-to", default=None, help="Requested date range upper bound (YYYY-MM-DD)")
    parser.add_argument(
        "--days-back", type=int, default=None, help="Rolling window when explicit day/range is not provided"
    )
    parser.add_argument("--timeout", type=int, default=None, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=None, help="HTTP retry count")
    parser.add_argument("--retry-delay", type=float, default=None, help="HTTP retry delay factor")
    parser.add_argument("--format", choices=["json", "text"], default="json")
    parser.add_argument(
        "--dry-run", action="store_true", help="Only print resolved configuration and do not hit live systems"
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = read_settings(args)
    if args.dry_run:
        print(_dump_payload({"mode": args.mode, "settings": asdict(settings)}, fmt=args.format))
        return 0

    project_root = Path(args.project_root).resolve()
    if args.mode == "connector":
        payload = run_connector_smoke(settings)
    else:
        payload = run_manifest_smoke(settings, project_root=project_root)
    print(_dump_payload(payload, fmt=args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
