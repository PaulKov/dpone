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

DEFAULT_MANIFEST = "examples/batch/landing_openexchangerates_api.batch.yaml"
DEFAULT_SELECTOR = "default.historical_rates_daily"
DEFAULT_RESOURCE = "historical_rates_daily"
DEFAULT_VAULT_PATH = "api/openexchangerates"
DEFAULT_SYMBOLS = ("ARS", "RUB", "EUR")
DEFAULT_DAYS_BACK = 1
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 1
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_RATE_LIMIT_DELAY = 1.0


@dataclass(frozen=True)
class OpenExchangeRatesSmokeSettings:
    vault_path: str
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    symbols: tuple[str, ...]
    day: str | None
    date_from: str | None
    date_to: str | None
    days_back: int
    timeout: int
    max_retries: int
    retry_delay: float
    rate_limit_delay: float


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


def read_settings(args: argparse.Namespace) -> OpenExchangeRatesSmokeSettings:
    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_OXR_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)

    symbols = tuple(args.symbol) if args.symbol else _parse_csv_list(os.getenv("DPONE_IT_OXR_SYMBOLS"))
    if not symbols:
        symbols = DEFAULT_SYMBOLS

    return OpenExchangeRatesSmokeSettings(
        vault_path=args.vault_path or os.getenv("DPONE_IT_OXR_VAULT_PATH") or DEFAULT_VAULT_PATH,
        resource=args.resource or os.getenv("DPONE_IT_OXR_RESOURCE") or DEFAULT_RESOURCE,
        selectors=selectors,
        manifest=args.manifest or os.getenv("DPONE_IT_OXR_MANIFEST") or DEFAULT_MANIFEST,
        symbols=symbols,
        day=args.day or os.getenv("DPONE_IT_OXR_DAY") or None,
        date_from=args.date_from or os.getenv("DPONE_IT_OXR_DATE_FROM") or None,
        date_to=args.date_to or os.getenv("DPONE_IT_OXR_DATE_TO") or None,
        days_back=int(args.days_back if args.days_back is not None else (os.getenv("DPONE_IT_OXR_DAYS_BACK") or 1)),
        timeout=int(
            args.timeout if args.timeout is not None else (os.getenv("DPONE_IT_OXR_TIMEOUT") or DEFAULT_TIMEOUT)
        ),
        max_retries=int(
            args.max_retries if args.max_retries is not None else (os.getenv("DPONE_IT_OXR_MAX_RETRIES") or 1)
        ),
        retry_delay=float(
            args.retry_delay if args.retry_delay is not None else (os.getenv("DPONE_IT_OXR_RETRY_DELAY") or 1.0)
        ),
        rate_limit_delay=float(
            args.rate_limit_delay
            if args.rate_limit_delay is not None
            else (os.getenv("DPONE_IT_OXR_RATE_LIMIT_DELAY") or DEFAULT_RATE_LIMIT_DELAY)
        ),
    )


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"OpenExchangeRates manifest not found: {candidate}")


def run_connector_smoke(settings: OpenExchangeRatesSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.openexchangerates import OpenExchangeRatesConnector

    connector = OpenExchangeRatesConnector.from_vault(
        vault_path=settings.vault_path,
        timeout=settings.timeout,
        max_retries=settings.max_retries,
        retry_delay=settings.retry_delay,
        rate_limit_delay=settings.rate_limit_delay,
    )
    request = _resolve_request(settings.day, settings.date_from, settings.date_to, settings.days_back)
    rows = list(
        connector.get_resources(
            settings.resource,
            filters={
                **request,
                "symbols": settings.symbols,
            },
        )
    )
    payload: dict[str, Any] = {
        "mode": "connector",
        "health_check": connector.health_check(),
        "resource": settings.resource,
        "request": request,
        "symbols": list(settings.symbols),
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = rows[0]
    return payload


def _patch_manifest_for_live_run(
    source: Path, settings: OpenExchangeRatesSmokeSettings
) -> tuple[Path, tuple[str, ...], dict[str, Any]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}
    request = _resolve_request(settings.day, settings.date_from, settings.date_to, settings.days_back)

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    source_cfg["vault_path"] = settings.vault_path
    options = source_cfg.setdefault("options", {})
    options["resource"] = settings.resource
    options["symbols"] = list(settings.symbols)
    options["timeout"] = settings.timeout
    options["max_retries"] = settings.max_retries
    options["retry_delay"] = settings.retry_delay
    options["rate_limit_delay"] = settings.rate_limit_delay
    for key in ("day", "start_date", "end_date"):
        options.pop(key, None)
    options.update(request)

    schemas = data.get("schemas") or {}
    default_schema = schemas.get("default")
    if not isinstance(default_schema, dict):
        raise ValueError("Expected schema 'default' in OpenExchangeRates batch manifest")
    tables = default_schema.get("tables") or []
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
        raise ValueError(f"Selected OpenExchangeRates selectors are not present in manifest: {sorted(missing)}")
    default_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-openexchangerates-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors, {"request": request, "symbols": list(settings.symbols)}


def run_manifest_smoke(settings: OpenExchangeRatesSmokeSettings, *, project_root: Path) -> dict[str, Any]:
    from dpone.contracts.run_context import RunContext
    from dpone.dag.config import ETLProcessConfig
    from dpone.dag.loader import ConfigLoader
    from dpone.dag.process import ETLProcess

    manifest_path = _resolve_manifest_path(project_root, settings.manifest)
    temp_manifest, selectors, details = _patch_manifest_for_live_run(manifest_path, settings)
    os.environ.setdefault("DPONE_PROJECT_DIR", str(project_root.resolve()))

    loader = ConfigLoader(base_path=manifest_path.parent)
    manifest = loader.get_manifest(temp_manifest, metadata_only=True)
    task_count = len(manifest.processes) if manifest else 0

    results: list[dict[str, Any]] = []
    for selector in selectors:
        ref = f"{temp_manifest}#{selector}"
        config = ETLProcessConfig.from_yaml(ref)
        process = ETLProcess(config=config, config_path=ref)
        result = process.run(context=RunContext(run_id=f"openexchangerates-live-{selector}"))
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
        **details,
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
    parser = argparse.ArgumentParser(description="Run OpenExchangeRates smoke checks against dpone runtime.")
    parser.add_argument("--project-root", default=".", help="Repository checkout root")
    parser.add_argument("--mode", choices=("connector", "manifest-run"), default="connector")
    parser.add_argument("--format", choices=("json", "text"), default="text")
    parser.add_argument("--resource", default=None)
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--vault-path", default=None)
    parser.add_argument("--selector", action="append", default=[])
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--day", default=None)
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--days-back", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--retry-delay", type=float, default=None)
    parser.add_argument("--rate-limit-delay", type=float, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    project_root = Path(args.project_root).resolve()
    settings = read_settings(args)

    payload: dict[str, Any] = {"settings": asdict(settings)}
    if args.mode == "connector":
        payload["result"] = run_connector_smoke(settings)
    else:
        if args.dry_run:
            manifest_path = _resolve_manifest_path(project_root, settings.manifest)
            temp_manifest, selectors, details = _patch_manifest_for_live_run(manifest_path, settings)
            payload["result"] = {
                "mode": "manifest-run",
                "manifest": str(manifest_path),
                "patched_manifest": str(temp_manifest),
                "selectors": list(selectors),
                **details,
            }
        else:
            payload["result"] = run_manifest_smoke(settings, project_root=project_root)

    print(_dump_payload(payload, fmt=args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
