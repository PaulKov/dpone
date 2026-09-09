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

DEFAULT_MANIFEST = "examples/batch/landing_google_ads_api.batch.yaml"
DEFAULT_SELECTOR = "app.ads_stats"
DEFAULT_RESOURCE = "ads_stats"
DEFAULT_VAULT_PATH = "api/google_ads"
DEFAULT_DAYS_BACK = 3
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 1
DEFAULT_RATE_LIMIT_DELAY = 1.0


@dataclass(frozen=True)
class GoogleAdsSmokeSettings:
    vault_path: str
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    customer_ids: tuple[str, ...]
    date_from: str
    date_to: str
    days_back: int
    timeout: int
    max_retries: int
    rate_limit_delay: float


def _parse_csv_list(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _parse_customer_ids(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip().replace("-", "") for item in value.split(",") if item.strip())


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


def _resolve_window(date_from: str | None, date_to: str | None, days_back: int) -> tuple[str, str]:
    if date_from and date_to:
        return date_from, date_to
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(days_back - 1, 0))
    return start_date.isoformat(), end_date.isoformat()


def read_settings(args: argparse.Namespace) -> GoogleAdsSmokeSettings:
    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_GA_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)

    customer_ids = (
        tuple(item.strip().replace("-", "") for item in args.customer_id if item.strip())
        if args.customer_id
        else _parse_customer_ids(os.getenv("DPONE_IT_GA_CUSTOMER_IDS"))
    )
    date_from, date_to = _resolve_window(
        args.date_from or os.getenv("DPONE_IT_GA_DATE_FROM"),
        args.date_to or os.getenv("DPONE_IT_GA_DATE_TO"),
        int(
            args.days_back if args.days_back is not None else (os.getenv("DPONE_IT_GA_DAYS_BACK") or DEFAULT_DAYS_BACK)
        ),
    )

    return GoogleAdsSmokeSettings(
        vault_path=args.vault_path or os.getenv("DPONE_IT_GA_VAULT_PATH") or DEFAULT_VAULT_PATH,
        resource=args.resource or os.getenv("DPONE_IT_GA_RESOURCE") or DEFAULT_RESOURCE,
        selectors=selectors,
        manifest=args.manifest or os.getenv("DPONE_IT_GA_MANIFEST") or DEFAULT_MANIFEST,
        customer_ids=customer_ids,
        date_from=date_from,
        date_to=date_to,
        days_back=int(
            args.days_back if args.days_back is not None else (os.getenv("DPONE_IT_GA_DAYS_BACK") or DEFAULT_DAYS_BACK)
        ),
        timeout=int(
            args.timeout if args.timeout is not None else (os.getenv("DPONE_IT_GA_TIMEOUT") or DEFAULT_TIMEOUT)
        ),
        max_retries=int(
            args.max_retries
            if args.max_retries is not None
            else (os.getenv("DPONE_IT_GA_MAX_RETRIES") or DEFAULT_MAX_RETRIES)
        ),
        rate_limit_delay=float(
            args.rate_limit_delay
            if args.rate_limit_delay is not None
            else (os.getenv("DPONE_IT_GA_RATE_LIMIT_DELAY") or DEFAULT_RATE_LIMIT_DELAY)
        ),
    )


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"Google Ads manifest not found: {candidate}")


def _build_load_config(settings: GoogleAdsSmokeSettings):
    from dpone.config import LoadConfig, LoadStrategy

    options: dict[str, Any] = {
        "resource": settings.resource,
        "start_date": settings.date_from,
        "end_date": settings.date_to,
    }
    if settings.customer_ids:
        options["customer_ids"] = list(settings.customer_ids)

    return LoadConfig(
        source_conn_id="api__google_ads",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table=settings.resource,
        target_schema="landing__google_ads__api",
        target_table=f"app__{settings.resource}",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options,
    )


def run_connector_smoke(settings: GoogleAdsSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.google_ads import GoogleAdsConnector
    from dpone.runtime.sources.api.google_ads import GoogleAdsSource

    class _Logger:
        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

        def log_etl_progress(self, *args, **kwargs):
            return None

    connector = GoogleAdsConnector.from_vault(
        vault_path=settings.vault_path,
        timeout=settings.timeout,
        max_retries=settings.max_retries,
        rate_limit_delay=settings.rate_limit_delay,
    )
    source = GoogleAdsSource(connector=connector, sink_connector=None, logger=_Logger())
    result = source.extract(_build_load_config(settings), None)
    rows = result.artifact._rows
    effective_customer_ids = settings.customer_ids or tuple(connector.credentials.customer_ids)

    payload: dict[str, Any] = {
        "mode": "connector",
        "resource": settings.resource,
        "auth_type": connector.credentials.auth_type,
        "customer_ids": list(effective_customer_ids),
        "date_from": settings.date_from,
        "date_to": settings.date_to,
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = {
            key: rows[0][key]
            for key in ("date", "login", "campaign_id", "term", "impressions", "clicks", "cost", "report")
            if key in rows[0]
        }
    return payload


def _patch_manifest_for_live_run(source: Path, settings: GoogleAdsSmokeSettings) -> tuple[Path, tuple[str, ...]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    source_cfg["vault_path"] = settings.vault_path
    options = source_cfg.setdefault("options", {})
    options["resource"] = settings.resource
    options["start_date"] = settings.date_from
    options["end_date"] = settings.date_to
    options["lookback_days"] = settings.days_back
    options["timeout"] = settings.timeout
    options["max_retries"] = settings.max_retries
    options["rate_limit_delay"] = settings.rate_limit_delay
    if settings.customer_ids:
        options["customer_ids"] = list(settings.customer_ids)
    else:
        options.pop("customer_ids", None)

    schemas = data.get("schemas") or {}
    app_schema = schemas.get("app")
    if not isinstance(app_schema, dict):
        raise ValueError("Expected schema 'app' in Google Ads batch manifest")
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
        raise ValueError(f"Selected Google Ads selectors are not present in manifest: {sorted(missing)}")
    app_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-google-ads-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors


def run_manifest_smoke(settings: GoogleAdsSmokeSettings, *, project_root: Path) -> dict[str, Any]:
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
        result = process.run(context=RunContext(run_id=f"google-ads-live-{selector}"))
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
        "customer_ids": list(settings.customer_ids),
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
    parser = argparse.ArgumentParser(description="Run manual/live Google Ads smoke checks against dpone runtime.")
    parser.add_argument("--project-root", default=".", help="Repository checkout root used to resolve manifests")
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path for manifest-run")
    parser.add_argument("--resource", default=None)
    parser.add_argument("--selector", action="append", default=[], help="Batch selector to run/filter")
    parser.add_argument("--customer-id", action="append", default=[], help="Repeatable Google Ads customer id")
    parser.add_argument("--vault-path", default=None, help="Vault path for Google Ads credentials")
    parser.add_argument("--date-from", default=None, help="Explicit YYYY-MM-DD start date")
    parser.add_argument("--date-to", default=None, help="Explicit YYYY-MM-DD end date")
    parser.add_argument("--days-back", type=int, default=None, help="Used when date range is not specified")
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--rate-limit-delay", type=float, default=None)
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
