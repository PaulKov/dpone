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

DEFAULT_MANIFEST = "examples/batch/landing_similarweb_api.batch.yaml"
DEFAULT_SELECTOR = "default.keywords"
DEFAULT_RESOURCE = "keywords"
DEFAULT_VAULT_PATH = "api/similarweb"
DEFAULT_DOMAINS = ("travel.example.com",)
DEFAULT_LIMIT = 5
DEFAULT_PAGE_SIZE = 5
DEFAULT_MIN_KEYWORDS_COUNT = 1
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 1
DEFAULT_RATE_LIMIT_DELAY = 1.0


@dataclass(frozen=True)
class SimilarwebSmokeSettings:
    vault_path: str
    domains: tuple[str, ...]
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    snapshot_month: str
    limit: int
    page_size: int
    min_keywords_count: int
    traffic_source: str
    web_source: str
    branded_type: str
    country: str
    timeout: int
    max_retries: int
    rate_limit_delay: float

    @property
    def default_domain(self) -> str:
        return self.domains[0]


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


def _resolve_snapshot_month(explicit: str | None) -> str:
    if explicit:
        raw = explicit.strip()
        if len(raw) == 7:
            return f"{raw}-01"
        return raw[:10]
    previous_month_last_day = date.today().replace(day=1) - timedelta(days=1)
    return previous_month_last_day.replace(day=1).isoformat()


def _month_bounds(snapshot_month: str) -> tuple[str, str]:
    first_day = date.fromisoformat(snapshot_month)
    if first_day.month == 12:
        next_month = date(first_day.year + 1, 1, 1)
    else:
        next_month = date(first_day.year, first_day.month + 1, 1)
    return first_day.isoformat(), (next_month - timedelta(days=1)).isoformat()


def read_settings(args: argparse.Namespace) -> SimilarwebSmokeSettings:
    domains = (
        tuple(args.domain) if args.domain else (_parse_csv_list(os.getenv("DPONE_IT_SW_DOMAINS")) or DEFAULT_DOMAINS)
    )

    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_SW_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)

    limit = int(args.limit if args.limit is not None else (os.getenv("DPONE_IT_SW_LIMIT") or DEFAULT_LIMIT))
    page_size = int(args.page_size if args.page_size is not None else (os.getenv("DPONE_IT_SW_PAGE_SIZE") or limit))
    snapshot_month = _resolve_snapshot_month(args.snapshot_month or os.getenv("DPONE_IT_SW_SNAPSHOT_MONTH"))

    return SimilarwebSmokeSettings(
        vault_path=args.vault_path or os.getenv("DPONE_IT_SW_VAULT_PATH") or DEFAULT_VAULT_PATH,
        domains=domains,
        resource=args.resource or os.getenv("DPONE_IT_SW_RESOURCE") or DEFAULT_RESOURCE,
        selectors=selectors,
        manifest=args.manifest or os.getenv("DPONE_IT_SW_MANIFEST") or DEFAULT_MANIFEST,
        snapshot_month=snapshot_month,
        limit=limit,
        page_size=page_size,
        min_keywords_count=int(
            args.min_keywords_count
            if args.min_keywords_count is not None
            else (os.getenv("DPONE_IT_SW_MIN_KEYWORDS_COUNT") or DEFAULT_MIN_KEYWORDS_COUNT)
        ),
        traffic_source=args.traffic_source or os.getenv("DPONE_IT_SW_TRAFFIC_SOURCE") or "Organic",
        web_source=args.web_source or os.getenv("DPONE_IT_SW_WEB_SOURCE") or "Total",
        branded_type=args.branded_type or os.getenv("DPONE_IT_SW_BRANDED_TYPE") or "All",
        country=args.country or os.getenv("DPONE_IT_SW_COUNTRY") or "world",
        timeout=int(
            args.timeout if args.timeout is not None else (os.getenv("DPONE_IT_SW_TIMEOUT") or DEFAULT_TIMEOUT)
        ),
        max_retries=int(
            args.max_retries
            if args.max_retries is not None
            else (os.getenv("DPONE_IT_SW_MAX_RETRIES") or DEFAULT_MAX_RETRIES)
        ),
        rate_limit_delay=float(
            args.rate_limit_delay
            if args.rate_limit_delay is not None
            else (os.getenv("DPONE_IT_SW_RATE_LIMIT_DELAY") or DEFAULT_RATE_LIMIT_DELAY)
        ),
    )


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"SimilarWeb manifest not found: {candidate}")


def run_connector_smoke(settings: SimilarwebSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.similarweb import SimilarwebConnector

    start_date, end_date = _month_bounds(settings.snapshot_month)
    connector = SimilarwebConnector.from_vault(
        vault_path=settings.vault_path,
        timeout=settings.timeout,
        max_retries=settings.max_retries,
        rate_limit_delay=settings.rate_limit_delay,
    )
    rows = connector.fetch_resource_rows(
        resource_name=settings.resource,
        url=settings.default_domain,
        start_date=start_date,
        end_date=end_date,
        limit=settings.limit,
        page_size=settings.page_size,
        traffic_source=settings.traffic_source,
        web_source=settings.web_source,
        branded_type=settings.branded_type,
        country=settings.country,
    )
    payload: dict[str, Any] = {
        "mode": "connector",
        "resource": settings.resource,
        "domain": settings.default_domain,
        "snapshot_month": settings.snapshot_month,
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = {key: rows[0][key] for key in ("keyword", "top_url", "serp_features") if key in rows[0]}
    return payload


def _patch_manifest_for_live_run(
    source: Path,
    settings: SimilarwebSmokeSettings,
) -> tuple[Path, tuple[str, ...], dict[str, Any]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    options = source_cfg.setdefault("options", {})
    options["resource"] = settings.resource
    options["domains"] = list(settings.domains)
    options["snapshot_month"] = settings.snapshot_month
    options["limit"] = settings.limit
    options["page_size"] = settings.page_size
    options["min_keywords_count"] = settings.min_keywords_count
    options["traffic_source"] = settings.traffic_source
    options["web_source"] = settings.web_source
    options["branded_type"] = settings.branded_type
    options["country"] = settings.country
    options["timeout"] = settings.timeout
    options["max_retries"] = settings.max_retries
    options["rate_limit_delay"] = settings.rate_limit_delay

    schemas = data.get("schemas") or {}
    default_schema = schemas.get("default")
    if not isinstance(default_schema, dict):
        raise ValueError("Expected schema 'default' in SimilarWeb batch manifest")
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
        raise ValueError(f"Selected SimilarWeb selectors are not present in manifest: {sorted(missing)}")
    default_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-similarweb-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)

    manifest_options = {
        "domains": list(settings.domains),
        "snapshot_month": settings.snapshot_month,
        "limit": settings.limit,
        "page_size": settings.page_size,
        "min_keywords_count": settings.min_keywords_count,
    }
    return temp_manifest, selectors, manifest_options


def run_manifest_smoke(settings: SimilarwebSmokeSettings, *, project_root: Path) -> dict[str, Any]:
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
        result = process.run(context=RunContext(run_id=f"similarweb-live-{selector}"))
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
    parser = argparse.ArgumentParser(description="Run manual/live SimilarWeb smoke checks against dpone runtime.")
    parser.add_argument("--project-root", default=".", help="Repository checkout root used to resolve manifests")
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument(
        "--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path used for manifest-run mode"
    )
    parser.add_argument("--resource", default=None, help="SimilarWeb resource used for connector mode")
    parser.add_argument("--selector", action="append", default=[], help="Batch selector to run/filter")
    parser.add_argument("--domain", action="append", default=[], help="Repeatable SimilarWeb domain")
    parser.add_argument("--vault-path", default=None, help="Vault path for SimilarWeb credentials")
    parser.add_argument("--snapshot-month", default=None, help="Explicit YYYY-MM or YYYY-MM-DD snapshot month")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--page-size", type=int, default=None)
    parser.add_argument("--min-keywords-count", type=int, default=None)
    parser.add_argument("--traffic-source", default=None)
    parser.add_argument("--web-source", default=None)
    parser.add_argument("--branded-type", default=None)
    parser.add_argument("--country", default=None)
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--rate-limit-delay", type=float, default=None)
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
