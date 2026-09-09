#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MANIFEST = "examples/batch/landing_fasttrack_api.batch.yaml"
DEFAULT_SELECTOR = "default.flex_cms_ratings"
DEFAULT_RESOURCE = "flex_cms_ratings"
DEFAULT_VAULT_PATH = "api/fasttrack"
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 2
DEFAULT_RATE_LIMIT_DELAY = 0.2
DEFAULT_PAGE_SIZE = 1000


@dataclass(frozen=True)
class FasttrackSmokeSettings:
    vault_path: str
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    timeout: int
    max_retries: int
    rate_limit_delay: float
    dashboard_uuid: str | None
    category: str | None
    limit: int | None
    offset: int | None
    page_size: int | None


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


def read_settings(args: argparse.Namespace) -> FasttrackSmokeSettings:
    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_FT_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)
    resource = (
        args.resource or os.getenv("DPONE_IT_FT_RESOURCE") or _table_from_selector(selectors[0]) or DEFAULT_RESOURCE
    )
    return FasttrackSmokeSettings(
        vault_path=args.vault_path or os.getenv("DPONE_IT_FT_VAULT_PATH") or DEFAULT_VAULT_PATH,
        resource=resource,
        selectors=selectors,
        manifest=args.manifest or os.getenv("DPONE_IT_FT_MANIFEST") or DEFAULT_MANIFEST,
        timeout=int(
            args.timeout if args.timeout is not None else (os.getenv("DPONE_IT_FT_TIMEOUT") or DEFAULT_TIMEOUT)
        ),
        max_retries=int(
            args.max_retries
            if args.max_retries is not None
            else (os.getenv("DPONE_IT_FT_MAX_RETRIES") or DEFAULT_MAX_RETRIES)
        ),
        rate_limit_delay=float(
            args.rate_limit_delay
            if args.rate_limit_delay is not None
            else (os.getenv("DPONE_IT_FT_RATE_LIMIT_DELAY") or DEFAULT_RATE_LIMIT_DELAY)
        ),
        dashboard_uuid=args.dashboard_uuid or os.getenv("DPONE_IT_FT_DASHBOARD_UUID") or None,
        category=args.category or os.getenv("DPONE_IT_FT_CATEGORY") or None,
        limit=int(args.limit if args.limit is not None else os.getenv("DPONE_IT_FT_LIMIT"))
        if (args.limit is not None or os.getenv("DPONE_IT_FT_LIMIT"))
        else None,
        offset=int(args.offset if args.offset is not None else os.getenv("DPONE_IT_FT_OFFSET"))
        if (args.offset is not None or os.getenv("DPONE_IT_FT_OFFSET"))
        else None,
        page_size=int(args.page_size if args.page_size is not None else os.getenv("DPONE_IT_FT_PAGE_SIZE"))
        if (args.page_size is not None or os.getenv("DPONE_IT_FT_PAGE_SIZE"))
        else DEFAULT_PAGE_SIZE,
    )


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"Fasttrack manifest not found: {candidate}")


def _build_fetch_filters(settings: FasttrackSmokeSettings) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if settings.dashboard_uuid:
        filters["uuid"] = settings.dashboard_uuid
    if settings.category:
        filters["category"] = settings.category
    if settings.limit is not None:
        filters["limit"] = settings.limit
    if settings.offset is not None:
        filters["offset"] = settings.offset
    if settings.page_size is not None:
        filters["page_size"] = settings.page_size
    return filters


def run_connector_smoke(settings: FasttrackSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.fasttrack import FasttrackConnector

    connector = FasttrackConnector.from_vault(
        vault_path=settings.vault_path,
        timeout=settings.timeout,
        max_retries=settings.max_retries,
        rate_limit_delay=settings.rate_limit_delay,
    )
    filters = _build_fetch_filters(settings)
    rows = connector.fetch_resource_rows(settings.resource, **filters)
    payload: dict[str, Any] = {
        "mode": "connector",
        "resource": settings.resource,
        "filters": filters,
        "health_check": connector.health_check(),
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = rows[0]
    return payload


def _patch_manifest_for_live_run(
    source: Path, settings: FasttrackSmokeSettings
) -> tuple[Path, tuple[str, ...], dict[str, Any]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}
    filters = _build_fetch_filters(settings)

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    options = source_cfg.setdefault("options", {})
    options["resource"] = settings.resource
    options["timeout"] = settings.timeout
    options["max_retries"] = settings.max_retries
    options["rate_limit_delay"] = settings.rate_limit_delay
    for key in ("uuid", "category", "limit", "offset", "page_size"):
        options.pop(key, None)
    options.update(filters)

    schemas = data.get("schemas") or {}
    default_schema = schemas.get("default")
    if not isinstance(default_schema, dict):
        raise ValueError("Expected schema 'default' in Fasttrack batch manifest")
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
        raise ValueError(f"Selected Fasttrack selectors are not present in manifest: {sorted(missing)}")
    default_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-fasttrack-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors, filters


def run_manifest_smoke(settings: FasttrackSmokeSettings, *, project_root: Path) -> dict[str, Any]:
    from dpone.contracts.run_context import RunContext
    from dpone.dag.config import ETLProcessConfig
    from dpone.dag.loader import ConfigLoader
    from dpone.dag.process import ETLProcess

    manifest_path = _resolve_manifest_path(project_root, settings.manifest)
    temp_manifest, selectors, filters = _patch_manifest_for_live_run(manifest_path, settings)
    os.environ.setdefault("DPONE_PROJECT_DIR", str(project_root.resolve()))

    loader = ConfigLoader(base_path=manifest_path.parent)
    manifest = loader.get_manifest(temp_manifest, metadata_only=True)
    task_count = len(manifest.processes) if manifest else 0

    results: list[dict[str, Any]] = []
    for selector in selectors:
        ref = f"{temp_manifest}#{selector}"
        config = ETLProcessConfig.from_yaml(ref)
        process = ETLProcess(config=config, config_path=ref)
        result = process.run(context=RunContext(run_id=f"fasttrack-live-{selector}"))
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
        "filters": filters,
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
    parser = argparse.ArgumentParser(description="Run manual/live Fasttrack smoke checks against dpone runtime.")
    parser.add_argument("--project-root", default=".", help="Repository checkout root used to resolve manifests")
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument(
        "--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path used for manifest-run mode"
    )
    parser.add_argument(
        "--selector", action="append", default=[], help="Batch selector to run/filter, e.g. default.flex_cms_ratings"
    )
    parser.add_argument("--resource", default=None, help="Fasttrack resource used for connector or manifest mode")
    parser.add_argument("--vault-path", default=None, help="Vault path with Fasttrack credentials")
    parser.add_argument("--dashboard-uuid", default=None, help="Override dashboard report UUID")
    parser.add_argument("--category", default=None, help="Override flex category UUID")
    parser.add_argument("--limit", type=int, default=None, help="Optional request limit")
    parser.add_argument("--offset", type=int, default=None, help="Optional request offset")
    parser.add_argument("--page-size", type=int, default=None, help="Optional page_size for flex endpoint")
    parser.add_argument("--timeout", type=int, default=None, help="HTTP timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=None, help="HTTP retry count")
    parser.add_argument("--rate-limit-delay", type=float, default=None, help="Sleep between API requests in seconds")
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
