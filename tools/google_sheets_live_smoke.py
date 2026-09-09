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

DEFAULT_MANIFEST = "examples/batch/landing_google_sheets_api.batch.yaml"
DEFAULT_SELECTOR = "app.worksheet_rows"
DEFAULT_RESOURCE = "worksheet_rows"
DEFAULT_VAULT_PATH = "api/google_sheets"
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 1
DEFAULT_RATE_LIMIT_DELAY = 0.2


@dataclass(frozen=True)
class GoogleSheetsSmokeSettings:
    vault_path: str
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    spreadsheet_id: str | None
    spreadsheet_url: str | None
    worksheet_title: str | None
    worksheet_index: int | None
    range_name: str | None
    header_row: int
    skip_rows: int
    add_metadata_columns: bool
    timeout: int
    max_retries: int
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


def read_settings(args: argparse.Namespace) -> GoogleSheetsSmokeSettings:
    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_GS_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)

    spreadsheet_id = args.spreadsheet_id or os.getenv("DPONE_IT_GS_SPREADSHEET_ID") or None
    spreadsheet_url = args.spreadsheet_url or os.getenv("DPONE_IT_GS_SPREADSHEET_URL") or None
    if not spreadsheet_id and not spreadsheet_url:
        raise SystemExit("Google Sheets live smoke requires DPONE_IT_GS_SPREADSHEET_ID or DPONE_IT_GS_SPREADSHEET_URL")

    worksheet_index_raw = (
        str(args.worksheet_index)
        if args.worksheet_index is not None
        else (os.getenv("DPONE_IT_GS_WORKSHEET_INDEX") or "")
    ).strip()

    return GoogleSheetsSmokeSettings(
        vault_path=args.vault_path or os.getenv("DPONE_IT_GS_VAULT_PATH") or DEFAULT_VAULT_PATH,
        resource=args.resource or os.getenv("DPONE_IT_GS_RESOURCE") or DEFAULT_RESOURCE,
        selectors=selectors,
        manifest=args.manifest or os.getenv("DPONE_IT_GS_MANIFEST") or DEFAULT_MANIFEST,
        spreadsheet_id=spreadsheet_id,
        spreadsheet_url=spreadsheet_url,
        worksheet_title=args.worksheet_title or os.getenv("DPONE_IT_GS_WORKSHEET_TITLE") or None,
        worksheet_index=int(worksheet_index_raw) if worksheet_index_raw else None,
        range_name=args.range_name or os.getenv("DPONE_IT_GS_RANGE_NAME") or None,
        header_row=int(args.header_row if args.header_row is not None else (os.getenv("DPONE_IT_GS_HEADER_ROW") or 1)),
        skip_rows=int(args.skip_rows if args.skip_rows is not None else (os.getenv("DPONE_IT_GS_SKIP_ROWS") or 0)),
        add_metadata_columns=str(
            args.add_metadata_columns
            if args.add_metadata_columns is not None
            else (os.getenv("DPONE_IT_GS_ADD_METADATA_COLUMNS") or "1")
        )
        .strip()
        .lower()
        in {"1", "true", "yes", "on"},
        timeout=int(
            args.timeout if args.timeout is not None else (os.getenv("DPONE_IT_GS_TIMEOUT") or DEFAULT_TIMEOUT)
        ),
        max_retries=int(
            args.max_retries
            if args.max_retries is not None
            else (os.getenv("DPONE_IT_GS_MAX_RETRIES") or DEFAULT_MAX_RETRIES)
        ),
        rate_limit_delay=float(
            args.rate_limit_delay
            if args.rate_limit_delay is not None
            else (os.getenv("DPONE_IT_GS_RATE_LIMIT_DELAY") or DEFAULT_RATE_LIMIT_DELAY)
        ),
    )


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"Google Sheets manifest not found: {candidate}")


def run_connector_smoke(settings: GoogleSheetsSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.google_sheets import GoogleSheetsConnector

    connector = GoogleSheetsConnector.from_vault(
        vault_path=settings.vault_path,
        timeout=settings.timeout,
        max_retries=settings.max_retries,
        rate_limit_delay=settings.rate_limit_delay,
    )
    rows = connector.get_records(
        spreadsheet_id=settings.spreadsheet_id,
        spreadsheet_url=settings.spreadsheet_url,
        worksheet_title=settings.worksheet_title,
        worksheet_index=settings.worksheet_index,
        range_name=settings.range_name,
        header_row=settings.header_row,
        skip_rows=settings.skip_rows,
        add_metadata_columns=settings.add_metadata_columns,
    )
    payload: dict[str, Any] = {
        "mode": "connector",
        "resource": settings.resource,
        "auth_type": connector.credentials.auth_type,
        "row_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = rows[0]
    return payload


def _patch_manifest_for_live_run(source: Path, settings: GoogleSheetsSmokeSettings) -> tuple[Path, tuple[str, ...]]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    source_cfg["vault_path"] = settings.vault_path
    options = source_cfg.setdefault("options", {})
    options["resource"] = settings.resource
    options["header_row"] = settings.header_row
    options["skip_rows"] = settings.skip_rows
    options["add_metadata_columns"] = settings.add_metadata_columns
    options["timeout"] = settings.timeout
    options["max_retries"] = settings.max_retries
    options["rate_limit_delay"] = settings.rate_limit_delay

    if settings.spreadsheet_id:
        options["spreadsheet_id"] = settings.spreadsheet_id
        options.pop("spreadsheet_url", None)
    else:
        options["spreadsheet_url"] = settings.spreadsheet_url
        options.pop("spreadsheet_id", None)

    if settings.worksheet_title:
        options["worksheet_title"] = settings.worksheet_title
        options.pop("worksheet_index", None)
    elif settings.worksheet_index is not None:
        options["worksheet_index"] = settings.worksheet_index
        options.pop("worksheet_title", None)
    else:
        options.pop("worksheet_title", None)
        options.pop("worksheet_index", None)

    if settings.range_name:
        options["range_name"] = settings.range_name
    else:
        options.pop("range_name", None)

    schemas = data.get("schemas") or {}
    app_schema = schemas.get("app")
    if not isinstance(app_schema, dict):
        raise ValueError("Expected schema 'app' in Google Sheets batch manifest")
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
        raise ValueError(f"Selected Google Sheets selectors are not present in manifest: {sorted(missing)}")
    app_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-google-sheets-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors


def run_manifest_smoke(settings: GoogleSheetsSmokeSettings, *, project_root: Path) -> dict[str, Any]:
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
        result = process.run(context=RunContext(run_id=f"google-sheets-live-{selector}"))
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
    parser = argparse.ArgumentParser(description="Run manual/live Google Sheets smoke checks against dpone runtime.")
    parser.add_argument("--project-root", default=".", help="Repository checkout root used to resolve manifests")
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path for manifest-run")
    parser.add_argument("--resource", default=None)
    parser.add_argument("--selector", action="append", default=[], help="Batch selector to run/filter")
    parser.add_argument("--vault-path", default=None, help="Vault path for Google Sheets credentials")
    parser.add_argument("--spreadsheet-id", default=None)
    parser.add_argument("--spreadsheet-url", default=None)
    parser.add_argument("--worksheet-title", default=None)
    parser.add_argument("--worksheet-index", type=int, default=None)
    parser.add_argument("--range-name", default=None)
    parser.add_argument("--header-row", type=int, default=None)
    parser.add_argument("--skip-rows", type=int, default=None)
    parser.add_argument("--add-metadata-columns", default=None)
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
