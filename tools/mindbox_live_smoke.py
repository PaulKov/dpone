#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import yaml

from dpone._compat import UTC

DEFAULT_MANIFEST = "examples/batch/landing_mindbox_api.batch.yaml"
DEFAULT_SELECTOR = "app.getactions"
DEFAULT_RESOURCE = "getactions"
DEFAULT_VAULT_PATH = "api/mindbox"
DEFAULT_UTC_BOUNDARY_TIME = "21:00:00"
DEFAULT_POLL_INTERVAL = 30
DEFAULT_EXPORT_TIMEOUT = 3000
DEFAULT_BATCH_SIZE = 1000


@dataclass(frozen=True)
class MindboxSmokeSettings:
    vault_path: str
    resource: str
    selectors: tuple[str, ...]
    manifest: str
    since_datetime_utc: str | None
    till_datetime_utc: str | None
    date_from: str | None
    date_to: str | None
    days_back: int
    poll_interval: int
    export_timeout: int
    batch_size: int
    utc_boundary_time: str


def _parse_csv_list(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _parse_datetime_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed.replace(tzinfo=None)


def _render_window_value(value: date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value.isoformat()


def _default_mindbox_hour_window_utc() -> tuple[str, str]:
    yesterday_utc = datetime.now(UTC).date() - timedelta(days=1)
    since = datetime.combine(yesterday_utc, time(hour=12), tzinfo=UTC)
    till = since + timedelta(hours=1)
    return since.strftime("%Y-%m-%dT%H:%M:%SZ"), till.strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_window(
    since_datetime_utc: str | None,
    till_datetime_utc: str | None,
    date_from: str | None,
    date_to: str | None,
    days_back: int,
) -> tuple[date | datetime | None, date | datetime | None]:
    since_dt = _parse_datetime_utc(since_datetime_utc)
    till_dt = _parse_datetime_utc(till_datetime_utc)
    if since_dt is not None or till_dt is not None:
        return since_dt, till_dt
    if date_from and date_to:
        return date.fromisoformat(date_from), date.fromisoformat(date_to)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(days_back - 1, 0))
    return start_date, end_date


def _resolve_days_back(
    since_datetime_utc: str | None,
    till_datetime_utc: str | None,
    date_from: str | None,
    date_to: str | None,
    days_back: int,
) -> int:
    since_dt = _parse_datetime_utc(since_datetime_utc)
    till_dt = _parse_datetime_utc(till_datetime_utc)
    if since_dt and till_dt:
        return max((till_dt.date() - since_dt.date()).days + 1, 1)
    if date_from and date_to:
        start_date = date.fromisoformat(date_from)
        end_date = date.fromisoformat(date_to)
        return max((end_date - start_date).days + 1, 1)
    return max(days_back, 1)


def read_settings(args: argparse.Namespace) -> MindboxSmokeSettings:
    selectors = tuple(args.selector) if args.selector else _parse_csv_list(os.getenv("DPONE_IT_MB_SELECTORS"))
    if not selectors:
        selectors = (DEFAULT_SELECTOR,)
    resource = (
        args.resource or os.getenv("DPONE_IT_MB_RESOURCE") or _table_from_selector(selectors[0]) or DEFAULT_RESOURCE
    )
    since_datetime_utc = args.since_datetime_utc or os.getenv("DPONE_IT_MB_SINCE_DATETIME_UTC") or None
    till_datetime_utc = args.till_datetime_utc or os.getenv("DPONE_IT_MB_TILL_DATETIME_UTC") or None
    date_from = args.date_from or os.getenv("DPONE_IT_MB_DATE_FROM") or None
    date_to = args.date_to or os.getenv("DPONE_IT_MB_DATE_TO") or None
    days_back_raw = args.days_back if args.days_back is not None else (os.getenv("DPONE_IT_MB_DAYS_BACK") or "")
    if (
        not since_datetime_utc
        and not till_datetime_utc
        and not date_from
        and not date_to
        and str(days_back_raw).strip() == ""
    ):
        since_datetime_utc, till_datetime_utc = _default_mindbox_hour_window_utc()
    return MindboxSmokeSettings(
        vault_path=args.vault_path or os.getenv("DPONE_IT_MB_VAULT_PATH") or DEFAULT_VAULT_PATH,
        resource=resource,
        selectors=selectors,
        manifest=args.manifest or os.getenv("DPONE_IT_MB_MANIFEST") or DEFAULT_MANIFEST,
        since_datetime_utc=since_datetime_utc,
        till_datetime_utc=till_datetime_utc,
        date_from=date_from,
        date_to=date_to,
        days_back=int(days_back_raw or 1),
        poll_interval=int(
            args.poll_interval
            if args.poll_interval is not None
            else (os.getenv("DPONE_IT_MB_POLL_INTERVAL") or DEFAULT_POLL_INTERVAL)
        ),
        export_timeout=int(
            args.export_timeout
            if args.export_timeout is not None
            else (os.getenv("DPONE_IT_MB_EXPORT_TIMEOUT") or DEFAULT_EXPORT_TIMEOUT)
        ),
        batch_size=int(
            args.batch_size
            if args.batch_size is not None
            else (os.getenv("DPONE_IT_MB_BATCH_SIZE") or DEFAULT_BATCH_SIZE)
        ),
        utc_boundary_time=args.utc_boundary_time
        or os.getenv("DPONE_IT_MB_UTC_BOUNDARY_TIME")
        or DEFAULT_UTC_BOUNDARY_TIME,
    )


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


def _resolve_manifest_path(project_root: Path, manifest: str) -> Path:
    candidate = (project_root / manifest).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"Mindbox manifest not found: {candidate}")


def run_connector_smoke(settings: MindboxSmokeSettings) -> dict[str, Any]:
    from dpone.runtime.connectors.api.mindbox import MindboxConnector

    connector = MindboxConnector.from_vault(
        vault_path=settings.vault_path,
        timeout=120,
        max_retries=1,
    )
    health = connector.health_check()
    since, till = _resolve_window(
        settings.since_datetime_utc,
        settings.till_datetime_utc,
        settings.date_from,
        settings.date_to,
        settings.days_back,
    )
    rows_iter = connector.get_resources(
        settings.resource,
        filters={
            "since": since,
            "till": till,
            "poll_interval": settings.poll_interval,
            "export_timeout": settings.export_timeout,
            "utc_boundary_time": settings.utc_boundary_time,
        },
    )
    rows = list(itertools.islice(rows_iter, 25))
    close = getattr(rows_iter, "close", None)
    if callable(close):
        close()
    payload: dict[str, Any] = {
        "mode": "connector",
        "health_check": health,
        "resource": settings.resource,
        "since": _render_window_value(since),
        "till": _render_window_value(till),
        "row_sample_count": len(rows),
        "sample_keys": sorted(rows[0].keys()) if rows else [],
    }
    if rows:
        payload["sample"] = rows[0]
    return payload


def _resolve_entry_mode(entry: Any, default_mode: str) -> str:
    if isinstance(entry, dict):
        overrides = entry.get("overrides") or {}
        sink = overrides.get("sink") or {}
        strategy = sink.get("strategy") or {}
        if strategy.get("mode"):
            return str(strategy["mode"])
    return default_mode


def _patch_manifest_for_live_run(source: Path, settings: MindboxSmokeSettings) -> tuple[Path, tuple[str, ...], int]:
    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    selectors = tuple(_normalize_selector(item) for item in settings.selectors)
    selected_tables = {_table_from_selector(item) for item in selectors}
    effective_days_back = _resolve_days_back(
        settings.since_datetime_utc,
        settings.till_datetime_utc,
        settings.date_from,
        settings.date_to,
        settings.days_back,
    )

    defaults = data.setdefault("defaults", {})
    source_cfg = defaults.setdefault("source", {})
    options = source_cfg.setdefault("options", {})
    options["poll_interval"] = settings.poll_interval
    options["export_timeout"] = settings.export_timeout
    options["batch_size"] = settings.batch_size
    options["utc_boundary_time"] = settings.utc_boundary_time

    default_mode = str(((defaults.get("sink") or {}).get("strategy") or {}).get("mode") or "incremental_merge")
    schemas = data.get("schemas") or {}
    app_schema = schemas.get("app")
    if not isinstance(app_schema, dict):
        raise ValueError("Expected schema 'app' in Mindbox batch manifest")

    tables = app_schema.get("tables") or []
    filtered_tables: list[Any] = []
    present_tables: set[str] = set()
    for entry in tables:
        if isinstance(entry, str):
            table_name = entry
            entry_obj: Any = entry
        elif isinstance(entry, dict):
            table_name = str(entry.get("table") or "")
            entry_obj = dict(entry)
        else:
            continue
        if table_name not in selected_tables:
            continue
        present_tables.add(table_name)
        if isinstance(entry_obj, dict):
            entry_copy = dict(entry_obj)
            overrides = dict(entry_copy.get("overrides") or {})
            entry_source = dict(overrides.get("source") or {})
            entry_options = dict(entry_source.get("options") or {})
            mode = _resolve_entry_mode(entry_obj, default_mode)
            if settings.since_datetime_utc or settings.till_datetime_utc:
                entry_options.pop("lookback_days", None)
                entry_options.pop("full_refresh_days", None)
                if settings.since_datetime_utc:
                    entry_options["since_datetime_utc"] = settings.since_datetime_utc
                if settings.till_datetime_utc:
                    entry_options["till_datetime_utc"] = settings.till_datetime_utc
            else:
                entry_options.pop("since_datetime_utc", None)
                entry_options.pop("till_datetime_utc", None)
                if mode == "full_refresh":
                    entry_options["full_refresh_days"] = effective_days_back
                else:
                    entry_options["lookback_days"] = effective_days_back
            entry_source["options"] = entry_options
            overrides["source"] = entry_source
            entry_copy["overrides"] = overrides
            filtered_tables.append(entry_copy)
        else:
            filtered_tables.append(entry_obj)
    missing = selected_tables - present_tables
    if missing:
        raise ValueError(f"Selected Mindbox selectors are not present in manifest: {sorted(missing)}")
    app_schema["tables"] = filtered_tables

    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-mindbox-live-"))
    temp_manifest = temp_dir / source.name
    with temp_manifest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)
    return temp_manifest, selectors, effective_days_back


def run_manifest_smoke(settings: MindboxSmokeSettings, *, project_root: Path) -> dict[str, Any]:
    from dpone.contracts.run_context import RunContext
    from dpone.dag.config import ETLProcessConfig
    from dpone.dag.loader import ConfigLoader
    from dpone.dag.process import ETLProcess

    manifest_path = _resolve_manifest_path(project_root, settings.manifest)
    temp_manifest, selectors, effective_days_back = _patch_manifest_for_live_run(manifest_path, settings)
    os.environ.setdefault("DPONE_PROJECT_DIR", str(project_root.resolve()))

    loader = ConfigLoader(base_path=manifest_path.parent)
    manifest = loader.get_manifest(temp_manifest, metadata_only=True)
    task_count = len(manifest.processes) if manifest else 0

    results: list[dict[str, Any]] = []
    for selector in selectors:
        ref = f"{temp_manifest}#{selector}"
        config = ETLProcessConfig.from_yaml(ref)
        process = ETLProcess(config=config, config_path=ref)
        result = process.run(context=RunContext(run_id=f"mindbox-live-{selector}"))
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
        "days_back": effective_days_back,
        "vault_path": settings.vault_path,
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
    parser = argparse.ArgumentParser(description="Run live/manual Mindbox smoke checks against dpone runtime.")
    parser.add_argument(
        "--project-root", default=".", help="Repository checkout root used to resolve example manifests"
    )
    parser.add_argument("--mode", choices=["connector", "manifest-run"], default="connector")
    parser.add_argument(
        "--manifest", default=DEFAULT_MANIFEST, help="Relative batch manifest path used for manifest-run mode"
    )
    parser.add_argument(
        "--selector", action="append", default=[], help="Batch selector to run/filter, e.g. app.getactions"
    )
    parser.add_argument("--resource", default=None, help="Mindbox resource used for connector mode")
    parser.add_argument("--vault-path", default=None, help="Vault path with Mindbox credentials")
    parser.add_argument(
        "--date-from",
        default=None,
        help="Explicit lower bound date (YYYY-MM-DD) for connector mode or days-back derivation",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        help="Explicit upper bound date (YYYY-MM-DD) for connector mode or days-back derivation",
    )
    parser.add_argument(
        "--since-datetime-utc",
        default=None,
        help="Explicit lower UTC datetime bound for Mindbox exports, e.g. 2026-03-30T09:00:00Z",
    )
    parser.add_argument(
        "--till-datetime-utc",
        default=None,
        help="Explicit upper UTC datetime bound for Mindbox exports, e.g. 2026-03-30T10:00:00Z",
    )
    parser.add_argument(
        "--days-back", type=int, default=None, help="Fallback rolling window when explicit dates are not provided"
    )
    parser.add_argument("--poll-interval", type=int, default=None, help="Mindbox export poll interval in seconds")
    parser.add_argument("--export-timeout", type=int, default=None, help="Mindbox export timeout in seconds")
    parser.add_argument("--batch-size", type=int, default=None, help="Target artifact batch size for manifest-run")
    parser.add_argument("--utc-boundary-time", default=None, help="Mindbox UTC boundary time, e.g. 21:00:00")
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
