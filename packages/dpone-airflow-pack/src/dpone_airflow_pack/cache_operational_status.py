"""Bounded operational-status projection for no-kubectl cache diagnostics."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_status_files import CacheStatusFileTooLarge, read_bounded_regular_file

_MAX_OPERATIONAL_STATUS_BYTES = 8 * 1024 * 1024
_STATUS_FILES = {
    "retention_plan": (
        "last-retention-plan.json",
        "dpone.deployment-cache-retention-plan.v1",
    ),
    "retention_apply": (
        "last-retention-apply.json",
        "dpone.deployment-cache-retention-apply.v3",
    ),
    "retention_plan_publication": (
        "last-retention-plan-publication.json",
        "dpone.airflow-cache-status-publication.v1",
    ),
    "retention_apply_publication": (
        "last-retention-apply-publication.json",
        "dpone.airflow-cache-status-publication.v1",
    ),
    "retention_plan_publication_failure": (
        "last-retention-plan-publication-failure.json",
        "dpone.airflow-cache-status-publication-failure.v1",
    ),
    "retention_apply_publication_failure": (
        "last-retention-apply-publication-failure.json",
        "dpone.airflow-cache-status-publication-failure.v1",
    ),
}


def read_cache_operational_status(root: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    """Read known bounded status files and turn unavailable evidence into warnings."""

    result: dict[str, dict[str, Any]] = {}
    warnings: list[dict[str, str]] = []
    status_root = root / "status"
    for key, (name, expected_schema) in _STATUS_FILES.items():
        path = status_root / name
        try:
            payload = _read_status(path)
        except FileNotFoundError:
            continue
        except (CacheStatusFileTooLarge, OSError, UnicodeError, ValueError):
            warnings.append(_warning("airflow_cache_operational_status_invalid", path))
            continue
        if payload.get("schema") != expected_schema:
            warnings.append(_warning("airflow_cache_operational_status_schema_mismatch", path))
            continue
        result[key] = payload
        if key.endswith("_failure") or payload.get("status") in {"rejected", "commit_unknown"}:
            warnings.append(_warning("airflow_cache_status_publication_attention", path))
    return result, warnings


def _read_status(path: Path) -> dict[str, Any]:
    raw = read_bounded_regular_file(path, max_bytes=_MAX_OPERATIONAL_STATUS_BYTES)
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("operational status must be a JSON object")
    return {str(key): value for key, value in payload.items() if isinstance(key, str)}


def _warning(code: str, path: Path) -> dict[str, str]:
    return {
        "code": code,
        "path": str(path),
        "message": "Retention or publication evidence needs operator attention",
    }


__all__ = ["read_cache_operational_status"]
