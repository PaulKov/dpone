"""Fail-visible diagnostics for an invalid Airflow cache layout."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_layout import CacheLayoutError


def invalid_layout_status(
    root: Path,
    *,
    workload_ids: tuple[str, ...],
    error: CacheLayoutError,
) -> dict[str, Any]:
    """Return one blocked status without interpreting a conflicting cache tree."""

    return {
        "kind": "dpone.airflow_pack_cache_status",
        "schema_version": "1",
        "status": "blocked",
        "layout": "invalid",
        "cache_dir": str(root),
        "current_generation": None,
        "release_id": None,
        "deployment_id": None,
        "generations": [],
        "index_sha256": None,
        "last_sync_status": {},
        "workloads": {workload_id: _missing_workload(workload_id) for workload_id in workload_ids},
        "warnings": [],
        "blockers": [
            {
                "code": "airflow_pack_cache_layout_invalid",
                "path": error.path,
                "message": str(error),
            }
        ],
    }


def _missing_workload(workload_id: str) -> dict[str, Any]:
    return {
        "exists": False,
        "workload_id": workload_id,
        "path": None,
        "blockers": [
            {
                "code": "airflow_pack_cache_missing",
                "path": workload_id,
                "message": "Cached workload pack is unavailable because the cache layout is invalid",
            }
        ],
    }


__all__ = ["invalid_layout_status"]
