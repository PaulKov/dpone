"""Read-only diagnostics for the local dpone Airflow pack cache."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_activation_contract import cache_read_lease
from dpone_airflow_pack.cache_authority import read_legacy_cache_authority
from dpone_airflow_pack.cache_layout import (
    EXACT_DEPLOYMENT_LAYOUT,
    LAYOUT_MARKER_NAME,
    LEGACY_PACK_INDEX_LAYOUT,
    CacheLayoutError,
    read_cache_layout,
)
from dpone_airflow_pack.cache_operational_status import read_cache_operational_status
from dpone_airflow_pack.cache_reconcile_status import DEFAULT_MAX_RECONCILE_AGE_SECONDS
from dpone_airflow_pack.cache_status_exact import exact_deployment_index_path, read_exact_cache_status
from dpone_airflow_pack.cache_status_files import (
    CacheStatusFileTooLarge,
    read_bounded_regular_file,
    read_cache_status_current,
)
from dpone_airflow_pack.cache_status_layout import invalid_layout_status
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.pack_index import find_index_entry, index_entries, safe_pack_relative_path

CANONICAL_CACHE_DIR = "/opt/airflow/.dpone-cache"
# Preserve the historical no-argument behavior for one compatibility window.
# Canonical deployments pass the root explicitly or through the environment.
DEFAULT_CACHE_DIR = "/opt/airflow/dags/.dpone-cache/airflow"
DEFAULT_MAX_CACHE_JSON_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_CACHED_PACK_BYTES = 64 * 1024 * 1024


def airflow_pack_cache_dir(cache_dir: str | Path | None = None) -> Path:
    """Resolve the scheduler-side cache directory."""

    return Path(cache_dir or os.environ.get("DPONE_AIRFLOW_PACK_CACHE_DIR") or DEFAULT_CACHE_DIR)


def read_airflow_pack_cache_status(
    cache_dir: str | Path | None = None,
    workload_ids: Iterable[str] = (),
    *,
    max_index_bytes: int = DEFAULT_MAX_CACHE_JSON_BYTES,
    max_pack_bytes: int = DEFAULT_MAX_CACHED_PACK_BYTES,
    max_reconcile_age_seconds: int = DEFAULT_MAX_RECONCILE_AGE_SECONDS,
) -> dict[str, Any]:
    """Return a non-mutating cache freshness snapshot.

    Supports both layouts under ``cache_dir``:

    - exact desired-state cache: ``current/airflow-index.json``
    - legacy pack sync cache: ``generations/<git_sha>/pack-index.json``
    """

    root = airflow_pack_cache_dir(cache_dir)
    requested = tuple(workload_ids)
    try:
        with cache_read_lease(root) as lease:
            if not lease.root_available:
                return frozen_missing_cache_status(root, workload_ids=requested)
            return _read_airflow_pack_cache_status_unleased(
                root,
                workload_ids=requested,
                max_index_bytes=max_index_bytes,
                max_pack_bytes=max_pack_bytes,
                max_reconcile_age_seconds=max_reconcile_age_seconds,
            )
    except (AirflowDeploymentIndexError, OSError, ValueError) as exc:
        return frozen_cache_read_failure_status(root, workload_ids=requested, error=exc)


def frozen_missing_cache_status(root: Path, *, workload_ids: tuple[str, ...]) -> dict[str, Any]:
    """Return one absence snapshot without re-reading a concurrently created root."""

    blocker = _blocker("airflow_pack_cache_missing", str(root), "Cache root was absent at lease acquisition")
    return {
        "kind": "dpone.airflow_pack_cache_status",
        "schema_version": "1",
        "status": "blocked",
        "layout": "missing",
        "cache_dir": str(root),
        "current_generation": None,
        "release_id": None,
        "deployment_id": None,
        "generations": [],
        "index_sha256": None,
        "last_sync_status": {},
        "workloads": {workload_id: _missing_workload_status(workload_id, None) for workload_id in workload_ids},
        "warnings": [],
        "blockers": [blocker],
    }


def frozen_cache_read_failure_status(
    root: Path,
    *,
    workload_ids: tuple[str, ...],
    error: Exception,
) -> dict[str, Any]:
    """Return machine-readable evidence when a read lease cannot be acquired."""

    status = frozen_missing_cache_status(root, workload_ids=workload_ids)
    status["layout"] = "unavailable"
    code = getattr(error, "code", "DPONE_CACHE_STATUS_READ_FAILED")
    path = getattr(error, "path", None) or str(root)
    status["blockers"] = [_blocker(str(code), str(path), str(error))]
    return status


def _read_airflow_pack_cache_status_unleased(
    root: Path,
    *,
    workload_ids: tuple[str, ...],
    max_index_bytes: int,
    max_pack_bytes: int,
    max_reconcile_age_seconds: int = DEFAULT_MAX_RECONCILE_AGE_SECONDS,
) -> dict[str, Any]:
    """Read one snapshot while the caller holds the cache read lease."""

    try:
        layout = read_cache_layout(root)
    except CacheLayoutError as exc:
        return invalid_layout_status(root, workload_ids=workload_ids, error=exc)
    exact_index = exact_deployment_index_path(root)
    if layout == EXACT_DEPLOYMENT_LAYOUT or exact_index is not None:
        return read_exact_cache_status(
            root,
            exact_index or root / "current" / "airflow-index.json",
            workload_ids=workload_ids,
            max_index_bytes=max_index_bytes,
            max_pack_bytes=max_pack_bytes,
            generation_names=_generation_names,
            read_current_generation=_read_current_generation,
            max_reconcile_age_seconds=max_reconcile_age_seconds,
        )
    if layout not in {None, LEGACY_PACK_INDEX_LAYOUT}:
        return invalid_layout_status(
            root,
            workload_ids=workload_ids,
            error=CacheLayoutError(
                "DPONE_CACHE_LAYOUT_INVALID",
                "Airflow cache layout is unsupported",
                str(root / LAYOUT_MARKER_NAME),
            ),
        )
    return _read_legacy_cache_status(
        root,
        workload_ids=workload_ids,
        max_index_bytes=max_index_bytes,
        max_pack_bytes=max_pack_bytes,
    )


def _read_legacy_cache_status(
    root: Path,
    *,
    workload_ids: tuple[str, ...],
    max_index_bytes: int,
    max_pack_bytes: int,
) -> dict[str, Any]:
    blockers: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    generation: str | None = None
    commit = None
    if os.path.lexists(root / LAYOUT_MARKER_NAME):
        try:
            authority = read_legacy_cache_authority(root)
            commit = authority.receipt
            generation = authority.current_generation
        except (OSError, ValueError) as exc:
            blockers.append(
                _blocker(
                    "airflow_pack_commit_receipt_invalid",
                    str(root / "status" / "current-commit.json"),
                    str(exc),
                )
            )
            generation = None
        if commit is None or not commit.durable:
            blockers.append(
                _blocker(
                    "airflow_pack_commit_receipt_missing",
                    str(root / "status" / "current-commit.json"),
                    "Versioned legacy cache has no durable current commit receipt",
                )
            )
        elif not authority.is_consistent_durable:
            blockers.append(
                _blocker(
                    "airflow_pack_commit_receipt_mismatch",
                    str(root / "status" / "current-commit.json"),
                    "Current pointers differ from the durable commit receipt",
                )
            )
    else:
        generation = _read_current_generation(root)
    generation_dir = root / "generations" / generation if generation else None
    index_path = generation_dir / "pack-index.json" if generation_dir else None
    index = (
        _read_json_mapping(
            index_path,
            blockers=blockers,
            max_bytes=max_index_bytes,
            too_large_code="airflow_pack_index_too_large",
        )
        if index_path
        else {}
    )
    actual_index_sha256 = (
        _sha256_file(index_path, max_bytes=max_index_bytes) if index_path and index_path.exists() and index else None
    )
    if (
        commit is not None
        and commit.durable
        and commit.index_sha256 is not None
        and actual_index_sha256 is not None
        and commit.index_sha256.removeprefix("sha256:") != actual_index_sha256.removeprefix("sha256:")
    ):
        blockers.append(
            _blocker(
                "airflow_pack_commit_index_mismatch",
                str(root / "status" / "current-commit.json"),
                "Durable commit receipt does not match the active pack index",
            )
        )
    if generation is None:
        blockers.append(_blocker("airflow_pack_cache_missing", str(root / "current"), "Current generation is absent"))
    elif not generation_dir or not generation_dir.exists():
        blockers.append(
            _blocker("airflow_pack_cache_missing", str(generation_dir), "Current generation directory is absent")
        )
    elif not index:
        blockers.append(_blocker("airflow_pack_index_missing", str(index_path), "pack-index.json is absent or invalid"))
    workloads = _workload_statuses(
        index=index,
        generation_dir=generation_dir,
        workload_ids=workload_ids,
        max_pack_bytes=max_pack_bytes,
    )
    blockers.extend(item for status in workloads.values() for item in status.get("blockers", ()))
    operational_status, operational_warnings = read_cache_operational_status(root)
    warnings.extend(operational_warnings)
    status = "blocked" if blockers else ("warning" if warnings else "success")
    return {
        "kind": "dpone.airflow_pack_cache_status",
        "schema_version": "1",
        "status": status,
        "layout": "legacy_pack_index",
        "cache_dir": str(root),
        "current_generation": generation,
        "commit_id": commit.commit_id if commit is not None and commit.durable else None,
        "commit_sequence": commit.sequence if commit is not None and commit.durable else None,
        "release_id": None,
        "deployment_id": None,
        "generations": _generation_names(root),
        "index_sha256": actual_index_sha256,
        "last_sync_status": _read_json_mapping(
            root / "status" / "last-sync-status.json",
            max_bytes=DEFAULT_MAX_CACHE_JSON_BYTES,
        ),
        "operational_status": operational_status,
        "workloads": workloads,
        "warnings": warnings,
        "blockers": blockers,
    }


def _workload_statuses(
    *,
    index: Mapping[str, Any],
    generation_dir: Path | None,
    workload_ids: tuple[str, ...],
    max_pack_bytes: int,
) -> dict[str, dict[str, Any]]:
    if generation_dir is None or not index:
        return {workload_id: _missing_workload_status(workload_id, generation_dir) for workload_id in workload_ids}
    requested = workload_ids or tuple(entry.workload_id for entry in index_entries(index) if entry.workload_id)
    return {
        workload_id: _workload_status(
            index=index,
            generation_dir=generation_dir,
            workload_id=workload_id,
            max_pack_bytes=max_pack_bytes,
        )
        for workload_id in requested
    }


def _workload_status(
    *,
    index: Mapping[str, Any],
    generation_dir: Path,
    workload_id: str,
    max_pack_bytes: int,
) -> dict[str, Any]:
    entry = find_index_entry(index, workload_id)
    if entry is None:
        return _missing_workload_status(workload_id, generation_dir)
    relative_path = safe_pack_relative_path(entry)
    if relative_path is None:
        return {
            "exists": False,
            "workload_id": workload_id,
            "blockers": [_blocker("airflow_pack_index_path_unsafe", entry.path, "Pack path is not relative-safe")],
        }
    path = generation_dir / relative_path
    exists = path.exists()
    actual_sha: str | None = None
    actual_bytes: int | None = None
    blockers: list[dict[str, str]] = []
    if not exists:
        blockers.append(_blocker("airflow_pack_cache_missing", str(path), "Cached pack file is absent"))
    elif max_pack_bytes <= 0:
        blockers.append(_blocker("airflow_pack_size_limit_invalid", str(path), "Pack size limit must be positive"))
    else:
        actual_bytes = path.stat().st_size
        if entry.bytes is not None and entry.bytes > max_pack_bytes:
            blockers.append(
                _blocker("airflow_pack_too_large", str(path), "Pack declared size exceeds configured size limit")
            )
        elif actual_bytes > max_pack_bytes:
            blockers.append(_blocker("airflow_pack_too_large", str(path), "Pack exceeds configured size limit"))
        else:
            try:
                actual_sha = _sha256_file(path, max_bytes=max_pack_bytes)
            except CacheStatusFileTooLarge:
                blockers.append(_blocker("airflow_pack_too_large", str(path), "Pack exceeds configured size limit"))
            except (OSError, ValueError):
                blockers.append(_blocker("airflow_pack_file_unsafe", str(path), "Pack is not a safe regular file"))
            if entry.sha256 and actual_sha is not None and actual_sha != entry.sha256:
                blockers.append(
                    _blocker("airflow_pack_hash_mismatch", str(path), "Cached pack checksum differs from index")
                )
            elif entry.bytes is not None and actual_bytes != entry.bytes:
                blockers.append(
                    _blocker("airflow_pack_size_mismatch", str(path), "Cached pack size differs from index")
                )
    return {
        "exists": exists,
        "workload_id": workload_id,
        "path": str(path),
        "relative_path": relative_path,
        "sha256": actual_sha,
        "expected_sha256": entry.sha256,
        "bytes": actual_bytes,
        "blockers": blockers,
    }


def _missing_workload_status(workload_id: str, generation_dir: Path | None) -> dict[str, Any]:
    path = generation_dir / f"airflow/{workload_id}/airflow-pack.json" if generation_dir else None
    return {
        "exists": False,
        "workload_id": workload_id,
        "path": str(path) if path else None,
        "blockers": [
            _blocker("airflow_pack_cache_missing", str(path or workload_id), "Cached workload pack is absent")
        ],
    }


def _read_current_generation(root: Path) -> str | None:
    return read_cache_status_current(root)


def _generation_names(root: Path) -> list[str]:
    generation_root = root / "generations"
    if not generation_root.exists():
        return []
    return sorted(path.name for path in generation_root.iterdir() if path.is_dir() and not path.name.startswith(".tmp"))


def _read_json_mapping(
    path: Path | None,
    *,
    blockers: list[dict[str, str]] | None = None,
    max_bytes: int = DEFAULT_MAX_CACHE_JSON_BYTES,
    too_large_code: str = "airflow_pack_json_too_large",
) -> dict[str, Any]:
    if path is None:
        return {}
    if max_bytes <= 0:
        if blockers is not None:
            blockers.append(
                _blocker("airflow_pack_json_size_limit_invalid", str(path), "JSON size limit must be positive")
            )
        return {}
    try:
        raw = read_bounded_regular_file(path, max_bytes=max_bytes)
    except FileNotFoundError:
        if blockers is not None:
            blockers.append(_blocker("airflow_pack_json_missing", str(path), "JSON file is absent"))
        return {}
    except CacheStatusFileTooLarge:
        if blockers is not None:
            blockers.append(_blocker(too_large_code, str(path), "JSON file exceeds configured size limit"))
        return {}
    except (OSError, ValueError):
        if blockers is not None:
            blockers.append(_blocker("airflow_pack_json_invalid", str(path), "JSON file is not a safe regular file"))
        return {}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError:
        if blockers is not None:
            blockers.append(_blocker("airflow_pack_json_invalid", str(path), "JSON file must be UTF-8"))
        return {}
    except json.JSONDecodeError as exc:
        if blockers is not None:
            blockers.append(_blocker("airflow_pack_json_invalid", str(path), exc.msg))
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _sha256_file(path: Path, *, max_bytes: int | None = None) -> str:
    limit = max_bytes if max_bytes is not None else DEFAULT_MAX_CACHED_PACK_BYTES
    return hashlib.sha256(read_bounded_regular_file(path, max_bytes=limit)).hexdigest()


def _blocker(code: str, path: str, message: str) -> dict[str, str]:
    return {"code": code, "path": path, "message": message}
