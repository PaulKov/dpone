"""Exact desired-state cache diagnostics for ``current/airflow-index.json``."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_activation_contract import CacheActivationIdentity
from dpone_airflow_pack.cache_artifact_contract import (
    read_confined_cache_file,
    read_confined_cache_file_with_identity,
)
from dpone_airflow_pack.cache_operational_status import read_cache_operational_status
from dpone_airflow_pack.cache_reconcile_status import read_reconcile_status
from dpone_airflow_pack.deployment_index_contract import (
    INDEX_SCHEMA_V1,
    INDEX_SCHEMA_V2,
    AirflowDeploymentIndexError,
    infer_cache_root,
    resolve_cache_artifact,
)

_SUPPORTED_INDEX_SCHEMAS = frozenset({INDEX_SCHEMA_V1, INDEX_SCHEMA_V2})
_DEFAULT_MAX_CACHE_JSON_BYTES = 8 * 1024 * 1024


def exact_deployment_index_path(root: Path) -> Path | None:
    """Return the active exact-cache index path when present."""

    current = root / "current"
    candidate = current / "airflow-index.json"
    if candidate.is_file():
        return candidate
    if current.is_symlink():
        try:
            resolved = current.resolve(strict=False) / "airflow-index.json"
        except OSError:
            return None
        if resolved.is_file():
            return resolved
    return None


def read_exact_cache_status(
    root: Path,
    index_path: Path,
    *,
    workload_ids: tuple[str, ...],
    max_index_bytes: int,
    max_pack_bytes: int,
    generation_names,
    read_current_generation,
    max_reconcile_age_seconds: int,
) -> dict[str, Any]:
    """Build a cache-status snapshot for an exact deployment index."""

    blockers: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    index, activation, index_sha = _read_exact_index_snapshot(
        root,
        index_path,
        blockers=blockers,
        max_bytes=max_index_bytes,
    )
    release_id: str | None = None
    deployment_id: str | None = None
    activation_id: str | None = None
    workloads: dict[str, dict[str, Any]] = {}
    if not index:
        if not blockers:
            blockers.append(
                _blocker(
                    "airflow_pack_index_missing",
                    str(index_path),
                    "current/airflow-index.json is absent or invalid",
                )
            )
        workloads = {workload_id: _missing_workload_status(workload_id) for workload_id in workload_ids}
    elif index.get("schema") not in _SUPPORTED_INDEX_SCHEMAS:
        blockers.append(
            _blocker(
                "airflow_pack_index_schema_mismatch",
                str(index_path),
                f"Expected schema {INDEX_SCHEMA_V1} or {INDEX_SCHEMA_V2}",
            )
        )
        workloads = {workload_id: _missing_workload_status(workload_id) for workload_id in workload_ids}
    else:
        release_id = _optional_text(index.get("release_id"))
        deployment_id = _optional_text(index.get("deployment_id"))
        activation_id = activation.activation_id if activation is not None else None
        if activation is not None and (
            activation.deployment_id != deployment_id
            or (activation.release_id is not None and activation.release_id != release_id)
        ):
            blockers.append(
                _blocker(
                    "airflow_pack_current_pointer_mismatch",
                    str(index_path),
                    "current-pointer identity differs from airflow-index identity",
                )
            )
        if index.get("schema") == INDEX_SCHEMA_V2 and activation_id is None:
            blockers.append(
                _blocker(
                    "airflow_pack_activation_id_missing",
                    str(root / "current-pointer.json"),
                    "strict v2 cache requires an exact activation_id in current-pointer.json",
                )
            )
        try:
            cache_root = infer_cache_root(index_path)
        except Exception:  # noqa: BLE001 - keep status fail-visible without crashing CLI.
            cache_root = root
        pack_entries = _workload_pack_entries(index)
        requested = workload_ids or tuple(sorted(pack_entries))
        workloads = {
            workload_id: _workload_status(
                cache_root=cache_root,
                workload_id=workload_id,
                entry=pack_entries.get(workload_id),
                max_pack_bytes=max_pack_bytes,
            )
            for workload_id in requested
        }
    blockers.extend(item for status in workloads.values() for item in status.get("blockers", ()))
    reconcile_status, reconcile_blockers, reconcile_warnings = read_reconcile_status(
        root,
        release_id=release_id,
        deployment_id=deployment_id,
        activation_id=activation_id,
        required=index.get("schema") == INDEX_SCHEMA_V2,
        airflow_index_sha256=("sha256:" + index_sha if index_sha is not None else None),
        max_age_seconds=max_reconcile_age_seconds,
    )
    blockers.extend(reconcile_blockers)
    warnings.extend(reconcile_warnings)
    operational_status, operational_warnings = read_cache_operational_status(root)
    warnings.extend(operational_warnings)
    status = "blocked" if blockers else ("warning" if warnings else "success")
    generation = deployment_id or read_current_generation(root)
    return {
        "kind": "dpone.airflow_pack_cache_status",
        "schema_version": "1",
        "status": status,
        "layout": "exact_deployment_index",
        "cache_dir": str(root),
        "current_generation": generation,
        "release_id": release_id,
        "deployment_id": deployment_id,
        "activation_id": activation_id,
        "generations": generation_names(root),
        "index_sha256": index_sha,
        "last_sync_status": _read_json_mapping(
            root / "status" / "last-sync-status.json",
            cache_root=root,
        ),
        "last_reconcile_status": reconcile_status,
        "operational_status": operational_status,
        "workloads": workloads,
        "warnings": warnings,
        "blockers": blockers,
    }


def _read_exact_index_snapshot(
    root: Path,
    index_path: Path,
    *,
    blockers: list[dict[str, str]],
    max_bytes: int,
) -> tuple[dict[str, Any], CacheActivationIdentity | None, str | None]:
    try:
        snapshot = read_confined_cache_file_with_identity(
            index_path,
            cache_root=root,
            max_bytes=max_bytes,
            allow_current_pointer=True,
        )
        payload = json.loads(snapshot.content)
        if not isinstance(payload, dict):
            raise ValueError("airflow-index root must be an object")
    except AirflowDeploymentIndexError as exc:
        code = (
            "airflow_pack_current_pointer_invalid"
            if exc.code == "DPONE_CURRENT_POINTER_INVALID"
            else "airflow_pack_index_read_failed"
        )
        blockers.append(_blocker(code, str(index_path), str(exc)))
        return {}, None, None
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        blockers.append(
            _blocker(
                "airflow_pack_index_invalid",
                str(index_path),
                "current/airflow-index.json is not a strict JSON object",
            )
        )
        return {}, None, None
    return (
        payload,
        snapshot.activation,
        hashlib.sha256(snapshot.content).hexdigest(),
    )


def _workload_pack_entries(index: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw = index.get("workload_packs")
    if not isinstance(raw, list):
        return {}
    entries: dict[str, Mapping[str, Any]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        workload_id = item.get("id")
        if isinstance(workload_id, str) and workload_id.strip():
            entries[workload_id] = item
    return entries


def _workload_status(
    *,
    cache_root: Path,
    workload_id: str,
    entry: Mapping[str, Any] | None,
    max_pack_bytes: int,
) -> dict[str, Any]:
    if entry is None:
        return _missing_workload_status(workload_id)
    artifact_ref = entry.get("artifact_ref")
    expected_sha = entry.get("sha256")
    if not isinstance(artifact_ref, str) or not artifact_ref.strip():
        return {
            "exists": False,
            "workload_id": workload_id,
            "blockers": [
                _blocker(
                    "airflow_pack_index_path_unsafe",
                    workload_id,
                    "Exact-cache workload pack is missing artifact_ref",
                )
            ],
        }
    try:
        path = resolve_cache_artifact(artifact_ref, cache_root=cache_root)
    except AirflowDeploymentIndexError as exc:
        return {
            "exists": False,
            "workload_id": workload_id,
            "blockers": [_blocker(exc.code, artifact_ref, str(exc))],
        }
    exists = False
    actual_sha: str | None = None
    actual_bytes: int | None = None
    blockers: list[dict[str, str]] = []
    if max_pack_bytes <= 0:
        blockers.append(_blocker("airflow_pack_size_limit_invalid", str(path), "Pack size limit must be positive"))
    else:
        try:
            content = read_confined_cache_file(
                path,
                cache_root=cache_root,
                max_bytes=max_pack_bytes,
            )
            exists = True
            actual_bytes = len(content)
            actual_sha = f"sha256:{hashlib.sha256(content).hexdigest()}"
        except AirflowDeploymentIndexError as exc:
            code = "airflow_pack_cache_missing" if exc.code == "DPONE_CACHE_ARTIFACT_MISSING" else exc.code
            blockers.append(_blocker(code, str(path), str(exc)))
        if isinstance(expected_sha, str) and expected_sha and actual_sha is not None and actual_sha != expected_sha:
            blockers.append(
                _blocker("airflow_pack_hash_mismatch", str(path), "Cached pack checksum differs from index")
            )
    return {
        "exists": exists,
        "workload_id": workload_id,
        "path": str(path),
        "artifact_ref": artifact_ref,
        "sha256": actual_sha,
        "expected_sha256": expected_sha if isinstance(expected_sha, str) else None,
        "bytes": actual_bytes,
        "blockers": blockers,
    }


def _missing_workload_status(workload_id: str) -> dict[str, Any]:
    return {
        "exists": False,
        "workload_id": workload_id,
        "path": None,
        "blockers": [
            _blocker("airflow_pack_cache_missing", workload_id, "Cached workload pack is absent from airflow-index")
        ],
    }


def _read_json_mapping(
    path: Path | None,
    *,
    cache_root: Path,
    blockers: list[dict[str, str]] | None = None,
    max_bytes: int = _DEFAULT_MAX_CACHE_JSON_BYTES,
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
        raw = read_confined_cache_file(path, cache_root=cache_root, max_bytes=max_bytes)
    except AirflowDeploymentIndexError as exc:
        if exc.code != "DPONE_CACHE_ARTIFACT_MISSING" and blockers is not None:
            blockers.append(_blocker("airflow_pack_json_invalid", str(path), str(exc)))
        return {}
    except FileNotFoundError:
        if blockers is not None:
            blockers.append(_blocker("airflow_pack_json_missing", str(path), "JSON file is absent"))
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


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _blocker(code: str, path: str, message: str) -> dict[str, str]:
    return {"code": code, "path": path, "message": message}
