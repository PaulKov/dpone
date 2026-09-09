"""Read one bounded, immutable active Airflow index snapshot."""

from __future__ import annotations

import json
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.runtime.deployment_cache_common import DeploymentCacheError, resolve_relative_current_symlink

ACTIVE_INDEX_RELATIVE_PATH = ".dpone-cache/current/airflow-index.json"
CACHE_RELATIVE_PATH = ".dpone-cache"
MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ActiveAirflowIndexSnapshot:
    """One bounded active-index read and its deeply immutable parsed mapping."""

    source: str
    content: bytes | None = None
    index: Mapping[str, Any] | None = None
    error_code: str | None = None
    error_message: str = ""


def load_active_index_snapshot(root: Path) -> ActiveAirflowIndexSnapshot:
    """Resolve and read the active deployment index once without remote I/O."""

    try:
        content = _read_active_index(root)
    except ConfinedFileError as exc:
        if exc.code == "file_not_found":
            return ActiveAirflowIndexSnapshot(source="missing")
        code, message = _index_read_failure(exc.code)
        return _invalid_snapshot(code, message)
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return _invalid_snapshot("DPONE_AIRFLOW_INDEX_INVALID", str(exc), content=content)
    if not isinstance(payload, dict):
        return _invalid_snapshot(
            "DPONE_AIRFLOW_INDEX_INVALID",
            "airflow-index.json must be a JSON object.",
            content=content,
        )
    return ActiveAirflowIndexSnapshot(
        source="active",
        content=content,
        index=_freeze_mapping(payload),
    )


def _invalid_snapshot(
    code: str,
    message: str,
    *,
    content: bytes | None = None,
) -> ActiveAirflowIndexSnapshot:
    return ActiveAirflowIndexSnapshot(
        source="invalid",
        content=content,
        error_code=code,
        error_message=message,
    )


def _read_active_index(root: Path) -> bytes:
    cache_root = root / CACHE_RELATIVE_PATH
    current = cache_root / "current"
    try:
        current_metadata = current.lstat()
    except FileNotFoundError:
        return read_confined_file(
            root,
            ACTIVE_INDEX_RELATIVE_PATH,
            max_bytes=MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES,
        )
    except OSError as exc:
        raise ConfinedFileError("file_unavailable", "Current cache pointer is unavailable.") from exc
    if stat.S_ISLNK(current_metadata.st_mode):
        try:
            active_root = resolve_relative_current_symlink(cache_root)
        except DeploymentCacheError as exc:
            raise ConfinedFileError("symlink_forbidden", "Current cache pointer is unsafe.") from exc
    elif stat.S_ISDIR(current_metadata.st_mode):
        active_root = current
    else:
        raise ConfinedFileError("not_regular_file", "Current cache pointer is not a directory.")
    relative_index = (active_root / "airflow-index.json").relative_to(root).as_posix()
    return read_confined_file(
        root,
        relative_index,
        max_bytes=MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES,
    )


def _index_read_failure(error_code: str) -> tuple[str, str]:
    if error_code == "file_too_large":
        return (
            "DPONE_AIRFLOW_INDEX_TOO_LARGE",
            "airflow-index.json exceeds the local diagnostics byte limit.",
        )
    if error_code in {"path_invalid", "symlink_forbidden", "not_regular_file"}:
        return (
            "DPONE_AIRFLOW_INDEX_UNSAFE",
            "airflow-index.json is not a confined regular cache file.",
        )
    return (
        "DPONE_AIRFLOW_INDEX_UNAVAILABLE",
        "airflow-index.json could not be opened safely.",
    )


def _freeze_mapping(value: dict[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return _freeze_mapping(value)
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


__all__ = [
    "ActiveAirflowIndexSnapshot",
    "MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES",
    "load_active_index_snapshot",
]
