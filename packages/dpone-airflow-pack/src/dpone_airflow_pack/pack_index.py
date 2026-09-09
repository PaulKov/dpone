"""Read compact remote Airflow pack indexes without importing full dpone."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any


@dataclass(frozen=True)
class AirflowPackIndexEntry:
    """One workload entry in `pack-index.json`."""

    workload_id: str
    path: str
    sha256: str | None = None
    bytes: int | None = None
    uri: str | None = None


def index_generation(index: Mapping[str, Any]) -> str | None:
    """Return the immutable generation id declared by the index."""

    value = index.get("git_sha") or index.get("generation") or index.get("release_id")
    return str(value).strip() if value else None


def index_entries(index: Mapping[str, Any]) -> tuple[AirflowPackIndexEntry, ...]:
    """Normalize supported pack-index shapes into entries."""

    # Publisher emits ``artifacts``; legacy indexes use ``packs`` / ``workloads``.
    # Skip empty placeholder maps (e.g. ``"packs": {}`` shadowing ``artifacts``).
    for key in ("artifacts", "packs", "workloads"):
        raw_entries = index.get(key)
        if isinstance(raw_entries, Mapping):
            if not raw_entries:
                continue
            return tuple(
                _entry_from_mapping(str(workload_id), item, default_path_kind="pack")
                for workload_id, item in raw_entries.items()
            )
        if isinstance(raw_entries, list):
            if not raw_entries:
                continue
            return tuple(
                _entry_from_mapping(None, item, default_path_kind="pack")
                for item in raw_entries
                if isinstance(item, Mapping)
            )
    return ()


def dag_spec_index_entries(index: Mapping[str, Any]) -> tuple[AirflowPackIndexEntry, ...]:
    """Normalize ``dag_specs`` map from pack-index.json into entries."""

    raw = index.get("dag_specs")
    if not isinstance(raw, Mapping):
        return ()
    return tuple(_entry_from_mapping(str(dag_id), item, default_path_kind="dag_spec") for dag_id, item in raw.items())


def find_index_entry(index: Mapping[str, Any], workload_id: str) -> AirflowPackIndexEntry | None:
    """Find a workload entry by id."""

    for entry in index_entries(index):
        if entry.workload_id == workload_id:
            return entry
    return None


def safe_pack_relative_path(entry: AirflowPackIndexEntry) -> str | None:
    """Return a safe relative cache path or `None` when the index path is unsafe."""

    path = PurePosixPath(entry.path)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        return None
    return path.as_posix()


def default_pack_relative_path(workload_id: str) -> str:
    return f"{workload_id}/airflow-pack.json"


def default_dag_spec_relative_path(dag_id: str) -> str:
    return f"airflow/_dags/{dag_id}.dag-spec.json"


def _entry_from_mapping(
    workload_id: str | None,
    value: object,
    *,
    default_path_kind: str = "pack",
) -> AirflowPackIndexEntry:
    item = value if isinstance(value, Mapping) else {}
    resolved_workload_id = str(
        workload_id or item.get("workload_id") or item.get("id") or item.get("name") or ""
    ).strip()
    default_path = (
        default_dag_spec_relative_path(resolved_workload_id)
        if default_path_kind == "dag_spec"
        else default_pack_relative_path(resolved_workload_id)
    )
    path = str(
        item.get("path")
        or item.get("relative_path")
        or item.get("artifact_path")
        or item.get("airflow_pack")
        or default_path
    )
    uri_value = item.get("uri") or item.get("artifact_uri")
    bytes_value = _optional_int(item.get("bytes") or item.get("size") or item.get("size_bytes"))
    sha_value = item.get("sha256") or item.get("hash")
    return AirflowPackIndexEntry(
        workload_id=resolved_workload_id,
        path=path,
        sha256=str(sha_value) if sha_value else None,
        bytes=bytes_value,
        uri=str(uri_value) if uri_value else None,
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if not isinstance(value, str | bytes | bytearray):
        return None
    try:
        return int(value)
    except ValueError:
        return None
