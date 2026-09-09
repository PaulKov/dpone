"""Run-marker helpers for MSSQL columnar object-storage snapshots."""

from __future__ import annotations

from typing import Any

from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.object_storage_retention import (
    ObjectStorageRetentionPolicy,
    ObjectStorageRunMarker,
    write_run_marker,
)
from dpone.storage import ObjectStorageUri
from dpone.storage.protocols import ObjectStorageClient


def write_columnar_run_marker(
    object_client: ObjectStorageClient,
    prefix: ObjectStorageUri,
    request: ColumnarSnapshotRequest,
) -> None:
    options = dict(request.options or {})
    policy = ObjectStorageRetentionPolicy.from_options(_mapping(options.get("retention")))
    marker = ObjectStorageRunMarker.running(
        run_id=request.run_id,
        workload_id=str(options.get("workload_id") or ""),
        table=str(options.get("table") or _table_from_query(request.query)),
        policy=policy,
    )
    write_run_marker(object_client, prefix, marker=marker)


def _table_from_query(query: str) -> str:
    lowered = query.lower()
    if " from " not in lowered:
        return ""
    after_from = query[lowered.index(" from ") + 6 :].strip().split()[0]
    return after_from.strip("[]").split(".")[-1].strip("[]")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


__all__ = ["write_columnar_run_marker"]
