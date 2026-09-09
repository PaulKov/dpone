"""Snapshot reader ports for route refresh verification."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

from dpone.ops.routes.refresh_verification_models import (
    RouteRefreshSideSnapshot,
    RouteRefreshVerificationRequest,
)


class RouteRefreshSnapshotReader(Protocol):
    """Read one source or sink snapshot for an executed refresh chunk."""

    def read_chunk(self, request: RouteRefreshVerificationRequest) -> RouteRefreshSideSnapshot: ...


class UnavailableRouteRefreshSnapshotReader:
    """Reader used when verification is requested without a configured backend."""

    def read_chunk(self, request: RouteRefreshVerificationRequest) -> RouteRefreshSideSnapshot:
        del request
        raise RuntimeError("route_refresh_verification.reader_unavailable")


class JsonRouteRefreshSnapshotReader:
    """Snapshot reader backed by a stable JSON artifact."""

    def __init__(self, snapshot_json: str | Path) -> None:
        self._path = Path(snapshot_json)
        self._by_ordinal = _load_snapshot_index(self._path)

    def read_chunk(self, request: RouteRefreshVerificationRequest) -> RouteRefreshSideSnapshot:
        snapshot = self._by_ordinal.get(request.ordinal)
        if snapshot is None:
            raise RuntimeError(f"route_refresh_verification.snapshot_missing:{request.ordinal}")
        return snapshot


def _load_snapshot_index(path: Path) -> dict[int, RouteRefreshSideSnapshot]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("route refresh snapshot JSON must be an object")
    chunks = payload.get("chunks", ())
    if not isinstance(chunks, list | tuple):
        raise ValueError("route refresh snapshot JSON must contain a chunks list")
    result: dict[int, RouteRefreshSideSnapshot] = {}
    for item in chunks:
        if isinstance(item, Mapping):
            result[int(item.get("ordinal", 0))] = RouteRefreshSideSnapshot(
                row_count=int(item.get("row_count", 0)),
                min_boundary=str(item.get("min_boundary", "")),
                max_boundary=str(item.get("max_boundary", "")),
                typed_hash=str(item.get("typed_hash", "")),
                duplicate_keys=int(item.get("duplicate_keys", 0)),
                null_keys=int(item.get("null_keys", 0)),
                sample_rows=int(item.get("sample_rows", 0)),
                summary=str(item.get("summary", "")),
            )
    return result


__all__ = [
    "JsonRouteRefreshSnapshotReader",
    "RouteRefreshSnapshotReader",
    "UnavailableRouteRefreshSnapshotReader",
]
