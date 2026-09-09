from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.routes.catalog import route_key_from_mapping
from dpone.ops.routes.refresh_snapshot_capture_models import RouteRefreshSnapshotCaptureRequest
from dpone.ops.routes.refresh_snapshot_capture_reader import chunk_bounds


def load_snapshot_execution(path: Path) -> Mapping[str, Any]:
    if not path.is_file():
        return {"passed": False, "status": "missing", "blockers": ["route_refresh_snapshot_capture.execution_missing"]}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "passed": False,
            "status": "invalid_json",
            "blockers": ["route_refresh_snapshot_capture.execution_invalid_json"],
        }
    return payload if isinstance(payload, Mapping) else {"passed": False, "status": "invalid_shape"}


def route_from_execution(payload: Mapping[str, Any]) -> Any:
    route = payload.get("route")
    values = route if isinstance(route, Mapping) else {}
    return route_key_from_mapping(values)


def chunks_from_execution(payload: Mapping[str, Any]) -> tuple[Mapping[str, object], ...]:
    chunks = payload.get("chunks", ())
    if not isinstance(chunks, list | tuple):
        return tuple()
    return tuple(item for item in chunks if isinstance(item, Mapping))


def base_blockers(*, execution_path: Path, execution: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    if not execution_path.is_file():
        blockers.append("route_refresh_snapshot_capture.execution_missing")
    if execution.get("status") != "succeeded" or not bool(execution.get("passed")):
        blockers.append("route_refresh_snapshot_capture.execution_not_succeeded")
    if not bool(execution.get("executed")):
        blockers.append("route_refresh_snapshot_capture.execution_not_run")
    return tuple(dict.fromkeys((*blockers, *_strings(execution.get("blockers")))))


def snapshot_request(
    *,
    route: Any,
    dataset: str,
    chunk: Mapping[str, object],
    execution_path: Path,
    runner_id: str,
    key_columns: tuple[str, ...],
    boundary_column: str,
    columns: tuple[str, ...],
    type_hints: Mapping[str, str],
) -> RouteRefreshSnapshotCaptureRequest:
    start, end, source_boundary, sink_boundary = chunk_bounds(chunk)
    return RouteRefreshSnapshotCaptureRequest(
        route=route,
        dataset=dataset,
        ordinal=_int(chunk.get("ordinal"), default=0),
        start=start,
        end=end,
        partition=str(chunk.get("partition", "")),
        source_boundary=source_boundary,
        sink_boundary=sink_boundary,
        idempotency_key=str(chunk.get("idempotency_key", "")),
        runner_id=runner_id,
        execution_path=str(execution_path),
        chunk_artifact_path=str(chunk.get("artifact_path", "")),
        columns=columns,
        key_columns=key_columns,
        boundary_column=boundary_column,
        type_hints=dict(type_hints),
    )


def _strings(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value else tuple()
    if isinstance(value, list | tuple):
        return tuple(str(item) for item in value if str(item))
    return tuple()


def _int(value: object, *, default: int) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
