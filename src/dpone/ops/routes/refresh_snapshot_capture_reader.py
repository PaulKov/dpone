"""Row reader ports and snapshot normalization for route refresh capture."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

from dpone.ops.routes.refresh_snapshot_capture_models import RouteRefreshSnapshotCaptureRequest
from dpone.ops.routes.refresh_verification_models import RouteRefreshSideSnapshot, typed_hash


class RouteRefreshRowsReader(Protocol):
    """Read raw rows for one route refresh chunk."""

    def read_rows(self, request: RouteRefreshSnapshotCaptureRequest) -> Sequence[Mapping[str, object]]: ...


class UnavailableRouteRefreshRowsReader:
    """Rows reader used when capture is requested without a configured backend."""

    def read_rows(self, request: RouteRefreshSnapshotCaptureRequest) -> Sequence[Mapping[str, object]]:
        del request
        raise RuntimeError("route_refresh_snapshot_capture.reader_unavailable")


class JsonRouteRefreshRowsReader:
    """Rows reader backed by a credential-free JSON fixture."""

    def __init__(self, rows_json: str | Path) -> None:
        self._path = Path(rows_json)
        self._by_ordinal, self._all_rows = _load_rows(self._path)

    def read_rows(self, request: RouteRefreshSnapshotCaptureRequest) -> Sequence[Mapping[str, object]]:
        rows = self._by_ordinal.get(request.ordinal)
        if rows is not None:
            return rows
        if self._all_rows is not None:
            return _bounded_rows(
                self._all_rows,
                boundary_column=request.boundary_column,
                start=request.start,
                end=request.end,
                type_hints=request.type_hints,
            )
        raise RuntimeError(f"route_refresh_snapshot_capture.rows_missing:{request.ordinal}")


def snapshot_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    columns: Sequence[str],
    key_columns: Sequence[str],
    boundary_column: str,
    type_hints: Mapping[str, str] | None = None,
) -> RouteRefreshSideSnapshot:
    """Build a normalized verification snapshot from raw rows."""

    hints = {str(key): str(value).lower() for key, value in (type_hints or {}).items()}
    values = [row.get(boundary_column) for row in rows if row.get(boundary_column) is not None]
    ordered_values = sorted(values, key=lambda value: _boundary_key(value, hints.get(boundary_column, "string")))
    return RouteRefreshSideSnapshot(
        row_count=len(rows),
        min_boundary="" if not ordered_values else str(ordered_values[0]),
        max_boundary="" if not ordered_values else str(ordered_values[-1]),
        typed_hash=typed_hash(rows, columns=columns, key_columns=key_columns, type_hints=hints),
        duplicate_keys=_duplicate_keys(rows, key_columns),
        null_keys=_null_keys(rows, key_columns),
    )


def empty_side_snapshot() -> RouteRefreshSideSnapshot:
    """Return an empty side snapshot for failed capture attempts."""

    return RouteRefreshSideSnapshot(row_count=0, min_boundary="", max_boundary="", typed_hash="")


def chunk_bounds(chunk: Mapping[str, object]) -> tuple[str, str, str, str]:
    """Return start/end and source/sink boundaries from current or legacy chunk receipts."""

    source_boundary = _text(chunk.get("source_boundary"))
    sink_boundary = _text(chunk.get("sink_boundary"))
    start = _text(chunk.get("start"))
    end = _text(chunk.get("end"))
    if not (start and end):
        start, end = _first_bounds(
            source_boundary,
            sink_boundary,
            _idempotency_boundary(chunk.get("idempotency_key")),
            _artifact_boundary(chunk.get("artifact_path")),
        )
    if not source_boundary and start and end:
        source_boundary = f"{start}..{end}"
    if not sink_boundary and start and end:
        sink_boundary = f"{start}..{end}"
    return start, end, source_boundary, sink_boundary


def _load_rows(
    path: Path,
) -> tuple[dict[int, tuple[Mapping[str, object], ...]], tuple[Mapping[str, object], ...] | None]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return {}, _rows(payload)
    if not isinstance(payload, Mapping):
        raise ValueError("route refresh rows JSON must be an object or a list")
    chunks = payload.get("chunks")
    if isinstance(chunks, list | tuple):
        by_ordinal: dict[int, tuple[Mapping[str, object], ...]] = {}
        for item in chunks:
            if isinstance(item, Mapping):
                by_ordinal[int(item.get("ordinal", 0))] = _rows(item.get("rows"))
        return by_ordinal, None
    return {}, _rows(payload.get("rows"))


def _rows(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list | tuple):
        return tuple()
    return tuple(item for item in value if isinstance(item, Mapping))


def _bounded_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    boundary_column: str,
    start: str,
    end: str,
    type_hints: Mapping[str, str],
) -> tuple[Mapping[str, object], ...]:
    hint = type_hints.get(boundary_column, "string")
    start_key = _boundary_key(start, hint)
    end_key = _boundary_key(end, hint)
    return tuple(
        row
        for row in rows
        if row.get(boundary_column) is not None
        and start_key <= _boundary_key(row.get(boundary_column), hint) <= end_key
    )


def _duplicate_keys(rows: Sequence[Mapping[str, object]], key_columns: Sequence[str]) -> int:
    seen: set[tuple[object, ...]] = set()
    duplicates = 0
    for row in rows:
        key = tuple(row.get(column) for column in key_columns)
        if any(value is None for value in key):
            continue
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return duplicates


def _null_keys(rows: Sequence[Mapping[str, object]], key_columns: Sequence[str]) -> int:
    return sum(1 for row in rows if any(row.get(column) is None for column in key_columns))


def _boundary_key(value: object, type_hint: str) -> tuple[int, Decimal | str]:
    if value is None:
        return (0, "")
    hint = str(type_hint).lower()
    if any(token in hint for token in ("int", "decimal", "numeric", "money", "float", "real", "double")):
        try:
            return (1, Decimal(str(value)))
        except InvalidOperation:
            return (1, str(value))
    return (1, str(value))


def _first_bounds(*values: str) -> tuple[str, str]:
    for value in values:
        start, end = _split_boundary(value)
        if start and end:
            return start, end
    return "", ""


def _split_boundary(value: str) -> tuple[str, str]:
    text = value.strip()
    if ".." not in text:
        return "", ""
    start, end = (part.strip() for part in text.split("..", 1))
    return start, end


def _idempotency_boundary(value: object) -> str:
    text = _text(value)
    return text.rsplit(":", 1)[-1] if ":" in text else text


def _artifact_boundary(value: object) -> str:
    path = Path(_text(value))
    if not path.is_file():
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, Mapping):
        return ""
    chunk = payload.get("chunk")
    values = chunk if isinstance(chunk, Mapping) else {}
    start = _text(values.get("start"))
    end = _text(values.get("end"))
    if start and end:
        return f"{start}..{end}"
    return _text(values.get("source_boundary")) or _text(values.get("sink_boundary"))


def _text(value: object) -> str:
    return str(value).strip() if value not in (None, "") else ""


__all__ = [
    "JsonRouteRefreshRowsReader",
    "RouteRefreshRowsReader",
    "UnavailableRouteRefreshRowsReader",
    "chunk_bounds",
    "empty_side_snapshot",
    "snapshot_from_rows",
]
