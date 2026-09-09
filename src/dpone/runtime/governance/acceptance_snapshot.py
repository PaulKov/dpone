"""Canonical acceptance metric requests and untrusted probe normalization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from dpone.contracts.technical_columns import TechnicalColumnCatalog

ACCEPTANCE_SNAPSHOT_KIND = "dpone.acceptance.metric_snapshot.v1"
SNAPSHOT_INVALID_WARNING = "acceptance_metric_snapshot_invalid"


@dataclass(frozen=True, slots=True)
class AcceptanceMetricRequest:
    """A single source/staged/target dataset metric request."""

    side: str
    columns: tuple[str, ...]
    dataset_identity: str
    schema: str | None = None
    table: str | None = None
    sql: str | None = None
    sql_hash: str | None = None
    include_row_count: bool = True
    null_count_columns: tuple[str, ...] = ()
    distinct_count_columns: tuple[str, ...] = ()
    database: str | None = None


@dataclass(frozen=True, slots=True)
class AcceptanceMetricSnapshot:
    """Canonical durable row/null/distinct evidence for one dataset side."""

    side: str
    row_count: int | None = None
    null_counts: Mapping[str, int] = field(default_factory=dict)
    distinct_counts: Mapping[str, int] = field(default_factory=dict)
    columns: tuple[str, ...] = ()
    dataset: str = ""
    warnings: tuple[str, ...] = ()
    kind: str = ACCEPTANCE_SNAPSHOT_KIND

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "side": self.side,
            "dataset": self.dataset,
            "columns": list(self.columns),
            "row_count": self.row_count,
            "null_counts": dict(self.null_counts),
            "distinct_counts": dict(self.distinct_counts),
            "warnings": list(self.warnings),
            "captured_before_cleanup": True,
        }


class AcceptanceMetricSnapshotError(RuntimeError):
    """Safe failure for a malformed or incomplete connector probe response."""

    def __init__(self, side: str) -> None:
        self.side = side
        self.code = f"{side}_acceptance_metric_snapshot_invalid"
        super().__init__(self.code)


def canonical_snapshot(
    request: AcceptanceMetricRequest,
    raw: object,
    *,
    mode: str,
    selection_warnings: tuple[str, ...] = (),
) -> AcceptanceMetricSnapshot:
    """Rebuild one untrusted probe response from request-owned identity."""

    if not isinstance(raw, AcceptanceMetricSnapshot) or raw.warnings:
        return _invalid_or_raise(request, mode=mode, selection_warnings=selection_warnings)
    row_count = _requested_count(raw.row_count) if request.include_row_count else None
    null_counts = _requested_mapping(raw.null_counts, request.null_count_columns)
    distinct_counts = _requested_mapping(raw.distinct_counts, request.distinct_count_columns)
    if request.include_row_count and row_count is None or null_counts is None or distinct_counts is None:
        return _invalid_or_raise(request, mode=mode, selection_warnings=selection_warnings)
    return AcceptanceMetricSnapshot(
        side=request.side,
        row_count=row_count,
        null_counts=null_counts,
        distinct_counts=distinct_counts,
        columns=request.columns,
        dataset=request.dataset_identity,
        warnings=selection_warnings,
    )


def business_columns(schema: Sequence[tuple[str, str]] | Sequence[str]) -> tuple[str, ...]:
    """Return stable non-technical columns for one physical dataset side."""

    catalog = TechnicalColumnCatalog()
    columns = (str(item[0] if isinstance(item, tuple) else item) for item in schema)
    return tuple(
        dict.fromkeys(
            column for column in columns if not column.startswith("__dpone__") and not catalog.is_canonical(column)
        )
    )


def selected_columns(value: str | tuple[str, ...], columns: Sequence[str]) -> tuple[str, ...]:
    """Resolve one normalized selector against the schema of its own side."""

    if isinstance(value, tuple):
        allowed = set(columns)
        return tuple(column for column in value if column in allowed)
    if value in {"all_columns", "business_columns", "source_and_binary_semantics", "strict"}:
        return tuple(columns)
    return ()


def _invalid_or_raise(
    request: AcceptanceMetricRequest,
    *,
    mode: str,
    selection_warnings: tuple[str, ...],
) -> AcceptanceMetricSnapshot:
    if mode == "required":
        raise AcceptanceMetricSnapshotError(request.side)
    return AcceptanceMetricSnapshot(
        side=request.side,
        columns=request.columns,
        dataset=request.dataset_identity,
        warnings=_merge_warnings(selection_warnings, (SNAPSHOT_INVALID_WARNING,)),
    )


def _requested_count(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _requested_mapping(
    raw: object,
    requested: tuple[str, ...],
) -> dict[str, int] | None:
    if not isinstance(raw, Mapping):
        return None
    normalized: dict[str, int] = {}
    for column in requested:
        value = raw.get(column)
        count = _requested_count(value)
        if count is None:
            return None
        normalized[column] = count
    return normalized


def _merge_warnings(*groups: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(warning for group in groups for warning in group))


__all__ = [
    "ACCEPTANCE_SNAPSHOT_KIND",
    "AcceptanceMetricRequest",
    "AcceptanceMetricSnapshot",
    "AcceptanceMetricSnapshotError",
    "business_columns",
    "canonical_snapshot",
    "selected_columns",
    "SNAPSHOT_INVALID_WARNING",
]
