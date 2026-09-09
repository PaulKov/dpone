"""ClickHouse implementation of runtime acceptance metric probes."""

from __future__ import annotations

import re
from typing import Any, SupportsIndex, SupportsInt

from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricRequest, AcceptanceMetricSnapshot


class ClickHouseAcceptanceMetricProbe:
    """Collect row/null/distinct metrics with one ClickHouse aggregate query."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def collect(self, request: AcceptanceMetricRequest) -> AcceptanceMetricSnapshot:
        aliases = _aliases(request)
        rows = self._connector.get_records(_query(request, aliases), as_dict=True)
        row = dict(rows[0]) if rows else {}
        return AcceptanceMetricSnapshot(
            side=request.side,
            row_count=_optional_int(row.get("row_count")) if request.include_row_count else None,
            null_counts={column: int(row.get(aliases.null_counts[column], 0)) for column in request.null_count_columns},
            distinct_counts={
                column: int(row.get(aliases.distinct_counts[column], 0)) for column in request.distinct_count_columns
            },
            columns=request.columns,
            dataset=request.dataset_identity,
        )


class _AliasMap:
    def __init__(self, request: AcceptanceMetricRequest) -> None:
        self.null_counts = {column: f"null__{_alias(column)}" for column in request.null_count_columns}
        self.distinct_counts = {column: f"distinct__{_alias(column)}" for column in request.distinct_count_columns}


def _query(request: AcceptanceMetricRequest, aliases: _AliasMap) -> str:
    expressions: list[str] = []
    if request.include_row_count:
        expressions.append("count() AS `row_count`")
    expressions.extend(
        f"countIf(isNull({_identifier(column)})) AS `{alias}`" for column, alias in aliases.null_counts.items()
    )
    expressions.extend(
        f"uniqExact({_identifier(column)}) AS `{alias}`" for column, alias in aliases.distinct_counts.items()
    )
    if not expressions:
        expressions.append("count() AS `row_count`")
    return f"SELECT {', '.join(expressions)} FROM {_dataset(request)}"


def _dataset(request: AcceptanceMetricRequest) -> str:
    if request.sql:
        return f"({request.sql}) AS dpone_acceptance_source"
    if not request.schema or not request.table:
        raise ValueError(f"{request.side}_acceptance_metric_relation_required")
    return f"{_identifier(request.schema)}.{_identifier(request.table)}"


def _aliases(request: AcceptanceMetricRequest) -> _AliasMap:
    return _AliasMap(request)


def _identifier(value: str) -> str:
    return f"`{str(value).replace('`', '``')}`"


def _alias(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_]+", "_", str(value)).strip("_") or "column"


def _optional_int(value: str | bytes | SupportsInt | SupportsIndex | None) -> int | None:
    return int(value) if value is not None else None


__all__ = ["ClickHouseAcceptanceMetricProbe"]
