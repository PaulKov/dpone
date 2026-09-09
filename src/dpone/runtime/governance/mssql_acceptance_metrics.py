"""SQL Server implementation of runtime acceptance metric probes."""

from __future__ import annotations

from typing import Any, SupportsIndex, SupportsInt

from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricRequest, AcceptanceMetricSnapshot
from dpone.runtime.support.mssql_object_name import MSSQLObjectName


class MssqlAcceptanceMetricProbe:
    """Collect target metrics with one aggregate query against an explicit database."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def collect(self, request: AcceptanceMetricRequest) -> AcceptanceMetricSnapshot:
        relation = _required_relation(request)
        aliases = _AliasMap(request)
        rows = self._connector.get_records(_query(self._connector, request, aliases, relation), as_dict=True)
        row = dict(rows[0]) if rows else {}
        return AcceptanceMetricSnapshot(
            side=request.side,
            row_count=_optional_int(row.get("row_count")) if request.include_row_count else None,
            null_counts={column: int(row.get(alias, 0)) for column, alias in aliases.null_counts.items()},
            distinct_counts={column: int(row.get(alias, 0)) for column, alias in aliases.distinct_counts.items()},
            columns=request.columns,
            dataset=request.dataset_identity,
        )


class _AliasMap:
    """Collision-free SQL aliases whose authority remains the request column order."""

    def __init__(self, request: AcceptanceMetricRequest) -> None:
        self.null_counts = {column: f"null__{index}" for index, column in enumerate(request.null_count_columns)}
        self.distinct_counts = {
            column: f"distinct__{index}" for index, column in enumerate(request.distinct_count_columns)
        }


def _query(
    connector: Any,
    request: AcceptanceMetricRequest,
    aliases: _AliasMap,
    relation: MSSQLObjectName | None,
) -> str:
    quote = connector.quote_identifier
    expressions: list[str] = []
    if request.include_row_count:
        expressions.append("COUNT_BIG(*) AS [row_count]")
    expressions.extend(
        "COALESCE(SUM(CASE WHEN "
        f"{quote(column)} IS NULL THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END), 0) AS {quote(alias)}"
        for column, alias in aliases.null_counts.items()
    )
    expressions.extend(
        f"COUNT_BIG(DISTINCT {quote(column)}) AS {quote(alias)}" for column, alias in aliases.distinct_counts.items()
    )
    if not expressions:
        expressions.append("COUNT_BIG(*) AS [row_count]")
    return f"SELECT {', '.join(expressions)} FROM {_dataset(request, relation)}"


def _dataset(request: AcceptanceMetricRequest, relation: MSSQLObjectName | None) -> str:
    if request.sql:
        return f"({request.sql}) AS [dpone_acceptance_source]"
    if relation is None:
        raise ValueError(f"{request.side}_acceptance_metric_relation_required")
    return relation.quoted()


def _required_relation(request: AcceptanceMetricRequest) -> MSSQLObjectName | None:
    if request.sql:
        return None
    if not request.schema or not request.table:
        raise ValueError(f"{request.side}_acceptance_metric_relation_required")
    relation = MSSQLObjectName.from_parts(
        database=request.database,
        schema=request.schema,
        table=request.table,
        strict=True,
    )
    if relation.database is None:
        raise ValueError(f"{request.side}_acceptance_metric_database_required")
    if request.dataset_identity != relation.dataset:
        raise ValueError(f"{request.side}_acceptance_metric_dataset_identity_mismatch")
    return relation


def _optional_int(value: str | bytes | SupportsInt | SupportsIndex | None) -> int | None:
    return int(value) if value is not None else None


__all__ = ["MssqlAcceptanceMetricProbe"]
