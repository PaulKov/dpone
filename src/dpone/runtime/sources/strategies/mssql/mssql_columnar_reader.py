from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from dpone.runtime.columnar_range_parallelism import RangeParallelismPreflight
from dpone.runtime.partitioning import RangePartition, RangePartitioner
from dpone.runtime.partitioning_predicates import MssqlPartitionPredicateRenderer


class ColumnarRangeSession:
    """Independently owned MSSQL session with a best-effort cancellation port."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connector, name)

    def cancel(self) -> None:
        cancel = getattr(self._connector, "cancel", None)
        if callable(cancel):
            cancel()
            return
        connection = getattr(self._connector, "_connection", None)
        connection_cancel = getattr(connection, "cancel", None)
        if callable(connection_cancel):
            connection_cancel()

    def close(self) -> None:
        close = getattr(self._connector, "close", None)
        if callable(close):
            close()


def range_read_blockers(request: Any, connector: Any) -> tuple[str, ...]:
    """Return fail-closed range admission blockers before any source I/O."""

    if request.range_plan is None:
        return ()
    blockers: list[str] = []
    if not hasattr(connector, "open_session"):
        blockers.append("mssql_independent_range_sessions_unavailable")
    if request.uri_prefix.startswith("local://"):
        blockers.append("columnar_range_parallelism_requires_object_storage")
    partitioner = request.range_partitioner
    if partitioner is None:
        blockers.append("columnar_range_partitioner_missing")
        return tuple(blockers)
    if partitioner.column.casefold() not in {column.casefold() for column, _ in request.schema}:
        blockers.append("columnar_range_column_not_projected")
    try:
        RangeParallelismPreflight.validate(
            request.range_plan.policy,
            partition_column=partitioner.column,
            query_has_window_functions=bool(re.search(r"\bover\s*\(", request.query, flags=re.IGNORECASE)),
            supported_topologies={"shared_per_run", "per_partition"},
        )
    except ValueError as exc:
        blockers.append(f"columnar_range_preflight:{exc}")
    return tuple(blockers)


def iter_columnar_batches(
    connector: Any,
    *,
    query: str,
    schema: Sequence[tuple[str, str]],
    batch_size: int,
    partitioner: RangePartitioner | None = None,
    partition: RangePartition | None = None,
):
    """Stream values after SQL-side projection of ODBC-lossy temporal types."""

    read_query = parquet_read_query(query, schema)
    if partitioner is not None or partition is not None:
        if partitioner is None or partition is None:
            raise ValueError("Both partitioner and partition are required for a ranged columnar read.")
        read_query = partitioner.wrap_query(
            read_query,
            _quote_identifier(partitioner.column),
            partition,
            renderer=MssqlPartitionPredicateRenderer(),
        )
    yield from connector.get_records_streaming(
        read_query,
        batch_size=batch_size,
        as_dict=False,
    )


def parquet_read_query(query: str, schema: Sequence[tuple[str, str]]) -> str:
    """Project 100ns MSSQL values to exact text before pyodbc truncates them."""

    if not any(_requires_text_projection(source_type) for _, source_type in schema):
        return query
    source_query = query.strip().removesuffix(";").strip()
    columns = ",\n    ".join(_projection(column, source_type) for column, source_type in schema)
    return f"SELECT\n    {columns}\nFROM (\n{source_query}\n) AS dpone_columnar_src"


def _projection(column: str, source_type: str) -> str:
    quoted = _quote_identifier(column)
    source = f"dpone_columnar_src.{quoted}"
    normalized = _normalize_source_type(source_type)
    base = normalized.split("(", 1)[0]
    if base == "datetime2":
        return f"CONVERT(VARCHAR(33), {source}, 121) AS {quoted}"
    if base == "time":
        return f"CONVERT(VARCHAR(16), {source}) AS {quoted}"
    if base == "datetimeoffset":
        utc = f"CAST(SWITCHOFFSET(CAST({source} AS datetimeoffset), '+00:00') AS datetime2(7))"
        return f"CONVERT(VARCHAR(33), {utc}, 121) AS {quoted}"
    return f"{source} AS {quoted}"


def _requires_text_projection(source_type: str) -> bool:
    base = _normalize_source_type(source_type).split("(", 1)[0]
    return base in {"datetime2", "datetimeoffset", "time"}


def _normalize_source_type(source_type: str) -> str:
    normalized = str(source_type).strip().lower().replace(" nullable", "").strip()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized.removeprefix("nullable(").removesuffix(")").strip()
    return re.sub(r"\s+", "", normalized)


def _quote_identifier(value: str) -> str:
    return "[" + str(value).replace("]", "]]") + "]"


__all__ = ["ColumnarRangeSession", "iter_columnar_batches", "parquet_read_query", "range_read_blockers"]
