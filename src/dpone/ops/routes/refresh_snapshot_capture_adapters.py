"""SQL row-reader adapters for route refresh snapshot capture."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from dpone.ops.routes.refresh_executors.mssql_clickhouse_config import (
    int_value,
    quote_clickhouse_identifier,
    safe_identifier,
    split_dataset,
)
from dpone.ops.routes.refresh_executors.postgres_mssql_config import (
    quote_mssql_identifier,
    quote_postgres_identifier,
)
from dpone.ops.routes.refresh_snapshot_capture_models import RouteRefreshSnapshotCaptureRequest


class _SqlRecordsConnector(Protocol):
    def get_records(
        self,
        query: str,
        params: object | None = None,
        as_dict: bool = False,
    ) -> Sequence[Mapping[str, object]]: ...


class PostgresRouteRefreshRowsReader:
    """Read bounded route refresh rows from a Postgres source table."""

    def __init__(
        self,
        *,
        connector: _SqlRecordsConnector,
        dataset: str,
        columns: tuple[str, ...],
        boundary_column: str,
        query_template: str = "",
    ) -> None:
        self._connector = connector
        self._dataset = dataset
        self._columns = columns
        self._boundary_column = boundary_column
        self._query_template = query_template

    def read_rows(self, request: RouteRefreshSnapshotCaptureRequest) -> Sequence[Mapping[str, object]]:
        return self._connector.get_records(
            _query(
                dataset=self._dataset,
                columns=self._columns,
                boundary_column=self._boundary_column,
                start=request.start,
                end=request.end,
                partition=request.partition,
                query_template=self._query_template,
                quote_identifier=quote_postgres_identifier,
                quote_dataset=_quoted_dataset(quote_postgres_identifier),
            ),
            as_dict=True,
        )


class MssqlRouteRefreshRowsReader:
    """Read bounded route refresh rows from an MSSQL source or sink table."""

    def __init__(
        self,
        *,
        connector: _SqlRecordsConnector,
        dataset: str,
        columns: tuple[str, ...],
        boundary_column: str,
        query_template: str = "",
    ) -> None:
        self._connector = connector
        self._dataset = dataset
        self._columns = columns
        self._boundary_column = boundary_column
        self._query_template = query_template

    def read_rows(self, request: RouteRefreshSnapshotCaptureRequest) -> Sequence[Mapping[str, object]]:
        return self._connector.get_records(
            _query(
                dataset=self._dataset,
                columns=self._columns,
                boundary_column=self._boundary_column,
                start=request.start,
                end=request.end,
                partition=request.partition,
                query_template=self._query_template,
                quote_identifier=quote_mssql_identifier,
                quote_dataset=_quoted_dataset(quote_mssql_identifier),
            ),
            as_dict=True,
        )


class ClickHouseRouteRefreshRowsReader:
    """Read bounded route refresh rows from a ClickHouse sink table."""

    def __init__(
        self,
        *,
        connector: _SqlRecordsConnector,
        dataset: str,
        columns: tuple[str, ...],
        boundary_column: str,
        query_template: str = "",
    ) -> None:
        self._connector = connector
        self._dataset = dataset
        self._columns = columns
        self._boundary_column = boundary_column
        self._query_template = query_template

    def read_rows(self, request: RouteRefreshSnapshotCaptureRequest) -> Sequence[Mapping[str, object]]:
        return self._connector.get_records(
            _query(
                dataset=self._dataset,
                columns=self._columns,
                boundary_column=self._boundary_column,
                start=request.start,
                end=request.end,
                partition=request.partition,
                query_template=self._query_template,
                quote_identifier=quote_clickhouse_identifier,
                quote_dataset=_quoted_dataset(quote_clickhouse_identifier),
            ),
            as_dict=True,
        )


def _query(
    *,
    dataset: str,
    columns: tuple[str, ...],
    boundary_column: str,
    start: str,
    end: str,
    partition: str,
    query_template: str,
    quote_identifier: _QuoteIdentifier,
    quote_dataset: _QuoteDataset,
) -> str:
    _validate_columns((*columns, boundary_column))
    column_sql = ", ".join(quote_identifier(column) for column in columns)
    boundary_sql = quote_identifier(boundary_column)
    source_table = quote_dataset(dataset)
    if query_template:
        return query_template.format(
            columns=column_sql,
            source_table=source_table,
            boundary_column=boundary_sql,
            start=_literal(start),
            end=_literal(end),
            partition=partition,
        )
    return (
        f"SELECT {column_sql}\n"
        f"FROM {source_table}\n"
        f"WHERE {boundary_sql} BETWEEN {_literal(start)} AND {_literal(end)}\n"
        f"ORDER BY {boundary_sql}"
    )


def _quoted_dataset(quote_identifier: _QuoteIdentifier) -> _QuoteDataset:
    def quote(value: str) -> str:
        first, second = split_dataset(value)
        return f"{quote_identifier(first)}.{quote_identifier(second)}"

    return quote


def _validate_columns(columns: tuple[str, ...]) -> None:
    if not columns or not all(safe_identifier(column) for column in columns):
        raise ValueError("route refresh snapshot capture received unsafe columns")


def _literal(value: str) -> str:
    text = str(value)
    if text.strip() == str(int_value(text, default=-1)):
        return text.strip()
    return "'" + text.replace("'", "''") + "'"


class _QuoteIdentifier(Protocol):
    def __call__(self, value: str) -> str: ...


class _QuoteDataset(Protocol):
    def __call__(self, value: str) -> str: ...


__all__ = [
    "ClickHouseRouteRefreshRowsReader",
    "MssqlRouteRefreshRowsReader",
    "PostgresRouteRefreshRowsReader",
]
