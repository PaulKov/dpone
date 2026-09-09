"""Native adapters for the Postgres -> MSSQL refresh executor."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.mssql_bulk import BcpCredentials, BcpOptions, BcpRunner

from .native_pipeline import NativeChunkExportResult, NativeChunkLoadResult, NativeChunkPrepareResult, line_count, tail
from .postgres_mssql_config import bool_value, int_value, mapping, optional_int, quote_mssql_identifier


class MssqlStatementRunner(Protocol):
    """Minimal SQL execution port used by the target-window prepare step."""

    def execute_query(self, query: object, params: object | None = None) -> int: ...


class PostgresCopyChunkExporter:
    """Postgres exporter backed by ``COPY (...) TO STDOUT``."""

    def __init__(self, connector: object) -> None:
        self._connector = connector

    def export_chunk(self, *, query: str, output_path: Path) -> NativeChunkExportResult:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        stats = self._connector.copy_to_file(
            query_sql=query,
            output_path=str(output_path),
            format="MSSQL_DELIMITED",
            compress=False,
        )
        return NativeChunkExportResult(
            rows_read=line_count(output_path),
            redacted_command=("postgres-copy", "COPY query TO STDOUT"),
            stdout_tail=tail(str(stats)),
            stderr_tail="",
        )


class MssqlBcpChunkLoader:
    """MSSQL loader backed by bounded DELETE and ``bcp in``."""

    def __init__(self, *, statement_runner: MssqlStatementRunner, bcp_runner: BcpRunner) -> None:
        self._statement_runner = statement_runner
        self._bcp_runner = bcp_runner

    def prepare_chunk(
        self,
        *,
        table: str,
        boundary_column: str,
        start: str,
        end: str,
        idempotency_key: str,
    ) -> NativeChunkPrepareResult:
        del idempotency_key
        start_int = int_value(start, default=0)
        end_int = int_value(end, default=0)
        query = f"DELETE FROM {table} WHERE {quote_mssql_identifier(boundary_column)} BETWEEN {start_int} AND {end_int}"
        rows_deleted = self._statement_runner.execute_query(query)
        return NativeChunkPrepareResult(
            rows_deleted=rows_deleted,
            redacted_command=("mssql", "DELETE", table, quote_mssql_identifier(boundary_column)),
        )

    def load_chunk(
        self,
        *,
        table: str,
        columns: tuple[str, ...],
        input_path: Path,
        idempotency_key: str,
    ) -> NativeChunkLoadResult:
        del columns, idempotency_key
        result = self._bcp_runner.import_file(table, str(input_path))
        return NativeChunkLoadResult(
            rows_written=int(result.rows_copied or line_count(input_path)),
            redacted_command=tuple(result.redacted_command),
            stdout_tail=tail(result.stdout),
            stderr_tail=tail(result.stderr),
        )


def build_postgres_connector(payload: Mapping[str, object]):
    from dpone.runtime.connectors.postgres import PostgresConnector

    return PostgresConnector(
        host=str(payload.get("host", "")),
        port=int_value(payload.get("port"), default=5432),
        database=str(payload.get("database", "")),
        user=str(payload.get("user", "")),
        password=str(payload.get("password", "")),
        application_name=str(payload.get("application_name", "dpone-postgres-mssql-refresh")),
    )


def build_mssql_connector(payload: Mapping[str, object]) -> MSSQLConnector:
    return MSSQLConnector(
        host=str(payload.get("host", "")),
        port=int_value(payload.get("port"), default=1433),
        database=str(payload.get("database", "")),
        user=str(payload.get("user", "")) or None,
        password=str(payload.get("password", "")) or None,
        driver=str(payload.get("driver", "ODBC Driver 18 for SQL Server")),
        trust_server_certificate=bool_value(payload.get("trust_server_certificate"), default=True),
        bcp_path=str(mapping(payload.get("options")).get("bcp_path", "bcp")),
    )


def build_bcp_runner(payload: Mapping[str, object]) -> BcpRunner:
    return BcpRunner(_bcp_credentials(payload), _bcp_options(payload))


def _bcp_credentials(payload: Mapping[str, object]) -> BcpCredentials:
    return BcpCredentials(
        host=str(payload.get("host", "")),
        port=int_value(payload.get("port"), default=1433),
        database=str(payload.get("database", "")),
        user=str(payload.get("user", "")) or None,
        password=str(payload.get("password", "")) or None,
        trusted_connection=bool_value(payload.get("trusted_connection"), default=False),
    )


def _bcp_options(payload: Mapping[str, object]) -> BcpOptions:
    options = mapping(payload.get("options"))
    return BcpOptions(
        bcp_path=str(options.get("bcp_path", "bcp")),
        batch_size=int_value(options.get("batch_size"), default=100_000),
        packet_size=int_value(options.get("packet_size"), default=16_384),
        timeout_seconds=optional_int(options.get("timeout_seconds")),
        trust_server_certificate=bool_value(options.get("trust_server_certificate"), default=True),
        password_transport=str(options.get("password_transport", "stdin")),
    )


__all__ = [
    "MssqlBcpChunkLoader",
    "MssqlStatementRunner",
    "PostgresCopyChunkExporter",
    "build_bcp_runner",
    "build_mssql_connector",
    "build_postgres_connector",
]
