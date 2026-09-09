"""Native transfer adapters for the MSSQL -> ClickHouse refresh executor."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.runtime.connectors.clickhouse_bulk import (
    ClickHouseClientCredentials,
    ClickHouseClientOptions,
    ClickHouseClientRunner,
)
from dpone.runtime.connectors.mssql_bulk import BcpCredentials, BcpOptions, BcpRunner

from .mssql_clickhouse_config import (
    bool_value,
    int_value,
    mapping,
    optional_int,
    quote_clickhouse_identifier,
    split_dataset,
)

TAIL_LIMIT = 4000


@dataclass(frozen=True, slots=True)
class MssqlChunkExportResult:
    """Result returned by an MSSQL chunk exporter."""

    rows_read: int
    redacted_command: tuple[str, ...] = ()
    stdout_tail: str = ""
    stderr_tail: str = ""


@dataclass(frozen=True, slots=True)
class ClickHouseChunkLoadResult:
    """Result returned by a ClickHouse chunk loader."""

    rows_written: int
    redacted_command: tuple[str, ...] = ()
    stdout_tail: str = ""
    stderr_tail: str = ""


@dataclass(frozen=True, slots=True)
class ClickHouseChunkPrepareResult:
    """Result returned by a ClickHouse chunk cleanup step."""

    rows_deleted: int
    redacted_command: tuple[str, ...] = ()
    stdout_tail: str = ""
    stderr_tail: str = ""


class MssqlChunkExporter(Protocol):
    """Export one bounded MSSQL chunk to a local transfer file."""

    def export_chunk(self, *, query: str, output_path: Path) -> object: ...


class ClickHouseChunkLoader(Protocol):
    """Prepare a bounded target window and load one transfer file."""

    def prepare_chunk(
        self, *, table: str, boundary_column: str, start: str, end: str, idempotency_key: str
    ) -> object: ...

    def load_chunk(
        self,
        *,
        table: str,
        columns: tuple[str, ...],
        input_path: Path,
        idempotency_key: str,
    ) -> object: ...


class MssqlBcpChunkExporter:
    """MSSQL exporter backed by ``bcp queryout``."""

    def __init__(self, runner: BcpRunner) -> None:
        self._runner = runner

    def export_chunk(self, *, query: str, output_path: Path) -> MssqlChunkExportResult:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result = self._runner.queryout(query, str(output_path))
        return MssqlChunkExportResult(
            rows_read=int(result.rows_copied or line_count(output_path)),
            redacted_command=tuple(result.redacted_command),
            stdout_tail=tail(result.stdout),
            stderr_tail=tail(result.stderr),
        )


class ClickHouseClientChunkLoader:
    """ClickHouse loader backed by ``clickhouse-client``."""

    def __init__(self, runner: ClickHouseClientRunner) -> None:
        self._runner = runner

    def prepare_chunk(
        self, *, table: str, boundary_column: str, start: str, end: str, idempotency_key: str
    ) -> ClickHouseChunkPrepareResult:
        del idempotency_key
        start_int = int_value(start, default=0)
        end_int = int_value(end, default=0)
        column = quote_clickhouse_identifier(boundary_column)
        query = (
            f"ALTER TABLE {table} DELETE WHERE {column} BETWEEN {start_int} AND {end_int} SETTINGS mutations_sync = 2"
        )
        result = self._runner.execute_query(query)
        return ClickHouseChunkPrepareResult(
            rows_deleted=0,
            redacted_command=tuple(result.redacted_command),
            stdout_tail=tail(result.stdout),
            stderr_tail=tail(result.stderr),
        )

    def load_chunk(
        self,
        *,
        table: str,
        columns: tuple[str, ...],
        input_path: Path,
        idempotency_key: str,
    ) -> ClickHouseChunkLoadResult:
        del idempotency_key
        result = self._runner.insert_file(table, columns, str(input_path))
        return ClickHouseChunkLoadResult(
            rows_written=line_count(input_path),
            redacted_command=tuple(result.redacted_command),
            stdout_tail=tail(result.stdout),
            stderr_tail=tail(result.stderr),
        )


def build_bcp_runner(payload: Mapping[str, object]) -> BcpRunner:
    return BcpRunner(_bcp_credentials(payload), _bcp_options(payload))


def build_clickhouse_runner(payload: Mapping[str, object], target_dataset: str) -> ClickHouseClientRunner:
    return ClickHouseClientRunner(
        _clickhouse_credentials(payload, target_dataset),
        _clickhouse_options(payload),
    )


def line_count(path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def tail(value: str) -> str:
    return value[-TAIL_LIMIT:]


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
        trust_server_certificate=bool_value(options.get("trust_server_certificate"), default=False),
        password_transport=str(options.get("password_transport", "stdin")),
    )


def _clickhouse_credentials(payload: Mapping[str, object], target_dataset: str) -> ClickHouseClientCredentials:
    database, _ = split_dataset(target_dataset)
    return ClickHouseClientCredentials(
        host=str(payload.get("host", "")),
        port=int_value(payload.get("port"), default=9000),
        database=str(payload.get("database", database)),
        user=str(payload.get("user", "default")),
        password=str(payload.get("password", "")),
        secure=bool_value(payload.get("secure"), default=False),
    )


def _clickhouse_options(payload: Mapping[str, object]) -> ClickHouseClientOptions:
    options = mapping(payload.get("options"))
    settings = mapping(options.get("settings"))
    return ClickHouseClientOptions(
        client_command=str(options.get("client_command", "clickhouse-client")),
        input_format=str(options.get("input_format", "TabSeparated")),
        timeout_seconds=optional_int(options.get("timeout_seconds")),
        max_insert_block_size=optional_int(options.get("max_insert_block_size")),
        settings=dict(settings),
    )


__all__ = [
    "ClickHouseChunkLoadResult",
    "ClickHouseChunkLoader",
    "ClickHouseChunkPrepareResult",
    "ClickHouseClientChunkLoader",
    "MssqlBcpChunkExporter",
    "MssqlChunkExportResult",
    "MssqlChunkExporter",
    "build_bcp_runner",
    "build_clickhouse_runner",
]
