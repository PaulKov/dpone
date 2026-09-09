#!/usr/bin/env python3
"""Live wide-type certification for MSSQL -> ClickHouse native transfer.

The harness is deliberately thin: it prepares a disposable wide MSSQL table,
uses the existing MSSQL source and ClickHouse sink runtime, then writes evidence
that proves counts, duplicates and schema-driven typed hashes for edge types.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy
from dpone.strategy_intelligence.typed_reconciliation import TypedColumnSpec, TypedRowHashService


@dataclass(frozen=True, slots=True)
class WideTypeColumn:
    """One MSSQL fixture column and the deterministic expression used to fill it."""

    name: str
    mssql_type: str
    insert_expression: str


@dataclass(frozen=True, slots=True)
class WideTypeCertificationConfig:
    """Configuration for one wide MSSQL -> ClickHouse certification run."""

    rows: int
    column_count: int
    source_schema: str
    source_table: str
    target_database: str
    target_table: str
    output_dir: Path
    mssql_params: dict[str, Any]
    clickhouse_params: dict[str, Any]
    target_rows_per_partition: int
    export_workers: int
    load_workers: int
    batch_size: int
    prepare_chunk_size: int = 100_000
    skip_source_prepare: bool = False
    skip_transfer: bool = False
    bcp_packet_size: int = 32767
    typed_hash_rows: int = 10000
    strategy_mode: str = "full_refresh"


@dataclass(frozen=True, slots=True)
class WideTypeCertificationResult:
    """Machine-readable result written by the wide certification harness."""

    rows: int
    column_count: int
    source_count: int
    target_count: int
    duplicate_count: int
    typed_hash_passed: bool
    typed_hash_source: str | None
    typed_hash_target: str | None
    elapsed_seconds: float
    prepare_seconds: float
    export_seconds: float
    load_seconds: float
    artifact_bytes: int
    passed: bool
    failed_phase: str | None = None
    error: str | None = None
    schema_version: str = "dpone.mssql_clickhouse.wide_type_certification.v1"

    @property
    def rows_per_second(self) -> float:
        return self.target_count / self.elapsed_seconds if self.elapsed_seconds > 0 else 0.0


@dataclass(frozen=True, slots=True)
class WrittenEvidence:
    """Paths written for one certification result."""

    json_path: Path
    markdown_path: Path


class WideTypeEvidenceWriter:
    """Persist wide certification evidence in JSON and Markdown."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def write(self, result: WideTypeCertificationResult) -> WrittenEvidence:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        json_path = self._output_dir / "mssql_clickhouse_wide_type_certification.json"
        markdown_path = self._output_dir / "mssql_clickhouse_wide_type_certification.md"
        payload = {
            **asdict(result),
            "rows_per_second": result.rows_per_second,
        }
        with json_path.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
        with markdown_path.open("x", encoding="utf-8") as stream:
            stream.write(_render_markdown(result))
        return WrittenEvidence(json_path=json_path, markdown_path=markdown_path)


class _Logger:
    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:
        print(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str), flush=True)

    def info(self, message: str) -> None:
        print(message, flush=True)

    def warning(self, message: str) -> None:
        print(f"WARNING: {message}", flush=True)


def _emit_event(event: str, payload: dict[str, Any]) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str), flush=True)


def _emit_event_if_configured(
    emit_event: Callable[[str, dict[str, Any]], None] | None,
    event: str,
    payload: dict[str, Any],
) -> None:
    if emit_event is not None:
        emit_event(event, payload)


def build_wide_columns(column_count: int) -> list[WideTypeColumn]:
    """Build a deterministic MSSQL wide-table schema with required edge types."""

    base = [
        WideTypeColumn("order_id", "int NOT NULL", "CAST(gs AS int)"),
        WideTypeColumn("customer_id", "bigint", "CAST(gs % 100000 AS bigint)"),
        WideTypeColumn("amount", "decimal(18,4)", "CAST((gs % 100000) / 10000.0 AS decimal(18,4))"),
        WideTypeColumn("amount_high", "numeric(38,9)", "CAST((gs % 1000000) / 1000000000.0 AS numeric(38,9))"),
        WideTypeColumn("money_amount", "money", "CAST((gs % 10000) / 100.0 AS money)"),
        WideTypeColumn("small_money_amount", "smallmoney", "CAST((gs % 1000) / 100.0 AS smallmoney)"),
        WideTypeColumn("is_active", "bit", "CAST(gs % 2 AS bit)"),
        WideTypeColumn(
            "trace_id", "uniqueidentifier", "CONVERT(uniqueidentifier, HASHBYTES('MD5', CONVERT(varchar(32), gs)))"
        ),
        WideTypeColumn(
            "created_at", "datetime2(7)", "DATEADD(second, gs % 31536000, CONVERT(datetime2(7), '2026-01-01T00:00:00'))"
        ),
        WideTypeColumn(
            "offset_at",
            "datetimeoffset(7)",
            "TODATETIMEOFFSET(DATEADD(second, gs % 31536000, CONVERT(datetime2(7), '2026-01-01T00:00:00')), '+03:00')",
        ),
        WideTypeColumn("business_date", "date", "DATEADD(day, gs % 365, CONVERT(date, '2026-01-01'))"),
        WideTypeColumn("business_time", "time(7)", "DATEADD(second, gs % 86400, CONVERT(time(7), '00:00:00'))"),
        WideTypeColumn("payload_bin", "varbinary(16)", "HASHBYTES('MD5', CONVERT(varchar(32), gs))"),
        WideTypeColumn("fixed_payload", "binary(4)", "CONVERT(binary(4), HASHBYTES('MD5', CONVERT(varchar(32), gs)))"),
        WideTypeColumn("row_version", "rowversion", ""),
        WideTypeColumn("description", "nvarchar(200)", "N'description-' + CONVERT(nvarchar(40), gs)"),
        WideTypeColumn("unicode_text", "nvarchar(200)", "N'Привет-' + CONVERT(nvarchar(40), gs)"),
        WideTypeColumn("empty_string", "nvarchar(20)", "CASE WHEN gs % 7 = 0 THEN N'' ELSE N'not-empty' END"),
        WideTypeColumn("nullable_text", "nvarchar(20)", "CASE WHEN gs % 11 = 0 THEN NULL ELSE N'value' END"),
        WideTypeColumn("long_text", "nvarchar(max)", "REPLICATE(N'x', 128) + CONVERT(nvarchar(40), gs)"),
        WideTypeColumn("ascii_text", "varchar(200)", "'ascii-' + CONVERT(varchar(40), gs)"),
        WideTypeColumn("float_value", "float", "CAST(gs % 100000 AS float)"),
        WideTypeColumn("real_value", "real", "CAST(gs % 10000 AS real)"),
    ]
    if column_count <= len(base):
        return base[:column_count]
    return [*base, *_extra_columns(column_count - len(base))]


def build_load_config(config: WideTypeCertificationConfig) -> LoadConfig:
    """Build the same runtime config users exercise through dpone manifests."""

    return LoadConfig(
        source_conn_id="mssql-wide-type-source",
        target_conn_id="clickhouse-wide-type-sink",
        source_schema=config.source_schema,
        source_table=config.source_table,
        target_schema=config.target_database,
        target_table=config.target_table,
        staging_schema="staging",
        load_strategy=LoadStrategy(config.strategy_mode),
        batch_size=config.batch_size,
        options={
            "extract_mode": "bcp_queryout",
            "mssql_export_mode": "bcp",
            "mssql_queryout_projection": "view",
            "type_fidelity": {
                "binary_encoding": "hex",
                "time_encoding": "seconds_since_midnight",
                "temporal": {"offset_timestamp": {"mode": "utc_instant", "timezone": "UTC"}},
            },
            "native_transfer": {"optimizer_profile": "high_throughput_safe"},
            "partition_tmp_dir": str(config.output_dir / "partition_files"),
            "bulk": {
                "mode": "bcp",
                "bcp": {
                    "bcp_path": str(config.mssql_params.get("bcp_path") or "bcp"),
                    "batch_size": config.batch_size,
                    "packet_size": config.bcp_packet_size,
                    "timeout_seconds": 3600,
                },
            },
            "partitioning": {
                "strategy": "auto",
                "column": "order_id",
                "bounds": "auto",
                "target_rows_per_partition": config.target_rows_per_partition,
                "max_partitions": 256,
                "export_workers": config.export_workers,
                "load_workers": config.load_workers,
            },
            "clickhouse_bulk": {
                "mode": "http",
                "http": {
                    "host": config.clickhouse_params.get("http_host") or config.clickhouse_params.get("host"),
                    "port": int(config.clickhouse_params.get("http_port", 8123)),
                    "database": config.target_database,
                    "user": config.clickhouse_params.get("username") or config.clickhouse_params.get("user", "default"),
                    "password": config.clickhouse_params.get("password", ""),
                },
                "insert_settings": {
                    "async_insert": 1,
                    "wait_for_async_insert": 1,
                    "max_insert_block_size": 1_000_000,
                    "input_format_parallel_parsing": 1,
                },
            },
        },
    )


def run_live_certification(config: WideTypeCertificationConfig) -> WideTypeCertificationResult:
    """Prepare a wide source, run native transfer and write certification evidence."""

    mssql = _mssql_connector(config)
    clickhouse = _clickhouse_connector(config)
    logger = _Logger()
    started = time.perf_counter()
    phase = "prepare_source"
    prepare_seconds = 0.0
    export_seconds = 0.0
    load_seconds = 0.0
    artifact_bytes = 0
    try:
        prepare_started = time.perf_counter()
        if config.skip_source_prepare:
            _emit_event(
                "MSSQL_WIDE_PREPARE_SOURCE_SKIPPED",
                {"Schema": config.source_schema, "Table": config.source_table},
            )
            prepare_seconds = 0.0
        else:
            _prepare_mssql_source(mssql, config, emit_event=_emit_event)
            prepare_seconds = time.perf_counter() - prepare_started
        if config.skip_transfer:
            _emit_event(
                "MSSQL_WIDE_TRANSFER_SKIPPED",
                {"Source": f"{config.source_schema}.{config.source_table}", "Target": config.target_table},
            )
        else:
            phase = "prepare_target"
            _prepare_clickhouse_target(clickhouse, config)
            load_config = build_load_config(config)
            (config.output_dir / "partition_files").mkdir(parents=True, exist_ok=True)
            source = MSSQLFullExtractStrategy(mssql, logger, sink_connector=clickhouse)
            sink = ClickHouseSink(clickhouse, logger=logger)
            phase = "source_export"
            extract_started = time.perf_counter()
            extract = source.extract(load_config, None)
            export_seconds = time.perf_counter() - extract_started
            artifact_bytes = _artifact_bytes(extract.artifact)
            phase = "target_load"
            load_started = time.perf_counter()
            sink.load(load_config, LoadPayload(artifact=extract.artifact, schema=extract.schema))
            load_seconds = time.perf_counter() - load_started
            phase = "reconnect_for_reconciliation"
            _close_quietly(mssql)
            _close_quietly(clickhouse)
            mssql = _mssql_connector(config)
            clickhouse = _clickhouse_connector(config)
        phase = "reconciliation"
        source_count = _count_mssql(mssql, config.source_schema, config.source_table)
        target_count = _count_clickhouse(clickhouse, config.target_database, config.target_table)
        duplicate_count = _count_clickhouse_duplicates(clickhouse, config.target_database, config.target_table)
        source_hash, target_hash = _typed_hashes(mssql, clickhouse, config)
        result = WideTypeCertificationResult(
            rows=config.rows,
            column_count=len(build_wide_columns(config.column_count)),
            source_count=source_count,
            target_count=target_count,
            duplicate_count=duplicate_count,
            typed_hash_passed=source_hash == target_hash,
            typed_hash_source=source_hash,
            typed_hash_target=target_hash,
            elapsed_seconds=time.perf_counter() - started,
            prepare_seconds=prepare_seconds,
            export_seconds=export_seconds,
            load_seconds=load_seconds,
            artifact_bytes=artifact_bytes,
            passed=source_count == target_count == config.rows and duplicate_count == 0 and source_hash == target_hash,
        )
        WideTypeEvidenceWriter(config.output_dir).write(result)
        return result
    except Exception as exc:
        result = WideTypeCertificationResult(
            rows=config.rows,
            column_count=len(build_wide_columns(config.column_count)),
            source_count=_safe_count_mssql(mssql, config.source_schema, config.source_table),
            target_count=_safe_count_clickhouse(clickhouse, config.target_database, config.target_table),
            duplicate_count=_safe_count_clickhouse_duplicates(clickhouse, config.target_database, config.target_table),
            typed_hash_passed=False,
            typed_hash_source=None,
            typed_hash_target=None,
            elapsed_seconds=time.perf_counter() - started,
            prepare_seconds=prepare_seconds,
            export_seconds=export_seconds,
            load_seconds=load_seconds,
            artifact_bytes=artifact_bytes,
            passed=False,
            failed_phase=phase,
            error=_error_text(exc),
        )
        WideTypeEvidenceWriter(config.output_dir).write(result)
        raise
    finally:
        _close_quietly(clickhouse)
        _close_quietly(mssql)


def build_config(args: argparse.Namespace) -> WideTypeCertificationConfig:
    suffix = args.suffix or uuid.uuid4().hex[:8]
    output_dir = Path(args.output_dir).expanduser().resolve()
    return WideTypeCertificationConfig(
        rows=args.rows,
        column_count=args.column_count,
        source_schema=args.source_schema or f"wide_type_{suffix}",
        source_table=args.source_table,
        target_database=args.clickhouse_database,
        target_table=args.target_table or f"wide_type_{suffix}",
        output_dir=output_dir,
        mssql_params=_mssql_params(args),
        clickhouse_params=_clickhouse_params(args),
        target_rows_per_partition=args.target_rows_per_partition,
        export_workers=args.export_workers,
        load_workers=args.load_workers,
        batch_size=args.batch_size,
        prepare_chunk_size=args.prepare_chunk_size,
        skip_source_prepare=args.skip_source_prepare,
        skip_transfer=args.skip_transfer,
        bcp_packet_size=args.bcp_packet_size,
        typed_hash_rows=args.typed_hash_rows,
        strategy_mode=args.strategy_mode,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--column-count", type=int, default=120)
    parser.add_argument("--typed-hash-rows", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--prepare-chunk-size", type=int, default=100000)
    parser.add_argument("--skip-source-prepare", action="store_true")
    parser.add_argument("--skip-transfer", action="store_true")
    parser.add_argument("--bcp-packet-size", type=int, default=32767)
    parser.add_argument("--target-rows-per-partition", type=int, default=250000)
    parser.add_argument("--export-workers", type=int, default=4)
    parser.add_argument("--load-workers", type=int, default=4)
    parser.add_argument(
        "--strategy-mode", default="full_refresh", choices=[strategy.value for strategy in LoadStrategy]
    )
    parser.add_argument("--output-dir", default="test_artifacts/live_certification/benchmarks/wide_type_latest")
    parser.add_argument("--suffix")
    parser.add_argument("--source-schema")
    parser.add_argument("--source-table", default="orders")
    parser.add_argument("--target-table")
    parser.add_argument("--mssql-host", default=os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"))
    parser.add_argument("--mssql-port", type=int, default=int(os.getenv("DPONE_IT_MSSQL_PORT", "1433")))
    parser.add_argument("--mssql-database", default=os.getenv("DPONE_IT_MSSQL_DATABASE", "master"))
    parser.add_argument("--mssql-user", default=os.getenv("DPONE_IT_MSSQL_USER", "sa"))
    parser.add_argument("--mssql-password", default=os.getenv("DPONE_IT_MSSQL_PASSWORD", ""))
    parser.add_argument("--mssql-driver", default=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"))
    parser.add_argument("--mssql-bcp-path", default=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"))
    parser.add_argument("--clickhouse-host", default=os.getenv("DPONE_IT_CH_HOST", "127.0.0.1"))
    parser.add_argument("--clickhouse-port", type=int, default=int(os.getenv("DPONE_IT_CH_PORT", "9000")))
    parser.add_argument("--clickhouse-http-port", type=int, default=int(os.getenv("DPONE_IT_CH_HTTP_PORT", "8123")))
    parser.add_argument("--clickhouse-database", default=os.getenv("DPONE_IT_CH_DATABASE", "default"))
    parser.add_argument("--clickhouse-user", default=os.getenv("DPONE_IT_CH_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("DPONE_IT_CH_PASSWORD", ""))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    result = run_live_certification(build_config(parse_args(argv)))
    print(_render_markdown(result))
    return 0 if result.passed else 1


def _extra_columns(count: int) -> list[WideTypeColumn]:
    templates = (
        ("wide_int", "int", "CAST((gs + {i}) % 2147483647 AS int)"),
        ("wide_decimal", "decimal(18,4)", "CAST(((gs + {i}) % 100000) / 10000.0 AS decimal(18,4))"),
        (
            "wide_text",
            "nvarchar(80)",
            "CASE WHEN (gs + {i}) % 17 = 0 THEN NULL ELSE N'wide-{i}-' + CONVERT(nvarchar(40), gs) END",
        ),
        ("wide_date", "date", "DATEADD(day, (gs + {i}) % 365, CONVERT(date, '2026-01-01'))"),
        ("wide_time", "time(7)", "DATEADD(second, (gs + {i}) % 86400, CONVERT(time(7), '00:00:00'))"),
        ("wide_binary", "varbinary(16)", "HASHBYTES('MD5', CONVERT(varchar(32), gs + {i}))"),
        ("wide_bit", "bit", "CAST((gs + {i}) % 2 AS bit)"),
        (
            "wide_datetime",
            "datetime2(7)",
            "DATEADD(second, (gs + {i}) % 31536000, CONVERT(datetime2(7), '2026-01-01T00:00:00'))",
        ),
    )
    columns: list[WideTypeColumn] = []
    for index in range(count):
        prefix, mssql_type, expression = templates[index % len(templates)]
        ordinal = index + 1
        columns.append(
            WideTypeColumn(
                name=f"{prefix}_{ordinal:03d}",
                mssql_type=mssql_type,
                insert_expression=expression.format(i=ordinal),
            )
        )
    return columns


def _prepare_mssql_source(
    connector: MSSQLConnector,
    config: WideTypeCertificationConfig,
    *,
    emit_event: Callable[[str, dict[str, Any]], None] | None = None,
) -> None:
    schema = _mssql_ident(config.source_schema)
    table = _mssql_ident(config.source_table)
    columns = build_wide_columns(config.column_count)
    _emit_event_if_configured(
        emit_event,
        "MSSQL_WIDE_PREPARE_SOURCE_START",
        {"Rows": config.rows, "Columns": len(columns), "Chunk_Size": config.prepare_chunk_size},
    )
    connector.execute_query(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{config.source_schema}') EXEC(N'CREATE SCHEMA {schema}')"
    )
    connector.execute_query(f"DROP TABLE IF EXISTS {schema}.{table}")
    ddl_columns = ",\n            ".join(_ddl_column(column) for column in columns)
    connector.execute_query(
        f"CREATE TABLE {schema}.{table} (\n            {ddl_columns},\n            PRIMARY KEY ([order_id])\n        )"
    )
    insert_columns = [column for column in columns if column.insert_expression]
    insert_sql = ", ".join(_mssql_ident(column.name) for column in insert_columns)
    select_sql = ", ".join(f"{column.insert_expression} AS {_mssql_ident(column.name)}" for column in insert_columns)
    chunk_size = max(1, int(config.prepare_chunk_size or config.rows))
    inserted = 0
    while inserted < config.rows:
        current_chunk = min(chunk_size, config.rows - inserted)
        connector.execute_query(
            f"""
            WITH digits(n) AS (
                SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(n)
            ),
            source_rows AS (
                SELECT TOP ({int(current_chunk)})
                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {int(inserted)} AS gs
                FROM digits AS a
                CROSS JOIN digits AS b
                CROSS JOIN digits AS c
                CROSS JOIN digits AS d
                CROSS JOIN digits AS e
                CROSS JOIN digits AS f
                CROSS JOIN digits AS g
            )
            INSERT INTO {schema}.{table} ({insert_sql})
            SELECT {select_sql}
            FROM source_rows
            ORDER BY gs
            """
        )
        inserted += current_chunk
        _emit_event_if_configured(
            emit_event,
            "MSSQL_WIDE_PREPARE_SOURCE_CHUNK",
            {"Inserted": inserted, "Rows": config.rows, "Chunk_Size": current_chunk},
        )
    _emit_event_if_configured(emit_event, "MSSQL_WIDE_PREPARE_SOURCE_COMPLETE", {"Rows": config.rows})


def _prepare_clickhouse_target(connector: ClickHouseConnector, config: WideTypeCertificationConfig) -> None:
    connector.execute_query(f"CREATE DATABASE IF NOT EXISTS {_ch_ident(config.target_database)}")
    connector.execute_query(
        f"DROP TABLE IF EXISTS {_ch_ident(config.target_database)}.{_ch_ident(config.target_table)}"
    )


def _typed_hashes(
    mssql: MSSQLConnector,
    clickhouse: ClickHouseConnector,
    config: WideTypeCertificationConfig,
) -> tuple[str | None, str | None]:
    limit = min(config.typed_hash_rows, config.rows)
    if limit <= 0:
        return None, None
    source_schema = _source_schema_for_typed_hash(mssql, config)
    policy = MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
    source_rows = _mssql_typed_rows(mssql, config, source_schema, limit)
    target_rows = _clickhouse_typed_rows(clickhouse, config, source_schema, limit)
    if len(source_rows) != limit or len(target_rows) != limit:
        raise RuntimeError("wide_type_typed_hash_row_count_mismatch")
    service = TypedRowHashService(
        tuple(TypedColumnSpec(name, source_type) for name, source_type in source_schema), policy=policy
    )
    return service.hash_rows(source_rows), service.hash_rows(target_rows)


def _source_schema_for_typed_hash(
    connector: MSSQLConnector,
    config: WideTypeCertificationConfig,
) -> list[tuple[str, str]]:
    return [
        (column, source_type)
        for column, source_type in connector.fetch_schema(config.source_schema, config.source_table)
    ]


def _mssql_typed_rows(
    connector: MSSQLConnector,
    config: WideTypeCertificationConfig,
    source_schema: Sequence[tuple[str, str]],
    limit: int,
) -> list[tuple[Any, ...]]:
    columns_sql = ", ".join(_mssql_hash_select_expression(column, source_type) for column, source_type in source_schema)
    return connector.get_records(
        f"""
        SELECT TOP ({int(limit)}) {columns_sql}
        FROM {connector.qualified_name(config.source_schema, config.source_table)}
        ORDER BY [order_id]
        """
    )


def _mssql_hash_select_expression(column: str, source_type: str) -> str:
    quoted = "[" + column.replace("]", "]]") + "]"
    normalized = source_type.strip().lower()
    if normalized.startswith("datetimeoffset"):
        return (
            "CONVERT(VARCHAR(MAX), "
            f"CAST(SWITCHOFFSET(CAST({quoted} AS datetimeoffset), '+00:00') AS datetime2(7)), "
            f"121) AS {quoted}"
        )
    base = normalized.split("(", 1)[0].replace(" nullable", "").strip()
    if base in {"datetime", "datetime2", "smalldatetime"}:
        return f"CONVERT(VARCHAR(33), {quoted}, 121) AS {quoted}"
    return quoted


def _clickhouse_typed_rows(
    connector: ClickHouseConnector,
    config: WideTypeCertificationConfig,
    source_schema: Sequence[tuple[str, str]],
    limit: int,
) -> list[tuple[Any, ...]]:
    binary_is_raw = str(getattr(config, "target_binary_representation", "encoded")).lower() == "raw"
    columns_sql = ", ".join(
        _clickhouse_hash_select_expression(column, source_type, binary_is_raw=binary_is_raw)
        for column, source_type in source_schema
    )
    return connector.get_records(
        f"""
        SELECT {columns_sql}
        FROM {_ch_ident(config.target_database)}.{_ch_ident(config.target_table)}
        ORDER BY order_id
        LIMIT {int(limit)}
        """
    )


def _clickhouse_hash_select_expression(
    column: str,
    source_type: str,
    *,
    binary_is_raw: bool = False,
) -> str:
    quoted = _ch_ident(column)
    normalized = source_type.strip().lower()
    base = normalized.split("(", 1)[0].replace(" nullable", "").strip()
    if binary_is_raw and base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return f"hex({quoted}) AS {quoted}"
    if base in {"datetime", "datetime2", "datetimeoffset", "smalldatetime"}:
        return f"toString({quoted}) AS {quoted}"
    return quoted


def _count_mssql(connector: MSSQLConnector, schema: str, table: str) -> int:
    rows = connector.get_records(f"SELECT COUNT_BIG(*) FROM {connector.qualified_name(schema, table)}")
    return int(rows[0][0]) if rows else 0


def _safe_count_mssql(connector: MSSQLConnector, schema: str, table: str) -> int:
    try:
        return _count_mssql(connector, schema, table)
    except Exception:
        return 0


def _count_clickhouse(connector: ClickHouseConnector, database: str, table: str) -> int:
    rows = connector.get_records(f"SELECT count() FROM {_ch_ident(database)}.{_ch_ident(table)}")
    return int(rows[0][0]) if rows else 0


def _safe_count_clickhouse(connector: ClickHouseConnector, database: str, table: str) -> int:
    try:
        return _count_clickhouse(connector, database, table)
    except Exception:
        return 0


def _count_clickhouse_duplicates(connector: ClickHouseConnector, database: str, table: str) -> int:
    rows = connector.get_records(f"SELECT count() - uniqExact(order_id) FROM {_ch_ident(database)}.{_ch_ident(table)}")
    return int(rows[0][0]) if rows else 0


def _safe_count_clickhouse_duplicates(connector: ClickHouseConnector, database: str, table: str) -> int:
    try:
        return _count_clickhouse_duplicates(connector, database, table)
    except Exception:
        return 0


def _error_text(exc: Exception, *, max_chars: int = 4000) -> str:
    text = f"{exc.__class__.__name__}: {exc}"
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "... [truncated]"


def _artifact_bytes(artifact: Any) -> int:
    partitions = list(getattr(artifact, "partitions", None) or [])
    if partitions:
        return sum(_file_size(getattr(partition, "file_path", None)) for partition in partitions)
    return _file_size(getattr(artifact, "file_path", None))


def _file_size(path: str | None) -> int:
    if not path:
        return 0
    file_path = Path(path)
    return file_path.stat().st_size if file_path.exists() else 0


def _ddl_column(column: WideTypeColumn) -> str:
    if column.name == "order_id":
        return f"{_mssql_ident(column.name)} {column.mssql_type}"
    return (
        f"{_mssql_ident(column.name)} {column.mssql_type} NULL"
        if column.mssql_type != "rowversion"
        else f"{_mssql_ident(column.name)} rowversion"
    )


def _render_markdown(result: WideTypeCertificationResult) -> str:
    return f"""# MSSQL -> ClickHouse wide type certification

| Metric | Value |
|---|---:|
| Passed | `{str(result.passed).lower()}` |
| Rows | {result.rows} |
| Columns | {result.column_count} |
| Source count | {result.source_count} |
| Target count | {result.target_count} |
| Duplicate keys | {result.duplicate_count} |
| Typed hash passed | `{str(result.typed_hash_passed).lower()}` |
| Rows/sec | {result.rows_per_second:.2f} |
| Prepare seconds | {result.prepare_seconds:.3f} |
| Export seconds | {result.export_seconds:.3f} |
| Load seconds | {result.load_seconds:.3f} |
| Artifact bytes | {result.artifact_bytes} |
| Failed phase | `{result.failed_phase or ""}` |

Source typed hash: `{result.typed_hash_source}`

Target typed hash: `{result.typed_hash_target}`

Error: `{result.error or ""}`
"""


def _mssql_params(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "host": args.mssql_host,
        "port": args.mssql_port,
        "database": args.mssql_database,
        "username": args.mssql_user,
        "password": args.mssql_password,
        "driver": args.mssql_driver,
        "trust_server_certificate": "yes",
        "encrypt": "yes",
        "bcp_path": args.mssql_bcp_path,
    }


def _clickhouse_params(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "host": args.clickhouse_host,
        "port": args.clickhouse_port,
        "http_host": args.clickhouse_host,
        "http_port": args.clickhouse_http_port,
        "database": args.clickhouse_database,
        "username": args.clickhouse_user,
        "user": args.clickhouse_user,
        "password": args.clickhouse_password,
        "secure": False,
        "compression": True,
    }


def _mssql_connector(config: WideTypeCertificationConfig) -> MSSQLConnector:
    params = config.mssql_params
    return MSSQLConnector(
        host=str(params["host"]),
        port=int(params.get("port") or 1433),
        database=str(params.get("database") or "master"),
        user=params.get("username"),
        password=params.get("password"),
        driver=str(params.get("driver") or "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=params.get("trust_server_certificate", "yes"),
        encrypt=params.get("encrypt", "yes"),
        bcp_path=str(params.get("bcp_path") or "bcp"),
        query_timeout=0,
    )


def _clickhouse_connector(config: WideTypeCertificationConfig) -> ClickHouseConnector:
    params = config.clickhouse_params
    return ClickHouseConnector(
        host=str(params.get("host") or "127.0.0.1"),
        port=int(params.get("port") or 9000),
        database=str(params.get("database") or config.target_database),
        user=str(params.get("username") or params.get("user") or "default"),
        password=str(params.get("password") or ""),
        secure=bool(params.get("secure", False)),
        compression=bool(params.get("compression", True)),
    )


def _mssql_ident(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def _ch_ident(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _close_quietly(connector: Any) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()


if __name__ == "__main__":
    raise SystemExit(main())
