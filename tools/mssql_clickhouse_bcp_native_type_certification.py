#!/usr/bin/env python3
"""Live BCP-native MSSQL -> ClickHouse type certification."""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.decision_audit import RuntimeDecision, RuntimeDecisionContext
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import mssql_clickhouse_wide_type_certification as base  # noqa: E402
from local_route_certification_receipt import (  # noqa: E402
    LocalSourceSnapshot,
    capture_local_source_snapshot,
    require_exact_upstream_evidence,
)
from local_route_certification_validation import prepare_create_once_directory  # noqa: E402
from mssql_clickhouse_bcp_native_config import (  # noqa: E402
    add_connection_args,
    clickhouse_http_options,
    native_transfer_options,
)
from mssql_clickhouse_bcp_native_evidence import (  # noqa: E402
    BcpDecisionCollector,
    assert_acceleration_decisions,
    loaded_slice_closure,
    write_bcp_route_receipt,
)
from mssql_clickhouse_bcp_native_fixtures import build_bcp_native_columns  # noqa: E402
from mssql_clickhouse_target_schema import (  # noqa: E402
    ClickHouseTargetSchemaMetrics,
    clickhouse_target_schema_metrics,
)
from mssql_dbt_wide_evidence import mssql_connection_sha256  # noqa: E402


@dataclass(frozen=True, slots=True)
class BcpNativeCertificationConfig:
    """Configuration for one BCP-native live certification run."""

    rows: int
    release_id: str
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
    binary_format: str = "rowbinary"
    prepare_chunk_size: int = 100_000
    typed_hash_rows: int = 10_000
    bcp_packet_size: int = 32_767
    acceleration_mode: str = "auto"
    skip_source_prepare: bool = False
    upstream_evidence: Path | None = None


def build_load_config(config: BcpNativeCertificationConfig) -> LoadConfig:
    """Build the v0.26 BCP-native runtime manifest in object form."""

    return LoadConfig(
        source_conn_id="mssql-bcp-native-source",
        target_conn_id="clickhouse-bcp-native-sink",
        source_schema=config.source_schema,
        source_table=config.source_table,
        target_schema=config.target_database,
        target_table=config.target_table,
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=config.batch_size,
        options={
            "extract_mode": "bcp_queryout",
            "mssql_export_mode": "bcp",
            "type_fidelity": {"binary_encoding": "hex", "time_encoding": "seconds_since_midnight"},
            "native_transfer": native_transfer_options(config.binary_format, config.acceleration_mode),
            "partition_tmp_dir": str(config.output_dir / "partition_files"),
            "bulk": {
                "mode": "bcp",
                "bcp": {
                    "bcp_path": str(config.mssql_params.get("bcp_path") or "bcp"),
                    "file_format": "native",
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
                "ingest_contract": "typed_binary_staging",
                "http": clickhouse_http_options(config),
            },
        },
    )


def run_live_certification(config: BcpNativeCertificationConfig) -> base.WideTypeCertificationResult:
    """Run BCP-native transfer and write wide type certification evidence."""

    source_snapshot = capture_local_source_snapshot()
    if not config.mssql_params.get("password") or not config.clickhouse_params.get("password"):
        raise ValueError("local_certification_connection_secrets_required")
    if config.typed_hash_rows != config.rows:
        raise ValueError("wide_release_requires_full_typed_hash")
    if config.skip_source_prepare and config.upstream_evidence is None:
        raise ValueError("skip_source_prepare_requires_upstream_evidence")
    if config.upstream_evidence is not None:
        require_exact_upstream_evidence(
            config.upstream_evidence,
            source_snapshot=source_snapshot,
            output_relation=f"{config.source_schema}.{config.source_table}",
            connection_sha256=mssql_connection_sha256(config.mssql_params),
            release_id=config.release_id,
            expected_rows=config.rows,
            expected_source_columns=config.column_count - 1,
            expected_target_columns=config.column_count,
        )
    prepare_create_once_directory(config.output_dir)
    base_config = cast(base.WideTypeCertificationConfig, config)
    mssql = base._mssql_connector(base_config)
    clickhouse = base._clickhouse_connector(base_config)
    collector = BcpDecisionCollector(decisions=[])
    started = time.perf_counter()
    phase = "prepare_source"
    prepare_seconds = export_seconds = load_seconds = 0.0
    extract_artifact: Any | None = None
    try:
        prep_started = time.perf_counter()
        if config.skip_source_prepare:
            base._emit_event(
                "MSSQL_BCP_NATIVE_PREPARE_SOURCE_SKIPPED",
                {"Schema": config.source_schema, "Table": config.source_table},
            )
        else:
            _prepare_source(mssql, config, emit_event=base._emit_event)
            prepare_seconds = time.perf_counter() - prep_started
        phase = "prepare_target"
        base._prepare_clickhouse_target(clickhouse, base_config)
        load_config = build_load_config(config)
        (config.output_dir / "partition_files").mkdir(parents=True, exist_ok=True)
        phase = "source_export"
        export_started = time.perf_counter()
        extract = MSSQLFullExtractStrategy(mssql, base._Logger(), sink_connector=clickhouse).extract(load_config, None)
        extract_artifact = extract.artifact
        export_seconds = time.perf_counter() - export_started
        phase = "target_load"
        load_started = time.perf_counter()
        with RuntimeDecisionContext.activate(collector):
            load_result = ClickHouseSink(clickhouse, logger=base._Logger()).load(
                load_config,
                LoadPayload(artifact=extract.artifact, schema=extract.schema),
            )
        assert_acceleration_decisions(config, collector.decisions)
        load_seconds = time.perf_counter() - load_started
        phase = "reconciliation"
        source_count = base._count_mssql(mssql, config.source_schema, config.source_table)
        target_count = base._count_clickhouse(clickhouse, config.target_database, config.target_table)
        duplicate_count = base._count_clickhouse_duplicates(clickhouse, config.target_database, config.target_table)
        source_hash, target_hash = base._typed_hashes(mssql, clickhouse, base_config)
        target_schema = clickhouse_target_schema_metrics(
            mssql=mssql,
            clickhouse=clickhouse,
            source_schema=config.source_schema,
            source_table=config.source_table,
            target_database=config.target_database,
            target_table=config.target_table,
            type_fidelity=load_config.options.get("type_fidelity"),
        )
        return _write_result(
            config,
            source_count,
            target_count,
            duplicate_count,
            source_hash,
            target_hash,
            started,
            prepare_seconds,
            export_seconds,
            load_seconds,
            source_snapshot,
            collector.decisions,
            loaded_slice_closure(extract_artifact, loaded_rows=load_result.staging_rows),
            target_schema,
        )
    except Exception as exc:
        result = _failed_result(config, phase, exc, started, prepare_seconds, export_seconds, load_seconds)
        try:
            _remove_owned_partial_evidence(config)
            written = base.WideTypeEvidenceWriter(config.output_dir).write(result)
            write_bcp_route_receipt(
                config=config,
                result=result,
                result_path=written.json_path,
                source_snapshot=source_snapshot,
                decisions=collector.decisions,
                loaded_slices=loaded_slice_closure(extract_artifact),
                target_schema=None,
            )
        except Exception:  # noqa: BLE001 - preserve the live failure as the primary exception.
            pass
        raise
    finally:
        base._close_quietly(clickhouse)
        base._close_quietly(mssql)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--column-count", type=int, default=202)
    parser.add_argument("--typed-hash-rows", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument("--prepare-chunk-size", type=int, default=100_000)
    parser.add_argument("--skip-source-prepare", action="store_true")
    parser.add_argument("--target-rows-per-partition", type=int, default=2_500)
    parser.add_argument("--export-workers", type=int, default=1)
    parser.add_argument("--load-workers", type=int, default=1)
    parser.add_argument("--bcp-packet-size", type=int, default=32_767)
    parser.add_argument("--binary-format", choices=("rowbinary", "native"), default="rowbinary")
    parser.add_argument("--acceleration-mode", choices=("auto", "off", "required"), default="auto")
    parser.add_argument(
        "--output-dir", default="test_artifacts/live_certification/benchmarks/bcp_native_wide_type_latest"
    )
    parser.add_argument("--suffix")
    parser.add_argument("--source-schema")
    parser.add_argument("--source-table", default="orders")
    parser.add_argument("--target-table")
    parser.add_argument("--upstream-evidence", type=Path)
    add_connection_args(parser)
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> BcpNativeCertificationConfig:
    suffix = args.suffix or uuid.uuid4().hex[:8]
    return BcpNativeCertificationConfig(
        rows=args.rows,
        release_id=args.release_id,
        column_count=args.column_count,
        source_schema=args.source_schema or f"bcp_native_{suffix}",
        source_table=args.source_table,
        target_database=args.clickhouse_database,
        target_table=args.target_table or f"bcp_native_{suffix}",
        output_dir=Path(args.output_dir).expanduser().resolve(),
        mssql_params=base._mssql_params(args),
        clickhouse_params=base._clickhouse_params(args),
        target_rows_per_partition=args.target_rows_per_partition,
        export_workers=args.export_workers,
        load_workers=args.load_workers,
        batch_size=args.batch_size,
        binary_format=args.binary_format,
        prepare_chunk_size=args.prepare_chunk_size,
        typed_hash_rows=args.typed_hash_rows,
        bcp_packet_size=args.bcp_packet_size,
        acceleration_mode=args.acceleration_mode,
        skip_source_prepare=args.skip_source_prepare,
        upstream_evidence=args.upstream_evidence,
    )


def main(argv: list[str] | None = None) -> int:
    config = build_config(parse_args(argv))
    result = run_live_certification(config)
    print(base._render_markdown(result))
    receipt = json.loads((config.output_dir / "local_route_certification_receipt.json").read_text(encoding="utf-8"))
    return 0 if result.passed and receipt["evidence_status"] == "PASS" else 1


def _prepare_source(
    connector: Any,
    config: BcpNativeCertificationConfig,
    *,
    emit_event: Callable[[str, dict[str, Any]], None] | None = None,
) -> None:
    schema = base._mssql_ident(config.source_schema)
    table = base._mssql_ident(config.source_table)
    columns = build_bcp_native_columns(config.column_count)
    base._emit_event_if_configured(
        emit_event, "MSSQL_BCP_NATIVE_PREPARE_SOURCE_START", {"Rows": config.rows, "Columns": len(columns)}
    )
    source_schema_literal = config.source_schema.replace("'", "''")
    connector.execute_query(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{source_schema_literal}') "
        f"EXEC(N'CREATE SCHEMA {schema}')"
    )
    connector.execute_query(f"DROP TABLE IF EXISTS {schema}.{table}")
    connector.execute_query(
        f"CREATE TABLE {schema}.{table} ({', '.join(_ddl_column(column) for column in columns)}, PRIMARY KEY ([order_id]))"
    )
    insert_columns = [column for column in columns if column.insert_expression]
    insert_sql = ", ".join(base._mssql_ident(column.name) for column in insert_columns)
    select_sql = ", ".join(
        f"{column.insert_expression} AS {base._mssql_ident(column.name)}" for column in insert_columns
    )
    connector.execute_query(_insert_sql(schema, table, insert_sql, select_sql, config.rows))
    base._emit_event_if_configured(emit_event, "MSSQL_BCP_NATIVE_PREPARE_SOURCE_COMPLETE", {"Rows": config.rows})


def _insert_sql(schema: str, table: str, insert_sql: str, select_sql: str, rows: int) -> str:
    return f"""
    WITH digits(n) AS (
        SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(n)
    ),
    source_rows AS (
        SELECT TOP ({int(rows)}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS gs
        FROM digits AS a CROSS JOIN digits AS b CROSS JOIN digits AS c CROSS JOIN digits AS d
        CROSS JOIN digits AS e CROSS JOIN digits AS f CROSS JOIN digits AS g
    )
    INSERT INTO {schema}.{table} ({insert_sql})
    SELECT {select_sql} FROM source_rows ORDER BY gs
    """


def _ddl_column(column: base.WideTypeColumn) -> str:
    suffix = "" if column.name == "order_id" else " NULL"
    return f"{base._mssql_ident(column.name)} {column.mssql_type}{suffix}"


def _write_result(
    config: BcpNativeCertificationConfig,
    source_count: int,
    target_count: int,
    duplicate_count: int,
    source_hash: str | None,
    target_hash: str | None,
    started: float,
    prepare_seconds: float,
    export_seconds: float,
    load_seconds: float,
    source_snapshot: LocalSourceSnapshot,
    decisions: list[RuntimeDecision],
    loaded_slices: tuple[dict[str, int], ...],
    target_schema: ClickHouseTargetSchemaMetrics,
) -> base.WideTypeCertificationResult:
    typed_hash_passed = source_hash is not None and source_hash == target_hash
    schema_passed = (
        target_schema.column_count == config.column_count
        and target_schema.mismatch_count == 0
        and target_schema.observed_sha256 == target_schema.expected_sha256
    )
    result = base.WideTypeCertificationResult(
        rows=config.rows,
        column_count=len(build_bcp_native_columns(config.column_count)),
        source_count=source_count,
        target_count=target_count,
        duplicate_count=duplicate_count,
        typed_hash_passed=typed_hash_passed,
        typed_hash_source=source_hash,
        typed_hash_target=target_hash,
        elapsed_seconds=time.perf_counter() - started,
        prepare_seconds=prepare_seconds,
        export_seconds=export_seconds,
        load_seconds=load_seconds,
        artifact_bytes=0,
        passed=(
            source_count == target_count == config.rows and duplicate_count == 0 and typed_hash_passed and schema_passed
        ),
    )
    written = base.WideTypeEvidenceWriter(config.output_dir).write(result)
    write_bcp_route_receipt(
        config=config,
        result=result,
        result_path=written.json_path,
        source_snapshot=source_snapshot,
        decisions=decisions,
        loaded_slices=loaded_slices,
        target_schema=target_schema,
    )
    return result


def _failed_result(
    config: BcpNativeCertificationConfig,
    phase: str,
    exc: Exception,
    started: float,
    prepare_seconds: float,
    export_seconds: float,
    load_seconds: float,
) -> base.WideTypeCertificationResult:
    return base.WideTypeCertificationResult(
        rows=config.rows,
        column_count=len(build_bcp_native_columns(config.column_count)),
        source_count=0,
        target_count=0,
        duplicate_count=0,
        typed_hash_passed=False,
        typed_hash_source=None,
        typed_hash_target=None,
        elapsed_seconds=time.perf_counter() - started,
        prepare_seconds=prepare_seconds,
        export_seconds=export_seconds,
        load_seconds=load_seconds,
        artifact_bytes=0,
        passed=False,
        failed_phase=phase,
        error=f"{phase}_failed",
    )


def _remove_owned_partial_evidence(config: BcpNativeCertificationConfig) -> None:
    """Remove only exact files created by this attempt's evidence writers."""

    names = {
        "local_route_certification_receipt.json",
        "mssql_clickhouse_wide_type_certification.json",
        "mssql_clickhouse_wide_type_certification.md",
    }
    if config.upstream_evidence is not None:
        names.add(f"upstream_{config.upstream_evidence.name}")
        upstream = json.loads(config.upstream_evidence.read_text(encoding="utf-8"))
        if not isinstance(upstream, dict):
            raise ValueError("upstream_evidence_invalid")
        for prefix in ("run_results", "manifest"):
            name = str(upstream.get(f"{prefix}_path") or "")
            if not name or Path(name).name != name:
                raise ValueError("upstream_evidence_artifact_path_invalid")
            names.add(name)
    for name in names:
        path = config.output_dir / name
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ValueError("local_route_certification_owned_artifact_invalid")
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
