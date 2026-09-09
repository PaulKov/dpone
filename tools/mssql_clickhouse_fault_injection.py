#!/usr/bin/env python
"""Run local MSSQL -> ClickHouse native transfer fault-injection certification."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import mssql_stress as stress
from dpone._compat import UTC
from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.lineage.artifact_checksum import ArtifactChecksumService
from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.runtime.lineage.partition_resume import PartitionResumePlan, PartitionResumePlanner, PlannedTransferPartition
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy
from dpone.strategy_intelligence.fault_injection import (
    FaultInjectionSnapshot,
    FaultInjectionStage,
    NativeTransferFaultInjectionScenario,
    NativeTransferFaultInjectionWorkflow,
)
from dpone.strategy_intelligence.native_transfer_runtime_report import (
    NativeTransferRuntimeReport,
    NativeTransferRuntimeReportWriter,
)
from dpone.strategy_intelligence.partition_correctness import (
    PartitionCorrectnessObservation,
    PartitionCorrectnessResult,
    PartitionCorrectnessService,
)
from dpone.strategy_intelligence.resume_certification import NativeTransferResumeEvidenceWriter
from dpone.strategy_intelligence.typed_reconciliation import TypedColumnSpec, TypedRowHashService

_QUERY_HASH = "mssql_clickhouse_fault_injection_query_v1"
_SCHEMA_HASH = "mssql_clickhouse_fault_injection_schema_v1"


@dataclass(frozen=True)
class FaultInjectionArtifact:
    stage: str
    json_path: str
    md_path: str
    runtime_report_json_path: str
    runtime_report_md_path: str
    passed: bool
    resume_plan: dict[str, Any]
    partition_correctness: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dpone MSSQL -> ClickHouse fault-injection certification")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument("--partition-column", default="id")
    parser.add_argument("--num-partitions", type=int, default=4)
    parser.add_argument("--lower-bound", type=int, default=1)
    parser.add_argument("--upper-bound", type=int, default=None)
    parser.add_argument("--export-workers", type=int, default=2)
    parser.add_argument("--load-workers", type=int, default=2)
    parser.add_argument("--optimizer-profile", choices=["high_throughput_safe"], default=None)
    parser.add_argument(
        "--output-dir", default="test_artifacts/live_certification/benchmarks/native_fault_injection_latest"
    )
    parser.add_argument("--json-output", default=None)
    parser.add_argument("--checkpoint-store", default=None)
    parser.add_argument(
        "--reconciliation-profile",
        default="count_and_checksum",
        choices=[
            "count_only",
            "count_and_checksum",
            "count_and_sum",
            "sample_hash",
            "full_partition_hash",
            "typed_hash",
        ],
    )
    parser.add_argument("--binary-encoding", default="none", choices=["none", "hex", "base64"])
    parser.add_argument("--time-encoding", default="string", choices=["string", "seconds_since_midnight"])
    parser.add_argument("--stages", default="after_export,during_load,before_finalizer")
    parser.add_argument("--bcp-path", default="/opt/homebrew/bin/bcp")
    parser.add_argument("--clickhouse-bulk-mode", default="http", choices=["auto", "python", "client", "http"])
    parser.add_argument("--clickhouse-client-command", default=None)
    parser.add_argument("--clickhouse-client-host", default=None)
    parser.add_argument("--clickhouse-client-port", type=int, default=None)
    parser.add_argument("--clickhouse-http-host", default="127.0.0.1")
    parser.add_argument("--clickhouse-http-port", type=int, default=58123)
    _add_connection_args(parser)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stress.CURRENT_ARGS = args
    logger = stress.StressLogger()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pg = stress.PostgresConnector(
        host=args.pg_host,
        port=args.pg_port,
        database=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="dpone-fault-pg",
    )
    mssql = stress.MSSQLConnector(
        host=args.mssql_host,
        port=args.mssql_port,
        database=args.mssql_database,
        user=args.mssql_user,
        password=args.mssql_password,
        driver=args.mssql_driver,
        trust_server_certificate=True,
        application_name="dpone-fault-mssql",
        bcp_path=args.bcp_path,
        query_timeout=0,
    )
    clickhouse = stress.ClickHouseConnector(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        database=args.clickhouse_database,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
        application_name="dpone-fault-clickhouse",
        compression=True,
    )
    try:
        stress.prepare_postgres(pg, args.rows)
        stress.prepare_mssql(mssql)
        stress.prepare_clickhouse(clickhouse, args.clickhouse_database)
        stress.run_postgres_to_mssql(pg, mssql, logger, args.batch_size)

        writer = NativeTransferResumeEvidenceWriter(output_dir)
        runtime_report_writer = NativeTransferRuntimeReportWriter(output_dir)
        workflow = NativeTransferFaultInjectionWorkflow()
        checkpoint_store = JsonlPartitionCheckpointStore(
            args.checkpoint_store or output_dir / "partition_checkpoints.jsonl"
        )
        artifacts: list[FaultInjectionArtifact] = []
        for stage in _parse_stages(args.stages):
            stress.prepare_clickhouse(clickhouse, args.clickhouse_database)
            operations = MSSQLClickHouseFaultOperations(args, mssql, clickhouse, logger, checkpoint_store)
            scenario = NativeTransferFaultInjectionScenario(
                run_id=f"mssql_clickhouse_{stage.value}_{_date_id()}",
                source_type="mssql",
                sink_type="clickhouse",
                strategy="full_refresh",
                failure_stage=stage,
                expected_rows=args.rows,
                query_hash=_QUERY_HASH,
                schema_hash=_SCHEMA_HASH,
            )
            result = workflow.run(scenario, operations)
            if operations.last_resume_plan is None or operations.last_correctness_result is None:
                raise RuntimeError("Fault-injection operations did not produce runtime resume/correctness evidence.")
            json_path, md_path = writer.write(result)
            runtime_json_path, runtime_md_path = runtime_report_writer.write(
                NativeTransferRuntimeReport(
                    run_id=scenario.run_id,
                    source_type=scenario.source_type,
                    sink_type=scenario.sink_type,
                    strategy=scenario.strategy,
                    checkpoint_summary=checkpoint_store.summary(),
                    resume_plan=operations.last_resume_plan,
                    partition_correctness=operations.last_correctness_result,
                )
            )
            artifacts.append(
                FaultInjectionArtifact(
                    stage=stage.value,
                    json_path=str(json_path),
                    md_path=str(md_path),
                    runtime_report_json_path=str(runtime_json_path),
                    runtime_report_md_path=str(runtime_md_path),
                    passed=result.passed,
                    resume_plan=operations.last_resume_plan.to_dict() if operations.last_resume_plan else {},
                    partition_correctness=(
                        operations.last_correctness_result.to_dict() if operations.last_correctness_result else {}
                    ),
                )
            )

        summary = {
            "schema_version": "dpone.native_transfer.fault_injection.summary.v1",
            "created_at": datetime.now(UTC).isoformat(),
            "rows": args.rows,
            "reconciliation_profile": args.reconciliation_profile,
            "checkpoint_store": str(checkpoint_store.path),
            "checkpoint_summary": checkpoint_store.summary(),
            "stages": [asdict(artifact) for artifact in artifacts],
            "partition_correctness_passed": all(
                bool(artifact.partition_correctness.get("passed")) for artifact in artifacts
            ),
            "passed": all(artifact.passed and artifact.partition_correctness.get("passed") for artifact in artifacts),
        }
        summary_path = Path(args.json_output) if args.json_output else output_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0 if summary["passed"] else 2
    finally:
        pg.close()
        mssql.close()


class MSSQLClickHouseFaultOperations:
    """Local Docker operations for MSSQL -> ClickHouse fault-injection certification."""

    def __init__(
        self,
        args: argparse.Namespace,
        mssql,
        clickhouse,
        logger: stress.StressLogger,
        checkpoint_store: JsonlPartitionCheckpointStore,
    ) -> None:
        self.args = args
        self.mssql = mssql
        self.clickhouse = clickhouse
        self.logger = logger
        self.checkpoint_store = checkpoint_store
        self.last_resume_plan: PartitionResumePlan | None = None
        self.last_correctness_result: PartitionCorrectnessResult | None = None

    def inject_failure(self, stage: FaultInjectionStage) -> FaultInjectionSnapshot:
        extract = self._export()
        status = cast(
            PartitionCheckpointStatus,
            {
                FaultInjectionStage.AFTER_EXPORT: PartitionCheckpointStatus.EXPORTED,
                FaultInjectionStage.DURING_LOAD: PartitionCheckpointStatus.LOADED,
                FaultInjectionStage.BEFORE_FINALIZER: PartitionCheckpointStatus.FINALIZED,
            }[stage],
        )
        checkpoints = _checkpoints_for_artifact(
            extract.artifact,
            status=status,
            rows=self.args.rows,
            source_table="dbo.bench_orders",
            target_table=f"{self.clickhouse.database}.bench_orders_ch",
        )
        self.checkpoint_store.upsert_many(checkpoints)
        self.last_resume_plan = PartitionResumePlanner().plan(
            _planned_partitions(
                rows=self.args.rows,
                source_table="dbo.bench_orders",
                target_table=f"{self.clickhouse.database}.bench_orders_ch",
            ),
            self.checkpoint_store,
        )
        if hasattr(extract.artifact, "cleanup"):
            extract.artifact.cleanup()
        return FaultInjectionSnapshot(
            checkpoints=checkpoints,
            row_count=0,
            duplicate_rows=0,
            state_committed=False,
        )

    def retry(self, stage: FaultInjectionStage) -> FaultInjectionSnapshot:
        del stage
        stress.prepare_clickhouse(self.clickhouse, self.args.clickhouse_database)
        stress.run_mssql_to_clickhouse(self.mssql, self.clickhouse, self.logger, self.args.batch_size)
        row_count = stress.count_clickhouse(self.clickhouse, self.args.clickhouse_database, "bench_orders_ch")
        duplicate_rows = _count_clickhouse_duplicates(self.clickhouse, self.args.clickhouse_database, "bench_orders_ch")
        self.last_correctness_result = _certify_partition_correctness(
            self.mssql,
            self.clickhouse,
            rows=self.args.rows,
            source_table="dbo.bench_orders",
            target_schema=self.args.clickhouse_database,
            target_table="bench_orders_ch",
            profile=self.args.reconciliation_profile,
        )
        checkpoints = _committed_checkpoints(
            rows=self.args.rows,
            source_table="dbo.bench_orders",
            target_table=f"{self.clickhouse.database}.bench_orders_ch",
        )
        self.checkpoint_store.upsert_many(checkpoints)
        return FaultInjectionSnapshot(
            checkpoints=checkpoints,
            row_count=row_count,
            duplicate_rows=duplicate_rows,
            state_committed=True,
        )

    def _export(self):
        config = _mssql_clickhouse_config(self.args, self.clickhouse.database)
        source = MSSQLFullExtractStrategy(self.mssql, self.logger, sink_connector=self.clickhouse)
        return source.extract(config, None)


def _mssql_clickhouse_config(args: argparse.Namespace, clickhouse_database: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="bench_orders",
        target_schema=clickhouse_database,
        target_table="bench_orders_ch",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=args.batch_size,
        options={
            **stress.mssql_bulk_options(),
            **stress.clickhouse_bulk_options(),
            **stress.partition_options(),
        },
    )


def _checkpoints_for_artifact(
    artifact: Any,
    *,
    status: PartitionCheckpointStatus,
    rows: int,
    source_table: str,
    target_table: str,
) -> tuple[PartitionCheckpoint, ...]:
    partitions = list(getattr(artifact, "partitions", None) or [])
    if not partitions:
        partitions = [artifact]
    checkpoints: list[PartitionCheckpoint] = []
    for index, partition in enumerate(partitions):
        bounds = _partition_bounds(index, len(partitions), rows)
        checkpoints.append(
            PartitionCheckpoint(
                transfer_partition_id=build_transfer_partition_id(
                    source_table=source_table,
                    target_table=target_table,
                    strategy="full_refresh",
                    query_hash=_QUERY_HASH,
                    schema_hash=_SCHEMA_HASH,
                    partition_bounds=bounds,
                ),
                status=status,
                query_hash=_QUERY_HASH,
                schema_hash=_SCHEMA_HASH,
                source_table=source_table,
                target_table=target_table,
                partition_bounds=bounds,
                completed_at=datetime.now(UTC),
                rows_exported=getattr(partition, "estimated_rows", None),
                bytes_exported=_file_size(getattr(partition, "file_path", None)),
                diagnostics=_artifact_diagnostics(getattr(partition, "file_path", None)),
            )
        )
    return tuple(checkpoints)


def _committed_checkpoints(*, rows: int, source_table: str, target_table: str) -> tuple[PartitionCheckpoint, ...]:
    partition_count = max(1, int(stress.CURRENT_ARGS.num_partitions if stress.CURRENT_ARGS else 1))
    return tuple(
        _checkpoint_for_partition(
            index=index,
            partition_count=partition_count,
            rows=rows,
            status=cast(PartitionCheckpointStatus, PartitionCheckpointStatus.COMMITTED),
            source_table=source_table,
            target_table=target_table,
        )
        for index in range(partition_count)
    )


def _checkpoint_for_partition(
    *,
    index: int,
    partition_count: int,
    rows: int,
    status: PartitionCheckpointStatus,
    source_table: str,
    target_table: str,
) -> PartitionCheckpoint:
    bounds = _partition_bounds(index, partition_count, rows)
    return PartitionCheckpoint(
        transfer_partition_id=build_transfer_partition_id(
            source_table=source_table,
            target_table=target_table,
            strategy="full_refresh",
            query_hash=_QUERY_HASH,
            schema_hash=_SCHEMA_HASH,
            partition_bounds=bounds,
        ),
        status=status,
        query_hash=_QUERY_HASH,
        schema_hash=_SCHEMA_HASH,
        source_table=source_table,
        target_table=target_table,
        partition_bounds=bounds,
        completed_at=datetime.now(UTC),
    )


def _planned_partitions(*, rows: int, source_table: str, target_table: str) -> tuple[PlannedTransferPartition, ...]:
    partition_count = max(1, int(stress.CURRENT_ARGS.num_partitions if stress.CURRENT_ARGS else 1))
    return tuple(
        _planned_partition(
            index=index,
            partition_count=partition_count,
            rows=rows,
            source_table=source_table,
            target_table=target_table,
        )
        for index in range(partition_count)
    )


def _planned_partition(
    *,
    index: int,
    partition_count: int,
    rows: int,
    source_table: str,
    target_table: str,
) -> PlannedTransferPartition:
    bounds = _partition_bounds(index, partition_count, rows)
    return PlannedTransferPartition(
        transfer_partition_id=build_transfer_partition_id(
            source_table=source_table,
            target_table=target_table,
            strategy="full_refresh",
            query_hash=_QUERY_HASH,
            schema_hash=_SCHEMA_HASH,
            partition_bounds=bounds,
        ),
        source_table=source_table,
        target_table=target_table,
        strategy="full_refresh",
        query_hash=_QUERY_HASH,
        schema_hash=_SCHEMA_HASH,
        partition_bounds=bounds,
    )


def _partition_bounds(index: int, partition_count: int, rows: int) -> dict[str, int]:
    lower = int(stress.CURRENT_ARGS.lower_bound if stress.CURRENT_ARGS else 1)
    upper = int(stress.CURRENT_ARGS.upper_bound or rows if stress.CURRENT_ARGS else rows)
    span = max(1, upper - lower + 1)
    step = max(1, span // partition_count)
    part_lower = lower + index * step
    part_upper = upper if index == partition_count - 1 else min(upper, part_lower + step - 1)
    return {"lower": part_lower, "upper": part_upper}


def _count_clickhouse_duplicates(connector, schema: str, table: str) -> int:
    rows = connector.get_records(f"SELECT count() - uniqExact(id) FROM `{schema}`.`{table}`")
    return int(rows[0][0]) if rows else 0


def _certify_partition_correctness(
    mssql,
    clickhouse,
    *,
    rows: int,
    source_table: str,
    target_schema: str,
    target_table: str,
    profile: str = "count_and_checksum",
) -> PartitionCorrectnessResult:
    observations: list[PartitionCorrectnessObservation] = []
    typed_schema = _source_schema_for_typed_hash(mssql, source_table) if profile == "typed_hash" else []
    typed_policy = MssqlClickHouseTypePolicy.from_config(
        {
            "binary_encoding": stress.CURRENT_ARGS.binary_encoding if stress.CURRENT_ARGS else "none",
            "time_encoding": stress.CURRENT_ARGS.time_encoding if stress.CURRENT_ARGS else "string",
        }
    )
    for partition in _planned_partitions(
        rows=rows, source_table=source_table, target_table=f"{target_schema}.{target_table}"
    ):
        lower = int(partition.partition_bounds["lower"])
        upper = int(partition.partition_bounds["upper"])
        source_count, source_checksum = _mssql_partition_checksum(mssql, lower=lower, upper=upper)
        target_count, target_checksum = _clickhouse_partition_checksum(
            clickhouse,
            schema=target_schema,
            table=target_table,
            lower=lower,
            upper=upper,
        )
        observations.append(
            PartitionCorrectnessObservation(
                partition_id=partition.transfer_partition_id,
                bounds=partition.partition_bounds,
                source_count=source_count,
                target_count=target_count,
                source_checksum=source_checksum,
                target_checksum=target_checksum,
                source_sample_hash=(
                    _mssql_partition_row_hash(mssql, lower=lower, upper=upper, limit=128)
                    if profile in {"sample_hash", "full_partition_hash"}
                    else None
                ),
                target_sample_hash=(
                    _clickhouse_partition_row_hash(
                        clickhouse,
                        schema=target_schema,
                        table=target_table,
                        lower=lower,
                        upper=upper,
                        limit=128,
                    )
                    if profile in {"sample_hash", "full_partition_hash"}
                    else None
                ),
                source_full_hash=(
                    _mssql_partition_row_hash(mssql, lower=lower, upper=upper, limit=None)
                    if profile == "full_partition_hash"
                    else None
                ),
                target_full_hash=(
                    _clickhouse_partition_row_hash(
                        clickhouse,
                        schema=target_schema,
                        table=target_table,
                        lower=lower,
                        upper=upper,
                        limit=None,
                    )
                    if profile == "full_partition_hash"
                    else None
                ),
                source_typed_hash=(
                    _mssql_partition_typed_hash(
                        mssql,
                        source_table=source_table,
                        source_schema=typed_schema,
                        policy=typed_policy,
                        lower=lower,
                        upper=upper,
                    )
                    if profile == "typed_hash"
                    else None
                ),
                target_typed_hash=(
                    _clickhouse_partition_typed_hash(
                        clickhouse,
                        schema=target_schema,
                        table=target_table,
                        source_schema=typed_schema,
                        policy=typed_policy,
                        lower=lower,
                        upper=upper,
                    )
                    if profile == "typed_hash"
                    else None
                ),
            )
        )
    return PartitionCorrectnessService().certify(tuple(observations), profile=profile)


def _mssql_partition_checksum(connector, *, lower: int, upper: int) -> tuple[int, str]:
    rows = connector.get_records(
        f"""
        SELECT
            COUNT_BIG(*) AS row_count,
            COALESCE(SUM(CONVERT(bigint, [id])), 0) AS sum_id,
            COALESCE(SUM(CONVERT(bigint, [customer_id])), 0) AS sum_customer_id
        FROM {connector.qualified_name("dbo", "bench_orders")}
        WHERE [id] BETWEEN {lower} AND {upper}
        """
    )
    row = rows[0]
    count = int(row[0])
    checksum = f"{count}:{int(row[1])}:{int(row[2])}"
    return count, checksum


def _mssql_partition_row_hash(connector, *, lower: int, upper: int, limit: int | None) -> str:
    top_clause = f"TOP ({int(limit)}) " if limit else ""
    rows = connector.get_records(
        f"""
        SELECT {top_clause}
            [id],
            [customer_id],
            [amount],
            [description]
        FROM {connector.qualified_name("dbo", "bench_orders")}
        WHERE [id] BETWEEN {lower} AND {upper}
        ORDER BY [id]
        """
    )
    return _rows_hash(rows)


def _clickhouse_partition_checksum(connector, *, schema: str, table: str, lower: int, upper: int) -> tuple[int, str]:
    rows = connector.get_records(
        f"""
        SELECT
            count() AS row_count,
            sum(toInt64(id)) AS sum_id,
            sum(toInt64(customer_id)) AS sum_customer_id
        FROM `{schema}`.`{table}`
        WHERE id BETWEEN {lower} AND {upper}
        """
    )
    row = rows[0]
    count = int(row[0])
    checksum = f"{count}:{int(row[1])}:{int(row[2])}"
    return count, checksum


def _clickhouse_partition_row_hash(
    connector,
    *,
    schema: str,
    table: str,
    lower: int,
    upper: int,
    limit: int | None,
) -> str:
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    rows = connector.get_records(
        f"""
        SELECT
            id,
            customer_id,
            amount,
            description
        FROM `{schema}`.`{table}`
        WHERE id BETWEEN {lower} AND {upper}
        ORDER BY id
        {limit_clause}
        """
    )
    return _rows_hash(rows)


def _mssql_partition_typed_hash(
    connector,
    *,
    source_table: str,
    source_schema: list[tuple[str, str]],
    policy: MssqlClickHouseTypePolicy,
    lower: int,
    upper: int,
) -> str:
    schema_name, table_name = _split_table_name(source_table)
    columns_sql = ",\n            ".join(connector.quote_identifier(column) for column, _ in source_schema)
    rows = connector.get_records(
        f"""
        SELECT
            {columns_sql}
        FROM {connector.qualified_name(schema_name, table_name)}
        WHERE [id] BETWEEN {lower} AND {upper}
        ORDER BY [id]
        """
    )
    return _typed_hash(rows, source_schema, policy)


def _clickhouse_partition_typed_hash(
    connector,
    *,
    schema: str,
    table: str,
    source_schema: list[tuple[str, str]],
    policy: MssqlClickHouseTypePolicy,
    lower: int,
    upper: int,
) -> str:
    columns_sql = ",\n            ".join(f"`{column}`" for column, _ in source_schema)
    rows = connector.get_records(
        f"""
        SELECT
            {columns_sql}
        FROM `{schema}`.`{table}`
        WHERE id BETWEEN {lower} AND {upper}
        ORDER BY id
        """
    )
    return _typed_hash(rows, source_schema, policy)


def _typed_hash(
    rows: list[tuple[Any, ...]],
    source_schema: list[tuple[str, str]],
    policy: MssqlClickHouseTypePolicy,
) -> str:
    specs = tuple(TypedColumnSpec(column, source_type) for column, source_type in source_schema)
    return TypedRowHashService(specs, policy=policy).hash_rows(rows)


def _source_schema_for_typed_hash(connector, source_table: str) -> list[tuple[str, str]]:
    schema_name, table_name = _split_table_name(source_table)
    return [(column, source_type) for column, source_type in connector.fetch_schema(schema_name, table_name)]


def _split_table_name(table_name: str) -> tuple[str, str]:
    if "." not in table_name:
        return "dbo", table_name
    schema_name, bare_table = table_name.split(".", 1)
    return schema_name, bare_table


def _rows_hash(rows: list[tuple[Any, ...]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(tuple(_normalize_hash_value(value) for value in row), default=str).encode("utf-8"))
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def _normalize_hash_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return format(Decimal(format(value, ".15g")).normalize(), "f")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _file_size(path: str | None) -> int | None:
    if not path:
        return None
    file_path = Path(path)
    return file_path.stat().st_size if file_path.exists() else None


def _artifact_diagnostics(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {"artifact_exists": False}
    return {
        "artifact_exists": True,
        "artifact_sha256": ArtifactChecksumService().checksum(_FilePathArtifact(str(file_path))),
    }


@dataclass(frozen=True)
class _FilePathArtifact:
    file_path: str


def _parse_stages(raw: str) -> tuple[FaultInjectionStage, ...]:
    return tuple(FaultInjectionStage(item.strip()) for item in raw.split(",") if item.strip())


def _date_id() -> str:
    return datetime.now(UTC).strftime("%Y_%m_%d_%H%M%S")


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=55432)
    parser.add_argument("--pg-database", default="dpone_it")
    parser.add_argument("--pg-user", default="dpone")
    parser.add_argument("--pg-password", default="dpone")
    parser.add_argument("--mssql-host", default="127.0.0.1")
    parser.add_argument("--mssql-port", type=int, default=51433)
    parser.add_argument("--mssql-database", default="dpone")
    parser.add_argument("--mssql-user", default="sa")
    parser.add_argument("--mssql-password", default="Dp0ne.Strong.Pw.2026!")
    parser.add_argument("--mssql-driver", default="ODBC Driver 18 for SQL Server")
    parser.add_argument("--clickhouse-host", default="127.0.0.1")
    parser.add_argument("--clickhouse-port", type=int, default=59000)
    parser.add_argument("--clickhouse-database", default="dpone_it")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="dpone")


if __name__ == "__main__":
    raise SystemExit(main())
