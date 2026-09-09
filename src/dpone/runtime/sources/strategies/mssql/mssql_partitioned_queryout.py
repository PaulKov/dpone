from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.partitioning import RangePartitioner
    from dpone.runtime.support.mssql_bulk import BcpOptions


import os
import tempfile
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any

from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.partition_checkpoint import build_transfer_partition_id
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_slicing import RangeSlicePlanner, TransferPartition, TransferSlice
from dpone.runtime.parallel import BoundedParallelMapExecutor
from dpone.runtime.partitioning import RangePartition
from dpone.runtime.partitioning_predicates import MssqlPartitionPredicateRenderer
from dpone.runtime.sources.strategies.mssql.mssql_bcp_native_artifacts import build_mssql_bcp_native_artifact
from dpone.runtime.storage_policy import RuntimeStoragePolicy
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy
from dpone.runtime.transfer_store_factory import build_slice_transfer_store


def build_partitioned_queryout(
    factory: Any,
    load_config: LoadConfig,
    query: str,
    schema: list[tuple[str, str]],
    partitioner: RangePartitioner,
    bcp_options: BcpOptions,
    *,
    artifact_format: str,
    bulk_text_codec: Any | None,
    bulk_wire_contract: Any | None = None,
) -> FileExportArtifact | PartitionedFileExportArtifact | PartitionedTransferPlanArtifact:
    tmp_dir = RuntimeStoragePolicy.from_options(load_config.options).work_dir
    tmp_dir.mkdir(parents=True, exist_ok=True)
    partitions = partitioner.partitions()
    columns = [column for column, _ in schema]
    quoted_column = factory.connector.quote_identifier(partitioner.column)
    context = _QueryoutContext.from_config(load_config, query, schema)

    factory.logger.log_etl_progress(
        "MSSQL_PARTITIONED_BCP_QUERYOUT_START",
        {
            "Column": partitioner.column,
            "Partitions": len(partitions),
            "Workers": partitioner.max_workers,
            "Execution": _execution_policy(load_config).mode,
        },
    )

    def export_range(
        *,
        range_partition: RangePartition,
        prefix: str,
        bounds: dict[str, Any],
    ) -> FileExportArtifact:
        file_path = tempfile.NamedTemporaryFile(prefix=prefix, suffix=".bcp", dir=tmp_dir, delete=False).name
        try:
            partition_query = partitioner.wrap_query(
                query,
                quoted_column,
                range_partition,
                renderer=MssqlPartitionPredicateRenderer(),
            )
            rows = factory.connector.bcp_queryout(partition_query, file_path, options=bcp_options)
            if artifact_format == "mssql-bcp-native":
                artifact = build_mssql_bcp_native_artifact(
                    file_path,
                    schema,
                    query=partition_query,
                    bcp_version=bcp_options.bcp_path,
                    type_policy=_type_policy(load_config),
                    estimated_rows=rows or None,
                    bulk_wire_contract=bulk_wire_contract,
                )
            else:
                artifact = FileExportArtifact(
                    file_path=file_path,
                    columns=columns,
                    compressed=False,
                    format=artifact_format,
                    estimated_rows=rows or None,
                    rows_exported=(
                        rows if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 else None
                    ),
                    bulk_text_codec=bulk_text_codec,
                )
            if bulk_wire_contract is not None:
                artifact.bulk_wire_contract = bulk_wire_contract
            if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 and artifact.rows_exported is None:
                artifact.rows_exported = rows
            for key, value in context.metadata(bounds).items():
                setattr(artifact, key, value)
            return artifact
        except Exception:
            try:
                os.remove(file_path)
            except OSError:
                pass
            raise

    policy = _execution_policy(load_config)
    if _should_pipeline(load_config, policy):
        slices = _slices(partitions, policy)
        artifact = PartitionedTransferPlanArtifact(
            slices=slices,
            columns=columns,
            exporter=lambda item: export_range(
                range_partition=RangePartition(
                    index=item.partition_index,
                    lower_bound=item.lower_bound,
                    upper_bound=item.upper_bound,
                    include_upper=item.include_upper,
                    is_null_partition=item.is_null_partition,
                    boundary=partitioner.boundary,
                ),
                prefix=f"dpone_mssql_queryout_p{item.partition_index}_s{item.slice_index}_",
                bounds=_slice_bounds(item, partitioner),
            ),
            resource_policy=policy.resource,
            estimated_rows=partitioner.source_row_count,
            format=artifact_format,
            bulk_text_codec=bulk_text_codec,
            bulk_wire_contract=bulk_wire_contract,
            transfer_store=build_slice_transfer_store(
                RuntimeStoragePolicy.from_options(load_config.options).transfer_store,
                run_id=_transfer_run_id(load_config),
                dataset="mssql",
                table=_table_label(load_config.source_schema, load_config.source_table, load_config.source_database),
            ),
        )
        for key in ("query_hash", "schema_hash", "source_table", "target_table", "strategy"):
            setattr(artifact, key, getattr(context, key))
        return artifact

    artifacts = _materialize_partitions(partitions, partitioner, export_range)
    estimated_rows = sum(artifact.estimated_rows or 0 for artifact in artifacts) or None
    factory.logger.log_etl_progress(
        "MSSQL_PARTITIONED_BCP_QUERYOUT_COMPLETE",
        {"Files": len(artifacts), "Rows": estimated_rows or "unknown", "Load_Workers": partitioner.load_workers},
    )
    artifact = PartitionedFileExportArtifact(
        partitions=artifacts,
        columns=columns,
        max_workers=partitioner.load_workers,
        estimated_rows=estimated_rows,
    )
    if bulk_wire_contract is not None:
        artifact.bulk_wire_contract = bulk_wire_contract
    return artifact


class _QueryoutContext:
    def __init__(
        self, *, query_hash: str, schema_hash: str, source_table: str, target_table: str, strategy: str
    ) -> None:
        self.query_hash = query_hash
        self.schema_hash = schema_hash
        self.source_table = source_table
        self.target_table = target_table
        self.strategy = strategy

    @classmethod
    def from_config(cls, load_config: LoadConfig, query: str, schema: list[tuple[str, str]]) -> _QueryoutContext:
        return cls(
            query_hash=_hash_text(query),
            schema_hash=_hash_text(repr(tuple(schema))),
            source_table=_table_label(load_config.source_schema, load_config.source_table, load_config.source_database),
            target_table=f"{load_config.target_schema}.{load_config.target_table}",
            strategy=load_config.load_strategy.value,
        )

    def metadata(self, bounds: dict[str, Any]) -> dict[str, Any]:
        return {
            "partition_bounds": bounds,
            "query_hash": self.query_hash,
            "schema_hash": self.schema_hash,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "strategy": self.strategy,
            "transfer_partition_id": build_transfer_partition_id(
                source_table=self.source_table,
                target_table=self.target_table,
                strategy=self.strategy,
                query_hash=self.query_hash,
                schema_hash=self.schema_hash,
                partition_bounds=bounds,
            ),
        }


def _materialize_partitions(
    partitions: list[RangePartition],
    partitioner: RangePartitioner,
    exporter: Any,
) -> list[FileExportArtifact]:
    def export_partition(partition: RangePartition) -> FileExportArtifact:
        return exporter(
            range_partition=partition,
            prefix=f"dpone_mssql_queryout_p{partition.index}_",
            bounds=_partition_bounds(partition),
        )

    if partitioner.max_workers == 1:
        return [export_partition(partition) for partition in partitions]
    return BoundedParallelMapExecutor(partitioner.max_workers).map(export_partition, partitions)


def _slices(partitions: list[RangePartition], policy: NativeTransferExecutionPolicy) -> tuple[TransferSlice, ...]:
    planner = RangeSlicePlanner(policy.resource)
    values: list[TransferSlice] = []
    for partition in partitions:
        if not _has_numeric_partition_bounds(partition):
            values.append(
                TransferSlice(
                    partition_index=partition.index,
                    slice_index=0,
                    lower_bound=partition.lower_bound,
                    upper_bound=partition.upper_bound,
                    include_upper=partition.include_upper,
                    is_null_partition=partition.is_null_partition,
                )
            )
            continue
        transfer_partition = TransferPartition(
            index=partition.index,
            lower_bound=int(partition.lower_bound),
            upper_bound=int(partition.upper_bound),
            include_upper=partition.include_upper,
        )
        values.extend(planner.plan(transfer_partition, estimated_bytes_per_row=None))
    return tuple(values)


def _has_numeric_partition_bounds(partition: RangePartition) -> bool:
    try:
        int(partition.lower_bound)
        int(partition.upper_bound)
    except (TypeError, ValueError):
        return False
    return True


def _execution_policy(load_config: LoadConfig) -> NativeTransferExecutionPolicy:
    raw = (load_config.options or {}).get("native_transfer", {})
    execution = raw.get("execution") if isinstance(raw, dict) else {}
    return NativeTransferExecutionPolicy.from_mapping(execution if isinstance(execution, dict) else {})


def _type_policy(load_config: LoadConfig) -> MssqlClickHouseTypePolicy:
    return MssqlClickHouseTypePolicy.from_config((load_config.options or {}).get("type_fidelity"))


def _should_pipeline(load_config: LoadConfig, policy: NativeTransferExecutionPolicy) -> bool:
    if policy.mode == "batch":
        return False
    if policy.mode == "pipelined":
        return True
    return str((load_config.options or {}).get("sink_type") or "").lower() in {"clickhouse", "mssql", "postgres"}


def _partition_bounds(partition: RangePartition) -> dict[str, Any]:
    return {
        "index": partition.index,
        "lower": _serializable_bound(partition.lower_bound),
        "upper": _serializable_bound(partition.upper_bound),
        "include_lower": partition.include_lower,
        "include_upper": partition.include_upper,
        "is_null_partition": partition.is_null_partition,
        "boundary_type": partition.boundary.kind.value,
    }


def _slice_bounds(item: TransferSlice, partitioner: RangePartitioner) -> dict[str, Any]:
    return {
        "partition_index": item.partition_index,
        "slice_index": item.slice_index,
        "lower": _serializable_bound(item.lower_bound),
        "upper": _serializable_bound(item.upper_bound),
        "include_upper": item.include_upper,
        "is_null_partition": item.is_null_partition,
        "boundary_type": partitioner.boundary.kind.value,
    }


def _serializable_bound(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes | bytearray):
        return "0x" + bytes(value).hex().upper()
    return value


def _hash_text(value: str) -> str:
    return f"sha256:{sha256(value.encode('utf-8')).hexdigest()}"


def _transfer_run_id(load_config: LoadConfig) -> str:
    options = load_config.options or {}
    raw = options.get("run_id") or options.get("dpone_run_id")
    if raw:
        return str(raw)
    return f"dpone/{_table_label(load_config.source_schema, load_config.source_table, load_config.source_database)}"


def _schema_label(schema: str, database: str | None) -> str:
    if database and "." not in str(schema):
        return f"{database}.{schema}"
    return str(schema)


def _table_label(schema: str, table: str, database: str | None) -> str:
    return f"{_schema_label(schema, database)}.{table}"


__all__ = ["build_partitioned_queryout"]
