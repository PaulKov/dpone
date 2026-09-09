"""Partitioned PostgreSQL file export helpers."""

from __future__ import annotations

import os
import tempfile
from hashlib import sha256
from typing import Any

from psycopg import sql

from dpone.ports.partition_clone import PartitionCloneFactory
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.file_artifacts import (
    FileExportArtifact,
    PartitionedFileExportArtifact,
)
from dpone.runtime.lineage.partition_checkpoint import build_transfer_partition_id
from dpone.runtime.owned_file_scope import OwnedFileScope
from dpone.runtime.parallel import BoundedParallelMapExecutor
from dpone.runtime.partitioning import RangePartitioner
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.storage_policy import RuntimeStoragePolicy
from dpone.runtime.support.bulk_text_codec import BulkTextCodec


class PostgresPartitionExportMixin:
    def _export_to_file_partitioned(
        self,
        query,
        schema,
        load_config,
        partitioner: RangePartitioner,
        *,
        extraction_lifecycle: ExtractionLifecycleAuthority,
        snapshot_token: str,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None = None,
    ) -> PartitionedFileExportArtifact:
        """Export a query as independent range files using native PostgreSQL COPY."""

        tmp_dir = RuntimeStoragePolicy.from_options(load_config.options).work_dir
        tmp_dir.mkdir(parents=True, exist_ok=True)
        export_format, compress_export = self._effective_postgres_file_wire(load_config)
        compress_level = int(os.environ.get("DPONE_EXPORT_GZIP_LEVEL", "1"))
        buffer_size = int(os.environ.get("DPONE_EXPORT_BUFFER_SIZE", str(16 * 1024 * 1024)))

        if export_format in {"mssql-delimited", "mssql_delimited"}:
            suffix = ".bcp.gz" if compress_export else ".bcp"
            format_name = "mssql-delimited"
            copy_format = "MSSQL_DELIMITED"
        elif export_format == "binary":
            suffix = ".bin.gz" if compress_export else ".bin"
            format_name = "binary"
            copy_format = "BINARY"
        else:
            suffix = ".csv.gz" if compress_export else ".csv"
            format_name = "csv"
            copy_format = "CSV"

        base_query = self._render_query(self.connector, query)
        bulk_text_codec = BulkTextCodec() if format_name == "mssql-delimited" else None
        quoted_column = f"dpone_partitioned.{self._quote_postgres_identifier(partitioner.column)}"
        columns = [column for column, _ in schema]
        partitions = partitioner.partitions()
        if snapshot_lease is not None:
            self._verify_postgres_source_authority(
                snapshot_lease,
                load_config,
            )
        query_hash = _hash_text(base_query)
        schema_hash = _hash_text(repr(tuple(schema)))
        source_table = _qualified_table(load_config, "source")
        target_table = _qualified_table(load_config, "target")
        strategy = _strategy(load_config)
        owned_files = OwnedFileScope()
        lifecycle = extraction_lifecycle
        coordinator_open = True

        self.logger.log_etl_progress(
            "POSTGRES_PARTITIONED_COPY_START",
            {
                "Column": partitioner.column,
                "Partitions": len(partitions),
                "Workers": partitioner.max_workers,
                "Format": copy_format,
            },
        )

        def export_partition(partition) -> FileExportArtifact:
            tmp_file = tempfile.NamedTemporaryFile(
                prefix=f"dpone_export_p{partition.index}_",
                suffix=suffix,
                dir=tmp_dir,
                delete=False,
            )
            tmp_file_path = owned_files.register(tmp_file.name)
            tmp_file.close()
            connector = PartitionCloneFactory().clone(self.connector, partition.index)
            worker_transaction = False
            try:
                if connector is not self.connector:
                    connector.begin()
                    worker_transaction = True
                    connector.execute_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    connector.execute_query(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot_token)))
                partition_query = partitioner.wrap_query(base_query, quoted_column, partition)
                partition_query = self._prepare_copy_select_sql(
                    partition_query,
                    schema,
                    load_config,
                    format_name=format_name,
                    bulk_text_codec=bulk_text_codec,
                )
                connector.copy_to_file(
                    query_sql=partition_query,
                    output_path=tmp_file_path,
                    format=copy_format,
                    compress=compress_export,
                    compress_level=compress_level,
                    buffer_size=buffer_size,
                    logger=self.logger,
                )
                artifact = FileExportArtifact(
                    file_path=tmp_file_path,
                    columns=columns,
                    compressed=compress_export,
                    format=format_name,
                    bulk_text_codec=bulk_text_codec,
                )
                self._attach_rows_exported(
                    artifact,
                    tmp_file_path,
                    compressed=compress_export,
                )
                _attach_partition_metadata(
                    artifact,
                    source_table=source_table,
                    target_table=target_table,
                    strategy=strategy,
                    query_hash=query_hash,
                    schema_hash=schema_hash,
                    partition=partition,
                )
                if worker_transaction:
                    connector.commit_transaction()
                    worker_transaction = False
                return artifact
            except BaseException:
                if worker_transaction:
                    connector.rollback()
                try:
                    os.remove(tmp_file_path)
                except OSError:
                    pass
                raise
            finally:
                if connector is not self.connector and hasattr(connector, "close"):
                    connector.close()

        try:
            if partitioner.max_workers == 1:
                artifacts = [export_partition(partition) for partition in partitions]
            else:
                artifacts = BoundedParallelMapExecutor(partitioner.max_workers).map(export_partition, partitions)
        except BaseException:
            if coordinator_open:
                self.connector.rollback()
                coordinator_open = False
            owned_files.cleanup()
            raise
        try:
            self.logger.log_etl_progress(
                "POSTGRES_PARTITIONED_COPY_COMPLETE",
                {
                    "Files": len(artifacts),
                    "Load_Workers": partitioner.load_workers,
                },
            )
            lifecycle.complete()
            self.connector.commit_transaction()
            coordinator_open = False
        except BaseException:
            if coordinator_open:
                self.connector.rollback()
            owned_files.cleanup()
            raise
        for artifact in artifacts:
            owned_files.transfer(artifact.file_path)
        result_artifact = PartitionedFileExportArtifact(
            partitions=artifacts,
            columns=columns,
            max_workers=partitioner.load_workers,
        )
        result_artifact.bind_extraction_lifecycle(lifecycle)
        for partition in artifacts:
            partition.bind_extraction_lifecycle(lifecycle)
        return result_artifact


__all__ = ["PostgresPartitionExportMixin"]


def _hash_text(value: str) -> str:
    return f"sha256:{sha256(value.encode('utf-8')).hexdigest()}"


def _qualified_table(load_config: Any, role: str) -> str:
    schema = getattr(load_config, f"{role}_schema", None) or "unknown"
    table = getattr(load_config, f"{role}_table", None) or "unknown"
    return f"{schema}.{table}"


def _strategy(load_config: Any) -> str:
    strategy = getattr(load_config, "load_strategy", None)
    return str(getattr(strategy, "value", strategy or "unknown"))


def _attach_partition_metadata(
    artifact: FileExportArtifact,
    *,
    source_table: str,
    target_table: str,
    strategy: str,
    query_hash: str,
    schema_hash: str,
    partition,
) -> None:
    bounds = {
        "index": partition.index,
        "lower": partition.lower_bound,
        "upper": partition.upper_bound,
        "include_upper": partition.include_upper,
    }
    metadata = {
        "partition_bounds": bounds,
        "query_hash": query_hash,
        "schema_hash": schema_hash,
        "source_table": source_table,
        "target_table": target_table,
        "strategy": strategy,
        "transfer_partition_id": build_transfer_partition_id(
            source_table=source_table,
            target_table=target_table,
            strategy=strategy,
            query_hash=query_hash,
            schema_hash=schema_hash,
            partition_bounds=bounds,
        ),
    }
    for key, value in metadata.items():
        setattr(artifact, key, value)
