"""Cohesive whole-file export service for PostgreSQL source strategies."""

from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import Callable, Sequence
from importlib import import_module
from typing import TYPE_CHECKING, Any

from dpone.runtime.file_artifacts import CompletedFileWrite, FileExportArtifact
from dpone.runtime.owned_file_scope import OwnedFileScope
from dpone.runtime.sources.strategies.postgres.postgres_mssql_source_value_guard import (
    PostgresMssqlSourceValueGuard,
)
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
    PreparedPostgresSourceBoundary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_key_schema import (
    project_snapshot_key_schema,
    resolve_snapshot_unique_key,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.storage_policy import RuntimeStoragePolicy
from dpone.runtime.support.bulk_text_codec import BulkTextCodec

if TYPE_CHECKING:
    from dpone.readiness.schema_contracts import SchemaContract


class PostgresWholeFileExportService:
    """Own transaction, file, and contract boundaries for one eager export."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy

    def export_full(
        self,
        query: Any,
        schema: list[tuple[str, str]],
        load_config: Any,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None = None,
        prepared_boundary: PreparedPostgresSourceBoundary | None = None,
        relation_schema: Sequence[tuple[str, str]] | None = None,
        query_params: tuple[object, ...] = (),
        after_copy: Callable[[], None] | None = None,
    ) -> FileExportArtifact:
        """Validate the complete configured contract without subset overrides."""

        return self._export(
            query,
            schema,
            load_config,
            snapshot_lease=snapshot_lease,
            prepared_boundary=prepared_boundary,
            schema_contract=None,
            relation_schema=relation_schema,
            query_params=query_params,
            after_copy=after_copy,
        )

    def export_exact_keys(
        self,
        query: Any,
        load_config: Any,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease,
    ) -> FileExportArtifact:
        """Derive exact keys and their partial contract inside one RR lease."""

        if not isinstance(snapshot_lease, PostgresRepeatableReadSnapshotLease):
            raise TypeError("postgres_file_export.snapshot_lease_required")
        snapshot_lease.require_for(self._strategy.connector)
        key_columns = resolve_snapshot_unique_key(load_config)
        projection = self._strategy.fetch_schema_projection(load_config)
        source_schema = list(projection.projected_schema)
        key_schema = project_snapshot_key_schema(source_schema, key_columns)
        key_relation_schema = project_snapshot_key_schema(
            list(projection.relation_schema),
            key_columns,
        )
        options = getattr(load_config, "options", {}) or {}
        raw = options.get("schema_contract")
        contract_type = import_module("dpone.readiness.schema_contracts").SchemaContract
        full_contract = contract_type.from_config(raw if isinstance(raw, dict) else {})
        key_contract = import_module("dpone.runtime.etl.file_contract_validation").project_key_snapshot_schema_contract(
            full_contract,
            key_columns=key_columns,
            source_schema=source_schema,
        )
        return self._export(
            query,
            key_schema,
            load_config,
            snapshot_lease=snapshot_lease,
            schema_contract=key_contract,
            relation_schema=key_relation_schema,
        )

    def _export(
        self,
        query: Any,
        schema: list[tuple[str, str]],
        load_config: Any,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None,
        prepared_boundary: PreparedPostgresSourceBoundary | None = None,
        schema_contract: SchemaContract | None,
        relation_schema: Sequence[tuple[str, str]] | None,
        query_params: tuple[object, ...] = (),
        after_copy: Callable[[], None] | None = None,
    ) -> FileExportArtifact:
        strategy = self._strategy
        params = query_params if isinstance(query_params, tuple) else tuple(query_params)
        if prepared_boundary is not None and type(prepared_boundary) is not PreparedPostgresSourceBoundary:
            raise TypeError("postgres_file_export.prepared_boundary_required")
        if prepared_boundary is not None and snapshot_lease is not None:
            raise TypeError("postgres_file_export.ambiguous_snapshot_authority")
        if snapshot_lease is not None:
            if not isinstance(snapshot_lease, PostgresRepeatableReadSnapshotLease):
                raise TypeError("postgres_file_export.snapshot_lease_required")
            snapshot_lease.require_for(strategy.connector)

        export_format, compress_export = strategy._effective_postgres_file_wire(load_config)
        suffix, format_name, copy_format = _wire_file_shape(export_format, compress_export)
        owned_files = OwnedFileScope()
        owns_transaction = snapshot_lease is None and prepared_boundary is None
        lifecycle = None
        transaction_open = False
        try:
            lifecycle = (
                prepared_boundary.lifecycle_for_copy()
                if prepared_boundary is not None
                else snapshot_lease.lifecycle
                if snapshot_lease is not None
                else strategy._new_extraction_lifecycle()
            )
            if owns_transaction:
                snapshot_lease = strategy._begin_repeatable_read_snapshot(lifecycle)
                transaction_open = True
            elif snapshot_lease is not None:
                snapshot_lease.require_for(strategy.connector)
            if snapshot_lease is not None:
                strategy._verify_postgres_source_authority(
                    snapshot_lease,
                    load_config,
                )
            columns = [column for column, _dtype in schema]
            select_sql = query
            copy_guard = None
            if strategy._targets_mssql(load_config):
                if relation_schema is None:
                    raise TypeError("postgres_mssql.source_value_guard_relation_schema_required")
                copy_guard = PostgresMssqlSourceValueGuard(strategy.connector).guard_copy_query(
                    select_sql,
                    relation_schema,
                )
                select_sql = copy_guard.query_sql
            tmp_dir = RuntimeStoragePolicy.from_options(load_config.options).work_dir
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_file = tempfile.NamedTemporaryFile(
                prefix="dpone_export_",
                suffix=suffix,
                dir=tmp_dir,
                delete=False,
            )
            tmp_file_path = owned_files.register(tmp_file.name)
            tmp_file.close()
            bulk_text_codec = BulkTextCodec() if format_name == "mssql-delimited" else None
            select_sql = strategy._prepare_copy_select_sql(
                select_sql,
                schema,
                load_config,
                format_name=format_name,
                bulk_text_codec=bulk_text_codec,
            )
            if prepared_boundary is not None:
                prepared_boundary.require_active_for_copy(strategy.connector)
            compress_level = int(os.environ.get("DPONE_EXPORT_GZIP_LEVEL", "1"))
            buffer_size = int(os.environ.get("DPONE_EXPORT_BUFFER_SIZE", str(16 * 1024 * 1024)))
            strategy.logger.log_etl_progress(
                "POSTGRES_COPY_START",
                {
                    "Format": copy_format,
                    "Compression": "gzip" if compress_export else "none",
                    "Compression Level": compress_level if compress_export else "N/A",
                    "Buffer Size": f"{buffer_size / 1024 / 1024:.1f}MB",
                    "Mode": "whole",
                },
            )
            if prepared_boundary is not None:
                prepared_boundary.require_active_for_copy(strategy.connector)
            try:
                stats = strategy.connector.copy_to_file(
                    query_sql=select_sql,
                    output_path=tmp_file_path,
                    format=copy_format,
                    compress=compress_export,
                    compress_level=compress_level,
                    buffer_size=buffer_size,
                    logger=strategy.logger,
                    params=params,
                )
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
                cancellation.__cause__ = cancellation.__context__ = None
                raise
            except BaseException as primary:
                translated = copy_guard.translated_error(primary) if copy_guard is not None else None
                if translated is not None:
                    raise translated from None
                raise
            strategy.logger.log_etl_progress(
                "POSTGRES_COPY_COMPLETE",
                _copy_completion_progress(stats),
            )
            if after_copy is not None:
                after_copy()
            if prepared_boundary is not None:
                prepared_boundary.require_active_for_copy(strategy.connector)
            streamed_rows = (
                int(stats["rows_exported"])
                if format_name == "mssql-delimited" and stats.get("rows_exported") is not None
                else None
            )
            streamed_sha256 = stats.get("sha256") if not compress_export else None
            if isinstance(streamed_sha256, str) and streamed_sha256:
                artifact = FileExportArtifact(
                    file_path=tmp_file_path,
                    columns=columns,
                    compressed=False,
                    format=format_name,
                    bulk_text_codec=bulk_text_codec,
                    _completed_write=CompletedFileWrite(
                        sha256=streamed_sha256,
                        size_bytes=int(stats["total_bytes"]),
                        rows_exported=streamed_rows,
                    ),
                )
            else:
                artifact = FileExportArtifact(
                    file_path=tmp_file_path,
                    columns=columns,
                    compressed=compress_export,
                    format=format_name,
                    bulk_text_codec=bulk_text_codec,
                    rows_exported=streamed_rows,
                )
            if artifact.rows_exported is None:
                strategy._attach_rows_exported(artifact, tmp_file_path, compressed=compress_export)
            _attach_contract_validation(
                strategy,
                artifact,
                schema,
                load_config,
                schema_contract=schema_contract,
            )
            if prepared_boundary is not None:
                prepared_boundary.require_active_for_copy(strategy.connector)
            if owns_transaction:
                lifecycle.complete()
            artifact.bind_extraction_lifecycle(lifecycle)
            if prepared_boundary is not None:
                prepared_boundary.complete(artifact)
            elif owns_transaction:
                strategy.connector.commit_transaction()
                transaction_open = False
            owned_files.transfer(tmp_file_path)
            return artifact
        except BaseException as primary:
            try:
                if prepared_boundary is not None:
                    prepared_boundary.abort_preserving(primary)
                elif transaction_open:
                    rollback_preserving_primary(strategy.connector, primary)
            finally:
                owned_files.cleanup()
            raise


def _copy_completion_progress(stats: dict[str, Any]) -> dict[str, object]:
    """Map physical COPY evidence to unambiguous operator-facing metrics."""

    return {
        "Total Bytes": f"{stats['total_bytes'] / 1024 / 1024:.1f}MB",
        "Rows Exported": stats["rows_exported"] if stats.get("rows_exported") is not None else "N/A",
        "Total COPY Reads": stats.get("copy_read_count", stats["chunk_count"]),
        "Duration": f"{stats['elapsed']:.1f}s",
        "Avg Throughput": f"{stats['throughput']:.1f}MB/s",
    }


def _wire_file_shape(export_format: str, compressed: bool) -> tuple[str, str, str]:
    if export_format == "binary":
        return (".bin.gz" if compressed else ".bin", "binary", "BINARY")
    if export_format in {"mssql-delimited", "mssql_delimited"}:
        return (".bcp.gz" if compressed else ".bcp", "mssql-delimited", "MSSQL_DELIMITED")
    return (".csv.gz" if compressed else ".csv", "csv", "CSV")


def _attach_contract_validation(
    strategy: Any,
    artifact: FileExportArtifact,
    schema: list[tuple[str, str]],
    load_config: Any,
    *,
    schema_contract: SchemaContract | None,
) -> None:
    if not strategy._targets_mssql(load_config):
        return
    contract_type = import_module("dpone.readiness.schema_contracts").SchemaContract
    if schema_contract is not None and not isinstance(schema_contract, contract_type):
        raise TypeError("postgres_file_export.schema_contract_projection_invalid")
    options = getattr(load_config, "options", {}) or {}
    raw = options.get("schema_contract")
    contract = schema_contract or contract_type.from_config(raw if isinstance(raw, dict) else {})
    if not contract.columns:
        return
    import_module("dpone.runtime.etl.file_contract_validation").validate_mssql_delimited_file_contract(
        artifact,
        schema=tuple((str(name), str(dtype)) for name, dtype in schema),
        contract=contract,
    )


__all__ = ["PostgresWholeFileExportService"]
