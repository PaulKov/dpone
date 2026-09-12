"""File export helpers for PostgreSQL source strategies."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from psycopg import sql

from dpone.config.postgres_mssql_wire_contract import normalize_postgres_mssql_wire
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.partitioning import RangePartitioner
from dpone.runtime.partitioning_options import PartitioningOptionsResolver
from dpone.runtime.sources.strategies.postgres.postgres_batched_export_mixin import PostgresBatchedExportMixin
from dpone.runtime.sources.strategies.postgres.postgres_partition_export_mixin import PostgresPartitionExportMixin
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import rollback_preserving_primary
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.sources.strategies.postgres.postgres_whole_file_export_service import (
    PostgresWholeFileExportService,
)
from dpone.runtime.support.bulk_text_codec import BulkTextCodec, is_bulk_text_type
from dpone.runtime.support.mssql_hex_binary import is_mssql_hex_binary_wire_type


class PostgresFileExportMixin(PostgresBatchedExportMixin, PostgresPartitionExportMixin):
    if TYPE_CHECKING:
        # Compile-time host contract for cooperative mixin composition.  These
        # declarations intentionally do not exist at runtime, so the concrete
        # strategy remains the sole DI authority and MRO behavior is unchanged.
        connector: Any
        logger: Any

        def _targets_mssql(self, load_config: Any) -> bool: ...

        def _new_extraction_lifecycle(self) -> ExtractionLifecycleAuthority: ...

        def _begin_exported_snapshot(self, lifecycle: ExtractionLifecycleAuthority) -> str: ...

        def _begin_repeatable_read_snapshot(
            self,
            lifecycle: ExtractionLifecycleAuthority,
        ) -> PostgresRepeatableReadSnapshotLease: ...

        def _render_query(self, connector: Any, query: Any) -> str: ...

        def _verify_postgres_source_authority(
            self,
            snapshot_lease: PostgresRepeatableReadSnapshotLease,
            load_config: Any,
        ) -> Any: ...

    def _export_to_file(
        self,
        query,
        schema,
        load_config,
        batch_size: int,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None = None,
        prepared_boundary: Any | None = None,
        relation_schema: list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
        query_params: tuple[object, ...] = (),
        after_copy: Callable[[], None] | None = None,
    ):
        """
        Router для выбора режима экспорта.

        Args:
            query: psycopg.sql.Composed запрос
            schema: Схема таблицы [(column, dtype), ...]
            load_config: Конфигурация с export_format и compress_export
            batch_size: Размер батча для separate mode

        Returns:
            FileExportArtifact или BatchedFileExportArtifact в зависимости от batch_commit_mode
        """
        targets_mssql = self._targets_mssql(load_config)
        wire_policy = normalize_postgres_mssql_wire(load_config) if targets_mssql else None
        options = getattr(load_config, "options", {}) or {}
        partition_options = PartitioningOptionsResolver.resolve(options)
        partition_candidate = bool(partition_options.column) and (
            partition_options.num_partitions > 1
            or (partition_options.bounds == "auto" and partition_options.target_rows_per_partition is not None)
        )
        batch_commit_mode = options.get("batch_commit_mode", "separate")
        if wire_policy is not None:
            batch_commit_mode = wire_policy.batch_commit_mode
        if prepared_boundary is not None and (partition_candidate or batch_commit_mode != "whole"):
            from dpone.runtime.errors import RuntimeConfigurationError

            code = "DPONE_POSTGRES_MSSQL_SOURCE_SCHEMA_AUTHORITY_EXPORT_PROFILE_UNSUPPORTED"
            error = RuntimeConfigurationError(code)
            setattr(error, "code", code)
            raise error
        if partition_candidate:
            if snapshot_lease is None:
                lifecycle = self._new_extraction_lifecycle()
                snapshot_token = self._begin_exported_snapshot(lifecycle)
            else:
                snapshot_lease.require_for(self.connector)
                self._verify_postgres_source_authority(
                    snapshot_lease,
                    load_config,
                )
                lifecycle = snapshot_lease.lifecycle
                rows = self.connector.get_records(
                    "SELECT pg_export_snapshot() AS snapshot_token",
                    params=None,
                    as_dict=True,
                )
                snapshot_token = str(rows[0].get("snapshot_token") or "") if rows else ""
                if not snapshot_token:
                    raise ValueError("postgres_extraction_exported_snapshot_unavailable")
            coordinator_open = True
            try:
                partitioner = RangePartitioner.from_options(
                    options,
                    bounds_resolver=lambda column: self._resolve_partition_bounds(query, column),
                )
                if partitioner.enabled:
                    return self._export_to_file_partitioned(
                        query,
                        schema,
                        load_config,
                        partitioner,
                        extraction_lifecycle=lifecycle,
                        snapshot_token=snapshot_token,
                        snapshot_lease=snapshot_lease,
                    )
                self.connector.rollback()
                coordinator_open = False
            except BaseException as primary:
                if coordinator_open:
                    rollback_preserving_primary(self.connector, primary)
                raise

        if batch_commit_mode == "separate":
            return self._export_to_file_batched(
                query,
                schema,
                load_config,
                batch_size,
                snapshot_lease=snapshot_lease,
            )
        elif batch_commit_mode == "whole":
            return self._export_to_file_whole(
                query,
                schema,
                load_config,
                snapshot_lease=snapshot_lease,
                prepared_boundary=prepared_boundary,
                relation_schema=relation_schema,
                query_params=query_params,
                after_copy=after_copy,
            )
        else:
            raise ValueError(f"Unknown batch_commit_mode: {batch_commit_mode}. Valid options: 'whole', 'separate'")

    def _export_to_file_whole(
        self,
        query,
        schema,
        load_config,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None = None,
        prepared_boundary: Any | None = None,
        relation_schema: list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
        query_params: tuple[object, ...] = (),
        after_copy: Callable[[], None] | None = None,
    ) -> FileExportArtifact:
        """Export a full-schema file under an owned or typed external lease."""

        return PostgresWholeFileExportService(self).export_full(
            query,
            schema,
            load_config,
            snapshot_lease=snapshot_lease,
            prepared_boundary=prepared_boundary,
            relation_schema=relation_schema,
            query_params=query_params,
            after_copy=after_copy,
        )

    def _export_key_snapshot_file(
        self,
        query,
        load_config,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease,
    ) -> FileExportArtifact:
        """Export exact reconciliation keys with an internally derived contract."""

        return PostgresWholeFileExportService(self).export_exact_keys(
            query,
            load_config,
            snapshot_lease=snapshot_lease,
        )

    def _effective_postgres_file_wire(self, load_config) -> tuple[str, bool]:
        """Project public CSV authoring to the internal MSSQL-safe wire."""

        public_format = str(getattr(load_config, "export_format", "csv") or "csv").strip().lower()
        compress_export = bool(getattr(load_config, "compress_export", False))
        if not self._targets_mssql(load_config):
            return public_format, compress_export
        policy = normalize_postgres_mssql_wire(load_config)
        return policy.runtime_export_format, policy.compress_export

    def _prepare_copy_select_sql(
        self,
        select_sql: Any,
        schema: list[tuple[str, str]],
        load_config,
        *,
        format_name: str,
        bulk_text_codec: BulkTextCodec | None = None,
    ) -> Any:
        """Apply sink-specific SELECT rewrites before COPY TO STDOUT."""

        if bulk_text_codec is not None:
            return self._wrap_mssql_bulk_text_query(select_sql, schema, bulk_text_codec)
        if format_name == "csv" and self._targets_bigquery_export(load_config):
            return self._wrap_bigquery_csv_query(select_sql, schema)
        return select_sql

    def _targets_bigquery_export(self, load_config) -> bool:
        targets = getattr(self, "_targets_bigquery", None)
        return bool(callable(targets) and targets(load_config))

    def _wrap_bigquery_csv_query(self, query_sql: Any, schema: list[tuple[str, str]]) -> Any:
        """Encode BYTES columns as RFC 4648 base64 for BigQuery CSV load jobs.

        PostgreSQL COPY emits bytea as ``\\x…`` hex, which BigQuery rejects for
        BYTES. ``encode(col, 'base64')`` matches the BigQuery CSV wire format.
        """

        expressions = []
        for column, dtype in schema:
            source_column = f"dpone_src.{self._quote_postgres_identifier(column)}"
            if str(dtype).upper() == "BYTES":
                expression = f"encode({source_column}, 'base64')"
            else:
                expression = source_column
            expressions.append(f"{expression} AS {self._quote_postgres_identifier(column)}")
        if not isinstance(query_sql, sql.Composable):
            return f"SELECT {', '.join(expressions)} FROM ({query_sql}) AS dpone_src"
        inner = query_sql
        return sql.SQL("SELECT {} FROM ({}) AS dpone_src").format(
            sql.SQL(", ").join(sql.SQL(expression) for expression in expressions),
            inner,
        )

    @staticmethod
    def _attach_rows_exported(artifact, file_path: str, *, compressed: bool) -> None:
        """Expose row count for vendor-live asserts (mirrors MySQL export metadata)."""

        import csv
        import gzip

        try:
            opener = gzip.open if compressed else open
            if str(getattr(artifact, "format", "")).lower() in {
                "mssql-delimited",
                "mssql_delimited",
                "tsv",
                "tab",
            }:
                with opener(file_path, "rb") as handle:
                    rows = 0
                    saw_bytes = False
                    ended_with_terminator = True
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        saw_bytes = True
                        rows += chunk.count(b"\n")
                        ended_with_terminator = chunk.endswith(b"\n")
                    if saw_bytes and not ended_with_terminator:
                        rows += 1
            else:
                with opener(file_path, "rt", encoding="utf-8", newline="") as handle:
                    rows = sum(1 for _record in csv.reader(handle))
        except OSError as exc:
            raise ArtifactIntegrityError("artifact_integrity.row_count_unavailable") from exc
        artifact.rows_exported = int(rows)

    def _wrap_mssql_bulk_text_query(
        self,
        query_sql: Any,
        schema: list[tuple[str, str]],
        codec: BulkTextCodec,
    ) -> Any:
        """Wrap a SELECT so values are safe for MSSQL bcp character files.

        PostgreSQL COPY emits booleans as ``t``/``f``; SQL Server ``bit`` and
        ``bcp`` character mode expect ``0``/``1``. ``datetimeoffset`` needs a
        colonated offset (``+00:00``) rather than PostgreSQL's ``+00``.
        Binary columns project lowercase hex for the character wire.
        """

        expressions = []
        for column, dtype in schema:
            source_column = f"dpone_src.{self._quote_postgres_identifier(column)}"
            normalized = str(dtype).lower().strip()
            if normalized in {"bit", "bool"} or normalized.startswith(("bit(", "bool(")):
                expression = f"CASE WHEN {source_column} IS NULL THEN NULL WHEN {source_column} THEN '1' ELSE '0' END"
            elif normalized.startswith("datetimeoffset"):
                expression = (
                    f"CASE WHEN {source_column} IS NULL THEN NULL "
                    f"ELSE to_char({source_column} AT TIME ZONE 'UTC', "
                    f"'YYYY-MM-DD HH24:MI:SS.US+00:00') END"
                )
            elif "datetime64" in normalized and "utc" in normalized:
                expression = (
                    f"CASE WHEN {source_column} IS NULL THEN NULL "
                    f"ELSE to_char({source_column} AT TIME ZONE 'UTC', "
                    f"'YYYY-MM-DD HH24:MI:SS.US') END"
                )
            elif normalized.startswith("datetime64"):
                expression = (
                    f"CASE WHEN {source_column} IS NULL THEN NULL "
                    f"ELSE to_char({source_column}, 'YYYY-MM-DD HH24:MI:SS.US') END"
                )
            elif is_mssql_hex_binary_wire_type(dtype) or normalized in {"bytea"}:
                # Hex text + BulkTextCodec preserves NULL vs empty bytea on character BCP.
                expression = codec.postgres_encode_expression(f"encode({source_column}, 'hex')")
            elif is_bulk_text_type(dtype):
                expression = codec.postgres_encode_expression(source_column)
            else:
                expression = source_column
            expressions.append(f"{expression} AS {self._quote_postgres_identifier(column)}")
        if not isinstance(query_sql, sql.Composable):
            return f"SELECT {', '.join(expressions)} FROM ({query_sql}) AS dpone_src"
        inner = query_sql
        return sql.SQL("SELECT {} FROM ({}) AS dpone_src").format(
            sql.SQL(", ").join(sql.SQL(expression) for expression in expressions),
            inner,
        )

    def _resolve_partition_bounds(self, query, column: str) -> tuple[object, object, int | None, int]:
        """Resolve MIN/MAX/COUNT for partitioning.bounds=auto.

        The resolver works on the already filtered source query, so custom
        predicates and incremental predicates are part of the profiled window.
        """

        base_query = self._render_query(self.connector, query)
        quoted_column = self._quote_postgres_identifier(column)
        bounds_query = (
            f"SELECT MIN(dpone_bounds.{quoted_column}) AS lower_bound, "
            f"MAX(dpone_bounds.{quoted_column}) AS upper_bound, "
            f"COUNT(*) AS row_count, "
            f"SUM(CASE WHEN dpone_bounds.{quoted_column} IS NULL THEN 1 ELSE 0 END) AS null_count "
            f"FROM ({base_query}) AS dpone_bounds"
        )
        rows = self.connector.get_records(bounds_query, as_dict=True)
        if not rows:
            raise ValueError(f"Could not resolve partition bounds for PostgreSQL column: {column}")
        row = rows[0]
        if isinstance(row, dict):
            lower = row.get("lower_bound")
            upper = row.get("upper_bound")
            row_count = row.get("row_count")
            null_count = row.get("null_count")
        else:
            lower, upper, row_count = row[0], row[1], row[2]
            null_count = row[3] if len(row) > 3 else None
        if lower is None or upper is None:
            raise ValueError(f"PostgreSQL partition bounds are empty for column: {column}")
        return lower, upper, int(row_count) if row_count is not None else None, int(null_count or 0)

    @staticmethod
    def _quote_postgres_identifier(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'


__all__ = ["PostgresFileExportMixin"]
