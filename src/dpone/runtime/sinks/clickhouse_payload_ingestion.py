"""ClickHouse payload ingestion helpers for file, partitioned, and row artifacts."""

from __future__ import annotations

import csv
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact
from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullInsertPolicy
from dpone.runtime.sinks.clickhouse_payload_support import (
    columnar_direct_push_loader,
    columnar_pull_loader,
    is_local_columnar_chunked_artifact,
    is_local_columnar_staging_manifest,
    is_object_storage_columnar_chunked_artifact,
    is_object_storage_staging_manifest,
    is_source_native_artifact,
    native_wire_transcoder,
)
from dpone.runtime.sinks.clickhouse_row_values import ClickHouseRowValueCoercer
from dpone.runtime.sinks.clickhouse_tsv_formats import CLICKHOUSE_TSV_ARTIFACT_FORMATS
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.source_materialization import PreparedSourceArtifact
from dpone.runtime.sql_query_artifact import SqlQueryArtifact
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypeMapper, MssqlClickHouseTypePolicy

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class ClickHousePayloadIngestionService:
    """Load prepared runtime artifacts into a ClickHouse staging/target table."""

    def __init__(self, sink: Any, *, sink_factory: Any) -> None:
        self._sink = sink
        self._sink_factory = sink_factory
        self._row_value_coercer = ClickHouseRowValueCoercer()

    def insert_payload(self, load_config: LoadConfig, payload: LoadPayload) -> int:
        if isinstance(payload.artifact, PreparedSourceArtifact):
            return payload.artifact.load_with(
                lambda artifact: self.insert_payload(
                    load_config,
                    payload.rebind(artifact=artifact),
                )
            )
        if is_object_storage_columnar_chunked_artifact(payload.artifact):
            return self.insert_object_storage_chunked(load_config, payload.artifact, payload.schema)
        if is_object_storage_staging_manifest(payload.artifact):
            return self.insert_object_storage_manifest(load_config, payload.artifact, payload.schema)
        if is_local_columnar_chunked_artifact(payload.artifact):
            return self.insert_local_columnar_chunked(load_config, payload.artifact, payload.schema)
        if is_local_columnar_staging_manifest(payload.artifact):
            return self.insert_local_columnar_manifest(load_config, payload.artifact, payload.schema)
        if isinstance(payload.artifact, SqlQueryArtifact):
            return self.insert_sql_query(load_config, payload.artifact, payload.schema)
        if isinstance(payload.artifact, FileExportArtifact):
            return self.insert_file(load_config, payload.artifact, payload.schema)
        if isinstance(payload.artifact, PartitionedFileExportArtifact):
            use_parallel_connections = payload.artifact.max_workers > 1 and getattr(load_config, "options", {}).get(
                "clickhouse_parallel_connections", True
            )
            return payload.artifact.load_with(
                lambda file_artifact: self.insert_partition_file(
                    load_config,
                    file_artifact,
                    payload.schema,
                    use_parallel_connections=use_parallel_connections,
                )
            )
        if isinstance(payload.artifact, PhysicalChunkedFileExportArtifact):
            return payload.artifact.load_with(
                lambda file_artifact: self.insert_partition_file(
                    load_config,
                    file_artifact,
                    payload.schema,
                    use_parallel_connections=False,
                )
            )
        if isinstance(payload.artifact, PartitionedTransferPlanArtifact):
            return payload.artifact.load_with(
                lambda file_artifact: self.insert_partition_file(
                    load_config,
                    file_artifact,
                    payload.schema,
                    use_parallel_connections=False,
                    count_delta=True,
                ),
                stream_loader=lambda stream_artifact: self.insert_partition_byte_stream(
                    load_config,
                    stream_artifact,
                    payload.schema,
                ),
            )
        if isinstance(payload.artifact, ByteStreamArtifact):
            return self.insert_byte_stream(load_config, payload.artifact, payload.schema)
        if isinstance(payload.artifact, InMemoryRowsArtifact):
            return self.insert_rows(load_config, payload.artifact._rows, payload.schema)
        if isinstance(payload.artifact, StreamingRowsArtifact):
            return self._insert_streaming_rows(load_config, payload.artifact, payload.schema)
        staging = payload.artifact.materialize(cast(Any, self._sink), load_config, payload.schema)
        return staging.row_count

    def insert_object_storage_manifest(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        loader = columnar_pull_loader(self._sink)
        return loader.load(load_config, artifact, schema)

    def insert_object_storage_chunked(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        loader = columnar_pull_loader(self._sink)
        return loader.load_windowed(load_config, artifact, schema)

    def insert_local_columnar_manifest(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        loader = columnar_direct_push_loader(self._sink)
        return loader.load(load_config, artifact, schema)

    def insert_local_columnar_chunked(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        loader = columnar_direct_push_loader(self._sink)
        return loader.load_chunked(load_config, artifact, schema)

    def insert_partition_file(
        self,
        load_config: LoadConfig,
        artifact: FileExportArtifact,
        schema: Sequence[tuple[str, str]],
        *,
        use_parallel_connections: bool,
        count_delta: bool = False,
    ) -> int:
        if use_parallel_connections:
            connector = self._sink._clone_connector()
            cloned_sink = self._sink_factory(connector)
            return cloned_sink._payload_ingestion.insert_file(load_config, artifact, schema)
        if count_delta:
            return self._insert_with_staging_delta(load_config, lambda: self.insert_file(load_config, artifact, schema))
        return self.insert_file(load_config, artifact, schema)

    def insert_file(
        self,
        load_config: LoadConfig,
        artifact: FileExportArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        if is_source_native_artifact(artifact):
            stream_artifact = native_wire_transcoder().to_clickhouse_binary(
                artifact,
                schema,
                clickhouse_schema=self._clickhouse_schema(load_config, schema),
                type_policy=self._type_policy(load_config),
            )
            return self.insert_byte_stream(load_config, stream_artifact, schema)
        if artifact.format in {"mssql-native", "mssql-bcp-native"}:
            raise ValueError("clickhouse_mssql_native_requires_native_wire_contract")
        if artifact.compressed:
            raise ValueError("ClickHouseSink cannot read gzip artifacts directly in this MVP; disable compress_export.")
        columns = [column for column, _ in schema]
        delimiter = "\t" if artifact.format in CLICKHOUSE_TSV_ARTIFACT_FORMATS else ","
        ClickHouseNullInsertPolicy.from_load_config(load_config).validate_delimited_file(
            artifact.file_path,
            columns,
            delimiter=delimiter,
        )
        if self._sink._should_use_http_bulk(load_config, artifact):
            return self._sink._insert_file_with_http(load_config, artifact, schema)
        if self._sink._should_use_client_bulk(load_config, artifact):
            return self._sink._insert_file_with_client(load_config, artifact, schema)
        total = 0
        with open(artifact.file_path, encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            batch: list[tuple[Any, ...]] = []
            for row in reader:
                batch.append(self._sink._coerce_file_row(row, schema))
                if len(batch) >= load_config.batch_size:
                    total += self.execute_insert(load_config, columns, batch)
                    batch.clear()
            if batch:
                total += self.execute_insert(load_config, columns, batch)
        return total

    def insert_byte_stream(
        self,
        load_config: LoadConfig,
        artifact: ByteStreamArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        if artifact.format not in {*CLICKHOUSE_TSV_ARTIFACT_FORMATS, "clickhouse-rowbinary", "clickhouse-native"}:
            raise ValueError("ClickHouseSink only supports certified native ByteStreamArtifact formats")
        if (
            artifact.format in {"clickhouse-rowbinary", "clickhouse-native"}
            and getattr(artifact, "bulk_wire_contract", None) is None
        ):
            raise ValueError(f"{artifact.format} ByteStreamArtifact requires a bulk_wire_contract")
        if self._sink._should_use_client_stream(load_config, artifact):
            return self._sink._insert_stream_with_client(load_config, artifact, schema)
        if not self._sink._should_use_http_stream(load_config, artifact):
            raise RuntimeError("byte_stream_artifact_requires_clickhouse_http_or_client_bulk")
        return self._sink._insert_stream_with_http(load_config, artifact, schema)

    def insert_partition_byte_stream(
        self,
        load_config: LoadConfig,
        artifact: ByteStreamArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        return self._insert_with_staging_delta(
            load_config, lambda: self.insert_byte_stream(load_config, artifact, schema)
        )

    def insert_sql_query(
        self,
        load_config: LoadConfig,
        artifact: SqlQueryArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        if artifact.dialect != "clickhouse":
            raise ValueError("clickhouse_sql_query_artifact_requires_clickhouse_dialect")
        columns = [column for column, _ in schema]
        column_sql = ", ".join(f"`{column}`" for column in columns)
        select_sql = ", ".join(f"`{column}`" for column in columns)
        settings_clause = ClickHouseNullInsertPolicy.from_load_config(load_config).insert_select_settings_clause()
        lifecycle = artifact.extraction_lifecycle
        if lifecycle is not None and lifecycle.receipt is None:
            lifecycle.acquire()
        self._sink.connector.execute_query(
            f"INSERT INTO {self._sink._table(load_config)} ({column_sql}){settings_clause} "
            f"SELECT {select_sql} FROM ({artifact.sql}) AS dpone_sql_query"
        )
        if lifecycle is not None:
            lifecycle.complete()
        # Promote independent source authority onto the extract artifact. Prefer the
        # extract-time COUNT (estimated_rows / rows_exported); never use staging
        # table counts here — those are target authority for source_target_count.
        exported = getattr(artifact, "rows_exported", None)
        if exported is None:
            exported = artifact.estimated_rows
        if exported is None:
            exported = self._count_sql_query(artifact.sql)
        if exported is not None:
            setattr(artifact, "rows_exported", int(exported))
            setattr(artifact, "row_count", int(exported))
        if artifact.estimated_rows is not None:
            return artifact.estimated_rows
        return self._sink._count(load_config)

    def _count_sql_query(self, sql: str) -> int | None:
        """Independent source COUNT for SqlQueryArtifact authority (not staging)."""

        try:
            rows = self._sink.connector.get_records(f"SELECT count() FROM ({sql}) AS dpone_sql_query")
        except Exception:
            return None
        if not rows:
            return None
        first = rows[0]
        value = first[0] if isinstance(first, (tuple, list)) else first
        try:
            count = int(value)
        except (TypeError, ValueError):
            return None
        return count if count >= 0 else None

    def insert_rows(
        self,
        load_config: LoadConfig,
        rows: Iterable[Mapping[str, Any]],
        schema: Sequence[tuple[str, str]],
    ) -> int:
        # Coercer expects ClickHouse target types; source schemas (MSSQL/MySQL/PG)
        # are mapped first so TIME/uniqueidentifier wire values land correctly.
        mapped_schema = self._clickhouse_schema(load_config, schema)
        columns = [column for column, _ in mapped_schema]
        column_types = [column_type for _, column_type in mapped_schema]
        batch = [
            self._row_value_coercer.coerce_row(
                tuple(row.get(column) for column in columns),
                column_types,
            )
            for row in rows
        ]
        if not batch:
            return 0
        return self.execute_insert(load_config, columns, batch)

    def _insert_with_staging_delta(self, load_config: LoadConfig, insert: Callable[[], int]) -> int:
        before = int(self._sink._count(load_config))
        reported = int(insert() or 0)
        after = int(self._sink._count(load_config))
        if after >= before and (reported == after or (reported == 0 and after > before)):
            return after - before
        return reported

    def execute_insert(self, load_config: LoadConfig, columns: Sequence[str], rows: Sequence[tuple[Any, ...]]) -> int:
        column_sql = ", ".join(f"`{column}`" for column in columns)
        policy = ClickHouseNullInsertPolicy.from_load_config(load_config)
        policy.validate_rows(columns, rows)
        settings = policy.driver_settings()
        kwargs = {"settings": settings} if settings else {}
        self._sink.connector.connection.execute(
            f"INSERT INTO {self._sink._table(load_config)} ({column_sql}) VALUES",
            list(rows),
            **kwargs,
        )
        return len(rows)

    def _clickhouse_schema(
        self,
        load_config: LoadConfig,
        schema: Sequence[tuple[str, str]],
    ) -> tuple[tuple[str, str], ...]:
        type_mapper = MssqlClickHouseTypeMapper(self._type_policy(load_config))
        map_column = getattr(self._sink, "_column_type", None)
        if not callable(map_column):
            # Thin test doubles and identity schemas may already carry ClickHouse types.
            return tuple((column, dtype) for column, dtype in schema)
        return tuple((column, map_column(load_config, type_mapper, column, dtype)) for column, dtype in schema)

    @staticmethod
    def _type_policy(load_config: LoadConfig) -> MssqlClickHouseTypePolicy:
        return MssqlClickHouseTypePolicy.from_config((load_config.options or {}).get("type_fidelity"))

    def _insert_streaming_rows(
        self,
        load_config: LoadConfig,
        artifact: StreamingRowsArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        total = 0
        while True:
            batch = []
            for _ in range(artifact._batch_size):
                try:
                    batch.append(next(artifact._iterator))
                except StopIteration:
                    break
            if not batch:
                break
            total += self.insert_rows(load_config, batch, schema)
        # Same authority as StreamingRowsArtifact.materialize for quality probes.
        artifact.rows_exported = total
        artifact.row_count = total
        lifecycle = artifact.extraction_lifecycle
        if lifecycle is not None:
            lifecycle.complete()
        return total


__all__ = ["ClickHousePayloadIngestionService"]
