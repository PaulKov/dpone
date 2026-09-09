"""Batched PostgreSQL file export helpers."""

from __future__ import annotations

import gzip
import os
import uuid
from threading import RLock
from typing import TYPE_CHECKING, Any

from psycopg import sql

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.file_artifacts import (
    BatchedFileExportArtifact,
    FileExportArtifact,
)
from dpone.runtime.owned_file_scope import OwnedFileScope
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.storage_policy import RuntimeStoragePolicy
from dpone.runtime.support.bulk_text_codec import BulkTextCodec


class PostgresBatchedExportMixin:
    if TYPE_CHECKING:
        connector: Any
        logger: Any

        def _new_extraction_lifecycle(self) -> ExtractionLifecycleAuthority: ...

        def _begin_repeatable_read_snapshot(
            self,
            lifecycle: ExtractionLifecycleAuthority,
        ) -> PostgresRepeatableReadSnapshotLease: ...

        def _effective_postgres_file_wire(self, load_config: Any) -> tuple[str, bool]: ...

        def _render_query(self, connector: Any, query: Any) -> str: ...

        def _verify_postgres_source_authority(
            self,
            snapshot_lease: PostgresRepeatableReadSnapshotLease,
            load_config: Any,
        ) -> Any: ...

        def _prepare_copy_select_sql(
            self,
            select_sql: str,
            schema: list[tuple[str, str]],
            load_config: Any,
            *,
            format_name: str,
            bulk_text_codec: BulkTextCodec | None = None,
        ) -> str: ...

        @staticmethod
        def _attach_rows_exported(artifact: Any, file_path: str, *, compressed: bool) -> None: ...

    def _export_to_file_batched(
        self,
        query,
        schema,
        load_config,
        batch_size: int,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None = None,
    ) -> BatchedFileExportArtifact:
        """
        Экспортирует данные батчами через LIMIT/OFFSET.

        Args:
            query: базовый SELECT запрос
            schema: схема таблицы
            load_config: конфигурация
            batch_size: размер батча

        Returns:
            BatchedFileExportArtifact с генератором батчей
        """
        if snapshot_lease is not None:
            if not isinstance(snapshot_lease, PostgresRepeatableReadSnapshotLease):
                raise TypeError("postgres_batched_export.snapshot_lease_required")
            snapshot_lease.require_for(self.connector)
        total_rows = self._estimate_total_rows(load_config)

        self.logger.log_etl_progress(
            "BATCHED_EXPORT_START",
            {
                "Batch_Size": batch_size,
                "Estimated_Total_Rows": total_rows,
                "Strategy": "LIMIT/OFFSET",
                "Mode": "separate",
            },
        )

        columns = [col for col, _ in schema]

        lifecycle = snapshot_lease.lifecycle if snapshot_lease is not None else self._new_extraction_lifecycle()
        active_lease = snapshot_lease
        transaction_lock = RLock()
        transaction_open = False

        def finish_source_transaction(*, commit: bool, primary: BaseException | None = None) -> None:
            nonlocal transaction_open
            with transaction_lock:
                if not transaction_open:
                    return
                if commit:
                    self.connector.commit_transaction()
                elif primary is not None:
                    rollback_preserving_primary(self.connector, primary)
                else:
                    self.connector.rollback()
                transaction_open = False

        def batch_generator():
            """Генератор батчей через LIMIT/OFFSET."""
            offset = 0
            batch_num = 1
            nonlocal active_lease, transaction_open
            try:
                if active_lease is None:
                    active_lease = self._begin_repeatable_read_snapshot(lifecycle)
                else:
                    active_lease.require_for(self.connector)
                self._verify_postgres_source_authority(
                    active_lease,
                    load_config,
                )
                transaction_open = True
                while True:
                    self.logger.log_etl_progress(
                        "BATCH_EXPORT_START",
                        {
                            "Batch_Num": batch_num,
                            "Offset": offset,
                            "Limit": batch_size,
                        },
                    )

                    query_part = sql.SQL(query) if isinstance(query, str) else query
                    batched_query = sql.Composed(
                        [
                            query_part,
                            sql.SQL(" ORDER BY ctid"),
                            sql.SQL(" LIMIT "),
                            sql.Literal(batch_size),
                            sql.SQL(" OFFSET "),
                            sql.Literal(offset),
                        ]
                    )

                    batch_artifact = self._export_query_to_file_single(
                        batched_query,
                        schema,
                        load_config,
                        batch_num,
                    )
                    setattr(batch_artifact, "batch_index", batch_num - 1)

                    if self._is_file_empty(batch_artifact.file_path):
                        self.logger.log_etl_progress(
                            "BATCHED_EXPORT_COMPLETE",
                            {
                                "Total_Batches": batch_num,
                                "Total_Offset": offset,
                            },
                        )
                        batch_artifact.cleanup()
                        break

                    self.logger.log_etl_progress(
                        "BATCH_EXPORT_COMPLETE",
                        {
                            "Batch_Num": batch_num,
                            "File": batch_artifact.file_path,
                        },
                    )

                    yield batch_artifact

                    offset += batch_size
                    batch_num += 1
                lifecycle.complete()
            except BaseException as primary:
                finish_source_transaction(commit=False, primary=primary)
                raise

        from dpone.runtime.file_artifacts import BatchedFileExportArtifact

        artifact = BatchedFileExportArtifact(
            batch_generator=batch_generator,
            columns=columns,
            batch_size=batch_size,
            format=self._effective_postgres_file_wire(load_config)[0],
            compressed=self._effective_postgres_file_wire(load_config)[1],
            estimated_rows=total_rows,
            on_success=lambda: finish_source_transaction(commit=True),
            on_abort=lambda: finish_source_transaction(commit=False),
            generator_owns_lifecycle_acquisition=True,
        )
        artifact.bind_extraction_lifecycle(lifecycle)
        return artifact

    def _export_query_to_file_single(
        self,
        query,
        schema,
        load_config,
        batch_num: int,
    ) -> FileExportArtifact:
        """
        Экспортирует один query в файл через COPY TO STDOUT.

        Переиспользуем существующую логику экспорта.
        """
        tmp_dir = RuntimeStoragePolicy.from_options(load_config.options).work_dir
        tmp_dir.mkdir(parents=True, exist_ok=True)

        export_format, compress_export = self._effective_postgres_file_wire(load_config)

        # Определяем extension
        if export_format == "binary":
            extension = ".bin.gz" if compress_export else ".bin"
            format_name = "binary"
            copy_format = "BINARY"
        elif export_format in {"mssql-delimited", "mssql_delimited"}:
            extension = ".bcp.gz" if compress_export else ".bcp"
            format_name = "mssql-delimited"
            copy_format = "MSSQL_DELIMITED"
        else:
            extension = ".csv.gz" if compress_export else ".csv"
            format_name = "csv"
            copy_format = "CSV"

        unique_id = uuid.uuid4().hex[:8]
        source_table = getattr(load_config, "source_table", "unknown")
        tmp_file_path = os.path.join(tmp_dir, f"dpone_{source_table}_{unique_id}_batch_{batch_num}{extension}")

        # Удаляем файл если он уже существует (cleanup из предыдущего запуска)
        if os.path.exists(tmp_file_path):
            try:
                os.remove(tmp_file_path)
            except OSError:
                pass

        columns = [column for column, _ in schema]
        select_sql = self._render_query(self.connector, query)
        bulk_text_codec = BulkTextCodec() if format_name == "mssql-delimited" else None
        select_sql = self._prepare_copy_select_sql(
            select_sql,
            schema,
            load_config,
            format_name=format_name,
            bulk_text_codec=bulk_text_codec,
        )

        compress_level = int(os.environ.get("DPONE_EXPORT_GZIP_LEVEL", "1"))
        buffer_size = int(os.environ.get("DPONE_EXPORT_BUFFER_SIZE", str(16 * 1024 * 1024)))

        owned_files = OwnedFileScope()
        owned_files.register(tmp_file_path)
        try:
            self.connector.copy_to_file(
                query_sql=select_sql,
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
            self._attach_rows_exported(artifact, tmp_file_path, compressed=compress_export)
            owned_files.transfer(tmp_file_path)
            return artifact
        except BaseException:
            owned_files.cleanup()
            raise

    def _is_file_empty(self, file_path: str) -> bool:
        """Проверяет, пустой ли файл (нет данных в батче)."""
        if not os.path.exists(file_path):
            return True

        if os.path.getsize(file_path) == 0:
            return True

        opener = gzip.open if file_path.endswith(".gz") else open
        with opener(file_path, "rb") as handle:
            return handle.read(1) == b""

    def _estimate_total_rows(self, load_config) -> int | None:
        """
        Опционально: оценивает total row count для прогресса.

        Использует pg_class.reltuples для быстрой оценки.
        """
        try:
            query = sql.SQL("""
                SELECT reltuples::bigint AS estimate
                FROM pg_class
                WHERE oid = %s::regclass
            """)

            table_name = f"{load_config.source_schema}.{load_config.source_table}"
            result = self.connector.get_records(query, (table_name,))

            if result and result[0]:
                return result[0][0]
        except Exception as e:
            self.logger.log_etl_progress("ROW_COUNT_ESTIMATE_FAILED", {"Error": str(e)})

        return None


__all__ = ["PostgresBatchedExportMixin"]
