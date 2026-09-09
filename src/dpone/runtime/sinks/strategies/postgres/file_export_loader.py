"""Загрузка PostgreSQL sink из FileExportArtifact."""

from __future__ import annotations

import gzip
import os
from collections.abc import Callable, Sequence
from typing import Any

from psycopg import sql

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.postgres.file_export_exchange import (
    load_with_exchange as exchange_load_from_artifact,
)
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager
from dpone.runtime.sql_helpers import ExchangeQueries


class PostgresFileExportLoader:
    """Загрузка target-таблиц PostgreSQL из экспортированных файлов."""

    def __init__(
        self,
        connector: Any,
        logger: ETLLogger | None,
        target_table_manager: PostgresTargetTableManager,
        log_target_sample: Callable[[Any, int], None],
    ) -> None:
        self.connector = connector
        self.logger = logger or etl_logger
        self.target_table_manager = target_table_manager
        self._log_target_sample = log_target_sample

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        artifact: FileExportArtifact = payload.artifact
        overwrite_type = getattr(load_config, "overwrite_type", None)

        if overwrite_type == "exchange":
            return self.load_with_exchange(load_config, payload, artifact)
        if overwrite_type == "truncate_insert":
            return self.load_with_truncate(load_config, payload, artifact)
        return self.load_standard(load_config, payload, artifact)

    def load_standard(
        self,
        load_config: Any,
        payload: LoadPayload,
        artifact: FileExportArtifact,
    ) -> LoadResult:
        self.logger.log_etl_progress(
            "FILE_EXPORT_LOAD_STANDARD",
            {
                "Target": f"{load_config.target_schema}.{load_config.target_table}",
                "File": artifact.file_path,
                "Compressed": artifact.compressed,
                "Format": artifact.format,
                "Mode": "DROP + CREATE + COPY",
            },
        )

        self.connector.begin()
        inserted = 0
        try:
            drop_sql = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
            )
            self.connector.execute_query(drop_sql)

            schema_with_tech, tech_columns = self.target_table_manager.build_schema_with_technical_columns(
                load_config,
                payload.schema,
            )
            create_sql = sql.SQL("CREATE TABLE {}.{} ({});").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
                self.target_table_manager.build_create_table_columns_sql(schema_with_tech),
            )
            self.connector.execute_query(create_sql)

            if tech_columns:
                self.logger.log_etl_progress(
                    "TECHNICAL_COLUMNS_ADDED",
                    {
                        "Target": f"{load_config.target_schema}.{load_config.target_table}",
                        "Columns": ", ".join(column for column, _ in tech_columns),
                    },
                )

            inserted = self.copy_from_artifact(
                target_schema=load_config.target_schema,
                target_table=load_config.target_table,
                schema=payload.schema,
                artifact=artifact,
            )
            self.connector.commit_transaction()

            if (
                inserted
                and inserted > 0
                and hasattr(load_config, "log_sample_rows")
                and load_config.log_sample_rows > 0
            ):
                self._log_target_sample(load_config, load_config.log_sample_rows)

            return LoadResult(
                inserted_rows=inserted,
                updated_rows=0,
                total_rows=inserted,
                staging_rows=inserted,
            )
        except Exception:
            self.connector.rollback()
            raise
        finally:
            self.cleanup_file(artifact)

    def load_with_truncate(
        self,
        load_config: Any,
        payload: LoadPayload,
        artifact: FileExportArtifact,
    ) -> LoadResult:
        self.logger.log_etl_progress(
            "FILE_EXPORT_LOAD_TRUNCATE",
            {
                "Target": f"{load_config.target_schema}.{load_config.target_table}",
                "File": artifact.file_path,
                "Compressed": artifact.compressed,
                "Format": artifact.format,
                "Mode": "TRUNCATE + COPY",
            },
        )

        self.connector.begin()
        inserted = 0
        try:
            target_created = self.target_table_manager.ensure_target_table(load_config, payload.schema)
            if not target_created:
                truncate_sql = sql.SQL("TRUNCATE TABLE {}.{} CASCADE").format(
                    sql.Identifier(load_config.target_schema),
                    sql.Identifier(load_config.target_table),
                )
                self.connector.execute_query(truncate_sql)
                self.logger.log_etl_progress(
                    "PG_TARGET_TRUNCATED",
                    {
                        "Target": f"{load_config.target_schema}.{load_config.target_table}",
                        "Reason": "Full refresh (overwrite_type=truncate_insert)",
                    },
                )

            inserted = self.copy_from_artifact(
                target_schema=load_config.target_schema,
                target_table=load_config.target_table,
                schema=payload.schema,
                artifact=artifact,
            )
            self.connector.commit_transaction()

            if (
                inserted
                and inserted > 0
                and hasattr(load_config, "log_sample_rows")
                and load_config.log_sample_rows > 0
            ):
                self._log_target_sample(load_config, load_config.log_sample_rows)

            return LoadResult(
                inserted_rows=inserted,
                updated_rows=0,
                total_rows=inserted,
                staging_rows=inserted,
            )
        except Exception:
            self.connector.rollback()
            raise
        finally:
            self.cleanup_file(artifact)

    def load_with_exchange(
        self,
        load_config: Any,
        payload: LoadPayload,
        artifact: FileExportArtifact,
    ) -> LoadResult:
        return exchange_load_from_artifact(self, load_config, payload, artifact)

    def copy_from_artifact(
        self,
        target_schema: str,
        target_table: str,
        schema: Sequence[tuple[str, str]],
        artifact: FileExportArtifact,
    ) -> int:
        inserted = 0
        copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH ({})").format(
            sql.Identifier(target_schema),
            sql.Identifier(target_table),
            sql.SQL(", ").join(sql.Identifier(column) for column, _ in schema),
            sql.SQL("FORMAT BINARY") if artifact.format == "binary" else sql.SQL("FORMAT CSV, HEADER FALSE"),
        )

        with self.connector.connection.cursor() as cursor:
            with cursor.copy(copy_sql) as copy:
                opener = gzip.open if artifact.compressed else open
                mode = "rb" if artifact.format == "binary" else "rt"
                open_kwargs = {} if artifact.format == "binary" else {"encoding": "utf-8", "newline": ""}
                with opener(artifact.file_path, mode, **open_kwargs) as data_file:
                    while True:
                        chunk = data_file.read(1024 * 1024)
                        if not chunk:
                            break
                        copy.write(chunk)

        inserted = self._count_rows(target_schema, target_table)
        return inserted

    def cleanup_file(self, artifact: FileExportArtifact) -> None:
        try:
            os.remove(artifact.file_path)
        except OSError:
            pass

    def _count_rows(self, target_schema: str, target_table: str) -> int:
        count_sql = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(target_schema),
            sql.Identifier(target_table),
        )
        result = self.connector.get_records(count_sql)
        return result[0][0] if result else 0

    def _cleanup_exchange_failure(
        self,
        target_schema: str,
        target_table: str,
        tmp_table: str,
        backup_table: str,
        error: Exception,
    ) -> None:
        try:
            drop_tmp_sql = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(target_schema),
                sql.Identifier(tmp_table),
            )
            self.connector.execute_query(drop_tmp_sql)
        except Exception:
            pass

        try:
            check_backup_sql = sql.SQL(ExchangeQueries.pg_check_table_exists())
            backup_exists = self.connector.get_records(check_backup_sql, (target_schema, backup_table))
            backup_exists = bool(backup_exists and backup_exists[0][0])
            if not backup_exists:
                return

            try:
                drop_target_sql = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(target_table),
                )
                self.connector.execute_query(drop_target_sql)
            except Exception:
                pass

            restore_sql = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                sql.Identifier(target_schema),
                sql.Identifier(backup_table),
                sql.Identifier(target_table),
            )
            self.connector.execute_query(restore_sql)
            self.logger.log_etl_progress(
                "EXCHANGE_ROLLBACK",
                {
                    "Target": f"{target_schema}.{target_table}",
                    "Status": "Restored from backup",
                },
            )
        except Exception as rollback_error:
            self.logger.log_etl_progress(
                "EXCHANGE_ROLLBACK_FAILED",
                {
                    "Error": str(rollback_error),
                    "Original_Error": str(error),
                },
            )
