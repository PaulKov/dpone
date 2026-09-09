"""Загрузка PostgreSQL sink из InternalQueryArtifact."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from psycopg import sql

from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager
from dpone.runtime.sql_helpers import ExchangeQueries


class PostgresInternalQueryLoader:
    """CREATE TABLE AS SELECT / exchange workflow для InternalQueryArtifact."""

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
        artifact: InternalQueryArtifact = payload.artifact
        target_schema = load_config.target_schema
        target_table = load_config.target_table
        backup_table = ExchangeQueries.get_backup_table_name(target_table)

        self.connector.begin()
        table_existed = False
        try:
            check_sql = sql.SQL(ExchangeQueries.pg_check_table_exists())
            exists = self.connector.get_records(check_sql, (target_schema, target_table))
            table_existed = bool(exists and exists[0][0])

            if table_existed:
                self.logger.log_etl_progress(
                    "EXCHANGE_START",
                    {
                        "Target": f"{target_schema}.{target_table}",
                        "Mode": "Exchange Pattern (атомарная замена)",
                    },
                )
                rename_sql = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(target_table),
                    sql.Identifier(backup_table),
                )
                self.connector.execute_query(rename_sql)
                self.logger.log_etl_progress(
                    "EXCHANGE_BACKUP",
                    {
                        "Target": f"{target_schema}.{target_table}",
                        "Backup": f"{target_schema}.{backup_table}",
                    },
                )
            else:
                self.logger.log_etl_progress(
                    "FULL_REFRESH_CREATE_NEW",
                    {
                        "Target": f"{target_schema}.{target_table}",
                        "Mode": "CREATE TABLE AS (первая загрузка)",
                    },
                )

            create_sql = sql.SQL("CREATE TABLE {}.{} AS {}").format(
                sql.Identifier(target_schema),
                sql.Identifier(target_table),
                sql.SQL(artifact.query),
            )
            inserted = self.connector.execute_query(create_sql, artifact.params)
            self.target_table_manager.ensure_technical_columns(load_config)

            if table_existed:
                self.logger.log_etl_progress(
                    "EXCHANGE_CREATE",
                    {
                        "Target": f"{target_schema}.{target_table}",
                        "Inserted": inserted,
                    },
                )
                drop_backup_sql = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(backup_table),
                )
                self.connector.execute_query(drop_backup_sql)
                self.logger.log_etl_progress(
                    "EXCHANGE_CLEANUP",
                    {
                        "Backup": f"{target_schema}.{backup_table}",
                        "Status": "Dropped",
                    },
                )
                self.logger.log_etl_progress(
                    "EXCHANGE_COMPLETE",
                    {
                        "Target": f"{target_schema}.{target_table}",
                        "Inserted": inserted,
                    },
                )
            else:
                self.logger.log_etl_progress(
                    "FULL_REFRESH_CREATE_COMPLETE",
                    {
                        "Target": f"{target_schema}.{target_table}",
                        "Inserted": inserted,
                    },
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
        except Exception as error:
            try:
                self.connector.rollback()
                drop_failed_sql = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(target_table),
                )
                self.connector.execute_query(drop_failed_sql)

                if table_existed:
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
            raise
