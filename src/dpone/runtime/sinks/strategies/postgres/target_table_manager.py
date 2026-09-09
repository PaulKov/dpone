"""Управление целевыми таблицами PostgreSQL sink-стратегий."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from psycopg import sql

from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sql_helpers import TechnicalColumnsQueries


class PostgresTargetTableManager:
    """Создание target-таблиц и обслуживание технических колонок."""

    _TECHNICAL_COLUMN_DEFINITIONS = {
        "__dpone__loaded_at": "timestamp with time zone",
        "__dpone__deleted_at": "timestamp with time zone",
    }

    def __init__(
        self,
        connector: Any,
        logger: ETLLogger | None,
        include_technical_columns: Callable[[Any], bool],
    ) -> None:
        self.connector = connector
        self.logger = logger or etl_logger
        self._include_technical_columns = include_technical_columns

    def build_schema_with_technical_columns(
        self,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        """Возвращает схему с добавленными техническими колонками, если они нужны."""
        existing_columns = {column for column, _ in schema}
        tech_columns: list[tuple[str, str]] = []

        if self._include_technical_columns(load_config):
            for column_name, dtype in self._TECHNICAL_COLUMN_DEFINITIONS.items():
                if column_name not in existing_columns:
                    tech_columns.append((column_name, dtype))

        return list(schema) + tech_columns, tech_columns

    def build_create_table_columns_sql(
        self,
        schema: Sequence[tuple[str, str]],
    ) -> sql.Composed:
        """Строит SQL-список колонок для CREATE TABLE."""
        return sql.SQL(", ").join(
            sql.SQL("{} {} {}").format(
                sql.Identifier(column),
                sql.SQL(dtype),
                sql.SQL("NOT NULL DEFAULT CURRENT_TIMESTAMP") if column == "__dpone__loaded_at" else sql.SQL(""),
            )
            for column, dtype in schema
        )

    def ensure_target_table(
        self,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> bool:
        """Гарантирует существование target-таблицы.

        Returns:
            True, если таблица была создана в этом вызове.
        """
        columns_sql = sql.SQL(", ").join(sql.SQL(f'"{column}" {dtype}') for column, dtype in schema)

        check_sql = sql.SQL(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """
        )

        exists = self.connector.get_records(
            check_sql,
            (load_config.target_schema, load_config.target_table),
        )

        if exists and exists[0][0]:
            self.ensure_technical_columns(load_config)
            return False

        create_sql = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            columns_sql,
        )
        self.connector.execute_query(create_sql)
        self.ensure_technical_columns(load_config)
        return True

    def ensure_technical_columns(self, load_config: Any) -> None:
        """Добавляет __dpone__loaded_at и __dpone__deleted_at если их нет."""
        if not self._include_technical_columns(load_config):
            return

        meta_load_dtm = "__dpone__loaded_at"
        meta_delete_dtm = "__dpone__deleted_at"

        check_columns_sql = sql.SQL(
            TechnicalColumnsQueries.pg_check_columns_exist(
                schema=load_config.target_schema,
                table=load_config.target_table,
            )
        )

        existing_columns = self.connector.get_records(
            check_columns_sql,
            (load_config.target_schema, load_config.target_table, meta_load_dtm, meta_delete_dtm),
        )
        existing_column_names = {col[0] for col in existing_columns} if existing_columns else set()

        if meta_load_dtm not in existing_column_names:
            add_load_dtm_sql = sql.SQL(
                TechnicalColumnsQueries.pg_add_load_dtm_column(
                    schema=load_config.target_schema,
                    table=load_config.target_table,
                )
            )
            self.connector.execute_query(add_load_dtm_sql)

            update_load_dtm_sql = sql.SQL(
                TechnicalColumnsQueries.pg_update_load_dtm_null_rows(
                    schema=load_config.target_schema,
                    table=load_config.target_table,
                )
            )
            self.connector.execute_query(update_load_dtm_sql)

            self.logger.log_etl_progress(
                "TECHNICAL_COLUMN_ADDED",
                {
                    "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    "Column": meta_load_dtm,
                },
            )

        if meta_delete_dtm not in existing_column_names:
            add_delete_dtm_sql = sql.SQL(
                TechnicalColumnsQueries.pg_add_delete_dtm_column(
                    schema=load_config.target_schema,
                    table=load_config.target_table,
                )
            )
            self.connector.execute_query(add_delete_dtm_sql)

            self.logger.log_etl_progress(
                "TECHNICAL_COLUMN_ADDED",
                {
                    "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    "Column": meta_delete_dtm,
                },
            )
