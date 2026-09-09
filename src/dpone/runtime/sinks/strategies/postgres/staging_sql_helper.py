"""SQL helper-ы для PostgreSQL sink-стратегий, работающих через staging."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from psycopg import sql

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sql_helpers import TechnicalColumnsQueries


class PostgresStagingSqlHelper:
    """Построение SQL для staging->target операций."""

    def __init__(
        self,
        connector: Any,
        logger: ETLLogger | None,
        include_technical_columns: Callable[[Any], bool],
        log_target_sample: Callable[[Any, int], None],
    ) -> None:
        self.connector = connector
        self.logger = logger or etl_logger
        self._include_technical_columns = include_technical_columns
        self._log_target_sample = log_target_sample

    def insert_from_staging(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        """Вставляет данные из staging в target с заполнением технических колонок."""
        all_schema_columns = [column for column, _ in schema]
        include_tech = self._include_technical_columns(load_config)
        tech_column_names = TechnicalColumnsQueries.get_technical_column_names() if include_tech else []
        service_meta_columns = {"__dpone__xmin"}

        data_columns = [column for column in all_schema_columns if column not in service_meta_columns]

        if not include_tech:
            all_columns = data_columns
            select_parts = [sql.Identifier(column) for column in data_columns]
        else:
            existing_tech_columns = [column for column in data_columns if column in tech_column_names]
            if existing_tech_columns:
                all_columns = data_columns
                select_parts = [sql.Identifier(column) for column in data_columns]
            else:
                all_columns = data_columns + tech_column_names
                select_parts = [sql.Identifier(column) for column in data_columns]
                select_parts.append(sql.SQL("CURRENT_TIMESTAMP"))
                select_parts.append(sql.SQL("NULL"))

        columns_sql = sql.SQL(", ").join(sql.Identifier(column) for column in all_columns)
        select_sql = sql.SQL(", ").join(select_parts)

        insert_sql = sql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {}
            FROM {}.{}
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            columns_sql,
            select_sql,
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
        )
        inserted = self.connector.execute_query(insert_sql)

        if inserted and inserted > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
            self._log_target_sample(load_config, load_config.log_sample_rows)

        return inserted

    def select_columns(self, schema: Sequence[tuple[str, str]]) -> sql.Composed:
        return sql.SQL(", ").join(sql.Identifier(column) for column, _ in schema)

    def build_key_condition(
        self,
        left_alias: str,
        right_alias: str,
        unique_key: str | Sequence[str],
    ) -> sql.Composed:
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        conditions: list[sql.Composed] = []
        for column in unique_key:
            conditions.append(
                sql.SQL("{}.{}::text = {}.{}::text").format(
                    sql.Identifier(left_alias),
                    sql.Identifier(column),
                    sql.Identifier(right_alias),
                    sql.Identifier(column),
                )
            )
        return sql.SQL(" AND ").join(conditions)

    def get_target_column_types(self, load_config: Any) -> dict[str, str]:
        cache_attr = "_target_column_types_cache"
        if hasattr(load_config, cache_attr):
            return getattr(load_config, cache_attr)

        types = self.connector.get_table_column_types(
            load_config.target_schema,
            load_config.target_table,
        )
        setattr(load_config, cache_attr, types)
        return types

    def build_typed_select(
        self,
        source_alias: str,
        schema: Sequence[tuple[str, str]],
        target_types: dict[str, str],
    ) -> sql.Composed:
        parts: list[sql.Composed] = []
        for column, _ in schema:
            if column in target_types:
                pg_type = target_types[column]
                parts.append(
                    sql.SQL("{}.{}::{}").format(
                        sql.Identifier(source_alias),
                        sql.Identifier(column),
                        sql.SQL(pg_type),
                    )
                )
            else:
                parts.append(
                    sql.SQL("{}.{}").format(
                        sql.Identifier(source_alias),
                        sql.Identifier(column),
                    )
                )
        return sql.SQL(", ").join(parts)
