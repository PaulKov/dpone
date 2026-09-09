"""Инкрементальная стратегия извлечения по timestamp колонке (не xmin).

Альтернатива xmin tracking для случаев:
- Append-only таблицы (логи, события)
- Реплицированные таблицы (где xmin не переносится)
- Когда нужен явный контроль над инкрементальной колонкой

**Логика:**
1. Получает MAX(incremental_column) из sink (BigQuery в UTC)
2. Конвертирует UTC → source_timezone (если указан)
3. Забирает из source только строки WHERE incremental_column > MAX
4. Вставляет в target

**Timezone конвертация:**
- BigQuery хранит данные в UTC
- PostgreSQL может хранить в локальном timezone (например Europe/Moscow)
- Параметр `source_timezone` указывает timezone PostgreSQL для конвертации
"""

from __future__ import annotations

from typing import Any

from psycopg import sql

from dpone.contracts.postgres_incremental_cursor import assert_postgres_column_cursor_route_supported
from dpone.runtime.extraction_lifecycle import ExtractionClock
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.postgres.postgres_base import PostgresBaseStrategy
from dpone.runtime.support.timezone import format_timestamp_for_sql
from dpone.type_system.source_sink.provenance import SourceRelationDialect


class PostgresIncrementalExtractStrategy(PostgresBaseStrategy):
    """
    Стратегия инкрементальной выборки из PostgreSQL по timestamp колонке.

    **Отличие от xmin:**
    - xmin tracking: системная колонка PostgreSQL, ловит все INSERT/UPDATE
    - incremental_column: явная колонка (created_at, updated_at), только INSERT после MAX

    **Когда использовать:**
    - Append-only таблицы (логи, события) - идеально
    - Таблицы без UPDATE (только INSERT)
    - Реплики PostgreSQL (xmin не переносится при репликации)
    """

    def __init__(
        self,
        connector,
        sink_connector,
        logger,
        *,
        extraction_clock: ExtractionClock | None = None,
    ):
        """
        Args:
            connector: PostgreSQL connector (source)
            sink_connector: BigQuery/PostgreSQL connector (sink) для получения MAX()
            logger: ETL logger
        """
        super().__init__(connector, logger, extraction_clock=extraction_clock)
        self.sink_connector = sink_connector

    def get_state(self, load_config) -> dict[str, Any] | None:
        """
        Получает инкрементальное состояние из sink таблицы.

        """
        self._assert_route_safe(load_config)
        options = getattr(load_config, "options", {}) or {}
        incremental_column = options.get("incremental_column")
        source_timezone = options.get("source_timezone")  # например 'Europe/Moscow'

        if not incremental_column:
            self.logger.warning(
                "incremental_column не указан в source.options. Будет использован xmin tracking или full refresh."
            )
            return None

        # Получаем MAX(incremental_column) из sink
        max_value = self._get_max_value_from_sink(load_config, incremental_column)

        if max_value is None:
            self.logger.info(
                f"MAX({incremental_column}) не найден в sink таблице. Возможно, это первая загрузка (full refresh)."
            )
            return None

        # Форматируем для PostgreSQL WHERE clause (с конвертацией timezone если указан)
        formatted_value = format_timestamp_for_sql(max_value, source_timezone)

        self.logger.log_etl_progress(
            "PG_INCREMENTAL_STATE",
            {
                "Column": incremental_column,
                "Max_Value_UTC": str(max_value),
                "Formatted": formatted_value,
                "Source_Timezone": source_timezone or "UTC (no conversion)",
                "Sink": f"{load_config.target_schema}.{load_config.target_table}",
            },
        )

        return {
            "last_value": formatted_value,
            "column": incremental_column,
            "raw_value": max_value,
            "source_timezone": source_timezone,
        }

    def extract(self, load_config, last_state: dict[str, Any] | None) -> ExtractResult:
        """
        Извлекает данные из PostgreSQL с фильтром по incremental_column.
        """
        self._assert_route_safe(load_config)
        options = getattr(load_config, "options", {}) or {}
        incremental_column = options.get("incremental_column")

        self.logger.info(f"🔄 Стратегия: PostgresIncrementalExtract (column={incremental_column})")

        # Получаем схему таблицы
        projection = self.fetch_schema_projection(load_config)
        schema = list(projection.projected_schema)
        relation_schema = projection.relation_schema
        relation_metadata = projection.relation_metadata
        columns = [col for col, _ in schema]

        # Если нет state или incremental_column — full refresh
        if not last_state or not incremental_column:
            self.logger.info("Нет инкрементального состояния или incremental_column. Выполняется full refresh.")
            return self._extract_full(
                load_config,
                schema,
                relation_schema,
                relation_metadata,
                projection.target_projection,
            )

        last_value = last_state.get("last_value")
        if not last_value:
            self.logger.info("last_value пустой. Выполняется full refresh.")
            return self._extract_full(
                load_config,
                schema,
                relation_schema,
                relation_metadata,
                projection.target_projection,
            )

        # Строим инкрементальный запрос
        query = self._build_incremental_query(
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            columns=columns,
            incremental_column=incremental_column,
            last_value=last_value,
            custom_predicate=load_config.custom_predicate,
        )

        try:
            tz_result = self.connector.get_records("SHOW timezone", as_dict=True)
            pg_timezone = tz_result[0].get("TimeZone", "unknown") if tz_result else "unknown"
        except Exception:
            pg_timezone = "error"

        self.logger.log_etl_progress(
            "PG_INCREMENTAL_QUERY",
            {
                "Column": incremental_column,
                "Last_Value": last_value,
                "PG_Session_Timezone": pg_timezone,
                "Query_Preview": query.as_string(self.connector.connection)[:300] + "...",
            },
        )

        # Оцениваем размер delta
        delta_count = self._estimate_delta_count(load_config, incremental_column, last_value)

        if delta_count is not None:
            self.logger.info(f"🔢 PostgreSQL delta COUNT: {delta_count:,} rows")

        delta_threshold = options.get("delta_size_threshold", 1_000_000)
        # BigQuery CSV BYTES requires base64 wire encoding applied in COPY SELECT
        # wraps; streaming rows would emit raw bytea and fail load jobs.
        # Explicit batch_commit_mode=whole also requests COPY file artifacts.
        force_file_export = (
            self._targets_bigquery(load_config) or str(options.get("batch_commit_mode", "")).lower() == "whole"
        )

        if force_file_export or (delta_count is not None and delta_count >= delta_threshold):
            mode = (
                "File Export (BigQuery BYTES wire)"
                if self._targets_bigquery(load_config)
                else "File Export (whole/batch_commit_mode)"
                if force_file_export
                else "File Export (memory-safe)"
            )
            self.logger.log_etl_progress(
                "PG_INCREMENTAL_FILE_EXPORT",
                {
                    "Delta_Rows": delta_count,
                    "Threshold": delta_threshold,
                    "Mode": mode,
                },
            )
            artifact = self._export_to_file(
                query,
                schema,
                load_config,
                batch_size=load_config.batch_size,
                relation_schema=relation_schema,
            )
        else:
            # Маленький delta — streaming
            self.logger.log_etl_progress(
                "PG_INCREMENTAL_STREAMING",
                {
                    "Estimated_Delta": delta_count or "unknown",
                    "Mode": "Streaming",
                },
            )
            artifact = self._open_repeatable_read_stream(
                query,
                params=None,
                batch_size=load_config.batch_size,
            )

        # Получаем новый max для state
        source_timezone = options.get("source_timezone")
        new_max_value = self._get_max_value_from_sink(load_config, incremental_column)
        new_state = {
            "last_value": format_timestamp_for_sql(new_max_value, source_timezone) if new_max_value else last_value,
            "column": incremental_column,
            "source_timezone": source_timezone,
        }

        return ExtractResult(
            artifact=artifact,
            schema=schema,
            relation_schema=relation_schema,
            relation_metadata=relation_metadata,
            relation_dialect=SourceRelationDialect.POSTGRES,
            target_projection=projection.target_projection,
            state=new_state,
            force_full_refresh=False,
        )

    def _assert_route_safe(self, load_config) -> None:
        """Reject the non-atomic SQL Server checkpoint before connector I/O."""

        options = getattr(load_config, "options", {}) or {}
        assert_postgres_column_cursor_route_supported(
            configured_sink=options.get("sink_type") or options.get("target_type"),
            sink_connector=self.sink_connector,
        )

    def _extract_full(
        self,
        load_config,
        schema,
        relation_schema,
        relation_metadata,
        target_projection,
    ) -> ExtractResult:
        """Выполняет full refresh (все данные)."""
        columns = [col for col, _ in schema]

        query = self.format_select_query(
            load_config.source_schema,
            load_config.source_table,
            columns,
            load_config.custom_predicate,
        )

        artifact = self._export_to_file(
            query,
            schema,
            load_config,
            batch_size=load_config.batch_size,
            relation_schema=relation_schema,
        )

        return ExtractResult(
            artifact=artifact,
            schema=schema,
            relation_schema=relation_schema,
            relation_metadata=relation_metadata,
            relation_dialect=SourceRelationDialect.POSTGRES,
            target_projection=target_projection,
            state=None,
            force_full_refresh=True,
        )

    def _build_incremental_query(
        self,
        source_schema: str,
        source_table: str,
        columns: list[str],
        incremental_column: str,
        last_value: str,
        custom_predicate: str | None = None,
    ) -> sql.Composed:
        """Строит запрос с фильтром по incremental_column."""

        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)

        # WHERE incremental_column > 'last_value'
        where_clause = sql.SQL("{} > {}").format(
            sql.Identifier(incremental_column),
            sql.Literal(last_value),
        )

        if custom_predicate:
            where_clause = sql.SQL("({}) AND ({})").format(
                where_clause,
                sql.SQL(custom_predicate),
            )

        query = sql.SQL("SELECT {columns} FROM {schema}.{table} WHERE {where}").format(
            columns=col_list,
            schema=sql.Identifier(source_schema),
            table=sql.Identifier(source_table),
            where=where_clause,
        )

        return query

    def _get_max_value_from_sink(self, load_config, incremental_column: str) -> Any | None:
        """
        Получает MAX(incremental_column) из sink таблицы (BigQuery).

        Это определяет "что уже загружено" — забираем из source (PostgreSQL)
        только строки WHERE column > MAX.
        """
        if not self.sink_connector:
            return None

        try:
            return self.sink_connector.get_max_column_value(
                schema=load_config.target_schema,
                table=load_config.target_table,
                column=incremental_column,
            )
        except Exception as exc:
            self.logger.warning(f"Ошибка при получении MAX({incremental_column}) из sink: {exc}")
            return None

    def _estimate_delta_count(
        self,
        load_config,
        incremental_column: str,
        last_value: str,
    ) -> int | None:
        """Оценивает количество строк в delta."""
        try:
            query = sql.SQL("SELECT COUNT(*) as cnt FROM {schema}.{table} WHERE {col} > {val}").format(
                schema=sql.Identifier(load_config.source_schema),
                table=sql.Identifier(load_config.source_table),
                col=sql.Identifier(incremental_column),
                val=sql.Literal(last_value),
            )

            # Логируем запрос для диагностики
            query_str = query.as_string(self.connector.connection)
            self.logger.info(f"🔍 Delta COUNT query: {query_str}")

            result = self.connector.get_records(query, as_dict=True)

            if result and len(result) > 0:
                count_val = result[0].get("cnt")
                self.logger.info(f"🔢 Delta COUNT result: {count_val}")
                return count_val

            self.logger.warning("Delta COUNT вернул пустой результат")
            return None

        except Exception as exc:
            self.logger.warning(f"Не удалось оценить размер delta: {exc}", exc_info=True)
            return None
