"""Управление инкрементальным state для ClickHouse."""

from typing import Any

from dpone.contracts.clickhouse_incremental_cursor import assert_clickhouse_mssql_cursor_supported
from dpone.runtime.support.timezone import TimezoneConverter


class IncrementalStateManager:
    """
    Управляет получением и форматированием инкрементального state из sink.

    **Назначение:**
    - Получает max(incremental_column) из sink через его dialect-aware API
    - Конвертирует из UTC в ClickHouse timezone
    - Форматирует для использования в ClickHouse WHERE clause
    """

    def __init__(self, sink_connector, logger, clickhouse_timezone: str = "Europe/Moscow"):
        """
        Args:
            sink_connector: sink connector для получения max(column)
            logger: ETL logger для логирования
            clickhouse_timezone: Timezone ClickHouse базы (по умолчанию Europe/Moscow)
        """
        self.sink_connector = sink_connector
        self.logger = logger
        self.tz_converter = TimezoneConverter(target_tz=clickhouse_timezone)

    def get_state(self, load_config) -> dict[str, Any] | None:
        """
        Получает инкрементальное состояние из sink.

        Raises:
            ValueError: Если incremental_column не указан в options
        """
        options = getattr(load_config, "options", {}) or {}
        assert_clickhouse_mssql_cursor_supported(
            configured_sink=options.get("sink_type") or options.get("target_type"),
            sink_connector=self.sink_connector,
        )
        incremental_column = options.get("incremental_column")

        # Проверяем, указаны ли явные даты (date_from/date_to)
        # Если да, это явный full refresh для диапазона дат, не инкрементальная загрузка
        date_from = options.get("date_from")
        date_to = options.get("date_to")
        has_explicit_date_range = date_from and date_to

        if not incremental_column:
            # Не выводим warning, если указан явный диапазон дат (это нормально для full refresh)
            if not has_explicit_date_range:
                self.logger.warning("incremental_column не указан в options, инкрементальная загрузка невозможна")
            return None

        # Получаем max(incremental_column) из sink
        max_value = self._get_max_value_from_sink(load_config, incremental_column)

        if not max_value:
            self.logger.info(
                f"Не найдено значение max({incremental_column}) в sink таблице. "
                f"Возможно, это первая загрузка (full refresh)."
            )
            return None

        # Конвертируем и форматируем для ClickHouse
        formatted_value = self._format_for_clickhouse(max_value, incremental_column, load_config)

        self.logger.info(
            f"Получено max({incremental_column}) из sink: {max_value} "
            f"→ ClickHouse формат (Europe/Moscow): {formatted_value}"
        )

        return {"last_value": formatted_value, "column": incremental_column}

    def _get_max_value_from_sink(self, load_config, incremental_column: str) -> Any | None:
        """
        Запрашивает максимальное значение колонки из sink таблицы.

        Returns:
            Максимальное значение или None
        """
        if not self.sink_connector:
            self.logger.warning("sink_connector не настроен, невозможно получить max() из sink")
            return None

        try:
            return self._query_sink_max(load_config, incremental_column)

        except Exception as exc:
            self.logger.warning(
                f"Ошибка при получении max({incremental_column}) из sink: {exc}. Будет выполнен full refresh."
            )
            return None

    def _query_sink_max(self, load_config, incremental_column: str) -> Any | None:
        schema = load_config.target_schema
        table = load_config.target_table
        database = getattr(load_config, "target_database", None)
        if hasattr(self.sink_connector, "get_max_column_value"):
            return self.sink_connector.get_max_column_value(schema, table, incremental_column, database=database)
        if hasattr(self.sink_connector, "build_max_query"):
            query = self._build_sink_max_query(schema, table, incremental_column, database)
        else:
            query = self._fallback_max_query(load_config, incremental_column)
        result = self.sink_connector.get_records(query)
        return self._first_max_value(result)

    def _build_sink_max_query(self, schema: str, table: str, column: str, database: str | None) -> Any:
        try:
            return self.sink_connector.build_max_query(schema, table, column, database=database)
        except TypeError:
            return self.sink_connector.build_max_query(schema, table, column)

    def _fallback_max_query(self, load_config, incremental_column: str) -> str:
        schema = load_config.target_schema
        table = load_config.target_table
        database = getattr(load_config, "target_database", None)
        if hasattr(self.sink_connector, "qualified_name"):
            qualified = self.sink_connector.qualified_name(schema, table, database=database)
        elif database:
            qualified = f"{database}.{schema}.{table}"
        else:
            qualified = f"{schema}.{table}"
        column = (
            self.sink_connector.quote_identifier(incremental_column)
            if hasattr(self.sink_connector, "quote_identifier")
            else incremental_column
        )
        return f"SELECT MAX({column}) AS max_val FROM {qualified}"

    @staticmethod
    def _first_max_value(result: Any) -> Any | None:
        if not result:
            return None
        first = result[0]
        if isinstance(first, dict):
            return first.get("max_val")
        return first[0] if first else None

    def _format_for_clickhouse(self, max_value: Any, incremental_column: str, load_config=None) -> str:
        """
        Форматирует max_value из sink для использования в ClickHouse.
        """
        if self._should_passthrough_sink_state(load_config):
            return self._format_passthrough(max_value)
        try:
            # Парсим и конвертируем в ClickHouse timezone
            ch_datetime = self.tz_converter.parse_and_convert(max_value)

            formatted = self.tz_converter.format_for_clickhouse(ch_datetime, include_milliseconds=True)

            return formatted

        except Exception as exc:
            self.logger.warning(
                f"Ошибка форматирования max_value для ClickHouse: {exc}. Используем значение как есть: {max_value}"
            )
            return str(max_value).split("+")[0].strip()

    def _should_passthrough_sink_state(self, load_config) -> bool:
        options = getattr(load_config, "options", {}) or {}
        mode = str(options.get("incremental_state_timezone") or options.get("incremental_state_time_zone") or "")
        if mode.strip().lower() in {"passthrough", "none", "sink_native"}:
            return True
        sink_type = str(options.get("sink_type") or "").strip().lower()
        return sink_type in {"mssql", "sqlserver", "sql_server"}

    @staticmethod
    def _format_passthrough(max_value: Any) -> str:
        if hasattr(max_value, "strftime"):
            return max_value.strftime("%Y-%m-%d %H:%M:%S")
        return str(max_value).split("+")[0].strip()
