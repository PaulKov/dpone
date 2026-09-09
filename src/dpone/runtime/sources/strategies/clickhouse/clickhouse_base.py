"""Базовый класс для ClickHouse стратегий извлечения."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from dpone.runtime.sources.strategies.base import SourceStrategy
from dpone.runtime.support.gcs import parse_date_value


class ClickHouseBaseStrategy(SourceStrategy):
    """
    Базовый класс для всех ClickHouse стратегий.

    Предоставляет общие методы:
    - get_table_schema() - получение схемы из system.columns
    - build_select_query() - построение SELECT запросов
    - build_date_predicates() - построение предикатов по датам
    - get_options() - извлечение options из load_config
    """

    def __init__(self, connector, logger):
        self.connector = connector
        self.logger = logger

    def get_options(self, load_config) -> dict:
        """Извлекает options из load_config с безопасным fallback на пустой dict."""
        return getattr(load_config, "options", {}) or {}

    def get_table_schema(self, load_config):
        """
        Получает схему таблицы из system.columns ClickHouse.

        Returns:
            List[Tuple[str, str]]: Список (column_name, column_type)
        """
        database = load_config.source_schema or self.connector.database
        query = """
            SELECT name, type
            FROM system.columns
            WHERE database = %(database)s AND table = %(table)s
            ORDER BY position
        """
        rows = self.connector.get_records(
            query,
            {"database": database, "table": load_config.source_table},
            as_dict=True,
        )
        return [(row["name"], row["type"]) for row in rows]

    @staticmethod
    def extract_timezone_from_column_type(column_type: str) -> str | None:
        """
        Извлекает timezone из типа колонки ClickHouse.

        Примеры:
            DateTime64(3, 'Europe/Moscow') -> 'Europe/Moscow'
            DateTime('Europe/Moscow') -> 'Europe/Moscow'
            DateTime -> None
            Date -> None

        Returns:
            Timezone string или None
        """
        import re

        # Ищем timezone в типе: DateTime64(3, 'Europe/Moscow') или DateTime('Europe/Moscow')
        match = re.search(r"['\"]([^'\"]+)['\"]", column_type)
        if match:
            return match.group(1)

        return None

    @staticmethod
    def transform_column_for_export(col_name: str, col_type: str) -> str:
        """
        Трансформирует колонку для корректного экспорта в Parquet/BigQuery.

        **ПРОБЛЕМА:**
        - DateTime('timezone') экспортируется в Parquet как INT32 (Unix timestamp в секундах)
        - BigQuery/Parquet reader интерпретирует INT32 как MICROSECONDS → epoch overflow
        - Результат: '1970-01-01 00:28:21' вместо '2023-11-29 18:19:52'

        **РЕШЕНИЕ:**
        - DateTime / DateTime('tz') → toDateTime64(col, 3)  [конвертация в ms precision]
        - DateTime64 → оставляем как есть (уже корректен, хранит ms/µs)
        - Остальные типы → оставляем как есть

        Args:
            col_name: Имя колонки
            col_type: Тип колонки из system.columns

        Returns:
            Трансформированное выражение для SELECT (например: "toDateTime64(`col`, 3)")
        """
        import re

        base_type = col_type
        base_type = re.sub(r"^Nullable\(", "", base_type)
        base_type = re.sub(r"^LowCardinality\(", "", base_type)
        base_type = re.sub(r"\)$", "", base_type)

        # Проверяем, является ли это DateTime (но НЕ DateTime64)
        if base_type.startswith("DateTime") and not base_type.startswith("DateTime64"):
            # DateTime или DateTime('timezone') → конвертируем в DateTime64(3)
            # Это обеспечивает корректную запись в Parquet с millisecond precision
            return f"toDateTime64(`{col_name}`, 3)"

        # Все остальные типы (включая DateTime64, Date, числа, строки) — оставляем как есть
        return f"`{col_name}`"

    @staticmethod
    def build_select_query(
        schema: str,
        table: str,
        columns: list[str],
        predicate: str | None = None,
        column_types: list[tuple[str, str]] | None = None,
    ) -> str:
        """
        Формирует SELECT запрос для ClickHouse с трансформацией типов для экспорта.

        Args:
            schema: Имя базы данных
            table: Имя таблицы
            columns: Список колонок для выборки
            predicate: WHERE clause (опционально)
            column_types: Список (col_name, col_type) для трансформации типов (опционально)

        Returns:
            SQL запрос в виде строки
        """
        # Если передана схема типов, используем трансформацию
        if column_types:
            from dpone.runtime.sources.strategies.clickhouse.clickhouse_base import ClickHouseBaseStrategy

            columns_str = ", ".join(
                f"{ClickHouseBaseStrategy.transform_column_for_export(col_name, col_type)} AS `{col_name}`"
                for col_name, col_type in column_types
            )
        elif columns:
            columns_str = ", ".join(f"`{col}`" for col in columns)
        else:
            columns_str = "*"

        full_table = f"`{schema}`.`{table}`" if schema else f"`{table}`"
        base = f"SELECT {columns_str} FROM {full_table}"
        if predicate:
            base += f" WHERE {predicate}"
        return base

    @staticmethod
    def build_date_predicates(
        date_column: str,
        date_from: Any | None,
        date_to: Any | None,
        partition_by: str | None,
    ) -> list[str]:
        """
        Строит предикаты для фильтрации по дате с поддержкой разных типов партиционирования.
        """
        predicates = []

        # Маппинг partition_by на функцию ClickHouse и формат даты
        partition_config = {
            "day": ("toYYYYMMDD", "%Y%m%d"),
            "month": ("toYYYYMM", "%Y%m"),
            "year": ("toYear", None),  # year не требует форматирования
        }

        if partition_by and partition_by in partition_config:
            ch_function, date_format = partition_config[partition_by]

            if date_from:
                dt = parse_date_value(date_from)
                if partition_by == "year":
                    formatted = str(dt.year)
                else:
                    formatted = dt.strftime(date_format)
                predicates.append(f"{ch_function}(`{date_column}`) >= {formatted}")

            if date_to:
                dt = parse_date_value(date_to)
                if partition_by == "year":
                    formatted = str(dt.year)
                else:
                    formatted = dt.strftime(date_format)
                predicates.append(f"{ch_function}(`{date_column}`) <= {formatted}")
        else:
            # Обычная фильтрация без партиционирования
            if date_from:
                dt = parse_date_value(date_from)
                predicates.append(f"`{date_column}` >= '{dt.strftime('%Y-%m-%d')}'")
            if date_to:
                dt = parse_date_value(date_to)
                predicates.append(f"`{date_column}` <= '{dt.strftime('%Y-%m-%d')}'")

        return predicates

    @staticmethod
    def build_incremental_query(
        schema: str,
        table: str,
        columns: list[str],
        incremental_column: str,
        last_value: Any,
        predicate: str | None = None,
        column_types: list[tuple[str, str]] | None = None,
    ) -> str:
        """
        Формирует инкрементальный SELECT запрос с трансформацией типов для экспорта.

        Args:
            schema: Имя базы данных
            table: Имя таблицы
            columns: Список колонок для выборки
            incremental_column: Колонка для инкремента
            last_value: Последнее значение (datetime, date, int, str)
            predicate: Дополнительный WHERE clause (опционально)
            column_types: Список (col_name, col_type) для трансформации типов (опционально)

        Returns:
            SQL запрос в виде строки
        """
        # Если передана схема типов, используем трансформацию
        if column_types:
            from dpone.runtime.sources.strategies.clickhouse.clickhouse_base import ClickHouseBaseStrategy

            columns_str = ", ".join(
                f"{ClickHouseBaseStrategy.transform_column_for_export(col_name, col_type)} AS `{col_name}`"
                for col_name, col_type in column_types
            )
        elif columns:
            columns_str = ", ".join(f"`{col}`" for col in columns)
        else:
            columns_str = "*"

        full_table = f"`{schema}`.`{table}`" if schema else f"`{table}`"

        # Формируем условие инкремента
        # Поддержка разных типов: datetime, date, int
        if isinstance(last_value, datetime | date):
            # Для datetime/date форматируем в ISO строку
            condition = f"`{incremental_column}` > '{last_value}'"
        elif isinstance(last_value, str):
            # Строка (может быть ISO datetime)
            condition = f"`{incremental_column}` > '{last_value}'"
        else:
            # Числовые типы
            condition = f"`{incremental_column}` > {last_value}"

        base = f"SELECT {columns_str} FROM {full_table} WHERE {condition}"

        if predicate:
            base += f" AND ({predicate})"

        return base
