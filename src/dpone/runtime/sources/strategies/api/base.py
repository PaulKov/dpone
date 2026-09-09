"""
Базовая стратегия извлечения данных из API источников.

Предоставляет общую инфраструктуру для всех API стратегий:
- Работа с API коннекторами
- Получение state из sink (MAX column)
- Формирование ExtractResult
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.sources.extract_result import ExtractResult


from abc import abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from dpone._compat import UTC
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.strategies.base import SourceStrategy
from dpone.runtime.streaming_rows import StreamingRowsArtifact


@dataclass
class APIExtractConfig:
    """Конфигурация извлечения из API."""

    resource: str  # cases, messages, users, etc.
    incremental_column: str | None = None  # updated_at, created_at
    lookback_days: int = 3
    max_pages: int | None = None
    batch_size: int = 1000

    # Фильтры для запроса
    filters: dict[str, Any] | None = None


class APIBaseStrategy(SourceStrategy):
    """
    Базовая стратегия для извлечения данных из API.

    Отличия от БД стратегий:
    - Нет SQL запросов
    - Работа с итераторами словарей
    - Схема определяется из первой записи или явно
    """

    def __init__(
        self,
        connector,
        sink_connector,
        logger,
    ):
        """
        Args:
            connector: API коннектор (OmnideskConnector, etc.)
            sink_connector: Коннектор к sink для получения MAX()
            logger: ETL logger
        """
        self.connector = connector
        self.sink_connector = sink_connector
        self.logger = logger

    # -------------------------------------------------------------------------
    # State Management (через MAX из sink)
    # -------------------------------------------------------------------------

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        """
        Получает инкрементальное состояние из sink таблицы.

        Returns:
            {"last_value": datetime, "column": "updated_at"} или None
        """
        options = self._get_options(load_config)
        incremental_column = options.get("incremental_column")

        if not incremental_column:
            self.logger.info("incremental_column не указан. Будет выполнен full refresh.")
            return None

        # Получаем MAX(incremental_column) из sink
        max_value = self._get_max_from_sink(load_config, incremental_column)

        if max_value is None:
            self.logger.info(f"MAX({incremental_column}) не найден в sink. Это первая загрузка (full refresh).")
            return None

        self.logger.log_etl_progress(
            "API_INCREMENTAL_STATE",
            {
                "Column": incremental_column,
                "Max_Value": str(max_value),
                "Sink": f"{load_config.target_schema}.{load_config.target_table}",
            },
        )

        return {
            "last_value": max_value,
            "column": incremental_column,
        }

    def _get_max_from_sink(
        self,
        load_config: Any,
        column: str,
    ) -> Any | None:
        """Получает MAX(column) из sink таблицы."""
        if not self.sink_connector:
            return None

        try:
            return self.sink_connector.get_max_column_value(
                schema=load_config.target_schema,
                table=load_config.target_table,
                column=column,
            )
        except Exception as exc:
            self.logger.warning(f"Ошибка при получении MAX({column}) из sink: {exc}")
            return None

    # -------------------------------------------------------------------------
    # Extract (абстрактный — реализуется в наследниках)
    # -------------------------------------------------------------------------

    @abstractmethod
    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        """Извлекает данные из API."""

    # -------------------------------------------------------------------------
    # Schema Detection (использует DataTypeMapper)
    # -------------------------------------------------------------------------

    def _get_target_db(self) -> str:
        """Определяет тип целевой БД по sink_connector."""
        if self.sink_connector is None:
            return "bigquery"  # default

        connector_class = type(self.sink_connector).__name__.lower()
        if "postgres" in connector_class:
            return "postgresql"
        elif "bigquery" in connector_class:
            return "bigquery"
        elif "clickhouse" in connector_class:
            return "clickhouse"
        else:
            return "bigquery"  # default fallback

    def _detect_schema_from_records(
        self,
        records: list[dict[str, Any]],
    ) -> Sequence[tuple[str, str]]:
        """
        Определяет схему из списка записей.

        Использует DataTypeMapper для корректного определения типов:
        - По Python типу значения (int → INT64, bool → BOOLEAN)
        - По имени колонки (created_at → TIMESTAMP)
        - По формату строки (ISO datetime → TIMESTAMP)
        """
        from dpone.runtime.support.data_type_mapper import DataTypeMapper

        if not records:
            return []

        # Берём первую запись как образец
        sample = records[0]
        target_db = self._get_target_db()
        return DataTypeMapper.detect_schema_from_record(sample, target_db=target_db)

    # -------------------------------------------------------------------------
    # Artifact Building
    # -------------------------------------------------------------------------

    def _build_artifact_from_list(
        self,
        records: list[dict[str, Any]],
    ) -> InMemoryRowsArtifact:
        """Создаёт артефакт из списка записей."""
        return InMemoryRowsArtifact(records)

    def _build_artifact_from_iterator(
        self,
        iterator: Iterator[dict[str, Any]],
        batch_size: int = 1000,
        estimated_rows: int | None = None,
    ) -> StreamingRowsArtifact:
        """Создаёт streaming артефакт из итератора."""
        return StreamingRowsArtifact(
            iterator=iterator,
            batch_size=batch_size,
            estimated_rows=estimated_rows,
        )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _get_options(self, load_config: Any) -> dict[str, Any]:
        """Получает options из load_config."""
        return getattr(load_config, "options", {}) or {}

    def _calculate_from_time(
        self,
        last_value: Any,
        lookback_days: int,
    ) -> datetime:
        """
        Вычисляет from_time с учётом lookback.

        Args:
            last_value: MAX(updated_at) из sink
            lookback_days: Дней назад от last_value

        Returns:
            datetime для from_time параметра API (всегда timezone-aware UTC)
        """
        if isinstance(last_value, datetime):
            # Если naive datetime — считаем что это UTC
            if last_value.tzinfo is None:
                last_value = last_value.replace(tzinfo=UTC)
            return last_value - timedelta(days=lookback_days)

        if isinstance(last_value, str):
            # Парсим строку
            from dpone.runtime.support.timezone import TimezoneConverter

            converter = TimezoneConverter(target_tz="UTC")
            dt = converter.parse_timestamp(last_value)
            if dt:
                # Убеждаемся что результат timezone-aware
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt - timedelta(days=lookback_days)

        # Fallback: N дней назад от сейчас
        self.logger.warning(f"Не удалось распарсить last_value: {last_value}. Используем now() - {lookback_days} дней.")
        return datetime.now(UTC) - timedelta(days=lookback_days)
