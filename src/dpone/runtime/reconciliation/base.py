"""Базовый класс для Reconciliation Service.

АРХИТЕКТУРА:
- Tech таблицы (__rs, __deleted_log) ВСЕГДА хранятся в BigQuery (централизованное хранилище state)
- Soft delete/tombstone применяется к target таблице (может быть PostgreSQL, BigQuery, ClickHouse, etc)
- Reconciliation НЕ зависит от sink — работает с любым target connector

ПРИНЦИП:
1. BigQuery (tech schema): Хранение снэпшотов + аудит удалений
2. Target DB (любой): Применение target-specific soft delete/tombstone
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dpone.runtime.reconciliation_logging import ETLLogger, etl_logger


class ReconciliationService(ABC):
    """Абстрактный базовый класс для Reconciliation Services.

    ВАЖНО: Все реализации должны:
    1. Хранить tech таблицы в BigQuery (централизованное хранилище)
    2. Применять soft delete к target connector (PostgreSQL, BigQuery, etc)
    3. Поддерживать любые source/target комбинации

    Реализации:
    - ReconciliationManager: Универсальный менеджер для любых sinks (PostgreSQL, BigQuery, ClickHouse)
    """

    META_LOAD_DTM = "__dpone__loaded_at"
    META_DELETE_DTM = "__dpone__deleted_at"

    def __init__(
        self,
        tech_connector: Any,
        target_connector: Any,
        logger: ETLLogger | None,
        tech_schema: str,
    ):
        """Инициализирует ReconciliationService.

        Args:
            tech_connector: BigQuery connector для tech таблиц (__rs, __deleted_log)
            target_connector: Connector для target таблицы (может быть любой: PostgreSQL, BigQuery, etc)
            logger: ETLLogger или None
            tech_schema: Схема для служебных таблиц reconciliation (в BigQuery)
        """
        self.tech_connector = tech_connector
        self.target_connector = target_connector
        self.logger = logger or etl_logger
        self.tech_schema = tech_schema

    @abstractmethod
    def ensure_reconciliation_infrastructure(
        self,
        target_schema: str,
        target_table: str,
        target_table_schema: list[tuple],  # [(column, type), ...]
        unique_key: str | list[str],
    ) -> dict[str, Any]:
        """Создает инфраструктуру для reconciliation если её нет.

        1. Создает dataset tech в BigQuery если его нет
        2. Создает служебные таблицы __rs и __deleted_log в BigQuery

        Args:
            target_schema: Целевая схема
            target_table: Целевая таблица
            target_table_schema: Схема целевой таблицы [(column, type), ...]
            unique_key: Уникальный ключ (строка или список колонок)

        Returns:
            Dict с информацией о созданной инфраструктуре
        """
        pass

    @abstractmethod
    def process_reconciliation(
        self,
        target_schema: str,
        target_table: str,
        unique_key: str | list[str],
        source_connector: Any | None = None,
        source_schema: str | None = None,
        source_table: str | None = None,
        batch_size: int = 50000,
    ) -> dict[str, Any]:
        """Выполняет reconciliation: сравнение снэпшотов и soft delete.

        Workflow:
        1. Вставляет текущий снэпшот в BigQuery __rs таблицу (из source)
        2. Сравнивает 2 последних снэпшота в BigQuery
        3. Выполняет soft delete в target таблице (любой connector)
        4. Логирует удаления в BigQuery __deleted_log

        Args:
            target_schema: Целевая схема
            target_table: Целевая таблица
            unique_key: Уникальный ключ (строка или список колонок)
            source_connector: Source connector (PostgresConnector, ClickHouseConnector, etc)
            source_schema: Схема source таблицы (опционально)
            source_table: Имя source таблицы (опционально)
            batch_size: Размер batch для streaming read из source

        Returns:
            Dict с метриками reconciliation
        """
        pass

    @abstractmethod
    def _soft_delete_in_target(
        self,
        target_schema: str,
        target_table: str,
        unique_key_list: list[str],
        deleted_keys: list[dict[str, Any]],  # List of {key_col: value, ...}
    ) -> int:
        """Выполняет soft delete в target таблице (PostgreSQL, BigQuery, etc).

        Помечает удаленные записи target-specific способом.

        Args:
            target_schema: Целевая схема
            target_table: Целевая таблица
            unique_key_list: Список колонок unique_key
            deleted_keys: Список удаленных ключей [{key_col: value, ...}, ...]

        Returns:
            Количество обновленных записей
        """
        pass

    def _normalize_unique_key(self, unique_key: str | list[str]) -> list[str]:
        """Нормализует unique_key в список."""
        if isinstance(unique_key, str):
            return [unique_key]
        return unique_key
