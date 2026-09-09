"""Soft Delete Registry для dynamic dispatch по типу connector."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any

from dpone.runtime.reconciliation_logging import ETLLogger


class SoftDeleteRegistry:
    """Registry для soft delete handlers по типу connector.

    Использует dynamic dispatch вместо хардкода if/elif.
    Позволяет легко добавлять новые sink типы без изменения логики диспатчинга.
    """

    META_LOAD_DTM = "__dpone__loaded_at"
    META_DELETE_DTM = "__dpone__deleted_at"

    def __init__(self, logger: ETLLogger):
        """Инициализирует SoftDeleteRegistry.

        Args:
            logger: ETL logger
        """
        self.logger = logger

        # Handler registry: connector type -> target function path. Import lazily
        # so generic reconciliation core does not import all vendor adapters.
        self._handlers: dict[str, str | Callable[..., int]] = {
            "PostgresConnector": "dpone.runtime.reconciliation.soft_delete.postgres:soft_delete_postgres",
            "BigQueryConnector": "dpone.runtime.reconciliation.soft_delete.bigquery:soft_delete_bigquery",
            "ClickHouseConnector": "dpone.runtime.reconciliation.soft_delete.clickhouse:soft_delete_clickhouse",
            "MSSQLConnector": "dpone.runtime.reconciliation.soft_delete.mssql:soft_delete_mssql",
        }

    def execute_soft_delete(
        self,
        target_connector: Any,
        target_schema: str,
        target_table: str,
        unique_key_list: list[str],
        deleted_keys: list[dict[str, Any]],
        tech_schema: str,
    ) -> int:
        """Выполняет soft delete в целевой таблице через dynamic dispatch.

        Args:
            target_connector: Target connector (PostgresConnector, BigQueryConnector, etc)
            target_schema: Схема целевой таблицы
            target_table: Имя целевой таблицы
            unique_key_list: Список колонок unique_key
            deleted_keys: Список удаленных ключей [{key: value}, ...]
            tech_schema: Схема для tech таблиц в BigQuery (используется только для BigQuery)

        Returns:
            Количество обновленных строк

        Raises:
            ValueError: Если connector type не поддерживается
        """
        connector_class = target_connector.__class__.__name__

        self.logger.log_etl_progress(
            "RECONCILIATION_SOFT_DELETE_TARGET",
            {
                "TargetConnector": connector_class,
                "TargetTable": f"{target_schema}.{target_table}",
                "DeletedKeysCount": len(deleted_keys),
            },
        )

        # Dynamic dispatch через handler registry
        handler_path = self._handlers.get(connector_class)

        if handler_path is None:
            supported = ", ".join(self._handlers.keys())
            raise ValueError(
                f"❌ Soft delete не поддерживается для connector type: {connector_class}.\n"
                f"Поддерживаемые типы: {supported}\n"
                f"Добавьте функцию soft_delete_{connector_class.lower().replace('connector', '')}() "
                f"и зарегистрируйте её в SoftDeleteRegistry._handlers"
            )
        if isinstance(handler_path, str):
            module_name, attr_name = handler_path.split(":", 1)
            handler = getattr(import_module(module_name), attr_name)
        else:
            handler = handler_path

        # Вызываем соответствующий handler
        # Для BigQuery передаем tech_schema, для остальных - игнорируем
        handler_kwargs = {
            "target_connector": target_connector,
            "target_schema": target_schema,
            "target_table": target_table,
            "unique_key_list": unique_key_list,
            "deleted_keys": deleted_keys,
            "meta_load_dtm": self.META_LOAD_DTM,
            "meta_delete_dtm": self.META_DELETE_DTM,
            "logger": self.logger,
        }

        # Для BigQuery добавляем tech_schema
        if connector_class == "BigQueryConnector":
            handler_kwargs["tech_schema"] = tech_schema

        return handler(**handler_kwargs)
