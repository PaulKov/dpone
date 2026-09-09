"""Универсальный Reconciliation Manager для любых sinks.

АРХИТЕКТУРА:
- Tech таблицы (__rs, __deleted_log) хранятся в BigQuery (централизованное хранилище)
- Soft delete применяется к target таблице (PostgreSQL, BigQuery, ClickHouse, etc)
- НЕ зависит от типа sink — работает с любым connector

WORKFLOW:
1. Снэпшот unique_key из source → BigQuery __rs таблица
2. Сравнение 2 последних снэпшотов в BigQuery (ANTI JOIN)
3. Soft delete/tombstone в target таблице через target-specific handler
4. Лог удалений → BigQuery __deleted_log таблица
"""

from __future__ import annotations

from typing import Any

from dpone.runtime.reconciliation.base import ReconciliationService
from dpone.runtime.reconciliation.bigquery import BigQueryReconciliationStore
from dpone.runtime.reconciliation.protocols import ReconciliationStore, SoftDeleteFinalizer
from dpone.runtime.reconciliation.soft_delete.registry import SoftDeleteRegistry
from dpone.runtime.reconciliation_logging import ETLLogger
from dpone.runtime.sql_helpers import ReconciliationQueries


class ReconciliationManager(ReconciliationService):
    """Универсальный Reconciliation Manager для любых sinks.

    Поддерживает:
    - PostgreSQL target (soft delete через psycopg)
    - BigQuery target (soft delete через google-cloud-bigquery)
    - ClickHouse target (soft delete через clickhouse-driver)

    Tech таблицы ВСЕГДА в BigQuery:
    - __rs (снэпшоты unique_key)
    - __deleted_log (аудит удалений)
    """

    def __init__(
        self,
        tech_connector: Any,
        target_connector: Any,
        logger: ETLLogger | None,
        tech_schema: str,
        *,
        reconciliation_store: ReconciliationStore | None = None,
        soft_delete_finalizer: SoftDeleteFinalizer | None = None,
    ):
        """Инициализирует ReconciliationManager с модульными компонентами.

        Args:
            tech_connector: BigQuery connector для tech таблиц
            target_connector: Connector целевой таблицы (PostgreSQL/BigQuery/ClickHouse)
            logger: ETL logger
            tech_schema: Схема для tech таблиц в BigQuery
        """
        super().__init__(tech_connector, target_connector, logger, tech_schema)
        self.target_connector_type = target_connector.__class__.__name__

        # Модульные компоненты
        self.tech_tables = reconciliation_store or BigQueryReconciliationStore(tech_connector, self.logger, tech_schema)
        self.soft_delete_registry = soft_delete_finalizer or SoftDeleteRegistry(self.logger)

    def ensure_reconciliation_infrastructure(
        self,
        target_schema: str,
        target_table: str,
        target_table_schema: list[tuple],
        unique_key: str | list[str],
    ) -> dict[str, Any]:
        """Создает инфраструктуру для reconciliation если её нет."""
        self.logger.log_etl_progress(
            "RECONCILIATION_INFRASTRUCTURE_SETUP",
            {
                "Target": f"{target_schema}.{target_table}",
                "TargetConnector": self.target_connector.__class__.__name__,
                "TechSchema": f"{self.tech_schema} (BigQuery)",
                "UniqueKey": str(unique_key),
            },
        )

        # 1. Создаем tech dataset в BigQuery
        self.tech_tables.ensure_tech_schema()

        # 2. Технические колонки создаются стратегиями автоматически
        self.logger.log_etl_progress(
            "RECONCILIATION_META_COLUMNS_CHECK",
            {
                "Target": f"{target_schema}.{target_table}",
                "Note": f"Технические колонки {self.META_LOAD_DTM} и {self.META_DELETE_DTM} создаются автоматически",
            },
        )

        # 3. Создаем служебные таблицы __rs и __deleted_log в BigQuery
        rs_table_created = self.tech_tables.ensure_snapshot_table(target_table, unique_key)
        deleted_log_created = self.tech_tables.ensure_deleted_log_table(target_table, unique_key)

        return {
            "tech_schema_created": True,
            "meta_columns_managed_by_strategies": True,
            "snapshot_table_created": rs_table_created,
            "deleted_log_table_created": deleted_log_created,
        }

    def process_reconciliation(
        self,
        target_schema: str,
        target_table: str,
        unique_key: str | list[str],
        source_connector: Any | None = None,
        source_schema: str | None = None,
        source_table: str | None = None,
        batch_size: int = 50000,
        batch_commit_mode: str = "whole",
        gcs_config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Выполняет reconciliation: сравнение снэпшотов и soft delete."""
        rs_table = f"{target_table}__rs"
        deleted_log_table = f"{target_table}__deleted_log"

        self.logger.log_etl_progress(
            "RECONCILIATION_PROCESS_START",
            {
                "Target": f"{target_schema}.{target_table} ({self.target_connector.__class__.__name__})",
                "SnapshotTable": f"{self.tech_schema}.{rs_table} (BigQuery)",
                "SnapshotSource": f"{source_connector.__class__.__name__} ({source_schema}.{source_table})",
                "BatchCommitMode": batch_commit_mode,
            },
        )

        # 1. Вставляем текущий снэпшот в BigQuery __rs
        unique_key_list = self._normalize_unique_key(unique_key)
        self.tech_tables.insert_snapshot_from_source(
            rs_table=rs_table,
            unique_key_list=unique_key_list,
            source_connector=source_connector,
            source_schema=source_schema,
            source_table=source_table,
            batch_size=batch_size,
            batch_commit_mode=batch_commit_mode,
            gcs_config=gcs_config,
        )

        # 2. Проверяем количество снэпшотов
        snapshot_count = self.tech_tables.get_snapshot_count(rs_table)

        # 3. Если снэпшотов меньше 2, reconciliation пропускаем
        if snapshot_count < 2:
            self.logger.log_etl_progress(
                "RECONCILIATION_SKIPPED",
                {
                    "Reason": "Недостаточно снэпшотов для сравнения",
                    "SnapshotCount": snapshot_count,
                    "RequiredCount": 2,
                },
            )
            return {
                "snapshot_count": snapshot_count,
                "soft_deleted_rows": 0,
                "deleted_log_entries": 0,
                "skipped": True,
            }

        # 4. Удаляем старые снэпшоты (оставляем только 2 последних)
        self.tech_tables.prune_old_snapshots(rs_table)

        # 5. Сравниваем снэпшоты и выполняем soft delete
        soft_delete_metrics = self._compare_snapshots_and_soft_delete(
            target_schema,
            target_table,
            rs_table,
            deleted_log_table,
            unique_key,
        )

        self.logger.log_etl_progress(
            "RECONCILIATION_PROCESS_COMPLETE",
            {
                "Target": f"{target_schema}.{target_table}",
                "SoftDeletedRows": soft_delete_metrics.get("soft_deleted_rows", 0),
                "DeletedLogEntries": soft_delete_metrics.get("deleted_log_entries", 0),
            },
        )

        return {
            "snapshot_count": snapshot_count,
            "soft_deleted_rows": soft_delete_metrics.get("soft_deleted_rows", 0),
            "deleted_log_entries": soft_delete_metrics.get("deleted_log_entries", 0),
            "skipped": False,
        }

    # ========================================================================
    # Snapshot Comparison & Soft Delete
    # ========================================================================

    def _compare_snapshots_and_soft_delete(
        self,
        target_schema: str,
        target_table: str,
        rs_table: str,
        deleted_log_table: str,
        unique_key: str | list[str],
    ) -> dict[str, int]:
        """Сравнивает 2 последних снэпшота в BigQuery и выполняет soft delete в target таблице.

        ⚠️ ВАЖНО: Если предыдущий снимок имеет другой unique_key, пропускает сравнение.
        """
        unique_key_list = self._normalize_unique_key(unique_key)
        project_id = self.tech_connector.project_id

        # Проверяем, что предыдущий снимок существует и имеет правильную схему
        # Если это первый снимок после изменения unique_key, предыдущего снимка может не быть
        # или он может иметь другую схему - в этом случае пропускаем сравнение
        try:
            # Получаем удаленные ключи из BigQuery (ANTI JOIN)
            deleted_keys_query = ReconciliationQueries.bq_get_deleted_keys(
                project_id=project_id,
                tech_schema=self.tech_schema,
                rs_table=rs_table,
                unique_key_columns=unique_key_list,
                meta_load_dtm=self.META_LOAD_DTM,
            )

            deleted_keys_result = self.tech_connector.get_records(deleted_keys_query, as_dict=True)
        except Exception as e:
            # Если запрос падает (например, из-за отсутствия колонок в предыдущем снимке),
            # пропускаем сравнение - это нормально при первом запуске после изменения unique_key
            error_msg = str(e)
            if "not found" in error_msg.lower() or "invalid" in error_msg.lower():
                self.logger.warning(
                    f"⚠️  Не удалось сравнить снимки (возможно, предыдущий снимок имеет другой unique_key). "
                    f"Пропускаем сравнение. Ошибка: {error_msg}"
                )
                self.logger.log_etl_progress(
                    "RECONCILIATION_SKIPPED",
                    {
                        "Reason": "Previous snapshot has different unique_key",
                        "CurrentUniqueKey": unique_key_list,
                        "Error": error_msg,
                    },
                )
                return {"soft_deleted_rows": 0, "deleted_log_entries": 0}
            else:
                raise

        if not deleted_keys_result:
            self.logger.log_etl_progress(
                "RECONCILIATION_NO_DELETIONS",
                {"SnapshotComparison": "No deletions detected"},
            )
            return {"soft_deleted_rows": 0, "deleted_log_entries": 0}

        # Выполняем soft delete в target таблице (PostgreSQL/BigQuery/ClickHouse)
        soft_deleted_rows = self.soft_delete_registry.execute_soft_delete(
            target_connector=self.target_connector,
            target_schema=target_schema,
            target_table=target_table,
            unique_key_list=unique_key_list,
            deleted_keys=deleted_keys_result,
            tech_schema=self.tech_schema,  # Для BigQuery
        )

        # Логируем удаления в BigQuery __deleted_log
        deleted_log_entries = 0
        if soft_deleted_rows > 0:
            insert_log_query = ReconciliationQueries.bq_insert_deleted_log(
                project_id=project_id,
                tech_schema=self.tech_schema,
                deleted_log_table=deleted_log_table,
                rs_table=rs_table,
                unique_key_columns=unique_key_list,
            )

            job_config = self.tech_connector._build_query_config(params=None)
            job = self.tech_connector.connection.query(insert_log_query, job_config=job_config)
            job.result()

            deleted_log_entries = self.tech_tables._extract_dml_stats(job, "insertedRowCount")

        return {
            "soft_deleted_rows": soft_deleted_rows,
            "deleted_log_entries": deleted_log_entries,
        }

    def _soft_delete_in_target(
        self,
        target_schema: str,
        target_table: str,
        unique_key_list: list[str],
        deleted_keys: list[dict[str, Any]],
    ) -> int:
        """Выполняет soft delete в target таблице через SoftDeleteRegistry.

        Реализация абстрактного метода из ReconciliationService.
        Делегирует выполнение в SoftDeleteRegistry для dynamic dispatch.

        Args:
            target_schema: Схема целевой таблицы
            target_table: Имя целевой таблицы
            unique_key_list: Список колонок unique_key
            deleted_keys: Список удаленных ключей [{key: value}, ...]

        Returns:
            Количество обновленных строк
        """
        return self.soft_delete_registry.execute_soft_delete(
            target_connector=self.target_connector,
            target_schema=target_schema,
            target_table=target_table,
            unique_key_list=unique_key_list,
            deleted_keys=deleted_keys,
            tech_schema=self.tech_schema,
        )
