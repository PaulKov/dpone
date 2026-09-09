"""Управление tech таблицами в BigQuery для reconciliation.

АРХИТЕКТУРА:
- Все tech таблицы (__rs, __deleted_log) хранятся в BigQuery (централизованное хранилище)
- Управление схемой, создание, пересоздание при изменении unique_key
- Снэпшоты из source → BigQuery __rs таблица
"""

from __future__ import annotations

import os
from typing import Any

from dpone.runtime.reconciliation.bigquery.tech_table_lifecycle import (
    ensure_deleted_log_table as lifecycle_ensure_deleted_log_table,
)
from dpone.runtime.reconciliation.bigquery.tech_table_lifecycle import (
    ensure_snapshot_table as lifecycle_ensure_snapshot_table,
)
from dpone.runtime.reconciliation.bigquery.tech_table_lifecycle import (
    ensure_tech_schema as lifecycle_ensure_tech_schema,
)
from dpone.runtime.reconciliation_logging import ETLLogger
from dpone.runtime.sql_helpers import ReconciliationQueries


class BigQueryReconciliationStore:
    """Управление tech таблицами в BigQuery для reconciliation.

    Отвечает за:
    - Создание/управление tech dataset в BigQuery
    - Создание/пересоздание snapshot таблицы (__rs)
    - Создание/пересоздание deleted_log таблицы (__deleted_log)
    - Вставка снэпшотов из source в BigQuery
    - Управление жизненным циклом снэпшотов (prune, count)
    """

    META_LOAD_DTM = "__dpone__loaded_at"
    META_DELETE_DTM = "__dpone__deleted_at"

    def __init__(
        self,
        tech_connector: Any,
        logger: ETLLogger,
        tech_schema: str,
    ):
        """Инициализирует BigQueryReconciliationStore.

        Args:
            tech_connector: BigQuery connector для tech таблиц
            logger: ETL logger
            tech_schema: Схема для tech таблиц в BigQuery
        """
        self.tech_connector = tech_connector
        self.logger = logger
        self.tech_schema = tech_schema

    def _ensure_clean_temp_file(self, file_path: str) -> None:
        """Удаляет временный файл если он существует (cleanup перед записью)."""
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass

    def ensure_tech_schema(self) -> None:
        """Создает схему (dataset) tech в BigQuery если её нет."""
        lifecycle_ensure_tech_schema(self)

    def ensure_snapshot_table(
        self,
        target_table: str,
        unique_key: str | list[str],
    ) -> bool:
        """Создает таблицу __rs для хранения снэпшотов в BigQuery если её нет.

        ⚠️ ВАЖНО: Если unique_key изменился, пересоздает таблицу (DROP + CREATE).

        Args:
            target_table: Имя целевой таблицы
            unique_key: Уникальный ключ (строка или список колонок)

        Returns:
            True если таблица была создана/пересоздана, False если уже существовала
        """
        return lifecycle_ensure_snapshot_table(self, target_table, unique_key)

    def ensure_deleted_log_table(
        self,
        target_table: str,
        unique_key: str | list[str],
    ) -> bool:
        """Создает таблицу __deleted_log для логирования удалений в BigQuery если её нет.

        ⚠️ ВАЖНО: Если unique_key изменился, пересоздает таблицу (DROP + CREATE).

        Args:
            target_table: Имя целевой таблицы
            unique_key: Уникальный ключ (строка или список колонок)

        Returns:
            True если таблица была создана/пересоздана, False если уже существовала
        """
        return lifecycle_ensure_deleted_log_table(self, target_table, unique_key)

    def _load_file_to_bigquery(
        self,
        tmp_file_path: str,
        table_id: str,
        job_config: Any,
        total_rows: int,
        rs_table: str,
        connector_class: str,
        source_schema: str,
        source_table: str,
    ) -> int:
        from dpone.runtime.reconciliation.bigquery.tech_table_load_jobs import load_file_to_bigquery

        return load_file_to_bigquery(
            self,
            tmp_file_path,
            table_id,
            job_config,
            total_rows,
            rs_table,
            connector_class,
            source_schema,
            source_table,
        )

    def insert_snapshot_from_source(
        self,
        rs_table: str,
        unique_key_list: list[str],
        source_connector: Any,
        source_schema: str,
        source_table: str,
        batch_size: int = 50000,
        batch_commit_mode: str = "whole",
        gcs_config: Any = None,
    ) -> int:
        """Снимает снэпшот из source БД и вставляет в BigQuery __rs таблицу.

        Универсальный метод для любых source connectors (PostgresConnector, ClickHouseConnector, etc).

        Args:
            rs_table: Имя snapshot таблицы (без __rs суффикса)
            unique_key_list: Список колонок unique_key
            source_connector: Source connector (PostgresConnector, ClickHouseConnector, etc)
            source_schema: Схема source таблицы
            source_table: Имя source таблицы
            batch_size: Размер batch для streaming read из source
            batch_commit_mode: Режим загрузки ('whole' или 'separate')

        Returns:
            Количество вставленных строк
        """
        if batch_commit_mode == "separate":
            # Батчевая загрузка через LIMIT/OFFSET
            return self._insert_snapshot_batched(
                rs_table=rs_table,
                unique_key_list=unique_key_list,
                source_connector=source_connector,
                source_schema=source_schema,
                source_table=source_table,
                batch_size=batch_size,
                gcs_config=gcs_config,
            )
        else:
            # Текущая реализация - один файл
            return self._insert_snapshot_whole(
                rs_table=rs_table,
                unique_key_list=unique_key_list,
                source_connector=source_connector,
                source_schema=source_schema,
                source_table=source_table,
                batch_size=batch_size,
            )

    def _insert_snapshot_whole(
        self,
        rs_table: str,
        unique_key_list: list[str],
        source_connector: Any,
        source_schema: str,
        source_table: str,
        batch_size: int = 50000,
    ) -> int:
        from dpone.runtime.reconciliation.bigquery.tech_table_snapshot_writer import insert_snapshot_whole

        return insert_snapshot_whole(
            self, rs_table, unique_key_list, source_connector, source_schema, source_table, batch_size
        )

    def _insert_snapshot_batched(
        self,
        rs_table: str,
        unique_key_list: list[str],
        source_connector: Any,
        source_schema: str,
        source_table: str,
        batch_size: int = 50000,
        gcs_config: Any = None,
    ) -> int:
        from dpone.runtime.reconciliation.bigquery.tech_table_snapshot_writer import insert_snapshot_batched

        return insert_snapshot_batched(
            self, rs_table, unique_key_list, source_connector, source_schema, source_table, batch_size, gcs_config
        )

    def _load_snapshot_batch_via_gcs(
        self,
        tmp_file_path: str,
        table_id: str,
        job_config: Any,
        batch_rows: int,
        batch_num: int,
        gcs_config: Any = None,
    ) -> int:
        from dpone.runtime.reconciliation.bigquery.tech_table_load_jobs import load_snapshot_batch_via_gcs

        return load_snapshot_batch_via_gcs(self, tmp_file_path, table_id, job_config, batch_rows, batch_num, gcs_config)

    def _load_snapshot_batch_direct(
        self,
        tmp_file_path: str,
        table_id: str,
        job_config: Any,
        batch_rows: int,
    ) -> int:
        from dpone.runtime.reconciliation.bigquery.tech_table_load_jobs import load_snapshot_batch_direct

        return load_snapshot_batch_direct(self, tmp_file_path, table_id, job_config, batch_rows)

    def prune_old_snapshots(self, rs_table: str) -> None:
        """Удаляет старые снэпшоты в BigQuery, оставляет только 2 последних.

        Args:
            rs_table: Имя snapshot таблицы (без __rs суффикса)
        """
        project_id = self.tech_connector.project_id

        prune_query = ReconciliationQueries.bq_prune_old_snapshots(
            project_id=project_id, tech_schema=self.tech_schema, rs_table=rs_table
        )

        job_config = self.tech_connector._build_query_config(params=None)
        job = self.tech_connector.connection.query(prune_query, job_config=job_config)
        job.result()

        deleted = self._extract_dml_stats(job, "deletedRowCount")

        if deleted > 0:
            self.logger.log_etl_progress(
                "OLD_SNAPSHOTS_PRUNED",
                {
                    "SnapshotTable": f"{self.tech_schema}.{rs_table} (BigQuery)",
                    "DeletedRows": deleted,
                },
            )

    def get_snapshot_count(self, rs_table: str) -> int:
        """Получает количество уникальных timestamp в BigQuery __rs таблице.

        Args:
            rs_table: Имя snapshot таблицы (без __rs суффикса)

        Returns:
            Количество уникальных timestamp (снэпшотов)
        """
        project_id = self.tech_connector.project_id

        count_query = ReconciliationQueries.bq_get_snapshot_count(
            project_id=project_id, tech_schema=self.tech_schema, rs_table=rs_table
        )

        result = self.tech_connector.get_records(count_query)
        if not result:
            return 0

        first_row = result[0]
        count_value = next(iter(first_row.values()))
        return int(count_value) if count_value is not None else 0

    def _extract_dml_stats(self, job: Any, stat_name: str, default: int = 0) -> int:
        """Извлекает DML статистику из BigQuery job результата."""
        try:
            stats = job._properties.get("statistics", {})
            query_stats = stats.get("query", {})
            dml = query_stats.get("dmlStats", {})
            return int(dml.get(stat_name, 0) or 0)
        except Exception:
            return default

    @staticmethod
    def _normalize_unique_key(unique_key: str | list[str]) -> list[str]:
        """Нормализует unique_key в список."""
        if isinstance(unique_key, str):
            return [unique_key]
        return unique_key
