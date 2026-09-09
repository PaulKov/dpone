"""Soft Delete для BigQuery target через google-cloud-bigquery."""

from __future__ import annotations

from typing import Any

from google.cloud import bigquery

from dpone.runtime.reconciliation_logging import ETLLogger
from dpone.runtime.sql_helpers import ReconciliationQueries


def soft_delete_bigquery(
    target_connector: Any,
    target_schema: str,
    target_table: str,
    unique_key_list: list[str],
    deleted_keys: list[dict[str, Any]],
    meta_load_dtm: str,
    meta_delete_dtm: str,
    logger: ETLLogger,
    tech_schema: str,  # Схема для tech таблиц в BigQuery
) -> int:
    """Soft delete в BigQuery target через google-cloud-bigquery.

    Обновляет:
    - __dpone__deleted_at = CURRENT_TIMESTAMP() (время удаления)
    - __dpone__loaded_at = CURRENT_TIMESTAMP() (время последнего обновления для audit trail)

    Args:
        target_connector: BigQueryConnector
        target_schema: Схема целевой таблицы
        target_table: Имя целевой таблицы
        unique_key_list: Список колонок unique_key
        deleted_keys: Список удаленных ключей [{key: value}, ...]
        meta_load_dtm: Имя колонки __dpone__loaded_at
        meta_delete_dtm: Имя колонки __dpone__deleted_at
        logger: ETL logger
        tech_schema: Схема для tech таблиц в BigQuery

    Returns:
        Количество обновленных строк
    """
    project_id = target_connector.project_id

    # Используем SQL из ReconciliationQueries
    update_query = ReconciliationQueries.bq_soft_delete_update(
        project_id=project_id,
        target_schema=target_schema,
        target_table=target_table,
        tech_schema=tech_schema,
        rs_table=f"{target_table}__rs",
        unique_key_columns=unique_key_list,
        meta_delete_dtm=meta_delete_dtm,
        meta_load_dtm=meta_load_dtm,
    )

    job_config = target_connector._build_query_config(params=None)
    job = target_connector.connection.query(update_query, job_config=job_config)
    job.result()

    updated_rows = _extract_dml_stats(job, "updatedRowCount")

    logger.log_etl_progress(
        "RECONCILIATION_SOFT_DELETE_COMPLETE",
        {
            "TargetTable": f"{project_id}.{target_schema}.{target_table} (BigQuery)",
            "UpdatedRows": updated_rows,
            "UpdatedColumns": f"{meta_delete_dtm}, {meta_load_dtm}",
        },
    )

    return updated_rows


def _extract_dml_stats(job: bigquery.QueryJob, stat_name: str, default: int = 0) -> int:
    """Извлекает DML статистику из BigQuery job результата."""
    try:
        stats = job._properties.get("statistics", {})
        query_stats = stats.get("query", {})
        dml = query_stats.get("dmlStats", {})
        return int(dml.get(stat_name, 0) or 0)
    except Exception:
        return default
