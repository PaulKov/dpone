"""Soft Delete для PostgreSQL target через psycopg."""

from __future__ import annotations

from typing import Any

from psycopg import sql

from dpone.runtime.reconciliation_logging import ETLLogger


def soft_delete_postgres(
    target_connector: Any,
    target_schema: str,
    target_table: str,
    unique_key_list: list[str],
    deleted_keys: list[dict[str, Any]],
    meta_load_dtm: str,
    meta_delete_dtm: str,
    logger: ETLLogger,
) -> int:
    """Soft delete в PostgreSQL target через psycopg.

    Обновляет:
    - __dpone__deleted_at = CURRENT_TIMESTAMP (время удаления)
    - __dpone__loaded_at = CURRENT_TIMESTAMP (время последнего обновления для audit trail)

    Args:
        target_connector: PostgresConnector
        target_schema: Схема целевой таблицы
        target_table: Имя целевой таблицы
        unique_key_list: Список колонок unique_key
        deleted_keys: Список удаленных ключей [{key: value}, ...]
        meta_load_dtm: Имя колонки __dpone__loaded_at
        meta_delete_dtm: Имя колонки __dpone__deleted_at
        logger: ETL logger

    Returns:
        Количество обновленных строк
    """

    if len(unique_key_list) == 1:
        # Single key
        key_col = unique_key_list[0]
        key_values = [row[key_col] for row in deleted_keys]

        update_sql = sql.SQL("""
            UPDATE {schema}.{table}
            SET {meta_delete_dtm} = CURRENT_TIMESTAMP,
                {meta_load_dtm} = CURRENT_TIMESTAMP
            WHERE {key_col} = ANY(%s)
              AND {meta_delete_dtm} IS NULL
        """).format(
            schema=sql.Identifier(target_schema),
            table=sql.Identifier(target_table),
            meta_delete_dtm=sql.Identifier(meta_delete_dtm),
            meta_load_dtm=sql.Identifier(meta_load_dtm),
            key_col=sql.Identifier(key_col),
        )

        result = target_connector.execute_query(update_sql, (key_values,))

    else:
        # Composite key: (key1, key2) IN ((val1, val2), ...)
        key_tuples = [tuple(row[key_col] for key_col in unique_key_list) for row in deleted_keys]

        key_identifiers = sql.SQL(", ").join([sql.Identifier(k) for k in unique_key_list])

        update_sql = sql.SQL("""
            UPDATE {schema}.{table}
            SET {meta_delete_dtm} = CURRENT_TIMESTAMP,
                {meta_load_dtm} = CURRENT_TIMESTAMP
            WHERE ({key_cols}) IN %s
              AND {meta_delete_dtm} IS NULL
        """).format(
            schema=sql.Identifier(target_schema),
            table=sql.Identifier(target_table),
            meta_delete_dtm=sql.Identifier(meta_delete_dtm),
            meta_load_dtm=sql.Identifier(meta_load_dtm),
            key_cols=key_identifiers,
        )

        result = target_connector.execute_query(update_sql, (tuple(key_tuples),))

    updated_rows = result if isinstance(result, int) else 0

    logger.log_etl_progress(
        "RECONCILIATION_SOFT_DELETE_COMPLETE",
        {
            "TargetTable": f"{target_schema}.{target_table} (PostgreSQL)",
            "UpdatedRows": updated_rows,
            "UpdatedColumns": f"{meta_delete_dtm}, {meta_load_dtm}",
        },
    )

    return updated_rows
