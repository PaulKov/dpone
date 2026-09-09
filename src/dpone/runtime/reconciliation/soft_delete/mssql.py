"""Soft delete support for Microsoft SQL Server targets."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from dpone.runtime.reconciliation_logging import ETLLogger

SQL_SERVER_MAX_PARAMETERS = 2100
SAFE_MAX_PARAMETERS = 2000


def soft_delete_mssql(
    target_connector: Any,
    target_schema: str,
    target_table: str,
    unique_key_list: list[str],
    deleted_keys: list[dict[str, Any]],
    meta_load_dtm: str,
    meta_delete_dtm: str,
    logger: ETLLogger,
) -> int:
    """Soft delete rows in a SQL Server target table.

    The reconciliation layer detects physical deletes by comparing key snapshots.
    This handler applies those detected deletes to SQL Server by setting
    ``__dpone__deleted_at`` and refreshing ``__dpone__loaded_at``. It intentionally uses
    parameterized ``?`` placeholders and connector-provided identifier quoting so
    the generated SQL stays safe for user-controlled key values and SQL Server
    reserved words.

    Large key lists are split into chunks below SQL Server's 2100 parameter
    limit. Composite keys use ``OR`` groups because SQL Server does not support
    PostgreSQL-style tuple ``IN`` parameter binding.

    Args:
        target_connector: MSSQLConnector-compatible target connector.
        target_schema: Target schema name.
        target_table: Target table name.
        unique_key_list: Unique key columns used by reconciliation.
        deleted_keys: Deleted key rows, one dict per deleted source row.
        meta_load_dtm: Audit load timestamp column.
        meta_delete_dtm: Soft-delete timestamp column.
        logger: ETL logger.

    Returns:
        Number of rows reported as updated by the target connector.
    """
    if not deleted_keys:
        logger.log_etl_progress(
            "RECONCILIATION_SOFT_DELETE_SKIPPED",
            {
                "TargetTable": f"{target_schema}.{target_table} (MSSQL)",
                "Reason": "No deleted keys",
            },
        )
        return 0

    if not unique_key_list:
        raise ValueError("MSSQL soft delete requires at least one unique key column")

    max_rows_per_chunk = max(1, SAFE_MAX_PARAMETERS // len(unique_key_list))
    updated_rows = 0

    for key_chunk in _chunks(deleted_keys, max_rows_per_chunk):
        query, params = _build_soft_delete_query(
            target_connector=target_connector,
            target_schema=target_schema,
            target_table=target_table,
            unique_key_list=unique_key_list,
            deleted_keys=key_chunk,
            meta_load_dtm=meta_load_dtm,
            meta_delete_dtm=meta_delete_dtm,
        )
        result = target_connector.execute_query(query, tuple(params))
        if isinstance(result, int):
            updated_rows += result

    logger.log_etl_progress(
        "RECONCILIATION_SOFT_DELETE_COMPLETE",
        {
            "TargetTable": f"{target_schema}.{target_table} (MSSQL)",
            "UpdatedRows": updated_rows,
            "UpdatedColumns": f"{meta_delete_dtm}, {meta_load_dtm}",
            "Chunks": _chunk_count(len(deleted_keys), max_rows_per_chunk),
        },
    )

    return updated_rows


def _build_soft_delete_query(
    *,
    target_connector: Any,
    target_schema: str,
    target_table: str,
    unique_key_list: list[str],
    deleted_keys: list[dict[str, Any]],
    meta_load_dtm: str,
    meta_delete_dtm: str,
) -> tuple[str, list[Any]]:
    qualified_table = _qualified_name(target_connector, target_schema, target_table)
    meta_load = _quote_identifier(target_connector, meta_load_dtm)
    meta_delete = _quote_identifier(target_connector, meta_delete_dtm)

    if len(unique_key_list) == 1:
        key_col = unique_key_list[0]
        key_identifier = _quote_identifier(target_connector, key_col)
        placeholders = ", ".join("?" for _ in deleted_keys)
        params = [_key_value(row, key_col) for row in deleted_keys]
        predicate = f"{key_identifier} IN ({placeholders})"
    else:
        predicates: list[str] = []
        params = []
        for row in deleted_keys:
            key_predicates = []
            for key_col in unique_key_list:
                key_predicates.append(f"{_quote_identifier(target_connector, key_col)} = ?")
                params.append(_key_value(row, key_col))
            predicates.append("(" + " AND ".join(key_predicates) + ")")
        predicate = "(" + " OR ".join(predicates) + ")"

    query = f"""
        UPDATE {qualified_table}
        SET {meta_delete} = SYSUTCDATETIME(),
            {meta_load} = SYSUTCDATETIME()
        WHERE {meta_delete} IS NULL
          AND {predicate}
    """
    return query, params


def _chunks(rows: list[dict[str, Any]], chunk_size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), chunk_size):
        yield rows[start : start + chunk_size]


def _chunk_count(row_count: int, chunk_size: int) -> int:
    return (row_count + chunk_size - 1) // chunk_size


def _qualified_name(target_connector: Any, schema: str, table: str) -> str:
    if hasattr(target_connector, "qualified_name"):
        return str(target_connector.qualified_name(schema, table))
    return f"{_quote_identifier(target_connector, schema)}.{_quote_identifier(target_connector, table)}"


def _quote_identifier(target_connector: Any, name: str) -> str:
    if hasattr(target_connector, "quote_identifier"):
        return str(target_connector.quote_identifier(name))
    return "[" + str(name).replace("]", "]]") + "]"


def _key_value(row: dict[str, Any], key_col: str) -> Any:
    try:
        return row[key_col]
    except KeyError as exc:
        raise ValueError(f"Deleted key row is missing unique key column: {key_col}") from exc
