"""Non-mutation soft-delete reconciliation for ClickHouse targets."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any

from dpone.runtime.reconciliation_logging import ETLLogger

DEFAULT_KEY_INSERT_CHUNK_SIZE = 10_000


def soft_delete_clickhouse(
    target_connector: Any,
    target_schema: str,
    target_table: str,
    unique_key_list: list[str],
    deleted_keys: list[dict[str, Any]],
    meta_load_dtm: str,
    meta_delete_dtm: str,
    logger: ETLLogger,
) -> int:
    """Apply source physical deletes to a ClickHouse target without mutations.

    ClickHouse ``ALTER TABLE ... UPDATE`` is implemented as an asynchronous table
    mutation and is a poor default for reconciliation SLOs. This handler therefore
    uses a deterministic shadow-table swap:

    1. Stage deleted keys in a temporary Memory table.
    2. Create an empty shadow table with the same structure as the target.
    3. Copy all target rows into the shadow table, replacing technical columns for
       rows whose keys disappeared from the source.
    4. Atomically rename the target to a backup and the shadow table to the target.
    5. Drop temporary artifacts.

    This preserves raw-table semantics without ClickHouse mutations. It is heavier
    than an append-only tombstone, but predictable and safe for snapshot-based
    reconciliation.
    """
    if not deleted_keys:
        logger.log_etl_progress(
            "RECONCILIATION_SOFT_DELETE_SKIPPED",
            {
                "TargetTable": f"{target_schema}.{target_table} (ClickHouse)",
                "Reason": "No deleted keys",
            },
        )
        return 0

    if not unique_key_list:
        raise ValueError("ClickHouse reconciliation requires at least one unique key column")

    unique_deleted_keys = _deduplicate_key_rows(deleted_keys, unique_key_list)
    columns = _fetch_target_columns(target_connector, target_schema, target_table)
    _validate_required_columns(columns, unique_key_list, meta_load_dtm, meta_delete_dtm)

    operation_id = _operation_id(target_connector)
    key_table = f"__dpone_deleted_keys_{operation_id}"
    shadow_table = f"{target_table}__dpone_reconcile_{operation_id}"
    backup_table = f"{target_table}__dpone_reconcile_backup_{operation_id}"
    key_insert_chunk_size = int(
        getattr(target_connector, "reconciliation_clickhouse_key_insert_chunk_size", DEFAULT_KEY_INSERT_CHUNK_SIZE)
        or DEFAULT_KEY_INSERT_CHUNK_SIZE
    )

    logger.log_etl_progress(
        "RECONCILIATION_CLICKHOUSE_SHADOW_SWAP_START",
        {
            "TargetTable": f"{target_schema}.{target_table}",
            "DeletedKeys": len(unique_deleted_keys),
            "Strategy": "shadow_table_swap",
            "MutationFree": True,
        },
    )

    swapped = False
    try:
        _create_key_table(target_connector, key_table, unique_key_list)
        _insert_deleted_keys(target_connector, key_table, unique_key_list, unique_deleted_keys, key_insert_chunk_size)
        matched_rows = _count_matching_active_rows(
            target_connector=target_connector,
            target_schema=target_schema,
            target_table=target_table,
            key_table=key_table,
            unique_key_list=unique_key_list,
            meta_delete_dtm=meta_delete_dtm,
        )
        _create_shadow_table(target_connector, target_schema, target_table, shadow_table)
        _copy_target_with_tombstones(
            target_connector=target_connector,
            target_schema=target_schema,
            target_table=target_table,
            shadow_table=shadow_table,
            key_table=key_table,
            columns=columns,
            unique_key_list=unique_key_list,
            meta_load_dtm=meta_load_dtm,
            meta_delete_dtm=meta_delete_dtm,
        )
        _swap_shadow_table(target_connector, target_schema, target_table, shadow_table, backup_table)
        swapped = True
        _drop_table(target_connector, _qualified_name(target_schema, backup_table))
    finally:
        _drop_table(target_connector, _quote_identifier(key_table))
        if not swapped:
            _drop_table(target_connector, _qualified_name(target_schema, shadow_table))

    logger.log_etl_progress(
        "RECONCILIATION_SOFT_DELETE_COMPLETE",
        {
            "TargetTable": f"{target_schema}.{target_table} (ClickHouse)",
            "UpdatedRows": matched_rows,
            "DeletedKeys": len(unique_deleted_keys),
            "Strategy": "shadow_table_swap",
            "MutationFree": True,
            "UpdatedColumns": f"{meta_delete_dtm}, {meta_load_dtm}",
        },
    )

    return matched_rows


def _create_key_table(target_connector: Any, key_table: str, unique_key_list: list[str]) -> None:
    key_columns = ", ".join(f"{_quote_identifier(column)} String" for column in unique_key_list)
    target_connector.execute_query(
        f"""
        CREATE TEMPORARY TABLE {_quote_identifier(key_table)}
        (
            {key_columns},
            {_quote_identifier("__dpone__deleted_marker")} UInt8
        )
        ENGINE = Memory
        """
    )


def _insert_deleted_keys(
    target_connector: Any,
    key_table: str,
    unique_key_list: list[str],
    deleted_keys: list[dict[str, Any]],
    chunk_size: int,
) -> None:
    columns = unique_key_list + ["__dpone__deleted_marker"]
    column_sql = ", ".join(_quote_identifier(column) for column in columns)
    for chunk in _chunks(deleted_keys, max(1, chunk_size)):
        values_sql = ", ".join(_key_values_tuple(row, unique_key_list) for row in chunk)
        target_connector.execute_query(
            f"""
            INSERT INTO {_quote_identifier(key_table)} ({column_sql})
            VALUES {values_sql}
            """
        )


def _count_matching_active_rows(
    *,
    target_connector: Any,
    target_schema: str,
    target_table: str,
    key_table: str,
    unique_key_list: list[str],
    meta_delete_dtm: str,
) -> int:
    if not hasattr(target_connector, "get_records"):
        return 0

    rows = target_connector.get_records(
        f"""
        SELECT count()
        FROM {_qualified_name(target_schema, target_table)} AS t
        ANY INNER JOIN {_quote_identifier(key_table)} AS k
            ON {_join_predicate(unique_key_list)}
        WHERE t.{_quote_identifier(meta_delete_dtm)} IS NULL
        """
    )
    return _first_int(rows)


def _create_shadow_table(target_connector: Any, target_schema: str, target_table: str, shadow_table: str) -> None:
    target_connector.execute_query(
        f"""
        CREATE TABLE {_qualified_name(target_schema, shadow_table)}
        AS {_qualified_name(target_schema, target_table)}
        """
    )


def _copy_target_with_tombstones(
    *,
    target_connector: Any,
    target_schema: str,
    target_table: str,
    shadow_table: str,
    key_table: str,
    columns: list[str],
    unique_key_list: list[str],
    meta_load_dtm: str,
    meta_delete_dtm: str,
) -> None:
    column_sql = ", ".join(_quote_identifier(column) for column in columns)
    select_sql = ",\n            ".join(
        _select_expression(column, meta_load_dtm=meta_load_dtm, meta_delete_dtm=meta_delete_dtm) for column in columns
    )
    target_connector.execute_query(
        f"""
        INSERT INTO {_qualified_name(target_schema, shadow_table)} ({column_sql})
        SELECT
            {select_sql}
        FROM {_qualified_name(target_schema, target_table)} AS t
        LEFT ANY JOIN {_quote_identifier(key_table)} AS k
            ON {_join_predicate(unique_key_list)}
        """
    )


def _swap_shadow_table(
    target_connector: Any,
    target_schema: str,
    target_table: str,
    shadow_table: str,
    backup_table: str,
) -> None:
    target_connector.execute_query(
        f"""
        RENAME TABLE
            {_qualified_name(target_schema, target_table)} TO {_qualified_name(target_schema, backup_table)},
            {_qualified_name(target_schema, shadow_table)} TO {_qualified_name(target_schema, target_table)}
        """
    )


def _drop_table(target_connector: Any, table_name: str) -> None:
    target_connector.execute_query(f"DROP TABLE IF EXISTS {table_name}")


def _fetch_target_columns(target_connector: Any, target_schema: str, target_table: str) -> list[str]:
    if not hasattr(target_connector, "get_records"):
        raise ValueError("ClickHouse shadow-table reconciliation requires connector.get_records for schema discovery")

    rows = target_connector.get_records(
        f"""
        SELECT name, type
        FROM system.columns
        WHERE database = {_quote_literal(target_schema)}
          AND table = {_quote_literal(target_table)}
        ORDER BY position
        """
    )
    columns: list[str] = []
    for row in rows:
        if isinstance(row, dict):
            columns.append(str(row["name"]))
        else:
            columns.append(str(row[0]))
    if not columns:
        raise ValueError(f"ClickHouse target table has no discoverable columns: {target_schema}.{target_table}")
    return columns


def _validate_required_columns(
    columns: list[str],
    unique_key_list: list[str],
    meta_load_dtm: str,
    meta_delete_dtm: str,
) -> None:
    missing = [column for column in [*unique_key_list, meta_load_dtm, meta_delete_dtm] if column not in columns]
    if missing:
        raise ValueError(
            "ClickHouse reconciliation target is missing required columns: "
            + ", ".join(missing)
            + ". Ensure dpone technical columns are enabled."
        )


def _select_expression(column: str, *, meta_load_dtm: str, meta_delete_dtm: str) -> str:
    identifier = _quote_identifier(column)
    if column == meta_delete_dtm:
        return (
            f"if(k.{_quote_identifier('__dpone__deleted_marker')} = 1 "
            f"AND t.{identifier} IS NULL, now(), t.{identifier}) AS {identifier}"
        )
    if column == meta_load_dtm:
        return (
            f"if(k.{_quote_identifier('__dpone__deleted_marker')} = 1 "
            f"AND t.{_quote_identifier(meta_delete_dtm)} IS NULL, now(), t.{identifier}) AS {identifier}"
        )
    return f"t.{identifier} AS {identifier}"


def _join_predicate(unique_key_list: list[str]) -> str:
    return " AND ".join(
        f"toString(t.{_quote_identifier(column)}) = k.{_quote_identifier(column)}" for column in unique_key_list
    )


def _key_values_tuple(row: dict[str, Any], unique_key_list: list[str]) -> str:
    values = [_quote_literal(_key_value(row, column)) for column in unique_key_list]
    values.append("1")
    return "(" + ", ".join(values) + ")"


def _deduplicate_key_rows(deleted_keys: list[dict[str, Any]], unique_key_list: list[str]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    unique_rows = []
    for row in deleted_keys:
        key_tuple = tuple(_key_value(row, column) for column in unique_key_list)
        if key_tuple in seen:
            continue
        seen.add(key_tuple)
        unique_rows.append(row)
    return unique_rows


def _chunks(rows: list[dict[str, Any]], chunk_size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), chunk_size):
        yield rows[start : start + chunk_size]


def _operation_id(target_connector: Any) -> str:
    configured = getattr(target_connector, "reconciliation_clickhouse_operation_id", None)
    return str(configured or uuid.uuid4().hex)


def _qualified_name(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _quote_identifier(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _quote_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def _key_value(row: dict[str, Any], key_col: str) -> Any:
    try:
        return row[key_col]
    except KeyError as exc:
        raise ValueError(f"Deleted key row is missing unique key column: {key_col}") from exc


def _first_int(rows: list[Any]) -> int:
    if not rows:
        return 0
    first = rows[0]
    if isinstance(first, dict):
        return int(next(iter(first.values())))
    return int(first[0])
