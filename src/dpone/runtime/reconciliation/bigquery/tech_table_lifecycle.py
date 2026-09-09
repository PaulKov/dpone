from __future__ import annotations

from typing import Any

from dpone.runtime.connectors.bigquery_runtime import _require_bigquery


def ensure_tech_schema(store: Any) -> None:
    bigquery = _require_bigquery()
    client = store.tech_connector.connection
    project_id = store.tech_connector.project_id

    dataset_id = f"{project_id}.{store.tech_schema}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    try:
        client.create_dataset(dataset, exists_ok=True)
        store.logger.log_etl_progress("TECH_DATASET_ENSURED", {"Dataset": dataset_id})
    except Exception as exc:
        store.logger.warning(f"Ошибка создания dataset {dataset_id}: {exc}")


def ensure_snapshot_table(store: Any, target_table: str, unique_key: str | list[str]) -> bool:
    rs_table = f"{target_table}__rs"
    unique_key_list = store._normalize_unique_key(unique_key)
    table_id = f"{store.tech_connector.project_id}.{store.tech_schema}.{rs_table}"
    client = store.tech_connector.connection
    bigquery = _require_bigquery()

    try:
        existing_table = client.get_table(table_id)
        existing_columns = {field.name for field in existing_table.schema if field.name != store.META_LOAD_DTM}
        expected_columns = set(unique_key_list)
        if existing_columns == expected_columns:
            return False
        store.logger.warning(
            f"⚠️  Схема snapshot таблицы не совпадает с текущим unique_key. "
            f"Существующие колонки: {sorted(existing_columns)}, "
            f"Ожидаемые колонки: {sorted(expected_columns)}. "
            f"Пересоздаем таблицу..."
        )
        client.delete_table(table_id, not_found_ok=True)
        store.logger.log_etl_progress(
            "SNAPSHOT_TABLE_RECREATED",
            {
                "Table": table_id,
                "OldUniqueKeyColumns": sorted(existing_columns),
                "NewUniqueKeyColumns": unique_key_list,
                "Reason": "unique_key changed",
            },
        )
    except Exception as exc:
        if not _is_bigquery_not_found(exc):
            raise

    schema = [bigquery.SchemaField(key_col, "STRING", mode="REQUIRED") for key_col in unique_key_list]
    schema.append(bigquery.SchemaField(store.META_LOAD_DTM, "TIMESTAMP", mode="REQUIRED"))
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=store.META_LOAD_DTM,
    )
    client.create_table(table)
    store.logger.log_etl_progress("SNAPSHOT_TABLE_CREATED", {"Table": table_id, "UniqueKeyColumns": unique_key_list})
    return True


def ensure_deleted_log_table(store: Any, target_table: str, unique_key: str | list[str]) -> bool:
    deleted_log_table = f"{target_table}__deleted_log"
    unique_key_list = store._normalize_unique_key(unique_key)
    table_id = f"{store.tech_connector.project_id}.{store.tech_schema}.{deleted_log_table}"
    client = store.tech_connector.connection
    bigquery = _require_bigquery()

    try:
        existing_table = client.get_table(table_id)
        existing_columns = {
            field.name
            for field in existing_table.schema
            if field.name not in (store.META_LOAD_DTM, store.META_DELETE_DTM)
        }
        expected_columns = set(unique_key_list)
        if existing_columns == expected_columns:
            return False
        store.logger.warning(
            f"⚠️  Схема deleted_log таблицы не совпадает с текущим unique_key. "
            f"Существующие колонки: {sorted(existing_columns)}, "
            f"Ожидаемые колонки: {sorted(expected_columns)}. "
            f"Пересоздаем таблицу..."
        )
        client.delete_table(table_id, not_found_ok=True)
        store.logger.log_etl_progress(
            "DELETED_LOG_TABLE_RECREATED",
            {
                "Table": table_id,
                "OldUniqueKeyColumns": sorted(existing_columns),
                "NewUniqueKeyColumns": unique_key_list,
                "Reason": "unique_key changed",
            },
        )
    except Exception as exc:
        if not _is_bigquery_not_found(exc):
            raise

    schema = [bigquery.SchemaField(key_col, "STRING", mode="REQUIRED") for key_col in unique_key_list]
    schema.append(bigquery.SchemaField(store.META_LOAD_DTM, "TIMESTAMP", mode="REQUIRED"))
    schema.append(bigquery.SchemaField(store.META_DELETE_DTM, "TIMESTAMP", mode="REQUIRED"))
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=store.META_DELETE_DTM,
    )
    client.create_table(table)
    store.logger.log_etl_progress(
        "DELETED_LOG_TABLE_CREATED",
        {"Table": table_id, "UniqueKeyColumns": unique_key_list},
    )
    return True


def _is_bigquery_not_found(exc: BaseException) -> bool:
    exc_type = type(exc)
    return exc_type.__name__ == "NotFound" and exc_type.__module__.startswith("google.")
