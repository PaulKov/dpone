"""BigQuery reconciliation snapshot writer helpers."""

from __future__ import annotations

import csv
import gzip
import os
import tempfile
from datetime import datetime
from typing import Any, Protocol

from dpone.runtime.connectors.bigquery_runtime import _require_bigquery
from dpone.runtime.reconciliation_logging import ETLLogger


class BigQueryTechTableSnapshotOwner(Protocol):
    META_LOAD_DTM: str
    tech_connector: Any
    logger: ETLLogger
    tech_schema: str

    def _ensure_clean_temp_file(self, file_path: str) -> None: ...

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
    ) -> int: ...

    def _load_snapshot_batch_via_gcs(
        self,
        tmp_file_path: str,
        table_id: str,
        job_config: Any,
        batch_rows: int,
        batch_num: int,
        gcs_config: Any = None,
    ) -> int: ...


def insert_snapshot_whole(
    owner: BigQueryTechTableSnapshotOwner,
    rs_table: str,
    unique_key_list: list[str],
    source_connector: Any,
    source_schema: str,
    source_table: str,
    batch_size: int = 50000,
) -> int:
    """Снимает снэпшот в один файл (текущая реализация)."""
    connector_class = source_connector.__class__.__name__

    # Используем batch_size из YAML
    batch_size = batch_size or int(os.environ.get("DPONE_SNAPSHOT_BATCH_SIZE", "50000"))

    owner.logger.log_etl_progress(
        "SNAPSHOT_FROM_SOURCE_START",
        {
            "SourceConnector": connector_class,
            "SourceTable": f"{source_schema}.{source_table}",
            "UniqueKeys": unique_key_list,
            "BatchSize": batch_size,
            "Method": "Temp File + Load Job API → BigQuery",
            "Mode": "whole",
        },
    )

    source_query = source_connector.build_select_query(
        schema=source_schema,
        table=source_table,
        columns=unique_key_list,
    )

    current_ts = datetime.utcnow()
    current_ts_str = current_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    # Создаем простой путь к файлу: /tmp/dpone_snapshot.csv.gz
    tmp_dir = os.environ.get("DPONE_EXPORT_TMP_DIR", tempfile.gettempdir())
    tmp_file_path = os.path.join(tmp_dir, "dpone_snapshot.csv.gz")

    # Cleanup перед записью
    owner._ensure_clean_temp_file(tmp_file_path)

    total_rows = 0

    try:
        # STREAMING: Source DB → CSV file
        owner.logger.log_etl_progress(
            "SNAPSHOT_WRITING_TO_FILE",
            {
                "TempFile": tmp_file_path,
                "Compressed": True,
            },
        )

        with gzip.open(tmp_file_path, "wt", encoding="utf-8", newline="") as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

            # Streaming read from source
            for batch in source_connector.get_records_streaming(source_query, batch_size=batch_size, as_dict=False):
                if not batch:
                    continue

                # Write batch to CSV (с timestamp)
                for record in batch:
                    # Convert all values to strings
                    row = []
                    for value in record:
                        if value is None:
                            row.append("")  # CSV NULL
                        else:
                            row.append(str(value))
                    row.append(current_ts_str)  # __dpone__loaded_at
                    writer.writerow(row)
                    total_rows += 1

                if total_rows % 100000 == 0:
                    owner.logger.log_etl_progress(
                        "SNAPSHOT_FILE_PROGRESS",
                        {"RowsWritten": total_rows},
                    )

        if total_rows == 0:
            owner.logger.log_etl_progress(
                "SNAPSHOT_FROM_SOURCE_EMPTY",
                {"SourceTable": f"{source_schema}.{source_table}"},
            )
            os.remove(tmp_file_path)
            return 0

        file_size_mb = os.path.getsize(tmp_file_path) / 1024 / 1024
        owner.logger.log_etl_progress(
            "SNAPSHOT_FILE_WRITTEN",
            {
                "TotalRows": total_rows,
                "FileSizeMB": round(file_size_mb, 2),
            },
        )

        # FAST LOAD: CSV file → BigQuery через Load Job API
        project_id = owner.tech_connector.project_id
        table_id = f"{project_id}.{owner.tech_schema}.{rs_table}"
        bigquery = _require_bigquery()

        # Build schema для Load Job
        schema_fields = []
        for key_col in unique_key_list:
            schema_fields.append(bigquery.SchemaField(key_col, "STRING", mode="REQUIRED"))
        schema_fields.append(bigquery.SchemaField(owner.META_LOAD_DTM, "TIMESTAMP", mode="REQUIRED"))

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema_fields,
            skip_leading_rows=0,  # No header
            allow_quoted_newlines=True,
            encoding="UTF-8",
            max_bad_records=100,
        )

        owner.logger.log_etl_progress(
            "SNAPSHOT_LOADING_TO_BIGQUERY",
            {
                "Table": table_id,
                "Method": "Load Job API",
            },
        )

        # Load from compressed file
        inserted_rows = owner._load_file_to_bigquery(
            tmp_file_path=tmp_file_path,
            table_id=table_id,
            job_config=job_config,
            total_rows=total_rows,
            rs_table=rs_table,
            connector_class=connector_class,
            source_schema=source_schema,
            source_table=source_table,
        )

        return inserted_rows

    except Exception as e:
        error_msg = str(e)
        # Если ошибка из-за несовпадения схемы - пересоздаем таблицу и повторяем загрузку
        if "Provided Schema does not match Table" in error_msg or "Cannot add fields" in error_msg:
            owner.logger.warning("⚠️  Схема snapshot таблицы не совпадает. Пересоздаем таблицу и повторяем загрузку...")
            # Пересоздаем таблицу
            client = owner.tech_connector.connection
            client.delete_table(table_id, not_found_ok=True)

            # Создаем таблицу с правильной схемой
            table = bigquery.Table(table_id, schema=schema_fields)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=owner.META_LOAD_DTM,
            )
            client.create_table(table)

            owner.logger.log_etl_progress(
                "SNAPSHOT_TABLE_RECREATED_DURING_LOAD",
                {
                    "Table": table_id,
                    "UniqueKeyColumns": unique_key_list,
                    "Reason": "Schema mismatch during load",
                },
            )

            # Повторяем загрузку
            inserted_rows = owner._load_file_to_bigquery(
                tmp_file_path=tmp_file_path,
                table_id=table_id,
                job_config=job_config,
                total_rows=total_rows,
                rs_table=rs_table,
                connector_class=connector_class,
                source_schema=source_schema,
                source_table=source_table,
            )

            return inserted_rows
        else:
            # Другая ошибка - логируем и возвращаем 0
            owner.logger.warning(f"⚠️  Ошибка snapshot через temp file: {e}. Обработано строк: {total_rows}")
            return 0

    finally:
        # Cleanup temporary file
        try:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
        except Exception as cleanup_err:
            owner.logger.warning(f"Failed to cleanup temp file {tmp_file_path}: {cleanup_err}")


def insert_snapshot_batched(
    owner: BigQueryTechTableSnapshotOwner,
    rs_table: str,
    unique_key_list: list[str],
    source_connector: Any,
    source_schema: str,
    source_table: str,
    batch_size: int = 50000,
    gcs_config: Any = None,
) -> int:
    """Снимает снэпшот батчами через LIMIT/OFFSET.

    Args:
        gcs_config: Конфигурация GCS (dict с keys: gcs_bucket, upload_file_to_gcs, load_from_gcs)
                   Если None или не все методы доступны, используется Direct Load
    """
    connector_class = source_connector.__class__.__name__

    owner.logger.log_etl_progress(
        "SNAPSHOT_FROM_SOURCE_START",
        {
            "SourceConnector": connector_class,
            "SourceTable": f"{source_schema}.{source_table}",
            "UniqueKeys": unique_key_list,
            "BatchSize": batch_size,
            "Method": "Batched Temp Files + Load Job API → BigQuery",
            "Mode": "separate",
            "GCS_Available": bool(gcs_config and gcs_config.get("gcs_bucket")),
        },
    )

    current_ts = datetime.utcnow()
    current_ts_str = current_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    bigquery = _require_bigquery()

    schema_fields = []
    for key_col in unique_key_list:
        schema_fields.append(bigquery.SchemaField(key_col, "STRING", mode="REQUIRED"))
    schema_fields.append(bigquery.SchemaField(owner.META_LOAD_DTM, "TIMESTAMP", mode="REQUIRED"))

    project_id = owner.tech_connector.project_id
    table_id = f"{project_id}.{owner.tech_schema}.{rs_table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema_fields,
        skip_leading_rows=0,
        allow_quoted_newlines=True,
        encoding="UTF-8",
        max_bad_records=100,
    )

    tmp_dir = os.environ.get("DPONE_EXPORT_TMP_DIR", tempfile.gettempdir())
    total_inserted = 0
    offset = 0
    batch_num = 1

    while True:
        owner.logger.log_etl_progress(
            "SNAPSHOT_BATCH_START",
            {
                "Batch_Num": batch_num,
                "Offset": offset,
                "Limit": batch_size,
            },
        )

        batched_query = source_connector.build_select_query(
            schema=source_schema,
            table=source_table,
            columns=unique_key_list,
            limit=batch_size,
            offset=offset,
        )

        # Создаем простой путь к файлу: /tmp/dpone_snapshot_batch_1.csv.gz
        tmp_file_path = os.path.join(tmp_dir, f"dpone_snapshot_batch_{batch_num}.csv.gz")

        owner._ensure_clean_temp_file(tmp_file_path)

        batch_rows = 0

        try:
            # Пишем батч в файл
            with gzip.open(tmp_file_path, "wt", encoding="utf-8", newline="") as csvfile:
                writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

                # Читаем данные из source
                for record_batch in source_connector.get_records_streaming(
                    batched_query, batch_size=batch_size, as_dict=False
                ):
                    if not record_batch:
                        continue

                    for record in record_batch:
                        row = []
                        for value in record:
                            if value is None:
                                row.append("")
                            else:
                                row.append(str(value))
                        row.append(current_ts_str)  # __dpone__loaded_at
                        writer.writerow(row)
                        batch_rows += 1

            if batch_rows == 0 or os.path.getsize(tmp_file_path) < 50:
                owner.logger.log_etl_progress(
                    "SNAPSHOT_BATCHED_COMPLETE",
                    {
                        "Total_Batches": batch_num,
                        "Total_Rows": total_inserted,
                    },
                )
                os.remove(tmp_file_path)
                break

            owner.logger.log_etl_progress(
                "SNAPSHOT_BATCH_FILE_WRITTEN",
                {
                    "Batch_Num": batch_num,
                    "Rows": batch_rows,
                    "File": tmp_file_path,
                    "FileSizeMB": round(os.path.getsize(tmp_file_path) / 1024 / 1024, 2),
                },
            )

            # Загружаем батч в BigQuery
            # Если gcs_config доступен - используем GCS route (оптимально для любой БД)
            # Иначе - Direct Load
            inserted_rows = owner._load_snapshot_batch_via_gcs(
                tmp_file_path=tmp_file_path,
                table_id=table_id,
                job_config=job_config,
                batch_rows=batch_rows,
                batch_num=batch_num,
                gcs_config=gcs_config,
            )
            total_inserted += inserted_rows

            owner.logger.log_etl_progress(
                "SNAPSHOT_BATCH_LOADED",
                {
                    "Batch_Num": batch_num,
                    "Rows": inserted_rows,
                    "Total_Rows": total_inserted,
                },
            )

        finally:
            # Cleanup temporary file после загрузки
            try:
                if os.path.exists(tmp_file_path):
                    os.remove(tmp_file_path)
                    owner.logger.log_etl_progress(
                        "SNAPSHOT_BATCH_FILE_CLEANED",
                        {"Batch_Num": batch_num, "File": tmp_file_path},
                    )
            except Exception as cleanup_err:
                owner.logger.warning(f"Failed to cleanup temp file {tmp_file_path}: {cleanup_err}")

        offset += batch_size
        batch_num += 1

    owner.logger.log_etl_progress(
        "SNAPSHOT_INSERTED",
        {
            "SnapshotTable": f"{owner.tech_schema}.{rs_table} (BigQuery)",
            "InsertedRows": total_inserted,
            "Source": f"{connector_class} ({source_schema}.{source_table})",
            "Mode": "separate (batched)",
        },
    )

    return total_inserted


__all__ = ["insert_snapshot_batched", "insert_snapshot_whole"]
