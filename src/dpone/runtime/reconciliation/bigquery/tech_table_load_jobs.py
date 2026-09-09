"""BigQuery reconciliation snapshot load-job helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.reconciliation_logging import ETLLogger


from typing import Any, Protocol

from dpone.runtime.support.bigquery_load_config import postgres_headerless_csv_load_config


class BigQueryTechTableLoadOwner(Protocol):
    tech_connector: Any
    logger: ETLLogger
    tech_schema: str
    META_LOAD_DTM: str

    def _load_snapshot_batch_direct(
        self, tmp_file_path: str, table_id: str, job_config: Any, batch_rows: int
    ) -> int: ...


def load_file_to_bigquery(
    owner: BigQueryTechTableLoadOwner,
    tmp_file_path: str,
    table_id: str,
    job_config: Any,
    total_rows: int,
    rs_table: str,
    connector_class: str,
    source_schema: str,
    source_table: str,
) -> int:
    """Загружает файл в BigQuery через Load Job API.

    Args:
        tmp_file_path: Путь к временному файлу для загрузки
        table_id: Полный ID таблицы в BigQuery (project.dataset.table)
        job_config: Конфигурация Load Job
        total_rows: Общее количество строк (для fallback, если result.output_rows недоступен)
        rs_table: Имя snapshot таблицы (для логирования)
        connector_class: Класс source connector (для логирования)
        source_schema: Схема source таблицы (для логирования)
        source_table: Имя source таблицы (для логирования)

    Returns:
        Количество загруженных строк
    """
    with open(tmp_file_path, "rb") as source_file:
        job = owner.tech_connector.connection.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )
        result = job.result()

    inserted_rows = result.output_rows if result else total_rows

    owner.logger.log_etl_progress(
        "SNAPSHOT_INSERTED",
        {
            "SnapshotTable": f"{owner.tech_schema}.{rs_table} (BigQuery)",
            "InsertedRows": inserted_rows,
            "Source": f"{connector_class} ({source_schema}.{source_table})",
        },
    )

    return inserted_rows


def load_snapshot_batch_via_gcs(
    owner: BigQueryTechTableLoadOwner,
    tmp_file_path: str,
    table_id: str,
    job_config: Any,
    batch_rows: int,
    batch_num: int,
    gcs_config: Any = None,
) -> int:
    """
    Загружает батч snapshot в BigQuery через GCS route (если доступен) или Direct Load.

    Оптимально для batched reconciliation: BigQuery загружает из GCS быстрее
    даже для небольших файлов (~10-50MB).

    Args:
        tmp_file_path: Путь к CSV файлу батча
        table_id: Полный ID таблицы в BigQuery
        job_config: Конфигурация Load Job
        batch_rows: Количество строк в батче
        batch_num: Номер батча (для logging)
        gcs_config: Конфигурация GCS (dict с keys: gcs_bucket, upload_file_to_gcs, load_from_gcs)

    Returns:
        Количество загруженных строк
    """
    # Проверяем доступность GCS config
    if not gcs_config or not gcs_config.get("gcs_bucket"):
        owner.logger.log_etl_progress(
            "SNAPSHOT_BATCH_LOAD_DIRECT",
            {
                "Batch_Num": batch_num,
                "Reason": "GCS config not available (batch_commit_mode=whole or gcs_bucket not configured)",
            },
        )
        return owner._load_snapshot_batch_direct(tmp_file_path, table_id, job_config, batch_rows)

    upload_file_to_gcs = gcs_config.get("upload_file_to_gcs")
    load_from_gcs = gcs_config.get("load_from_gcs")

    if not upload_file_to_gcs or not load_from_gcs:
        # Fallback на Direct Load если GCS методы не доступны
        owner.logger.log_etl_progress(
            "SNAPSHOT_BATCH_LOAD_DIRECT",
            {
                "Batch_Num": batch_num,
                "Reason": "GCS methods not available in connector",
            },
        )
        return owner._load_snapshot_batch_direct(tmp_file_path, table_id, job_config, batch_rows)

    gcs_bucket = gcs_config["gcs_bucket"]
    attempt_scope = gcs_config.get("attempt_scope")
    if attempt_scope is None:
        owner.logger.log_etl_progress(
            "SNAPSHOT_BATCH_LOAD_DIRECT",
            {
                "Batch_Num": batch_num,
                "Reason": "GCS attempt scope not available",
            },
        )
        return owner._load_snapshot_batch_direct(tmp_file_path, table_id, job_config, batch_rows)

    from dpone.runtime.gcs_replacement import derive_reconciliation_batch_path

    gcs_path = derive_reconciliation_batch_path(table_id, batch_num, attempt_scope)

    try:
        # 1. Upload в GCS используя метод из gcs_config
        owner.logger.log_etl_progress(
            "SNAPSHOT_BATCH_UPLOADING_TO_GCS",
            {
                "Batch_Num": batch_num,
                "GCS_Path": f"gs://{gcs_bucket}/{gcs_path}",
            },
        )

        gcs_uri = upload_file_to_gcs(
            local_file_path=tmp_file_path,
            gcs_bucket=gcs_bucket,
            gcs_path=gcs_path,
            delete_local_after_upload=False,
        )

        owner.logger.log_etl_progress(
            "SNAPSHOT_BATCH_UPLOADED_TO_GCS",
            {
                "Batch_Num": batch_num,
                "GCS_URI": gcs_uri,
            },
        )

        # 2. Load из GCS в BigQuery используя метод из gcs_config
        rows_loaded = load_from_gcs(
            table_id=table_id,
            gcs_uri=gcs_uri,
            source_format="CSV",
            write_disposition="WRITE_APPEND",
            schema=job_config.schema if hasattr(job_config, "schema") else None,
            autodetect_schema=False,
            csv_config=postgres_headerless_csv_load_config(),
        )

        inserted_rows = rows_loaded if rows_loaded is not None else batch_rows

        owner.logger.log_etl_progress(
            "SNAPSHOT_BATCH_LOADED_FROM_GCS",
            {
                "Batch_Num": batch_num,
                "Rows": inserted_rows,
                "Method": "GCS route (optimized)",
            },
        )

        # 3. Cleanup attempt-owned GCS object after successful load
        try:
            storage_client = gcs_config.get("storage_client")
            if storage_client is not None and gcs_uri.startswith("gs://"):
                path = gcs_uri[5:]
                blob_bucket, blob_path = path.split("/", 1)
                blob = storage_client.bucket(blob_bucket).blob(blob_path)
                if blob.exists():
                    blob.delete()
                    owner.logger.log_etl_progress(
                        "SNAPSHOT_BATCH_GCS_CLEANED",
                        {"Batch_Num": batch_num, "GCS_URI": gcs_uri},
                    )
        except Exception as cleanup_err:
            owner.logger.warning(f"Failed to cleanup GCS file {gcs_uri}: {cleanup_err}")

        return inserted_rows

    except Exception as e:
        # Fallback на Direct Load при ошибке GCS
        owner.logger.warning(f"⚠️  GCS route failed for batch {batch_num}: {e}. Falling back to Direct Load.")
        return owner._load_snapshot_batch_direct(tmp_file_path, table_id, job_config, batch_rows)


def load_snapshot_batch_direct(
    owner: BigQueryTechTableLoadOwner,
    tmp_file_path: str,
    table_id: str,
    job_config: Any,
    batch_rows: int,
) -> int:
    """Загружает батч snapshot через Direct Load (fallback)."""
    with open(tmp_file_path, "rb") as source_file:
        job = owner.tech_connector.connection.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )
        result = job.result()

    return result.output_rows if result else batch_rows


__all__ = ["load_file_to_bigquery", "load_snapshot_batch_direct", "load_snapshot_batch_via_gcs"]
