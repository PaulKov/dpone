"""Google Cloud Storage load helpers for BigQuery connector."""

from __future__ import annotations

import logging
import os
import time
from importlib import import_module
from typing import Any

from dpone.runtime.connectors.bigquery_runtime import _require_bigquery, format_gcs_uri_for_display
from dpone.runtime.support.bigquery_load_config import CSVLoadConfig

logger = logging.getLogger(__name__)


class BigQueryGcsMixin:
    def upload_file_to_gcs(
        self,
        local_file_path: str,
        gcs_bucket: str,
        gcs_path: str,
        *,
        delete_local_after_upload: bool = False,
    ) -> str:
        """
        Загружает локальный файл в Google Cloud Storage.

        Используется для оптимизации загрузки больших CSV файлов из PostgreSQL:
        PG → local CSV → GCS → BQ (вместо медленного streaming API).
        """
        storage = import_module("google.cloud.storage")

        # Создаем GCS client используя те же credentials что и BigQuery
        scoped_credentials = self._get_scoped_credentials()
        gcs_client = storage.Client(
            project=self.project_id,
            credentials=scoped_credentials,
        )

        # Получаем bucket и blob
        bucket = gcs_client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_path)

        # Загружаем файл
        start_time = time.time()
        file_size = os.path.getsize(local_file_path)

        logger.info(
            "Uploading file to GCS: %s → gs://%s/%s (size: %.1f MB)",
            local_file_path,
            gcs_bucket,
            gcs_path,
            file_size / 1024 / 1024,
        )

        blob.upload_from_filename(local_file_path)

        elapsed = time.time() - start_time
        throughput = (file_size / 1024 / 1024) / elapsed if elapsed > 0 else 0

        logger.info(
            "GCS upload complete: %.1f MB in %.1fs (throughput: %.1f MB/s)",
            file_size / 1024 / 1024,
            elapsed,
            throughput,
        )

        # Удаляем локальный файл если указано
        if delete_local_after_upload:
            try:
                os.remove(local_file_path)
                logger.debug("Deleted local file: %s", local_file_path)
            except OSError as e:
                logger.warning("Failed to delete local file %s: %s", local_file_path, e)

        return f"gs://{gcs_bucket}/{gcs_path}"

    def load_from_gcs(
        self,
        table_id: str,
        gcs_uri: str | list[str],
        *,
        source_format: str | Any = "PARQUET",
        write_disposition: str | Any = "WRITE_APPEND",
        schema: list[Any] | None = None,
        hive_partitioning: bool = False,
        source_uri_prefix: str | None = None,
        autodetect_schema: bool = True,
        csv_config: CSVLoadConfig | None = None,
    ) -> int:
        """
        Загружает данные из Google Cloud Storage в таблицу BigQuery.

        Нативная загрузка через BigQuery Load API — быстро, масштабируемо, без Python буферизации.

        Args:
            table_id: Полный ID таблицы (project.dataset.table или dataset.table)
            gcs_uri: GCS URI или список URIs (gs://bucket/path/file.parquet)
            source_format: Формат файлов (enum bigquery.SourceFormat)
            write_disposition: Режим записи (enum bigquery.WriteDisposition)
            schema: Опциональная схема таблицы (если указана, autodetect_schema игнорируется)
            hive_partitioning: Использовать Hive partitioning
            source_uri_prefix: Prefix для Hive partitioning (требуется для hive_partitioning=True)
            autodetect_schema: Автоматически определять схему таблицы из файлов (работает только если schema=None)
            csv_config: Конфигурация для CSV файлов (применяется только если source_format=CSV)

        Returns:
            Количество загруженных строк
        """
        self._ensure_client_initialized()
        bigquery = _require_bigquery()
        if isinstance(source_format, str):
            source_format = getattr(bigquery.SourceFormat, source_format.upper(), source_format)

        if isinstance(write_disposition, str):
            # Обрабатываем как прямое имя атрибута, так и полное значение
            disposition_str = write_disposition.upper().replace("WRITE_", "")
            if hasattr(bigquery.WriteDisposition, f"WRITE_{disposition_str}"):
                write_disposition = getattr(bigquery.WriteDisposition, f"WRITE_{disposition_str}")
            elif hasattr(bigquery.WriteDisposition, write_disposition.upper()):
                write_disposition = getattr(bigquery.WriteDisposition, write_disposition.upper())

        job_config = bigquery.LoadJobConfig()

        # Настраиваем формат и режим записи
        job_config.source_format = source_format
        job_config.write_disposition = write_disposition

        # Схема: если передана явно — используем её, иначе autodetect
        if schema:
            job_config.schema = schema
            job_config.autodetect = False
        else:
            job_config.autodetect = autodetect_schema

        # Hive partitioning для BigQuery
        if hive_partitioning and source_uri_prefix:
            job_config.hive_partitioning = bigquery.HivePartitioningOptions()
            job_config.hive_partitioning.mode = "AUTO"
            job_config.hive_partitioning.source_uri_prefix = source_uri_prefix

        # CSV специфичные настройки (применяются только для CSV формата)
        if source_format == bigquery.SourceFormat.CSV:
            # Используем переданную конфигурацию или дефолтную
            config = csv_config or CSVLoadConfig()

            job_config.skip_leading_rows = config.skip_leading_rows
            job_config.allow_quoted_newlines = config.allow_quoted_newlines
            job_config.allow_jagged_rows = config.allow_jagged_rows
            job_config.field_delimiter = config.field_delimiter
            job_config.encoding = config.encoding

            logger.debug(
                "CSV config applied: skip_rows=%d, quoted_newlines=%s, jagged_rows=%s, delimiter=%s",
                config.skip_leading_rows,
                config.allow_quoted_newlines,
                config.allow_jagged_rows,
                config.field_delimiter,
            )

        # Форматируем URI для логирования (разделение ответственности)
        display_uri = format_gcs_uri_for_display(gcs_uri)

        logger.info(
            "Loading data from GCS into BigQuery: %s → %s (format=%s, disposition=%s)",
            display_uri,
            table_id,
            source_format,
            write_disposition,
        )

        # Запускаем Load Job (BigQuery API принимает как строку, так и список)
        load_job = self.connection.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config,
        )

        # Ждём завершения
        load_job.result()

        # Получаем статистику
        rows_loaded = getattr(load_job, "output_rows", 0) or 0

        logger.info(
            "BigQuery Load Job completed: %d rows loaded from %s into %s",
            rows_loaded,
            display_uri,
            table_id,
        )

        return rows_loaded


__all__ = ["BigQueryGcsMixin"]
