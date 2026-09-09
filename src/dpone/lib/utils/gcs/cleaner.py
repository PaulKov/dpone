"""GCS cleaner utilities."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class GCSCleaner:
    """
    Утилита для очистки данных в GCS.

    Предоставляет как низкоуровневый API (delete_prefix),
    так и высокоуровневый API (cleanup_from_load_config) для ETL стратегий.
    """

    @staticmethod
    def delete_prefix(
        storage_client: Any,
        bucket_name: str,
        prefix: str,
        *,
        batch_size: int = 1000,
        logger_instance: logging.Logger | None = None,
    ) -> int:
        """
        Удаляет все файлы с заданным префиксом из GCS bucket.

        Низкоуровневый API для прямого удаления файлов.

        Args:
            storage_client: Google Cloud Storage client
            bucket_name: Имя GCS bucket
            prefix: Префикс для удаления (например, "path/to/partition/")
            batch_size: Размер батча для удаления (по умолчанию 1000)
            logger_instance: Logger для логирования (опционально)

        Returns:
            Количество удалённых файлов

        Example:
            >>> from google.cloud import storage
            >>> from dpone.utils.gcs import GCSCleaner
            >>>
            >>> client = storage.Client()
            >>> deleted = GCSCleaner.delete_prefix(
            ...     client,
            ...     "my-bucket",
            ...     "data/partition_date=2025-01-15/"
            ... )
            >>> print(f"Deleted {deleted} files")
        """
        if not storage_client:
            return 0

        try:
            bucket = storage_client.bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=prefix))

            if not blobs:
                return 0

            deleted_count = 0
            to_delete = []

            for blob in blobs:
                to_delete.append(blob)
                if len(to_delete) >= batch_size:
                    bucket.delete_blobs(
                        to_delete,
                        on_error=lambda e: (
                            logger_instance.warning("GCS delete error: %s", e) if logger_instance else None
                        ),
                    )
                    deleted_count += len(to_delete)
                    to_delete.clear()

            # Удаляем остаток
            if to_delete:
                bucket.delete_blobs(
                    to_delete,
                    on_error=lambda e: logger_instance.warning("GCS delete error: %s", e) if logger_instance else None,
                )
                deleted_count += len(to_delete)

            if logger_instance:
                logger_instance.info(
                    "GCS cleanup: deleted %d files from gs://%s/%s", deleted_count, bucket_name, prefix
                )

            return deleted_count

        except Exception as exc:
            if logger_instance:
                logger_instance.warning("Failed to delete GCS prefix gs://%s/%s: %s", bucket_name, prefix, exc)
            return 0

    @classmethod
    def cleanup_from_load_config(
        cls,
        load_config,
        logger=None,
        batch_size: int = 1000,
    ) -> int:
        """
        Выполняет cleanup GCS на основе load_config.options.

        Читает параметры из load_config.options:
        - cleanup_prefix: префикс для удаления
        - cleanup_prefix_dry_run: режим dry run (по умолчанию True)
        """
        from importlib.util import find_spec

        if find_spec("google.cloud.storage") is None:
            raise ImportError(
                "google-cloud-storage is required for GCS operations. Install it with: pip install google-cloud-storage"
            )

        from dpone.config import get_env_code
        from dpone.lib.utils.gcs.client import create_gcs_client
        from dpone.lib.utils.gcs.path_manager import get_gcs_bucket_name

        # Читаем параметры из load_config
        options = getattr(load_config, "options", {}) or {}
        cleanup_prefix = options.get("cleanup_prefix")

        if not cleanup_prefix:
            return 0

        dry_run = options.get("cleanup_prefix_dry_run", True)
        env_code = get_env_code()
        bucket_name = get_gcs_bucket_name(load_config.target_schema, env_code)
        target_conn_id = load_config.target_conn_id or "bigquery_default"

        prefix = cleanup_prefix
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        if logger:
            logger.log_etl_progress(
                "GCS_OLD_STRUCTURE_CLEANUP_START",
                {
                    "Bucket": bucket_name,
                    "Prefix": prefix,
                    "DryRun": dry_run,
                },
            )

        try:
            client, _ = create_gcs_client(
                env_code=env_code,
                target_conn_id=target_conn_id,
            )
            bucket = client.bucket(bucket_name)

            # Проверяем существование бакета
            if not bucket.exists():
                raise ValueError(f"Bucket '{bucket_name}' does not exist")

            # Получаем все объекты с префиксом
            blobs = list(client.list_blobs(bucket_name, prefix=prefix))
            total_count = len(blobs)

            if total_count == 0:
                if logger:
                    logger.log_etl_progress(
                        "GCS_CLEANUP_NO_FILES",
                        {
                            "Bucket": bucket_name,
                            "Prefix": prefix,
                        },
                    )
                return 0

            # Формируем список файлов с размерами для вывода
            total_size = 0
            file_list = []
            for blob in blobs:
                file_size = blob.size if blob.size else 0
                total_size += file_size
                file_size_mb = file_size / (1024 * 1024)
                file_list.append(f"{blob.name} ({file_size_mb:.2f} MB)")

            total_size_mb = total_size / (1024 * 1024)
            total_size_gb = total_size / (1024 * 1024 * 1024)

            if total_size_gb >= 1:
                size_str = f"{total_size_gb:.2f} GB"
            else:
                size_str = f"{total_size_mb:.2f} MB"

            if dry_run:
                if logger:
                    log_data = {
                        "FilesFound": total_count,
                        "TotalSize": size_str,
                        "Bucket": bucket_name,
                        "Prefix": prefix,
                        "📋 Files to delete": "",
                    }
                    # Добавляем каждый файл как отдельное поле
                    for i, file_info in enumerate(file_list, 1):
                        log_data[f"   {i}"] = file_info

                    logger.log_etl_progress("GCS_CLEANUP_DRY_RUN", log_data)
                return total_count

            deleted_count = 0
            to_delete = []

            for blob in blobs:
                to_delete.append(blob)
                if len(to_delete) >= batch_size:
                    bucket.delete_blobs(
                        to_delete,
                        on_error=lambda e: (
                            logger.log_etl_progress("GCS_DELETE_WARNING", {"Error": str(e)}) if logger else None
                        ),
                    )
                    deleted_count += len(to_delete)
                    to_delete.clear()

            if to_delete:
                bucket.delete_blobs(
                    to_delete,
                    on_error=lambda e: (
                        logger.log_etl_progress("GCS_DELETE_WARNING", {"Error": str(e)}) if logger else None
                    ),
                )
                deleted_count += len(to_delete)

            if logger:
                log_data = {
                    "FilesDeleted": deleted_count,
                    "TotalSize": size_str,
                    "Bucket": bucket_name,
                    "Prefix": prefix,
                    "📋 Deleted files": "",
                }
                for i, file_info in enumerate(file_list, 1):
                    log_data[f"   {i}"] = file_info

                logger.log_etl_progress("GCS_OLD_STRUCTURE_CLEANUP_COMPLETE", log_data)

            return deleted_count

        except Exception as e:
            if logger:
                logger.log_etl_progress(
                    "GCS_CLEANUP_ERROR",
                    {
                        "Error": str(e),
                        "Bucket": bucket_name,
                        "Prefix": cleanup_prefix,
                    },
                )
            return 0
