"""File-based loading helpers for BigQuery staging tables."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.connectors import BigQueryConnector
    from dpone.runtime.file_artifacts import FileExportArtifact
    from dpone.runtime.sink_logging import ETLLogger


class BigQueryStagingFileLoader:
    def __init__(self, connector: BigQueryConnector, logger: ETLLogger):
        self.connector = connector
        self.logger = logger

    def load_from_file(self, artifact: StagingTableArtifact, file_artifact: FileExportArtifact) -> int:
        return self._load_from_file_internal(artifact, file_artifact, batch_mode=False)

    def load_from_file_batched(self, artifact: StagingTableArtifact, file_artifact: FileExportArtifact) -> int:
        return self._load_from_file_internal(artifact, file_artifact, batch_mode=True)

    def _load_from_file_internal(
        self, artifact: StagingTableArtifact, file_artifact: FileExportArtifact, batch_mode: bool = False
    ) -> int:
        table_id = f"{self.connector.project_id}.{artifact.schema}.{artifact.table}"
        try:
            if file_artifact.format == "csv":
                should_use_gcs = self._should_use_gcs_route(file_artifact, batch_mode)
                if should_use_gcs:
                    target_schema = artifact.target_schema or artifact.schema
                    return self._load_csv_via_gcs(table_id, file_artifact, artifact, target_schema)
                return self._load_csv_file(table_id, file_artifact, artifact)
            if file_artifact.format == "binary":
                self.logger.log_etl_error(
                    "BigQuery НЕ поддерживает PostgreSQL Binary формат. Измените конфигурацию: options.export_format = 'csv'",
                    {
                        "table": artifact.qualified_name(),
                        "file": file_artifact.file_path,
                        "format": file_artifact.format,
                    },
                )
                raise ValueError("BigQuery sink requires CSV format. Set 'export_format: csv' in source options.")
            self.logger.log_etl_error(
                f"Неподдерживаемый формат файла: {file_artifact.format}",
                {"table": artifact.qualified_name()},
            )
            raise ValueError(
                f"BigQuery sink requires CSV format, got '{file_artifact.format}'. "
                "Set 'export_format: csv' in source options."
            )
        except Exception as exc:
            self.logger.log_etl_error(
                f"Ошибка загрузки staging из файла: {str(exc)}",
                {"table": artifact.qualified_name()},
            )
            raise

    def _should_use_gcs_route(self, file_artifact: FileExportArtifact, batch_mode: bool = False) -> bool:
        if batch_mode:
            self.logger.log_etl_progress(
                "BQ_GCS_ROUTE_DECISION",
                {"Decision": "Use GCS", "Reason": "batch_commit_mode=True (optimal for BigQuery)", "Mode": "separate"},
            )
            return True
        if not os.path.exists(file_artifact.file_path):
            self.logger.log_etl_progress(
                "BQ_GCS_ROUTE_DECISION",
                {"Decision": "Direct Load (default)", "Reason": "File not found"},
            )
            return False
        file_size_threshold_mb = int(os.environ.get("DPONE_GCS_FILE_SIZE_THRESHOLD_MB", "1000"))
        file_size_mb = os.path.getsize(file_artifact.file_path) / 1024 / 1024
        use_gcs = file_size_mb >= file_size_threshold_mb
        self.logger.log_etl_progress(
            "BQ_GCS_ROUTE_DECISION",
            {
                "Decision": "Use GCS" if use_gcs else "Direct Load",
                "Reason": "file_size check",
                "FileSizeMB": round(file_size_mb, 1),
                "FileSizeThresholdMB": file_size_threshold_mb,
                "Mode": "whole",
            },
        )
        return use_gcs

    def _load_csv_file(self, table_id: str, file_artifact: FileExportArtifact, artifact: StagingTableArtifact) -> int:
        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 0
        job_config.allow_quoted_newlines = True
        job_config.allow_jagged_rows = True
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.field_delimiter = ","
        job_config.encoding = "UTF-8"
        schema = getattr(artifact.staging_manager, "_last_schema", None)
        if schema:
            job_config.schema = schema
            job_config.autodetect = False
        else:
            job_config.autodetect = True
        max_bad_records = int(os.environ.get("DPONE_BQ_MAX_BAD_RECORDS", "100"))
        job_config.max_bad_records = max_bad_records
        file_size = os.path.getsize(file_artifact.file_path)
        with open(file_artifact.file_path, "rb") as source_file:
            if file_size > 10 * 1024 * 1024:
                job = self.connector.connection.load_table_from_file(
                    source_file, table_id, job_config=job_config, size=file_size
                )
            else:
                job = self.connector.connection.load_table_from_file(source_file, table_id, job_config=job_config)
            result = job.result()
        rows_loaded = result.output_rows if result else 0
        self.logger.log_etl_progress(
            "BQ_STAGING_LOAD_FROM_FILE_CSV",
            {
                "Table": table_id,
                "File": file_artifact.file_path,
                "Format": "CSV",
                "Rows": rows_loaded,
                "Method": "Direct Load Job",
                "BadRecords": result.errors if result and hasattr(result, "errors") else 0,
            },
        )
        return rows_loaded

    def _load_csv_via_gcs(
        self, table_id: str, file_artifact: FileExportArtifact, artifact: StagingTableArtifact, target_schema: str
    ) -> int:
        import uuid

        from dpone.runtime.support.gcs import get_env_code, get_gcs_bucket_name

        env_code = get_env_code()
        bucket_name = get_gcs_bucket_name(target_schema, env_code)
        target_table = artifact.table.replace("__tmp", "")
        file_uuid = uuid.uuid4().hex[:8]
        filename = os.path.basename(file_artifact.file_path)
        gcs_path = f"{target_schema}/full-refresh/{target_schema}/{target_schema}.{target_table}/{file_uuid}_{filename}"
        self.logger.log_etl_progress(
            "BQ_STAGING_LOAD_VIA_GCS_START",
            {"Table": table_id, "LocalFile": file_artifact.file_path, "GCSBucket": bucket_name, "GCSPath": gcs_path},
        )
        gcs_uri = self.connector.upload_file_to_gcs(
            local_file_path=file_artifact.file_path,
            gcs_bucket=bucket_name,
            gcs_path=gcs_path,
            delete_local_after_upload=False,
        )
        try:
            schema = getattr(artifact.staging_manager, "_last_schema", None)
            rows_loaded = self.connector.load_from_gcs(
                table_id=table_id,
                gcs_uri=gcs_uri,
                source_format="CSV",
                write_disposition="WRITE_APPEND",
                schema=schema,
                autodetect_schema=False if schema else True,
            )
            self.logger.log_etl_progress(
                "BQ_STAGING_LOAD_VIA_GCS_COMPLETE",
                {"Table": table_id, "GCS_URI": gcs_uri, "Rows": rows_loaded, "Method": "GCS (optimized)"},
            )
            return rows_loaded
        finally:
            try:
                from google.cloud import storage

                parts = gcs_uri[5:].split("/", 1)
                bucket_name_from_uri = parts[0]
                blob_path = parts[1] if len(parts) > 1 else ""
                scoped_credentials = self.connector._get_scoped_credentials()
                gcs_client = storage.Client(project=self.connector.project_id, credentials=scoped_credentials)
                bucket = gcs_client.bucket(bucket_name_from_uri)
                blob = bucket.blob(blob_path)
                blob.delete()
                self.logger.log_etl_progress("BQ_STAGING_GCS_CLEANUP", {"GCS_URI": gcs_uri, "Status": "Deleted"})
            except Exception as cleanup_exc:
                self.logger.warning(f"Failed to cleanup GCS file {gcs_uri}: {cleanup_exc}")
