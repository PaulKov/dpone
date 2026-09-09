"""BigQuery-backed canonical load audit storage."""

from __future__ import annotations

from typing import Any

from dpone.runtime.lineage.audit import LoadAuditRecord


class BigQueryLoadAuditStorage:
    """Stores canonical dpone load lifecycle records in BigQuery."""

    def __init__(self, connector: Any, dataset: str = "etl_state", table: str = "__dpone__loads") -> None:
        self.connector = connector
        self.dataset = dataset
        self.table = table
        self._table_created = False

    @property
    def project_id(self) -> str:
        return str(self.connector.project_id)

    @property
    def fq_dataset(self) -> str:
        return f"{self.project_id}.{self.dataset}"

    @property
    def fq_table(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}"

    def create_load_table(self) -> None:
        if self._table_created:
            return
        bigquery = _bigquery()
        client = self.connector.connection
        client.create_dataset(bigquery.Dataset(self.fq_dataset), exists_ok=True)
        table_ref = bigquery.Table(self.fq_table)
        table_ref.schema = [
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("load_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("process_name", "STRING"),
            bigquery.SchemaField("source_schema", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("target_schema", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("target_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("strategy", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("staged_at", "TIMESTAMP"),
            bigquery.SchemaField("committed_at", "TIMESTAMP"),
            bigquery.SchemaField("failed_at", "TIMESTAMP"),
            bigquery.SchemaField("extracted_rows", "INT64"),
            bigquery.SchemaField("staged_rows", "INT64"),
            bigquery.SchemaField("inserted_rows", "INT64"),
            bigquery.SchemaField("updated_rows", "INT64"),
            bigquery.SchemaField("loaded_rows", "INT64"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("artifact_uri", "STRING"),
            bigquery.SchemaField("__dpone__loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        client.create_table(table_ref, exists_ok=True)
        self._table_created = True

    def record_load_started(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def record_load_staged(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def record_load_committed(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def record_load_failed(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def _upsert(self, record: LoadAuditRecord) -> None:
        self.create_load_table()
        bigquery = _bigquery()
        self.connector.execute_query(
            self._merge_sql(), job_config=bigquery.QueryJobConfig(query_parameters=self._params(record))
        )

    def _merge_sql(self) -> str:
        return f"""
        MERGE `{self.fq_table}` AS target
        USING (
            SELECT
                @run_id AS run_id,
                @load_id AS load_id,
                @status AS status,
                @process_name AS process_name,
                @source_schema AS source_schema,
                @source_table AS source_table,
                @target_schema AS target_schema,
                @target_table AS target_table,
                @strategy AS strategy,
                @started_at AS started_at,
                @staged_at AS staged_at,
                @committed_at AS committed_at,
                @failed_at AS failed_at,
                @extracted_rows AS extracted_rows,
                @staged_rows AS staged_rows,
                @inserted_rows AS inserted_rows,
                @updated_rows AS updated_rows,
                @loaded_rows AS loaded_rows,
                @error_message AS error_message,
                @artifact_uri AS artifact_uri,
                CURRENT_TIMESTAMP() AS __dpone__loaded_at
        ) AS source
        ON target.load_id = source.load_id
        WHEN MATCHED THEN UPDATE SET
            status = source.status,
            staged_at = source.staged_at,
            committed_at = source.committed_at,
            failed_at = source.failed_at,
            extracted_rows = source.extracted_rows,
            staged_rows = source.staged_rows,
            inserted_rows = source.inserted_rows,
            updated_rows = source.updated_rows,
            loaded_rows = source.loaded_rows,
            error_message = source.error_message,
            artifact_uri = source.artifact_uri,
            __dpone__loaded_at = source.__dpone__loaded_at
        WHEN NOT MATCHED THEN INSERT (
            run_id, load_id, status, process_name, source_schema, source_table,
            target_schema, target_table, strategy, started_at, staged_at, committed_at,
            failed_at, extracted_rows, staged_rows, inserted_rows, updated_rows,
            loaded_rows, error_message, artifact_uri, __dpone__loaded_at
        ) VALUES (
            source.run_id, source.load_id, source.status, source.process_name,
            source.source_schema, source.source_table, source.target_schema,
            source.target_table, source.strategy, source.started_at, source.staged_at,
            source.committed_at, source.failed_at, source.extracted_rows,
            source.staged_rows, source.inserted_rows, source.updated_rows,
            source.loaded_rows, source.error_message, source.artifact_uri,
            source.__dpone__loaded_at
        )
        """

    def _params(self, record: LoadAuditRecord) -> list[Any]:
        bigquery = _bigquery()
        return [
            bigquery.ScalarQueryParameter("run_id", "STRING", record.run_id),
            bigquery.ScalarQueryParameter("load_id", "STRING", record.load_id),
            bigquery.ScalarQueryParameter("status", "STRING", record.status),
            bigquery.ScalarQueryParameter("process_name", "STRING", record.process_name),
            bigquery.ScalarQueryParameter("source_schema", "STRING", record.source_schema),
            bigquery.ScalarQueryParameter("source_table", "STRING", record.source_table),
            bigquery.ScalarQueryParameter("target_schema", "STRING", record.target_schema),
            bigquery.ScalarQueryParameter("target_table", "STRING", record.target_table),
            bigquery.ScalarQueryParameter("strategy", "STRING", record.strategy),
            bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", record.started_at),
            bigquery.ScalarQueryParameter("staged_at", "TIMESTAMP", record.staged_at),
            bigquery.ScalarQueryParameter("committed_at", "TIMESTAMP", record.committed_at),
            bigquery.ScalarQueryParameter("failed_at", "TIMESTAMP", record.failed_at),
            bigquery.ScalarQueryParameter("extracted_rows", "INT64", record.extracted_rows),
            bigquery.ScalarQueryParameter("staged_rows", "INT64", record.staged_rows),
            bigquery.ScalarQueryParameter("inserted_rows", "INT64", record.inserted_rows),
            bigquery.ScalarQueryParameter("updated_rows", "INT64", record.updated_rows),
            bigquery.ScalarQueryParameter("loaded_rows", "INT64", record.loaded_rows),
            bigquery.ScalarQueryParameter("error_message", "STRING", record.error_message),
            bigquery.ScalarQueryParameter("artifact_uri", "STRING", record.artifact_uri),
        ]


def _bigquery() -> Any:
    from google.cloud import bigquery

    return bigquery
