"""GCS-based loading helpers for BigQuery staging tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.runtime.support.bigquery_load_config import postgres_headerless_csv_load_config

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.connectors import BigQueryConnector
    from dpone.runtime.sink_logging import ETLLogger


class BigQueryStagingGcsLoader:
    def __init__(self, connector: BigQueryConnector, logger: ETLLogger):
        self.connector = connector
        self.logger = logger

    def load_from_gcs_artifact(self, artifact: StagingTableArtifact, gcs_artifact) -> int:
        table_id = f"{self.connector.project_id}.{artifact.schema}.{artifact.table}"
        if gcs_artifact.hive_partitioning and gcs_artifact.source_uri_prefix:
            if gcs_artifact.new_partitions and gcs_artifact.load_patterns:
                gcs_uri_with_pattern = [
                    f"{gcs_artifact.source_uri_prefix}dt_date={partition}/*"
                    for partition in gcs_artifact.new_partitions
                ]
            elif gcs_artifact.new_partitions:
                gcs_uri_with_pattern = [
                    f"{gcs_artifact.source_uri_prefix}dt_date={partition}/*"
                    for partition in gcs_artifact.new_partitions
                ]
            else:
                gcs_uri_with_pattern = f"{gcs_artifact.source_uri_prefix}*"
        elif gcs_artifact.pattern:
            gcs_uri_with_pattern = f"{gcs_artifact.gcs_uri}/{gcs_artifact.pattern}"
        else:
            ext_map = {"parquet": "*.parquet", "csv": "*.csv", "orc": "*.orc"}
            ext_pattern = ext_map.get(gcs_artifact.format.lower(), "*")
            gcs_uri_with_pattern = f"{gcs_artifact.gcs_uri}/{ext_pattern}"

        if isinstance(gcs_uri_with_pattern, list):
            display_uri = (
                "\n".join(gcs_uri_with_pattern)
                if len(gcs_uri_with_pattern) <= 5
                else f"[{len(gcs_uri_with_pattern)} URIs: {gcs_artifact.source_uri_prefix}dt_date=...]"
            )
        else:
            display_uri = gcs_uri_with_pattern

        self.logger.log_etl_progress(
            "BQ_LOADING_FROM_GCS",
            {
                "Source": display_uri,
                "Target": table_id,
                "Format": gcs_artifact.format.upper(),
                "HivePartitioning": gcs_artifact.hive_partitioning,
                "SourceURIPrefix": gcs_artifact.source_uri_prefix if gcs_artifact.hive_partitioning else "N/A",
                "NewPartitions": gcs_artifact.new_partitions if gcs_artifact.new_partitions else "All",
                "LoadPatterns": gcs_artifact.load_patterns if gcs_artifact.load_patterns else "N/A",
            },
        )
        try:
            schema = getattr(artifact.staging_manager, "_last_schema", None)
            if schema is None:
                client = self.connector.connection
                table_ref = client.get_table(table_id)
                schema = table_ref.schema

            if (
                gcs_artifact.hive_partitioning
                and gcs_artifact.new_partitions
                and len(gcs_artifact.new_partitions) > 1
                and isinstance(gcs_uri_with_pattern, list)
            ):
                total_rows_loaded = 0
                partition_rows = {}
                for partition_date in gcs_artifact.new_partitions:
                    partition_uri = f"{gcs_artifact.source_uri_prefix}dt_date={partition_date}/*"
                    partition_rows_loaded = self.connector.load_from_gcs(
                        table_id=table_id,
                        gcs_uri=partition_uri,
                        source_format=gcs_artifact.format,
                        write_disposition="WRITE_APPEND",
                        hive_partitioning=gcs_artifact.hive_partitioning,
                        source_uri_prefix=gcs_artifact.source_uri_prefix,
                        schema=schema,
                        autodetect_schema=False,
                        csv_config=postgres_headerless_csv_load_config()
                        if gcs_artifact.format.lower() == "csv"
                        else None,
                    )
                    partition_rows[partition_date] = partition_rows_loaded
                    total_rows_loaded += partition_rows_loaded
                if not hasattr(artifact, "_partition_rows"):
                    artifact._partition_rows = {}
                artifact._partition_rows.update(partition_rows)
                self.logger.log_etl_progress(
                    "BQ_LOADED_FROM_GCS",
                    {
                        "Table": table_id,
                        "GCS_URI": display_uri,
                        "Rows": total_rows_loaded,
                        "Partitions": len(gcs_artifact.new_partitions),
                        "PartitionRows": partition_rows,
                    },
                )
                return total_rows_loaded

            rows_loaded = self.connector.load_from_gcs(
                table_id=table_id,
                gcs_uri=gcs_uri_with_pattern,
                source_format=gcs_artifact.format,
                write_disposition="WRITE_APPEND",
                hive_partitioning=gcs_artifact.hive_partitioning,
                source_uri_prefix=gcs_artifact.source_uri_prefix,
                schema=schema,
                autodetect_schema=False,
                csv_config=postgres_headerless_csv_load_config() if gcs_artifact.format.lower() == "csv" else None,
            )
            self.logger.log_etl_progress(
                "BQ_LOADED_FROM_GCS",
                {"Table": table_id, "GCS_URI": display_uri, "Rows": rows_loaded},
            )
            return rows_loaded
        except Exception as exc:
            self.logger.log_etl_error(
                f"Ошибка загрузки из GCS в BigQuery staging: {str(exc)}",
                {"GCS_URI": display_uri, "Table": table_id},
            )
            raise
