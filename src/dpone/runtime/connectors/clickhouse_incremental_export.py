"""Partitioned incremental GCS export helpers for ClickHouseConnector."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.gcs_replacement import GcsAttemptScope


from typing import Any

from dpone.runtime.gcs_replacement import (
    canonical_partition_prefix,
    derive_attempt_table_prefix,
    derive_partition_attempt_uri,
    derive_table_attempt_uri,
    resolve_gcs_attempt_scope,
)


class ClickHouseIncrementalExportService:
    def __init__(self, connector: Any):
        self.connector = connector

    @property
    def logger(self):
        return self.connector.logger

    def export_partitions(
        self,
        query: str,
        partitions: list[tuple[str, str]],
        gcs_base_uri: str,
        *,
        format: str = "parquet",
        file_prefix: str = "data",
    ) -> list[str]:
        exported = []
        for partition_label, partition_predicate in partitions:
            partition_query = f"SELECT * FROM ({query}) AS sub WHERE {partition_predicate}"
            partition_gcs_uri = f"{gcs_base_uri}/dt_date={partition_label}"
            self.connector.export_to_gcs(
                partition_query,
                partition_gcs_uri,
                format=format,
                file_prefix=file_prefix,
            )
            exported.append(partition_label)
        return exported

    def _determine_partition_type(
        self,
        partition_label: str,
        partition_predicate: str,
        *,
        lookback_partitions: list[str] | None = None,
    ) -> dict[str, Any]:
        lookback_partitions = lookback_partitions or []
        is_lookback = partition_label in lookback_partitions
        has_incremental_filter = " > " in partition_predicate
        is_same_partition_reload = not is_lookback and has_incremental_filter
        if is_lookback:
            partition_type = "LOOKBACK"
            should_replace = True
        elif is_same_partition_reload:
            partition_type = "INCREMENTAL"
            should_replace = True
        else:
            partition_type = "NEW"
            should_replace = False
        return {
            "partition_type": partition_type,
            "should_replace": should_replace,
            "is_lookback": is_lookback,
            "is_same_partition_reload": is_same_partition_reload,
            "has_incremental_filter": has_incremental_filter,
        }

    def _build_select_clause(self, schema: list[tuple[str, str]]) -> str:
        from dpone.runtime.sources.strategies.clickhouse.clickhouse_base import ClickHouseBaseStrategy

        transformed = [
            f"{ClickHouseBaseStrategy.transform_column_for_export(col, col_type)} AS `{col}`"
            for col, col_type in schema
        ]
        return f"SELECT {', '.join(transformed)}"

    def _build_partition_query(
        self,
        query: str,
        schema: list[tuple[str, str]],
        partition_predicate: str,
        *,
        is_same_partition_reload: bool = False,
        partition_label: str | None = None,
        incremental_column: str | None = None,
        source_schema: str | None = None,
        source_table: str | None = None,
    ) -> tuple[str, str]:
        select_clause = self._build_select_clause(schema)
        if is_same_partition_reload and source_schema and source_table and incremental_column:
            actual_predicate = f"toDate(`{incremental_column}`) = '{partition_label}'"
            from_clause = f"`{source_schema}`.`{source_table}`"
        else:
            actual_predicate = partition_predicate
            from_clause = f"({query}) AS incremental_data"
        partition_query = f"""
            {select_clause}
            FROM {from_clause}
            WHERE {actual_predicate}
        """
        return partition_query, actual_predicate

    def export_incremental_with_cleanup(
        self,
        query: str,
        schema: list[tuple[str, str]],
        partitions: list[tuple[str, str]],
        bucket_name: str,
        base_table_path: str,
        base_gcs_uri: str,
        *,
        format: str = "parquet",
        chunk_rows: int | None = None,
        file_prefix: str = "data",
        lookback_partitions: list[str] | None = None,
        incremental_column: str | None = None,
        source_schema: str | None = None,
        source_table: str | None = None,
        attempt_scope: GcsAttemptScope | None = None,
        load_config: Any | None = None,
    ) -> dict[str, Any]:
        del base_gcs_uri  # attempt-owned identity is resolved before export
        scope = attempt_scope or resolve_gcs_attempt_scope(load_config)
        attempt_gcs_uri = derive_table_attempt_uri(bucket_name, base_table_path, scope)
        attempt_table_prefix = derive_attempt_table_prefix(base_table_path, scope)

        created_partitions = []
        incremental_partition_labels = []
        replacement_partition_labels = []
        for partition_label, partition_predicate in partitions:
            partition_info = self._determine_partition_type(
                partition_label,
                partition_predicate,
                lookback_partitions=lookback_partitions,
            )
            should_replace = partition_info["should_replace"]
            partition_type = partition_info["partition_type"]
            is_same_partition_reload = partition_info["is_same_partition_reload"]
            partition_gcs_uri = derive_partition_attempt_uri(
                bucket_name,
                base_table_path,
                scope,
                partition_label,
            )
            if should_replace:
                incremental_partition_labels.append(partition_label)
                replacement_partition_labels.append(partition_label)
            partition_query, actual_predicate = self._build_partition_query(
                query,
                schema,
                partition_predicate,
                is_same_partition_reload=is_same_partition_reload,
                partition_label=partition_label,
                incremental_column=incremental_column,
                source_schema=source_schema,
                source_table=source_table,
            )
            self.logger.log_etl_progress(
                "CH_GCS_INCREMENTAL_PARTITION_START",
                {
                    "Partition": partition_label,
                    "Type": partition_type,
                    "GCS_URI": partition_gcs_uri,
                    "FilePrefix": file_prefix,
                    "Predicate": actual_predicate,
                    "Replacement": should_replace,
                },
            )
            try:
                self.connector.export_to_gcs(
                    partition_query,
                    partition_gcs_uri,
                    format=format,
                    chunk_rows=chunk_rows,
                    file_prefix=file_prefix,
                )
                created_partitions.append(partition_label)
                self.logger.log_etl_progress(
                    "CH_GCS_INCREMENTAL_PARTITION_SUCCESS",
                    {
                        "Partition": partition_label,
                        "Type": partition_type,
                        "Destination": f"{partition_gcs_uri}/{file_prefix}",
                    },
                )
            except Exception as exc:  # pragma: no cover - passthrough
                self.logger.log_etl_error(
                    f"Ошибка экспорта инкрементальной партиции {partition_label}: {str(exc)}",
                    {
                        "Partition": partition_label,
                        "GCS_URI": partition_gcs_uri,
                        "Query": partition_query[:200],
                    },
                )
                raise
        prior_generation_prefixes = tuple(
            canonical_partition_prefix(base_table_path, label) for label in replacement_partition_labels
        )
        self.logger.log_etl_progress(
            "CH_GCS_INCREMENTAL_EXPORT_COMPLETE",
            {
                "GCS_Base_URI": attempt_gcs_uri,
                "Partitions": len(created_partitions),
                "Created_Partitions": created_partitions,
                "Lookback_Partitions": lookback_partitions or [],
                "Incremental_Partitions": incremental_partition_labels,
                "Prior_Generations": list(prior_generation_prefixes),
            },
        )
        return {
            "created_partitions": created_partitions,
            "lookback_partitions": lookback_partitions or [],
            "incremental_partitions": incremental_partition_labels,
            "attempt_gcs_uri": attempt_gcs_uri,
            "attempt_table_prefix": attempt_table_prefix,
            "attempt_scope": scope,
            "prior_generation_prefixes": prior_generation_prefixes,
        }
