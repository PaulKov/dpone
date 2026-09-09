"""Partition validation helpers for BigQuery sink strategies."""

from __future__ import annotations

from typing import Any

from dpone.runtime.sink_logging import ETLLogger, etl_logger


class BigQueryPartitionValidationService:
    """Validate ClickHouse partitions after BigQuery loads."""

    def __init__(self, logger: ETLLogger | None) -> None:
        self.logger = logger or etl_logger

    def validate_clickhouse(
        self,
        load_config: Any,
        payload: Any,
        inserted: int,
    ) -> dict:
        """Validate partition counts for ClickHouse -> BigQuery loads."""
        options = load_config.options or {}
        source_connector = options.get("_source_connector")
        date_column = options.get("date_column")
        partition_by = options.get("partition_by", "day")

        if not (
            source_connector
            and hasattr(source_connector, "__class__")
            and source_connector.__class__.__name__ == "ClickHouseConnector"
        ):
            return {}

        partitions = []
        if options.get("_new_partitions"):
            partitions = options.get("_new_partitions")
        elif hasattr(payload.artifact, "new_partitions") and payload.artifact.new_partitions:
            partitions = payload.artifact.new_partitions
        elif hasattr(payload.artifact, "lookback_partitions") and payload.artifact.lookback_partitions:
            partitions = payload.artifact.lookback_partitions

        if not partitions or not date_column:
            return {}

        partition_rows = {}
        if hasattr(payload.artifact, "_partition_rows") and payload.artifact._partition_rows:
            partition_rows = payload.artifact._partition_rows

        partition_validation_results = {}
        for partition_date in partitions:
            target_count = partition_rows.get(partition_date, 0)

            if not partition_rows and len(partitions) > 1:
                ch_count = source_connector.count_rows_by_date(
                    schema=load_config.source_schema,
                    table=load_config.source_table,
                    date_column=date_column,
                    partition_date=partition_date,
                    partition_by=partition_by,
                )
                total_ch_rows = sum(
                    source_connector.count_rows_by_date(
                        schema=load_config.source_schema,
                        table=load_config.source_table,
                        date_column=date_column,
                        partition_date=part,
                        partition_by=partition_by,
                    )
                    for part in partitions
                )
                if total_ch_rows > 0:
                    target_count = int((inserted * ch_count) / total_ch_rows)
                else:
                    target_count = 0
            elif not partition_rows and len(partitions) == 1:
                target_count = inserted

            match = self.logger._validate_single_partition(
                partition_date=partition_date,
                source_connector=source_connector,
                source_schema=load_config.source_schema,
                source_table=load_config.source_table,
                date_column=date_column,
                partition_by=partition_by,
                target_count=target_count,
            )
            partition_validation_results[partition_date] = {
                "target_count": target_count,
                "match": match,
            }

        return partition_validation_results
