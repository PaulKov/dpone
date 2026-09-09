"""DML and SQL helper methods for BigQuery sink strategies."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any

from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sql_helpers import TechnicalColumnsQueries


class BigQueryDmlHelper:
    """Owns SQL snippets and DML execution for BigQuery strategies."""

    def __init__(
        self,
        connector: Any,
        logger: ETLLogger | None,
        include_technical_columns: Callable[[Any], bool],
        get_fq_table: Callable[[str, str], str],
    ) -> None:
        self.connector = connector
        self.logger = logger or etl_logger
        self._include_technical_columns = include_technical_columns
        self._get_fq_table = get_fq_table

    def build_select_with_json_parse(
        self,
        schema: Sequence[tuple[str, str]],
        staging_table_alias: str | None = None,
        include_technical_columns: bool = True,
    ) -> tuple[str, str]:
        """Build a SELECT clause that PARSE_JSONs JSON/JSONB columns."""
        data_columns = [column for column, _ in schema]
        json_columns = [column for column, dtype in schema if dtype.lower() in ("json", "jsonb")]

        if include_technical_columns:
            return TechnicalColumnsQueries.bq_build_select_with_technical_columns(
                data_columns=data_columns,
                json_columns=json_columns,
                table_alias=staging_table_alias or "",
            )

        prefix = f"{staging_table_alias}." if staging_table_alias else ""
        select_columns: list[str] = []
        column_names: list[str] = []
        for column, dtype in schema:
            column_names.append(f"`{column}`")
            if dtype.lower() in ("json", "jsonb"):
                select_columns.append(f"PARSE_JSON({prefix}`{column}`) AS `{column}`")
            else:
                select_columns.append(f"{prefix}`{column}`")

        return ", ".join(column_names), ", ".join(select_columns)

    def insert_from_staging(
        self,
        load_config: Any,
        staging_table: str,
        full_staging_table: str,
        columns: Sequence[str],
    ) -> int:
        """Insert rows from a staging table into target."""
        del staging_table  # kept for API compatibility / logs in callers
        columns_str = ", ".join(f"`{column}`" for column in columns)
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)

        query = f"""
        INSERT INTO {fq_target} ({columns_str})
        SELECT {columns_str} FROM {full_staging_table}
        """

        self.logger.log_etl_progress(
            "BQ_INSERT_FROM_STAGING",
            {
                "Target": fq_target,
                "Columns": len(columns),
            },
        )

        result = self.execute_dml_query(query)
        if result and result > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
            self.log_target_sample(fq_target, load_config.log_sample_rows)
        return result or 0

    def build_unique_key_condition(
        self,
        left_alias: str,
        right_alias: str,
        unique_key: str | Sequence[str],
    ) -> str:
        """Build `left.col = right.col` comparison for unique key columns."""
        if isinstance(unique_key, str):
            unique_key = [unique_key]
        return " AND ".join(f"{left_alias}.`{column}` = {right_alias}.`{column}`" for column in unique_key)

    def delete_existing(
        self,
        load_config: Any,
        full_staging_table: str,
        unique_key: str | Sequence[str],
    ) -> int:
        """Delete target rows matching staging unique keys."""
        condition = self.build_unique_key_condition("t", "s", unique_key)
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        query = f"""
        DELETE FROM {fq_target} AS t
        WHERE EXISTS (
            SELECT 1 FROM {full_staging_table} AS s
            WHERE {condition}
        )
        """

        self.logger.log_etl_progress(
            "BQ_DELETE_EXISTING",
            {
                "Target": fq_target,
                "Condition": condition,
            },
        )
        result = self.execute_dml_query(query)
        return result or 0

    def delete_with_predicate(self, load_config: Any, predicate: str) -> int:
        """Delete target rows by arbitrary predicate."""
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        query = f"""
        DELETE FROM {fq_target}
        WHERE {predicate}
        """

        self.logger.log_etl_progress(
            "BQ_DELETE_WITH_PREDICATE",
            {
                "Target": fq_target,
                "Predicate": predicate[:100],
            },
        )
        result = self.execute_dml_query(query)
        return result or 0

    def delete_partitions(
        self,
        load_config: Any,
        partition_dates: Sequence[str],
        partition_column: str = "dt",
    ) -> int:
        """Delete rows for a set of lookback partitions."""
        if not partition_dates:
            return 0

        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        options = getattr(load_config, "options", {}) or {}
        timezone = options.get("_column_timezone")
        min_date = min(partition_dates)
        max_date = max(partition_dates)
        max_date_plus_one = (datetime.strptime(max_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        if timezone:
            query = f"""
            DELETE FROM {fq_target}
            WHERE `{partition_column}` >= TIMESTAMP('{min_date} 00:00:00', '{timezone}')
              AND `{partition_column}` < TIMESTAMP('{max_date_plus_one} 00:00:00', '{timezone}')
            """
        else:
            query = f"""
            DELETE FROM {fq_target}
            WHERE `{partition_column}` >= TIMESTAMP('{min_date} 00:00:00 UTC')
              AND `{partition_column}` < TIMESTAMP('{max_date_plus_one} 00:00:00 UTC')
            """

        self.logger.log_etl_progress(
            "BQ_DELETE_LOOKBACK_PARTITIONS",
            {
                "Target": fq_target,
                "Partitions": list(partition_dates),
                "PartitionColumn": partition_column,
                "Timezone": timezone or "UTC (default)",
                "Reason": "Lookback window refresh",
            },
        )
        self.logger.info(f"🔍 DELETE SQL:\n{query}")
        result = self.execute_dml_query(query)
        self.logger.info(
            f"🗑️ LOOKBACK CLEANUP: удалено {result or 0} строк из партиций {partition_dates}"
            f"{f' (timezone: {timezone})' if timezone else ''}"
        )
        return result or 0

    def truncate_table(self, load_config: Any) -> None:
        """Delete all rows from target table."""
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        query = f"DELETE FROM {fq_target} WHERE TRUE"
        self.logger.log_etl_progress("BQ_TRUNCATE_TABLE", {"Target": fq_target})
        self.connector.execute_query(query)

    def execute_dml_query(self, query: str, params=None) -> int:
        """Execute a BigQuery DML query and return affected row count."""
        job_config = self.connector._build_query_config(params)
        job = self.connector.connection.query(query, job_config=job_config)
        job.result()
        try:
            stats = job._properties.get("statistics", {})
            query_stats = stats.get("query", {})
            dml = query_stats.get("dmlStats", {})
            inserted = int(dml.get("insertedRowCount", 0) or 0)
            updated = int(dml.get("updatedRowCount", 0) or 0)
            deleted = int(dml.get("deletedRowCount", 0) or 0)
            return inserted + updated + deleted
        except Exception:
            return 0

    def log_target_sample(self, fq_target: str, max_rows: int = 5) -> None:
        """Log a sample of target rows."""
        try:
            query = f"SELECT * FROM {fq_target} LIMIT {max_rows}"
            rows = self.connector.get_records(query)
            if rows:
                self.logger.log_data_sample("TARGET", rows, max_rows)
        except Exception:
            pass
