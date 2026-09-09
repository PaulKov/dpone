"""Target table and metadata management for BigQuery sink strategies."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import is_dataclass, replace
from typing import Any

from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sql_helpers import ExchangeQueries, TechnicalColumnsQueries
from dpone.runtime.support.data_type_mapper import DataTypeMapper


class BigQueryTargetTableManager:
    """Owns target table lifecycle for BigQuery strategies.

    Responsibilities:
    - fully-qualified table names
    - target table creation
    - labels / description updates
    - technical columns creation
    - exchange helper primitives (exists / rename / drop)
    - CREATE TABLE AS SELECT from staging
    """

    def __init__(
        self,
        connector: Any,
        logger: ETLLogger | None,
        include_technical_columns: Callable[[Any], bool],
    ) -> None:
        self.connector = connector
        self.logger = logger or etl_logger
        self._include_technical_columns = include_technical_columns

    def get_fq_table(self, dataset: str, table: str) -> str:
        """Return fully-qualified BigQuery table id in query form."""
        project = self.connector.project_id.strip("`")
        return f"`{project}.{dataset}.{table}`"

    def get_staging_table(self, load_config: Any) -> str:
        """Return generated staging table name."""
        return f"{load_config.target_table}_stg_{id(self)}"

    def create_target_table_if_not_exists(
        self,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> bool:
        """Create target table when missing.

        Returns True when a table was created, False when the table already existed.
        """
        from google.cloud.exceptions import NotFound

        bigquery = self._require_bigquery_module()
        fq_table = self.get_fq_table(load_config.target_schema, load_config.target_table)
        table_id = f"{self.connector.project_id}.{load_config.target_schema}.{load_config.target_table}"
        client = self.connector.connection

        try:
            existing = client.get_table(table_id)
            self.maybe_update_table_metadata(load_config, existing)
            self.ensure_technical_columns(load_config)
            return False
        except NotFound:
            pass

        schema_fields = self.build_schema_fields(schema)
        table_ref = bigquery.Table(table_id, schema=schema_fields)
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
        )

        opts = getattr(load_config, "options", {}) or {}
        try:
            table_labels = opts.get("table_labels")
            if table_labels is not None:
                if not isinstance(table_labels, dict):
                    raise ValueError("table_labels должен быть объектом")
                table_ref.labels = {str(k): str(v) for k, v in table_labels.items() if v is not None}

            table_description = opts.get("table_description")
            if table_description:
                table_ref.description = str(table_description)
        except Exception as exc:
            self.logger.log_etl_error(
                f"Некорректные метаданные таблицы (labels/description): {exc}",
                {"Target": fq_table},
            )

        self.logger.log_etl_progress(
            "BQ_CREATE_TARGET_TABLE",
            {
                "Table": fq_table,
                "Columns": len(schema),
            },
        )
        client.create_table(table_ref, exists_ok=True)
        self.ensure_technical_columns(load_config)
        return True

    def maybe_update_table_metadata(self, load_config: Any, table: Any | None = None) -> None:
        """Best-effort update of BigQuery labels / description for existing tables."""
        opts = getattr(load_config, "options", {}) or {}
        if not bool(opts.get("apply_table_metadata")):
            return

        table_labels = opts.get("table_labels")
        table_description = opts.get("table_description")
        if table_labels is None and not table_description:
            return

        try:
            client = self.connector.connection
            table_id = f"{self.connector.project_id}.{load_config.target_schema}.{load_config.target_table}"
            if table is None:
                table = client.get_table(table_id)

            fields: list[str] = []

            if table_labels is not None:
                if not isinstance(table_labels, dict):
                    raise ValueError("table_labels должен быть объектом")
                desired_labels = {str(k): str(v) for k, v in table_labels.items() if v is not None}
                if desired_labels != (table.labels or {}):
                    table.labels = desired_labels
                    fields.append("labels")

            if table_description:
                desired_desc = str(table_description)
                if desired_desc != (table.description or ""):
                    table.description = desired_desc
                    fields.append("description")

            if not fields:
                return

            client.update_table(table, fields)
            self.logger.log_etl_progress(
                "BQ_UPDATE_TABLE_METADATA",
                {
                    "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    "Fields": fields,
                },
            )
        except Exception as exc:
            self.logger.warning(f"Не удалось обновить метаданные таблицы (labels/description): {exc}")

    def ensure_technical_columns(self, load_config: Any) -> None:
        """Add missing __dpone__loaded_at / __dpone__deleted_at columns when enabled."""
        if not self._include_technical_columns(load_config):
            return

        bigquery = self._require_bigquery_module()
        meta_load_dtm = "__dpone__loaded_at"
        meta_delete_dtm = "__dpone__deleted_at"
        project_id = self.connector.project_id
        table_id = f"{project_id}.{load_config.target_schema}.{load_config.target_table}"

        try:
            client = self.connector.connection
            table = client.get_table(table_id)
            existing_columns = {field.name for field in table.schema}
            new_schema = list(table.schema)
            columns_added: list[str] = []

            if meta_load_dtm not in existing_columns:
                new_schema.append(bigquery.SchemaField(meta_load_dtm, "TIMESTAMP", mode="NULLABLE"))
                columns_added.append(meta_load_dtm)
            if meta_delete_dtm not in existing_columns:
                new_schema.append(bigquery.SchemaField(meta_delete_dtm, "TIMESTAMP", mode="NULLABLE"))
                columns_added.append(meta_delete_dtm)

            if columns_added:
                table.schema = new_schema
                client.update_table(table, ["schema"])

                if meta_load_dtm in columns_added:
                    update_load_dtm_query = TechnicalColumnsQueries.bq_update_load_dtm_null_rows(
                        project_id=project_id,
                        schema=load_config.target_schema,
                        table=load_config.target_table,
                    )
                    self.connector.execute_query(update_load_dtm_query)

                for column in columns_added:
                    self.logger.log_etl_progress(
                        "TECHNICAL_COLUMN_ADDED",
                        {
                            "Target": f"{load_config.target_schema}.{load_config.target_table}",
                            "Column": column,
                        },
                    )
        except Exception as exc:
            self.logger.warning(f"Не удалось добавить технические колонки: {exc}")

    def build_schema_fields(self, schema: Sequence[tuple[str, str]]) -> list[Any]:
        """Convert source schema to BigQuery schema fields."""
        return [DataTypeMapper.to_bigquery_schema_field(column, dtype) for column, dtype in schema]

    def check_table_exists(self, load_config: Any) -> bool:
        """Return True when target table exists."""
        from google.cloud.exceptions import NotFound

        table_id = f"{self.connector.project_id}.{load_config.target_schema}.{load_config.target_table}"
        client = self.connector.connection
        try:
            client.get_table(table_id)
            return True
        except NotFound:
            return False

    def rename_table(self, load_config: Any, from_table: str, to_table: str) -> None:
        """Rename a BigQuery table via DDL."""
        query = ExchangeQueries.bq_rename_table(
            project_id=self.connector.project_id,
            dataset=load_config.target_schema,
            from_table=from_table,
            to_table=to_table,
        )
        self.connector.execute_query(query)

    def drop_table(self, load_config: Any, table: str) -> None:
        """Drop a BigQuery table via DDL."""
        query = ExchangeQueries.bq_drop_table(
            project_id=self.connector.project_id,
            dataset=load_config.target_schema,
            table=table,
        )
        self.connector.execute_query(query)

    def create_table_from_staging(
        self,
        load_config: Any,
        target_table: str,
        payload: Any,
        build_select_with_json_parse: Callable[..., tuple[str, str]],
    ) -> int:
        """Create a target table from the staging table via CTAS."""
        fq_target = self.get_fq_table(load_config.target_schema, target_table)
        fq_staging = f"`{self.connector.project_id}.{load_config.staging_schema}.{load_config.target_table}__tmp`"

        include_tech = self._include_technical_columns(load_config)
        _columns_str, select_str = build_select_with_json_parse(
            payload.schema,
            include_technical_columns=include_tech,
        )

        query = f"""
        CREATE TABLE {fq_target} AS
        SELECT {select_str}
        FROM {fq_staging}
        """
        self.connector.execute_query(query)

        count_query = f"SELECT COUNT(*) AS row_count FROM {fq_target}"
        result = self.connector.get_records(count_query)
        if result and len(result) > 0:
            first_row = result[0]
            inserted = int(first_row.get("row_count", 0)) if first_row.get("row_count") is not None else 0
        else:
            inserted = 0

        if is_dataclass(load_config):
            temp_load_config = replace(load_config, target_table=target_table)
        else:
            temp_load_config = type("TempLoadConfig", (), vars(load_config).copy())()
            setattr(temp_load_config, "target_table", target_table)
        self.ensure_technical_columns(temp_load_config)
        return inserted

    @staticmethod
    def _require_bigquery_module() -> Any:
        try:
            from google.cloud import bigquery
        except Exception as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError(
                "google-cloud-bigquery is required for BigQuery sink execution. "
                "Install runtime extras before running BigQuery loads."
            ) from exc
        return bigquery
