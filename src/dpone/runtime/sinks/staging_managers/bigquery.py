"""BigQuery staging manager."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.bigquery_staging_file_loader import BigQueryStagingFileLoader
from dpone.runtime.sinks.bigquery_staging_gcs_loader import BigQueryStagingGcsLoader
from dpone.runtime.staging import StagingManager
from dpone.runtime.support.data_type_mapper import DataTypeMapper


class BigQueryStagingManager(StagingManager):
    """Управляет жизненным циклом staging таблиц в BigQuery."""

    def __init__(self, connector, logger: ETLLogger | None = None):
        from dpone.runtime.connectors import BigQueryConnector

        self.connector: BigQueryConnector = connector
        self.logger = logger or etl_logger
        self.file_loader = BigQueryStagingFileLoader(connector, self.logger)
        self.gcs_loader = BigQueryStagingGcsLoader(connector, self.logger)

    def create(self, load_config, schema: Sequence[tuple[str, str]]) -> StagingTableArtifact:
        from google.cloud import bigquery

        from dpone.config.load_strategy import LoadStrategy

        client = self.connector.connection
        overwrite_type = getattr(load_config, "overwrite_type", None)
        is_full_refresh_exchange = (
            load_config.load_strategy == LoadStrategy.FULL_REFRESH and overwrite_type == "exchange"
        )
        dataset_id = load_config.target_schema if is_full_refresh_exchange else load_config.staging_schema
        dataset_ref = bigquery.Dataset(f"{self.connector.project_id}.{dataset_id}")
        dataset_ref.location = "US"
        try:
            client.create_dataset(dataset_ref, exists_ok=True)
        except Exception:
            pass

        table_name = f"{load_config.target_table}__tmp"
        table_id = f"{self.connector.project_id}.{dataset_id}.{table_name}"
        schema_fields = []
        for column, dtype in schema:
            if is_full_refresh_exchange:
                dtype_for_staging = dtype
            else:
                dtype_for_staging = "text" if dtype.lower() in ("json", "jsonb") else dtype
            schema_fields.append(DataTypeMapper.to_bigquery_schema_field(column, dtype_for_staging))

        has_meta_xmin = any(col == "__dpone__xmin" for col, _ in schema)
        if not has_meta_xmin and load_config.load_strategy in (
            LoadStrategy.INCREMENTAL_MERGE,
            LoadStrategy.INCREMENTAL_APPEND,
        ):
            schema_fields.append(DataTypeMapper.to_bigquery_schema_field("__dpone__xmin", "bigint"))

        self._last_schema = schema_fields
        table_ref = bigquery.Table(table_id, schema=schema_fields)
        try:
            client.create_table(table_ref, exists_ok=True)
        except Exception as e:
            self.logger.log_etl_error(f"Ошибка создания staging таблицы: {str(e)}", {"table_id": table_id})

        self.logger.log_etl_progress(
            "BQ_STAGING_CREATED",
            {"Table": table_id, "Columns": len(schema)},
        )
        return StagingTableArtifact(
            schema=dataset_id,
            table=table_name,
            columns=[col for col, _ in schema],
            staging_manager=self,
            target_schema=load_config.target_schema,
        )

    def insert_rows(self, artifact: StagingTableArtifact, rows: Iterable[Mapping[str, object]]) -> int:
        import pandas as pd
        from google.cloud import bigquery

        if not rows:
            return 0
        try:
            rows_list = self._prepare_rows_for_dataframe(rows)
            df = pd.DataFrame(rows_list)
            self._apply_temporal_pandas_dtypes(df)
            self._apply_repeated_arrow_dtypes(df)
            table_id = f"{self.connector.project_id}.{artifact.schema}.{artifact.table}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = self.connector.connection.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            self.logger.log_etl_progress(
                "BQ_STAGING_INSERT_ROWS",
                {"Table": table_id, "Rows": len(rows_list)},
            )
            return len(rows_list)
        except Exception as exc:
            self.logger.log_etl_error(
                f"Ошибка вставки в staging таблицу BigQuery: {str(exc)}",
                {"table": artifact.qualified_name()},
            )
            return 0

    def _prepare_rows_for_dataframe(self, rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
        repeated_field_names = self._repeated_field_names()
        rows_list = [dict(row) for row in rows]
        for row in rows_list:
            for key, value in list(row.items()):
                row[key] = self._normalize_dataframe_value(key, value, repeated_field_names)
        return rows_list

    def _repeated_field_names(self) -> set[str]:
        schema = getattr(self, "_last_schema", None) or []
        return {field.name for field in schema if getattr(field, "mode", "").upper() == "REPEATED"}

    @staticmethod
    def _normalize_dataframe_value(key: str, value: object, repeated_field_names: set[str]) -> object:
        if isinstance(value, uuid.UUID):
            return str(value)
        if key == "__dpone__xmin" and isinstance(value, str):
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        if isinstance(value, dict):
            return json.dumps(value)
        if isinstance(value, list):
            if key in repeated_field_names:
                return [item for item in value if item is not None]
            return json.dumps(value)
        return value

    def _apply_repeated_arrow_dtypes(self, df) -> None:
        schema = getattr(self, "_last_schema", None) or []
        if not schema:
            return
        for field in schema:
            if getattr(field, "mode", "").upper() != "REPEATED":
                continue
            if field.name not in df.columns:
                continue
            column = df[field.name].apply(
                lambda value: value if isinstance(value, list) else ([] if value is None else [value])
            )
            try:
                import pandas as pd
                import pyarrow as pa

                pa_type = DataTypeMapper.bq_field_type_to_pyarrow(field.field_type)
                df[field.name] = column.astype(pd.ArrowDtype(pa.list_(pa_type)))
            except Exception:
                df[field.name] = column

    def _apply_temporal_pandas_dtypes(self, df) -> None:
        schema = getattr(self, "_last_schema", None) or []
        if not schema:
            return

        import pandas as pd

        for field in schema:
            if field.name not in df.columns:
                continue

            field_type = getattr(field, "field_type", "").upper()
            if field_type not in {"DATE", "DATETIME", "TIMESTAMP", "TIME"}:
                continue

            series = df[field.name]
            if series.empty:
                continue

            if field_type == "DATE":
                df[field.name] = pd.to_datetime(series, errors="coerce").dt.date
                continue

            if field_type in {"DATETIME", "TIMESTAMP"}:
                parsed = pd.to_datetime(series, errors="coerce")
                if getattr(parsed.dt, "tz", None) is not None:
                    parsed = parsed.dt.tz_convert(None)
                df[field.name] = parsed
                continue

            if field_type == "TIME":
                parsed = pd.to_datetime(series, format="%H:%M:%S", errors="coerce")
                df[field.name] = parsed.dt.time

    def insert_from_query(
        self,
        artifact: StagingTableArtifact,
        query: str,
        schema: Sequence[tuple[str, str]],
        params: Sequence[Any] | None = None,
    ) -> int:
        fq_target = f"`{self.connector.project_id}.{artifact.schema}.{artifact.table}`"
        columns = [col for col, _ in schema]
        columns_str = ", ".join(f"`{col}`" for col in columns)
        insert_query = f"""
        INSERT INTO {fq_target} ({columns_str})
        {query}
        """
        try:
            result = self.connector.execute_query(insert_query, params)
            self.logger.log_etl_progress(
                "BQ_STAGING_INSERT_QUERY",
                {"Table": fq_target, "Rows": result or 0},
            )
            return result or 0
        except Exception as exc:
            self.logger.log_etl_error(
                f"Ошибка заполнения staging из query: {str(exc)}",
                {"table": artifact.qualified_name()},
            )
            return 0

    def load_from_file(self, artifact: StagingTableArtifact, file_artifact) -> int:
        return self.file_loader.load_from_file(artifact, file_artifact)

    def load_from_file_batched(self, artifact: StagingTableArtifact, file_artifact) -> int:
        return self.file_loader.load_from_file_batched(artifact, file_artifact)

    def load_from_gcs_artifact(self, artifact: StagingTableArtifact, gcs_artifact) -> int:
        return self.gcs_loader.load_from_gcs_artifact(artifact, gcs_artifact)

    def drop(self, artifact: StagingTableArtifact) -> None:
        table_id = f"{self.connector.project_id}.{artifact.schema}.{artifact.table}"
        try:
            try:
                self.connector.connection.delete_table(table_id, delete_contents=True, not_found_ok=True)
            except TypeError:
                self.connector.connection.delete_table(table_id, not_found_ok=True)
            self.logger.log_etl_progress("BQ_STAGING_DROPPED", {"Table": table_id})
        except Exception as exc:
            self.logger.log_etl_error(
                f"Ошибка удаления staging таблицы: {str(exc)}",
                {"table": artifact.qualified_name()},
            )
