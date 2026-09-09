"""PostgreSQL staging manager."""

from __future__ import annotations

import csv
import gzip
import io
import json
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from psycopg import sql

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.staging import StagingManager

if TYPE_CHECKING:
    from dpone.runtime.connectors import PostgresConnector


class PostgresStagingManager(StagingManager):
    """Управляет жизненным циклом staging таблиц в PostgreSQL."""

    def __init__(self, connector: PostgresConnector, logger: ETLLogger | None = None):
        self.connector = connector
        self.logger = logger or etl_logger

    def create(self, load_config, schema: Sequence[tuple[str, str]]) -> StagingTableArtifact:
        table_name = self._generate_table_name(load_config.target_table)
        staging_schema = load_config.staging_schema or load_config.target_schema

        columns_sql = sql.SQL(", ".join(f'"{column}" {dtype}' for column, dtype in schema))
        create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(staging_schema),
            sql.Identifier(table_name),
            columns_sql,
        )

        self.connector.execute_query(create_sql)
        self.logger.log_etl_progress(
            "STAGING_CREATED",
            {"Table": f"{staging_schema}.{table_name}", "Columns": len(schema)},
        )

        return StagingTableArtifact(
            schema=staging_schema,
            table=table_name,
            columns=[col for col, _ in schema],
            staging_manager=self,
        )

    def insert_rows(
        self,
        artifact: StagingTableArtifact,
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        columns = artifact.columns
        csv_rows = list(self._rows_to_csv(rows, columns))
        count = self.connector.copy_from_iter(artifact.schema, artifact.table, columns, csv_rows)
        self.logger.log_etl_progress(
            "STAGING_INSERT_ROWS",
            {"Table": artifact.qualified_name(), "Rows": count},
        )
        return count

    def insert_from_query(
        self,
        artifact: StagingTableArtifact,
        query: str,
        schema: Sequence[tuple[str, str]],
        params: Sequence[Any] | None = None,
    ) -> int:
        columns = [column for column, _ in schema]
        insert_sql = sql.SQL("INSERT INTO {}.{} ({}) {}").format(
            sql.Identifier(artifact.schema),
            sql.Identifier(artifact.table),
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            sql.SQL(query),
        )

        inserted = self.connector.execute_query(insert_sql, params)
        self.logger.log_etl_progress(
            "STAGING_INSERT_QUERY",
            {"Table": artifact.qualified_name(), "Rows": inserted},
        )
        return inserted

    def load_from_file(
        self,
        artifact: StagingTableArtifact,
        file_artifact: FileExportArtifact,
    ) -> int:
        artifact_format = str(getattr(file_artifact, "format", "") or "").lower().replace("_", "-")
        if artifact_format in {"mssql-delimited"}:
            raise ValueError(
                "Postgres sink cannot load mssql-delimited artifacts; set export_format=csv for MySQL→Postgres."
            )
        columns = artifact.columns
        format_clause = "FORMAT BINARY" if file_artifact.format == "binary" else "FORMAT CSV, HEADER FALSE"
        copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH ({})").format(
            sql.Identifier(artifact.schema),
            sql.Identifier(artifact.table),
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            sql.SQL(format_clause),
        )

        open_func = gzip.open if file_artifact.compressed else open
        mode = "rb" if file_artifact.format == "binary" else "rt"
        open_kwargs = {} if file_artifact.format == "binary" else {"encoding": "utf-8", "newline": ""}

        with self.connector.connection.cursor() as cursor:
            with cursor.copy(copy_sql) as copy:
                with open_func(file_artifact.file_path, mode, **open_kwargs) as data_file:
                    if file_artifact.format == "binary":
                        while True:
                            chunk = data_file.read(1024 * 1024)
                            if not chunk:
                                break
                            copy.write(chunk)
                    else:
                        while True:
                            chunk = data_file.read(1024 * 1024)
                            if not chunk:
                                break
                            copy.write(chunk)
        return self._count_rows(artifact)

    def load_from_gcs_artifact(self, artifact: StagingTableArtifact, gcs_artifact) -> int:
        del artifact, gcs_artifact
        raise NotImplementedError(
            "PostgreSQL не поддерживает прямую загрузку из GCS. GCS экспорт работает только для BigQuery sink."
        )

    def drop(self, artifact: StagingTableArtifact) -> None:
        drop_sql = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(artifact.schema),
            sql.Identifier(artifact.table),
        )
        self.connector.execute_query(drop_sql)
        self.logger.log_etl_progress("STAGING_DROPPED", {"Table": artifact.qualified_name()})

    def _generate_table_name(self, base: str) -> str:
        suffix = uuid.uuid4().hex[:8]
        return f"stg_{base}_{suffix}"

    def _count_rows(self, artifact: StagingTableArtifact) -> int:
        count_sql = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(artifact.schema),
            sql.Identifier(artifact.table),
        )
        result = self.connector.get_records(count_sql)
        return result[0][0] if result else 0

    def _convert_value_for_postgres(self, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, list):

            def escape_array_elem(elem):
                if elem is None:
                    return "NULL"
                if isinstance(elem, dict | list):
                    s = json.dumps(elem, ensure_ascii=False)
                else:
                    s = str(elem)
                if any(c in s for c in ['"', "\\", "{", "}", ","]):
                    s = s.replace("\\", "\\\\").replace('"', '\\"')
                    return f'"{s}"'
                return s

            elements = [escape_array_elem(e) for e in value]
            return "{" + ",".join(elements) + "}"
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        return value

    def _rows_to_csv(self, rows: Iterable[Mapping[str, object]], columns: Sequence[str]):
        for row in rows:
            buffer = io.StringIO()
            writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
            converted_row = [self._convert_value_for_postgres(row.get(col)) for col in columns]
            writer.writerow(converted_row)
            yield buffer.getvalue()
