"""Shared MySQL source extraction helpers."""

from __future__ import annotations

import os
import tempfile
from typing import TYPE_CHECKING, Any

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sources.strategies.base import SourceStrategy
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.type_system.source_sink.mysql_bigquery import MySQLBigQueryTypeMapper
from dpone.type_system.source_sink.mysql_clickhouse import MySQLClickHouseTypeMapper
from dpone.type_system.source_sink.mysql_mssql import MySQLMssqlTypeMapper
from dpone.type_system.source_sink.mysql_postgres import MySQLPostgresTypeMapper

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class MySQLBaseExtractStrategy(SourceStrategy):
    """Shared schema, SELECT, and sink-aware export helpers."""

    def __init__(self, connector: Any, logger: Any, sink_connector: Any = None) -> None:
        self.connector = connector
        self.logger = logger
        self.sink_connector = sink_connector
        self._postgres_type_mapper = MySQLPostgresTypeMapper()
        self._clickhouse_type_mapper = MySQLClickHouseTypeMapper()
        self._bigquery_type_mapper = MySQLBigQueryTypeMapper()
        self._mssql_type_mapper = MySQLMssqlTypeMapper()

    def fetch_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        database = load_config.source_database or load_config.source_schema
        columns = self.connector.get_table_column_types(
            load_config.source_schema,
            load_config.source_table,
            database=database,
        )
        selected = load_config.options.get("columns")
        if selected:
            missing = [str(name) for name in selected if name not in columns]
            if missing:
                raise ValueError("MySQL source.options.columns references unknown columns: " + ", ".join(missing))
            schema = [(str(name), columns[name]) for name in selected]
            if not schema:
                raise ValueError("MySQL source.options.columns resolved to an empty projection")
            return self._schema_for_sink(load_config, schema)
        schema = list(columns.items())
        if not schema:
            raise ValueError(
                f"MySQL source table has no columns: {load_config.source_schema}.{load_config.source_table}"
            )
        return self._schema_for_sink(load_config, schema)

    def _schema_for_sink(self, load_config: LoadConfig, schema: list[tuple[str, str]]) -> list[tuple[str, str]]:
        """Map MySQL COLUMN_TYPE names to sink DDL types when the pair has a profile."""

        if self._targets_postgres(load_config):
            return [(name, self._postgres_type_mapper.resolve(dtype).target_type) for name, dtype in schema]
        if self._targets_clickhouse(load_config):
            return [(name, self._clickhouse_type_mapper.resolve(dtype).target_type) for name, dtype in schema]
        if self._targets_bigquery(load_config):
            return [(name, self._bigquery_type_mapper.resolve(dtype).target_type) for name, dtype in schema]
        if self._targets_mssql(load_config):
            return [(name, self._mssql_type_mapper.resolve(dtype).target_type) for name, dtype in schema]
        return schema

    def _select_query(self, load_config: LoadConfig, columns: list[str], predicate: str | None = None) -> str:
        database = load_config.source_database or load_config.source_schema
        query = self.connector.build_select_query(
            load_config.source_schema,
            load_config.source_table,
            columns,
            database=database,
        )
        if predicate:
            query += f" WHERE {predicate}"
        return query

    def _artifact_for_query(
        self,
        load_config: LoadConfig,
        query: str,
        schema: list[tuple[str, str]],
        *,
        params: tuple[Any, ...] | None = None,
    ) -> FileExportArtifact:
        export_format = self._resolve_export_format(load_config)
        if export_format == "mssql-delimited" and self._targets_postgres(load_config):
            raise ValueError(
                "MySQL export_format=mssql-delimited is incompatible with Postgres sinks; use export_format=csv."
            )
        if export_format == "mssql-delimited" and self._targets_clickhouse(load_config):
            raise ValueError(
                "MySQL export_format=mssql-delimited is incompatible with ClickHouse sinks; "
                "use export_format=csv (produces ClickHouse TabSeparated wire)."
            )
        if export_format == "mssql-delimited" and self._targets_kafka(load_config):
            raise ValueError(
                "MySQL export_format=mssql-delimited is incompatible with Kafka sinks; use export_format=csv."
            )
        if export_format == "mssql-delimited" and self._targets_bigquery(load_config):
            raise ValueError(
                "MySQL export_format=mssql-delimited is incompatible with BigQuery sinks; use export_format=csv."
            )
        if export_format == "clickhouse-tsv" and self._targets_bigquery(load_config):
            raise ValueError(
                "MySQL export_format=clickhouse-tsv is incompatible with BigQuery sinks; use export_format=csv."
            )
        compress = bool(load_config.options.get("compress_export", False) or load_config.compress_export)
        if compress and self._targets_kafka(load_config):
            raise ValueError(
                "MySQL compress_export is incompatible with Kafka sinks; KafkaSink cannot read gzip artifacts."
            )
        suffix = _artifact_suffix(export_format, compress=compress)
        fd, path = tempfile.mkstemp(prefix="dpone-mysql-", suffix=suffix)
        os.close(fd)
        if export_format == "clickhouse-tsv":
            export_stats = self.connector.export_clickhouse_tsv_to_file(
                query,
                path,
                schema,
                params=params,
                compress=compress,
                batch_size=load_config.batch_size or 10000,
            )
            codec = None
        elif export_format == "csv":
            export_stats = self.connector.export_csv_to_file(
                query,
                path,
                schema,
                params=params,
                compress=compress,
                batch_size=load_config.batch_size or 10000,
            )
            codec = None
        else:
            field_terminator = "\t"
            codec = BulkTextCodec(field_terminator=field_terminator)
            export_stats = self.connector.export_mssql_delimited_to_file(
                query,
                path,
                schema,
                params=params,
                compress=compress,
                batch_size=load_config.batch_size or 10000,
                bulk_text_codec=codec,
                field_terminator=field_terminator,
            )
        artifact = FileExportArtifact(
            file_path=path,
            columns=[column for column, _ in schema],
            compressed=compress,
            format=export_format,
            estimated_rows=int(export_stats.get("row_count") or 0),
            rows_exported=int(export_stats.get("row_count") or 0),
            bulk_text_codec=codec,
        )
        return artifact

    def _resolve_export_format(self, load_config: LoadConfig) -> str:
        raw = getattr(load_config, "export_format", None) or load_config.options.get("export_format")
        if raw:
            export_format = str(raw).lower().replace("_", "-")
        elif (
            self._targets_postgres(load_config)
            or self._targets_kafka(load_config)
            or self._targets_bigquery(load_config)
        ):
            export_format = "csv"
        elif self._targets_clickhouse(load_config):
            export_format = "clickhouse-tsv"
        else:
            export_format = "mssql-delimited"
        if self._targets_clickhouse(load_config):
            if export_format in {"csv", "clickhouse-tsv"}:
                return "clickhouse-tsv"
            return export_format
        if export_format not in {"mssql-delimited", "csv"}:
            # Preserve clickhouse-tsv so BigQuery fail-closed can reject it explicitly.
            if self._targets_bigquery(load_config) and export_format == "clickhouse-tsv":
                return export_format
            if (
                self._targets_postgres(load_config)
                or self._targets_kafka(load_config)
                or self._targets_bigquery(load_config)
            ):
                export_format = "csv"
            else:
                export_format = "mssql-delimited"
        return export_format

    def _targets_postgres(self, load_config: LoadConfig) -> bool:
        return self._sink_matches(load_config, tokens=("postgres", "postgresql", "pg"))

    def _targets_clickhouse(self, load_config: LoadConfig) -> bool:
        return self._sink_matches(load_config, tokens=("clickhouse", "ch"))

    def _targets_kafka(self, load_config: LoadConfig) -> bool:
        return self._sink_matches(load_config, tokens=("kafka",))

    def _targets_bigquery(self, load_config: LoadConfig) -> bool:
        return self._sink_matches(load_config, tokens=("bigquery", "bq"))

    def _targets_mssql(self, load_config: LoadConfig) -> bool:
        return self._sink_matches(load_config, tokens=("mssql", "sqlserver", "sql_server"))

    def _sink_matches(self, load_config: LoadConfig, *, tokens: tuple[str, ...]) -> bool:
        hint = str(
            load_config.options.get("sink_type")
            or load_config.options.get("target_type")
            or load_config.options.get("sink")
            or ""
        ).lower()
        if any(token == hint or token in hint for token in tokens if len(token) > 2) or hint in tokens:
            return True
        sink = self.sink_connector
        if sink is None:
            return False
        type_name = type(sink).__name__.lower()
        module_name = type(sink).__module__.lower()
        return any(token in type_name or token in module_name for token in tokens if len(token) > 2)


def _artifact_suffix(export_format: str, *, compress: bool) -> str:
    if export_format == "mssql-delimited":
        return ".bcp.gz" if compress else ".bcp"
    if export_format == "clickhouse-tsv":
        return ".tsv.gz" if compress else ".tsv"
    return ".csv.gz" if compress else ".csv"


__all__ = ["MySQLBaseExtractStrategy"]
