"""Базовый класс для PostgreSQL стратегий извлечения."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from psycopg import sql

from dpone.runtime.extraction_lifecycle import ExtractionClock
from dpone.runtime.internal_query_capability import (
    INTERNAL_QUERY_NOT_ISSUED,
    InternalQueryCapabilityDecision,
    log_internal_query_capability,
)
from dpone.runtime.sources.strategies.base import SourceStrategy
from dpone.runtime.sources.strategies.postgres.postgres_extraction_lifecycle_mixin import (
    PostgresExtractionLifecycleMixin,
)
from dpone.runtime.sources.strategies.postgres.postgres_file_export_mixin import PostgresFileExportMixin
from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import (
    PostgresFetchedSchema,
    column_provenance,
    custom_type_identity,
)
from dpone.runtime.support.postgres_mssql_projection import project_postgres_mssql_relation
from dpone.type_system.source_sink.postgres_bigquery import PostgresBigQueryTypeMapper
from dpone.type_system.source_sink.postgres_clickhouse import PostgresClickHouseTypeMapper
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper

if TYPE_CHECKING:
    from dpone.runtime.streaming_rows import StreamingRowsArtifact


class PostgresBaseStrategy(PostgresExtractionLifecycleMixin, PostgresFileExportMixin, SourceStrategy):
    """
    Базовый класс для всех PostgreSQL стратегий.

    Предоставляет общие методы:
    - fetch_schema() - получение схемы из information_schema
    - format_select_query() - построение SELECT запросов с psycopg.sql
    """

    def __init__(
        self,
        connector,
        logger,
        *,
        internal_query_capability: InternalQueryCapabilityDecision | None = None,
        extraction_clock: ExtractionClock | None = None,
    ):
        self.connector = connector
        self.logger = logger
        self.internal_query_capability = internal_query_capability or InternalQueryCapabilityDecision.not_issued(
            source_dialect="postgres"
        )
        self._extraction_clock = extraction_clock
        self._postgres_source_authority_verifier: Any | None = None
        self._bigquery_type_mapper = PostgresBigQueryTypeMapper()
        self._mssql_type_mapper = PostgresMssqlTypeMapper()
        self._clickhouse_type_mapper = PostgresClickHouseTypeMapper()

    def bind_internal_query_capability(self, decision: InternalQueryCapabilityDecision) -> None:
        """Bind one composition-root decision to this hydrated strategy."""

        self.internal_query_capability = decision

    def bind_postgres_source_authority(self, verifier: Any) -> None:
        """Bind the strict RR source verifier selected at hydration."""

        if verifier is None or not callable(getattr(verifier, "verify_snapshot", None)):
            raise ValueError("postgres_source_authority.verifier_required")
        current = self._postgres_source_authority_verifier
        if current is not None and current is not verifier:
            raise ValueError("postgres_source_authority.verifier_already_bound")
        self._postgres_source_authority_verifier = verifier

    def _verify_postgres_source_authority(
        self,
        snapshot_lease: Any,
        load_config: Any,
    ) -> Any:
        """Verify the signed source on the exact branded RR session."""

        verifier = self._postgres_source_authority_verifier
        if verifier is None:
            if self._targets_mssql(load_config):
                raise RuntimeError("mssql_transaction.postgres_source_authority_verifier_required")
            return None
        return verifier.verify_snapshot(
            connector=self.connector,
            snapshot_lease=snapshot_lease,
            load_config=load_config,
        )

    def _internal_query_authorized(self, load_config: Any) -> bool:
        decision = self.internal_query_capability
        if decision.diagnostic.code == INTERNAL_QUERY_NOT_ISSUED:
            target_dialect = str((getattr(load_config, "options", {}) or {}).get("sink_type", ""))
            decision = InternalQueryCapabilityDecision.not_issued(
                source_dialect="postgres",
                target_dialect=target_dialect,
            )
        authorized = decision.authorizes(
            source_connector=self.connector,
            dialect="postgres",
            source_database=getattr(load_config, "source_database", None),
            source_schema=str(getattr(load_config, "source_schema", "")),
            source_table=str(getattr(load_config, "source_table", "")),
        )
        log_internal_query_capability(self.logger, decision)
        return authorized

    def fetch_schema(self, load_config):
        """
        Получает схему таблицы из information_schema.

        Returns:
            List[Tuple[str, str]]: Список (column_name, data_type)
        """
        return list(self.fetch_schema_projection(load_config).projected_schema)

    def fetch_schema_projection(self, load_config: Any) -> PostgresFetchedSchema:
        """Fetch source provenance and derive a contract-approved sink schema."""

        query = sql.SQL(
            """
            SELECT
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.datetime_precision,
                c.interval_type,
                c.interval_precision,
                c.character_set_name,
                c.collation_schema,
                c.collation_name,
                c.udt_schema,
                c.udt_name,
                c.domain_schema,
                c.domain_name,
                c.is_nullable,
                c.column_default,
                c.is_identity,
                c.identity_generation,
                c.is_generated,
                c.generation_expression,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS catalog_declared_type,
                t.typtype AS udt_kind,
                t.typcategory AS udt_category
            FROM information_schema.columns AS c
            INNER JOIN pg_catalog.pg_namespace AS rn
              ON rn.nspname = c.table_schema
            INNER JOIN pg_catalog.pg_class AS r
              ON r.relnamespace = rn.oid AND r.relname = c.table_name
            INNER JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = r.oid
             AND a.attname = c.column_name
             AND a.attnum > 0
             AND NOT a.attisdropped
            INNER JOIN pg_catalog.pg_type AS t
              ON t.oid = a.atttypid
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
            """
        )
        rows = self.connector.get_records(
            query,
            (load_config.source_schema, load_config.source_table),
            as_dict=True,
        )
        relation_metadata = tuple(column_provenance(row, self._format_type(row)) for row in rows)
        relation_schema = tuple((column.name, column.declared_type) for column in relation_metadata)
        if self._targets_mssql(load_config):
            projection = project_postgres_mssql_relation(
                relation_schema,
                load_config,
                relation_metadata=relation_metadata,
            )
            return PostgresFetchedSchema(
                relation_schema=projection.relation_schema,
                projected_schema=projection.projected_schema,
                relation_metadata=relation_metadata,
                target_projection=projection,
            )
        return PostgresFetchedSchema(
            relation_schema=relation_schema,
            projected_schema=tuple(self._schema_for_sink(load_config, list(relation_schema))),
            relation_metadata=relation_metadata,
            target_projection=None,
        )

    def _schema_for_sink(self, load_config: Any, schema: list[tuple[str, str]]) -> list[tuple[str, str]]:
        """Map PostgreSQL information_schema types to sink DDL when a pair profile exists."""

        if self._targets_bigquery(load_config):
            return [(name, self._bigquery_type_mapper.resolve(dtype).target_type) for name, dtype in schema]
        if self._targets_mssql(load_config):
            return [(name, self._mssql_type_mapper.resolve(dtype).target_type) for name, dtype in schema]
        if self._targets_clickhouse(load_config):
            return [(name, self._clickhouse_type_mapper.resolve(dtype).target_type) for name, dtype in schema]
        return schema

    def _targets_bigquery(self, load_config: Any) -> bool:
        return self._sink_matches(load_config, tokens=("bigquery", "bq"))

    def _targets_mssql(self, load_config: Any) -> bool:
        return self._sink_matches(load_config, tokens=("mssql", "sqlserver", "sql_server"))

    def _targets_clickhouse(self, load_config: Any) -> bool:
        return self._sink_matches(load_config, tokens=("clickhouse",))

    def _sink_matches(self, load_config: Any, *, tokens: tuple[str, ...]) -> bool:
        hint = str(
            (getattr(load_config, "options", {}) or {}).get("sink_type")
            or (getattr(load_config, "options", {}) or {}).get("target_type")
            or ""
        ).lower()
        if any(token in hint or hint == token for token in tokens):
            return True
        sink = getattr(self, "sink_connector", None)
        if sink is None:
            return False
        type_name = type(sink).__name__.lower()
        module_name = type(sink).__module__.lower()
        return any(token in type_name or token in module_name for token in tokens)

    @staticmethod
    def _format_type(row) -> str:
        data_type = str(row["data_type"]).strip().lower()
        custom = custom_type_identity(row, data_type)
        if custom is not None:
            return custom
        # ``information_schema`` encodes PostgreSQL's signed NUMERIC typmod in
        # an implementation-specific unsigned scale (for example -3 becomes
        # 2045).  It also normalizes several aliases and qualified temporal
        # declarations.  ``pg_catalog.format_type`` is the server authority
        # that reverses ``atttypmod`` without inventing a lossy declaration.
        catalog_declared = str(row.get("catalog_declared_type") or "").strip().lower()
        if catalog_declared:
            return catalog_declared
        if data_type in {"character varying", "varchar", "character", "char"}:
            length = row.get("character_maximum_length")
            return data_type if length in (None, -1) else f"{data_type}({int(length)})"
        if data_type in {"bit", "bit varying"}:
            length = row.get("character_maximum_length")
            return data_type if length in (None, -1) else f"{data_type}({int(length)})"
        if data_type in {"numeric", "decimal"}:
            precision = row.get("numeric_precision")
            scale = row.get("numeric_scale")
            # PostgreSQL unconstrained NUMERIC has no finite precision/scale.
            # Inventing decimal(18,0) here silently rounds or overflows valid
            # source values before native-staging integrity checks.
            if precision is None:
                return data_type
            return f"{data_type}({int(precision)},{int(scale or 0)})"
        if data_type.startswith(("timestamp", "time")):
            precision = row.get("datetime_precision")
            if precision is None:
                return data_type
            head, separator, tail = data_type.partition(" without time zone")
            if not separator:
                head, separator, tail = data_type.partition(" with time zone")
            return f"{head}({int(precision)}){separator}{tail}"
        if data_type == "interval":
            fields = str(row.get("interval_type") or "").strip().lower()
            precision = row.get("interval_precision")
            suffix = f" {fields}" if fields else ""
            return f"interval{suffix}{f'({int(precision)})' if precision is not None else ''}"
        return data_type

    @staticmethod
    def format_select_query(schema: str, table: str, columns: list[str], predicate: str | None = None):
        """
        Формирует SELECT запрос для PostgreSQL используя psycopg.sql.

        Args:
            schema: Имя схемы
            table: Имя таблицы
            columns: Список колонок для выборки
            predicate: WHERE clause (опционально)

        Returns:
            psycopg.sql.Composed объект
        """
        base = sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(", ").join(sql.Identifier(col) for col in columns) if columns else sql.SQL("*"),
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        if predicate:
            base += sql.SQL(" WHERE ") + sql.SQL(predicate)
        return base

    def _render_query(self, connector, query) -> str:
        """Рендерит psycopg.sql.Composed в строку."""
        if hasattr(query, "as_string"):
            return query.as_string(connector.connection)
        return str(query)

    def _build_rows_artifact(
        self,
        connector,
        query,
        schema,
        *,
        params=None,
        batch_size: int | None = None,
    ) -> StreamingRowsArtifact:
        """Создает StreamingRowsArtifact для потоковой передачи данных."""

        resolved_batch_size = batch_size or self.STREAM_BATCH_SIZE
        expected_columns = tuple(name for name, _dtype in schema)
        if connector is not self.connector:
            raise ValueError("postgres_stream_connector_must_match_strategy_session")
        return self._open_repeatable_read_stream(
            query,
            params=params,
            batch_size=resolved_batch_size,
            expected_columns=expected_columns,
        )


__all__ = ["PostgresBaseStrategy", "PostgresFetchedSchema"]
