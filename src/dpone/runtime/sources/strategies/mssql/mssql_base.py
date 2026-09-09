"""Shared SQL Server source extraction strategy support."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.internal_query_capability import InternalQueryCapabilityDecision
from dpone.runtime.sources.strategies.base import SourceStrategy
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.runtime.sources.strategies.mssql.mssql_schema_access import build_select_query, fetch_schema
from dpone.runtime.sources.strategies.mssql.mssql_schema_access import source_database as _source_database

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class MSSQLBaseExtractStrategy(SourceStrategy):
    """Shared query and artifact helpers for SQL Server extraction strategies."""

    def __init__(
        self,
        connector: Any,
        logger: Any,
        sink_connector: Any = None,
        queryout_artifacts: Any = None,
        internal_query_capability: InternalQueryCapabilityDecision | None = None,
    ) -> None:
        self.connector = connector
        self.logger = logger
        self.sink_connector = sink_connector
        self.queryout_artifacts = queryout_artifacts or MSSQLQueryoutArtifactFactory(
            connector=connector,
            logger=logger,
            sink_connector=sink_connector,
            internal_query_capability=internal_query_capability,
        )

    def bind_internal_query_capability(self, decision: InternalQueryCapabilityDecision) -> None:
        """Bind a runtime-issued decision to the artifact factory."""

        binder = getattr(self.queryout_artifacts, "bind_internal_query_capability", None)
        if callable(binder):
            binder(decision)

    def fetch_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        schema = fetch_schema(
            self.connector,
            load_config.source_schema,
            load_config.source_table,
            database=_source_database(load_config.source_schema, load_config.source_database),
        )
        if not schema:
            raise ValueError(
                f"MSSQL source table has no columns: {load_config.source_schema}.{load_config.source_table}"
            )
        return schema

    def _select_query(self, load_config: LoadConfig, columns: list[str], predicate: str | None = None) -> str:
        query = build_select_query(
            self.connector,
            load_config.source_schema,
            load_config.source_table,
            columns,
            database=_source_database(load_config.source_schema, load_config.source_database),
        )
        if predicate:
            query += f" WHERE {predicate}"
        return query

    def _artifact_for_query(self, load_config: LoadConfig, query: str, schema: list[tuple[str, str]]) -> Any:
        return self.queryout_artifacts.artifact_for_query(load_config, query, schema)

    def _partitioned_queryout(
        self,
        load_config: LoadConfig,
        query: str,
        schema: list[tuple[str, str]],
        partitioner: Any,
        bcp_options: Any,
        *,
        artifact_format: str = "mssql-delimited",
        bulk_text_codec: Any | None = None,
    ) -> Any:
        return self.queryout_artifacts.partitioned_queryout(
            load_config,
            query,
            schema,
            partitioner,
            bcp_options,
            artifact_format=artifact_format,
            bulk_text_codec=bulk_text_codec,
        )

    def _resolve_partition_bounds(self, query: str, column: str) -> tuple[Any, Any, int | None, int]:
        return self.queryout_artifacts.resolve_partition_bounds(query, column)

    def _should_encode_for_mssql_sink(self) -> bool:
        return self.queryout_artifacts.should_encode_for_mssql_sink()

    def _should_encode_for_clickhouse_direct(self, load_config: LoadConfig) -> bool:
        return self.queryout_artifacts.should_encode_for_clickhouse_direct(load_config)

    def _wrap_mssql_bulk_text_query(self, query: str, schema: list[tuple[str, str]], codec: Any) -> str:
        return self.queryout_artifacts.wrap_mssql_bulk_text_query(query, schema, codec)

    def _wrap_clickhouse_tabseparated_query(self, query: str, schema: list[tuple[str, str]], codec: Any) -> str:
        return self.queryout_artifacts.wrap_clickhouse_tabseparated_query(query, schema, codec)

    def _output_schema(self, load_config: LoadConfig, schema: list[tuple[str, str]]) -> list[tuple[str, str]]:
        return self.queryout_artifacts.output_schema(load_config, schema)

    def _should_materialize_queryout_projection(self, load_config: LoadConfig, query: str) -> bool:
        return self.queryout_artifacts.should_materialize_queryout_projection(load_config, query)

    def _materialize_queryout_projection(
        self,
        load_config: LoadConfig,
        query: str,
        schema: list[tuple[str, str]],
    ) -> tuple[str, Any]:
        return self.queryout_artifacts.materialize_queryout_projection(load_config, query, schema)


__all__ = ["MSSQLBaseExtractStrategy"]
