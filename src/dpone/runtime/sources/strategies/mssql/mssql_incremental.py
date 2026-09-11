"""Incremental SQL Server source extraction strategy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.contracts.target_max_incremental_cursor import assert_target_max_mssql_cursor_supported
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.mssql.mssql_base import MSSQLBaseExtractStrategy
from dpone.runtime.sources.strategies.mssql.mssql_csv_artifacts import build_csv_file_artifact
from dpone.runtime.sources.strategies.mssql.mssql_schema_access import get_max_column_value, prune_schema
from dpone.runtime.sources.strategies.mssql.mssql_schema_access import source_database as _source_database
from dpone.type_system.source_sink.provenance import SourceRelationDialect

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.source_cursor_admission import SourceCursorRouteBinding


class MSSQLIncrementalExtractStrategy(MSSQLBaseExtractStrategy):
    """Incremental extract using a user-owned monotonic column."""

    @staticmethod
    def require_source_route_safe(load_config: LoadConfig, binding: SourceCursorRouteBinding) -> None:
        """Check current facade inputs before invoking the selected strategy.

        Keep options/binding evaluation order. Direct entrypoints retain their
        independent checks and their own sink binding; no admission is cached.
        """
        options = getattr(load_config, "options", {}) or {}
        assert_target_max_mssql_cursor_supported(
            source_type=binding.target_max_cursor_source_type,
            configured_sink=options.get("sink_type") or options.get("target_type"),
            sink_connector=binding.sink_connector,
        )

    def get_state(self, load_config: LoadConfig) -> dict[str, Any] | None:
        self._assert_route_safe(load_config)
        incremental_column = load_config.options.get("incremental_column")
        if not incremental_column:
            return None
        sink = self.sink_connector or self.connector
        if not hasattr(sink, "get_max_column_value"):
            return None
        try:
            max_value = get_max_column_value(
                sink,
                load_config.target_schema,
                load_config.target_table,
                incremental_column,
                database=_source_database(load_config.target_schema, load_config.target_database),
            )
        except Exception:  # noqa: BLE001 - missing target on first run is a cold start, not a hard failure
            return None
        if max_value is None:
            return None
        return {"last_value": max_value, "column": incremental_column}

    def extract(self, load_config: LoadConfig, last_state: dict[str, Any] | None) -> ExtractResult:
        self._assert_route_safe(load_config)
        incremental_column = load_config.options.get("incremental_column")
        schema = prune_schema(self.fetch_schema(load_config), load_config.options.get("columns"))
        columns = [column for column, _ in schema]
        if not incremental_column or not last_state or last_state.get("last_value") is None:
            query = self._select_query(load_config, columns, load_config.options.get("source_custom_predicate"))
            artifact = self._artifact_for_query(load_config, query, schema)
            return ExtractResult(
                artifact=artifact,
                schema=self._output_schema(load_config, schema),
                relation_schema=schema,
                relation_dialect=SourceRelationDialect.MSSQL,
                state=None,
                force_full_refresh=True,
            )

        predicate = f"{self.connector.quote_identifier(incremental_column)} > ?"
        if load_config.options.get("source_custom_predicate"):
            predicate = f"({predicate}) AND ({load_config.options['source_custom_predicate']})"
        query = self._select_query(load_config, columns, predicate)
        params = (last_state["last_value"],)
        if self.queryout_artifacts.uses_csv_file_export(load_config):
            artifact = build_csv_file_artifact(
                self.connector,
                load_config,
                query,
                schema,
                params=params,
                binary_encoding=("base64" if self.queryout_artifacts.targets_bigquery(load_config) else "postgres_hex"),
            )
        else:
            artifact = self.queryout_artifacts.streaming_artifact(
                query,
                params=params,
                batch_size=load_config.batch_size,
            )
        new_value = get_max_column_value(
            self.connector,
            load_config.source_schema,
            load_config.source_table,
            incremental_column,
            database=_source_database(load_config.source_schema, load_config.source_database),
        )
        return ExtractResult(
            artifact=artifact,
            schema=self._output_schema(load_config, schema),
            relation_schema=schema,
            relation_dialect=SourceRelationDialect.MSSQL,
            state={"last_value": new_value or last_state["last_value"], "column": incremental_column},
        )

    def _assert_route_safe(self, load_config: LoadConfig) -> None:
        options = getattr(load_config, "options", {}) or {}
        assert_target_max_mssql_cursor_supported(
            source_type="mssql",
            configured_sink=options.get("sink_type") or options.get("target_type"),
            sink_connector=self.sink_connector,
        )


__all__ = ["MSSQLIncrementalExtractStrategy"]
