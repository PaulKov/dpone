"""Watermark incremental MySQL source extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.contracts.target_max_incremental_cursor import assert_target_max_mssql_cursor_supported
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.mysql.mysql_base import MySQLBaseExtractStrategy

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class MySQLIncrementalExtractStrategy(MySQLBaseExtractStrategy):
    """Incremental extract using an explicit monotonic ``incremental_column``."""

    def get_state(self, load_config: LoadConfig) -> dict[str, Any] | None:
        self._assert_route_safe(load_config)
        incremental_column = load_config.options.get("incremental_column")
        if not incremental_column:
            return None
        sink = self.sink_connector
        if sink is None or not hasattr(sink, "get_max_column_value"):
            return None
        try:
            try:
                max_value = sink.get_max_column_value(
                    load_config.target_schema,
                    load_config.target_table,
                    incremental_column,
                    database=getattr(load_config, "target_database", None),
                )
            except TypeError:
                # Postgres/other sinks use the port signature without database=.
                max_value = sink.get_max_column_value(
                    load_config.target_schema,
                    load_config.target_table,
                    incremental_column,
                )
        except Exception:  # noqa: BLE001 - missing target on first run is a cold start, not a hard failure
            return None
        if max_value is None:
            return None
        return {"last_value": max_value, "column": incremental_column}

    def extract(self, load_config: LoadConfig, last_state: dict[str, Any] | None) -> ExtractResult:
        self._assert_route_safe(load_config)
        incremental_column = load_config.options.get("incremental_column")
        schema = self.fetch_schema(load_config)
        columns = [column for column, _ in schema]
        if not incremental_column:
            raise ValueError(
                "MySQL incremental strategies require source.options.incremental_column "
                "(MySQL has no xmin-style system cursor)."
            )
        if not last_state or last_state.get("last_value") is None:
            query = self._select_query(load_config, columns, load_config.options.get("source_custom_predicate"))
            artifact = self._artifact_for_query(load_config, query, schema)
            return ExtractResult(artifact=artifact, schema=schema, state=None, force_full_refresh=True)

        quoted = self.connector.quote_identifier(incremental_column)
        predicate = f"{quoted} > %s"
        custom = load_config.options.get("source_custom_predicate")
        if custom:
            predicate = f"({predicate}) AND ({custom})"
        query = self._select_query(load_config, columns, predicate)
        artifact = self._artifact_for_query(
            load_config,
            query,
            schema,
            params=(last_state["last_value"],),
        )
        database = load_config.source_database or load_config.source_schema
        new_value = self.connector.get_max_column_value(
            load_config.source_schema,
            load_config.source_table,
            incremental_column,
            database=database,
        )
        return ExtractResult(
            artifact=artifact,
            schema=schema,
            state={"last_value": new_value or last_state["last_value"], "column": incremental_column},
        )

    def _assert_route_safe(self, load_config: LoadConfig) -> None:
        options = getattr(load_config, "options", {}) or {}
        assert_target_max_mssql_cursor_supported(
            source_type="mysql",
            configured_sink=options.get("sink_type") or options.get("target_type"),
            sink_connector=self.sink_connector,
        )


__all__ = ["MySQLIncrementalExtractStrategy"]
