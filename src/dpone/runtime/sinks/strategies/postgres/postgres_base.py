"""Базовые классы для PostgreSQL стратегий."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.sinks.load_payload import LoadPayload


from abc import ABC
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from psycopg import sql

from dpone.contracts.technical_columns import include_technical_columns
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sinks.strategies.postgres.staging_sql_helper import PostgresStagingSqlHelper
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager

_FILE_EXPORT_DELEGATES = frozenset(
    {
        "_load_from_file_export",
        "_load_from_file_export_standard",
        "_load_from_file_export_with_truncate",
        "_load_from_file_export_with_exchange",
    }
)

_TARGET_TABLE_DELEGATES = {
    "_ensure_target_table": "ensure_target_table",
    "_ensure_technical_columns": "ensure_technical_columns",
}

_STAGING_SQL_DELEGATES = {
    "_insert_from_staging": "insert_from_staging",
    "_select_columns": "select_columns",
    "_build_key_condition": "build_key_condition",
    "_get_target_column_types": "get_target_column_types",
    "_build_typed_select": "build_typed_select",
}


class PostgresStrategyBase(SinkStrategy, ABC):
    """Общий базовый класс для стратегий PostgreSQL."""

    def __init__(self, connector, logger: ETLLogger | None, staging_manager):
        self.connector = connector
        self.logger = logger or etl_logger
        self.staging_manager = staging_manager

        self.target_table_manager = PostgresTargetTableManager(
            connector=self.connector,
            logger=self.logger,
            include_technical_columns=self._include_technical_columns,
        )
        self.staging_sql_helper = PostgresStagingSqlHelper(
            connector=self.connector,
            logger=self.logger,
            include_technical_columns=self._include_technical_columns,
            log_target_sample=self._log_target_sample,
        )

    def __getattr__(self, name: str) -> Any:
        if name == "_load_from_internal_query" or name in _FILE_EXPORT_DELEGATES:
            return self._load_legacy_artifact
        if name in _TARGET_TABLE_DELEGATES:
            return getattr(self.target_table_manager, _TARGET_TABLE_DELEGATES[name])
        if name in _STAGING_SQL_DELEGATES:
            return getattr(self.staging_sql_helper, _STAGING_SQL_DELEGATES[name])
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    def _load_legacy_artifact(self, load_config: Any, payload: LoadPayload, artifact: Any = None) -> LoadResult:
        """Keep legacy helpers within the selected strategy and caller transaction."""
        if artifact is not None:
            payload = payload.rebind(artifact=artifact)
        return self.load(load_config, payload)

    def _include_technical_columns(self, load_config: Any) -> bool:
        """Whether to ensure/populate technical columns __dpone__loaded_at/__dpone__deleted_at.

        UX:
            Prefer ``sink.options.technical_columns`` with tri-state values:
            - required | optional | forbidden

        Backward compatibility:
            ``sink.options.include_technical_columns`` is still supported.
        """
        opts = getattr(load_config, "options", {}) or {}
        return include_technical_columns(opts)

    def _consume_with_staging(
        self,
        load_config: Any,
        payload: LoadPayload,
        handler: Callable[[StagingTableArtifact], LoadResult],
    ) -> LoadResult:
        # Transport materializes rows; only the selected handler chooses write semantics.
        staging_artifact = payload.artifact.materialize(
            self.staging_manager,
            load_config,
            payload.schema,
        )
        try:
            result = handler(staging_artifact)
        except BaseException as primary_error:
            try:
                staging_artifact.cleanup()
            except BaseException as cleanup_error:
                add_note = getattr(primary_error, "add_note", None)
                if callable(add_note):
                    add_note(f"staging cleanup failed: {type(cleanup_error).__name__}")
            raise
        staging_artifact.cleanup()
        return replace(result, staging_rows=staging_artifact.row_count)

    def _log_target_sample(self, load_config: Any, max_rows: int = 5) -> None:
        """Логирует sample данных из целевой таблицы."""
        query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
        )
        # A failed SQL query aborts the transaction; preserve its diagnostic.
        rows = self.connector.get_records(query, (max_rows,), as_dict=True)
        try:
            if rows:
                self.logger.log_data_sample("TARGET", rows, max_rows)
        except Exception:
            pass
