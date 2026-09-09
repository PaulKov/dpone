"""Базовые классы для PostgreSQL стратегий."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.sinks.load_payload import LoadPayload


from abc import ABC
from collections.abc import Callable
from typing import Any

from psycopg import sql

from dpone.contracts.technical_columns import include_technical_columns
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sinks.strategies.postgres.file_export_loader import PostgresFileExportLoader
from dpone.runtime.sinks.strategies.postgres.internal_query_loader import PostgresInternalQueryLoader
from dpone.runtime.sinks.strategies.postgres.staging_sql_helper import PostgresStagingSqlHelper
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager

_FILE_EXPORT_DELEGATES = {
    "_load_from_file_export": "load",
    "_load_from_file_export_standard": "load_standard",
    "_load_from_file_export_with_truncate": "load_with_truncate",
    "_load_from_file_export_with_exchange": "load_with_exchange",
}

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
        self.file_export_loader = PostgresFileExportLoader(
            connector=self.connector,
            logger=self.logger,
            target_table_manager=self.target_table_manager,
            log_target_sample=self._log_target_sample,
        )
        self.internal_query_loader = PostgresInternalQueryLoader(
            connector=self.connector,
            logger=self.logger,
            target_table_manager=self.target_table_manager,
            log_target_sample=self._log_target_sample,
        )
        self.staging_sql_helper = PostgresStagingSqlHelper(
            connector=self.connector,
            logger=self.logger,
            include_technical_columns=self._include_technical_columns,
            log_target_sample=self._log_target_sample,
        )

    def __getattr__(self, name: str) -> Any:
        if name == "_load_from_internal_query":
            return self.internal_query_loader.load
        if name in _FILE_EXPORT_DELEGATES:
            return getattr(self.file_export_loader, _FILE_EXPORT_DELEGATES[name])
        if name in _TARGET_TABLE_DELEGATES:
            return getattr(self.target_table_manager, _TARGET_TABLE_DELEGATES[name])
        if name in _STAGING_SQL_DELEGATES:
            return getattr(self.staging_sql_helper, _STAGING_SQL_DELEGATES[name])
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

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
        if isinstance(payload.artifact, InternalQueryArtifact):
            return self._load_from_internal_query(load_config, payload)

        # FileExportArtifact (CSV/binary) materializes into staging, then the strategy
        # handler runs (merge/append/truncate/exchange). Do not short-circuit to the
        # DROP+CREATE file-export loader — that broke incremental_merge accumulation
        # for cross-DB routes such as mysql→postgres.
        staging_artifact = payload.artifact.materialize(
            self.staging_manager,
            load_config,
            payload.schema,
        )
        try:
            result = handler(staging_artifact)
            return LoadResult(
                inserted_rows=result.inserted_rows,
                updated_rows=result.updated_rows,
                total_rows=result.total_rows,
                state=result.state,
                staging_rows=staging_artifact.row_count,
            )
        finally:
            staging_artifact.cleanup()

    def _log_target_sample(self, load_config: Any, max_rows: int = 5) -> None:
        """Логирует sample данных из целевой таблицы."""
        try:
            query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
            )
            rows = self.connector.get_records(query, (max_rows,), as_dict=True)
            if rows:
                self.logger.log_data_sample("TARGET", rows, max_rows)
        except Exception:
            pass
