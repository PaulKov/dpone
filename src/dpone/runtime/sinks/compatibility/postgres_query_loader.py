"""Загрузка PostgreSQL sink из InternalQueryArtifact."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager


class PostgresInternalQueryLoader:
    """Compatibility adapter for direct internal-query loader callers."""

    def __init__(
        self,
        connector: Any,
        logger: ETLLogger | None,
        target_table_manager: PostgresTargetTableManager,
        log_target_sample: Callable[[Any, int], None],
    ) -> None:
        self.connector = connector
        self.logger = logger or etl_logger
        self.target_table_manager = target_table_manager
        self._log_target_sample = log_target_sample

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        """Preserve the historical call signature through the strategy entry point."""
        from dpone.runtime.sinks.postgres import PostgresSink

        return PostgresSink(self.connector, None, self.logger).load(load_config, payload)
