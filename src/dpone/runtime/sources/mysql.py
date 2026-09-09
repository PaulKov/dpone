"""MySQL source implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.target_max_incremental_cursor import assert_target_max_mssql_cursor_supported
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.source_protocol import AbstractSource
from dpone.runtime.sources.strategies.base import SourceStrategy
from dpone.runtime.sources.strategies.mysql import MySQLFullExtractStrategy, MySQLIncrementalExtractStrategy

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.mysql_connector import MySQLConnectorPort


class MySQLSource(AbstractSource):
    """Source for MySQL tables.

    Supports full refresh and watermark incremental loads via an explicit
    ``incremental_column``. Binlog CDC is out of scope for v1.
    """

    target_max_cursor_source_type = "mysql"

    def __init__(self, connector: MySQLConnectorPort, logger, sink_connector: Any = None):
        self.connector = connector
        self.sink_connector = sink_connector
        self.logger = logger
        full = MySQLFullExtractStrategy(connector, logger, sink_connector=sink_connector)
        incremental = MySQLIncrementalExtractStrategy(connector, logger, sink_connector=sink_connector)
        self._incremental_extract = incremental
        self._strategy_map: dict[LoadStrategy, SourceStrategy] = {
            LoadStrategy.FULL_REFRESH: full,
            LoadStrategy.REPLACE: full,
            LoadStrategy.PARTITION_REPLACE: full,
            LoadStrategy.INCREMENTAL_APPEND: incremental,
            LoadStrategy.INCREMENTAL_MERGE: incremental,
            LoadStrategy.BACKFILL: full,
            LoadStrategy.SNAPSHOT_DIFF: full,
            # SCD2 finalization is sink-owned; extract is a full bounded scan.
            LoadStrategy.SCD2: full,
        }

    def get_incremental_state(self, load_config: LoadConfig) -> Any | None:
        return self._resolve_strategy(load_config).get_state(load_config)

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> ExtractResult:
        return self._resolve_strategy(load_config).extract(load_config, last_state)

    def mssql_transaction_checkpoint_mode(
        self,
        load_config: LoadConfig,
    ) -> MssqlTransactionCheckpointMode:
        """Reject single-column target MAX while retaining stateless scans."""

        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(f"Unsupported MySQL load strategy: {load_config.load_strategy.value}")
        if strategy is self._incremental_extract:
            return MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE
        return MssqlTransactionCheckpointMode.STATELESS

    def _resolve_strategy(self, load_config: LoadConfig) -> SourceStrategy:
        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(f"Unsupported MySQL load strategy: {load_config.load_strategy.value}")
        if strategy is self._incremental_extract:
            options = getattr(load_config, "options", {}) or {}
            assert_target_max_mssql_cursor_supported(
                source_type=self.target_max_cursor_source_type,
                configured_sink=options.get("sink_type") or options.get("target_type"),
                sink_connector=self.sink_connector,
            )
        return strategy


__all__ = ["MySQLSource"]
