"""SQL Server source implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.runtime.internal_query_capability import InternalQueryCapabilityDecision
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.source_protocol import AbstractSource
from dpone.runtime.sources.strategies.base import SourceStrategy
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy, MSSQLIncrementalExtractStrategy

# Preserve the historical helper import as an identity re-export.
from dpone.runtime.sources.strategies.mssql.mssql_incremental import (
    assert_target_max_mssql_cursor_supported as assert_target_max_mssql_cursor_supported,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.mssql_connector import MSSQLConnectorPort


class MSSQLSource(AbstractSource):
    """Source for SQL Server tables.

    Supports full refresh and incremental loads by an explicit user-owned
    ``incremental_column``. SQL Server has no PostgreSQL-like xmin, so
    incremental MSSQL source config must opt into a column-based state model.
    """

    target_max_cursor_source_type = "mssql"

    def __init__(
        self,
        connector: MSSQLConnectorPort,
        logger,
        sink_connector: Any = None,
        internal_query_capability: InternalQueryCapabilityDecision | None = None,
    ):
        self.connector = connector
        self.sink_connector = sink_connector
        self.logger = logger
        self.internal_query_capability = internal_query_capability or InternalQueryCapabilityDecision.not_issued(
            source_dialect="mssql"
        )
        full = MSSQLFullExtractStrategy(
            connector,
            logger,
            sink_connector=sink_connector,
            internal_query_capability=self.internal_query_capability,
        )
        incremental = MSSQLIncrementalExtractStrategy(
            connector,
            logger,
            sink_connector=sink_connector,
            internal_query_capability=self.internal_query_capability,
        )
        self._incremental_extract = incremental
        self._strategy_map: dict[LoadStrategy, SourceStrategy] = {
            LoadStrategy.FULL_REFRESH: full,
            LoadStrategy.REPLACE: full,
            LoadStrategy.PARTITION_REPLACE: full,
            LoadStrategy.INCREMENTAL_APPEND: incremental,
            LoadStrategy.INCREMENTAL_MERGE: incremental,
            # Backfill extraction is a full scan bounded by the chunk predicate
            # (options.source_custom_predicate injected by the orchestrator).
            LoadStrategy.BACKFILL: full,
        }

    def bind_internal_query_capability(self, decision: InternalQueryCapabilityDecision) -> None:
        """Bind one post-hydration decision to every MSSQL source strategy."""

        self.internal_query_capability = decision
        for strategy in set(self._strategy_map.values()):
            binder = getattr(strategy, "bind_internal_query_capability", None)
            if callable(binder):
                binder(decision)

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
            raise ValueError(f"Unsupported MSSQL load strategy: {load_config.load_strategy.value}")
        if strategy is self._incremental_extract:
            return MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE
        return MssqlTransactionCheckpointMode.STATELESS

    def _resolve_strategy(self, load_config: LoadConfig) -> SourceStrategy:
        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(f"Unsupported MSSQL load strategy: {load_config.load_strategy.value}")
        if strategy is self._incremental_extract:
            MSSQLIncrementalExtractStrategy.require_source_route_safe(load_config, self)
        return strategy
