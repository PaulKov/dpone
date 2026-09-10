"""PostgreSQL sink strategy composition boundary."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sink_logging import ETLLogger
from dpone.runtime.sinks.staging_managers.postgres import PostgresStagingManager
from dpone.runtime.sinks.strategies.backfill import BackfillStrategy
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sinks.strategies.postgres.postgres_full_refresh import PostgresFullRefreshStrategy
from dpone.runtime.sinks.strategies.postgres.postgres_increment_append import PostgresIncrementAppendStrategy
from dpone.runtime.sinks.strategies.postgres.postgres_increment_merge import (
    PostgresIncrementMergeStrategy,
    PostgresPartitionReplaceStrategy,
)
from dpone.runtime.sinks.strategies.postgres.postgres_production import (
    PostgresSCD2Strategy,
    PostgresSnapshotDiffStrategy,
)
from dpone.runtime.sinks.strategies.postgres.postgres_replace_strategy import PostgresReplaceStrategy
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager


@dataclass(frozen=True)
class PostgresSinkComposition:
    """Runtime collaborators assembled for a PostgreSQL sink instance."""

    staging_manager: PostgresStagingManager
    strategy_map: dict[LoadStrategy, SinkStrategy]


class PostgresSinkCompositionFactory:
    """Build PostgreSQL sink collaborators behind a narrow DI boundary."""

    def __init__(
        self,
        *,
        target_table_manager: PostgresTargetTableManager | None = None,
        log_target_sample: Callable[[Any, int], None] | None = None,
    ) -> None:
        """Retain caller-owned target policy and sample handling for legacy adapters."""
        self._target_table_manager = target_table_manager
        self._log_target_sample = log_target_sample

    def build(self, connector: Any, logger: ETLLogger) -> PostgresSinkComposition:
        """Create the staging manager and strategy registry for one sink."""

        staging_manager = PostgresStagingManager(connector, logger)
        strategy_types = {
            LoadStrategy.FULL_REFRESH: PostgresFullRefreshStrategy,
            LoadStrategy.INCREMENTAL_MERGE: PostgresIncrementMergeStrategy,
            LoadStrategy.INCREMENTAL_APPEND: PostgresIncrementAppendStrategy,
            LoadStrategy.REPLACE: PostgresReplaceStrategy,
            LoadStrategy.PARTITION_REPLACE: PostgresPartitionReplaceStrategy,
            LoadStrategy.SNAPSHOT_DIFF: PostgresSnapshotDiffStrategy,
            LoadStrategy.SCD2: PostgresSCD2Strategy,
        }
        strategy_map: dict[LoadStrategy, SinkStrategy] = {
            name: strategy_type(
                connector,
                logger,
                staging_manager,
                target_table_manager=self._target_table_manager,
                log_target_sample=self._log_target_sample,
            )
            for name, strategy_type in strategy_types.items()
        }
        strategy_map[LoadStrategy.BACKFILL] = BackfillStrategy(strategy_map)
        return PostgresSinkComposition(staging_manager=staging_manager, strategy_map=strategy_map)


__all__ = ["PostgresSinkComposition", "PostgresSinkCompositionFactory"]
