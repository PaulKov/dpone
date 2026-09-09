"""PostgreSQL sink strategy composition boundary."""

from __future__ import annotations

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


@dataclass(frozen=True)
class PostgresSinkComposition:
    """Runtime collaborators assembled for a PostgreSQL sink instance."""

    staging_manager: PostgresStagingManager
    strategy_map: dict[LoadStrategy, SinkStrategy]


class PostgresSinkCompositionFactory:
    """Build PostgreSQL sink collaborators behind a narrow DI boundary."""

    def build(self, connector: Any, logger: ETLLogger) -> PostgresSinkComposition:
        """Create the staging manager and strategy registry for one sink."""

        staging_manager = PostgresStagingManager(connector, logger)
        strategy_map: dict[LoadStrategy, SinkStrategy] = {
            LoadStrategy.FULL_REFRESH: PostgresFullRefreshStrategy(connector, logger, staging_manager),
            LoadStrategy.INCREMENTAL_MERGE: PostgresIncrementMergeStrategy(connector, logger, staging_manager),
            LoadStrategy.INCREMENTAL_APPEND: PostgresIncrementAppendStrategy(connector, logger, staging_manager),
            LoadStrategy.REPLACE: PostgresReplaceStrategy(connector, logger, staging_manager),
            LoadStrategy.PARTITION_REPLACE: PostgresPartitionReplaceStrategy(connector, logger, staging_manager),
            LoadStrategy.SNAPSHOT_DIFF: PostgresSnapshotDiffStrategy(connector, logger, staging_manager),
            LoadStrategy.SCD2: PostgresSCD2Strategy(connector, logger, staging_manager),
        }
        strategy_map[LoadStrategy.BACKFILL] = BackfillStrategy(strategy_map)
        return PostgresSinkComposition(staging_manager=staging_manager, strategy_map=strategy_map)


__all__ = ["PostgresSinkComposition", "PostgresSinkCompositionFactory"]
