"""BigQuery sink strategy composition boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sink_logging import ETLLogger
from dpone.runtime.sinks.staging_managers.bigquery import BigQueryStagingManager
from dpone.runtime.sinks.strategies.backfill import BackfillStrategy
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sinks.strategies.bigquery.bigquery_full_refresh import BigQueryFullRefreshStrategy
from dpone.runtime.sinks.strategies.bigquery.bigquery_increment_append import BigQueryIncrementAppendStrategy
from dpone.runtime.sinks.strategies.bigquery.bigquery_increment_merge import (
    BigQueryIncrementMergeStrategy,
    BigQueryPartitionReplaceStrategy,
)
from dpone.runtime.sinks.strategies.bigquery.bigquery_production import (
    BigQuerySCD2Strategy,
    BigQuerySnapshotDiffStrategy,
)
from dpone.runtime.sinks.strategies.bigquery.bigquery_replace_strategy import BigQueryReplaceStrategy


@dataclass(frozen=True)
class BigQuerySinkComposition:
    """Runtime collaborators assembled for a BigQuery sink instance."""

    staging_manager: BigQueryStagingManager
    strategy_map: dict[LoadStrategy, SinkStrategy]


class BigQuerySinkCompositionFactory:
    """Build BigQuery sink collaborators behind a narrow DI boundary."""

    def build(self, connector: Any, logger: ETLLogger) -> BigQuerySinkComposition:
        """Create the staging manager and strategy registry for one sink."""

        staging_manager = BigQueryStagingManager(connector, logger)
        strategy_map: dict[LoadStrategy, SinkStrategy] = {
            LoadStrategy.FULL_REFRESH: BigQueryFullRefreshStrategy(connector, logger),
            LoadStrategy.INCREMENTAL_MERGE: BigQueryIncrementMergeStrategy(connector, logger),
            LoadStrategy.INCREMENTAL_APPEND: BigQueryIncrementAppendStrategy(connector, logger),
            LoadStrategy.REPLACE: BigQueryReplaceStrategy(connector, logger),
            LoadStrategy.PARTITION_REPLACE: BigQueryPartitionReplaceStrategy(connector, logger),
            LoadStrategy.SNAPSHOT_DIFF: BigQuerySnapshotDiffStrategy(connector, logger),
            LoadStrategy.SCD2: BigQuerySCD2Strategy(connector, logger),
        }
        strategy_map[LoadStrategy.BACKFILL] = BackfillStrategy(strategy_map)
        return BigQuerySinkComposition(staging_manager=staging_manager, strategy_map=strategy_map)


__all__ = ["BigQuerySinkComposition", "BigQuerySinkCompositionFactory"]
