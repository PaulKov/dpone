"""Runtime load-strategy policy helpers."""

from __future__ import annotations

from dpone.config.load_strategy import LoadStrategy

STATEFUL_EXTRACT_STRATEGIES: frozenset[LoadStrategy] = frozenset(
    {
        LoadStrategy.INCREMENTAL_MERGE,
        LoadStrategy.INCREMENTAL_APPEND,
        LoadStrategy.SNAPSHOT_DIFF,
        LoadStrategy.SCD2,
        LoadStrategy.CDC_APPLY,
        LoadStrategy.BACKFILL,
    }
)

LOAD_ELIGIBLE_STRATEGIES: frozenset[LoadStrategy] = frozenset(
    {
        LoadStrategy.INCREMENTAL_MERGE,
        LoadStrategy.INCREMENTAL_APPEND,
        LoadStrategy.FULL_REFRESH,
        LoadStrategy.REPLACE,
        LoadStrategy.PARTITION_REPLACE,
        LoadStrategy.SNAPSHOT_DIFF,
        LoadStrategy.SCD2,
        LoadStrategy.CDC_APPLY,
        LoadStrategy.BACKFILL,
    }
)


def should_load_incremental_state(strategy: LoadStrategy) -> bool:
    """Return whether a source should receive previously saved state."""

    return strategy in STATEFUL_EXTRACT_STRATEGIES


def is_load_eligible_strategy(strategy: LoadStrategy) -> bool:
    """Return whether a strategy should execute the sink load phase."""

    return strategy in LOAD_ELIGIBLE_STRATEGIES


__all__ = [
    "LOAD_ELIGIBLE_STRATEGIES",
    "STATEFUL_EXTRACT_STRATEGIES",
    "is_load_eligible_strategy",
    "should_load_incremental_state",
]
