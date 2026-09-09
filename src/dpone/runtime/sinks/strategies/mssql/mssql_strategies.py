"""Compatibility facade for MSSQL sink strategies."""

from __future__ import annotations

from dpone.runtime.sinks.strategies.mssql.mssql_load_strategies import (
    MSSQLFullRefreshStrategy,
    MSSQLIncrementAppendStrategy,
    MSSQLIncrementMergeStrategy,
    MSSQLPartitionReplaceStrategy,
    MSSQLReplaceStrategy,
    MSSQLStrategyBase,
)

__all__ = [
    "MSSQLStrategyBase",
    "MSSQLFullRefreshStrategy",
    "MSSQLIncrementAppendStrategy",
    "MSSQLIncrementMergeStrategy",
    "MSSQLPartitionReplaceStrategy",
    "MSSQLReplaceStrategy",
]
