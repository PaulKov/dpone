"""Compatibility facade for set-based SQL Server load strategies."""

from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import (
    MSSQLFullRefreshStrategy,
    MSSQLIncrementAppendStrategy,
    MSSQLIncrementMergeStrategy,
    MSSQLPartitionReplaceStrategy,
    MSSQLReplaceStrategy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_strategy_base import MSSQLStrategyBase

__all__ = [
    "MSSQLStrategyBase",
    "MSSQLFullRefreshStrategy",
    "MSSQLIncrementAppendStrategy",
    "MSSQLIncrementMergeStrategy",
    "MSSQLPartitionReplaceStrategy",
    "MSSQLReplaceStrategy",
]
