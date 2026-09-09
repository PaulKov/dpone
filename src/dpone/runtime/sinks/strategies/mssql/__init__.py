"""MSSQL sink strategies."""

from dpone.runtime.sinks.strategies.mssql.mssql_production import MSSQLSCD2Strategy, MSSQLSnapshotDiffStrategy
from dpone.runtime.sinks.strategies.mssql.mssql_strategies import (
    MSSQLFullRefreshStrategy,
    MSSQLIncrementAppendStrategy,
    MSSQLIncrementMergeStrategy,
    MSSQLPartitionReplaceStrategy,
    MSSQLReplaceStrategy,
    MSSQLStrategyBase,
)

__all__ = [
    "MSSQLFullRefreshStrategy",
    "MSSQLIncrementAppendStrategy",
    "MSSQLIncrementMergeStrategy",
    "MSSQLPartitionReplaceStrategy",
    "MSSQLReplaceStrategy",
    "MSSQLSCD2Strategy",
    "MSSQLSnapshotDiffStrategy",
    "MSSQLStrategyBase",
]
