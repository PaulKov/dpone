"""Deprecated compatibility shim for MSSQL load strategies.

Use ``dpone.runtime.sinks.strategies.mssql.mssql_strategies`` for public imports or
``dpone.runtime.sinks.strategies.mssql.mssql_load_strategies`` for internal implementation imports.
"""

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
