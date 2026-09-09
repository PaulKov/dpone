"""MSSQL source strategies."""

from dpone.runtime.sources.strategies.mssql.mssql_strategies import (
    MSSQLFullExtractStrategy,
    MSSQLIncrementalExtractStrategy,
)

__all__ = ["MSSQLFullExtractStrategy", "MSSQLIncrementalExtractStrategy"]
