"""Compatibility facade for SQL Server source extraction strategies."""

from __future__ import annotations

from dpone.runtime.sources.strategies.mssql.mssql_full import MSSQLFullExtractStrategy
from dpone.runtime.sources.strategies.mssql.mssql_incremental import MSSQLIncrementalExtractStrategy

__all__ = ["MSSQLFullExtractStrategy", "MSSQLIncrementalExtractStrategy"]
