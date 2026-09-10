"""Compatibility exports for canonical signed database-authority policies."""

from dpone.contracts.governed_database_authority import (
    require_governed_mssql_database_authority,
    require_governed_postgres_source_authority,
)

__all__ = ["require_governed_mssql_database_authority", "require_governed_postgres_source_authority"]
