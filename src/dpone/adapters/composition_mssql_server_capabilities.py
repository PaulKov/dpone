"""Compatibility exports for SQL Server catalog capability detection."""

from dpone.adapters.composition_mssql_catalog_query import (
    ledger_catalog_projections as ledger_catalog_projections,
)
from dpone.adapters.composition_mssql_catalog_query import (
    require_database_collation as require_database_collation,
)
from dpone.adapters.composition_mssql_catalog_query import supports_ledger_catalog as supports_ledger_catalog

__all__ = ("ledger_catalog_projections", "require_database_collation", "supports_ledger_catalog")
