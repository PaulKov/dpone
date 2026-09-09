"""Compatibility imports for the shared governed MSSQL live fixture.

The implementation lives in :mod:`tools.mssql_stress_governance` so
release tooling and integration tests exercise the same operator boundary.
"""

from tools.mssql_stress_governance import (
    STATE_SCHEMA,
    GovernedEtlResult,
    GovernedMssqlCampaign,
    GovernedMssqlRoute,
    GovernedPostgresSnapshotSource,
    GovernedRunContext,
    GovernedStandardEtlRunner,
    bind_factual_mssql_database_authority,
    bind_factual_postgres_source_authority,
    factual_postgres_source_authority,
    governed_mssql_campaign,
    governed_mssql_route,
)

__all__ = [
    "STATE_SCHEMA",
    "GovernedEtlResult",
    "GovernedMssqlCampaign",
    "GovernedMssqlRoute",
    "GovernedPostgresSnapshotSource",
    "GovernedRunContext",
    "GovernedStandardEtlRunner",
    "bind_factual_mssql_database_authority",
    "bind_factual_postgres_source_authority",
    "factual_postgres_source_authority",
    "governed_mssql_campaign",
    "governed_mssql_route",
]
