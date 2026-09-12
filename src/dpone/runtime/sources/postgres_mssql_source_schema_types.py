"""Runtime boundary for selected-relation source-schema authority types.

Issuer, projection, and prepared-boundary owners import these types here so
the contracts package stays one explicit dependency instead of three
repeated cross-layer edges.
"""

from dpone.contracts.postgres_mssql_source_schema_authority import (
    PostgresMssqlSelectedRelationSchemaAuthorityV1,
    PostgresMssqlSourceSchemaAuthorityErrorV1,
    derive_observed_source_column,
    require_issuer_type_policy,
    translate_issuer_failure,
)

__all__ = [
    "PostgresMssqlSelectedRelationSchemaAuthorityV1",
    "PostgresMssqlSourceSchemaAuthorityErrorV1",
    "derive_observed_source_column",
    "require_issuer_type_policy",
    "translate_issuer_failure",
]
