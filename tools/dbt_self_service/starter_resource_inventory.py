"""Immutable inventories belonging to persisted starter evidence versions.

Historical evidence must never inherit the installed package's evolving list.
The dependency files remain last in each version's default writer ordering;
readers retain the ordering recorded by the individual manifest.
"""

LEGACY_RESOURCE_PATHS = (
    "src/dpone/_assets/dbt_dpone/dbt_project.yml",
    "src/dpone/_assets/dbt_dpone/INSTALL.md",
    "src/dpone/_assets/dbt_dpone/macros/dpone_publish.sql",
    "src/dpone/_assets/dbt_dpone/macros/semantic_refresh_restore.sql",
    "src/dpone/_assets/dbt_dpone/macros/semantic_refresh_scope_merge.sql",
    "src/dpone/_assets/dbt_dpone/macros/materializations/mssql_managed_table.sql",
    "src/dpone/_assets/dbt_dpone/macros/physical/mssql_admission.sql",
    "src/dpone/_assets/dbt_dpone/macros/physical/mssql_candidate.sql",
    "src/dpone/_assets/dbt_dpone/macros/physical/mssql_catalog.sql",
    "src/dpone/_assets/dbt_dpone/macros/physical/mssql_receipt.sql",
    "src/dpone/_assets/dbt_dpone/control/sqlserver/physical-v1/schema.sql",
    "src/dpone/_assets/dbt_dpone/control/sqlserver/physical-v1/admission.sql",
    "src/dpone/_assets/dbt_dpone/control/sqlserver/physical-v1/catalog.sql",
    "src/dpone/_assets/dbt_dpone/control/sqlserver/physical-v1/receipt.sql",
    "src/dpone/_assets/dbt_starter/v4/packages.yml",
    "src/dpone/_assets/dbt_starter/v4/package-lock.yml",
)
RESOURCE_PATHS = (
    *LEGACY_RESOURCE_PATHS[:-2],
    "src/dpone/_assets/dbt_dpone/control/sqlserver/physical-v1/catalog-v2.sql",
    *LEGACY_RESOURCE_PATHS[-2:],
)


def evidence_paths(schema: object, kind: str) -> tuple[str, ...]:
    """Resolve only the two declared versions of the requested evidence kind."""
    if kind not in {"transaction", "recovery"}:
        raise ValueError("Unknown starter evidence kind.")
    if schema == f"dpone.starter-resource-{kind}.v1":
        return LEGACY_RESOURCE_PATHS
    if schema == f"dpone.starter-resource-{kind}.v2":
        return RESOURCE_PATHS
    raise ValueError("Unknown starter evidence schema.")
