"""Exact target-local registry catalog required before MSSQL source routes."""

from __future__ import annotations

from typing import Any

from dpone.runtime.state.mssql_catalog_integrity import (
    MssqlDefaultContract,
    MssqlIndexContract,
    MssqlTableIntegrityContract,
    require_table_catalog_integrity,
)
from dpone.runtime.state.mssql_contract import (
    MssqlColumnShape,
    exact_external_table_contract,
    require_external_table_shape,
)
from dpone.runtime.support.mssql_identifier_integrity import require_case_unambiguous_identifiers
from dpone.runtime.support.mssql_trigger_integrity import (
    require_exact_immutable_trigger,
    require_exact_table_trigger_set,
)

TARGET_IDENTITY_REGISTRY_SCHEMA = "dbo"
TARGET_IDENTITY_REGISTRY_TABLE = "dpone_target_identity"
TARGET_IDENTITY_REGISTRY_TRIGGER = "trg_dpone_target_identity_immutable"

TARGET_IDENTITY_REGISTRY_CONTRACT = exact_external_table_contract(
    columns=frozenset({"binding_id", "schema_name", "table_name", "created_at_utc"}),
    unique_indexes=(("binding_id",), ("schema_name", "table_name")),
    shapes=(
        MssqlColumnShape("binding_id", "uniqueidentifier", 16, None, None, False),
        MssqlColumnShape("schema_name", "nvarchar", 256, None, None, False),
        MssqlColumnShape("table_name", "nvarchar", 256, None, None, False),
        MssqlColumnShape("created_at_utc", "datetime2", 8, None, 7, False),
    ),
)

TARGET_IDENTITY_REGISTRY_INTEGRITY = MssqlTableIntegrityContract(
    indexes=(
        MssqlIndexContract(kind="primary_key", columns=("binding_id",), clustered=True),
        MssqlIndexContract(
            kind="unique_constraint",
            columns=("schema_name", "table_name"),
            clustered=False,
        ),
    ),
    defaults=(MssqlDefaultContract("created_at_utc", "SYSUTCDATETIME()"),),
    exact_indexes=True,
    exact_foreign_keys=True,
    exact_checks=True,
    exact_defaults=True,
)


def require_target_identity_registry_contract(
    connector: Any,
    *,
    database: str,
    database_collation: str,
) -> None:
    """Validate exact columns, physical design, collation and immutability."""

    require_external_table_shape(
        connector,
        database=database,
        schema=TARGET_IDENTITY_REGISTRY_SCHEMA,
        table=TARGET_IDENTITY_REGISTRY_TABLE,
        contract=TARGET_IDENTITY_REGISTRY_CONTRACT,
    )
    require_table_catalog_integrity(
        connector,
        database=database,
        schema=TARGET_IDENTITY_REGISTRY_SCHEMA,
        table=TARGET_IDENTITY_REGISTRY_TABLE,
        contract=TARGET_IDENTITY_REGISTRY_INTEGRITY,
    )
    prefix = f"{connector.quote_identifier(database)}."
    rows = connector.get_records(
        "SELECT c.name AS column_name, c.collation_name "
        f"FROM {prefix}sys.columns AS c "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = c.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ? AND c.name IN ('schema_name', 'table_name')",
        (TARGET_IDENTITY_REGISTRY_SCHEMA, TARGET_IDENTITY_REGISTRY_TABLE),
        as_dict=True,
    )
    column_names = require_case_unambiguous_identifiers(
        (row.get("column_name") for row in rows),
        error_code="mssql_physical_target_registry_identifier_case_ambiguity",
    )
    actual_collations = {
        column_name: str(row.get("collation_name") or "") for row, column_name in zip(rows, column_names, strict=True)
    }
    if actual_collations != {"schema_name": database_collation, "table_name": database_collation}:
        raise RuntimeError("mssql_physical_target_registry_collation_mismatch")
    require_exact_table_trigger_set(
        connector,
        database=database,
        schema=TARGET_IDENTITY_REGISTRY_SCHEMA,
        table=TARGET_IDENTITY_REGISTRY_TABLE,
        triggers=frozenset({TARGET_IDENTITY_REGISTRY_TRIGGER}),
        error_code="mssql_physical_target_registry_trigger_set",
    )
    require_exact_immutable_trigger(
        connector,
        database=database,
        schema=TARGET_IDENTITY_REGISTRY_SCHEMA,
        table=TARGET_IDENTITY_REGISTRY_TABLE,
        trigger=TARGET_IDENTITY_REGISTRY_TRIGGER,
        throw_token="DPONE_TARGET_IDENTITY_IMMUTABLE",
        error_code="mssql_physical_target_registry_immutable_trigger",
    )


__all__ = [
    "TARGET_IDENTITY_REGISTRY_CONTRACT",
    "TARGET_IDENTITY_REGISTRY_INTEGRITY",
    "TARGET_IDENTITY_REGISTRY_SCHEMA",
    "TARGET_IDENTITY_REGISTRY_TABLE",
    "require_target_identity_registry_contract",
]
