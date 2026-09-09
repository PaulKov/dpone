"""Exact catalog proof fragments for the generic MSSQL v2 migration."""

from __future__ import annotations

import re
from typing import TypeAlias

from dpone.runtime.state.mssql_generic_fence_trigger import render_fence_trigger_body
from dpone.runtime.state.mssql_generic_operation_trigger import render_operation_trigger_body
from dpone.runtime.state.mssql_generic_table_contracts import (
    ATTEMPT_CONTRACT,
    FENCE_CONTRACT,
    OPERATION_CONTRACT,
    RECEIPT_CONTRACT,
)
from dpone.runtime.state.mssql_generic_transaction_migration_relational_ddl import (
    CheckSpec,
    DefaultSpec,
    ForeignKeySpec,
    render_relational_family_proof,
)
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    ATTEMPT_TRIGGER,
    FENCE_TABLE,
    FENCE_TRIGGER,
    GENERIC_TRANSACTION_TABLES,
    OPERATION_TABLE,
    OPERATION_TRIGGER,
    RECEIPT_TABLE,
    RECEIPT_TRIGGER,
)

IndexSpec: TypeAlias = tuple[str, bool, bool, tuple[str, ...]]


def render_migration_common_proof(*, schema: str) -> str:
    """Render the exact v1/v2 common catalog proof before any ALTER."""

    assertions = [
        _render_column_proof(schema=schema, table=FENCE_TABLE, contract=FENCE_CONTRACT),
        _render_column_proof(schema=schema, table=ATTEMPT_TABLE, contract=ATTEMPT_CONTRACT),
        _render_column_proof(schema=schema, table=OPERATION_TABLE, contract=OPERATION_CONTRACT),
        _render_column_proof(schema=schema, table=RECEIPT_TABLE, contract=RECEIPT_CONTRACT),
    ]
    for table, specs in _COMMON_INDEX_SPECS.items():
        assertions.extend(_render_index_proof(schema=schema, table=table, spec=spec) for spec in specs)
    assertions.extend(
        render_relational_family_proof(
            schema=schema,
            table=table,
            foreign_keys=_FOREIGN_KEY_SPECS.get(table, ()),
            checks=_CHECK_SPECS.get(table, ()),
            defaults=_DEFAULT_SPECS.get(table, ()),
        )
        for table in GENERIC_TRANSACTION_TABLES
    )
    assertions.extend(
        _render_trigger_proof(
            schema=schema,
            table=table,
            trigger=trigger,
            action=action,
            body=body,
        )
        for table, trigger, action, body in _trigger_specs()
    )
    table_names = ", ".join(f"N'{_sql_text(name)}'" for name in GENERIC_TRANSACTION_TABLES)
    assertions.insert(
        0,
        f"""    IF SCHEMA_ID(N'{_sql_text(schema)}') IS NULL
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_CATALOG_MISMATCH', 1;
    IF (SELECT COUNT_BIG(*) FROM sys.tables
        WHERE schema_id = SCHEMA_ID(N'{_sql_text(schema)}')
          AND name IN ({table_names})) <> 4
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_CATALOG_MISMATCH', 1;""",
    )
    common_index_count = sum(len(specs) for specs in _COMMON_INDEX_SPECS.values())
    assertions.append(
        f"""    IF (SELECT COUNT_BIG(*) FROM sys.indexes AS i
        INNER JOIN sys.tables AS t ON t.object_id = i.object_id
        WHERE t.schema_id = SCHEMA_ID(N'{_sql_text(schema)}')
          AND t.name IN ({table_names}) AND i.index_id > 0) NOT IN ({common_index_count}, {common_index_count + 1})
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_INDEX_CONTRACT_MISMATCH', 1;
    IF (SELECT COUNT_BIG(*) FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'{_sql_text(f"{_quote(schema)}.{_quote(ATTEMPT_TABLE)}")}')
          AND index_id > 0) NOT IN (3, 4)
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_INDEX_CONTRACT_MISMATCH', 1;"""
    )
    return "\n\n".join(assertions)


def render_invocation_index_proof(*, schema: str) -> str:
    """Render the exact v2 invocation-uniqueness proof."""

    proof = _render_index_proof(
        schema=schema,
        table=ATTEMPT_TABLE,
        spec=("uq_dpone_load_attempt_invocation", False, False, ("invocation_digest",)),
    )
    qualified = _sql_text(f"{_quote(schema)}.{_quote(ATTEMPT_TABLE)}")
    return (
        proof.replace("V1_INDEX", "V2_INDEX")
        + f"""
    IF (SELECT COUNT_BIG(*) FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'{qualified}') AND index_id > 0) <> 4
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V2_INDEX_CONTRACT_MISMATCH', 1;"""
    )


def _render_column_proof(*, schema: str, table: str, contract: object) -> str:
    qualified = _sql_text(f"{_quote(schema)}.{_quote(table)}")
    shapes = tuple(getattr(contract, "shapes"))
    checks = [
        f"""    IF OBJECT_ID(N'{qualified}', N'U') IS NULL
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_CATALOG_MISMATCH', 1;
    IF (SELECT COUNT_BIG(*) FROM sys.columns WHERE object_id = OBJECT_ID(N'{qualified}')) <> {len(shapes)}
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_COLUMN_CONTRACT_MISMATCH', 1;"""
    ]
    for shape in shapes:
        predicates = [
            f"c.name = N'{_sql_text(shape.name)}'",
            f"ty.name = N'{_sql_text(shape.type_name)}'",
            f"c.is_nullable = {int(bool(shape.nullable))}",
        ]
        for field in ("max_length", "precision", "scale", "generated_always_type"):
            expected = getattr(shape, field)
            if expected is not None:
                predicates.append(f"c.{field} = {int(expected)}")
        for field in (
            "is_identity",
            "is_computed",
            "is_sparse",
            "is_rowguidcol",
            "is_hidden",
            "is_masked",
            "is_ansi_padded",
            "is_filestream",
            "is_column_set",
        ):
            expected = getattr(shape, field.removeprefix("is_") if field == "is_identity" else field, None)
            if field == "is_identity":
                expected = shape.identity
            if expected is not None:
                predicates.append(f"c.{field} = {int(bool(expected))}")
        if shape.is_encrypted is not None:
            predicates.append(f"CASE WHEN c.encryption_type IS NULL THEN 0 ELSE 1 END = {int(shape.is_encrypted)}")
        if shape.uses_database_default_collation is not None:
            predicates.append(
                "CASE WHEN c.collation_name IS NULL OR c.collation_name = "
                "CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation')) THEN 1 ELSE 0 END = "
                f"{int(shape.uses_database_default_collation)}"
            )
        if shape.is_user_defined is not None:
            predicates.append(f"ty.is_user_defined = {int(shape.is_user_defined)}")
        if shape.is_assembly_type is not None:
            predicates.append(f"ty.is_assembly_type = {int(shape.is_assembly_type)}")
        if shape.has_bound_rule is not None:
            predicates.append(f"CASE WHEN c.rule_object_id = 0 THEN 0 ELSE 1 END = {int(shape.has_bound_rule)}")
        if shape.has_bound_default is not None:
            predicates.append(
                "CASE WHEN c.default_object_id <> 0 AND dc.object_id IS NULL THEN 1 ELSE 0 END = "
                f"{int(shape.has_bound_default)}"
            )
        checks.append(
            f"""    IF NOT EXISTS (
        SELECT 1 FROM sys.columns AS c
        INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
        LEFT JOIN sys.default_constraints AS dc ON dc.object_id = c.default_object_id
        WHERE c.object_id = OBJECT_ID(N'{qualified}')
          AND {" AND ".join(predicates)}
    )
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_COLUMN_CONTRACT_MISMATCH', 1;"""
        )
    return "\n".join(checks)


def _render_index_proof(*, schema: str, table: str, spec: IndexSpec) -> str:
    name, primary, clustered, columns = spec
    qualified = _sql_text(f"{_quote(schema)}.{_quote(table)}")
    key_csv = ",".join(f"[{column}]" for column in columns)
    return f"""    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes AS i
        WHERE i.object_id = OBJECT_ID(N'{qualified}')
          AND i.name = N'{_sql_text(str(name))}'
          AND i.is_unique = 1 AND i.is_primary_key = {int(bool(primary))}
          AND i.is_unique_constraint = {int(not bool(primary))}
          AND i.type_desc = N'{"CLUSTERED" if clustered else "NONCLUSTERED"}'
          AND i.is_disabled = 0 AND i.is_hypothetical = 0
          AND i.has_filter = 0 AND i.ignore_dup_key = 0
          AND (SELECT STRING_AGG(QUOTENAME(c.name), ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
               FROM sys.index_columns AS ic
               INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
               WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                 AND ic.is_included_column = 0) = N'{_sql_text(key_csv)}'
          AND NOT EXISTS (SELECT 1 FROM sys.index_columns AS include_column
                          WHERE include_column.object_id = i.object_id
                            AND include_column.index_id = i.index_id
                            AND include_column.is_included_column = 1)
          AND (SELECT COUNT_BIG(*) FROM sys.partitions AS p
               WHERE p.object_id = i.object_id AND p.index_id = i.index_id) = 1
          AND NOT EXISTS (SELECT 1 FROM sys.partitions AS p
                          WHERE p.object_id = i.object_id AND p.index_id = i.index_id
                            AND p.data_compression_desc <> N'NONE')
    )
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_INDEX_CONTRACT_MISMATCH', 1;"""


def _render_trigger_proof(
    *,
    schema: str,
    table: str,
    trigger: str,
    action: str,
    body: str,
) -> str:
    qualified = _sql_text(f"{_quote(schema)}.{_quote(table)}")
    definition = (
        f"CREATE TRIGGER {_quote(schema)}.{_quote(trigger)}\nON {_quote(schema)}.{_quote(table)}\n{action}\nAS\n{body}"
    )
    canonical = _sql_text(re.sub(r"\s+", "", definition).upper())
    return f"""    IF (SELECT COUNT_BIG(*) FROM sys.triggers
        WHERE parent_id = OBJECT_ID(N'{qualified}')) <> 1
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_TRIGGER_CONTRACT_MISMATCH', 1;
    IF NOT EXISTS (
        SELECT 1 FROM sys.triggers AS tr
        WHERE tr.parent_id = OBJECT_ID(N'{qualified}')
          AND tr.name = N'{_sql_text(trigger)}' AND tr.is_disabled = 0
          AND tr.is_not_for_replication = 0
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(OBJECT_DEFINITION(tr.object_id)),
              N' ', N''), NCHAR(9), N''), NCHAR(10), N''), NCHAR(13), N'') = N'{canonical}'
    )
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_TRIGGER_CONTRACT_MISMATCH', 1;"""


def _trigger_specs() -> tuple[tuple[str, str, str, str], ...]:
    return (
        (FENCE_TABLE, FENCE_TRIGGER, "AFTER UPDATE, DELETE", render_fence_trigger_body()),
        (
            ATTEMPT_TABLE,
            ATTEMPT_TRIGGER,
            "INSTEAD OF UPDATE, DELETE",
            "    THROW 51000, 'DPONE_LOAD_ATTEMPT_IMMUTABLE', 1;",
        ),
        (OPERATION_TABLE, OPERATION_TRIGGER, "AFTER UPDATE, DELETE", render_operation_trigger_body()),
        (
            RECEIPT_TABLE,
            RECEIPT_TRIGGER,
            "INSTEAD OF UPDATE, DELETE",
            "    THROW 51000, 'DPONE_LOAD_RECEIPT_IMMUTABLE', 1;",
        ),
    )


_COMMON_INDEX_SPECS: dict[str, tuple[IndexSpec, ...]] = {
    FENCE_TABLE: (("pk_dpone_target_fence", True, True, ("target_identity",)),),
    ATTEMPT_TABLE: (
        ("pk_dpone_load_attempt", True, True, ("attempt_key",)),
        ("uq_dpone_load_attempt_generation", False, False, ("target_identity", "generation")),
        (
            "uq_dpone_load_attempt_authority",
            False,
            False,
            ("attempt_key", "target_identity", "generation", "route_fingerprint"),
        ),
    ),
    OPERATION_TABLE: (
        ("pk_dpone_load_operation", True, True, ("operation_key",)),
        ("uq_dpone_load_operation_scope", False, False, ("attempt_key", "scope_hash")),
        ("uq_dpone_load_operation_attempt", False, False, ("operation_key", "attempt_key")),
        (
            "uq_dpone_load_operation_receipt_authority",
            False,
            False,
            ("operation_key", "attempt_key", "scope_hash", "current_epoch", "current_owner_digest"),
        ),
    ),
    RECEIPT_TABLE: (
        ("pk_dpone_load_receipt", True, True, ("receipt_id",)),
        ("uq_dpone_load_receipt_operation", False, False, ("operation_key",)),
    ),
}

_FOREIGN_KEY_SPECS: dict[str, tuple[ForeignKeySpec, ...]] = {
    FENCE_TABLE: (
        (
            "fk_dpone_target_fence_attempt",
            (
                "current_attempt_key",
                "target_identity",
                "current_generation",
                "current_route_fingerprint",
            ),
            ATTEMPT_TABLE,
            ("attempt_key", "target_identity", "generation", "route_fingerprint"),
        ),
    ),
    OPERATION_TABLE: (("fk_dpone_load_operation_attempt", ("attempt_key",), ATTEMPT_TABLE, ("attempt_key",)),),
    RECEIPT_TABLE: (
        (
            "fk_dpone_load_receipt_operation",
            ("operation_key", "attempt_key", "scope_hash", "operation_epoch", "owner_digest"),
            OPERATION_TABLE,
            (
                "operation_key",
                "attempt_key",
                "scope_hash",
                "current_epoch",
                "current_owner_digest",
            ),
        ),
    ),
}

_CHECK_SPECS: dict[str, tuple[CheckSpec, ...]] = {
    FENCE_TABLE: (("ck_dpone_target_fence_generation", "current_generation >= 1"),),
    ATTEMPT_TABLE: (("ck_dpone_load_attempt_generation", "generation >= 1"),),
    OPERATION_TABLE: (("ck_dpone_load_operation_epoch", "current_epoch >= 1"),),
    RECEIPT_TABLE: (
        ("ck_dpone_load_receipt_operation_epoch", "operation_epoch >= 1"),
        (
            "ck_dpone_load_receipt_payload_rows",
            "declared_rows >= 0 AND declared_rows = actual_raw_rows AND actual_raw_rows = actual_native_rows",
        ),
        (
            "ck_dpone_load_receipt_lifecycle",
            "extraction_started_at_utc <= extraction_completed_at_utc AND "
            "((snapshot_acquired_at_utc IS NULL AND snapshot_authority IS NULL "
            "AND source_token_sha256 IS NULL) OR (snapshot_acquired_at_utc IS NOT NULL "
            "AND snapshot_authority IS NOT NULL "
            "AND extraction_started_at_utc <= snapshot_acquired_at_utc "
            "AND snapshot_acquired_at_utc <= extraction_completed_at_utc)) "
            "AND loaded_at_utc <= committed_at_utc",
        ),
        (
            "ck_dpone_load_receipt_metrics",
            "inserted_rows >= 0 AND updated_rows >= 0 AND total_rows >= 0 "
            "AND (staging_rows IS NULL OR staging_rows >= 0) "
            "AND (replaced_rows IS NULL OR replaced_rows >= 0) "
            "AND (soft_deleted_rows IS NULL OR soft_deleted_rows >= 0) "
            "AND (reactivated_rows IS NULL OR reactivated_rows >= 0) "
            "AND (unchanged_rows IS NULL OR unchanged_rows >= 0) "
            "AND (hard_deleted_rows IS NULL OR hard_deleted_rows >= 0) "
            "AND (active_rows IS NULL OR active_rows >= 0)",
        ),
    ),
}

_DEFAULT_SPECS: dict[str, tuple[DefaultSpec, ...]] = {
    FENCE_TABLE: (("df_dpone_target_fence_updated", "updated_at_utc", "SYSUTCDATETIME()"),),
    ATTEMPT_TABLE: (("df_dpone_load_attempt_created", "created_at_utc", "SYSUTCDATETIME()"),),
    OPERATION_TABLE: (("df_dpone_load_operation_updated", "updated_at_utc", "SYSUTCDATETIME()"),),
    RECEIPT_TABLE: (("df_dpone_load_receipt_committed", "committed_at_utc", "SYSUTCDATETIME()"),),
}


def _quote(value: str) -> str:
    return f"[{str(value).replace(']', ']]')}]"


def _sql_text(value: str) -> str:
    return str(value).replace("'", "''")


__all__ = ["render_invocation_index_proof", "render_migration_common_proof"]
