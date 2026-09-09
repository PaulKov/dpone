"""Read-only shape preflight for externally provisioned MSSQL state tables."""

from __future__ import annotations

from typing import Any

from dpone.runtime.state.mssql_column_shape_contract import (
    MssqlColumnShape,
    MssqlExternalTableContract,
    exact_external_table_contract,
    matches_mssql_column_shape,
)
from dpone.runtime.state.mssql_load_audit_contract import load_audit_additive_ddl
from dpone.runtime.support.mssql_identifier_integrity import require_case_unambiguous_identifiers

SOURCE_STATE_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "state_key",
            "contract_version",
            "environment",
            "process_name",
            "source_connection",
            "source_database",
            "source_schema",
            "source_table",
            "target_database",
            "target_schema",
            "target_table",
            "target_identity",
            "unique_key_json",
            "schema_hash",
            "scope_hash",
            "xmin_value",
            "state_revision",
            "is_initial",
            "wraparound_detected",
            "frozen_xid",
            "source_snapshot_token",
            "last_load_id",
            "superseded_at_utc",
            "superseded_by_state_key",
            "__dpone__loaded_at",
            "__dpone__updated_at",
        }
    ),
    unique_indexes=(("state_key",),),
    shapes=(
        MssqlColumnShape("state_key", "binary", 32, None, None, False),
        MssqlColumnShape("contract_version", "nvarchar", 128, None, None, False),
        MssqlColumnShape("environment", "nvarchar", 256, None, None, False),
        MssqlColumnShape("process_name", "nvarchar", 1024, None, None, False),
        MssqlColumnShape("source_connection", "nvarchar", 1024, None, None, False),
        MssqlColumnShape("source_database", "nvarchar", 512, None, None, False),
        MssqlColumnShape("source_schema", "nvarchar", 512, None, None, False),
        MssqlColumnShape("source_table", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_database", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_schema", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_table", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_identity", "binary", 32, None, None, False),
        MssqlColumnShape("unique_key_json", "nvarchar", -1, None, None, False),
        MssqlColumnShape("schema_hash", "char", 71, None, None, False),
        MssqlColumnShape("scope_hash", "char", 71, None, None, False),
        MssqlColumnShape("xmin_value", "bigint", 8, 19, 0, False),
        MssqlColumnShape("state_revision", "bigint", 8, 19, 0, False),
        MssqlColumnShape("is_initial", "bit", 1, 1, 0, False),
        MssqlColumnShape("wraparound_detected", "bit", 1, 1, 0, False),
        MssqlColumnShape("frozen_xid", "bigint", 8, 19, 0, True),
        MssqlColumnShape("source_snapshot_token", "char", 71, None, None, False),
        MssqlColumnShape("last_load_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("superseded_at_utc", "datetime2", 8, None, 7, True),
        MssqlColumnShape("superseded_by_state_key", "binary", 32, None, None, True),
        MssqlColumnShape("__dpone__loaded_at", "datetime2", 8, None, 7, False),
        MssqlColumnShape("__dpone__updated_at", "datetime2", 8, None, 7, False),
    ),
)
COMMIT_RECEIPT_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "receipt_id",
            "state_key",
            "load_id",
            "previous_xmin",
            "candidate_xmin",
            "previous_revision",
            "candidate_revision",
            "source_snapshot_token",
            "publication_receipt_id",
            "committed_at",
        }
    ),
    unique_indexes=(("receipt_id",), ("state_key", "load_id")),
    shapes=(
        MssqlColumnShape("receipt_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("state_key", "binary", 32, None, None, False),
        MssqlColumnShape("load_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("previous_xmin", "bigint", 8, 19, 0, True),
        MssqlColumnShape("candidate_xmin", "bigint", 8, 19, 0, False),
        MssqlColumnShape("previous_revision", "bigint", 8, 19, 0, True),
        MssqlColumnShape("candidate_revision", "bigint", 8, 19, 0, False),
        MssqlColumnShape("source_snapshot_token", "char", 71, None, None, False),
        MssqlColumnShape("publication_receipt_id", "nvarchar", 256, None, None, True),
        MssqlColumnShape("committed_at", "datetime2", 8, None, 7, False),
    ),
)
RUN_STATE_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "id",
            "run_state_key",
            "dag_id",
            "process_name",
            "source_schema",
            "source_table",
            "target_schema",
            "target_table",
            "load_strategy",
            "execution_date",
            "state",
            "started_at",
            "ended_at",
            "duration_min",
            "error_message",
            "rows_read",
            "rows_written",
            "rows_updated",
            "rows_deleted",
            "__dpone__loaded_at",
            "__dpone__updated_at",
        }
    ),
    unique_indexes=(("id",), ("run_state_key",)),
    shapes=(
        MssqlColumnShape("id", "bigint", 8, 19, 0, False, identity=True),
        MssqlColumnShape("run_state_key", "binary", 32, None, None, False),
        MssqlColumnShape("dag_id", "nvarchar", 1024, None, None, False),
        MssqlColumnShape("process_name", "nvarchar", 1024, None, None, False),
        MssqlColumnShape("source_schema", "nvarchar", 512, None, None, False),
        MssqlColumnShape("source_table", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_schema", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_table", "nvarchar", 512, None, None, False),
        MssqlColumnShape("load_strategy", "nvarchar", 128, None, None, False),
        MssqlColumnShape("execution_date", "datetime2", 8, None, 7, False),
        MssqlColumnShape("state", "nvarchar", 64, None, None, False),
        MssqlColumnShape("started_at", "datetime2", 8, None, 7, False),
        MssqlColumnShape("ended_at", "datetime2", 8, None, 7, True),
        MssqlColumnShape("duration_min", "float", 8, 53, 0, True),
        MssqlColumnShape("error_message", "nvarchar", -1, None, None, True),
        MssqlColumnShape("rows_read", "bigint", 8, 19, 0, True),
        MssqlColumnShape("rows_written", "bigint", 8, 19, 0, True),
        MssqlColumnShape("rows_updated", "bigint", 8, 19, 0, True),
        MssqlColumnShape("rows_deleted", "bigint", 8, 19, 0, True),
        MssqlColumnShape("__dpone__loaded_at", "datetime2", 8, None, 7, False),
        MssqlColumnShape("__dpone__updated_at", "datetime2", 8, None, 7, False),
    ),
    identity_columns=frozenset({"id"}),
)
LOAD_AUDIT_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "run_id",
            "load_id",
            "status",
            "process_name",
            "source_schema",
            "source_table",
            "target_schema",
            "target_table",
            "strategy",
            "started_at",
            "staged_at",
            "committed_at",
            "failed_at",
            "extracted_rows",
            "staged_rows",
            "inserted_rows",
            "updated_rows",
            "loaded_rows",
            "deleted_rows",
            "reactivated_rows",
            "unchanged_rows",
            "soft_deleted_rows",
            "hard_deleted_rows",
            "active_rows",
            "total_rows",
            "commit_receipt_id",
            "commit_outcome",
            "error_message",
            "artifact_uri",
            "__dpone__loaded_at",
        }
    ),
    unique_indexes=(("load_id",),),
    shapes=(
        MssqlColumnShape("run_id", "char", 26, None, None, False),
        MssqlColumnShape("load_id", "char", 26, None, None, False),
        MssqlColumnShape("status", "nvarchar", 64, None, None, False),
        MssqlColumnShape("process_name", "nvarchar", 1024, None, None, True),
        MssqlColumnShape("source_schema", "nvarchar", 512, None, None, False),
        MssqlColumnShape("source_table", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_schema", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_table", "nvarchar", 512, None, None, False),
        MssqlColumnShape("strategy", "nvarchar", 128, None, None, False),
        MssqlColumnShape("started_at", "datetime2", 8, None, 7, False),
        MssqlColumnShape("staged_at", "datetime2", 8, None, 7, True),
        MssqlColumnShape("committed_at", "datetime2", 8, None, 7, True),
        MssqlColumnShape("failed_at", "datetime2", 8, None, 7, True),
        *(
            MssqlColumnShape(column, "bigint", 8, 19, 0, True)
            for column in (
                "extracted_rows",
                "staged_rows",
                "inserted_rows",
                "updated_rows",
                "loaded_rows",
                "deleted_rows",
                "reactivated_rows",
                "unchanged_rows",
                "soft_deleted_rows",
                "hard_deleted_rows",
                "active_rows",
                "total_rows",
            )
        ),
        MssqlColumnShape("commit_receipt_id", "nvarchar", 256, None, None, True),
        MssqlColumnShape("commit_outcome", "nvarchar", 128, None, None, True),
        MssqlColumnShape("error_message", "nvarchar", -1, None, None, True),
        MssqlColumnShape("artifact_uri", "nvarchar", -1, None, None, True),
        MssqlColumnShape("__dpone__loaded_at", "datetime2", 8, None, 7, False),
    ),
)


def require_external_table_shape(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
    contract: MssqlExternalTableContract,
) -> None:
    """Fail before DML when a pre-created state table has drifted."""

    prefix = f"{connector.quote_identifier(database)}." if database else ""
    rows = connector.get_records(
        "SELECT c.name AS column_name, ty.name AS type_name, c.max_length, "
        "c.precision, c.scale, c.is_nullable, c.is_identity, c.is_computed, "
        "c.is_sparse, c.is_rowguidcol, c.generated_always_type, c.is_hidden, "
        "c.is_masked, c.is_ansi_padded, c.is_filestream, c.is_column_set, "
        "CASE WHEN c.encryption_type IS NULL THEN 0 ELSE 1 END AS is_encrypted, "
        "CASE WHEN c.collation_name IS NULL OR c.collation_name = "
        "CONVERT(sysname, DATABASEPROPERTYEX(COALESCE(?, DB_NAME()), 'Collation')) "
        "THEN 1 ELSE 0 END AS uses_database_default_collation, "
        "ty.is_user_defined, ty.is_assembly_type, "
        "CASE WHEN c.rule_object_id = 0 THEN 0 ELSE 1 END AS has_bound_rule, "
        "CASE WHEN c.default_object_id <> 0 AND dc.object_id IS NULL "
        "THEN 1 ELSE 0 END AS has_bound_default "
        f"FROM {prefix}sys.columns AS c "
        f"INNER JOIN {prefix}sys.types AS ty ON ty.user_type_id = c.user_type_id "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = c.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        f"LEFT JOIN {prefix}sys.default_constraints AS dc "
        "ON dc.object_id = c.default_object_id "
        "AND dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id "
        "WHERE s.name = ? AND t.name = ?",
        (database, schema, table),
        as_dict=True,
    )
    label = f"{database}.{schema}.{table}"
    column_names = require_case_unambiguous_identifiers(
        (row.get("column_name") for row in rows),
        error_code=f"mssql_external_state_identifier_case_ambiguity:{label}",
    )
    metadata = dict(zip(column_names, rows, strict=True))
    actual = set(metadata)
    missing = sorted(contract.columns - actual)
    if missing:
        raise RuntimeError(f"mssql_external_state_contract_missing:{label}:{','.join(missing)}")
    if contract.exact_columns and actual != contract.columns:
        unexpected = sorted(actual - contract.columns)
        raise RuntimeError(f"mssql_external_state_contract_unexpected:{label}:{','.join(unexpected)}")
    for shape in contract.shapes:
        if not matches_mssql_column_shape(metadata[shape.name], shape):
            raise RuntimeError(f"mssql_external_state_contract_shape:{label}:{shape.name}")
    unique_indexes = _unique_indexes(connector, prefix, schema, table)
    if any(required not in unique_indexes for required in contract.unique_indexes):
        raise RuntimeError(f"mssql_external_state_unique_index_missing:{label}")


def runtime_table_columns(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
) -> set[str]:
    """Read only managed table column names for compatibility routing."""

    prefix = f"{connector.quote_identifier(database)}." if database else ""
    rows = connector.get_records(
        f"SELECT c.name AS column_name FROM {prefix}sys.columns AS c "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = c.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ?",
        (schema, table),
        as_dict=True,
    )
    return set(
        require_case_unambiguous_identifiers(
            (row.get("column_name") for row in rows),
            error_code=f"mssql_runtime_table_identifier_case_ambiguity:{database}.{schema}.{table}",
        )
    )


def _unique_indexes(connector: Any, prefix: str, schema: str, table: str) -> set[tuple[str, ...]]:
    rows = connector.get_records(
        "SELECT i.index_id, c.name AS column_name, ic.key_ordinal "
        f"FROM {prefix}sys.indexes AS i "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = i.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        f"INNER JOIN {prefix}sys.index_columns AS ic "
        "ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        f"INNER JOIN {prefix}sys.columns AS c "
        "ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE s.name = ? AND t.name = ? AND i.is_unique = 1 "
        "AND i.is_hypothetical = 0 AND i.is_disabled = 0 "
        "AND i.has_filter = 0 AND ic.is_included_column = 0 "
        "ORDER BY i.index_id, ic.key_ordinal",
        (schema, table),
        as_dict=True,
    )
    column_names = require_case_unambiguous_identifiers(
        (row.get("column_name") for row in rows),
        error_code=f"mssql_external_state_index_identifier_case_ambiguity:{schema}.{table}",
    )
    grouped: dict[int, list[tuple[int, str]]] = {}
    for row, column_name in zip(rows, column_names, strict=True):
        grouped.setdefault(int(row["index_id"]), []).append((int(row["key_ordinal"]), column_name))
    return {tuple(column for _ordinal, column in sorted(columns)) for columns in grouped.values()}


__all__ = [
    "COMMIT_RECEIPT_CONTRACT",
    "LOAD_AUDIT_CONTRACT",
    "RUN_STATE_CONTRACT",
    "SOURCE_STATE_CONTRACT",
    "MssqlExternalTableContract",
    "MssqlColumnShape",
    "require_external_table_shape",
    "load_audit_additive_ddl",
    "runtime_table_columns",
    "exact_external_table_contract",
]
