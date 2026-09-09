"""Backward-compatible MSSQL load-audit schema evolution contract."""

from __future__ import annotations

_LOAD_AUDIT_ADDITIVE_COLUMNS = (
    ("deleted_rows", "bigint"),
    ("reactivated_rows", "bigint"),
    ("unchanged_rows", "bigint"),
    ("soft_deleted_rows", "bigint"),
    ("hard_deleted_rows", "bigint"),
    ("active_rows", "bigint"),
    ("total_rows", "bigint"),
    ("commit_receipt_id", "nvarchar(128)"),
    ("commit_outcome", "nvarchar(64)"),
)


def load_audit_additive_ddl(*, fq_table: str, object_id: str) -> str:
    """Render the backward-compatible additive audit migration."""

    object_literal = object_id.replace("'", "''")
    return "\n".join(
        f"IF COL_LENGTH(N'{object_literal}', N'{column}') IS NULL "
        f"ALTER TABLE {fq_table} ADD [{column}] {data_type} NULL;"
        for column, data_type in _LOAD_AUDIT_ADDITIVE_COLUMNS
    )


__all__ = ["load_audit_additive_ddl"]
