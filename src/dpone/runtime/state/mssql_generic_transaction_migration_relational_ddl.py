"""Relational-family proof fragments for the generic MSSQL v2 migration."""

from __future__ import annotations

import re
from typing import TypeAlias

ForeignKeySpec: TypeAlias = tuple[str, tuple[str, ...], str, tuple[str, ...]]
CheckSpec: TypeAlias = tuple[str, str]
DefaultSpec: TypeAlias = tuple[str, str, str]


def render_relational_family_proof(
    *,
    schema: str,
    table: str,
    foreign_keys: tuple[ForeignKeySpec, ...],
    checks: tuple[CheckSpec, ...],
    defaults: tuple[DefaultSpec, ...],
) -> str:
    """Prove the exact FK, CHECK, and default families for one v1 table."""

    qualified = _sql_text(f"{_quote(schema)}.{_quote(table)}")
    clauses = [
        f"""    IF (SELECT COUNT_BIG(*) FROM sys.foreign_keys
        WHERE parent_object_id = OBJECT_ID(N'{qualified}')) <> {len(foreign_keys)}
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_FOREIGN_KEY_CONTRACT_MISMATCH', 1;
    IF (SELECT COUNT_BIG(*) FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID(N'{qualified}')) <> {len(checks)}
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_CHECK_CONTRACT_MISMATCH', 1;
    IF (SELECT COUNT_BIG(*) FROM sys.default_constraints
        WHERE parent_object_id = OBJECT_ID(N'{qualified}')) <> {len(defaults)}
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_DEFAULT_CONTRACT_MISMATCH', 1;"""
    ]
    clauses.extend(_render_foreign_key_proof(schema=schema, table=table, spec=spec) for spec in foreign_keys)
    for name, expression in checks:
        canonical = _sql_text(_canonical_sql_expression(expression))
        clauses.append(
            f"""    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints AS ck
        WHERE ck.parent_object_id = OBJECT_ID(N'{qualified}')
          AND ck.name = N'{_sql_text(name)}'
          AND ck.is_disabled = 0 AND ck.is_not_trusted = 0
          AND ck.is_not_for_replication = 0
          AND {_sql_canonical_expression("ck.definition")} = N'{canonical}'
    )
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_CHECK_CONTRACT_MISMATCH', 1;"""
        )
    for name, column, expression in defaults:
        canonical = _sql_text(_canonical_sql_expression(expression))
        clauses.append(
            f"""    IF NOT EXISTS (
        SELECT 1 FROM sys.default_constraints AS dc
        INNER JOIN sys.columns AS c ON c.object_id = dc.parent_object_id
                                    AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'{qualified}')
          AND dc.name = N'{_sql_text(name)}' AND c.name = N'{_sql_text(column)}'
          AND {_sql_canonical_expression("dc.definition")} = N'{canonical}'
    )
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_DEFAULT_CONTRACT_MISMATCH', 1;"""
        )
    return "\n".join(clauses)


def _render_foreign_key_proof(*, schema: str, table: str, spec: ForeignKeySpec) -> str:
    name, parent_columns, referenced_table, referenced_columns = spec
    qualified = _sql_text(f"{_quote(schema)}.{_quote(table)}")
    parent_csv = _sql_text(",".join(f"[{column}]" for column in parent_columns))
    referenced_csv = _sql_text(",".join(f"[{column}]" for column in referenced_columns))
    return f"""    IF NOT EXISTS (
        SELECT 1 FROM sys.foreign_keys AS fk
        INNER JOIN sys.tables AS rt ON rt.object_id = fk.referenced_object_id
        INNER JOIN sys.schemas AS rs ON rs.schema_id = rt.schema_id
        WHERE fk.parent_object_id = OBJECT_ID(N'{qualified}')
          AND fk.name = N'{_sql_text(str(name))}'
          AND rs.name = N'{_sql_text(schema)}' AND rt.name = N'{_sql_text(str(referenced_table))}'
          AND fk.is_disabled = 0 AND fk.is_not_trusted = 0
          AND fk.is_not_for_replication = 0
          AND fk.delete_referential_action = 0 AND fk.update_referential_action = 0
          AND (SELECT STRING_AGG(QUOTENAME(pc.name), ',')
                     WITHIN GROUP (ORDER BY fkc.constraint_column_id)
               FROM sys.foreign_key_columns AS fkc
               INNER JOIN sys.columns AS pc ON pc.object_id = fkc.parent_object_id
                                            AND pc.column_id = fkc.parent_column_id
               WHERE fkc.constraint_object_id = fk.object_id) = N'{parent_csv}'
          AND (SELECT STRING_AGG(QUOTENAME(rc.name), ',')
                     WITHIN GROUP (ORDER BY fkc.constraint_column_id)
               FROM sys.foreign_key_columns AS fkc
               INNER JOIN sys.columns AS rc ON rc.object_id = fkc.referenced_object_id
                                            AND rc.column_id = fkc.referenced_column_id
               WHERE fkc.constraint_object_id = fk.object_id) = N'{referenced_csv}'
    )
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V1_FOREIGN_KEY_CONTRACT_MISMATCH', 1;"""


def _canonical_sql_expression(value: str) -> str:
    return re.sub(r"[\s\[\]\(\)]", "", value).upper()


def _sql_canonical_expression(reference: str) -> str:
    result = f"UPPER({reference})"
    for token in (" ", "\\t", "\\n", "\\r", "[", "]", "(", ")"):
        literal = {
            "\\t": "NCHAR(9)",
            "\\n": "NCHAR(10)",
            "\\r": "NCHAR(13)",
        }.get(token, f"N'{token}'")
        result = f"REPLACE({result}, {literal}, N'')"
    return result


def _quote(value: str) -> str:
    return f"[{str(value).replace(']', ']]')}]"


def _sql_text(value: str) -> str:
    return str(value).replace("'", "''")


__all__ = [
    "CheckSpec",
    "DefaultSpec",
    "ForeignKeySpec",
    "render_relational_family_proof",
]
