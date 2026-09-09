"""SQL Server storage compatibility for fixed-width dpone columns.

ULIDs and SHA-256 hex values have invariant lengths by construction. SQL
Server can therefore store those framework-owned values losslessly in either
``char(N)`` or ``varchar(N)`` when ``N`` is the exact role width. This narrow
preflight rule intentionally does not apply to business columns, Unicode
types, or different lengths.
"""

from __future__ import annotations

import re

from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.readiness.schema_evolution import ColumnDef

_FIXED_ASCII_WIDTHS: dict[TechnicalColumnRole, int] = {
    TechnicalColumnRole.RUN_ID: 26,
    TechnicalColumnRole.LOAD_ID: 26,
    TechnicalColumnRole.ROW_ID: 64,
    TechnicalColumnRole.PARENT_ROW_ID: 64,
    TechnicalColumnRole.ROOT_ROW_ID: 64,
    TechnicalColumnRole.ROW_HASH: 64,
}
_FIXED_ASCII_WIDTHS_BY_NAME = {
    definition.name.casefold(): _FIXED_ASCII_WIDTHS[definition.role]
    for definition in TechnicalColumnCatalog().definitions()
    if definition.role in _FIXED_ASCII_WIDTHS
}
_FIXED_ASCII_TYPE = re.compile(r"(char|varchar)\((\d+)\)")


def mssql_fixed_technical_types_compatible(
    column: str,
    desired_type: str,
    existing_type: str,
) -> bool:
    """Return whether two exact physical types preserve one fixed dpone role."""

    expected_width = _FIXED_ASCII_WIDTHS_BY_NAME.get(str(column).casefold())
    if expected_width is None:
        return False
    try:
        desired = normalize_mssql_physical_type(desired_type)
        existing = normalize_mssql_physical_type(existing_type)
    except ValueError:
        return False
    desired_match = _FIXED_ASCII_TYPE.fullmatch(desired)
    existing_match = _FIXED_ASCII_TYPE.fullmatch(existing)
    if desired_match is None or existing_match is None:
        return False
    desired_family, desired_width = desired_match.groups()
    existing_family, existing_width = existing_match.groups()
    return (
        desired_family != existing_family
        and int(desired_width) == expected_width
        and int(existing_width) == expected_width
    )


def bind_mssql_fixed_technical_target_types(
    desired_columns: list[ColumnDef],
    existing_columns: tuple[ColumnDef, ...],
) -> list[ColumnDef]:
    """Bind exact live storage for lossless fixed-width framework values."""

    existing_by_name = {column.name.casefold(): column for column in existing_columns}
    bound: list[ColumnDef] = []
    for column in desired_columns:
        existing = existing_by_name.get(column.name.casefold())
        if existing is None or not mssql_fixed_technical_types_compatible(
            column.name,
            column.dtype,
            existing.dtype,
        ):
            bound.append(column)
            continue
        bound.append(
            ColumnDef(
                column.name,
                existing.dtype,
                nullable=column.nullable,
                collation=column.collation,
            )
        )
    return bound


__all__ = [
    "bind_mssql_fixed_technical_target_types",
    "mssql_fixed_technical_types_compatible",
]
