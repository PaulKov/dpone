"""Parameterized SQL Server renderer for the portable relation-scope AST."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from dpone.contracts.portable_scope_model import (
    PortableEqualityScope,
    PortableRangeScope,
    PortableRelationScope,
    PortableScopeRenderBinding,
)


@dataclass(frozen=True, slots=True)
class MssqlPortableScopePredicate:
    """One SQL Server predicate and its immutable ODBC parameters."""

    sql: str
    params: tuple[object, ...]


def render_mssql_portable_scope(
    scope: PortableRelationScope,
    binding: PortableScopeRenderBinding,
    *,
    quote_identifier: Callable[[str], str],
) -> MssqlPortableScopePredicate:
    """Render one quoted identifier and parameter markers without SQL literals."""

    binding.require_scope(scope)
    column = quote_identifier(scope.column)
    if isinstance(scope, PortableEqualityScope):
        if binding.comparison_contract == "binary_unicode_scalar_equality_v1":
            fragment = f"CONVERT(varbinary(max), {column}) = CONVERT(varbinary(max), CONVERT(nvarchar(max), ?))"
        else:
            fragment = f"{column} = {_parameter_expression(binding)}"
        return MssqlPortableScopePredicate(fragment, (scope.value.parameter_value,))

    assert isinstance(scope, PortableRangeScope)
    fragments: list[str] = []
    params: list[object] = []
    rendered_column = _range_column_expression(column, binding)
    if scope.lower is not None:
        operator = ">=" if scope.lower.inclusive else ">"
        fragments.append(f"{rendered_column} {operator} {_parameter_expression(binding)}")
        params.append(_parameter_value(scope.lower.value))
    if scope.upper is not None:
        operator = "<=" if scope.upper.inclusive else "<"
        fragments.append(f"{rendered_column} {operator} {_parameter_expression(binding)}")
        params.append(_parameter_value(scope.upper.value))
    return MssqlPortableScopePredicate(" AND ".join(fragments), tuple(params))


def _parameter_expression(binding: PortableScopeRenderBinding) -> str:
    """Pin temporal ODBC values to the catalog-proven SQL Server type."""

    if binding.literal_type == "date":
        return "CONVERT(date, ?)"
    if binding.literal_type in {"timestamp", "timestamptz"}:
        # ``target_type`` was canonicalized by the binding validator and can
        # only be datetime2(p) or datetimeoffset(p) for these literal types.
        return f"CONVERT({binding.target_type}, ?)"
    if binding.literal_type == "uuid":
        return "LOWER(CONVERT(char(36), ?)) COLLATE Latin1_General_100_BIN2"
    return "?"


def _range_column_expression(column: str, binding: PortableScopeRenderBinding) -> str:
    if binding.literal_type == "uuid":
        return f"LOWER(CONVERT(char(36), {column})) COLLATE Latin1_General_100_BIN2"
    return column


def _parameter_value(literal: object) -> object:
    value = getattr(literal, "parameter_value")
    return str(value) if getattr(literal, "kind") == "uuid" else value


__all__ = ["MssqlPortableScopePredicate", "render_mssql_portable_scope"]
