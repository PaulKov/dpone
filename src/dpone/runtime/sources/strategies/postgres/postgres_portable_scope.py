"""Parameterized PostgreSQL renderer for the portable relation-scope AST."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg import sql

from dpone.contracts.portable_scope_model import (
    PortableEqualityScope,
    PortableRangeScope,
    PortableRelationScope,
    PortableScopeRenderBinding,
)


@dataclass(frozen=True, slots=True)
class PostgresPortableScopePredicate:
    """One composable PostgreSQL predicate and its immutable DB-API values."""

    sql: sql.Composable
    params: tuple[object, ...]


def render_postgres_portable_scope(
    scope: PortableRelationScope,
    binding: PortableScopeRenderBinding,
) -> PostgresPortableScopePredicate:
    """Render only identifiers/operators; every authored value remains a parameter."""

    binding.require_scope(scope)
    column = sql.Identifier(scope.column)
    if isinstance(scope, PortableEqualityScope):
        if binding.comparison_contract == "binary_unicode_scalar_equality_v1":
            fragment = sql.SQL("convert_to({}::text, 'UTF8') = convert_to(%s::text, 'UTF8')").format(column)
        else:
            fragment = sql.SQL("{} = %s").format(column)
        return PostgresPortableScopePredicate(fragment, (scope.value.parameter_value,))

    assert isinstance(scope, PortableRangeScope)
    fragments: list[sql.Composable] = []
    params: list[object] = []
    if scope.lower is not None:
        operator = ">=" if scope.lower.inclusive else ">"
        if scope.lower.value.kind == "uuid":
            fragments.append(sql.SQL("{} {} %s::uuid").format(column, sql.SQL(operator)))
        else:
            fragments.append(sql.SQL("{} {} %s").format(column, sql.SQL(operator)))
        params.append(_parameter_value(scope.lower.value))
    if scope.upper is not None:
        operator = "<=" if scope.upper.inclusive else "<"
        if scope.upper.value.kind == "uuid":
            fragments.append(sql.SQL("{} {} %s::uuid").format(column, sql.SQL(operator)))
        else:
            fragments.append(sql.SQL("{} {} %s").format(column, sql.SQL(operator)))
        params.append(_parameter_value(scope.upper.value))
    return PostgresPortableScopePredicate(
        sql.SQL(" AND ").join(fragments),
        tuple(params),
    )


def _parameter_value(literal: object) -> object:
    value = getattr(literal, "parameter_value")
    return str(value) if getattr(literal, "kind") == "uuid" else value


__all__ = ["PostgresPortableScopePredicate", "render_postgres_portable_scope"]
