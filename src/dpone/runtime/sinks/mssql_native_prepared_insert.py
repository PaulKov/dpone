"""Authoritative metadata projection over a verified typed UNION ALL source."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import MssqlNativeLineageProjection
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import ResolvedMssqlNativeSchema
from dpone.runtime.support.mssql_native_canonical import row_hash_expression


def build_prepared_insert(
    *,
    target_sql: str,
    source_sql: str,
    business_schema: Sequence[tuple[str, str]],
    resolved: ResolvedMssqlNativeSchema,
    lineage: MssqlNativeLineageProjection,
    quote_identifier: Callable[[str], str],
) -> str:
    """Render one explicit INSERT without executing SQL or issuing evidence.

    ``target_sql`` is the qualified invocation-owned prepared table. The caller
    builds ``source_sql`` as explicit SELECTs joined with UNION ALL from receipt
    stages whose ownership and identity it has already verified. These are
    trusted SQL fragments, never user-authored SQL; quoting is not ownership
    validation. An empty load still supplies its verified zero-row raw stage.

    ``business_schema`` is the ordered target-native business schema, excluding
    framework columns, with types from ``resolved.types``. Source columns use
    the resolved wire names. Target order, generated placeholders, lineage and
    business hashing match the existing normalizer projection. Its validation
    and terminal evidence operations must still run after this INSERT.

    The lifecycle loaded_at placeholder remains intact; publication's target
    clock UPDATE is still mandatory. This statement is not idempotent outside
    the preparer's existing ownership, journal and cleanup protocol.
    """

    generated = {column.name: column.placeholder_sql for column in resolved.generated_columns}
    source_values = {
        target: f"r.{quote_identifier(wire)}"
        for wire, target in resolved.wire_to_target.items()
        if target not in generated
    }
    values = {**source_values, **generated}
    if lineage.columns:
        values.update(
            lineage.expressions(
                business_schema=business_schema,
                wire_to_target=resolved.wire_to_target,
                value_expression=source_values.__getitem__,
            )
        )
    for target in resolved.ordered_target_names:
        if target.casefold() == "__dpone__row_hash":
            values[target] = row_hash_expression(business_schema, source_values.__getitem__)
    columns = ", ".join(quote_identifier(target) for target in resolved.ordered_target_names)
    projection = ", ".join(
        f"{values[target]} AS {quote_identifier(target)}" for target in resolved.ordered_target_names
    )
    return f"INSERT INTO {target_sql} ({columns}) SELECT {projection} FROM ({source_sql}) AS r"
