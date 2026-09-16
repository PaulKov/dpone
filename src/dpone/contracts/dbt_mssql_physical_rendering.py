"""Pure deterministic candidate DDL; no membership or execution authority.

The caller must separately authenticate the plan, admit its schema/types/resources,
resolve filegroup and collation identities, exclude name collisions, and own the
required transaction/session. This module does no I/O and settles no transaction.
"""

import re

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_mssql_physical import PhysicalModelPlan
from dpone.contracts.dbt_mssql_physical_validation import PhysicalPlanError
from dpone.contracts.dbt_mssql_physical_wire import _plan
from dpone.contracts.mssql_object_name import quote_mssql_identifier


def _validated(plan: PhysicalModelPlan) -> PhysicalModelPlan:
    """Reuse the wire codec's single-model boundary, including all derived fields.

    Its private decoder is intentionally reused rather than cloning type/name/hash
    rules or constructing a synthetic plan set with invented admission identities.
    Reconstructing from the projection never repairs or mutates the input record.
    """
    if type(plan) is not PhysicalModelPlan:
        raise PhysicalPlanError("rendering requires an exact physical model plan")
    try:
        result = _plan(plan.to_dict())
    except (ValueError, DbtPublishingError, AttributeError, TypeError):
        raise PhysicalPlanError("rendering requires a canonical, internally consistent physical plan") from None
    if result.spec.filegroup.name.strip().casefold() == "default":
        raise PhysicalPlanError("rendering requires a named filegroup, not the default alias")
    for column in result.spec.columns:
        if column.dtype.endswith("(max)"):
            raise PhysicalPlanError("MAX types are unsupported by the managed physical policy")
        if column.collation is not None and column.collation.strip().casefold() == "database_default":
            raise PhysicalPlanError("rendering requires a named collation, not database_default")
        if column.collation is not None and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", column.collation) is None:
            raise PhysicalPlanError(
                "rendering requires a collation name containing only ASCII letters, digits and underscores"
            )
    return result


def _candidate(plan: PhysicalModelPlan) -> str:
    return ".".join(
        quote_mssql_identifier(part)
        for part in (
            plan.spec.relation.database,
            plan.spec.relation.schema,
            plan.candidate_name,
        )
    )


def render_candidate_create(plan: PhysicalModelPlan) -> str:
    """Render an empty heap with explicit ordered columns, filegroup and NONE.

    Type spellings have passed the canonical physical type normalizer through the
    existing plan decoder. Nothing accepts raw SQL. The filegroup ID stays an
    admission/readback identity; SQL addresses its independently resolved name.
    No helper, load query, key, default, filtering or target mutation is emitted.
    """
    value = _validated(plan)
    columns = []
    for column in value.spec.columns:
        collation = "" if column.collation is None else f" COLLATE {column.collation}"
        nullable = "NULL" if column.nullable else "NOT NULL"
        columns.append(f"    {quote_mssql_identifier(column.name)} {column.dtype}{collation} {nullable}")
    definition = ",\n".join(columns)
    return (
        f"CREATE TABLE {_candidate(value)} (\n{definition}\n) "
        f"ON {quote_mssql_identifier(value.spec.filegroup.name)}\nWITH (DATA_COMPRESSION = NONE);"
    )


def render_candidate_layout(plan: PhysicalModelPlan) -> str | None:
    """Render one offline layout action, or None for an unchanged NONE heap.

    ROW/PAGE rebuild the candidate once; COLUMNSTORE creates exactly its derived
    managed CCI on the selected filegroup. No SQL execution, repair, rename, drop,
    layout readback, receipt or retry is performed. MAXDOP is fixed to one by the
    approved first managed cell, independently of any caller/session default.
    """
    value = _validated(plan)
    if value.spec.layout == "rowstore_none":
        return None
    relation = _candidate(value)
    if value.spec.layout in {"rowstore_row", "rowstore_page"}:
        compression = "ROW" if value.spec.layout == "rowstore_row" else "PAGE"
        return f"ALTER TABLE {relation} REBUILD\nWITH (DATA_COMPRESSION = {compression}, ONLINE = OFF, MAXDOP = 1);"
    assert value.columnstore_index_name is not None
    return (
        f"CREATE CLUSTERED COLUMNSTORE INDEX {quote_mssql_identifier(value.columnstore_index_name)}\n"
        f"ON {relation}\nWITH (DATA_COMPRESSION = COLUMNSTORE, ONLINE = OFF, MAXDOP = 1)\n"
        f"ON {quote_mssql_identifier(value.spec.filegroup.name)};"
    )
