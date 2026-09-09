"""Read-only T-SQL parsing adapter for semantic refresh model-definition proof."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

from sqlglot import exp, parse
from sqlglot.errors import ParseError

from dpone.contracts.dbt_semantic_refresh_common import (
    SemanticRefreshProofIssue,
    nonconformant,
    proof_status,
    unverified,
)
from dpone.contracts.dbt_semantic_refresh_common import (
    TargetIndependentSqlProof as ModelDefinitionProof,
)

MODEL_DEFINITION_PROOF_SCHEMA = "dpone.semantic-refresh-model-definition-proof.v1"


class SqlglotSemanticRefreshSqlProof:
    """Injectable parser adapter used by the protected catalog-proof service."""

    def prove(
        self,
        compiled_sql_by_target: dict[str, str],
        *,
        forbidden_relations: tuple[tuple[str, str, str], ...],
    ) -> ModelDefinitionProof:
        return prove_target_independent_compiled_sql(
            compiled_sql_by_target,
            forbidden_relations=forbidden_relations,
        )


def prove_target_independent_compiled_sql(
    compiled_sql_by_target: Mapping[str, str],
    *,
    forbidden_relations: tuple[tuple[str, str, str], ...],
) -> ModelDefinitionProof:
    """Parse two or more target compilations and prove an identical read-only query."""

    issues: list[SemanticRefreshProofIssue] = []
    if (
        not isinstance(compiled_sql_by_target, Mapping)
        or len(compiled_sql_by_target) < 2
        or any(not isinstance(name, str) or not name for name in compiled_sql_by_target)
        or any(not isinstance(sql, str) or not sql.strip() for sql in compiled_sql_by_target.values())
    ):
        issues.append(
            unverified(
                "DPONE_DBT_V2_COMPILE_UNVERIFIED",
                "compiled_sql_by_target",
                "at least two named target compilations are required",
            )
        )
        return ModelDefinitionProof("UNVERIFIED", None, (), tuple(issues))
    forbidden = _relations(forbidden_relations, "forbidden_relations")
    normalized: dict[str, str] = {}
    relations: dict[str, tuple[tuple[str, str, str], ...]] = {}
    for target_name, sql in sorted(compiled_sql_by_target.items()):
        try:
            statements = parse(sql, read="tsql")
        except ParseError:
            issues.append(
                unverified(
                    "DPONE_DBT_V2_SQL_PARSE_UNVERIFIED",
                    f"compiled_sql_by_target.{target_name}",
                    "compiled SQL cannot be parsed as one T-SQL query",
                )
            )
            continue
        if len(statements) != 1 or not isinstance(statements[0], exp.Query):
            issues.append(
                nonconformant(
                    "DPONE_DBT_V2_READ_ONLY_QUERY_REQUIRED",
                    f"compiled_sql_by_target.{target_name}",
                    "compiled SQL must be one read-only query, CTE or set expression",
                )
            )
            continue
        statement = statements[0]
        if statement.find(exp.Into) is not None or _contains_external_rowset(statement):
            issues.append(
                nonconformant(
                    "DPONE_DBT_V2_READ_ONLY_QUERY_REQUIRED",
                    f"compiled_sql_by_target.{target_name}",
                    "SELECT INTO, OPENQUERY and OPENROWSET are unsupported",
                )
            )
            continue
        unsupported_functions = tuple(_unsupported_function_calls(statement))
        if unsupported_functions:
            issues.append(
                nonconformant(
                    "DPONE_DBT_V2_FUNCTION_UNSUPPORTED",
                    f"compiled_sql_by_target.{target_name}",
                    "scalar, multi-statement, CLR, or unresolved function calls are unsupported",
                )
            )
            continue
        normalized[target_name] = statement.sql(dialect="tsql", pretty=False, normalize=True)
        cte_names = {cte.alias.casefold() for cte in statement.find_all(exp.CTE) if cte.alias}
        relations[target_name] = tuple(
            sorted(
                {
                    _table_identity(table)
                    for table in statement.find_all(exp.Table)
                    if table.db or table.catalog or table.name.casefold() not in cte_names
                }
            )
        )
    if issues:
        ordered = _ordered_issues(issues)
        return ModelDefinitionProof(proof_status(ordered), None, (), ordered)
    if len(set(normalized.values())) != 1:
        issues.append(
            nonconformant(
                "DPONE_DBT_V2_TARGET_DEPENDENT_SQL",
                "compiled_sql_by_target",
                "normalized compiled SQL differs between certified targets",
            )
        )
    if len(set(relations.values())) != 1:
        issues.append(
            nonconformant(
                "DPONE_DBT_V2_TARGET_DEPENDENT_SQL",
                "read_relations",
                "compiled relation reads differ between certified targets",
            )
        )
    read_relations = next(iter(relations.values()))
    for relation in read_relations:
        if any(_relation_matches(relation, blocked) for blocked in forbidden):
            issues.append(
                nonconformant(
                    "DPONE_DBT_V2_TARGET_READ_UNSUPPORTED",
                    "read_relations",
                    "compiled SQL reads its target or another forbidden relation",
                )
            )
    ordered = _ordered_issues(issues)
    canonical_sql = next(iter(normalized.values()))
    return ModelDefinitionProof(
        proof_status(ordered),
        "sha256:" + hashlib.sha256(canonical_sql.encode("utf-8")).hexdigest(),
        read_relations,
        ordered,
    )


def _contains_external_rowset(statement: exp.Query) -> bool:
    sql = statement.sql(dialect="tsql", pretty=False).casefold()
    return "openquery(" in sql.replace(" ", "") or "openrowset(" in sql.replace(" ", "")


def _unsupported_function_calls(statement: exp.Query):
    for call in statement.find_all(exp.Anonymous):
        parent = call.parent
        if isinstance(parent, exp.Table) and parent.this is call:
            continue
        yield call


def _table_identity(table: exp.Table) -> tuple[str, str, str]:
    name = table.name
    if not name and isinstance(table.this, exp.Anonymous):
        name = table.this.name
    return (
        table.catalog.casefold(),
        table.db.casefold(),
        name.casefold(),
    )


def _relations(
    values: object,
    field: str,
) -> tuple[tuple[str, str, str], ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(
            not isinstance(value, tuple)
            or len(value) != 3
            or any(not isinstance(part, str) or not part for part in value)
            for value in values
        )
    ):
        raise ValueError(f"{field} must contain qualified three-part relations")
    normalized: list[tuple[str, str, str]] = []
    for database, schema, name in values:
        normalized.append((database.casefold(), schema.casefold(), name.casefold()))
    return tuple(normalized)


def _relation_matches(
    observed: tuple[str, str, str],
    forbidden: tuple[str, str, str],
) -> bool:
    observed_parts = tuple(part for part in observed if part)
    forbidden_parts = tuple(part for part in forbidden if part)
    comparable = min(len(observed_parts), len(forbidden_parts))
    return comparable >= 2 and observed_parts[-comparable:] == forbidden_parts[-comparable:]


def _ordered_issues(
    issues: list[SemanticRefreshProofIssue],
) -> tuple[SemanticRefreshProofIssue, ...]:
    return tuple(sorted(issues, key=lambda issue: (issue.field, issue.code)))


__all__ = [
    "MODEL_DEFINITION_PROOF_SCHEMA",
    "ModelDefinitionProof",
    "SqlglotSemanticRefreshSqlProof",
    "prove_target_independent_compiled_sql",
]
