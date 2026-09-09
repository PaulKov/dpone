"""Exact dbt selection and mutation-closure policy for semantic refresh V2."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeGuard

from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.dbt_semantic_refresh_common import (
    ProofStatus,
    SemanticRefreshProofIssue,
    nonconformant,
    proof_status,
    unverified,
)

V2_DBT_INDIRECT_SELECTION = "empty"
MUTATION_CLOSURE_SCHEMA = "dpone.semantic-refresh-mutation-closure.v1"

_EPHEMERAL = "DPONE_DBT_V2_EPHEMERAL_UNSUPPORTED"
_GRAPH_UNVERIFIED = "DPONE_DBT_V2_GRAPH_UNVERIFIED"
_UNCLASSIFIED = "DPONE_DBT_V2_MUTATION_UNCLASSIFIED"
_MODEL_UNSUPPORTED = "DPONE_DBT_V2_MODEL_UNSUPPORTED"
_TEST_MUTATION = "DPONE_DBT_V2_TEST_MUTATION_UNSUPPORTED"


@dataclass(frozen=True, slots=True)
class MutationClosureReport:
    """Deterministic V2 mutation-closure proof."""

    status: ProofStatus
    selected_mutating_node_ids: tuple[str, ...]
    transitive_node_ids: tuple[str, ...]
    issues: tuple[SemanticRefreshProofIssue, ...]
    proof_sha256: str
    schema: str = MUTATION_CLOSURE_SCHEMA


def exact_fqn_selectors(
    manifest: Mapping[str, Any],
    selected_unique_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """Return exact ``fqn:`` selectors; V1 ancestor ``+`` semantics stay untouched."""

    selected = _require_ids(selected_unique_ids, "selected_unique_ids")
    sections = _manifest_sections(manifest)
    selectors: list[str] = []
    for unique_id in selected:
        node = _manifest_node(sections, unique_id)
        fqn = node.get("fqn") if node is not None else None
        if not _safe_fqn(fqn):
            raise ValueError(f"{unique_id} has no safe exact FQN")
        selectors.append(f"fqn:{'.'.join(fqn)}")
    result = tuple(sorted(selectors))
    if len(result) != len(set(result)):
        raise ValueError("selected resources have colliding exact FQN selectors")
    return result


def prove_mutation_closure(
    manifest: Mapping[str, Any],
    *,
    selected_graph_unique_ids: tuple[str, ...],
    selected_mutating_node_ids: tuple[str, ...],
) -> MutationClosureReport:
    """Prove that exact selection can execute only managed scope mutations."""

    selected = _require_ids(selected_graph_unique_ids, "selected_graph_unique_ids")
    mutating = _require_ids(selected_mutating_node_ids, "selected_mutating_node_ids")
    sections = _manifest_sections(manifest)
    issues: list[SemanticRefreshProofIssue] = []
    if any(unique_id not in selected for unique_id in mutating):
        issues.append(
            nonconformant(
                _UNCLASSIFIED,
                "selected_mutating_node_ids",
                "every mutating node must be part of the exact selected graph",
            )
        )

    for unique_id in selected:
        node = _manifest_node(sections, unique_id)
        if node is None:
            issues.append(_missing_node(unique_id))
            continue
        resource_type = node.get("resource_type")
        if resource_type == "model":
            config = node.get("config")
            if isinstance(config, Mapping) and config.get("materialized") == "ephemeral":
                issues.append(_ephemeral_issue(unique_id))
            elif unique_id in mutating:
                issues.extend(_managed_model_issues(unique_id, node))
            else:
                issues.append(
                    nonconformant(
                        _UNCLASSIFIED,
                        "selected_graph_unique_ids",
                        "a selected model would mutate outside the managed operation set",
                        unique_id=unique_id,
                    )
                )
        elif resource_type == "test":
            issues.extend(_data_test_issues(unique_id, node))
        elif resource_type == "unit_test":
            issues.extend(_unit_test_issues(unique_id, node))
        else:
            issues.append(
                nonconformant(
                    _UNCLASSIFIED,
                    "resource_type",
                    "selected seeds, snapshots, operations and unknown resources are not classified",
                    unique_id=unique_id,
                )
            )

    transitive = _transitive_dependencies(sections, mutating, set(selected), issues)
    payload = {
        "schema": MUTATION_CLOSURE_SCHEMA,
        "selected_graph_unique_ids": list(selected),
        "selected_mutating_node_ids": list(mutating),
        "transitive_node_ids": list(transitive),
        "issues": [issue.to_jsonable() for issue in _ordered_issues(issues)],
    }
    ordered_issues = _ordered_issues(issues)
    return MutationClosureReport(
        status=proof_status(ordered_issues),
        selected_mutating_node_ids=mutating,
        transitive_node_ids=transitive,
        issues=ordered_issues,
        proof_sha256=canonical_fingerprint(payload),
    )


def _managed_model_issues(
    unique_id: str,
    node: Mapping[str, Any],
) -> list[SemanticRefreshProofIssue]:
    issues: list[SemanticRefreshProofIssue] = []
    config = node.get("config")
    if not isinstance(config, Mapping):
        return [_unsupported(unique_id, "config", "be an object")]
    expectations: tuple[tuple[str, object], ...] = (
        ("enabled", True),
        ("materialized", "incremental"),
        ("incremental_strategy", "dpone_scope_merge"),
        ("on_schema_change", "fail"),
        ("pre-hook", []),
        ("post-hook", []),
    )
    for field, expected in expectations:
        if config.get(field) != expected:
            issues.append(_unsupported(unique_id, f"config.{field}", f"equal {expected!r}"))
    contract = config.get("contract")
    if not isinstance(contract, Mapping) or contract.get("enforced") is not True:
        issues.append(_unsupported(unique_id, "config.contract", "enforce the model contract"))
    if node.get("language") != "sql":
        issues.append(_unsupported(unique_id, "language", "equal 'sql'"))
    publish = node.get("meta")
    dpone = publish.get("dpone") if isinstance(publish, Mapping) else None
    publish_config = dpone.get("publish") if isinstance(dpone, Mapping) else None
    if not isinstance(publish_config, Mapping) or publish_config.get("enabled") is not True:
        issues.append(_unsupported(unique_id, "meta.dpone.publish.enabled", "equal true"))
    return issues


def _data_test_issues(unique_id: str, node: Mapping[str, Any]) -> list[SemanticRefreshProofIssue]:
    config = node.get("config")
    if not isinstance(config, Mapping):
        return [_unsupported_test(unique_id, "config")]
    unsafe = (
        config.get("enabled") is not True
        or config.get("materialized") != "test"
        or config.get("store_failures") not in (None, False)
        or config.get("pre-hook", []) != []
        or config.get("post-hook", []) != []
    )
    return [_unsupported_test(unique_id, "config.store_failures")] if unsafe else []


def _unit_test_issues(unique_id: str, node: Mapping[str, Any]) -> list[SemanticRefreshProofIssue]:
    config = node.get("config")
    if not isinstance(config, Mapping) or config.get("enabled") is not True:
        return [_unsupported_test(unique_id, "config.enabled")]
    return []


def _transitive_dependencies(
    sections: tuple[Mapping[str, Any], Mapping[str, Any]],
    roots: tuple[str, ...],
    selected: set[str],
    issues: list[SemanticRefreshProofIssue],
) -> tuple[str, ...]:
    visited: set[str] = set()
    pending = list(reversed(roots))
    while pending:
        unique_id = pending.pop()
        node = _manifest_node(sections, unique_id)
        if node is None:
            if unique_id.startswith("source."):
                continue
            issues.append(_missing_node(unique_id))
            continue
        dependencies = _node_dependencies(unique_id, node, issues)
        for dependency_id in reversed(dependencies):
            if dependency_id in visited:
                continue
            visited.add(dependency_id)
            dependency = _manifest_node(sections, dependency_id)
            if dependency is None:
                if not dependency_id.startswith("source."):
                    issues.append(_missing_node(dependency_id))
                continue
            config = dependency.get("config")
            if (
                dependency.get("resource_type") == "model"
                and isinstance(config, Mapping)
                and config.get("materialized") == "ephemeral"
            ):
                issues.append(_ephemeral_issue(dependency_id))
            pending.append(dependency_id)
    return tuple(sorted(visited - selected))


def _node_dependencies(
    unique_id: str,
    node: Mapping[str, Any],
    issues: list[SemanticRefreshProofIssue],
) -> tuple[str, ...]:
    depends_on = node.get("depends_on")
    values = depends_on.get("nodes") if isinstance(depends_on, Mapping) else None
    if (
        not isinstance(values, Sequence)
        or isinstance(values, str | bytes)
        or any(not isinstance(value, str) or not value for value in values)
    ):
        issues.append(
            unverified(
                _GRAPH_UNVERIFIED,
                "depends_on.nodes",
                "dependency metadata is absent or malformed",
                unique_id=unique_id,
            )
        )
        return ()
    return tuple(sorted(set(values)))


def _manifest_sections(
    manifest: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if not isinstance(manifest, Mapping):
        raise ValueError("dbt manifest must be an object")
    nodes = manifest.get("nodes")
    unit_tests = manifest.get("unit_tests", {})
    if not isinstance(nodes, Mapping) or not isinstance(unit_tests, Mapping):
        raise ValueError("dbt manifest node sections must be objects")
    return nodes, unit_tests


def _manifest_node(
    sections: tuple[Mapping[str, Any], Mapping[str, Any]],
    unique_id: str,
) -> Mapping[str, Any] | None:
    section = sections[1] if unique_id.startswith("unit_test.") else sections[0]
    node = section.get(unique_id)
    return node if isinstance(node, Mapping) and node.get("unique_id") == unique_id else None


def _safe_fqn(value: object) -> TypeGuard[Sequence[str]]:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, str | bytes)
        and bool(value)
        and all(
            isinstance(part, str) and bool(part) and re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", part) is not None
            for part in value
        )
    )


def _require_ids(values: object, field: str) -> tuple[str, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{field} must be a non-empty unique tuple")
    return tuple(sorted(values))


def _unsupported(unique_id: str, field: str, expectation: str) -> SemanticRefreshProofIssue:
    return nonconformant(
        _MODEL_UNSUPPORTED,
        field,
        f"managed semantic refresh model must {expectation}",
        unique_id=unique_id,
    )


def _ephemeral_issue(unique_id: str) -> SemanticRefreshProofIssue:
    return nonconformant(
        _EPHEMERAL,
        "config.materialized",
        "selected and transitive ephemeral models are unsupported",
        unique_id=unique_id,
    )


def _unsupported_test(unique_id: str, field: str) -> SemanticRefreshProofIssue:
    return nonconformant(
        _TEST_MUTATION,
        field,
        "selected tests must be read-only and cannot persist failures or hooks",
        unique_id=unique_id,
    )


def _missing_node(unique_id: str) -> SemanticRefreshProofIssue:
    return unverified(
        _GRAPH_UNVERIFIED,
        "manifest.nodes",
        "the complete dependency node metadata is unavailable",
        unique_id=unique_id,
    )


def _ordered_issues(
    issues: list[SemanticRefreshProofIssue],
) -> tuple[SemanticRefreshProofIssue, ...]:
    return tuple(sorted(set(issues), key=lambda issue: (issue.unique_id or "", issue.field, issue.code)))


__all__ = [
    "MUTATION_CLOSURE_SCHEMA",
    "MutationClosureReport",
    "V2_DBT_INDIRECT_SELECTION",
    "exact_fqn_selectors",
    "prove_mutation_closure",
]
