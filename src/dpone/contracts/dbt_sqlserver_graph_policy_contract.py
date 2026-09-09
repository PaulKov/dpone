"""Stable identity and public error surface for the SQL Server graph policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts import dbt_sqlserver_graph_rules as graph_rules
from dpone.contracts.dbt_graph_contract import dbt_graph_contract_sha256
from dpone.contracts.dbt_sqlserver_macro_authority import (
    DBT_SQLSERVER_MACRO_AUTHORITY_INVALID,
    DBT_SQLSERVER_MACRO_AUTHORITY_POLICY_PAYLOAD,
    evaluate_dbt_sqlserver_macro_authority,
)
from dpone.contracts.dbt_unique_key_policy import (
    DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED,
    DBT_UNIQUE_KEY_INVALID,
    DBT_UNIQUE_KEY_MISSING,
    DBT_UNIQUE_KEY_NOT_IN_CONTRACT,
    DBT_UNIQUE_KEY_NULLABLE,
)
from dpone.contracts.dbt_workflow_graph_policy import (
    DBT_WORKFLOW_GRAPH_POLICY_PAYLOAD,
)

DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED = graph_rules.DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED
DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED = graph_rules.DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED
DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW = "DPONE_DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW"
DBT_TEST_DEPENDENCY_POLICY_PAYLOAD: Mapping[str, object] = {
    "indirect_selection": "eager",
    "minimum_model_dependencies": 1,
    "model_dependencies": "subset_of_current_workflow_model_closure",
    "attached_node": "absent_or_local_model_dependency",
}
DBT_SQLSERVER_GRAPH_POLICY_ID = "dpone.dbt-sqlserver-selected-graph-policy.v1"
DBT_SQLSERVER_GRAPH_POLICY_ERROR_CODES = frozenset(
    {
        DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED,
        DBT_SQLSERVER_MACRO_AUTHORITY_INVALID,
        DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
        DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW,
        DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED,
        DBT_UNIQUE_KEY_INVALID,
        DBT_UNIQUE_KEY_MISSING,
        DBT_UNIQUE_KEY_NOT_IN_CONTRACT,
        DBT_UNIQUE_KEY_NULLABLE,
    }
)

MODEL_MATERIALIZATIONS = ("table", "view", "incremental")
INCREMENTAL_STRATEGIES = ("append", "merge")
ON_SCHEMA_CHANGE_VALUES = ("ignore", "fail")

DBT_SQLSERVER_GRAPH_POLICY_PAYLOAD: Mapping[str, Any] = {
    "policy_id": DBT_SQLSERVER_GRAPH_POLICY_ID,
    "identity": {
        "selected_graph": "non_empty_unique_tuple",
        "node_unique_id_matches_manifest_key": True,
        "id_prefix_matches_resource_type": True,
    },
    "resources": {
        "model": {
            "language": ["sql"],
            "materialized": list(MODEL_MATERIALIZATIONS),
            "table_refresh_method": {
                "absent_normalizes_to": "rename",
                "allowed": ["rename"],
            },
            "as_columnstore": {"required": True, "allowed": [False]},
            "indexes": {"absent_or_empty": True},
            "drop_unmanaged_indexes": {"absent_or_null_or_false": True},
            "prefer_single_alter_column": {"absent_or_null_or_false": True},
            "incremental_strategy": list(INCREMENTAL_STRATEGIES),
            "merge_unique_key": {
                "identifier_only": True,
                "distinct_casefold": True,
                "enforced_contract_membership": True,
                "column_not_null_constraint": True,
            },
            "on_schema_change": list(ON_SCHEMA_CHANGE_VALUES),
            "logical_target": "matches_publish_model_target",
        },
        "test": {
            "language": ["sql"],
            "materialized": ["test"],
            "store_failures": {
                "absent_or_null_normalizes_to": False,
                "allowed": [False],
            },
        },
        "unit_test": {
            "language": "absent",
            "materialized": "absent",
            "model_field_matches_single_selected_admitted_model": True,
        },
        "seed": {"supported": False},
        "snapshot": {"supported": False},
    },
    "safety": {
        "config": "mapping",
        "enabled": "literal_true",
        "pre_hook": "absent_or_empty",
        "post_hook": "absent_or_empty",
        "grants": "absent_or_empty_mapping",
        "full_refresh": "absent_or_null_or_literal_false",
        "project_operations": False,
    },
    "closed_graph_rules": graph_rules.DBT_SQLSERVER_GRAPH_RULES_PAYLOAD,
    "macro_authority": DBT_SQLSERVER_MACRO_AUTHORITY_POLICY_PAYLOAD,
    "test_dependency_policy": DBT_TEST_DEPENDENCY_POLICY_PAYLOAD,
    "workflow_ownership": DBT_WORKFLOW_GRAPH_POLICY_PAYLOAD,
}
DBT_SQLSERVER_GRAPH_POLICY_SHA256 = graph_rules.sqlserver_policy_fingerprint(DBT_SQLSERVER_GRAPH_POLICY_PAYLOAD)


def has_workflow_local_test_dependencies(
    node: Mapping[str, Any],
    *,
    selected_model_ids: frozenset[str],
) -> bool:
    """Return whether every model read by a data test is in this workflow."""

    depends_on = node.get("depends_on")
    raw_nodes = depends_on.get("nodes") if isinstance(depends_on, Mapping) else None
    if not isinstance(raw_nodes, Sequence) or isinstance(raw_nodes, str | bytes):
        return False
    model_dependencies = tuple(value for value in raw_nodes if isinstance(value, str) and value.startswith("model."))
    if (
        not model_dependencies
        or len(model_dependencies) != len(set(model_dependencies))
        or any(value not in selected_model_ids for value in model_dependencies)
    ):
        return False
    attached_node = node.get("attached_node")
    return attached_node is None or (
        isinstance(attached_node, str) and attached_node in model_dependencies and attached_node in selected_model_ids
    )


def dbt_sqlserver_graph_contract_sha256(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: tuple[str, ...],
) -> str:
    """Fingerprint selected semantics and their exact executable macro projection."""

    authority = evaluate_dbt_sqlserver_macro_authority(
        manifest,
        selected_graph_unique_ids,
    )
    if not authority.passed or authority.projection_sha256 is None:
        raise ValueError("dbt SQL Server macro authority is unavailable or invalid")
    return dbt_graph_contract_sha256(
        manifest,
        selected_graph_unique_ids,
        policy_projection={
            "dbt_sqlserver_macro_authority": {
                "baseline_sha256": authority.baseline_sha256,
                "projection_sha256": authority.projection_sha256,
            }
        },
    )


@dataclass(frozen=True, slots=True)
class DbtSqlServerGraphPolicyIssue:
    """One stable, node-scoped explanation of an unsupported capability."""

    code: str
    unique_id: str
    field: str
    message: str
    path: str
    remediation: str

    def to_jsonable(self) -> dict[str, str]:
        """Return a deterministic public issue payload."""

        return {
            "code": self.code,
            "unique_id": self.unique_id,
            "field": self.field,
            "message": self.message,
            "path": self.path,
            "remediation": self.remediation,
        }


@dataclass(frozen=True, slots=True)
class DbtSqlServerGraphPolicyReport:
    """Immutable outcome of evaluating one complete exact selected graph."""

    policy_id: str
    policy_sha256: str
    issues: tuple[DbtSqlServerGraphPolicyIssue, ...]

    @property
    def passed(self) -> bool:
        """Return whether the complete graph is admitted."""

        return not self.issues

    def to_jsonable(self) -> dict[str, object]:
        """Return a stable report without manifest contents."""

        return {
            "policy_id": self.policy_id,
            "policy_sha256": self.policy_sha256,
            "passed": self.passed,
            "issues": [issue.to_jsonable() for issue in self.issues],
        }


def unsupported_issue(
    unique_id: str,
    node: Mapping[str, Any] | None,
    field: str,
    expectation: str,
    *,
    code: str = DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED,
) -> DbtSqlServerGraphPolicyIssue:
    """Render one content-free, actionable policy issue."""

    path_value = None
    if node is not None:
        constraint_field = field == "constraints" or field.startswith("columns.")
        primary = "patch_path" if constraint_field else "original_file_path"
        fallback = "original_file_path" if constraint_field else "patch_path"
        path_value = node.get(primary) or node.get(fallback)
    path = path_value if isinstance(path_value, str) and path_value else f"manifest.json:{unique_id}"
    if code == DBT_SQLSERVER_MACRO_AUTHORITY_INVALID:
        message = f"SQL Server v1 cannot prove macro authority for {unique_id!r} at {field!r}."
        remediation = (
            "Restore the certified dbt Core and dbt-sqlserver versions and "
            "reviewed package source, run `dbt deps` when declarations changed, "
            "then run `dbt parse` and retry; do not edit manifest.json or the "
            "generated baseline."
        )
    elif code == DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED:
        message = f"SQL Server v1 does not admit unsupported physical constraints for {unique_id!r} at {field!r}."
        remediation = (
            "Remove the unsupported physical constraint declaration, keep the "
            "dbt contract, and express validation with admitted data or unit "
            "tests; then compile a new immutable selection."
        )
    elif code == DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW:
        message = f"SQL Server v1 test {unique_id!r} reads a model outside its locked workflow closure."
        remediation = (
            "Move every model dependency into one workflow closure or rewrite "
            "the assertion as a same-workflow test; run `dbt parse` and retry."
        )
    elif field == "config.unique_key":
        message = f"SQL Server v1 does not admit the merge key for {unique_id!r} at {field!r}: it must {expectation}."
        remediation = (
            "Use distinct exact contracted identifier columns with column-level "
            "`not_null`, run `dbt parse`, and compile a new immutable selection."
        )
    elif field == "manifest.macros":
        message = "SQL Server v1 cannot prove the exact pinned dbt-sqlserver macro set."
        remediation = (
            "Restore the certified dbt Core and dbt-sqlserver versions, run "
            "`dbt deps` when package declarations changed, then run `dbt parse` "
            "and retry; do not edit manifest.json."
        )
    elif field.startswith("macros."):
        message = f"SQL Server v1 does not admit adapter macro shadowing at {field!r}."
        remediation = (
            "Remove or rename the project/package macro that shadows the pinned "
            "dbt-sqlserver adapter macro, run `dbt parse`, and retry."
        )
    else:
        message = f"SQL Server v1 does not admit {unique_id!r}: {field} must {expectation}."
        remediation = f"Configure {field} to {expectation}, then compile a new immutable selection."
    return DbtSqlServerGraphPolicyIssue(
        code=code,
        unique_id=unique_id,
        field=field,
        message=message,
        path=path,
        remediation=remediation,
    )


def rule_issues(
    violations: tuple[graph_rules.DbtSqlServerGraphRuleViolation, ...],
) -> list[DbtSqlServerGraphPolicyIssue]:
    """Translate pure closed-surface rule violations."""

    return [
        unsupported_issue(
            violation.unique_id,
            violation.node,
            violation.field,
            violation.expectation,
            code=violation.code,
        )
        for violation in violations
    ]


def manifest_issue(
    field: str,
    expectation: str,
) -> DbtSqlServerGraphPolicyIssue:
    """Render a manifest-envelope violation."""

    return unsupported_issue("<manifest>", None, field, expectation)


def graph_policy_report(
    issues: list[DbtSqlServerGraphPolicyIssue],
) -> DbtSqlServerGraphPolicyReport:
    """Deduplicate and sort issues under the current policy identity."""

    distinct = {
        (
            issue.code,
            issue.unique_id,
            issue.field,
            issue.message,
            issue.path,
            issue.remediation,
        ): issue
        for issue in issues
    }
    return DbtSqlServerGraphPolicyReport(
        policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        issues=tuple(distinct[key] for key in sorted(distinct)),
    )


model_rule_violations = graph_rules.model_rule_violations
test_rule_violations = graph_rules.test_rule_violations
unit_test_rule_violations = graph_rules.unit_test_rule_violations


__all__ = [
    "DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED",
    "DBT_SQLSERVER_GRAPH_POLICY_ERROR_CODES",
    "DBT_SQLSERVER_GRAPH_POLICY_ID",
    "DBT_SQLSERVER_GRAPH_POLICY_PAYLOAD",
    "DBT_SQLSERVER_GRAPH_POLICY_SHA256",
    "DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED",
    "DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW",
    "DBT_TEST_DEPENDENCY_POLICY_PAYLOAD",
    "DbtSqlServerGraphPolicyIssue",
    "DbtSqlServerGraphPolicyReport",
    "INCREMENTAL_STRATEGIES",
    "MODEL_MATERIALIZATIONS",
    "ON_SCHEMA_CHANGE_VALUES",
    "dbt_sqlserver_graph_contract_sha256",
    "evaluate_dbt_sqlserver_macro_authority",
    "graph_policy_report",
    "has_workflow_local_test_dependencies",
    "manifest_issue",
    "model_rule_violations",
    "rule_issues",
    "test_rule_violations",
    "unit_test_rule_violations",
    "unsupported_issue",
]
