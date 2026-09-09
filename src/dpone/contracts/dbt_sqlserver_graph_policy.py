"""Pure fail-closed capability policy for a selected dbt SQL Server graph."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypeGuard

from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED,
    DBT_SQLSERVER_GRAPH_POLICY_ERROR_CODES,
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
    DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
    DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW,
    INCREMENTAL_STRATEGIES,
    MODEL_MATERIALIZATIONS,
    ON_SCHEMA_CHANGE_VALUES,
    DbtSqlServerGraphPolicyIssue,
    DbtSqlServerGraphPolicyReport,
    dbt_sqlserver_graph_contract_sha256,
    evaluate_dbt_sqlserver_macro_authority,
    has_workflow_local_test_dependencies,
    model_rule_violations,
    test_rule_violations,
    unit_test_rule_violations,
)
from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    graph_policy_report as _report,
)
from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    manifest_issue as _manifest_issue,
)
from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    rule_issues as _rule_issues,
)
from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    unsupported_issue as _unsupported,
)
from dpone.contracts.dbt_unique_key_policy import (
    evaluate_manifest_dbt_unique_key,
)

_RESOURCE_PREFIXES = frozenset(("model", "seed", "snapshot", "test", "unit_test"))


def evaluate_dbt_sqlserver_selected_graph(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: tuple[str, ...],
    *,
    expected_logical_target: tuple[str, str] | None = None,
) -> DbtSqlServerGraphPolicyReport:
    """Evaluate every exact selected ID without I/O or manifest mutation."""

    issues: list[DbtSqlServerGraphPolicyIssue] = []
    if not isinstance(manifest, Mapping):
        return _report([_manifest_issue("manifest", "be an object")])
    nodes = _section(manifest, "nodes", issues)
    unit_tests = _section(manifest, "unit_tests", issues)
    selected = _selected_ids(selected_graph_unique_ids, issues)
    selected_set = set(selected)
    selected_model_ids = frozenset(unique_id for unique_id in selected if unique_id.startswith("model."))
    admitted_models: dict[str, str] = {}
    model_targets: dict[str, tuple[str, str] | None] = {}
    pending_unit_tests: list[tuple[str, Mapping[str, Any]]] = []

    for unique_id in selected:
        prefix = unique_id.partition(".")[0]
        if (
            prefix not in _RESOURCE_PREFIXES
            or not unique_id.startswith(f"{prefix}.")
            or not unique_id.removeprefix(f"{prefix}.")
        ):
            issues.append(_unsupported(unique_id, None, "unique_id", "use an admitted resource prefix"))
            continue
        section_name = "unit_tests" if prefix == "unit_test" else "nodes"
        section = unit_tests if prefix == "unit_test" else nodes
        node = section.get(unique_id)
        if not isinstance(node, Mapping):
            issues.append(_unsupported(unique_id, None, f"manifest.{section_name}", "contain the selected resource"))
            continue
        if node.get("unique_id") != unique_id:
            issues.append(_unsupported(unique_id, node, "unique_id", "match the selected manifest key"))
            continue
        if node.get("resource_type") != prefix:
            issues.append(_unsupported(unique_id, node, "resource_type", f"match the {prefix!r} ID prefix"))
            continue
        if prefix == "model":
            model_issues = _model_issues(unique_id, node)
            issues.extend(model_issues)
            if not model_issues:
                model_name = node.get("name")
                admitted_models[unique_id] = model_name if isinstance(model_name, str) else ""
                model_targets[unique_id] = _logical_target(node)
        elif prefix == "test":
            issues.extend(
                _data_test_issues(
                    unique_id,
                    node,
                    selected_model_ids=selected_model_ids,
                )
            )
        elif prefix == "unit_test":
            pending_unit_tests.append((unique_id, node))
        else:
            issues.append(_unsupported(unique_id, node, "resource_type", "be model, test or unit_test"))

    for unique_id, unit_test in pending_unit_tests:
        issues.extend(_unit_test_issues(unique_id, unit_test, selected_set, admitted_models))
    issues.extend(
        _logical_target_issues(
            nodes,
            model_targets,
            expected_logical_target=expected_logical_target,
        )
    )
    issues.extend(_project_operation_issues(nodes))
    issues.extend(_macro_authority_issues(manifest, selected))
    return _report(issues)


def _section(
    manifest: Mapping[str, Any],
    name: str,
    issues: list[DbtSqlServerGraphPolicyIssue],
) -> Mapping[str, Any]:
    value = manifest.get(name)
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    issues.append(_manifest_issue(f"manifest.{name}", "be an object"))
    return {}


def _selected_ids(
    value: object,
    issues: list[DbtSqlServerGraphPolicyIssue],
) -> tuple[str, ...]:
    if not isinstance(value, tuple) or not value:
        issues.append(_manifest_issue("selected_graph_unique_ids", "be a non-empty tuple"))
        return ()
    valid = tuple(item for item in value if isinstance(item, str) and item)
    if len(valid) != len(value):
        issues.append(_manifest_issue("selected_graph_unique_ids", "contain only non-empty strings"))
    if len(valid) != len(set(valid)):
        issues.append(_manifest_issue("selected_graph_unique_ids", "contain no duplicates"))
    return tuple(sorted(set(valid)))


def _model_issues(unique_id: str, node: Mapping[str, Any]) -> list[DbtSqlServerGraphPolicyIssue]:
    issues, config = _base_node_issues(unique_id, node)
    if node.get("language") != "sql":
        issues.append(_unsupported(unique_id, node, "language", "equal 'sql'"))
    if config is None:
        return issues
    materialized = config.get("materialized")
    if materialized not in MODEL_MATERIALIZATIONS:
        issues.append(_unsupported(unique_id, node, "config.materialized", "equal 'table', 'view' or 'incremental'"))
        return issues
    if config.get("as_columnstore") is not False:
        issues.append(_unsupported(unique_id, node, "config.as_columnstore", "equal false explicitly"))
    issues.extend(_rule_issues(model_rule_violations(unique_id, node, config)))
    indexes = config.get("indexes")
    if indexes is not None and not (_is_sequence(indexes) and not indexes):
        issues.append(_unsupported(unique_id, node, "config.indexes", "be absent, null or empty"))
    for field in ("drop_unmanaged_indexes", "prefer_single_alter_column"):
        value = config.get(field)
        if value is not None and value is not False:
            issues.append(_unsupported(unique_id, node, f"config.{field}", "be absent, null or false"))
    if materialized == "table" and config.get("table_refresh_method", "rename") != "rename":
        issues.append(_unsupported(unique_id, node, "config.table_refresh_method", "be absent or equal 'rename'"))
    if materialized == "incremental":
        issues.extend(_incremental_issues(unique_id, node, config))
    return issues


def _incremental_issues(
    unique_id: str,
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[DbtSqlServerGraphPolicyIssue]:
    issues: list[DbtSqlServerGraphPolicyIssue] = []
    strategy = config.get("incremental_strategy")
    if strategy not in INCREMENTAL_STRATEGIES:
        issues.append(_unsupported(unique_id, node, "config.incremental_strategy", "equal 'append' or 'merge'"))
    elif strategy == "merge":
        key_report = evaluate_manifest_dbt_unique_key(
            config.get("unique_key"),
            node=node,
            config=config,
        )
        issues.extend(
            _unsupported(
                unique_id,
                node,
                "config.unique_key",
                issue.expectation,
                code=issue.code,
            )
            for issue in key_report.issues
        )
    if config.get("on_schema_change") not in ON_SCHEMA_CHANGE_VALUES:
        issues.append(_unsupported(unique_id, node, "config.on_schema_change", "equal 'ignore' or 'fail'"))
    return issues


def _data_test_issues(
    unique_id: str,
    node: Mapping[str, Any],
    *,
    selected_model_ids: frozenset[str],
) -> list[DbtSqlServerGraphPolicyIssue]:
    issues, config = _base_node_issues(unique_id, node)
    if node.get("language") != "sql":
        issues.append(_unsupported(unique_id, node, "language", "equal 'sql'"))
    if config is None:
        return issues
    if config.get("materialized") != "test":
        issues.append(_unsupported(unique_id, node, "config.materialized", "equal 'test'"))
    issues.extend(_rule_issues(test_rule_violations(unique_id, node, config)))
    store_failures = config.get("store_failures")
    if store_failures is not None and store_failures is not False:
        issues.append(_unsupported(unique_id, node, "config.store_failures", "be absent, null or false"))
    if not has_workflow_local_test_dependencies(
        node,
        selected_model_ids=selected_model_ids,
    ):
        issues.append(
            _unsupported(
                unique_id,
                node,
                "depends_on.nodes",
                "contain one or more models from this workflow closure only",
                code=DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW,
            )
        )
    return issues


def _unit_test_issues(
    unique_id: str,
    node: Mapping[str, Any],
    selected: set[str],
    admitted_models: Mapping[str, str],
) -> list[DbtSqlServerGraphPolicyIssue]:
    issues, config = _base_node_issues(unique_id, node)
    if "language" in node:
        issues.append(_unsupported(unique_id, node, "language", "be absent for a standard unit test"))
    if config is not None and "materialized" in config:
        issues.append(_unsupported(unique_id, node, "config.materialized", "be absent for a standard unit test"))
    if config is not None:
        issues.extend(_rule_issues(unit_test_rule_violations(unique_id, node, config)))
    depends_on = node.get("depends_on")
    values = depends_on.get("nodes") if isinstance(depends_on, Mapping) else None
    dependencies = tuple(values) if _is_sequence(values) else ()
    models = tuple(item for item in dependencies if isinstance(item, str) and item.startswith("model."))
    attached_name = node.get("model")
    attachment_valid = (
        len(models) == 1
        and models[0] in selected
        and isinstance(attached_name, str)
        and bool(attached_name.strip())
        and admitted_models.get(models[0]) == attached_name
    )
    if not attachment_valid:
        issues.append(
            _unsupported(
                unique_id,
                node,
                "depends_on.nodes",
                "name one selected model admitted by the SQL policy",
            )
        )
    return issues


def _base_node_issues(
    unique_id: str,
    node: Mapping[str, Any],
) -> tuple[list[DbtSqlServerGraphPolicyIssue], Mapping[str, Any] | None]:
    issues: list[DbtSqlServerGraphPolicyIssue] = []
    config = node.get("config")
    if not isinstance(config, Mapping):
        return [_unsupported(unique_id, node, "config", "be an object")], None
    if config.get("enabled") is not True:
        issues.append(_unsupported(unique_id, node, "config.enabled", "equal true"))
    for hook in ("pre-hook", "post-hook"):
        if hook in config and not (_is_sequence(config.get(hook)) and not config.get(hook)):
            issues.append(_unsupported(unique_id, node, f"config.{hook}", "be absent or empty"))
    if "grants" in config:
        grants = config.get("grants")
        if not isinstance(grants, Mapping) or grants:
            issues.append(_unsupported(unique_id, node, "config.grants", "be absent or empty"))
    full_refresh = config.get("full_refresh")
    if full_refresh is not None and full_refresh is not False:
        issues.append(_unsupported(unique_id, node, "config.full_refresh", "be absent, null or false"))
    return issues, config


def _project_operation_issues(nodes: Mapping[str, Any]) -> list[DbtSqlServerGraphPolicyIssue]:
    return [
        _unsupported(unique_id, node, "resource_type", "exclude project-level on-run hooks")
        for unique_id, node in nodes.items()
        if isinstance(unique_id, str) and isinstance(node, Mapping) and node.get("resource_type") == "operation"
    ]


def _macro_authority_issues(
    manifest: Mapping[str, Any],
    selected: tuple[str, ...],
) -> list[DbtSqlServerGraphPolicyIssue]:
    raw_nodes = manifest.get("nodes")
    raw_unit_tests = manifest.get("unit_tests")
    nodes = raw_nodes if isinstance(raw_nodes, Mapping) else {}
    unit_tests = raw_unit_tests if isinstance(raw_unit_tests, Mapping) else {}
    authority_selected = tuple(
        unique_id for unique_id in selected if _has_exact_executable_identity(unique_id, nodes, unit_tests)
    )
    if not authority_selected:
        return []
    report = evaluate_dbt_sqlserver_macro_authority(
        manifest,
        authority_selected,
    )
    sections = tuple(
        value for name in ("nodes", "unit_tests", "macros") if isinstance((value := manifest.get(name)), Mapping)
    )
    issues = []
    for issue in report.issues:
        raw = next(
            (section.get(issue.unique_id) for section in sections if isinstance(section.get(issue.unique_id), Mapping)),
            None,
        )
        issues.append(
            _unsupported(
                issue.unique_id,
                raw if isinstance(raw, Mapping) else None,
                issue.field,
                issue.expectation,
                code=issue.code,
            )
        )
    return issues


def _has_exact_executable_identity(
    unique_id: str,
    nodes: Mapping[str, Any],
    unit_tests: Mapping[str, Any],
) -> bool:
    prefix = unique_id.partition(".")[0]
    if prefix not in {"model", "test", "unit_test"}:
        return False
    section = unit_tests if prefix == "unit_test" else nodes
    node = section.get(unique_id)
    return isinstance(node, Mapping) and node.get("unique_id") == unique_id and node.get("resource_type") == prefix


def _logical_target(node: Mapping[str, Any]) -> tuple[str, str] | None:
    database = node.get("database")
    schema = node.get("schema")
    if not isinstance(database, str) or not database.strip() or not isinstance(schema, str) or not schema.strip():
        return None
    return database.strip(), schema.strip()


def _logical_target_issues(
    nodes: Mapping[str, Any],
    targets: Mapping[str, tuple[str, str] | None],
    *,
    expected_logical_target: tuple[str, str] | None,
) -> list[DbtSqlServerGraphPolicyIssue]:
    if not targets:
        return []
    expected = expected_logical_target
    if expected is None:
        observed = sorted({target for target in targets.values() if target is not None})
        expected = observed[0] if len(observed) == 1 else None
    issues: list[DbtSqlServerGraphPolicyIssue] = []
    for unique_id, target in sorted(targets.items()):
        if target is None or expected is None or target != expected:
            node = nodes.get(unique_id)
            issues.append(
                _unsupported(
                    unique_id,
                    node if isinstance(node, Mapping) else None,
                    "database/schema",
                    "match the workflow publish-model logical target",
                )
            )
    return issues


def _is_sequence(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, Sequence) and not isinstance(value, str | bytes)


__all__ = [
    "DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED",
    "DBT_SQLSERVER_GRAPH_POLICY_ERROR_CODES",
    "DBT_SQLSERVER_GRAPH_POLICY_ID",
    "DBT_SQLSERVER_GRAPH_POLICY_SHA256",
    "DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED",
    "DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW",
    "DbtSqlServerGraphPolicyIssue",
    "DbtSqlServerGraphPolicyReport",
    "dbt_sqlserver_graph_contract_sha256",
    "evaluate_dbt_sqlserver_selected_graph",
]
