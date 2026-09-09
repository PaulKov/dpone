"""Pure project-level ownership policy for dbt workflow model closures."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED = "DPONE_DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED"
DBT_WORKFLOW_GRAPH_OVERLAP = "DPONE_DBT_WORKFLOW_GRAPH_OVERLAP"

DBT_WORKFLOW_GRAPH_POLICY_PAYLOAD: Mapping[str, object] = {
    "publish_model_owner": "exactly_one_workflow",
    "foreign_publish_model_in_closure": False,
    "materialized_model_closure_overlap": False,
    "shared_non_model_resources": True,
}


@dataclass(frozen=True, slots=True)
class DbtWorkflowGraphPolicyIssue:
    """One deterministic project-level workflow ownership violation."""

    code: str
    unique_id: str
    workflow: str | None
    owner_workflow: str | None
    workflows: tuple[str, ...]
    message: str
    path: str
    remediation: str


@dataclass(frozen=True, slots=True)
class DbtWorkflowGraphPolicyReport:
    """Result of comparing every materialized model workflow closure."""

    issues: tuple[DbtWorkflowGraphPolicyIssue, ...]

    @property
    def passed(self) -> bool:
        """Return whether every workflow owns a disjoint model closure."""

        return not self.issues


def evaluate_dbt_workflow_graph_ownership(
    *,
    publish_model_ids_by_workflow: Mapping[str, tuple[str, ...]],
    selected_graph_ids_by_workflow: Mapping[str, tuple[str, ...]],
) -> DbtWorkflowGraphPolicyReport:
    """Reject foreign publish ownership and shared materialized model closures."""

    publish_models = _normalized_models_by_workflow(
        publish_model_ids_by_workflow,
        field="publish_model_ids_by_workflow",
    )
    selected_models = _normalized_models_by_workflow(
        selected_graph_ids_by_workflow,
        field="selected_graph_ids_by_workflow",
        ignore_non_models=True,
    )
    if set(publish_models) != set(selected_models):
        raise ValueError("dbt workflow graph policy requires matching workflow sets")

    publish_owners = _publish_owners(publish_models)
    closure_owners = _closure_owners(selected_models)
    issues: list[DbtWorkflowGraphPolicyIssue] = []

    for workflow, models in selected_models.items():
        for unique_id in models:
            owner = publish_owners.get(unique_id)
            if owner is None or owner == workflow:
                continue
            issues.append(
                DbtWorkflowGraphPolicyIssue(
                    code=DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED,
                    unique_id=unique_id,
                    workflow=workflow,
                    owner_workflow=owner,
                    workflows=(owner, workflow),
                    message=(
                        f"Workflow '{workflow}' closure contains publish-enabled "
                        f"model '{unique_id}' owned by workflow '{owner}'; "
                        "cross-workflow publish dependencies are unsupported."
                    ),
                    path=f"workflow:{workflow}",
                    remediation=(
                        "Move the dependent publish models into one workflow, or "
                        "replace the cross-workflow ref() with a separately "
                        "governed source boundary; then run dbt parse and compile "
                        "a new immutable release."
                    ),
                )
            )

    for unique_id, workflows in closure_owners.items():
        if unique_id in publish_owners or len(workflows) < 2:
            continue
        joined = ", ".join(f"'{item}'" for item in workflows)
        issues.append(
            DbtWorkflowGraphPolicyIssue(
                code=DBT_WORKFLOW_GRAPH_OVERLAP,
                unique_id=unique_id,
                workflow=None,
                owner_workflow=None,
                workflows=workflows,
                message=(f"Materialized selected model '{unique_id}' belongs to multiple workflow closures: {joined}."),
                path=unique_id,
                remediation=(
                    "Make the materialized model closures disjoint or merge the "
                    "workflows; do not use an ephemeral model as a workaround. "
                    "Run dbt parse and compile into a new empty output directory."
                ),
            )
        )
    return DbtWorkflowGraphPolicyReport(tuple(issues))


def _normalized_models_by_workflow(
    values: Mapping[str, tuple[str, ...]],
    *,
    field: str,
    ignore_non_models: bool = False,
) -> dict[str, tuple[str, ...]]:
    if not isinstance(values, Mapping) or not values:
        raise ValueError(f"{field} must be a non-empty mapping")
    result: dict[str, tuple[str, ...]] = {}
    for workflow, raw_ids in sorted(values.items()):
        if (
            not isinstance(workflow, str)
            or not workflow
            or not isinstance(raw_ids, tuple)
            or not raw_ids
            or any(not isinstance(item, str) or not item for item in raw_ids)
            or len(raw_ids) != len(set(raw_ids))
        ):
            raise ValueError(f"{field} contains an invalid workflow selection")
        model_ids = tuple(sorted(item for item in raw_ids if item.startswith("model.")))
        if not ignore_non_models and len(model_ids) != len(raw_ids):
            raise ValueError(f"{field} must contain only dbt model identities")
        if not model_ids:
            raise ValueError(f"{field} contains no selected model")
        result[workflow] = model_ids
    return result


def _publish_owners(
    values: Mapping[str, tuple[str, ...]],
) -> dict[str, str]:
    owners: dict[str, str] = {}
    for workflow, models in values.items():
        for unique_id in models:
            existing = owners.setdefault(unique_id, workflow)
            if existing != workflow:
                raise ValueError("one publish-enabled dbt model cannot have multiple workflow owners")
    return owners


def _closure_owners(
    values: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    owners: dict[str, set[str]] = {}
    for workflow, models in values.items():
        for unique_id in models:
            owners.setdefault(unique_id, set()).add(workflow)
    return {unique_id: tuple(sorted(workflows)) for unique_id, workflows in sorted(owners.items())}


__all__ = [
    "DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED",
    "DBT_WORKFLOW_GRAPH_OVERLAP",
    "DBT_WORKFLOW_GRAPH_POLICY_PAYLOAD",
    "DbtWorkflowGraphPolicyIssue",
    "DbtWorkflowGraphPolicyReport",
    "evaluate_dbt_workflow_graph_ownership",
]
