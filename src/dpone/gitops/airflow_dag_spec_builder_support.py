"""Shared helpers for GitOps Airflow dag-spec builder modules."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import yaml

from dpone.gitops.airflow_asset_graph import AssetGraphReport
from dpone.gitops.airflow_dag_spec import (
    DAG_SPEC_PRODUCER,
    DagScheduleAssets,
    DagSpecDeclaration,
    DagSpecNode,
    parse_dag_declaration,
)
from dpone.gitops.airflow_process_identity import resolve_airflow_process_identities
from dpone.gitops.airflow_step_visibility import (
    AirflowStepVisibilityError,
    VisibleTaskBudget,
    build_visible_task_plan,
    estimate_visible_tasks,
    resolve_step_visibility,
    separate_airflow_hook_count,
)
from dpone.gitops.workload_catalog_models import (
    GitOpsWorkloadCatalogIssue,
    GitOpsWorkloadCatalogReport,
    GitOpsWorkloadDefinition,
    issue,
)
from dpone.manifest.loader import ManifestConfigurationError

BATCH_MANIFEST_KIND = "dpone.batch.v1"


class AirflowDagSpecInputError(ValueError):
    """A workload-set or included domain document cannot be read safely."""


@dataclass(frozen=True, slots=True)
class DomainDocument:
    path: str
    payload: Mapping[str, Any]


class ManifestMetadataLoader(Protocol):
    def load(self, path: Path, *, metadata_only: bool) -> Any: ...


def build_workload_nodes(
    *,
    loader: ManifestMetadataLoader,
    repo_root: Path,
    declaration: DagSpecDeclaration,
    workload: GitOpsWorkloadDefinition,
    dependencies: dict[str, tuple[Any, ...]],
) -> tuple[tuple[DagSpecNode, ...], list[GitOpsWorkloadCatalogIssue]]:
    """Load one workload's process metadata and build its DAG-spec nodes."""

    try:
        manifest = loader.load(repo_root / workload.manifest, metadata_only=True)
    except (ManifestConfigurationError, OSError, UnicodeError) as exc:
        return (), [
            dag_spec_blocker(
                declaration.dag_id,
                "dag_spec_manifest_invalid",
                f"Manifest {workload.manifest} of workload {workload.workload_id} failed to load: {exc}",
            )
        ]
    nodes: list[DagSpecNode] = []
    for identity in resolve_airflow_process_identities(manifest, workload_id=workload.workload_id):
        process = identity.process
        node, blocker = _build_process_node(
            declaration=declaration,
            workload=workload,
            process=process,
            node_id=identity.node_id,
            require_selector=identity.selector_required,
        )
        if blocker is not None:
            return (), [blocker]
        assert node is not None
        dependencies[node.node_id] = tuple(getattr(process.config, "dependencies", ()) or ())
        nodes.append(node)
    return tuple(nodes), []


def visible_task_budget_issues(
    *,
    dag_id: str,
    nodes: tuple[DagSpecNode, ...],
    budget: VisibleTaskBudget,
) -> tuple[list[GitOpsWorkloadCatalogIssue], list[GitOpsWorkloadCatalogIssue]]:
    """Return warning and blocker lists for one resolved DAG task plan."""

    task_plan = build_visible_task_plan(
        tuple(node.estimated_visible_tasks for node in nodes),
        budget=budget,
    )
    if task_plan.status == "warning":
        return [
            dag_spec_blocker(
                dag_id,
                "DPONE_AIRFLOW_VISIBLE_TASK_BUDGET_WARNING",
                f"DAG estimates {task_plan.estimated_total} Airflow tasks; warning threshold is {budget.warn}.",
            )
        ], []
    if task_plan.status == "blocked":
        return [], [
            dag_spec_blocker(
                dag_id,
                "DPONE_AIRFLOW_VISIBLE_TASK_BUDGET_EXCEEDED",
                f"DAG estimates {task_plan.estimated_total} Airflow tasks; maximum is {budget.maximum}.",
            )
        ]
    return [], []


def _build_process_node(
    *,
    declaration: DagSpecDeclaration,
    workload: GitOpsWorkloadDefinition,
    process: Any,
    node_id: str,
    require_selector: bool,
) -> tuple[DagSpecNode | None, GitOpsWorkloadCatalogIssue | None]:
    selector = process.selector
    if require_selector and not selector:
        return None, dag_spec_blocker(
            declaration.dag_id,
            "DPONE_AIRFLOW_NODE_SELECTOR_MISSING",
            f"Process {process.name!r} in workload {workload.workload_id!r} has no stable selector.",
        )
    try:
        visibility = resolve_step_visibility(process.raw_config)
        estimated_visible_tasks = estimate_visible_tasks(
            visibility=visibility,
            separate_hook_count=separate_airflow_hook_count(process.raw_config),
        )
    except AirflowStepVisibilityError as exc:
        return None, dag_spec_blocker(declaration.dag_id, exc.code, str(exc))
    return (
        DagSpecNode(
            node_id=node_id,
            workload_id=workload.workload_id,
            selector=selector if require_selector else None,
            task_group=process.task_group,
            visibility=visibility,
            estimated_visible_tasks=estimated_visible_tasks,
        ),
        None,
    )


def member_workload_ids(
    declaration: DagSpecDeclaration, groups: Mapping[str, tuple[str, ...]]
) -> tuple[tuple[str, ...], list[GitOpsWorkloadCatalogIssue]]:
    if declaration.workloads is not None:
        return declaration.workloads, []
    group = declaration.group or ""
    if group not in groups:
        blocker = dag_spec_blocker(
            declaration.dag_id, "dag_spec_group_unknown", f"Unknown workflow group: {group or '<empty>'}"
        )
        return (), [blocker]
    return groups[group], []


def schedule_asset_blockers(
    declaration: DagSpecDeclaration, outlet_uris: frozenset[str]
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    schedule = declaration.schedule
    if not isinstance(schedule, DagScheduleAssets):
        return ()
    return tuple(
        dag_spec_blocker(
            declaration.dag_id,
            "dag_spec_schedule_asset_unknown",
            (
                f"schedule.assets URI {asset.uri!r} is not produced by any workload outlet; "
                "declare it in some manifest's airflow.execution.outlets or mark it external: true"
            ),
        )
        for asset in schedule.assets
        if not asset.external and asset.uri not in outlet_uris
    )


def declared_outlet_uris(catalog: GitOpsWorkloadCatalogReport) -> frozenset[str]:
    uris: set[str] = set()
    for workload in catalog.workloads:
        airflow = mapping(workload.effective_config.get("airflow"))
        execution = mapping(airflow.get("execution"))
        for raw in execution.get("outlets") or ():
            text = str(raw.get("uri") or "").strip() if isinstance(raw, Mapping) else str(raw).strip()
            if text:
                uris.add(text)
    return frozenset(uris)


def materialized_outlet_uris(
    catalog: GitOpsWorkloadCatalogReport,
    asset_graph: AssetGraphReport,
) -> frozenset[str]:
    inferred = {uri for node in asset_graph.nodes for uri in node.sink_uris}
    return declared_outlet_uris(catalog).union(inferred)


def workflow_groups(payload: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    raw_groups = mapping(payload.get("workflow_groups") or payload.get("workload_groups") or payload.get("groups"))
    groups: dict[str, tuple[str, ...]] = {}
    for name, raw_group in raw_groups.items():
        if isinstance(raw_group, Mapping):
            raw_ids = raw_group.get("workload_ids") or raw_group.get("workloads") or ()
        else:
            raw_ids = raw_group or ()
        groups[str(name)] = tuple(str(item) for item in raw_ids)
    return groups


def dags_block(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return mapping(payload.get("dags"))


def dag_spec_blocker(dag_id: str, code: str, message: str) -> GitOpsWorkloadCatalogIssue:
    return issue(code=code, message=message, path=dag_id, source=DAG_SPEC_PRODUCER)


def load_yaml(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        return mapping(yaml.safe_load(path.read_text(encoding="utf-8")) or {})
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise AirflowDagSpecInputError("dag_spec_input_invalid") from exc


def mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


__all__ = [
    "AirflowDagSpecInputError",
    "BATCH_MANIFEST_KIND",
    "DagSpecDeclaration",
    "DomainDocument",
    "ManifestMetadataLoader",
    "build_workload_nodes",
    "dag_spec_blocker",
    "dags_block",
    "declared_outlet_uris",
    "load_yaml",
    "mapping",
    "materialized_outlet_uris",
    "member_workload_ids",
    "optional_str",
    "parse_dag_declaration",
    "schedule_asset_blockers",
    "visible_task_budget_issues",
    "workflow_groups",
]
