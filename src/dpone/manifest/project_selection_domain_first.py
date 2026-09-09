"""Adapter from one domain-first discovery snapshot to canonical selection."""

from __future__ import annotations

from pathlib import Path

from dpone.manifest.project_discovery_models import ProjectDiscoverySnapshot
from dpone.manifest.project_selection_contracts import (
    LoadedProjectSelectionGraph,
    ProjectDagParser,
    ProjectSelectionDag,
)
from dpone.manifest.project_selection_projection import (
    SelectionNodeFactory,
    optional_text,
    selection_node,
    text_tuple,
)
from dpone.manifest.selection import SelectionError, SelectionGraph


def load_domain_first_selection(
    snapshot: ProjectDiscoverySnapshot,
    *,
    dag_parser: ProjectDagParser,
    node_factory: SelectionNodeFactory = selection_node,
    project_config_inputs: dict[str, str],
    workload_ids: frozenset[str] | None = None,
) -> LoadedProjectSelectionGraph:
    """Fail closed and project checked discovery into the shared selection graph."""

    if snapshot.issues:
        issue = snapshot.issues[0]
        location = f" ({issue.path})" if issue.path else ""
        raise SelectionError(issue.code, f"{issue.message}{location}")
    selected_workloads = tuple(
        workload for workload in snapshot.workloads if workload_ids is None or workload.pipeline_id in workload_ids
    )
    if workload_ids is not None and {item.pipeline_id for item in selected_workloads} != set(workload_ids):
        raise SelectionError(
            "DPONE_PIPELINE_SOURCE_NOT_FOUND",
            "Requested pipeline is not present in the domain-first discovery snapshot.",
        )
    nodes = tuple(
        node_factory(
            workload.checked_source,
            catalog_domain=workload.domain,
            catalog_owner=workload.owner,
            tags=(),
            groups=(),
            semantic_fingerprint=workload.workload_fingerprint,
        )
        for workload in selected_workloads
    )
    graph = SelectionGraph.build(nodes=nodes, edges=())
    dags: list[ProjectSelectionDag] = []
    nodes_by_id = {node.node_id: node for node in nodes}
    for workload in selected_workloads:
        if not workload.airflow_enabled:
            continue
        node = nodes_by_id[workload.pipeline_id]
        declaration, issues = dag_parser(
            node.node_id,
            {
                "workloads": [node.node_id],
                "schedule": None,
                "start_date": "2026-01-01",
                "catchup": False,
            },
        )
        if declaration is None or issues:
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                f"Generated domain-first DAG declaration is invalid: {node.node_id}.",
            )
        dags.append(ProjectSelectionDag(declaration, node.domain, (node.node_id,)))
    consumed = dict(project_config_inputs)
    for path, digest in snapshot.consumed_files.items():
        existing = consumed.get(path)
        if existing is not None and existing != digest:
            raise SelectionError(
                "DPONE_SELECTION_STATE_CHANGED",
                "The same selection input was observed with different content.",
            )
        consumed[path] = digest
    checked_sources = {workload.pipeline_id: workload.checked_source for workload in selected_workloads}
    return LoadedProjectSelectionGraph(
        graph,
        checked_sources,
        dict(sorted(consumed.items())),
        tuple(sorted(dags, key=lambda item: item.declaration.dag_id)),
    )


def is_project_target(root: Path, target: str | Path) -> bool:
    """Return whether a filesystem alias addresses the configured project root."""

    raw = Path(target)
    absolute = raw if raw.is_absolute() else root / raw
    try:
        return absolute.resolve(strict=True) == root.resolve(strict=True)
    except OSError:
        return False


__all__ = [
    "SelectionNodeFactory",
    "is_project_target",
    "load_domain_first_selection",
    "optional_text",
    "selection_node",
    "text_tuple",
]
