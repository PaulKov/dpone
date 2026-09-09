"""Project selected workload packs into one deterministic preview DAG spec."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_dag_spec import DagScheduleAssets, compute_spec_fingerprint
from dpone.readiness.airflow_preview_dag_spec import (
    preview_process_edges,
    preview_process_nodes,
    visible_task_plan,
)
from dpone.readiness.project_selection_loader import ProjectSelectionDag, SelectionError


def selected_dag_payload(
    dag: ProjectSelectionDag,
    *,
    members: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
    packs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Expand workload-level selection edges over process-level DAG nodes."""

    declaration = dag.declaration
    schedule: Any = declaration.schedule
    if isinstance(schedule, DagScheduleAssets):
        schedule = schedule.to_jsonable()
    tags = set(declaration.tags)
    tags.update(("dpone", "preview"))
    if dag.domain:
        tags.add(dag.domain)
    nodes = [node for workload_id in members for node in preview_process_nodes(workload_id, packs[workload_id])]
    node_ids_by_workload = {
        workload_id: tuple(str(node["node_id"]) for node in nodes if node["workload_id"] == workload_id)
        for workload_id in members
    }
    expanded_edges = tuple(
        sorted(
            (upstream_node, downstream_node)
            for upstream, downstream in edges
            for upstream_node in node_ids_by_workload[upstream]
            for downstream_node in node_ids_by_workload[downstream]
        )
    )
    internal_edges = [
        edge for workload_id in members for edge in preview_process_edges(workload_id, packs[workload_id], nodes)
    ]
    all_edges = tuple(
        sorted(
            {
                *expanded_edges,
                *((edge["upstream"], edge["downstream"]) for edge in internal_edges),
            }
        )
    )
    payload: dict[str, Any] = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "dpone airflow preview --select",
        "dag_id": declaration.dag_id,
        "domain": dag.domain,
        "description": declaration.description or f"Non-runnable selected preview for {declaration.dag_id}",
        "schedule": schedule,
        "start_date": declaration.start_date,
        "timezone": declaration.timezone,
        "catchup": declaration.catchup,
        "max_active_runs": declaration.max_active_runs,
        "tags": sorted(tags),
        "default_args": declaration.default_args,
        "operator_overrides": declaration.operator_overrides,
        "visible_task_plan": visible_task_plan(nodes),
        "nodes": nodes,
        "edges": [
            {
                "upstream": upstream,
                "downstream": downstream,
                "reason": "declared",
                "origin": "selection_graph",
            }
            for upstream, downstream in expanded_edges
        ]
        + internal_edges,
        "topological_order": list(
            _topological_order(
                tuple(str(node["node_id"]) for node in nodes),
                all_edges,
            )
        ),
        "warnings": [],
    }
    payload["spec_fingerprint"] = compute_spec_fingerprint(payload)
    return payload


def _topological_order(
    members: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
) -> tuple[str, ...]:
    incoming = {member: 0 for member in members}
    outgoing: dict[str, list[str]] = {member: [] for member in members}
    for upstream, downstream in edges:
        incoming[downstream] += 1
        outgoing[upstream].append(downstream)
    ready = sorted(member for member, count in incoming.items() if count == 0)
    order: list[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for downstream in sorted(outgoing[current]):
            incoming[downstream] -= 1
            if incoming[downstream] == 0:
                ready.append(downstream)
                ready.sort()
    if len(order) != len(members):
        raise SelectionError("DPONE_SELECTION_GRAPH_CYCLE", "Selected DAG contains a cycle.")
    return tuple(order)


__all__ = ["selected_dag_payload"]
