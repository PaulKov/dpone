"""Project compact-pack process plans into self-service preview DAG specs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.gitops.airflow_dag_spec import compute_spec_fingerprint
from dpone.gitops.airflow_step_visibility import VisibleTaskBudget, build_visible_task_plan


def preview_process_nodes(workload_id: str, pack: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return stable DAG-spec nodes from one generated workload pack."""

    plans = pack.get("process_plans")
    if not isinstance(plans, Mapping) or not plans:
        return [_legacy_node(workload_id)]
    nodes: list[dict[str, Any]] = []
    multiple = len(plans) > 1
    for key in sorted(str(item) for item in plans):
        plan = plans.get(key)
        if not isinstance(plan, Mapping):
            raise ValueError("DPONE_AIRFLOW_PROCESS_PLAN_INVALID: process plan must be an object")
        dag_node = plan.get("dag_node")
        if not isinstance(dag_node, Mapping):
            raise ValueError("DPONE_AIRFLOW_PROCESS_PLAN_INVALID: process plan is missing dag_node metadata")
        process_name = _required_id(dag_node.get("process_name"), field="process_name")
        raw_node_id = dag_node.get("node_id")
        node_id = (
            _required_id(raw_node_id, field="node_id")
            if raw_node_id is not None
            else f"{workload_id}__{process_name}"
            if multiple
            else workload_id
        )
        nodes.append(
            {
                "node_id": node_id,
                "workload_id": workload_id,
                "selector": plan.get("selector"),
                "task_group": dag_node.get("task_group"),
                "visibility": str(dag_node.get("visibility") or "inline"),
                "estimated_visible_tasks": _positive_int(
                    dag_node.get("estimated_visible_tasks"),
                    field="estimated_visible_tasks",
                ),
                "pack_ref": f"cached://workloads/{workload_id}",
                "pack_path": f".dpone/gitops/airflow/{workload_id}/airflow-pack.json",
            }
        )
    return nodes


def visible_task_plan(nodes: Sequence[Mapping[str, Any]]) -> dict[str, int | str]:
    """Return the default bounded task plan used by self-service previews."""

    estimates = tuple(
        _positive_int(node.get("estimated_visible_tasks"), field="estimated_visible_tasks") for node in nodes
    )
    return build_visible_task_plan(estimates, budget=VisibleTaskBudget()).to_jsonable()


def preview_process_edges(
    workload_id: str,
    pack: Mapping[str, Any],
    nodes: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    """Return selector-local dependencies preserved in process-plan metadata."""

    plans = pack.get("process_plans")
    if not isinstance(plans, Mapping) or not plans:
        return []
    node_by_selector = {
        str(node.get("selector")): str(node.get("node_id")) for node in nodes if node.get("workload_id") == workload_id
    }
    edges: set[tuple[str, str]] = set()
    for key in sorted(str(item) for item in plans):
        plan = plans.get(key)
        dag_node = plan.get("dag_node") if isinstance(plan, Mapping) else None
        if not isinstance(dag_node, Mapping):
            raise ValueError("DPONE_AIRFLOW_PROCESS_PLAN_INVALID: process plan is missing dag_node metadata")
        downstream = node_by_selector.get(str(plan.get("selector")))
        raw_dependencies = dag_node.get("depends_on_process_selectors") or []
        if not isinstance(raw_dependencies, list):
            raise ValueError("DPONE_AIRFLOW_PROCESS_PLAN_INVALID: process dependencies must be a list")
        if downstream is None:
            raise ValueError("DPONE_AIRFLOW_PROCESS_PLAN_INVALID: process plan selector has no preview node")
        for raw_selector in raw_dependencies:
            upstream_selector = str(raw_selector)
            upstream = node_by_selector.get(upstream_selector)
            if upstream is None:
                raise ValueError(
                    f"DPONE_AIRFLOW_PROCESS_PLAN_INVALID: dependency selector {upstream_selector!r} has no process plan"
                )
            if upstream != downstream:
                edges.add((upstream, downstream))
    return [
        {
            "upstream": upstream,
            "downstream": downstream,
            "reason": "declared",
            "origin": "process_plan_dependency",
        }
        for upstream, downstream in sorted(edges)
    ]


def build_preview_dag_spec(
    *,
    pipeline_id: str,
    domain: str,
    pack: Mapping[str, Any],
    start_date: str,
    producer: str = "dpone airflow preview",
) -> dict[str, Any]:
    """Build one fingerprinted non-runnable preview DAG spec."""

    nodes = preview_process_nodes(pipeline_id, pack)
    edges = preview_process_edges(pipeline_id, pack, nodes)
    node_ids = tuple(str(node["node_id"]) for node in nodes)
    edge_pairs = tuple((edge["upstream"], edge["downstream"]) for edge in edges)
    payload: dict[str, Any] = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": producer,
        "dag_id": pipeline_id,
        "domain": domain,
        "description": f"Non-runnable dpone preview for {pipeline_id}",
        "schedule": None,
        "start_date": start_date,
        "catchup": False,
        "tags": ["dpone", "preview", domain],
        "default_args": {"retries": 0},
        "operator_overrides": {},
        "visible_task_plan": visible_task_plan(nodes),
        "nodes": nodes,
        "edges": edges,
        "topological_order": list(_topological_order(node_ids, edge_pairs)),
        "warnings": [
            {
                "code": "DPONE_PREVIEW_NON_RUNNABLE",
                "message": "Preview deployment is non-runnable; its pack exists only to materialize the graph.",
            }
        ],
    }
    payload["spec_fingerprint"] = compute_spec_fingerprint(payload)
    return payload


def preview_result_details(
    *,
    pipeline_id: str,
    release: Mapping[str, Any],
    deployment: Mapping[str, Any],
    dag_spec: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the beginner result projection shared by JSON and text output."""

    return {
        "pipeline_id": pipeline_id,
        "release": dict(release),
        "deployment": dict(deployment),
        "dag_spec_nodes": list(dag_spec["nodes"]),
        "visible_task_plan": dict(dag_spec["visible_task_plan"]),
        "airflow_index_path": ".dpone-cache/current/airflow-index.json",
    }


def _legacy_node(workload_id: str) -> dict[str, Any]:
    return {
        "node_id": workload_id,
        "workload_id": workload_id,
        "selector": None,
        "task_group": None,
        "visibility": "task",
        "estimated_visible_tasks": 2,
        "pack_ref": f"cached://workloads/{workload_id}",
        "pack_path": f".dpone/gitops/airflow/{workload_id}/airflow-pack.json",
    }


def _required_id(value: object, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or any(not (char.isalnum() or char in "_.-") for char in normalized):
        raise ValueError(f"DPONE_AIRFLOW_TASK_ID_CONFLICT: invalid {field} {normalized!r}")
    return normalized


def _positive_int(value: object, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"DPONE_AIRFLOW_VISIBLE_TASK_ESTIMATE_INVALID: invalid {field}")
    return value


def _topological_order(
    node_ids: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
) -> tuple[str, ...]:
    incoming = {node_id: 0 for node_id in node_ids}
    outgoing: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for upstream, downstream in edges:
        incoming[downstream] += 1
        outgoing[upstream].append(downstream)
    ready = sorted(node_id for node_id, count in incoming.items() if count == 0)
    order: list[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for downstream in sorted(outgoing[current]):
            incoming[downstream] -= 1
            if incoming[downstream] == 0:
                ready.append(downstream)
                ready.sort()
    if len(order) != len(node_ids):
        raise ValueError("DPONE_AIRFLOW_PROCESS_PLAN_CYCLE: process-plan dependencies contain a cycle")
    return tuple(order)


__all__ = [
    "build_preview_dag_spec",
    "preview_process_nodes",
    "preview_process_edges",
    "preview_result_details",
    "visible_task_plan",
]
