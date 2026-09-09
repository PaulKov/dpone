from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tests.ci_shadow_pr3b_report_contract_support import canonical_route_id_v1, event_variant, is_integer, is_text


@dataclass(frozen=True)
class RouteValidation:
    routes_by_id: dict[str, dict[str, Any]]
    workflows: set[str]
    jobs: set[tuple[str, str]]
    edges: set[tuple[object, ...]]


def validate_routes(report: dict[str, Any], limits: dict[str, int]) -> RouteValidation | None:
    routes_by_id: dict[str, dict[str, Any]] = {}
    workflows = {item["workflow"] for item in report["roots"]}
    jobs: set[tuple[str, str]] = set()
    edges: set[tuple[object, ...]] = set()
    for route in report["routes"]:
        if not _valid_route_shape(route, report["roots"]):
            return None
        route_value = {key: value for key, value in route.items() if key != "route_id"}
        if route["route_id"] != canonical_route_id_v1(route_value):
            return None
        if not isinstance(route["edge_chain"], list) or len(route["edge_chain"]) > limits["route_edges"]:
            return None
        route_result = _validate_edge_chain(route, report["roots"], limits)
        if route_result is None:
            return None
        route_workflows, route_jobs, route_edges = route_result
        workflows.update(route_workflows)
        jobs.update(route_jobs)
        edges.update(route_edges)
        workflows.add(route["workflow"])
        jobs.add((route["workflow"], route["job_id"]))
        routes_by_id[route["route_id"]] = route
    if len(routes_by_id) != len(report["routes"]) or list(routes_by_id) != sorted(routes_by_id):
        return None
    return RouteValidation(routes_by_id=routes_by_id, workflows=workflows, jobs=jobs, edges=edges)


def _valid_route_shape(route: object, roots: list[dict[str, Any]]) -> bool:
    if not isinstance(route, dict) or set(route) != {
        "route_id",
        "root_index",
        "event_variant",
        "edge_chain",
        "workflow",
        "job_id",
        "classification",
    }:
        return False
    if not is_integer(route["root_index"]) or not 0 <= route["root_index"] < len(roots):
        return False
    if not event_variant(route["event_variant"]):
        return False
    if not is_text(route["workflow"], 1024) or not is_text(route["job_id"], 256):
        return False
    return route["classification"] in {"PR_HEAD", "POST_MERGE_INTEGRATED_CODE", "PROVEN_NOT_PR_REACHABLE"}


def _validate_edge_chain(
    route: dict[str, Any],
    roots: list[dict[str, Any]],
    limits: dict[str, int],
) -> tuple[set[str], set[tuple[str, str]], set[tuple[object, ...]]] | None:
    current_workflow = roots[route["root_index"]]["workflow"]
    current_job: str | None = None
    local_call_edges = 0
    workflow_run_edges = 0
    workflows = {current_workflow}
    jobs: set[tuple[str, str]] = set()
    edges: set[tuple[object, ...]] = set()
    for edge in route["edge_chain"]:
        if not _valid_edge_shape(edge, current_workflow, current_job):
            return None
        coordinate = (
            edge["kind"],
            edge["source_workflow"],
            edge["source_job"],
            edge["target_workflow"],
            edge["target_job"],
        )
        if coordinate in edges:
            return None
        if edge["kind"] in {"LOCAL_WORKFLOW_CALL", "WORKFLOW_RUN"} and edge["target_workflow"] in workflows:
            return None
        source_job = (edge["source_workflow"], edge["source_job"])
        target_job = (edge["target_workflow"], edge["target_job"])
        if edge["kind"] == "NEEDS" and (source_job == target_job or target_job in jobs):
            return None
        workflows.update((edge["source_workflow"], edge["target_workflow"]))
        for workflow_field, job_field in (
            ("source_workflow", "source_job"),
            ("target_workflow", "target_job"),
        ):
            if edge[job_field] is not None:
                jobs.add((edge[workflow_field], edge[job_field]))
        edges.add(coordinate)
        local_call_edges += edge["kind"] == "LOCAL_WORKFLOW_CALL"
        workflow_run_edges += edge["kind"] == "WORKFLOW_RUN"
        current_workflow = edge["target_workflow"]
        current_job = edge["target_job"]
    if current_workflow != route["workflow"]:
        return None
    if current_job is not None and current_job != route["job_id"]:
        return None
    if local_call_edges > limits["local_call_edges"]:
        return None
    if len(workflows) > limits["reusable_workflows_including_root"]:
        return None
    has_workflow_run_suffix = ">WORKFLOW_RUN:" in route["event_variant"]
    if workflow_run_edges != int(has_workflow_run_suffix) or workflow_run_edges > limits["workflow_run_edges"]:
        return None
    return workflows, jobs, edges


def _valid_edge_shape(edge: object, current_workflow: str, current_job: str | None) -> bool:
    if not isinstance(edge, dict) or set(edge) != {
        "kind",
        "source_workflow",
        "source_job",
        "target_workflow",
        "target_job",
    }:
        return False
    if edge["kind"] not in {"LOCAL_WORKFLOW_CALL", "WORKFLOW_RUN", "NEEDS"}:
        return False
    if not is_text(edge["source_workflow"], 1024) or not is_text(edge["target_workflow"], 1024):
        return False
    if edge["source_workflow"] != current_workflow:
        return False
    for job_field in ("source_job", "target_job"):
        if edge[job_field] is not None and not is_text(edge[job_field], 256):
            return False
    if edge["kind"] == "LOCAL_WORKFLOW_CALL" and not (edge["source_job"] is not None and edge["target_job"] is None):
        return False
    if edge["kind"] == "WORKFLOW_RUN" and not (edge["source_job"] is None and edge["target_job"] is None):
        return False
    if edge["kind"] == "NEEDS" and not (
        edge["source_job"] is not None
        and edge["target_job"] is not None
        and edge["source_workflow"] == edge["target_workflow"]
    ):
        return False
    return not (
        current_job is not None
        and edge["kind"] in {"LOCAL_WORKFLOW_CALL", "NEEDS"}
        and edge["source_job"] != current_job
    )
