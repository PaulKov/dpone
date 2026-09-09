"""Construct bounded PR-reachability roots, edges, and canonical routes."""

from __future__ import annotations

import functools
from collections.abc import Iterator, Mapping
from graphlib import CycleError, TopologicalSorter
from heapq import merge
from typing import Any

from tools.agent_policy import workflow_privilege_contracts as contracts
from tools.agent_policy import workflow_privilege_parser as parser

_Workflows, _Policy, _Jobs = Mapping[str, Mapping[str, Any]], Mapping[str, Any], Mapping[str, Any]
_Chain = tuple[contracts.Edge, ...]
_State = tuple[int, str, str, _Chain, tuple[str, ...], int, int]


class _LimitReached(Exception): ...


def job_dependencies(workflows: _Workflows, workflow: str, job_id: str) -> tuple[str, ...]:
    jobs = workflows.get(workflow, {}).get("jobs", {})
    if not isinstance(jobs, Mapping) or job_id not in jobs or not isinstance(jobs[job_id], Mapping):
        raise KeyError(f"unknown workflow job: {workflow}::{job_id}")
    return parser.normalized_needs(jobs[job_id])


def job_dependency_order(workflows: _Workflows, workflow: str) -> tuple[str, ...]:
    jobs = workflows.get(workflow, {}).get("jobs", {})
    if not isinstance(jobs, Mapping):
        raise ValueError(f"workflow jobs are not a mapping: {workflow}")
    dependencies = {job_id: job_dependencies(workflows, workflow, job_id) for job_id in sorted(jobs, key=str.encode)}
    if any(predecessor not in jobs for values in dependencies.values() for predecessor in values):
        raise ValueError(f"workflow job dependency is missing: {workflow}")
    try:
        return tuple(TopologicalSorter(dependencies).static_order())
    except CycleError as exc:
        raise ValueError(f"workflow job graph contains a cycle: {workflow}") from exc


def _local_target(value: object) -> str | None:
    return value.removeprefix("./") if isinstance(value, str) and value.startswith("./.github/workflows/") else None


def _local_callee(job: Mapping[str, Any], workflows: _Workflows) -> str | None:
    target = _local_target(job.get("uses"))
    return target if target in workflows and "workflow_call" in _events(workflows[target]) else None


def _events(workflow: Mapping[str, Any]) -> Mapping[str, Any]:
    return value if isinstance(value := workflow.get("on", {}), Mapping) else {}


def _event_variants(event: str, configuration: object) -> tuple[str, ...]:
    if event not in {"pull_request", "pull_request_target"}:
        return ()
    types = configuration.get("types") if isinstance(configuration, Mapping) else None
    activities = (
        ("opened", "reopened", "synchronize") if types is None else (types,) if isinstance(types, str) else types
    )
    return tuple(
        dict.fromkeys(
            variant
            for activity in activities
            for variant in (("CLOSED_UNMERGED", "CLOSED_MERGED") if activity == "closed" else (f"ACTIVITY:{activity}",))
        )
    )


def _workflow_run_values(cfg: object, key: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    values = cfg.get(key, default) if isinstance(cfg, Mapping) else ()
    values = (values,) if isinstance(values, str) else values
    return tuple(dict.fromkeys(x for x in values if isinstance(x, str))) if isinstance(values, (list, tuple)) else ()


def _edge_key(edge: contracts.Edge) -> tuple[object, ...]:
    return (edge.kind, edge.source_workflow, edge.source_job or "", edge.target_workflow, edge.target_job or "")


def _deduplicate_findings(values: list[contracts.Finding]) -> tuple[contracts.Finding, ...]:
    return tuple(sorted(set(values), key=contracts.finding_key))


def _policy_finding(code: str, subject: str, detail: str, policy: _Policy) -> contracts.Finding:
    return contracts.finding(code, subject=subject, detail=detail, policy=policy)


def _resource_finding(dimension: str, limit: int, policy: _Policy) -> contracts.Finding:
    return _policy_finding(
        "PRIVILEGE_RESOURCE_LIMIT", dimension, f"{dimension} exceeded closed maximum {limit}", policy
    )


def _record_limit(values: list[contracts.Finding], dims: set[str], name: str, limit: int, policy: _Policy) -> None:
    dims.add(name)
    _record_finding(values, dims, _resource_finding(name, limit, policy), policy, stop=False)


def _record_finding(
    values: list[contracts.Finding], dims: set[str], item: contracts.Finding, policy: _Policy, *, stop: bool = True
) -> None:
    if item in values:
        return
    limit = int(policy.get("limits", {}).get("findings", 4096))
    if len(values) < limit:
        values.append(item)
        return
    dims.add("findings")
    values[-1:] = [_resource_finding("findings", limit, policy)]
    if stop:
        raise _LimitReached("findings", limit)


def _job_issues(path: str, workflows: _Workflows, edges: set[contracts.Edge]) -> Iterator[str]:
    jobs = workflows[path]["jobs"]
    if not jobs:
        yield "pr-reachable workflow has no jobs"
        return
    ordered = sorted(jobs.items(), key=lambda item: str(item[0]).encode())
    for job_id, job in ordered:
        target = _local_callee(job, workflows)
        if target is not None and contracts.Edge("LOCAL_WORKFLOW_CALL", path, job_id, target, None) not in edges:
            yield f"invalid workflow_call inputs from {job_id}"
    missing = merge(*((item for item in parser.normalized_needs(job) if item not in jobs) for _, job in ordered))
    for predecessor in dict.fromkeys(missing):
        yield f"missing needs job {predecessor}"
    for job_id, job in ordered:
        if "uses" in job and _local_callee(job, workflows) is None:
            yield f"unsupported workflow call from {job_id}"


def _add_edge(edges: set[contracts.Edge], edge: contracts.Edge, limit: int) -> None:
    if edge in edges:
        return
    if len(edges) >= limit:
        raise _LimitReached("graph_edges", limit)
    edges.add(edge)


def _finding_candidates(
    workflows: _Workflows,
    names: Mapping[str, list[str]],
    reachable: set[str],
    edges: set[contracts.Edge],
    policy: _Policy,
) -> Iterator[contracts.Finding]:
    if _has_cycle(edges):
        yield _edge_finding(".github/workflows", "workflow graph contains a cycle", policy)
    for path in sorted(workflows, key=str.encode):
        run = _events(workflows[path]).get("workflow_run")
        for name in sorted(_workflow_run_values(run, "workflows"), key=str.encode):
            candidates = names.get(name, ())
            if len(candidates) != 1 and bool(reachable):
                yield _edge_finding(path, f"ambiguous workflow_run producer {name}", policy)
        if path in reachable:
            for detail in _job_issues(path, workflows, edges):
                yield _edge_finding(path, detail, policy)


def build_graph(workflows: _Workflows, policy: Mapping[str, Any]) -> contracts.GraphResult:
    limits = policy.get("limits", {})
    roots: list[contracts.Root] = []
    edges: set[contracts.Edge] = set()
    findings: list[contracts.Finding] = []
    display_names: dict[str, list[str]] = {}
    dimensions: set[str] = set()
    job_limit, root_limit = int(limits.get("jobs", 4096)), int(limits.get("roots", 512))
    job_count = root_count = 0
    edge_limit = int(limits.get("graph_edges", 8192))
    validator_for = functools.cache(lambda path: parser.workflow_call_validator(workflows[path]))
    try:
        for path in sorted(workflows, key=str.encode):
            workflow = workflows[path]
            display_names.setdefault(str(workflow.get("name", "")), []).append(path)
            jobs = workflow["jobs"]
            job_count = min(job_count + len(jobs), job_limit + 1)
            for event in ("pull_request", "pull_request_target"):
                if event not in _events(workflow):
                    continue
                root_count += 1
                if root_count > root_limit:
                    raise _LimitReached("roots", root_limit)
                roots.append(contracts.Root(path, event))
                if event == "pull_request_target":
                    detail = "pull_request_target is forbidden for repository workflows"
                    item = _policy_finding("PRIVILEGE_PULL_REQUEST_TARGET", path, detail, policy)
                    _record_finding(findings, dimensions, item, policy)
            if job_count > job_limit:
                raise _LimitReached("jobs", job_limit)
        for path in sorted(workflows, key=str.encode):
            jobs = workflows[path]["jobs"]
            run = _events(workflows[path]).get("workflow_run")
            for producer_name in _workflow_run_values(run, "workflows"):
                candidates = tuple(display_names.get(producer_name, ()))
                if len(candidates) == 1:
                    _add_edge(edges, contracts.Edge("WORKFLOW_RUN", candidates[0], None, path, None), edge_limit)
            for job_id, job_value in sorted(jobs.items(), key=lambda item: str(item[0]).encode()):
                for predecessor in parser.normalized_needs(job_value):
                    if predecessor in jobs:
                        _add_edge(edges, contracts.Edge("NEEDS", path, predecessor, path, job_id), edge_limit)
                if "uses" in job_value:
                    target = _local_callee(job_value, workflows)
                    if target is not None and validator_for(target)(job_value):
                        _add_edge(edges, contracts.Edge("LOCAL_WORKFLOW_CALL", path, job_id, target, None), edge_limit)
    except _LimitReached as exc:
        _record_limit(findings, dimensions, str(exc.args[0]), int(exc.args[1]), policy)
    else:
        reachable = _reachable_workflows(roots, edges)
        reachable_edges = {edge for edge in edges if {edge.source_workflow, edge.target_workflow} <= reachable}
        try:
            for item in _finding_candidates(workflows, display_names, reachable, reachable_edges, policy):
                _record_finding(findings, dimensions, item, policy)
            reusable = {root.workflow for root in roots}
            for edge in reachable_edges:
                if edge.kind == "LOCAL_WORKFLOW_CALL":
                    reusable.update((edge.source_workflow, edge.target_workflow))
            reusable_limit = int(limits.get("reusable_workflows_including_root", 50))
            if len(reusable) > reusable_limit:
                _record_limit(findings, dimensions, "reusable_workflows_including_root", reusable_limit, policy)
        except _LimitReached:
            pass
    return contracts.GraphResult(
        _deduplicate_findings(findings),
        tuple(sorted(roots, key=lambda item: (item.workflow.encode(), item.event))),
        tuple(sorted(edges, key=_edge_key)),
        job_count,
        edge_limit + 1 if "graph_edges" in dimensions else len(edges),
        root_count,
        tuple(sorted(dimensions)),
    )


def _reachable_workflows(roots: list[contracts.Root], edges: set[contracts.Edge]) -> set[str]:
    reachable = {root.workflow for root in roots}
    while targets := {
        edge.target_workflow
        for edge in edges
        if edge.kind != "NEEDS" and edge.source_workflow in reachable and edge.target_workflow not in reachable
    }:
        reachable.update(targets)
    return reachable


def _edge_finding(subject: str, detail: str, policy: Mapping[str, Any]) -> contracts.Finding:
    return _policy_finding("PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE", subject, detail, policy)


def _has_cycle(edges: set[contracts.Edge]) -> bool:
    dependencies: dict[tuple[str, str | None], set[tuple[str, str | None]]] = {}
    for edge in edges:
        source, target = (edge.source_workflow, edge.source_job), (edge.target_workflow, edge.target_job)
        dependencies.setdefault(source, set())
        dependencies.setdefault(target, set()).add(source)
    try:
        tuple(TopologicalSorter(dependencies).static_order())
    except CycleError:
        return True
    return False


def expand_routes(graph: contracts.GraphResult, workflows: _Workflows, policy: _Policy) -> contracts.RouteExpansion:
    return _RouteBuilder(graph, workflows, policy).expand()


class _RouteBuilder:
    def __init__(self, graph: contracts.GraphResult, workflows: _Workflows, policy: _Policy) -> None:
        self.graph, self.workflows, self.policy = graph, workflows, policy
        self.limits, self.edges = policy.get("limits", {}), set(graph.edges)
        self.route_limit = int(self.limits.get("routes", 16384))
        self.edge_limit = int(self.limits.get("route_edges", 256))
        self.routes: dict[str, contracts.Route] = {}
        self.findings, self.dimensions = list(graph.findings), set(graph.overflow_dimensions)
        self.path_candidates = 0

    def expand(self) -> contracts.RouteExpansion:
        if self.dimensions:
            return self._result()
        try:
            for root_index, root in enumerate(self.graph.roots):
                configuration = _events(self.workflows[root.workflow]).get(root.event)
                for variant in _event_variants(root.event, configuration):
                    self._walk((root_index, variant, root.workflow, (), (root.workflow,), 0, 0))
        except _LimitReached:
            pass
        return self._result()

    def _result(self) -> contracts.RouteExpansion:
        overflow = bool({"route_edges", "routes"} & self.dimensions)
        ordered = () if overflow else tuple(sorted(self.routes.values(), key=lambda item: item.route_id))
        route_count = self.route_limit + 1 if "routes" in self.dimensions else len(self.routes)
        route_count += int("route_edges" in self.dimensions)
        return contracts.RouteExpansion(
            findings=_deduplicate_findings(self.findings),
            routes=ordered,
            canonical_ids=tuple(route.route_id for route in ordered),
            route_count=route_count,
            overflow_dimensions=tuple(sorted(self.dimensions)),
        )

    def _walk(self, state: _State) -> None:
        root_index, variant, path, base, seen, run_depth, local_depth = state
        jobs = self.workflows[path]["jobs"]
        for job_id, job in sorted(jobs.items(), key=lambda item: str(item[0]).encode()):
            target = _local_target(job.get("uses"))
            local_edge = contracts.Edge("LOCAL_WORKFLOW_CALL", path, job_id, target or "", None)
            remaining = self.edge_limit - len(base) + 1
            for needs_chain in self._needs_paths(path, job_id, jobs, remaining):
                chain = (*base, *needs_chain)
                if local_edge not in self.edges:
                    if job.get("uses") is not None:
                        continue
                    classification = "POST_MERGE_INTEGRATED_CODE" if variant == "CLOSED_MERGED" else "PR_HEAD"
                    self._retain(contracts.Route(root_index, variant, chain, path, job_id, classification))
                elif target in seen or local_depth >= int(self.limits.get("local_call_edges", 9)):
                    self._problem(path, "local workflow call depth or cycle is not closed")
                else:
                    self._descend(
                        (root_index, variant, target, (*chain, local_edge), (*seen, target), run_depth, local_depth + 1)
                    )
        run_edges = sorted(
            (edge for edge in self.edges if edge.kind == "WORKFLOW_RUN" and edge.source_workflow == path), key=_edge_key
        )
        for edge in run_edges:
            if run_depth >= int(self.limits.get("workflow_run_edges", 1)):
                self._problem(edge.target_workflow, "workflow_run depth exceeds the closed limit")
            elif edge.target_workflow in seen:
                self._problem(edge.target_workflow, "workflow_run cycle is not closed")
            else:
                run = _events(self.workflows[edge.target_workflow]).get("workflow_run")
                activities = _workflow_run_values(run, "types", ("requested", "in_progress", "completed"))
                for activity in activities:
                    next_variant = f"{variant}>WORKFLOW_RUN:{activity}"
                    if not contracts.valid_public_text(next_variant, 128):
                        self._problem(edge.target_workflow, "workflow_run event variant exceeds the closed limit")
                        continue
                    next_state = (
                        root_index,
                        next_variant,
                        edge.target_workflow,
                        (*base, edge),
                        (*seen, edge.target_workflow),
                        run_depth + 1,
                        local_depth,
                    )
                    self._descend(next_state)

    def _problem(self, subject: str, detail: str) -> None:
        if (item := _edge_finding(subject, detail, self.policy)) not in self.findings:
            _record_finding(self.findings, self.dimensions, item, self.policy)

    def _consume_path(self) -> None:
        self.path_candidates += 1
        if self.path_candidates > self.route_limit:
            self._overflow("routes", self.route_limit)

    def _descend(self, state: _State) -> None:
        if len(state[3]) > self.edge_limit:
            self._overflow("route_edges", self.edge_limit)
        self._consume_path()
        self._walk(state)

    def _needs_paths(self, workflow: str, job_id: str, jobs: _Jobs, maximum: int) -> Iterator[_Chain]:
        stack: list[tuple[str, frozenset[str], _Chain]] = [(job_id, frozenset(), ())]
        while stack:
            current, seen, reverse_path = stack.pop()
            if current in seen:
                continue
            if len(reverse_path) >= maximum:
                yield tuple(reversed(reverse_path))
                continue
            value = jobs.get(current)
            predecessors = parser.normalized_needs(value) if isinstance(value, Mapping) else ()
            valid = [
                predecessor
                for predecessor in predecessors
                if contracts.Edge("NEEDS", workflow, predecessor, workflow, current) in self.edges
            ]
            if not valid:
                yield tuple(reversed(reverse_path))
            for predecessor in reversed(valid):
                edge = contracts.Edge("NEEDS", workflow, predecessor, workflow, current)
                if len(reverse_path) + 1 >= maximum:
                    self._overflow("route_edges", self.edge_limit)
                self._consume_path()
                stack.append((predecessor, seen | {current}, (*reverse_path, edge)))

    def _retain(self, route: contracts.Route) -> None:
        if len(route.edge_chain) > self.edge_limit:
            self._overflow("route_edges", self.edge_limit)
        if route.route_id in self.routes:
            return
        if len(self.routes) >= self.route_limit:
            self._overflow("routes", self.route_limit)
        self.routes[route.route_id] = route

    def _overflow(self, dimension: str, limit: int) -> None:
        _record_limit(self.findings, self.dimensions, dimension, limit, self.policy)
        raise _LimitReached
