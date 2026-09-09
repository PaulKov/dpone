from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

import pytest
from tests.ci_shadow_pr3b_report_contract_support import canonical_route_id_v1, policy
from tools.agent_policy import workflow_privilege_graph as graph_api

ROOT_WORKFLOW = ".github/workflows/root.yml"
CALLEE_WORKFLOW = ".github/workflows/callee.yml"


def _job(
    *,
    needs: tuple[str, ...] = (),
    uses: str | None = None,
    inputs: Mapping[str, object] | None = None,
    condition: str | None = None,
) -> dict[str, object]:
    return {
        "needs": list(needs),
        "permissions": None,
        "uses": uses,
        "with": dict(inputs or {}),
        **({"if": condition} if condition is not None else {}),
        **({} if uses is not None else {"runs-on": "ubuntu-latest", "secrets": "NONE", "steps": []}),
    }


def _workflow(
    path: str,
    *,
    name: str,
    events: Mapping[str, object],
    jobs: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    return {
        "path": path,
        "name": name,
        "on": dict(events),
        "permissions": {"contents": "read"},
        "env": {},
        "defaults": {},
        "jobs": dict(jobs),
    }


class _UnexpectedJob(dict[str, object]):
    def get(self, key: str, default: object = None) -> object:
        raise AssertionError(f"job after N+1 was inspected: {key}")


def _resource_workflows(case: str) -> dict[str, dict[str, object]]:
    dimension, raw_count = case.split(":")
    count = int(raw_count)
    if dimension == "jobs":
        jobs = {f"job_{index:04d}": _job() for index in range(count)}
        if count == 4097:
            jobs["job_0000"] = _UnexpectedJob()
        return {ROOT_WORKFLOW: _workflow(ROOT_WORKFLOW, name="Root", events={"pull_request": {}}, jobs=jobs)}
    if dimension == "graph_edges":
        sources = tuple(f"source_{index:03d}" for index in range(128))
        jobs = {source: _job() for source in sources}
        jobs.update({f"fan_{index:02d}": _job(needs=sources) for index in range(64)})
        if count == 8193:
            jobs.update(overflow=_job(needs=(sources[0],)), zz_after_overflow=_UnexpectedJob())
        return {ROOT_WORKFLOW: _workflow(ROOT_WORKFLOW, name="Root", events={"push": {}}, jobs=jobs)}
    return {
        f".github/workflows/root_{index}.yml": _workflow(
            f".github/workflows/root_{index}.yml", name=str(index), events={"pull_request": {}}, jobs={"safe": _job()}
        )
        for index in range(count)
    }


def _as_mapping(value: object) -> dict[str, Any]:
    converted = value if isinstance(value, Mapping) else getattr(value, "to_mapping")()
    assert isinstance(converted, Mapping)
    return dict(converted)


def _finding_codes(*results: object) -> set[str]:
    return {_as_mapping(item)["code"] for result in results for item in getattr(result, "findings", ())}


def _edge_coordinates(route: dict[str, Any]) -> list[tuple[object, ...]]:
    fields = ("kind", "source_workflow", "source_job", "target_workflow", "target_job")
    return [tuple(edge[key] for key in fields) for edge in route["edge_chain"]]


def _expand(workflows: Mapping[str, Mapping[str, object]]) -> tuple[Any, Any, list[dict[str, Any]]]:
    graph = graph_api.build_graph(workflows, policy())
    expansion = graph_api.expand_routes(graph, workflows, policy())
    return graph, expansion, [_as_mapping(route) for route in expansion.routes]


def _assert_canonical_routes(expansion: Any, routes: list[dict[str, Any]]) -> None:
    route_ids = [route["route_id"] for route in routes]
    assert route_ids == sorted(set(route_ids)) == list(expansion.canonical_ids)
    for route in routes:
        value = {key: item for key, item in route.items() if key != "route_id"}
        assert route["route_id"] == canonical_route_id_v1(value)


@pytest.mark.parametrize(
    ("pull_request", "expected_variants"),
    [
        ({"branches": ["master"]}, {"ACTIVITY:opened", "ACTIVITY:reopened", "ACTIVITY:synchronize"}),
        (
            {"branches": ["master"], "types": ["opened", "closed"]},
            {"ACTIVITY:opened", "CLOSED_UNMERGED", "CLOSED_MERGED"},
        ),
    ],
)
def test_direct_pr_routes_expand_correlated_activity_variants(
    pull_request: dict[str, object],
    expected_variants: set[str],
) -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="Root",
            events={"pull_request": pull_request},
            jobs={"inspect": _job()},
        )
    }

    graph, expansion, routes = _expand(workflows)
    roots = [_as_mapping(root) for root in graph.roots]
    assert roots == [{"workflow": ROOT_WORKFLOW, "event": "pull_request"}]
    assert {route["event_variant"] for route in routes} == expected_variants
    assert {(route["workflow"], route["job_id"]) for route in routes} == {(ROOT_WORKFLOW, "inspect")}
    assert all(route["edge_chain"] == [] for route in routes)
    _assert_canonical_routes(expansion, routes)


def test_pull_request_target_is_a_blocking_root_even_behind_false_job_guard() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="Forbidden root",
            events={"pull_request_target": {"branches": ["master"]}},
            jobs={"never": _job(condition="false")},
        )
    }

    graph, expansion, _routes = _expand(workflows)
    assert [_as_mapping(root) for root in graph.roots] == [{"workflow": ROOT_WORKFLOW, "event": "pull_request_target"}]
    assert "PRIVILEGE_PULL_REQUEST_TARGET" in _finding_codes(graph, expansion)


def test_local_workflow_call_and_needs_edges_form_one_continuous_route() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="Root",
            events={"pull_request": {"branches": ["master"]}},
            jobs={
                "delegate": _job(
                    uses="./.github/workflows/callee.yml",
                    inputs={"mode": "safe"},
                )
            },
        ),
        CALLEE_WORKFLOW: _workflow(
            CALLEE_WORKFLOW,
            name="Callee",
            events={
                "workflow_call": {
                    "inputs": {"mode": {"type": "string", "required": True}},
                }
            },
            jobs={"build": _job(), "publish": _job(needs=("build",))},
        ),
    }

    _graph, expansion, routes = _expand(workflows)
    assert (ROOT_WORKFLOW, "delegate") not in {(route["workflow"], route["job_id"]) for route in routes}
    publish = next(
        route
        for route in routes
        if route["workflow"] == CALLEE_WORKFLOW
        and route["job_id"] == "publish"
        and route["event_variant"] == "ACTIVITY:opened"
    )
    assert _edge_coordinates(publish) == [
        ("LOCAL_WORKFLOW_CALL", ROOT_WORKFLOW, "delegate", CALLEE_WORKFLOW, None),
        ("NEEDS", CALLEE_WORKFLOW, "build", CALLEE_WORKFLOW, "publish"),
    ]
    assert publish["event_variant"] == "ACTIVITY:opened"
    _assert_canonical_routes(expansion, routes)


def test_two_pr_callers_retain_distinct_routes_to_the_same_callee_job() -> None:
    second_root = ".github/workflows/second.yml"
    workflows = {
        path: _workflow(
            path,
            name=name,
            events={"pull_request": {"branches": ["master"], "types": ["opened"]}},
            jobs={"delegate": _job(uses="./.github/workflows/callee.yml")},
        )
        for path, name in ((ROOT_WORKFLOW, "First"), (second_root, "Second"))
    }
    workflows[CALLEE_WORKFLOW] = _workflow(
        CALLEE_WORKFLOW,
        name="Callee",
        events={"workflow_call": {}},
        jobs={"inspect": _job()},
    )

    _graph, expansion, routes = _expand(workflows)
    callee_routes = [route for route in routes if route["workflow"] == CALLEE_WORKFLOW and route["job_id"] == "inspect"]
    assert len(callee_routes) == 2
    assert {route["edge_chain"][0]["source_workflow"] for route in callee_routes} == {
        ROOT_WORKFLOW,
        second_root,
    }
    assert len({route["route_id"] for route in callee_routes}) == 2
    _assert_canonical_routes(expansion, routes)


@pytest.mark.parametrize(
    ("workflows_value", "types_value"),
    [(["PR producer"], ["completed"]), ("PR producer", ["completed"]), (["PR producer"], "completed")],
)
def test_workflow_run_routes_accept_scalar_selectors(workflows_value: object, types_value: object) -> None:
    consumer = ".github/workflows/consumer.yml"
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="PR producer",
            events={"pull_request": {"branches": ["master"]}},
            jobs={"build": _job()},
        ),
        consumer: _workflow(
            consumer,
            name="Consumer",
            events={"workflow_run": {"workflows": workflows_value, "types": types_value}},
            jobs={"audit": _job()},
        ),
    }

    _graph, expansion, routes = _expand(workflows)
    consumer_routes = [route for route in routes if route["workflow"] == consumer]
    assert {route["event_variant"] for route in consumer_routes} == {
        "ACTIVITY:opened>WORKFLOW_RUN:completed",
        "ACTIVITY:reopened>WORKFLOW_RUN:completed",
        "ACTIVITY:synchronize>WORKFLOW_RUN:completed",
    }
    assert all(
        _edge_coordinates(route) == [("WORKFLOW_RUN", ROOT_WORKFLOW, None, consumer, None)] for route in consumer_routes
    )
    _assert_canonical_routes(expansion, routes)


@pytest.mark.parametrize(
    "invalid_uses",
    [
        "owner/repository/.github/workflows/reusable.yml@0123456789012345678901234567890123456789",
        "${{ inputs.workflow }}",
        "./.github/workflows/missing.yml",
    ],
)
def test_external_dynamic_and_missing_workflow_calls_fail_closed(invalid_uses: str) -> None:
    push = ".github/workflows/push.yml"
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="Root",
            events={"pull_request": {"branches": ["master"]}},
            jobs={"delegate": _job(uses=invalid_uses)},
        ),
        push: _workflow(push, name="Push", events={"push": {}}, jobs={"external": _job(uses="owner/repo/x.yml@0")}),
    }

    graph, expansion, _routes = _expand(workflows)
    assert "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE" in _finding_codes(graph, expansion)
    assert all(item.subject != push for item in graph.findings)


def test_reusable_caller_job_envelope_is_closed() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="Root",
            events={"pull_request": {}},
            jobs={"call": {**_job(uses="./.github/workflows/callee.yml"), "environment": "production"}},
        ),
        CALLEE_WORKFLOW: _workflow(CALLEE_WORKFLOW, name="Callee", events={"workflow_call": {}}, jobs={"safe": _job()}),
    }
    graph, expansion, _routes = _expand(workflows)
    assert not graph.edges and "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE" in _finding_codes(graph, expansion)


def test_graph_cycles_and_second_workflow_run_edge_are_unverified_not_truncated() -> None:
    consumer = ".github/workflows/consumer.yml"
    downstream = ".github/workflows/downstream.yml"
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="Producer",
            events={"pull_request": {"branches": ["master"]}},
            jobs={"left": _job(needs=("right",)), "right": _job(needs=("left",))},
        ),
        consumer: _workflow(
            consumer,
            name="Consumer",
            events={"workflow_run": {"workflows": ["Producer"], "types": ["completed"]}},
            jobs={"audit": _job()},
        ),
        downstream: _workflow(
            downstream,
            name="Downstream",
            events={"workflow_run": {"workflows": ["Consumer"], "types": ["completed"]}},
            jobs={"publish": _job()},
        ),
    }

    graph, expansion, routes = _expand(workflows)
    assert "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE" in _finding_codes(graph, expansion)
    assert all(route["workflow"] != downstream for route in routes)
    assert all(sum(edge["kind"] == "WORKFLOW_RUN" for edge in route["edge_chain"]) <= 1 for route in routes)
    _assert_canonical_routes(expansion, routes)


@pytest.mark.parametrize(
    "case", ["jobs:4096", "jobs:4097", "graph_edges:8192", "graph_edges:8193", "roots:2", "roots:3"]
)
def test_graph_resources_stop_at_the_exact_n_plus_one_candidate(case: str) -> None:
    dimension, raw_count = case.split(":")
    limited = deepcopy(policy())
    if dimension == "roots":
        limited["limits"]["roots"] = 2
    graph = graph_api.build_graph(_resource_workflows(case), limited)
    count = int(raw_count)
    overflow = count > limited["limits"][dimension]
    assert getattr(graph, {"jobs": "job_count", "graph_edges": "edge_count", "roots": "root_count"}[dimension]) == count
    assert (dimension in graph.overflow_dimensions) is overflow
    assert ("PRIVILEGE_RESOURCE_LIMIT" in _finding_codes(graph)) is overflow
    if dimension != "jobs":
        assert len(getattr(graph, {"graph_edges": "edges", "roots": "roots"}[dimension])) == min(
            count, limited["limits"][dimension]
        )


@pytest.mark.parametrize("count", [50, 51])
def test_reusable_closure_counts_unique_root_and_callees(count: int) -> None:
    paths = [ROOT_WORKFLOW, *(f".github/workflows/callee_{index:02d}.yml" for index in range(count - 1))]
    workflows = {
        path: _workflow(
            path,
            name=path,
            events={"pull_request": {}} if index == 0 else {"workflow_call": {}},
            jobs={"delegate": _job(uses=f"./{paths[index + 1]}" if index + 1 < count else None)},
        )
        for index, path in enumerate(paths)
    }

    graph = graph_api.build_graph(workflows, policy())
    dimension = "reusable_workflows_including_root"
    assert (dimension in graph.overflow_dimensions) is (count == 51)
    assert ("PRIVILEGE_RESOURCE_LIMIT" in _finding_codes(graph)) is (count == 51)


def test_dependency_helpers_preserve_fan_in_and_reject_cycles() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            name="Root",
            events={"pull_request": {}},
            jobs={"b": _job(), "a": _job(), "publish": _job(needs=("b", "a"))},
        )
    }
    assert graph_api.job_dependencies(workflows, ROOT_WORKFLOW, "publish") == ("a", "b")
    assert graph_api.job_dependency_order(workflows, ROOT_WORKFLOW) == ("a", "b", "publish")
    workflows[ROOT_WORKFLOW]["jobs"] = {"a": _job(needs=("b",)), "b": _job(needs=("a",))}
    with pytest.raises(ValueError, match="cycle"):
        graph_api.job_dependency_order(workflows, ROOT_WORKFLOW)


@pytest.mark.parametrize("case", ["exact", "routes", "route_edges"])
def test_route_limits_retain_only_complete_canonical_evidence(case: str) -> None:
    limited = deepcopy(policy())
    limited["limits"].update(routes=2, route_edges=1 if case != "routes" else 256)
    jobs = {"a": _job(), "b": _job(needs=("a",))}
    if case != "exact":
        jobs["c"] = _job(needs=("b",) if case == "route_edges" else ())
    workflows = {
        ROOT_WORKFLOW: _workflow(ROOT_WORKFLOW, name="Root", events={"pull_request": {"types": ["opened"]}}, jobs=jobs)
    }

    graph = graph_api.build_graph(workflows, limited)
    expansion = graph_api.expand_routes(graph, workflows, limited)
    assert expansion.route_count == (2 if case == "exact" else 3)
    assert bool(expansion.routes) is (case == "exact")
    assert expansion.canonical_ids == tuple(sorted(route.route_id for route in expansion.routes))
    assert expansion.overflow_dimensions == (() if case == "exact" else (case,))
