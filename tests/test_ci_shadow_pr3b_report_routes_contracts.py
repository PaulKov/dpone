from __future__ import annotations

import copy
from typing import Any

import pytest

from tests.ci_shadow_pr3b_report_contract_model import valid_report_shape
from tests.ci_shadow_pr3b_report_contract_support import (
    PERMISSION_ACCESS,
    SnapshotReference,
    canonical_route_id_v1,
    report_routes_match_graph,
    report_snapshot_matches,
)
from tests.ci_shadow_pr3b_report_examples import (
    make_unprivileged,
    profile_report,
    rebind_route,
    report_with_one_codeql_route,
)


def test_pr3b_positive_causal_edge_routes_are_executable() -> None:
    local_needs = report_with_one_codeql_route()
    make_unprivileged(local_needs)
    local_needs["routes"][0].update(
        workflow=".github/workflows/reusable.yml",
        job_id="publish",
        edge_chain=[
            _edge(
                "LOCAL_WORKFLOW_CALL",
                ".github/workflows/codeql.yml",
                "call-reusable",
                ".github/workflows/reusable.yml",
                None,
            ),
            _edge(
                "NEEDS",
                ".github/workflows/reusable.yml",
                "prepare",
                ".github/workflows/reusable.yml",
                "publish",
            ),
        ],
    )
    local_needs["route_authority"][0].update(workflow=".github/workflows/reusable.yml", job_id="publish")
    local_needs["inventory"].update(workflow_count=2, job_count=3, edge_count=2)
    rebind_route(local_needs)

    workflow_run = report_with_one_codeql_route()
    make_unprivileged(workflow_run)
    workflow_run["routes"][0].update(
        event_variant="ACTIVITY:opened>WORKFLOW_RUN:completed",
        workflow=".github/workflows/consumer.yml",
        job_id="audit",
        edge_chain=[
            _edge(
                "WORKFLOW_RUN",
                ".github/workflows/codeql.yml",
                None,
                ".github/workflows/consumer.yml",
                None,
            )
        ],
    )
    workflow_run["route_authority"][0].update(workflow=".github/workflows/consumer.yml", job_id="audit")
    workflow_run["inventory"].update(workflow_count=2, job_count=2, edge_count=1)
    rebind_route(workflow_run)

    assert valid_report_shape(local_needs)
    assert valid_report_shape(workflow_run)
    for count, invalid_value in (("workflow_count", 1), ("job_count", 2), ("edge_count", 1)):
        incomplete_count = copy.deepcopy(local_needs)
        incomplete_count["inventory"][count] = invalid_value
        assert not valid_report_shape(incomplete_count)


def test_pr3b_route_id_v1_is_stable_for_non_ascii_coordinates() -> None:
    route = {
        "root_index": 0,
        "event_variant": "ACTIVITY:opened",
        "edge_chain": [],
        "workflow": ".github/workflows/данные.yml",
        "job_id": "分析",
        "classification": "PR_HEAD",
    }

    assert (
        report_with_one_codeql_route()["routes"][0]["route_id"]
        == "a84e4722d190f008c5ca800763d6bb309aa76e5576cb76c8c97e222432bcac83"
    )
    assert canonical_route_id_v1(route) == "73b9d0febbd055e6e2f419ecfeca068d795c87cffee5c5900d67cad847e1842c"


def _edge(
    kind: str, source_workflow: str, source_job: str | None, target_workflow: str, target_job: str | None
) -> dict[str, Any]:
    return {
        "kind": kind,
        "source_workflow": source_workflow,
        "source_job": source_job,
        "target_workflow": target_workflow,
        "target_job": target_job,
    }


def _local_call_report(edge_count: int) -> dict[str, Any]:
    report = report_with_one_codeql_route()
    make_unprivileged(report)
    edges = []
    for index in range(edge_count):
        source = ".github/workflows/codeql.yml" if index == 0 else f".github/workflows/call-{index}.yml"
        edges.append(
            _edge(
                "LOCAL_WORKFLOW_CALL",
                source,
                f"call-{index + 1}",
                f".github/workflows/call-{index + 1}.yml",
                None,
            )
        )
    endpoint = f".github/workflows/call-{edge_count}.yml"
    report["routes"][0].update(edge_chain=edges, workflow=endpoint, job_id="publish")
    report["route_authority"][0].update(workflow=endpoint, job_id="publish")
    report["inventory"].update(
        workflow_count=edge_count + 1,
        job_count=edge_count + 1,
        edge_count=edge_count,
    )
    rebind_route(report)
    return report


def test_pr3b_local_call_limit_accepts_nine_and_rejects_ten() -> None:
    assert valid_report_shape(_local_call_report(9))
    assert not valid_report_shape(_local_call_report(10))


def test_pr3b_report_rejects_cyclic_or_replayed_edge_chains() -> None:
    needs_self_loop = report_with_one_codeql_route()
    make_unprivileged(needs_self_loop)
    needs_self_loop["routes"][0]["edge_chain"] = [
        _edge(
            "NEEDS",
            ".github/workflows/codeql.yml",
            "analyze",
            ".github/workflows/codeql.yml",
            "analyze",
        )
    ]
    needs_self_loop["inventory"]["edge_count"] = 1
    rebind_route(needs_self_loop)

    repeated_edge = report_with_one_codeql_route()
    make_unprivileged(repeated_edge)
    repeated = _edge(
        "LOCAL_WORKFLOW_CALL",
        ".github/workflows/codeql.yml",
        "analyze",
        ".github/workflows/codeql.yml",
        None,
    )
    repeated_edge["routes"][0]["edge_chain"] = [repeated, copy.deepcopy(repeated)]
    repeated_edge["inventory"]["edge_count"] = 1
    rebind_route(repeated_edge)

    workflow_cycle = report_with_one_codeql_route()
    make_unprivileged(workflow_cycle)
    workflow_cycle["routes"][0]["edge_chain"] = [
        _edge(
            "LOCAL_WORKFLOW_CALL",
            ".github/workflows/codeql.yml",
            "call-reusable",
            ".github/workflows/reusable.yml",
            None,
        ),
        _edge(
            "LOCAL_WORKFLOW_CALL",
            ".github/workflows/reusable.yml",
            "call-codeql",
            ".github/workflows/codeql.yml",
            None,
        ),
    ]
    workflow_cycle["inventory"].update(workflow_count=2, job_count=3, edge_count=2)
    rebind_route(workflow_cycle)

    assert not valid_report_shape(needs_self_loop)
    assert not valid_report_shape(repeated_edge)
    assert not valid_report_shape(workflow_cycle)


def test_pr3b_all_closed_privileged_profiles_have_positive_reports() -> None:
    assert valid_report_shape(report_with_one_codeql_route())
    assert valid_report_shape(profile_report("governance"))
    assert valid_report_shape(profile_report("merged_closure"))
    with pytest.raises(ValueError, match="unknown closed profile"):
        profile_report("not-a-profile")


def test_pr3b_report_route_ids_equal_the_complete_graph_route_set() -> None:
    report = report_with_one_codeql_route()
    route_id = report["routes"][0]["route_id"]
    assert report_routes_match_graph(report, [route_id])

    missing = copy.deepcopy(report)
    missing["routes"] = []
    extra = copy.deepcopy(report)
    extra_route = copy.deepcopy(extra["routes"][0])
    extra_route["event_variant"] = "ACTIVITY:synchronize"
    extra_route["route_id"] = canonical_route_id_v1(
        {key: value for key, value in extra_route.items() if key != "route_id"}
    )
    extra["routes"].append(extra_route)
    duplicate = copy.deepcopy(report)
    duplicate["routes"].append(copy.deepcopy(duplicate["routes"][0]))

    assert not report_routes_match_graph(missing, [route_id])
    assert not report_routes_match_graph(extra, [route_id])
    assert not report_routes_match_graph(duplicate, [route_id])

    second_id = extra_route["route_id"]
    assert not report_routes_match_graph(extra, sorted([route_id, second_id], reverse=True))


def test_pr3b_report_binds_exact_policy_and_manifest_snapshot() -> None:
    report = report_with_one_codeql_route()
    policy_sha = report["policy"]["sha256"]
    manifest_sha = report["inventory"]["manifest_sha256"]
    complete_reference = SnapshotReference(policy_sha, 1, manifest_sha, True)

    assert report_snapshot_matches(report, snapshot_reference=complete_reference)
    assert not report_snapshot_matches(
        report,
        snapshot_reference=SnapshotReference("b" * 64, 1, manifest_sha, True),
    )
    assert not report_snapshot_matches(
        report,
        snapshot_reference=SnapshotReference(policy_sha, 1, "c" * 64, True),
    )

    incomplete = copy.deepcopy(report)
    incomplete.update(status="UNVERIFIED", ok=False)
    incomplete["policy"].update(sha256=None, schema_version=None)
    incomplete["inventory"].update(
        complete=False,
        manifest_sha256=None,
        workflow_count=0,
        job_count=0,
        edge_count=0,
        root_count=0,
        route_count=0,
    )
    incomplete["roots"] = []
    incomplete["routes"] = []
    incomplete["route_authority"] = []
    incomplete["privileged_profiles"] = []
    incomplete["findings"] = [
        {
            "code": "PRIVILEGE_CONCURRENT_MUTATION",
            "status": "UNVERIFIED",
            "subject": ".",
            "route_id": None,
            "detail": "stable complete inventory could not be acquired",
            "recovery_command_id": "RERUN_IMMUTABLE_CHECKOUT",
        }
    ]
    incomplete_reference = SnapshotReference(None, None, None, False)
    assert valid_report_shape(incomplete)
    assert report_snapshot_matches(incomplete, snapshot_reference=incomplete_reference)

    wrong_incomplete_policy = copy.deepcopy(incomplete)
    wrong_incomplete_policy["policy"]["sha256"] = "d" * 64
    wrong_incomplete_manifest = copy.deepcopy(incomplete)
    wrong_incomplete_manifest["inventory"]["manifest_sha256"] = "e" * 64
    assert not report_snapshot_matches(wrong_incomplete_policy, snapshot_reference=incomplete_reference)
    assert not report_snapshot_matches(wrong_incomplete_manifest, snapshot_reference=incomplete_reference)


def test_pr3b_report_binds_policy_parse_state_and_snapshot_completeness() -> None:
    report = report_with_one_codeql_route()
    policy_sha = report["policy"]["sha256"]
    manifest_sha = report["inventory"]["manifest_sha256"]
    stable_invalid_policy = copy.deepcopy(report)
    stable_invalid_policy.update(status="UNVERIFIED", ok=False)
    stable_invalid_policy["policy"]["schema_version"] = None
    stable_invalid_policy["roots"] = []
    stable_invalid_policy["routes"] = []
    stable_invalid_policy["route_authority"] = []
    stable_invalid_policy["privileged_profiles"] = []
    stable_invalid_policy["inventory"].update(root_count=0, route_count=0, edge_count=0)
    stable_invalid_policy["findings"] = [
        {
            "code": "PRIVILEGE_INVALID_POLICY",
            "status": "UNVERIFIED",
            "subject": ".agents/policy/workflow-security-privileged.yml",
            "route_id": None,
            "detail": "strict policy/schema validation failed",
            "recovery_command_id": "REPAIR_PRIVILEGE_POLICY",
        }
    ]
    invalid_reference = SnapshotReference(policy_sha, None, manifest_sha, True)

    assert valid_report_shape(stable_invalid_policy)
    assert report_snapshot_matches(stable_invalid_policy, snapshot_reference=invalid_reference)
    assert not report_snapshot_matches(
        stable_invalid_policy,
        snapshot_reference=SnapshotReference(policy_sha, 1, manifest_sha, True),
    )
    assert not report_snapshot_matches(
        stable_invalid_policy,
        snapshot_reference=SnapshotReference(policy_sha, None, manifest_sha, False),
    )
    assert not report_snapshot_matches(
        stable_invalid_policy,
        snapshot_reference=SnapshotReference(None, 1, None, False),
    )


def test_pr3b_pass_requires_at_least_one_route_for_every_root() -> None:
    report = report_with_one_codeql_route()
    report["roots"].append({"workflow": ".github/workflows/orphan.yml", "event": "pull_request"})
    second_route = copy.deepcopy(report["routes"][0])
    second_route["event_variant"] = "ACTIVITY:synchronize"
    second_route["route_id"] = canonical_route_id_v1(
        {key: value for key, value in second_route.items() if key != "route_id"}
    )
    second_authority = copy.deepcopy(report["route_authority"][0])
    second_authority["route_id"] = second_route["route_id"]
    second_profile = copy.deepcopy(report["privileged_profiles"][0])
    second_profile["route_id"] = second_route["route_id"]
    report["routes"] = sorted([report["routes"][0], second_route], key=lambda item: item["route_id"])
    report["route_authority"] = sorted(
        [report["route_authority"][0], second_authority], key=lambda item: item["route_id"]
    )
    report["privileged_profiles"] = sorted(
        [report["privileged_profiles"][0], second_profile], key=lambda item: (item["id"], item["route_id"])
    )
    report["inventory"].update(workflow_count=2, job_count=2, root_count=2, route_count=2)
    assert not valid_report_shape(report)


def test_pr3b_merged_closure_profile_rejects_a_direct_activity_variant() -> None:
    report = profile_report("merged_closure")
    report["routes"][0]["event_variant"] = "ACTIVITY:opened"
    rebind_route(report)
    assert not valid_report_shape(report)


def test_pr3b_authority_is_preserved_per_route() -> None:
    report = report_with_one_codeql_route()
    report["privileged_profiles"] = []
    report["route_authority"][0]["profile_id"] = None
    none_permissions = {key: "none" for key in PERMISSION_ACCESS}
    report["route_authority"][0]["declared_permissions"] = none_permissions
    report["route_authority"][0]["effective_permissions"] = none_permissions
    first_route = report["routes"][0]
    second_route = {**first_route, "event_variant": "ACTIVITY:synchronize"}
    second_route["route_id"] = canonical_route_id_v1(
        {key: value for key, value in second_route.items() if key != "route_id"}
    )
    second_job = copy.deepcopy(report["route_authority"][0])
    second_job["route_id"] = second_route["route_id"]
    second_job["declared_permissions"]["actions"] = "read"
    second_job["effective_permissions"]["actions"] = "read"
    report["routes"] = sorted([first_route, second_route], key=lambda item: item["route_id"])
    report["route_authority"] = sorted([report["route_authority"][0], second_job], key=lambda item: item["route_id"])
    report["inventory"]["route_count"] = 2
    assert valid_report_shape(report)
    assert len({job["effective_permissions"]["actions"] for job in report["route_authority"]}) == 2
