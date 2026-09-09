from __future__ import annotations

import copy
from pathlib import Path

from tests.ci_shadow_pr3b_report_contract_model import valid_report_shape
from tests.ci_shadow_pr3b_report_examples import rebind_route as _rebind_route
from tests.ci_shadow_pr3b_report_examples import report_example as _report_example
from tests.ci_shadow_pr3b_report_examples import report_with_one_codeql_route as _report_with_one_codeql_route

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"


def _read() -> str:
    return SPEC.read_text(encoding="utf-8")


def _squash(value: str) -> str:
    return " ".join(value.split())


def test_pr3b_report_shapes_and_text_protocol_are_exact() -> None:
    spec = _squash(_read())
    for expected in (
        "Every listed field is required",
        "every object has `additionalProperties: false`",
        "all 17 `permission_access` keys",
        "value must be allowed by that permission's exact policy row",
        "exactly one authority record for every",
        "exactly one match for each and only each",
        "at most two roots per workflow and max 512",
        "suffix if and only if its causal `edge_chain` contains exactly one",
        "Counts are exact only when `complete=true`",
        "saturated observed lower bounds",
        "manifest is SHA-256 of canonical JSON array entries",
        "If route, path, finding, or output expansion reaches N+1",
        "first 4,095 candidates in that order plus the resource finding",
        "lexicographically smallest observed `FAIL`",
        "fallback below 64 KiB",
        "empty `roots`/`routes`/`route_authority`/ `privileged_profiles` arrays",
        "status=<PASS|FAIL|UNVERIFIED>",
        "finding[0001]=<canonical compact JSON of the complete finding object>",
        "runbook=docs/cicd/runbooks.md#semantic-pr-privilege-boundary",
        "recheck=uv run python tools/agent_policy/workflow_security_privileged.py --root . --format text",
    ):
        assert expected in spec

    report = _report_example()
    assert valid_report_shape(report)
    null_policy = copy.deepcopy(report)
    null_policy["policy"]["sha256"] = None
    null_manifest = copy.deepcopy(report)
    null_manifest["inventory"]["manifest_sha256"] = None
    overflow_pass = copy.deepcopy(report)
    overflow_pass["inventory"].update(
        complete=False,
        manifest_sha256=None,
        route_count=report["limits"]["routes"] + 1,
        overflow_dimensions=["routes"],
    )
    negative_count = copy.deepcopy(report)
    negative_count["inventory"]["route_count"] = -1
    arbitrary_limits = copy.deepcopy(report)
    arbitrary_limits["limits"]["routes"] = 1
    invalid_root = copy.deepcopy(report)
    invalid_root["roots"] = [{"workflow": 7, "event": "pull_request"}]
    empty_inventory = copy.deepcopy(report)
    empty_inventory["inventory"]["workflow_count"] = 0
    root_without_route = copy.deepcopy(report)
    root_without_route["roots"] = [{"workflow": ".github/workflows/codeql.yml", "event": "pull_request"}]
    root_without_route["inventory"]["root_count"] = 1
    mutations = [
        {**report, "extra": True},
        {**report, "root": "/tmp/repository"},
        {**report, "ok": False},
        {**report, "status": "UNKNOWN"},
        {**report, "policy": {**report["policy"], "sha256": "bad"}},
        {**report, "inventory": {**report["inventory"], "workflow_count": True}},
        null_policy,
        null_manifest,
        overflow_pass,
        negative_count,
        arbitrary_limits,
        invalid_root,
        empty_inventory,
        root_without_route,
    ]
    assert not any(valid_report_shape(mutation) for mutation in mutations)


def test_pr3b_manifest_completeness_and_digest_are_biconditional() -> None:
    report = _report_example()
    digest = "a" * 64

    cases = (
        (False, "garbage", False),
        (False, digest, False),
        (True, None, False),
        (True, digest, True),
    )
    for complete, manifest, accepted in cases:
        candidate = copy.deepcopy(report)
        candidate["inventory"].update(complete=complete, manifest_sha256=manifest)
        assert valid_report_shape(candidate) is accepted


def test_pr3b_resource_limit_report_is_closed_and_saturated() -> None:
    report = _report_example()
    report.update(status="UNVERIFIED", ok=False)
    report["inventory"].update(
        complete=False,
        manifest_sha256=None,
        route_count=report["limits"]["routes"] + 1,
        overflow_dimensions=["routes"],
    )
    report["findings"] = [
        {
            "code": "PRIVILEGE_RESOURCE_LIMIT",
            "status": "UNVERIFIED",
            "subject": "routes",
            "route_id": None,
            "detail": "observed routes exceed the closed limit",
            "recovery_command_id": "REDUCE_OR_PARTITION_WORKFLOWS",
        }
    ]
    assert valid_report_shape(report)
    swapped_resource = copy.deepcopy(report)
    swapped_resource.update(status="FAIL", ok=False)
    swapped_resource["findings"][0]["status"] = "FAIL"
    deterministic_as_unknown = copy.deepcopy(report)
    deterministic_as_unknown["findings"][0].update(
        code="PRIVILEGE_PULL_REQUEST_TARGET",
        recovery_command_id="REMOVE_PULL_REQUEST_TARGET",
    )
    assert not valid_report_shape(swapped_resource)
    assert not valid_report_shape(deterministic_as_unknown)
    report["inventory"]["route_count"] -= 1
    assert not valid_report_shape(report)


def test_pr3b_report_nested_shapes_reject_ambiguous_values() -> None:
    report = _report_with_one_codeql_route()
    assert valid_report_shape(report)
    missing_authority = copy.deepcopy(report)
    missing_authority["route_authority"] = []
    missing_profile = copy.deepcopy(report)
    missing_profile["privileged_profiles"] = []
    unexpected_profile = copy.deepcopy(report)
    unexpected_profile["route_authority"][0]["profile_id"] = None
    mismatched_profile = copy.deepcopy(report)
    mismatched_profile["privileged_profiles"][0]["id"] = "ADR0037_GOVERNANCE_SOURCE_ATTESTOR"
    invalid_name = copy.deepcopy(report)
    invalid_name["route_authority"][0]["workflow_name"] = None
    wrong_variant = copy.deepcopy(report)
    wrong_variant["routes"][0]["event_variant"] = "CLOSED_MERGED"
    _rebind_route(wrong_variant)
    wrong_root = copy.deepcopy(report)
    wrong_root["roots"][0]["event"] = "pull_request_target"
    wrong_edge = copy.deepcopy(report)
    wrong_edge["routes"][0]["edge_chain"] = [
        {
            "kind": "NEEDS",
            "source_workflow": ".github/workflows/codeql.yml",
            "source_job": "prepare",
            "target_workflow": ".github/workflows/codeql.yml",
            "target_job": "analyze",
        }
    ]
    _rebind_route(wrong_edge)
    suffix_without_edge = copy.deepcopy(report)
    suffix_without_edge["routes"][0]["event_variant"] = "ACTIVITY:opened>WORKFLOW_RUN:completed"
    _rebind_route(suffix_without_edge)
    edge_without_suffix = copy.deepcopy(report)
    edge_without_suffix["privileged_profiles"] = []
    edge_without_suffix["route_authority"][0].update(
        workflow=".github/workflows/consumer-one.yml",
        profile_id=None,
    )
    edge_without_suffix["routes"][0].update(
        workflow=".github/workflows/consumer-one.yml",
        edge_chain=[
            {
                "kind": "WORKFLOW_RUN",
                "source_workflow": ".github/workflows/codeql.yml",
                "source_job": None,
                "target_workflow": ".github/workflows/consumer-one.yml",
                "target_job": None,
            }
        ],
    )
    _rebind_route(edge_without_suffix)
    two_workflow_run_edges = copy.deepcopy(edge_without_suffix)
    two_workflow_run_edges["routes"][0].update(
        event_variant="ACTIVITY:opened>WORKFLOW_RUN:completed",
        workflow=".github/workflows/consumer-two.yml",
    )
    two_workflow_run_edges["routes"][0]["edge_chain"].append(
        {
            "kind": "WORKFLOW_RUN",
            "source_workflow": ".github/workflows/consumer-one.yml",
            "source_job": None,
            "target_workflow": ".github/workflows/consumer-two.yml",
            "target_job": None,
        }
    )
    two_workflow_run_edges["route_authority"][0]["workflow"] = ".github/workflows/consumer-two.yml"
    _rebind_route(two_workflow_run_edges)
    wrong_final_job = copy.deepcopy(report)
    wrong_final_job["privileged_profiles"] = []
    wrong_final_job["route_authority"][0]["profile_id"] = None
    wrong_final_job["routes"][0]["edge_chain"] = [
        {
            "kind": "NEEDS",
            "source_workflow": ".github/workflows/codeql.yml",
            "source_job": "prepare",
            "target_workflow": ".github/workflows/codeql.yml",
            "target_job": "not-analyze",
        }
    ]
    _rebind_route(wrong_final_job)
    disconnected_jobs = copy.deepcopy(wrong_final_job)
    disconnected_jobs["routes"][0]["edge_chain"] = [
        {
            "kind": "NEEDS",
            "source_workflow": ".github/workflows/codeql.yml",
            "source_job": "prepare",
            "target_workflow": ".github/workflows/codeql.yml",
            "target_job": "middle",
        },
        {
            "kind": "NEEDS",
            "source_workflow": ".github/workflows/codeql.yml",
            "source_job": "other",
            "target_workflow": ".github/workflows/codeql.yml",
            "target_job": "analyze",
        },
    ]
    _rebind_route(disconnected_jobs)
    disconnected_local_call = copy.deepcopy(wrong_final_job)
    disconnected_local_call["routes"][0].update(
        workflow=".github/workflows/reusable.yml",
        edge_chain=[
            {
                "kind": "NEEDS",
                "source_workflow": ".github/workflows/codeql.yml",
                "source_job": "prepare",
                "target_workflow": ".github/workflows/codeql.yml",
                "target_job": "middle",
            },
            {
                "kind": "LOCAL_WORKFLOW_CALL",
                "source_workflow": ".github/workflows/codeql.yml",
                "source_job": "other",
                "target_workflow": ".github/workflows/reusable.yml",
                "target_job": None,
            },
        ],
    )
    disconnected_local_call["route_authority"][0]["workflow"] = ".github/workflows/reusable.yml"
    _rebind_route(disconnected_local_call)
    profile_permission_drift = copy.deepcopy(report)
    profile_permission_drift["route_authority"][0]["effective_permissions"]["security-events"] = "none"
    unprofiled_write = copy.deepcopy(report)
    unprofiled_write["route_authority"][0]["profile_id"] = None
    unprofiled_write["privileged_profiles"] = []
    pull_request_target_pass = copy.deepcopy(report)
    pull_request_target_pass["roots"][0]["event"] = "pull_request_target"
    assert not any(
        valid_report_shape(candidate)
        for candidate in (
            missing_authority,
            missing_profile,
            unexpected_profile,
            mismatched_profile,
            invalid_name,
            wrong_variant,
            wrong_root,
            wrong_edge,
            suffix_without_edge,
            edge_without_suffix,
            two_workflow_run_edges,
            wrong_final_job,
            disconnected_jobs,
            disconnected_local_call,
            profile_permission_drift,
            unprofiled_write,
            pull_request_target_pass,
        )
    )


def test_pr3b_internal_policy_and_resource_failures_are_controlled() -> None:
    spec = _squash(_read())
    for expected in (
        "exits `3`",
        "writes no stdout",
        "PRIVILEGE_INTERNAL_REPORT_INVALID: report construction or schema validation failed",
        "without a traceback",
        "`PRIVILEGE_INTERNAL_REPORT_INVALID` is reserved for the exit-3 stderr contract",
        "`PRIVILEGE_INVALID_POLICY`",
        "`REPAIR_PRIVILEGE_POLICY`",
        "PRIVILEGE_INVALID_POLICY, snapshot_reference, policy.findings",
        "controlled_internal_exit_3(PRIVILEGE_INTERNAL_REPORT_INVALID)",
    ):
        assert expected in spec
