from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path

import pytest
from tests.ci_shadow_pr3b_report_examples import (
    make_unprivileged,
    profile_report,
    rebind_route,
    report_example,
    report_with_one_codeql_route,
)

ROOT = Path(__file__).resolve().parents[2]
REPORT_SCHEMA = ROOT / "evals/agent/workflow-security-privileged-report.schema.json"
_COUNT_FIELDS = ("workflow_count", "job_count", "edge_count", "root_count", "route_count")
_EVIDENCE_FIELDS = "inventory roots route_authority privileged_profiles findings".split()


def _report_module():
    return importlib.import_module("tools.agent_policy.workflow_privilege_report")


def _schema() -> dict[str, object]:
    value = json.loads(REPORT_SCHEMA.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _assert_invalid(report: dict[str, object]) -> None:
    report_module = _report_module()
    with pytest.raises(report_module.InternalReportError):
        report_module.validate_report(report, _schema())


def _evidence(report: dict[str, object]):
    contracts = importlib.import_module("tools.agent_policy.workflow_privilege_contracts")
    identities = tuple(contracts.canonical_sha256(report[section]) for section in _EVIDENCE_FIELDS)
    return contracts.ReportEvidence(identities, report["status"] != "PASS")


def test_closed_report_validation_and_canonical_rendering() -> None:
    report_module = _report_module()
    report = report_with_one_codeql_route()

    report_module.validate_report(report, _schema())

    expected_json = (
        json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode() + b"\n"
    )
    assert report_module.canonical_json_bytes(report) == expected_json
    assert report_module.render_text(report) == (
        "status=PASS complete=true workflows=1 jobs=1 edges=0 roots=1 routes=1\n"
        "finding_count=0\n"
        "runbook=docs/cicd/runbooks.md#semantic-pr-privilege-boundary\n"
        "recheck=uv run python tools/agent_policy/workflow_security_privileged.py "
        "--root . --format text\n"
    )


def test_finding_rendering_is_compact_ascii_and_has_no_newline() -> None:
    report_module = _report_module()
    finding = {
        "code": "PRIVILEGE_UNKNOWN_EXPRESSION",
        "status": "UNVERIFIED",
        "subject": ".github/workflows/данные.yml",
        "route_id": None,
        "detail": "неизвестное выражение",
        "recovery_command_id": "SIMPLIFY_PRIVILEGE_GUARD",
    }

    rendered = report_module.canonical_finding_json(finding)

    assert rendered == json.dumps(finding, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    assert "\n" not in rendered
    assert "\\u043d" in rendered


@pytest.mark.parametrize(
    "mutation",
    (
        lambda report: report.update(extra=True),
        lambda report: report["inventory"].update(route_count=True),
        lambda report: report["routes"].append(copy.deepcopy(report["routes"][0])),
        lambda report: report["route_authority"][0].update(profile_id=None),
    ),
)
def test_report_schema_rejects_nonclosed_or_incomplete_evidence(mutation) -> None:
    report = report_with_one_codeql_route()
    mutation(report)
    _assert_invalid(report)


def test_finalizer_binds_snapshot_and_complete_graph_routes() -> None:
    report_module = _report_module()
    contracts = importlib.import_module("tools.agent_policy.workflow_privilege_contracts")
    report = report_with_one_codeql_route()
    route_ids = tuple(route["route_id"] for route in report["routes"])
    reference = contracts.SnapshotReference(report["policy"]["sha256"], 1, report["inventory"]["manifest_sha256"], True)

    assert (
        report_module.finalize_report(
            report,
            snapshot_reference=reference,
            canonical_route_ids=route_ids,
            evidence=_evidence(report),
            schema=_schema(),
        )
        == report
    )

    bad_references = (
        contracts.SnapshotReference("a" * 64, 1, reference.manifest_sha256, True),
        contracts.SnapshotReference(reference.policy_sha256, None, reference.manifest_sha256, True),
        contracts.SnapshotReference(reference.policy_sha256, 1, None, False),
    )
    for bad_reference in bad_references:
        with pytest.raises(report_module.InternalReportError):
            report_module.finalize_report(
                report,
                snapshot_reference=bad_reference,
                canonical_route_ids=route_ids,
                evidence=_evidence(report),
                schema=_schema(),
            )

    for incomplete_ids in ((), (*route_ids, route_ids[0]), ("f" * 64,)):
        with pytest.raises(report_module.InternalReportError):
            report_module.finalize_report(
                report,
                snapshot_reference=reference,
                canonical_route_ids=incomplete_ids,
                evidence=_evidence(report),
                schema=_schema(),
            )


def test_finalizer_binds_post_proof_route_identity_after_reclassification() -> None:
    report_module = _report_module()
    contracts = importlib.import_module("tools.agent_policy.workflow_privilege_contracts")
    report = report_with_one_codeql_route()
    preproof_id = report["routes"][0]["route_id"]
    make_unprivileged(report)
    report["routes"][0]["classification"] = "PROVEN_NOT_PR_REACHABLE"
    report["route_authority"][0]["classification"] = "PROVEN_NOT_PR_REACHABLE"
    rebind_route(report)
    resolved_id = report["routes"][0]["route_id"]
    reference = contracts.SnapshotReference(report["policy"]["sha256"], 1, report["inventory"]["manifest_sha256"], True)

    assert resolved_id != preproof_id
    assert (
        report_module.finalize_report(
            report,
            snapshot_reference=reference,
            canonical_route_ids=(resolved_id,),
            evidence=_evidence(report),
            schema=_schema(),
        )
        == report
    )
    with pytest.raises(report_module.InternalReportError):
        report_module.finalize_report(
            report,
            snapshot_reference=reference,
            canonical_route_ids=(preproof_id,),
            evidence=_evidence(report),
            schema=_schema(),
        )


@pytest.mark.parametrize("field", ("workflow", "job_id", "classification"))
def test_report_rejects_authority_coordinates_that_disagree_with_route(field: str) -> None:
    report = report_with_one_codeql_route()
    report["route_authority"][0][field] = "drifted"
    _assert_invalid(report)


def test_pass_report_rejects_privileged_authority_without_profile_evidence() -> None:
    report = report_with_one_codeql_route()
    report["route_authority"][0]["profile_id"] = None
    report["privileged_profiles"] = []
    _assert_invalid(report)


@pytest.mark.parametrize(
    ("field", "replacement"),
    (("permission_source", "JOB"), ("secrets", "EXPLICIT"), ("environment", "production")),
)
def test_profile_evidence_rejects_incomplete_authority_binding(field: str, replacement: object) -> None:
    report = report_with_one_codeql_route()
    report["route_authority"][0][field] = replacement
    _assert_invalid(report)


@pytest.mark.parametrize(
    "report",
    (
        pytest.param(report_with_one_codeql_route(), id="job-count"),
        pytest.param(profile_report("governance"), id="edge-count"),
    ),
)
def test_report_rejects_inventory_counts_below_route_coordinates(report: dict[str, object]) -> None:
    inventory = report["inventory"]
    assert isinstance(inventory, dict)
    inventory["job_count"] = 0
    inventory["edge_count"] = 0
    _assert_invalid(report)


def test_incomplete_resource_report_requires_null_manifest_identity() -> None:
    report_module = _report_module()
    report = report_with_one_codeql_route()
    report.update(status="UNVERIFIED", ok=False)
    report["inventory"].update(
        complete=False,
        route_count=report["limits"]["routes"] + 1,
        overflow_dimensions=["routes"],
    )
    report["findings"] = [
        {
            "code": "PRIVILEGE_RESOURCE_LIMIT",
            "status": "UNVERIFIED",
            "subject": ".github/workflows",
            "route_id": None,
            "detail": "route enumeration exceeded its closed limit",
            "recovery_command_id": "REDUCE_OR_PARTITION_WORKFLOWS",
        }
    ]

    with pytest.raises(report_module.InternalReportError):
        report_module.validate_report(report, _schema())

    report["inventory"]["manifest_sha256"] = None
    report_module.validate_report(report, _schema())


def _finding(code: str, recovery: str) -> dict[str, object]:
    return {
        "code": code,
        "status": "UNVERIFIED",
        "subject": ".github/workflows",
        "route_id": None,
        "detail": "closed report invariant test",
        "recovery_command_id": recovery,
    }


@pytest.mark.parametrize("version", (True, 1.0), ids=("boolean", "float"))
@pytest.mark.parametrize("target", ("report", "policy"))
def test_schema_identity_requires_an_exact_integer(version: object, target: str) -> None:
    report = report_with_one_codeql_route()
    container = report if target == "report" else report["policy"]
    container["schema_version"] = version
    _assert_invalid(report)


@pytest.mark.parametrize("section", ("inventory", "limits"))
def test_inventory_and_limit_counts_require_exact_integers(section: str) -> None:
    template = report_with_one_codeql_route()
    fields = _COUNT_FIELDS if section == "inventory" else template["limits"]
    for field in fields:
        report = report_with_one_codeql_route()
        report[section][field] = float(report[section][field])
        _assert_invalid(report)


@pytest.mark.parametrize(
    ("factory", "policy", "inventory", "finding"),
    (
        pytest.param(
            report_example,
            {"sha256": None, "schema_version": None},
            {},
            _finding("PRIVILEGE_INVALID_POLICY", "REPAIR_PRIVILEGE_POLICY"),
            id="complete-null-policy",
        ),
        pytest.param(
            report_example,
            {"sha256": None, "schema_version": 1},
            {},
            _finding("PRIVILEGE_CONCURRENT_MUTATION", "RERUN_IMMUTABLE_CHECKOUT"),
            id="schema-without-policy-bytes",
        ),
        pytest.param(
            report_example,
            {},
            {"manifest_sha256": None},
            _finding("PRIVILEGE_CONCURRENT_MUTATION", "RERUN_IMMUTABLE_CHECKOUT"),
            id="complete-null-manifest",
        ),
        pytest.param(
            report_example,
            {},
            {"overflow_dimensions": ["expression_bytes"]},
            _finding("PRIVILEGE_RESOURCE_LIMIT", "REDUCE_OR_PARTITION_WORKFLOWS"),
            id="complete-overflow",
        ),
        pytest.param(
            report_example,
            {},
            {"complete": False, "manifest_sha256": None, "overflow_dimensions": ["expression_bytes"]},
            _finding("PRIVILEGE_CONCURRENT_MUTATION", "RERUN_IMMUTABLE_CHECKOUT"),
            id="overflow-without-resource-finding",
        ),
        pytest.param(
            report_example,
            {},
            {"complete": False, "manifest_sha256": None},
            _finding("PRIVILEGE_RESOURCE_LIMIT", "REDUCE_OR_PARTITION_WORKFLOWS"),
            id="resource-finding-without-overflow",
        ),
        pytest.param(
            report_with_one_codeql_route,
            {},
            {"complete": False, "manifest_sha256": None, "root_count": 0},
            _finding("PRIVILEGE_CONCURRENT_MUTATION", "RERUN_IMMUTABLE_CHECKOUT"),
            id="incomplete-root-count-below-evidence",
        ),
        pytest.param(
            report_with_one_codeql_route,
            {},
            {"complete": False, "manifest_sha256": None, "route_count": 0},
            _finding("PRIVILEGE_CONCURRENT_MUTATION", "RERUN_IMMUTABLE_CHECKOUT"),
            id="incomplete-route-count-below-evidence",
        ),
    ),
)
def test_report_rejects_impossible_identity_resource_and_count_states(factory, policy, inventory, finding) -> None:
    report = factory()
    if report["route_authority"]:
        make_unprivileged(report)
    report.update(status="UNVERIFIED", ok=False)
    report["policy"].update(policy)
    report["inventory"].update(inventory)
    report["findings"] = [finding]
    _assert_invalid(report)


@pytest.mark.parametrize("missing_dimension", (False, True))
def test_saturated_inventory_count_and_overflow_dimension_are_biconditional(missing_dimension: bool) -> None:
    report = report_with_one_codeql_route()
    report.update(status="UNVERIFIED", ok=False)
    report["inventory"].update(complete=False, manifest_sha256=None)
    if missing_dimension:
        report["inventory"]["workflow_count"] = report["limits"]["workflow_files"] + 1
        report["inventory"]["overflow_dimensions"] = []
    else:
        report["inventory"]["overflow_dimensions"] = ["workflow_files"]
    report["findings"] = [_finding("PRIVILEGE_RESOURCE_LIMIT", "REDUCE_OR_PARTITION_WORKFLOWS")]
    _assert_invalid(report)


def test_pass_rejects_pull_request_target_even_without_privilege() -> None:
    report = report_with_one_codeql_route()
    make_unprivileged(report)
    report["roots"][0]["event"] = "pull_request_target"
    _assert_invalid(report)


@pytest.mark.parametrize("variant", ("opened", "CLOSED", "ACTIVITY:opened>BOGUS:completed"))
def test_event_variant_uses_the_closed_v1_grammar(variant: str) -> None:
    report = report_with_one_codeql_route()
    make_unprivileged(report)
    report["routes"][0]["event_variant"] = variant
    rebind_route(report)
    _assert_invalid(report)


@pytest.mark.parametrize(
    "chain",
    (
        (("analyze", "analyze"),),
        (("prepare", "publish"), ("publish", "prepare")),
        (("prepare", "build"), ("build", "publish"), ("publish", "build")),
    ),
    ids=("self-loop", "cycle", "job-revisit"),
)
def test_needs_chain_rejects_self_loops_cycles_and_job_revisits(chain) -> None:
    report = report_with_one_codeql_route()
    make_unprivileged(report)
    workflow = report["routes"][0]["workflow"]
    edges = [
        {
            "kind": "NEEDS",
            "source_workflow": workflow,
            "source_job": source,
            "target_workflow": workflow,
            "target_job": target,
        }
        for source, target in chain
    ]
    report["routes"][0].update(edge_chain=edges, job_id=chain[-1][1])
    report["route_authority"][0]["job_id"] = chain[-1][1]
    report["inventory"].update(job_count=len({job for edge in chain for job in edge}), edge_count=len(edges))
    rebind_route(report)
    _assert_invalid(report)
