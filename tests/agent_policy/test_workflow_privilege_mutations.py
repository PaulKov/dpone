from __future__ import annotations

import hashlib
import importlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest
from tests.ci_shadow_pr3b_report_examples import make_unprivileged, report_with_one_codeql_route

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/ci-shadow-pr3b"
REPORT_SCHEMA = ROOT / "evals/agent/workflow-security-privileged-report.schema.json"
_EVIDENCE_FIELDS = "inventory roots route_authority privileged_profiles findings".split()


@dataclass(frozen=True, slots=True)
class Mutation:
    name: str
    relative_path: str
    old: str
    new: str
    expected_codes: frozenset[str]


PROFILE_MUTATIONS = (
    Mutation(
        "codeql-checkout-credentials",
        ".github/workflows/codeql.yml",
        "          persist-credentials: false",
        "          persist-credentials: true",
        frozenset({"PRIVILEGE_CODEQL_PROFILE_DRIFT"}),
    ),
    Mutation(
        "codeql-custom-config",
        ".github/workflows/codeql.yml",
        "          languages: python",
        "          languages: python\n          config-file: .github/codeql/codeql-config.yml",
        frozenset({"PRIVILEGE_CODEQL_PROFILE_DRIFT"}),
    ),
    Mutation(
        "producer-overwrite",
        ".github/workflows/ci.yml",
        "          overwrite: false",
        "          overwrite: true",
        frozenset({"PRIVILEGE_ADR0037_PROFILE_DRIFT"}),
    ),
    Mutation(
        "finalizer-digest-mismatch",
        ".github/workflows/ci.yml",
        "          digest-mismatch: error",
        "          digest-mismatch: warn",
        frozenset({"PRIVILEGE_ADR0037_PROFILE_DRIFT"}),
    ),
    Mutation(
        "finalizer-subject-glob",
        ".github/workflows/ci.yml",
        "          subject-path: test_artifacts/agent-policy/attested-governance/agent_governance_gate.json",
        "          subject-path: test_artifacts/agent-policy/attested-governance/*",
        frozenset({"PRIVILEGE_ADR0037_PROFILE_DRIFT"}),
    ),
    Mutation(
        "merged-predicate",
        ".github/workflows/agent-pr-receipt.yml",
        "    if: github.event.action == 'closed' && github.event.pull_request.merged == true",
        "    if: github.event.action == 'closed'",
        frozenset({"PRIVILEGE_ADR0037_PROFILE_DRIFT"}),
    ),
    Mutation(
        "pull-request-target",
        ".github/workflows/codeql.yml",
        "  pull_request:",
        "  pull_request_target:",
        frozenset(
            {
                "PRIVILEGE_PULL_REQUEST_TARGET",
                "PRIVILEGE_CODEQL_PROFILE_DRIFT",
            }
        ),
    ),
)


def _copy_fixture(tmp_path: Path) -> Path:
    destination = tmp_path / "repository"
    shutil.copytree(FIXTURES / "target", destination)
    return destination


def _modules():
    service = importlib.import_module("tools.agent_policy.workflow_privilege_service")
    report = importlib.import_module("tools.agent_policy.workflow_privilege_report")
    return service, report


def _codes(value: dict[str, object]) -> set[str]:
    findings = value["findings"]
    assert isinstance(findings, list)
    return {str(item["code"]) for item in findings}


def _schema() -> dict[str, object]:
    value = json.loads(REPORT_SCHEMA.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _scan_twice(root: Path) -> tuple[dict[str, object], bytes]:
    service, report_module = _modules()
    first = service.scan_repository(root)
    second = service.scan_repository(root)
    first_bytes = report_module.canonical_json_bytes(first)
    assert (first, first_bytes) == (second, report_module.canonical_json_bytes(second))
    return first, first_bytes


@pytest.mark.parametrize("mutation", PROFILE_MUTATIONS, ids=lambda item: item.name)
def test_closed_profiles_reject_drift_deterministically(
    mutation: Mutation,
    tmp_path: Path,
) -> None:
    root = _copy_fixture(tmp_path)
    path = root / mutation.relative_path
    source = path.read_text(encoding="utf-8")
    assert source.count(mutation.old) == 1
    path.write_text(source.replace(mutation.old, mutation.new), encoding="utf-8")

    report, first_bytes = _scan_twice(root)

    assert report["status"] == "FAIL"
    assert mutation.expected_codes <= _codes(report)
    assert first_bytes.endswith(b"\n")


@pytest.mark.parametrize(
    ("name", "workflow", "status", "expected_code"),
    (
        (
            "direct-write",
            """
name: Direct write
on: pull_request
permissions:
  contents: write
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - run: echo publish
""",
            "FAIL",
            "PRIVILEGE_UNAPPROVED_PR_WRITE",
        ),
        (
            "external-call",
            """
name: External call
on: pull_request
permissions: {}
jobs:
  call:
    uses: owner/repository/.github/workflows/reusable.yml@0123456789012345678901234567890123456789
""",
            "UNVERIFIED",
            "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE",
        ),
        (
            "self-hosted",
            """
name: Self hosted
on: pull_request
permissions: {}
jobs:
  build:
    runs-on: [self-hosted, linux]
    steps:
      - run: echo build
""",
            "FAIL",
            "PRIVILEGE_PR_SELF_HOSTED",
        ),
        (
            "environment",
            """
name: Environment
on: pull_request
permissions: {}
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - run: echo deploy
""",
            "FAIL",
            "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT",
        ),
        (
            "unknown-privileged-guard",
            """
name: Unknown privileged guard
on: pull_request
permissions: {}
jobs:
  publish:
    if: github.repository_owner == 'PaulKov'
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - run: echo publish
""",
            "UNVERIFIED",
            "PRIVILEGE_UNKNOWN_EXPRESSION",
        ),
    ),
)
def test_route_authority_mutations_fail_closed_and_replay(
    name: str,
    workflow: str,
    status: str,
    expected_code: str,
    tmp_path: Path,
) -> None:
    root = _copy_fixture(tmp_path)
    (root / ".github/workflows" / f"{name}.yml").write_text(
        workflow.lstrip(),
        encoding="utf-8",
    )

    report, _ = _scan_twice(root)

    assert report["status"] == status
    assert expected_code in _codes(report)


def test_read_only_unknown_expression_has_no_public_finding(tmp_path: Path) -> None:
    root = _copy_fixture(tmp_path)
    (root / ".github/workflows/read-only-unknown.yml").write_text(
        """
name: Read-only unknown
on: pull_request
permissions: {}
jobs:
  inspect:
    if: github.repository_owner == 'PaulKov'
    runs-on: ubuntu-latest
    steps:
      - run: echo inspect
""".lstrip(),
        encoding="utf-8",
    )

    report, _ = _scan_twice(root)

    assert (report["status"], "PRIVILEGE_UNKNOWN_EXPRESSION" in _codes(report)) == ("PASS", False)


def test_report_validation_rejects_duplicate_canonical_finding() -> None:
    _, report_module = _modules()
    report = report_with_one_codeql_route()
    report["route_authority"][0]["workflow_name"] = "é" * 256
    with pytest.raises(report_module.InternalReportError):
        report_module.validate_report(report, _schema())
    report["route_authority"][0]["workflow_name"] = "CodeQL"
    finding = {
        "code": "PRIVILEGE_UNAPPROVED_PR_WRITE",
        "status": "FAIL",
        "subject": ".github/workflows/codeql.yml::analyze",
        "route_id": report["routes"][0]["route_id"],
        "detail": "unprofiled write is reachable\nwith\tcontext",
        "recovery_command_id": "REDUCE_OR_ISOLATE_PR_AUTHORITY",
    }
    report.update(status="FAIL", ok=False, findings=[{**finding, "subject": "unsafe\0subject"}])
    with pytest.raises(report_module.InternalReportError):
        report_module.validate_report(report, _schema())
    report.update(status="FAIL", ok=False, findings=[finding])
    report_module.validate_report(report, _schema())
    report["findings"].append(dict(finding))

    with pytest.raises(report_module.InternalReportError):
        report_module.validate_report(report, _schema())


def test_fixture_bytes_are_frozen_and_target_removes_custom_codeql() -> None:
    expected = {
        "pre-split/.github/workflows/ci.yml": "531a033f54771bfbbd6dd83b39ebb6dce51ceefecd9f5ef8a05706fcd7a61936",
        "pre-split/.github/workflows/codeql.yml": "5d671699e7d6c5ff4feec7e65a1dbd55f87b6cba61a9968e3b09288edd762cd3",
        "pre-split/.github/codeql/codeql-config.yml": "d205fae08b32848b8333f5e64410612f63071da26f1da6ad0e4617d01ea14ec5",
        "target/.github/workflows/ci.yml": "151049cfe6a2935a12a9f1402037f5884535e4913edba5860e432806a401556d",
        "target/.github/workflows/codeql.yml": "204b6ef34b8798be2d771c45d3706f2b4f43be893889ccf78b52ee3b92c5eb2a",
    }
    for relative, digest in expected.items():
        assert hashlib.sha256((FIXTURES / relative).read_bytes()).hexdigest() == digest
    assert not (FIXTURES / "target/.github/codeql/codeql-config.yml").exists()


def test_minimal_report_preserves_saturated_inventory_and_is_below_64_kib() -> None:
    _, report_module = _modules()
    contracts = importlib.import_module("tools.agent_policy.workflow_privilege_contracts")
    reference = contracts.SnapshotReference("0" * 64, 1, None, False)
    snapshot = contracts.Snapshot(workflow_count=257)
    findings = (
        contracts.Finding(
            "PRIVILEGE_WRITE_ALL",
            "FAIL",
            ".github/workflows/z.yml::publish",
            "write-all is reachable",
            "REPLACE_WRITE_ALL",
        ),
        contracts.Finding(
            "PRIVILEGE_UNKNOWN_EXPRESSION",
            "UNVERIFIED",
            ".github/workflows/a.yml::test",
            "expression is unknown",
            "SIMPLIFY_PRIVILEGE_GUARD",
        ),
        contracts.Finding(
            "PRIVILEGE_UNAPPROVED_PR_WRITE",
            "FAIL",
            ".github/workflows/a.yml::publish",
            "unprofiled write is reachable",
            "REDUCE_OR_ISOLATE_PR_AUTHORITY",
        ),
    )
    source_dimensions = (
        "text_stdout_bytes report_bytes report_bytes workflow_files jobs graph_edges roots routes".split()
    )
    inventory = {
        "workflow_count": 257,
        "job_count": 4097,
        "edge_count": 8193,
        "root_count": 513,
        "route_count": 16385,
        "overflow_dimensions": source_dimensions,
    }

    report = report_module.build_minimal_report(snapshot, reference, findings, inventory)
    source_dimensions.append("findings")

    assert report["roots"] == report["routes"] == report["route_authority"] == report["privileged_profiles"] == []
    assert [item["code"] for item in report["findings"]] == [
        "PRIVILEGE_UNAPPROVED_PR_WRITE",
        "PRIVILEGE_RESOURCE_LIMIT",
    ]
    assert report["inventory"] == {
        "complete": False,
        "manifest_sha256": None,
        **inventory,
        "overflow_dimensions": [
            "graph_edges",
            "jobs",
            "report_bytes",
            "roots",
            "routes",
            "text_stdout_bytes",
            "workflow_files",
        ],
    }
    evidence = contracts.ReportEvidence(
        tuple(contracts.canonical_sha256(report[field]) for field in _EVIDENCE_FIELDS),
        True,
    )
    assert report_module.finalize_report(
        report, snapshot_reference=reference, canonical_route_ids=(), evidence=evidence, schema=_schema()
    )
    assert len(report_module.canonical_json_bytes(report)) < 65_536
    assert len(report_module.render_text(report).encode()) < 65_536

    semantic_evidence = json.loads(json.dumps(report))
    route_report = report_with_one_codeql_route()
    make_unprivileged(route_report)
    for field in ("roots", "routes", "route_authority"):
        semantic_evidence[field] = route_report[field]
    extra_finding = json.loads(json.dumps(report))
    extra_finding["findings"].append(findings[1].to_mapping())
    duplicate_resource = json.loads(json.dumps(report))
    second_resource = dict(duplicate_resource["findings"][-1], subject="z-resource")
    duplicate_resource["findings"].append(second_resource)
    for candidate in (semantic_evidence, extra_finding, duplicate_resource):
        with pytest.raises(report_module.InternalReportError):
            report_module.validate_report(candidate, _schema())

    for invalid_dimensions in (["text_stdout_bytes", "report_bytes"], ["report_bytes", "report_bytes"]):
        report["inventory"]["overflow_dimensions"] = invalid_dimensions
        with pytest.raises(report_module.InternalReportError):
            report_module.validate_report(report, _schema())
