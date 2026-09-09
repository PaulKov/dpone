from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest
from tests.ci_shadow_pr3b_report_contract_model import valid_report_shape

ROOT = Path(__file__).resolve().parents[2]


def _fixture(tmp_path: Path, variant: str) -> Path:
    fixtures = importlib.import_module("tests.agent_policy.workflow_privilege_fixtures")
    return fixtures.copy_repository_fixture(tmp_path, variant)


def _service_module():
    return importlib.import_module("tools.agent_policy.workflow_privilege_service")


def _codes(report: dict[str, Any]) -> set[str]:
    findings = report["findings"]
    assert isinstance(findings, list)
    return {str(finding["code"]) for finding in findings}


def _scan(root: Path) -> dict[str, Any]:
    report = _service_module().scan_repository(root)
    assert report == _service_module().scan_repository(root)
    assert valid_report_shape(report)
    return report


def _scan_with_workflows(tmp_path: Path, **workflows: str) -> dict[str, Any]:
    root = _fixture(tmp_path, "target")
    directory = root / ".github/workflows"
    for name, source in workflows.items():
        (directory / f"{name.replace('_', '-')}.yml").write_text(source.lstrip(), encoding="utf-8")
    return _scan(root)


def test_target_repository_is_a_complete_deterministic_pass(tmp_path: Path) -> None:
    first = _scan(_fixture(tmp_path, "target"))
    assert first["status"] == "PASS"
    assert first["ok"] is True
    assert first["inventory"]["complete"] is True
    assert first["findings"] == []
    assert first["routes"]
    assert len(first["routes"]) == len(first["route_authority"])


def test_exact_pre_split_fixture_remains_deterministically_red(tmp_path: Path) -> None:
    first = _scan(_fixture(tmp_path, "pre-split"))
    assert first["status"] == "FAIL"
    assert first["ok"] is False
    assert {
        "PRIVILEGE_ADR0037_PROFILE_DRIFT",
        "PRIVILEGE_CODEQL_PROFILE_DRIFT",
    } <= _codes(first)


def test_current_repository_passes_only_after_atomic_pr3b_migration() -> None:
    report = _scan(ROOT)
    assert report["status"] == "PASS"
    assert report["ok"] is True
    assert report["inventory"]["complete"] is True
    assert report["findings"] == []


def test_service_exposes_one_default_composition_entry() -> None:
    service = _service_module()

    assert callable(service.WorkflowPrivilegeService.scan)
    assert callable(service.scan_repository)


def test_expression_overflow_returns_bounded_resource_report(tmp_path: Path) -> None:
    oversized = ("true||" * 1_639) + "true"
    report = _scan_with_workflows(
        tmp_path,
        expression_overflow=(
            "name: Expression overflow\non: pull_request\npermissions: {}\njobs:\n  inspect:\n"
            f"    if: ${{{{ {oversized} }}}}\n    runs-on: ubuntu-latest\n    steps: [{{run: echo inspect}}]\n"
        ),
    )
    inventory = report["inventory"]
    assert valid_report_shape(report)
    assert (report["status"], report["ok"], inventory["complete"], inventory["overflow_dimensions"]) == (
        "UNVERIFIED",
        False,
        False,
        ["expression_bytes"],
    )
    assert _codes(report) == {"PRIVILEGE_RESOURCE_LIMIT"}


@pytest.mark.parametrize(
    ("name", "trigger", "guard", "runner", "permissions", "status", "code"),
    (
        ("custom-runner", "pull_request", None, "corp-prod-runner", None, "FAIL", "PRIVILEGE_PR_SELF_HOSTED"),
        ("mixed-runner", "pull_request", None, "[ubuntu-latest, 42]", None, "FAIL", "PRIVILEGE_PR_SELF_HOSTED"),
        (
            "unknown-read-permission",
            "pull_request",
            "github.actor",
            "ubuntu-latest",
            "future-security-scope: read",
            "UNVERIFIED",
            "PRIVILEGE_UNKNOWN_PERMISSION",
        ),
        (
            "symbolic-pr-ref",
            "pull_request",
            "github.ref != 'refs/pull/1/merge'",
            "ubuntu-latest",
            "contents: write",
            "UNVERIFIED",
            "PRIVILEGE_UNKNOWN_EXPRESSION",
        ),
        (
            "develop-merged-write",
            "pull_request:\n    branches: [develop]\n    types: [closed]",
            "github.event.pull_request.merged == true && github.ref != 'refs/heads/master'",
            "ubuntu-latest",
            "contents: write",
            "FAIL",
            "PRIVILEGE_UNAPPROVED_PR_WRITE",
        ),
        (
            "ambiguous-merged-branch",
            "pull_request:\n    branches: [master, develop]\n    types: [closed]",
            "github.event.pull_request.merged == true && github.ref != 'refs/heads/master'",
            "ubuntu-latest",
            "contents: write",
            "UNVERIFIED",
            "PRIVILEGE_UNKNOWN_EXPRESSION",
        ),
        (
            "glob-merged-branch",
            "pull_request:\n    branches: ['release/**']\n    types: [closed]",
            "github.event.pull_request.merged == true && github.ref != 'refs/heads/master'",
            "ubuntu-latest",
            "contents: write",
            "UNVERIFIED",
            "PRIVILEGE_UNKNOWN_EXPRESSION",
        ),
    ),
)
def test_closed_authority_boundaries_never_false_pass(
    name: str,
    trigger: str,
    guard: str | None,
    runner: str,
    permissions: str | None,
    status: str,
    code: str,
    tmp_path: Path,
) -> None:
    guard_line = f"    if: {guard}\n" if guard else ""
    permission_block = f"    permissions:\n      {permissions}\n" if permissions else ""
    source = (
        f"name: {name}\non:\n  {trigger}\npermissions: {{}}\njobs:\n  inspect:\n"
        f"{guard_line}    runs-on: {runner}\n{permission_block}    steps:\n      - run: echo inspect\n"
    )

    report = _scan_with_workflows(tmp_path, **{name: source})

    assert report["status"] == status
    assert code in _codes(report)


@pytest.mark.parametrize("case", ("scalar-workflows", "scalar-types", "step-secret"))
def test_workflow_run_consumer_authority_is_not_skipped(case: str, tmp_path: Path) -> None:
    secret_case = case == "step-secret"
    workflows = "PR producer" if case == "scalar-workflows" else "[PR producer]"
    types = "completed" if case == "scalar-types" else "[completed]"
    permissions = "{}" if secret_case else "{contents: write}"
    steps = "[{run: echo deploy, env: {DEPLOY_KEY: '${{ secrets.DEPLOY_KEY }}'}}]" if secret_case else "[]"
    report = _scan_with_workflows(
        tmp_path,
        scalar_producer=(
            "name: PR producer\non: pull_request\npermissions: {}\njobs:\n"
            "  build: {runs-on: ubuntu-latest, steps: [{run: echo build}]}\n"
        ),
        scalar_consumer=(
            f"name: PR consumer\non:\n  workflow_run:\n    workflows: {workflows}\n    types: {types}\n"
            f"permissions: {{}}\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    permissions: {permissions}\n"
            f"    steps: {steps}\n"
        ),
    )

    assert report["status"] == "FAIL"
    expected_code = "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT" if secret_case else "PRIVILEGE_UNAPPROVED_PR_WRITE"
    assert expected_code in _codes(report)
    routes = [item for item in report["routes"] if item["workflow"].endswith("scalar-consumer.yml")]
    assert len(routes) == 3
    assert {item["event_variant"] for item in routes} == {
        "ACTIVITY:opened>WORKFLOW_RUN:completed",
        "ACTIVITY:reopened>WORKFLOW_RUN:completed",
        "ACTIVITY:synchronize>WORKFLOW_RUN:completed",
    }
    assert all([edge["kind"] for edge in item["edge_chain"]] == ["WORKFLOW_RUN"] for item in routes)
    route_ids = {item["route_id"] for item in routes}
    authorities = [item for item in report["route_authority"] if item["route_id"] in route_ids]
    assert len(authorities) == 3
    if secret_case:
        assert {item["secrets"] for item in authorities} == {"EXPLICIT"}
    else:
        assert {item["effective_permissions"]["contents"] for item in authorities} == {"write"}


@pytest.mark.parametrize(
    "expression",
    (
        "${{ toJSON(secrets) }}",
        "${{ format('{0}', secrets.API_KEY) }}",
        "${{ format('}} {0}', secrets.API_KEY) }}",
        "${{ format('{{Hello {0}!}}', secrets.API_KEY) }}",
        "${{ secrets['API_KEY'] }}",
        "${{ SeCrEtS.API_KEY }}",
        "${{ github.actor }}:${{ secrets.API_KEY }}",
    ),
)
def test_nested_secret_context_is_never_false_pass(expression: str, tmp_path: Path) -> None:
    report = _scan_with_workflows(
        tmp_path,
        nested_secret=f"""
name: Nested secret
on: pull_request
permissions: {{}}
jobs:
  inspect:
    runs-on: ubuntu-latest
    steps:
      - run: echo inspect
        env:
          TOKEN: {expression}
""",
    )

    assert report["status"] == "FAIL"
    assert "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT" in _codes(report)


@pytest.mark.parametrize(
    "expression",
    (
        "${{ github.event.secrets }}",
        "${{ github.event.no-secrets }}",
        "${{ format('secrets.API_KEY', github.actor) }}",
        "${{ 'secrets }} literal' }}",
    ),
)
def test_non_secret_context_text_does_not_create_authority(expression: str, tmp_path: Path) -> None:
    report = _scan_with_workflows(
        tmp_path,
        non_secret=f"""
name: Non secret
on: pull_request
permissions: {{}}
jobs:
  inspect:
    runs-on: ubuntu-latest
    steps:
      - run: echo inspect
        env:
          VALUE: {expression}
""",
    )

    assert report["status"] == "PASS"
    assert report["findings"] == []


def test_missing_policy_is_invalid_policy_not_concurrent_mutation(tmp_path: Path) -> None:
    root = _fixture(tmp_path, "target")
    policy = root / ".agents/policy/workflow-security-privileged.yml"
    policy.unlink()

    report = _service_module().scan_repository(root)

    inventory = report["inventory"]
    assert isinstance(inventory, dict)
    assert report["status"] == "UNVERIFIED"
    assert report["policy"] == {
        "path": ".agents/policy/workflow-security-privileged.yml",
        "sha256": None,
        "schema_version": None,
    }
    assert report["roots"] == report["routes"] == report["route_authority"] == report["privileged_profiles"] == []
    assert inventory["complete"] is False
    assert _codes(report) == {"PRIVILEGE_INVALID_POLICY"}


def test_final_snapshot_mutation_preserves_already_proven_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = importlib.import_module("tools.agent_policy.workflow_privilege_snapshot")
    root = _fixture(tmp_path, "pre-split")
    monkeypatch.setattr(snapshot.SnapshotLease, "_revalidates", lambda _lease: False)

    report = _service_module().scan_repository(root)

    assert report["status"] == "FAIL"
    assert "PRIVILEGE_CONCURRENT_MUTATION" in _codes(report)
    assert {"PRIVILEGE_ADR0037_PROFILE_DRIFT", "PRIVILEGE_CODEQL_PROFILE_DRIFT"} <= _codes(report)


@pytest.mark.parametrize(
    ("name", "jobs", "status", "code"),
    (
        (
            "fan-in",
            """
  skipped:
    if: false
    runs-on: ubuntu-latest
    steps: [{run: echo skipped}]
  reached:
    runs-on: ubuntu-latest
    steps: [{run: echo reached}]
  publish:
    needs: [skipped, reached]
    runs-on: ubuntu-latest
    permissions: {contents: write}
    steps: [{run: echo publish}]
""",
            "PASS",
            None,
        ),
        (
            "skipped-handler",
            """
  build:
    if: false
    runs-on: ubuntu-latest
    steps: [{run: echo build}]
  publish:
    needs: [build]
    if: always() && needs.build.result == 'skipped'
    runs-on: ubuntu-latest
    permissions: {contents: write}
    steps: [{run: echo publish}]
""",
            "FAIL",
            "PRIVILEGE_UNAPPROVED_PR_WRITE",
        ),
    ),
)
def test_needs_fan_in_uses_complete_predecessor_state(
    name: str,
    jobs: str,
    status: str,
    code: str | None,
    tmp_path: Path,
) -> None:
    job_source = jobs.removeprefix("\n")
    source = f"name: {name}\non: pull_request\npermissions: {{}}\njobs:\n{job_source}"

    report = _scan_with_workflows(tmp_path, **{name: source})

    assert report["status"] == status
    assert (code in _codes(report)) if code else report["findings"] == []


@pytest.mark.parametrize(
    ("caller_permissions", "status"),
    (("", "PASS"), ("    permissions: {id-token: write}\n", "FAIL")),
)
def test_local_reusable_caller_is_only_a_transition(caller_permissions: str, status: str, tmp_path: Path) -> None:
    report = _scan_with_workflows(
        tmp_path,
        local_caller=(
            "name: Local caller\non: pull_request\npermissions: {}\njobs:\n  call:\n"
            f"{caller_permissions}    uses: ./.github/workflows/local-callee.yml\n"
        ),
        local_callee=(
            "name: Local callee\non: workflow_call\npermissions: {}\njobs:\n"
            "  inspect: {runs-on: ubuntu-latest, steps: [{run: echo inspect}]}\n"
        ),
    )

    assert report["status"] == status
    routes = [item for item in report["routes"] if item["workflow"].endswith("local-callee.yml")]
    assert len(routes) == 3
    assert all([edge["kind"] for edge in item["edge_chain"]] == ["LOCAL_WORKFLOW_CALL"] for item in routes)
    assert all(item["job_id"] != "call" for item in report["routes"])
    route_ids = {item["route_id"] for item in routes}
    authorities = [item for item in report["route_authority"] if item["route_id"] in route_ids]
    assert {item["permission_source"] for item in authorities} == {"CALL_INTERSECTION"}
    assert {item["effective_permissions"]["id-token"] for item in authorities} == {"none"}
    if status == "PASS":
        assert report["findings"] == []
    else:
        assert "PRIVILEGE_UNAPPROVED_PR_WRITE" in _codes(report)
