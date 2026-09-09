from __future__ import annotations

from pathlib import Path

import pytest
from tests.agent_policy.test_workflow_security import _policy_payload, workflow_security


def test_workflow_security_enforces_job_scoped_write_exception(tmp_path: Path) -> None:
    policy = workflow_security.WorkflowSecurityPolicy.from_mapping(
        _policy_payload()
        | {
            "allowed_workflow_write_permissions": {
                "receipt.yml": {
                    "jobs": {"merge-closure": ["checks"]},
                    "reason": "Only the immutable merge-closure job may publish the exact check.",
                }
            }
        },
        label="policy.yml",
    )
    workflow = tmp_path / "receipt.yml"
    workflow.write_text(
        """
name: Receipt
on: pull_request
permissions:
  checks: read
jobs:
  reviewed-head:
    permissions:
      checks: write
    runs-on: ubuntu-latest
    steps:
      - run: echo unsafe
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert any("job reviewed-head permission checks: write is not documented" in error for error in result.errors)


def test_workflow_security_rejects_dbt_execution_with_oidc_authority(tmp_path: Path) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe dbt authority
on:
  workflow_call:
permissions:
  contents: read
jobs:
  build:
    permissions:
      contents: read
      id-token: write
      attestations: write
    runs-on: ubuntu-latest
    steps:
      - run: dbt parse --project-dir project
""".lstrip(),
        encoding="utf-8",
    )
    policy = workflow_security.WorkflowSecurityPolicy(
        allowed_write_permissions={"unsafe.yml": frozenset({"id-token", "attestations"})},
        required_audit_artifacts={},
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert any("dbt execution must run with contents: read only" in error for error in result.errors)


def test_workflow_security_rejects_attestation_job_checkout(tmp_path: Path) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe attestation boundary
on:
  workflow_call:
permissions:
  contents: read
jobs:
  build:
    permissions:
      contents: read
    runs-on: ubuntu-latest
    steps:
      - run: dpone dbt compile project
  attest:
    permissions:
      contents: read
      id-token: write
      attestations: write
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0
      - uses: actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26
        with:
          subject-path: release.json
""".lstrip(),
        encoding="utf-8",
    )
    policy = workflow_security.WorkflowSecurityPolicy(
        allowed_write_permissions={"unsafe.yml": frozenset({"id-token", "attestations"})},
        required_audit_artifacts={},
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert any("attestation job must not checkout repository source" in error for error in result.errors)


@pytest.mark.parametrize("status_function", ("failure", "cancelled"))
def test_semantic_boundary_preserves_transitive_status_uncertainty(status_function: str, tmp_path: Path) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy.workflow_privilege_service import scan_repository

    root = copy_repository_fixture(tmp_path, "target")
    (root / ".github/workflows/status-chain.yml").write_text(
        "name: Status chain\non: pull_request\npermissions: {}\njobs:\n"
        "  ancestor: {runs-on: ubuntu-latest, steps: [{run: exit 1}]}\n"
        "  skipped: {needs: ancestor, if: false, runs-on: ubuntu-latest, steps: []}\n"
        f"  publisher:\n    needs: skipped\n    if: {status_function}()\n    runs-on: ubuntu-latest\n"
        "    permissions: {contents: write}\n    steps: []\n",
        encoding="utf-8",
    )

    report = scan_repository(root)
    publisher = [route for route in report["routes"] if route["job_id"] == "publisher"]

    assert report["status"] == "UNVERIFIED"
    assert publisher and {route["classification"] for route in publisher} == {"PR_HEAD"}
    assert "PRIVILEGE_UNKNOWN_EXPRESSION" in {finding["code"] for finding in report["findings"]}
