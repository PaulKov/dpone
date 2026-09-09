from __future__ import annotations

from pathlib import Path

import pytest
from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
from tools.agent_policy.workflow_privilege_service import scan_repository


@pytest.mark.parametrize(
    ("runner", "authority", "expected_status", "expected_code"),
    [
        ("windows-latest", "permissions: {contents: read}", "PASS", None),
        ("windows-latest", "permissions: {contents: write}", "FAIL", "PRIVILEGE_UNAPPROVED_PR_WRITE"),
        (
            "windows-latest",
            "permissions: {contents: read}\n    environment: production",
            "FAIL",
            "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT",
        ),
        ("windows-2025", "permissions: {contents: read}", "FAIL", "PRIVILEGE_PR_SELF_HOSTED"),
        ("self-hosted", "permissions: {contents: read}", "FAIL", "PRIVILEGE_PR_SELF_HOSTED"),
    ],
)
def test_exact_windows_hosted_label_is_limited_to_read_only_credential_free_pr_jobs(
    runner: str,
    authority: str,
    expected_status: str,
    expected_code: str | None,
    tmp_path: Path,
) -> None:
    root = copy_repository_fixture(tmp_path, "target")
    workflow = root / ".github/workflows/windows.yml"
    workflow.write_text(
        "name: Windows import contract\n"
        "on: pull_request\n"
        "permissions: {}\n"
        "jobs:\n"
        "  doctor-import:\n"
        f"    runs-on: {runner}\n"
        f"    {authority}\n"
        "    steps: [{run: echo inspect}]\n",
        encoding="utf-8",
    )

    report = scan_repository(root)
    codes = {finding["code"] for finding in report["findings"]}

    assert report["status"] == expected_status
    if expected_code is None:
        assert not codes
    else:
        assert expected_code in codes
