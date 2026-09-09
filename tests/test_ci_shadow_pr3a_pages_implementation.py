from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
PAGES = ROOT / ".github/workflows/pages.yml"
CURRENT_SHA = "a" * 40
STALE_SHA = "b" * 40


def _payload() -> dict[str, object]:
    parsed = yaml.safe_load(PAGES.read_text(encoding="utf-8"))
    assert isinstance(parsed, dict)
    return parsed


def _normalized(value: object) -> str:
    assert isinstance(value, str)
    return " ".join(value.split())


def _steps(job: dict[str, object]) -> list[dict[str, object]]:
    steps = job["steps"]
    assert isinstance(steps, list) and all(isinstance(step, dict) for step in steps)
    return steps


def _step_using(job: dict[str, object], action: str) -> dict[str, object]:
    matches = [step for step in _steps(job) if str(step.get("uses", "")).startswith(action + "@")]
    assert len(matches) == 1
    return matches[0]


def _fake_gh(tmp_path: Path) -> None:
    executable = tmp_path / "gh"
    executable.write_text(
        """#!/usr/bin/env python3
import os
import sys

expected = [
    "api",
    "--hostname",
    "github.com",
    "repos/PaulKov/dpone/git/ref/heads/master",
    "--jq",
    ".object.sha",
]
if sys.argv[1:] != expected:
    raise SystemExit(70)
mode = os.environ["PR3A_PAGES_MODE"]
if mode == "api-fail":
    raise SystemExit(1)
if mode == "multiple":
    print(os.environ["PR3A_MASTER_SHA"])
    print("c" * 40)
elif mode == "malformed":
    print("not-a-sha")
else:
    print(os.environ["PR3A_MASTER_SHA"])
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)


def _run_verify(
    tmp_path: Path,
    script: str,
    *,
    mode: str = "ok",
    expected_sha: str = CURRENT_SHA,
    master_sha: str = CURRENT_SHA,
    attempt: str = "1",
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    _fake_gh(tmp_path)
    output = tmp_path / "github-output"
    output.unlink(missing_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "EXPECTED_SHA": expected_sha,
            "GH_HOST": "attacker.example",
            "GH_TOKEN": "test-token",
            "GITHUB_OUTPUT": str(output),
            "GITHUB_REPOSITORY": "PaulKov/dpone",
            "GITHUB_RUN_ATTEMPT": attempt,
            "PATH": f"{tmp_path}:{env['PATH']}",
            "PR3A_MASTER_SHA": master_sha,
            "PR3A_PAGES_MODE": mode,
        }
    )
    result = subprocess.run(
        ["bash", "-c", script],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )
    values: dict[str, str] = {}
    if output.exists():
        for line in output.read_text(encoding="utf-8").splitlines():
            key, value = line.split("=", maxsplit=1)
            values[key] = value
    return result, values


def test_pages_permissions_and_whole_workflow_concurrency_are_exact() -> None:
    payload = _payload()
    jobs = payload["jobs"]

    assert payload["permissions"] == {}
    assert payload["concurrency"] == {
        "group": "pages-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.ref }}",
        "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
    }
    assert jobs["build"]["permissions"] == {"contents": "read"}
    assert jobs["verify_current_master"]["permissions"] == {"contents": "read"}
    assert jobs["deploy"]["permissions"] == {"pages": "write", "id-token": "write"}
    assert not any("queue" in job for job in jobs.values())
    assert "queue" not in payload["concurrency"]


def test_pr3a_workflow_security_policy_is_job_scoped_and_read_only() -> None:
    policy = yaml.safe_load((ROOT / ".agents/policy/workflow-security.yml").read_text(encoding="utf-8"))
    allowed = policy["allowed_workflow_write_permissions"]
    pages_exception = allowed["pages.yml"]
    dependency_review = yaml.safe_load((ROOT / ".github/workflows/dependency-review.yml").read_text(encoding="utf-8"))

    assert pages_exception["jobs"] == {"deploy": ["id-token", "pages"]}
    assert "permissions" not in pages_exception
    assert "dependency-review.yml" not in allowed
    assert dependency_review["permissions"] == {"contents": "read"}


def test_pages_build_is_locked_and_non_pr_upload_is_attempt_one_only() -> None:
    build = _payload()["jobs"]["build"]
    upload = _step_using(build, "actions/upload-pages-artifact")
    sync = [step for step in _steps(build) if step.get("name") == "Install docs dependencies"]

    assert build["if"] == "github.event_name == 'pull_request' || github.run_attempt == 1"
    assert sync == [{"name": "Install docs dependencies", "run": "uv sync --locked"}]
    assert upload["if"] == "github.event_name != 'pull_request'"
    assert upload["with"] == {"path": "site"}


def test_pages_current_master_verification_has_read_only_attempt_bound_outputs() -> None:
    verify = _payload()["jobs"]["verify_current_master"]

    assert verify["needs"] == "build"
    assert _normalized(verify["if"]) == (
        "github.event_name != 'pull_request' && github.ref == 'refs/heads/master' && github.run_attempt == 1"
    )
    assert verify["outputs"] == {
        "verified_sha": "${{ steps.verify.outputs.sha }}",
        "verified_attempt": "${{ steps.verify.outputs.attempt }}",
    }
    assert verify["runs-on"] == "ubuntu-latest"
    steps = _steps(verify)
    assert len(steps) == 1
    assert steps[0]["id"] == "verify"
    assert steps[0]["env"] == {
        "GH_TOKEN": "${{ github.token }}",
        "EXPECTED_SHA": "${{ github.sha }}",
    }


def test_pages_current_master_shell_guard_fails_closed(tmp_path: Path) -> None:
    verify = _payload()["jobs"]["verify_current_master"]
    script = _steps(verify)[0]["run"]
    accepted, outputs = _run_verify(tmp_path, script)

    assert accepted.returncode == 0
    assert accepted.stdout == ""
    assert accepted.stderr == ""
    assert outputs == {"sha": CURRENT_SHA, "attempt": "1"}

    cases = (
        {"master_sha": STALE_SHA},
        {"mode": "malformed"},
        {"mode": "multiple"},
        {"mode": "api-fail"},
        {"attempt": ""},
        {"attempt": "0"},
        {"attempt": "not-an-integer"},
    )
    for case in cases:
        rejected, rejected_outputs = _run_verify(tmp_path, script, **case)
        assert rejected.returncode != 0
        assert rejected_outputs == {}


def test_pages_current_master_guard_rejects_ambient_host_fallback(tmp_path: Path) -> None:
    verify = _payload()["jobs"]["verify_current_master"]
    script = _steps(verify)[0]["run"]
    assert script.count("gh api --hostname github.com") == 1

    unbound = script.replace("gh api --hostname github.com", "gh api")
    rejected, rejected_outputs = _run_verify(tmp_path, unbound)

    assert rejected.returncode != 0
    assert rejected_outputs == {}


def test_pages_deploy_requires_successful_current_same_attempt_verification() -> None:
    deploy = _payload()["jobs"]["deploy"]
    condition = _normalized(deploy["if"])

    assert deploy["needs"] == ["build", "verify_current_master"]
    assert condition.split(" && ") == [
        "github.event_name != 'pull_request'",
        "github.ref == 'refs/heads/master'",
        "github.run_attempt == 1",
        "needs.build.result == 'success'",
        "needs.verify_current_master.result == 'success'",
        "needs.verify_current_master.outputs.verified_sha == github.sha",
        "needs.verify_current_master.outputs.verified_attempt == format('{0}', github.run_attempt)",
    ]
    assert deploy["environment"] == {
        "name": "github-pages",
        "url": "${{ steps.deployment.outputs.page_url }}",
    }


def test_pages_deploy_actions_are_unique_ordered_and_pinned() -> None:
    deploy = _payload()["jobs"]["deploy"]
    steps = _steps(deploy)

    assert steps == [
        {
            "name": "Configure Pages",
            "uses": "actions/configure-pages@45bfe0192ca1faeb007ade9deae92b16b8254a0d",
        },
        {
            "name": "Deploy Pages",
            "id": "deployment",
            "uses": "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128",
        },
    ]
