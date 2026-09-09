from __future__ import annotations

import os
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md"
MARKER = "job boundary is:"
OLDER_SHA = "a" * 40
NEWER_SHA = "b" * 40


def _pages_jobs() -> dict[str, object]:
    text = SPEC.read_text(encoding="utf-8")
    assert text.count(MARKER) == 1
    tail = text.split(MARKER, maxsplit=1)[1]
    block = tail.split("```yaml\n", maxsplit=1)[1].split("\n```", maxsplit=1)[0]
    parsed = yaml.safe_load(block)
    assert isinstance(parsed, dict)
    return parsed


def _guard_script() -> str:
    verify = _pages_jobs()["verify_current_master"]
    assert isinstance(verify, dict)
    steps = verify["steps"]
    assert isinstance(steps, list)
    script = steps[0]["run"]
    assert isinstance(script, str)
    return script


def _deploy_condition() -> str:
    deploy = _pages_jobs()["deploy"]
    assert isinstance(deploy, dict)
    condition = deploy["if"]
    assert isinstance(condition, str)
    return condition


def _deploy_eligible(
    condition: str,
    *,
    github_sha: str,
    github_run_attempt: int,
    verified_sha: str | None,
    verified_attempt: str | None,
    event_name: str = "push",
    ref: str = "refs/heads/master",
    build_result: str = "success",
    verify_result: str = "success",
) -> bool:
    clauses = [part.strip() for part in " ".join(condition.split()).split(" && ")]
    assert clauses == [
        "github.event_name != 'pull_request'",
        "github.ref == 'refs/heads/master'",
        "github.run_attempt == 1",
        "needs.build.result == 'success'",
        "needs.verify_current_master.result == 'success'",
        "needs.verify_current_master.outputs.verified_sha == github.sha",
        "needs.verify_current_master.outputs.verified_attempt == format('{0}', github.run_attempt)",
    ]
    return (
        event_name != "pull_request"
        and ref == "refs/heads/master"
        and github_run_attempt == 1
        and build_result == "success"
        and verify_result == "success"
        and verified_sha == github_sha
        and verified_attempt == str(github_run_attempt)
    )


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
if mode == "ok":
    print(os.environ["PR3A_MASTER_SHA"])
elif mode == "malformed":
    print("not-a-sha")
elif mode == "multiple":
    print(os.environ["PR3A_MASTER_SHA"])
    print("c" * 40)
else:
    raise SystemExit(70)
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)


def _run_guard(
    tmp_path: Path,
    mode: str,
    run_sha: str = OLDER_SHA,
    master_sha: str = OLDER_SHA,
    run_attempt: int | str = 1,
    script: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    _fake_gh(tmp_path)
    output_path = tmp_path / "github-output"
    output_path.unlink(missing_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "EXPECTED_SHA": run_sha,
            "GH_HOST": "attacker.example",
            "GH_TOKEN": "test-token",
            "GITHUB_OUTPUT": str(output_path),
            "GITHUB_REPOSITORY": "PaulKov/dpone",
            "GITHUB_RUN_ATTEMPT": str(run_attempt),
            "PATH": f"{tmp_path}:{env['PATH']}",
            "PR3A_PAGES_MODE": mode,
            "PR3A_MASTER_SHA": master_sha,
        }
    )
    result = subprocess.run(
        ["bash", "-c", script or _guard_script()],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )
    outputs = {}
    if output_path.exists():
        for line in output_path.read_text(encoding="utf-8").splitlines():
            key, value = line.split("=", maxsplit=1)
            outputs[key] = value
    return result, outputs


def test_pages_jobs_freeze_attempt_bound_deploy_contract() -> None:
    jobs = _pages_jobs()
    verify = jobs["verify_current_master"]
    deploy = jobs["deploy"]

    assert verify["outputs"] == {
        "verified_sha": "${{ steps.verify.outputs.sha }}",
        "verified_attempt": "${{ steps.verify.outputs.attempt }}",
    }
    assert verify["steps"][0]["id"] == "verify"
    assert verify["steps"][0]["run"].count("gh api --hostname github.com") == 1
    assert deploy["needs"] == ["build", "verify_current_master"]
    assert deploy["permissions"] == {"pages": "write", "id-token": "write"}
    assert _deploy_eligible(
        _deploy_condition(),
        github_sha=OLDER_SHA,
        github_run_attempt=1,
        verified_sha=OLDER_SHA,
        verified_attempt="1",
    )


def test_pages_guard_accepts_only_current_master(tmp_path: Path) -> None:
    result, outputs = _run_guard(tmp_path, "ok")

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
    assert outputs == {"sha": OLDER_SHA, "attempt": "1"}


def test_pages_guard_rejects_removing_explicit_github_host(tmp_path: Path) -> None:
    unbound = _guard_script().replace("gh api --hostname github.com", "gh api")
    assert unbound != _guard_script()

    result, outputs = _run_guard(tmp_path, "ok", script=unbound)

    assert result.returncode != 0
    assert outputs == {}


@pytest.mark.parametrize(
    ("mode", "run_sha", "master_sha"),
    [
        ("ok", OLDER_SHA, NEWER_SHA),
        ("malformed", OLDER_SHA, OLDER_SHA),
        ("multiple", OLDER_SHA, OLDER_SHA),
        ("api-fail", OLDER_SHA, OLDER_SHA),
    ],
)
def test_pages_guard_fails_closed(
    tmp_path: Path,
    mode: str,
    run_sha: str,
    master_sha: str,
) -> None:
    result, outputs = _run_guard(tmp_path, mode, run_sha, master_sha)

    assert result.returncode != 0
    assert outputs == {}


@pytest.mark.parametrize("run_attempt", ("", "0", "not-an-integer"))
def test_pages_guard_rejects_invalid_run_attempt(
    tmp_path: Path,
    run_attempt: str,
) -> None:
    result, outputs = _run_guard(tmp_path, "ok", run_attempt=run_attempt)

    assert result.returncode != 0
    assert outputs == {}


def test_pages_guard_rejects_inverted_current_sha_comparison(tmp_path: Path) -> None:
    inverted = _guard_script().replace(
        '[[ "${actual}" != "${EXPECTED_SHA}" ]]',
        '[[ "${actual}" == "${EXPECTED_SHA}" ]]',
    )
    assert inverted != _guard_script()

    result, outputs = _run_guard(tmp_path, "ok", script=inverted)

    assert result.returncode != 0
    assert outputs == {}


@pytest.mark.parametrize(
    (
        "github_run_attempt",
        "verified_sha",
        "verified_attempt",
        "verify_result",
        "eligible",
    ),
    [
        (1, OLDER_SHA, "1", "success", True),
        (2, OLDER_SHA, "1", "success", False),
        (2, None, None, "success", False),
        (2, OLDER_SHA, "2", "success", False),
        (2, NEWER_SHA, "2", "success", False),
        (2, OLDER_SHA, "2", "failure", False),
    ],
)
def test_pages_deploy_requires_current_same_attempt_outputs(
    github_run_attempt: int,
    verified_sha: str | None,
    verified_attempt: str | None,
    verify_result: str,
    eligible: bool,
) -> None:
    assert (
        _deploy_eligible(
            _deploy_condition(),
            github_sha=OLDER_SHA,
            github_run_attempt=github_run_attempt,
            verified_sha=verified_sha,
            verified_attempt=verified_attempt,
            verify_result=verify_result,
        )
        is eligible
    )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: value.replace(
            "needs.verify_current_master.outputs.verified_attempt == format('{0}', github.run_attempt)",
            "true",
        ),
        lambda value: value.replace(
            "needs.verify_current_master.outputs.verified_attempt == format('{0}', github.run_attempt)",
            "needs.verify_current_master.outputs.verified_attempt != format('{0}', github.run_attempt)",
        ),
    ),
)
def test_pages_deploy_rejects_removed_or_inverted_attempt_comparison(
    mutation: Callable[[str], str],
) -> None:
    condition = _deploy_condition()
    mutated = mutation(condition)
    assert mutated != condition

    with pytest.raises(AssertionError):
        _deploy_eligible(
            mutated,
            github_sha=OLDER_SHA,
            github_run_attempt=2,
            verified_sha=OLDER_SHA,
            verified_attempt="1",
        )


def test_reversed_build_completion_never_redeploys_older_sha(tmp_path: Path) -> None:
    deployed: list[str] = []

    newer, newer_outputs = _run_guard(tmp_path, "ok", NEWER_SHA, NEWER_SHA)
    if newer.returncode == 0 and _deploy_eligible(
        _deploy_condition(),
        github_sha=NEWER_SHA,
        github_run_attempt=1,
        verified_sha=newer_outputs.get("sha"),
        verified_attempt=newer_outputs.get("attempt"),
    ):
        deployed.append(NEWER_SHA)
    older, older_outputs = _run_guard(tmp_path, "ok", OLDER_SHA, NEWER_SHA)
    if older.returncode == 0 and _deploy_eligible(
        _deploy_condition(),
        github_sha=OLDER_SHA,
        github_run_attempt=1,
        verified_sha=older_outputs.get("sha"),
        verified_attempt=older_outputs.get("attempt"),
    ):
        deployed.append(OLDER_SHA)

    assert deployed == [NEWER_SHA]


def test_stale_full_rerun_remains_ineligible(tmp_path: Path) -> None:
    first, first_outputs = _run_guard(tmp_path, "ok", OLDER_SHA, NEWER_SHA)
    rerun, rerun_outputs = _run_guard(
        tmp_path,
        "ok",
        OLDER_SHA,
        NEWER_SHA,
        run_attempt=2,
    )

    assert first.returncode != 0
    assert rerun.returncode != 0
    assert first_outputs == {}
    assert rerun_outputs == {}


def test_deploy_only_rerun_after_newer_deployment_remains_blocked() -> None:
    deployed = [NEWER_SHA]

    if _deploy_eligible(
        _deploy_condition(),
        github_sha=OLDER_SHA,
        github_run_attempt=2,
        verified_sha=OLDER_SHA,
        verified_attempt="1",
    ):
        deployed.append(OLDER_SHA)

    assert deployed == [NEWER_SHA]


def test_verify_job_rerun_outputs_do_not_authorize_deploy(tmp_path: Path) -> None:
    result, outputs = _run_guard(tmp_path, "ok", run_attempt=2)

    assert result.returncode == 0
    assert outputs == {"sha": OLDER_SHA, "attempt": "2"}
    assert not _deploy_eligible(
        _deploy_condition(),
        github_sha=OLDER_SHA,
        github_run_attempt=2,
        verified_sha=outputs["sha"],
        verified_attempt=outputs["attempt"],
    )
