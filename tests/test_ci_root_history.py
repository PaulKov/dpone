"""Exercise the real CI comparison script without certifying a fake ratchet."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


@pytest.fixture
def repository(tmp_path: Path) -> tuple[Path, str]:
    _git(tmp_path, "init", "-q", "-b", "master")
    _git(tmp_path, "config", "user.name", "Example Maintainer")
    _git(tmp_path, "config", "user.email", "maintainer@example.com")
    _git(tmp_path, "commit", "-q", "--allow-empty", "-m", "Clean root")
    root_sha = _git(tmp_path, "rev-parse", "HEAD")
    _git(tmp_path, "update-ref", "refs/remotes/origin/master", root_sha)
    return tmp_path, root_sha


def _run(root: Path, event: str, base: str, head: str) -> subprocess.CompletedProcess[str]:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["quality-preflight"]["steps"]
    step = next(step for step in steps if step.get("name") == "Module size")
    # Only substitute the eventual CLI invocation; comparison uses real Git.
    script = 'uv() { printf "RATCHET_CALLED %s\\n" "$*"; };\n' + step["run"]
    return subprocess.run(
        ["bash", "-c", script],
        cwd=root,
        env={**os.environ, "EVENT_NAME": event, "EVENT_BASE_SHA": base, "HEAD_SHA": head, "DEFAULT_BRANCH": "master"},
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_root_cannot_claim_a_comparison_pass(repository: tuple[Path, str], event: str) -> None:
    root, head = repository
    result = _run(root, event, "0" * 40, head)
    assert result.returncode == 2
    assert "ROOT_COMPARISON_UNAVAILABLE" in result.stderr
    assert "RATCHET_CALLED" not in result.stdout


@pytest.mark.parametrize("event", ["push", "pull_request", "workflow_dispatch"])
def test_first_successor_uses_real_distinct_base(repository: tuple[Path, str], event: str) -> None:
    root, base = repository
    _git(root, "commit", "-q", "--allow-empty", "-m", "Reviewed successor")
    head = _git(root, "rev-parse", "HEAD")
    if event == "workflow_dispatch":
        _git(root, "update-ref", "refs/remotes/origin/master", head)
    result = _run(root, event, base, head)
    assert result.returncode == 0, result.stderr
    assert f"--base-ref {base} --head-ref {head}" in result.stdout


def test_missing_event_base_is_not_implicitly_certified(repository: tuple[Path, str]) -> None:
    root, head = repository
    result = _run(root, "push", "", head)
    assert result.returncode == 2
    assert "ROOT_COMPARISON_UNAVAILABLE" in result.stderr
    assert "RATCHET_CALLED" not in result.stdout
