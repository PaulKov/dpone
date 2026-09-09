from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "ci" / "change_plan.py"


def _write(path: Path, value: object) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_github_event_mode_does_not_require_fixture_event(tmp_path: Path) -> None:
    event = _write(
        tmp_path / "event.json",
        {"number": 1, "repository": {"id": 1}, "pull_request": {"base": {"sha": "a" * 40}, "head": {"sha": "b" * 40}}},
    )
    policy = _write(
        tmp_path / "policy.json", {"schema_version": "dpone.ci-shadow-route-policy.v1", "default_route": "full"}
    )
    output = tmp_path / "plan.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--github-event",
            str(event),
            "--merge-sha",
            "c" * 40,
            "--policy",
            str(policy),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(output.read_text(encoding="utf-8"))["merge_sha"] == "c" * 40


def test_github_event_mode_requires_workflow_bound_merge_sha(tmp_path: Path) -> None:
    event = _write(tmp_path / "event.json", {})
    policy = _write(
        tmp_path / "policy.json", {"schema_version": "dpone.ci-shadow-route-policy.v1", "default_route": "full"}
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--github-event",
            str(event),
            "--policy",
            str(policy),
            "--output",
            str(tmp_path / "plan.json"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 2
    assert "--merge-sha is required" in completed.stderr
