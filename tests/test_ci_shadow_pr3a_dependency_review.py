from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/dependency-review.yml"
BEFORE_EXPRESSION = "${{ github.event.before }}"
ACTION_PIN = "actions/dependency-review-action@2031cfc080254a8a887f58cffee85186f0e49e48"


def _payload() -> dict[str, object]:
    parsed = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(parsed, dict)
    return parsed


def _events(payload: dict[str, object]) -> dict[str, object]:
    events = payload.get("on", payload.get(True))
    assert isinstance(events, dict)
    return events


def _steps() -> list[dict[str, object]]:
    steps = _payload()["jobs"]["dependency-review"]["steps"]
    assert isinstance(steps, list) and all(isinstance(step, dict) for step in steps)
    return steps


def _normalized(value: object) -> str:
    assert isinstance(value, str)
    return " ".join(value.split())


def _before_guard() -> tuple[int, str, str]:
    matches: list[tuple[int, str, str]] = []
    for index, step in enumerate(_steps()):
        env = step.get("env", {})
        script = step.get("run")
        if not isinstance(env, dict) or not isinstance(script, str):
            continue
        for variable, value in env.items():
            if value == BEFORE_EXPRESSION:
                assert _normalized(step["if"]) == "github.event_name == 'push'"
                matches.append((index, str(variable), script))
    assert len(matches) == 1
    index, variable, script = matches[0]
    assert BEFORE_EXPRESSION not in script
    return index, variable, script


def test_dependency_review_has_only_native_pr_and_master_push_events() -> None:
    payload = _payload()

    assert payload["name"] == "Dependency Review"
    assert _events(payload) == {
        "pull_request": {"branches": ["master"]},
        "push": {"branches": ["master"]},
    }
    assert payload["permissions"] == {"contents": "read"}
    job = payload["jobs"]["dependency-review"]
    assert job["name"] == "Dependency Review"
    assert "permissions" not in job


def test_dependency_review_uses_two_pinned_read_only_native_invocations() -> None:
    steps = _steps()
    reviews = [step for step in steps if step.get("uses") == ACTION_PIN]

    assert len(reviews) == 2
    by_condition = {_normalized(step["if"]): step["with"] for step in reviews}
    assert by_condition == {
        "github.event_name == 'pull_request'": {
            "fail-on-severity": "high",
            "comment-summary-in-pr": "never",
        },
        "github.event_name == 'push'": {
            "fail-on-severity": "high",
            "comment-summary-in-pr": "never",
            "base-ref": "${{ github.event.before }}",
            "head-ref": "${{ github.sha }}",
        },
    }
    guard_index, _, _ = _before_guard()
    push_index = steps.index(next(step for step in reviews if _normalized(step["if"]) == "github.event_name == 'push'"))
    assert guard_index < push_index


def test_dependency_review_push_base_guard_is_executable_and_fail_closed() -> None:
    _, variable, script = _before_guard()
    decisions: dict[str, bool] = {}
    for before in ("a" * 40, "0" * 40, "a" * 39, "g" * 40, ""):
        env = os.environ.copy()
        env[variable] = before
        result = subprocess.run(
            ["bash", "-c", script],
            check=False,
            capture_output=True,
            env=env,
            text=True,
        )
        decisions[before] = result.returncode == 0

    assert decisions == {
        "a" * 40: True,
        "0" * 40: False,
        "a" * 39: False,
        "g" * 40: False,
        "": False,
    }


def test_dependency_review_contains_no_manual_or_synthetic_success_path() -> None:
    payload = _payload()
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch" not in _events(payload)
    assert "pull-requests" not in payload["permissions"]
    assert "checks" not in payload["permissions"]
    assert "gh api" not in text
    assert "check-runs" not in text
    assert "conclusion='success'" not in text
