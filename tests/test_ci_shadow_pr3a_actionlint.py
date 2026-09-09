from __future__ import annotations

import copy
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
CI_WORKFLOW = WORKFLOWS / "ci.yml"

STEP_NAME = "Validate changed workflows with pinned actionlint"
QUEUE_MESSAGE = 'unexpected key "queue" for "concurrency" section. expected one of "cancel-in-progress", "group"'
QUEUE_IGNORE = (
    '-ignore \'^unexpected key "queue" for "concurrency" section\\. expected one of "cancel-in-progress", "group"$\''
)


def _yaml(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _actionlint_steps(payload: dict[str, object]) -> list[dict[str, object]]:
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    preflight = jobs["quality-preflight"]
    assert isinstance(preflight, dict)
    steps = preflight["steps"]
    assert isinstance(steps, list)
    return [step for step in steps if isinstance(step, dict) and step.get("name") == STEP_NAME]


def _contract_errors(step: dict[str, object]) -> list[str]:
    errors: list[str] = []
    script = step.get("run")
    if "if" in step:
        errors.append("unexpected conditional execution")
    if step.get("shell") != "bash":
        errors.append("wrong shell")
    if not isinstance(script, str):
        return [*errors, "missing script"]

    exact_counts = {
        "ACTIONLINT_VERSION=1.7.12": 1,
        "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8": 1,
        "c872d6db8c6bf83a8eaa704fc93999f027d55dffbc63b8a6abdccb47df5f4cd4": 1,
        'test "$(wc -c < "${ACTIONLINT_BIN}.tmp" | tr -d \' \')" = "6074530"': 1,
        'test "$("${ACTIONLINT_BIN}" -version | sed -n \'1p\')" = "${ACTIONLINT_VERSION}"': 1,
        "actionlint_flags=(-no-color -format '{{json .}}' -shellcheck '' -pyflakes '')": 1,
        ".github/workflows/ci.yml": 1,
        ".github/workflows/airflow-pack-compat.yml": 1,
        ".github/workflows/pages.yml": 1,
        ".github/workflows/dependency-review.yml": 1,
        ".github/workflows/airflow-pack-compat-nightly.yml": 3,
        QUEUE_IGNORE: 1,
        "ACTIONLINT_QUEUE_WAIVER_OBSOLETE": 1,
        'test ! -s "${nonqueue_stderr}"': 1,
        'test ! -s "${nightly_stderr}"': 1,
        'test ! -s "${waived_stderr}"': 1,
        'cmp "${expected_empty}" "${nonqueue_stdout}"': 1,
        'cmp "${expected_empty}" "${waived_stdout}"': 1,
        QUEUE_MESSAGE: 1,
    }
    for fragment, expected_count in exact_counts.items():
        actual_count = script.count(fragment)
        if actual_count != expected_count:
            errors.append(f"{fragment!r}: expected {expected_count}, got {actual_count}")

    required_fragments = (
        "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/",
        "curl --fail --silent --show-error --location --proto '=https' --tlsv1.2",
        'mkdir "${ACTIONLINT_DIR}"',
        'LC_ALL=C tar -xOf "${ACTIONLINT_ARCHIVE}" actionlint',
        'chmod 0755 "${ACTIONLINT_BIN}.tmp"',
        'mv "${ACTIONLINT_BIN}.tmp" "${ACTIONLINT_BIN}"',
        'test "${nightly_status}" -eq 1',
        "assert isinstance(diagnostics, list) and len(diagnostics) == 1",
        '"kind": "syntax-check"',
    )
    for fragment in required_fragments:
        if fragment not in script:
            errors.append(f"missing {fragment!r}")

    if "|| true" in script or "latest" in script:
        errors.append("failure or floating-version bypass")
    return errors


def test_ci_has_one_exact_pinned_actionlint_enforcement_step() -> None:
    steps = _actionlint_steps(_yaml(CI_WORKFLOW))

    assert len(steps) == 1
    assert _contract_errors(steps[0]) == []
    result = subprocess.run(
        ["bash", "-n"],
        input=str(steps[0]["run"]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_no_other_workflow_can_define_an_actionlint_enforcement_step() -> None:
    matches: list[tuple[str, str]] = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        payload = _yaml(path)
        jobs = payload.get("jobs", {})
        assert isinstance(jobs, dict)
        for job_name, job in jobs.items():
            if not isinstance(job, dict):
                continue
            for step in job.get("steps", []):
                if isinstance(step, dict) and "actionlint" in str(step.get("run", "")):
                    matches.append((path.name, str(job_name)))

    assert matches == [("ci.yml", "quality-preflight")]


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (QUEUE_IGNORE, "-ignore 'unexpected key'"),
        ("ACTIONLINT_QUEUE_WAIVER_OBSOLETE", "QUEUE_WAIVER_STILL_ALLOWED"),
        ("8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8", "0" * 64),
        ("c872d6db8c6bf83a8eaa704fc93999f027d55dffbc63b8a6abdccb47df5f4cd4", "f" * 64),
        (".github/workflows/dependency-review.yml", ".github/workflows/release.yml"),
        ('test ! -s "${waived_stderr}"', ":"),
    ],
)
def test_actionlint_contract_rejects_weakened_script(old: str, new: str) -> None:
    step = copy.deepcopy(_actionlint_steps(_yaml(CI_WORKFLOW))[0])
    script = str(step["run"])
    assert old in script
    step["run"] = script.replace(old, new, 1)

    assert _contract_errors(step)


def test_actionlint_contract_rejects_weakened_location_or_duplicates() -> None:
    payload = _yaml(CI_WORKFLOW)
    steps = _actionlint_steps(payload)
    assert len(steps) == 1

    weakened = copy.deepcopy(steps[0])
    weakened["if"] = "always()"
    assert _contract_errors(weakened)

    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    preflight = jobs["quality-preflight"]
    assert isinstance(preflight, dict)
    preflight_steps = preflight["steps"]
    assert isinstance(preflight_steps, list)
    preflight_steps.append(copy.deepcopy(steps[0]))
    assert len(_actionlint_steps(payload)) == 2
