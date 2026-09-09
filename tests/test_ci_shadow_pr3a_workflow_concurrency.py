from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
AIRFLOW = WORKFLOWS / "airflow-pack-compat.yml"
NIGHTLY = WORKFLOWS / "airflow-pack-compat-nightly.yml"
RAW_PARALLELISM = "${{ inputs.airflow_max_parallel }}"
AIRFLOW_MATRIX_SHA256 = "742f991a855366f81cf10bd5e85ff5916ac939371ef6457a60ce91d217389b86"
RUNTIME_MATRIX_SHA256 = "20d8cd1e61082e468a1ce49389361df78bbefb8d1ca7b95258eb71948bebba64"


def _yaml(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _events(payload: dict[str, object]) -> dict[str, object]:
    events = payload.get("on", payload.get(True))
    assert isinstance(events, dict)
    return events


def _canonical_sha256(value: object) -> str:
    canonical = json.dumps(value, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _raw_parallelism_guard(payload: dict[str, object]) -> tuple[str, str]:
    build = payload["jobs"]["build-airflow-distributions"]
    matches: list[tuple[str, str]] = []
    for step in build["steps"]:
        env = step.get("env", {})
        script = step.get("run")
        if not isinstance(env, dict) or not isinstance(script, str):
            continue
        for variable, value in env.items():
            if value == RAW_PARALLELISM:
                assert "github.event_name" not in json.dumps(step)
                matches.append((str(variable), script))
    assert len(matches) == 1
    variable, script = matches[0]
    assert RAW_PARALLELISM not in script
    return variable, script


def test_ci_and_airflow_direct_runs_cancel_only_the_same_pull_request() -> None:
    expected = {
        "ci.yml": {
            "group": "ci-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id }}",
            "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
        },
        "airflow-pack-compat.yml": {
            "group": "airflow-pack-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id }}",
            "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
        },
    }

    assert {name: _yaml(WORKFLOWS / name)["concurrency"] for name in expected} == expected


def test_required_airflow_checks_run_on_every_master_commit() -> None:
    events = _events(_yaml(AIRFLOW))

    assert events["push"] == {"branches": ["master"]}
    assert events["pull_request"] == {"branches": ["master"]}


def test_airflow_reusable_input_and_matrix_ceilings_are_exact() -> None:
    payload = _yaml(AIRFLOW)
    inputs = _events(payload)["workflow_call"]["inputs"]
    jobs = payload["jobs"]
    airflow_strategy = jobs["airflow-compat"]["strategy"]
    runtime_strategy = jobs["runtime-wheel-smoke"]["strategy"]
    airflow_input = inputs["airflow_max_parallel"]

    assert set(inputs) == {"airflow_max_parallel"}
    assert airflow_input["required"] is False
    assert airflow_input["type"] == "number"
    assert airflow_input["default"] == 2
    assert airflow_strategy["fail-fast"] is False
    assert airflow_strategy["max-parallel"] == "${{ inputs.airflow_max_parallel || 2 }}"
    assert _canonical_sha256(airflow_strategy["matrix"]) == AIRFLOW_MATRIX_SHA256
    assert runtime_strategy["fail-fast"] is False
    assert runtime_strategy["max-parallel"] == 1
    assert _canonical_sha256(runtime_strategy["matrix"]) == RUNTIME_MATRIX_SHA256


def test_airflow_raw_parallelism_guard_accepts_only_empty_two_or_four() -> None:
    variable, script = _raw_parallelism_guard(_yaml(AIRFLOW))
    decisions: dict[str, bool] = {}
    for raw in ("", "0", "1", "2", "4", "5"):
        env = os.environ.copy()
        env[variable] = raw
        result = subprocess.run(
            ["bash", "-c", script],
            check=False,
            capture_output=True,
            env=env,
            text=True,
        )
        decisions[raw] = result.returncode == 0

    assert decisions == {"": True, "0": False, "1": False, "2": True, "4": True, "5": False}


def test_nightly_wrapper_is_source_free_and_queues_without_cancellation() -> None:
    payload = _yaml(NIGHTLY)
    events = _events(payload)

    assert events == {
        "schedule": [{"cron": "23 1 * * *", "timezone": "Europe/Berlin"}],
        "workflow_dispatch": None,
    }
    assert payload["permissions"] == {"contents": "read"}
    assert payload["concurrency"] == {
        "group": "airflow-pack-compat-nightly",
        "queue": "max",
        "cancel-in-progress": False,
    }
    assert set(payload) == {"name", True, "permissions", "concurrency", "jobs"}
    jobs = payload["jobs"]
    assert isinstance(jobs, dict) and len(jobs) == 1
    job = next(iter(jobs.values()))
    assert set(job) <= {"name", "uses", "with"}
    assert job["uses"] == "./.github/workflows/airflow-pack-compat.yml"
    assert job["with"] == {"airflow_max_parallel": 4}


def test_nightly_is_the_only_local_caller_that_overrides_airflow_parallelism() -> None:
    callers: list[tuple[str, object]] = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        for job in _yaml(path).get("jobs", {}).values():
            if job.get("uses") == "./.github/workflows/airflow-pack-compat.yml":
                callers.append((path.name, job.get("with", {}).get("airflow_max_parallel")))

    assert callers == [("airflow-pack-compat-nightly.yml", 4)]
