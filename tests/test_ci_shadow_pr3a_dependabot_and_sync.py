from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"


def _yaml(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _sync_steps(path: Path) -> list[tuple[str, str, str]]:
    jobs = _yaml(path)["jobs"]
    assert isinstance(jobs, dict)
    found: list[tuple[str, str, str]] = []
    for job_name, raw_job in jobs.items():
        assert isinstance(raw_job, dict)
        for raw_step in raw_job.get("steps", []):
            assert isinstance(raw_step, dict)
            command = raw_step.get("run")
            if isinstance(command, str) and "uv sync" in command:
                found.append((str(job_name), str(raw_step.get("name", "")), command))
    return found


def test_dependabot_matches_the_exact_approved_update_contract() -> None:
    payload = _yaml(ROOT / ".github/dependabot.yml")

    assert payload == {
        "version": 2,
        "updates": [
            {
                "package-ecosystem": "github-actions",
                "directory": "/",
                "schedule": {
                    "interval": "weekly",
                    "day": "wednesday",
                    "time": "08:00",
                    "timezone": "Europe/Berlin",
                },
                "labels": ["dependencies", "github-actions"],
                "open-pull-requests-limit": 2,
                "groups": {
                    "github-actions-minor-patch": {
                        "patterns": ["*"],
                        "update-types": ["minor", "patch"],
                    }
                },
            },
            {
                "package-ecosystem": "uv",
                "directory": "/",
                "schedule": {
                    "interval": "weekly",
                    "day": "monday",
                    "time": "08:30",
                    "timezone": "Europe/Berlin",
                },
                "labels": ["dependencies", "python:uv"],
                "open-pull-requests-limit": 3,
                "groups": {
                    "uv-minor-patch": {
                        "patterns": ["*"],
                        "update-types": ["minor", "patch"],
                    }
                },
            },
        ],
    }


def test_only_the_declared_ci_project_sync_steps_become_locked() -> None:
    ci_syncs = _sync_steps(WORKFLOWS / "ci.yml")
    pages_syncs = _sync_steps(WORKFLOWS / "pages.yml")

    assert ci_syncs == [
        ("quality-preflight", "Install dependencies", "uv sync --locked --all-extras"),
        ("quality-shards", "Install dependencies", "uv sync --locked --all-extras"),
        ("quality", "Install dependencies", "uv sync --locked --all-extras"),
        ("doctor-import-windows", "Install dependencies", "uv sync --locked --all-extras"),
        ("postgres-xmin", "Install dependencies", "uv sync --locked --all-extras"),
        ("acceptance-plan", "Install dependencies", "uv sync --locked --all-extras"),
        ("acceptance-contracts", "Install dependencies", "uv sync --locked --all-extras"),
        ("bounded-window-smoke", "Install dependencies", "uv sync --locked --all-extras"),
    ]
    assert pages_syncs == [("build", "Install docs dependencies", "uv sync --locked")]
    assert _sync_steps(WORKFLOWS / "airflow-pack-compat.yml") == []


def test_locked_sync_occurrences_are_confined_to_declared_workflow_steps() -> None:
    locked: list[tuple[str, str, str]] = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        for job_name, step_name, command in _sync_steps(path):
            if "--locked" in command:
                locked.append((path.name, job_name, step_name))

    assert locked == [
        ("backfill-integration.yml", "backfill-integration", "Install dependencies"),
        ("ci.yml", "quality-preflight", "Install dependencies"),
        ("ci.yml", "quality-shards", "Install dependencies"),
        ("ci.yml", "quality", "Install dependencies"),
        ("ci.yml", "doctor-import-windows", "Install dependencies"),
        ("ci.yml", "postgres-xmin", "Install dependencies"),
        ("ci.yml", "acceptance-plan", "Install dependencies"),
        ("ci.yml", "acceptance-contracts", "Install dependencies"),
        ("ci.yml", "bounded-window-smoke", "Install dependencies"),
        ("composition-mssql-component.yml", "control-ledger", "Install locked component dependencies"),
        ("live-certification.yml", "local-live-certification", "Install dependencies"),
        ("pages.yml", "build", "Install docs dependencies"),
        ("pr-gate-shadow.yml", "static", "Run static shadow route"),
        ("pr-gate-shadow.yml", "docs", "Run docs shadow route"),
        ("pr-gate-shadow.yml", "contracts", "Run contract shadow route"),
        ("pr-gate-shadow.yml", "python_311", ""),
        ("pr-gate-shadow.yml", "python_312", ""),
        ("pr-gate-shadow.yml", "postgresql", "Run exact PostgreSQL XMin integration route"),
        (
            "release-candidate-evidence.yml",
            "release-candidate-evidence",
            "Install locked evidence dependencies",
        ),
        ("source-release-readiness.yml", "build", "Install locked validation dependencies"),
    ]
