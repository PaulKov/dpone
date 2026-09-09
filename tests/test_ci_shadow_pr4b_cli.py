from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from tools.ci.audit_pr_gate_shadow import _live_event

from dpone.services.ci.shadow_bundle import load_bundle_digest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "ci" / "audit_pr_gate_shadow.py"


def test_fixture_audit_writes_create_only_receipt(tmp_path: Path) -> None:
    sha_a, sha_b, sha_c = "a" * 40, "b" * 40, "c" * 40
    record = {
        "number": 7,
        "repository_id": 1,
        "base_repository_id": 1,
        "head_repository_id": 2,
        "base_ref": "master",
        "head_ref": "feature/audit",
        "base_sha": sha_a,
        "head_sha": sha_b,
        "mergeable": True,
        "merge_commit_sha": sha_c,
        "state": "open",
    }
    fixture = {
        "event": {
            "workflow_run": {
                "repository_id": 1,
                "run_id": 10,
                "run_attempt": 1,
                "head_repository_id": 2,
                "head_branch": "feature/audit",
                "head_sha": sha_b,
                "status": "completed",
                "conclusion": "success",
            },
            "claims": {
                "repository_id": 1,
                "pr_number": 7,
                "run_id": 10,
                "run_attempt": 1,
                "base_sha": sha_a,
                "head_sha": sha_b,
                "merge_sha": sha_c,
                "implementation_bundle_digest": load_bundle_digest(ROOT / ".agents/policy/ci-shadow-bundle-v1.yml"),
                "jobs": {
                    name: {"selection": "RUN", "outcome": "PASS"}
                    for name in (
                        "static",
                        "contracts",
                        "docs",
                        "python-3.11",
                        "python-3.12",
                        "packaging",
                        "postgresql",
                        "airflow",
                        "runtime-wheel-smoke",
                    )
                },
                "status": "PASS",
            },
            "auditor": {"workflow_id": 20, "run_id": 30, "run_attempt": 1, "revision": sha_a},
        },
        "open_pull_requests": [record],
        "workflow_runs": [
            {
                "repository_id": 1,
                "run_id": 10,
                "run_attempt": 1,
                "head_repository_id": 2,
                "head_branch": "feature/audit",
                "head_sha": sha_b,
                "status": "completed",
                "conclusion": "success",
                "event": "pull_request",
                "path": ".github/workflows/pr-gate-shadow.yml",
                "workflow_id": 10,
            }
        ],
        "attempt_jobs": {
            "10-1": [
                {"name": name, "status": "completed", "conclusion": "success"}
                for name in (
                    "PR Gate shadow static",
                    "PR Gate shadow contracts",
                    "PR Gate shadow docs",
                    "PR Gate shadow Python 3.11",
                    "PR Gate shadow Python 3.12",
                    "PR Gate shadow packaging",
                    "PR Gate shadow PostgreSQL XMin",
                    "PR Gate shadow runtime wheel smoke",
                    "PR Gate shadow Airflow 2.10.5 / py3.11",
                    "PR Gate shadow Airflow 2.10.5 / py3.12",
                    "PR Gate shadow Airflow 2.11.0 / py3.11",
                    "PR Gate shadow Airflow 2.11.0 / py3.12",
                    "PR Gate shadow Airflow 3.2.0 / py3.11",
                    "PR Gate shadow Airflow 3.2.0 / py3.12",
                    "PR Gate shadow Airflow 3.3.0 / py3.11",
                    "PR Gate shadow Airflow 3.3.0 / py3.12",
                )
            ]
        },
        "pull_requests": [record],
        "merge_refs": {"7": sha_c},
        "commits": {sha_c: [sha_a, sha_b]},
        "git_trees": {
            sha: {
                ".github/workflows/pr-gate-shadow.yml": {
                    "sha": "d" * 40,
                    "mode": "100644",
                    "type": "blob",
                }
            }
            for sha in (sha_a, sha_b, sha_c)
        },
        "git_blobs": {"d" * 40: b"workflow".hex()},
    }
    event = tmp_path / "event.json"
    output = tmp_path / "receipt.json"
    bundle = tmp_path / "bundle.yml"
    bundle.write_text(
        "\n".join(
            (
                "schema_version: dpone.ci-shadow-bundle.v1",
                "files:",
                "  - path: .github/workflows/pr-gate-shadow.yml",
                "    role: EXECUTION_WORKFLOW",
                f"    sha256: {hashlib.sha256(b'workflow').hexdigest()}",
                "",
            )
        ),
        encoding="utf-8",
    )
    fixture["event"]["claims"]["implementation_bundle_digest"] = load_bundle_digest(bundle)
    event.write_text(json.dumps(fixture), encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--event", str(event), "--bundle", str(bundle), "--output", str(output)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(output.read_text(encoding="utf-8"))["decision"] == "PASS"
    second = subprocess.run(
        [sys.executable, str(SCRIPT), "--event", str(event), "--bundle", str(bundle), "--output", str(output)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert second.returncode != 0


def test_live_event_preserves_workflow_run_source_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_RUN_ID", "30")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    payload = {
        "repository": {"id": 1},
        "workflow_run": {
            "id": 10,
            "run_attempt": 1,
            "head_repository": {"id": 2},
            "head_branch": "feature/audit",
            "head_sha": "b" * 40,
            "status": "completed",
            "conclusion": "success",
        },
    }

    event = _live_event(payload, {})

    assert event["workflow_run"]["head_repository_id"] == 2  # type: ignore[index]
    assert event["workflow_run"]["head_branch"] == "feature/audit"  # type: ignore[index]
