"""Fixtures for checkout-free release authority consumer tests."""

from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
from typing import Any

gate = importlib.import_module("tools.agent_policy.release_commit_artifact_gate")

REPOSITORY = "PaulKov/dpone"
COMMIT_SHA = "a" * 40
RULESET_ID = 18806829
APP_ID = 15368
CONTEXTS = ("Agent PR receipt", "Quality checks (3.12)")
POLICY = b"schema_version: 1\nruleset:\n  id: 18806829\n"
RULESET_PROJECTION = {
    "id": RULESET_ID,
    "target": "branch",
    "enforcement": "active",
    "name": "protect-master",
    "conditions": {"include": ["refs/heads/master"], "exclude": []},
    "required_status_checks": {
        "strict": True,
        "checks": [{"context": name, "integration_id": APP_ID} for name in sorted(CONTEXTS)],
    },
    "rules": [
        {
            "parameters": {
                "required_status_checks": [{"context": name, "integration_id": APP_ID} for name in sorted(CONTEXTS)],
                "strict_required_status_checks_policy": True,
            },
            "type": "required_status_checks",
        }
    ],
    "updated_at": "2026-07-19T18:37:47.596000Z",
}
PRIVILEGED_BASELINE = {
    "bypass_actors": [],
    "updated_at": RULESET_PROJECTION["updated_at"],
    "version_id": 7,
}


def projection_digest(
    projection: dict[str, Any] = RULESET_PROJECTION,
    baseline: dict[str, Any] = PRIVILEGED_BASELINE,
) -> str:
    full = {
        **projection,
        "bypass_actors": baseline["bypass_actors"],
        "version_id": baseline["version_id"],
    }
    return hashlib.sha256(json.dumps(full, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()


def context(name: str, *, app_id: int = APP_ID) -> dict[str, Any]:
    return {
        "blocker_codes": [],
        "context": name,
        "current_observations": [],
        "integration_id": app_id,
        "observed_count": 1,
        "status": "PASS",
    }


def receipt() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "PASS",
        "decision": "GO",
        "repository": REPOSITORY,
        "commit_sha": COMMIT_SHA,
        "ruleset_id": RULESET_ID,
        "attempts": 1,
        "contexts": [context(name) for name in CONTEXTS],
        "blockers": [],
        "policy_sha256": hashlib.sha256(POLICY).hexdigest(),
        "policy_path": ".agents/policy/github-branch-protection.yml",
    }


def baseline() -> dict[str, Any]:
    return {
        "schema": "dpone.release_authority_baseline.v1",
        "policy_path": ".agents/policy/github-branch-protection.yml",
        "policy_sha256": hashlib.sha256(POLICY).hexdigest(),
        "ruleset_id": RULESET_ID,
        "required_check_producers": [{"context": name, "integration_id": APP_ID} for name in sorted(CONTEXTS)],
        "ruleset_projection": RULESET_PROJECTION,
        "privileged_baseline": PRIVILEGED_BASELINE,
        "policy_projection_sha256": projection_digest(),
    }


def closed_files(tmp_path: Path, monkeypatch: Any, payload: dict[str, Any] | None = None) -> Path:
    fake_module = tmp_path / "release_commit_artifact_gate.py"
    fake_module.write_text("# identity only\n", encoding="utf-8")
    (tmp_path / gate.POLICY_FILENAME).write_bytes(POLICY)
    (tmp_path / gate.BASELINE_FILENAME).write_text(json.dumps(baseline()), encoding="utf-8")
    report = tmp_path / "exact_commit_checks.json"
    report.write_text(json.dumps(payload or receipt()), encoding="utf-8")
    report_digest = hashlib.sha256(report.read_bytes()).hexdigest()
    (tmp_path / "exact_ruleset_projection.json").write_text(
        json.dumps(
            {
                "schema": "dpone.release_ruleset_snapshot.v1",
                "status": "PASS",
                "decision": "GO",
                "repository": REPOSITORY,
                "commit_sha": COMMIT_SHA,
                "ruleset_id": RULESET_ID,
                "policy_sha256": hashlib.sha256(POLICY).hexdigest(),
                "policy_projection_sha256": projection_digest(),
                "privileged_baseline": PRIVILEGED_BASELINE,
                "required_check_report_sha256": report_digest,
                "projection": RULESET_PROJECTION,
                "blockers": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(gate, "__file__", str(fake_module))
    return report


__all__ = [
    "APP_ID",
    "COMMIT_SHA",
    "CONTEXTS",
    "POLICY",
    "PRIVILEGED_BASELINE",
    "REPOSITORY",
    "RULESET_ID",
    "RULESET_PROJECTION",
    "closed_files",
    "context",
    "gate",
    "projection_digest",
    "receipt",
]
