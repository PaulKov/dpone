from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pr_receipt = _load("dpone_agent_pr_receipt_evidence_chain_test", "tools/agent_policy/pr_receipt.py")
governance_artifact_content = _load(
    "dpone_agent_governance_artifact_content_evidence_chain_test",
    "tools/agent_policy/governance_artifact_content.py",
)


COMPLETE_BODY = """
## Summary

- Problem: agent governance controls changed.

## Design and scope

- Approved specification or issue: #275
- In scope: PR receipt traceability evidence.
- Non-goals: Runtime ETL behavior.

## Validation evidence

| Check | Status | Command/workflow | Artifact/notes |
|---|---|---|---|
| Focused tests | PASS | `uv run pytest tests/agent_policy/test_pr_receipt.py -q` | test output |
| Agent governance gate | PASS | `uv run python tools/agent_policy/governance_gate.py --base-ref origin/master` | agent_governance_gate.json |

## Owner attestation

- [x] Owner reviewed the final diff and accepts the change.
- [x] Required GitHub checks are green on the reviewed head commit.
- [x] Admin bypass was not used.
- [x] Agent governance receipt is attached when agent controls changed.

Evidence: agent_governance_gate.json
""".strip()


def _governance_artifact_archive(changed_paths: list[str]) -> bytes:
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "control_surface_changed": True,
        "changed_paths": changed_paths,
        "head_commit": "merge123",
        "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
    }
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zip_file:
        zip_file.writestr("agent_governance_gate.json", json.dumps(payload))
    return archive.getvalue()


def test_pr_receipt_fails_when_governance_artifact_digest_is_missing() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    expired=False,
                    url="https://github.example/artifact",
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("SHA-256 digest" in error for error in result.errors)


def test_pr_receipt_fails_when_downloaded_archive_digest_differs_from_metadata() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    digest="sha256:" + "a" * 64,
                    size_in_bytes=200,
                    archive_sha256="sha256:" + "b" * 64,
                    archive_size_bytes=200,
                    expired=False,
                    url="https://github.example/artifact",
                    content=governance_artifact_content.content_from_payload(
                        {
                            "schema_version": 1,
                            "status": "PASS",
                            "control_surface_changed": True,
                            "changed_paths": ["tools/agent_policy/governance_gate.py"],
                            "head_commit": "merge123",
                            "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
                        }
                    ),
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("does not match downloaded archive SHA-256" in error for error in result.errors)


def test_pr_receipt_fails_when_downloaded_archive_size_differs_from_metadata() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    digest="sha256:" + "a" * 64,
                    size_in_bytes=200,
                    archive_sha256="sha256:" + "a" * 64,
                    archive_size_bytes=201,
                    expired=False,
                    url="https://github.example/artifact",
                    content=governance_artifact_content.content_from_payload(
                        {
                            "schema_version": 1,
                            "status": "PASS",
                            "control_surface_changed": True,
                            "changed_paths": ["tools/agent_policy/governance_gate.py"],
                            "head_commit": "merge123",
                            "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
                        }
                    ),
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("size does not match downloaded archive size" in error for error in result.errors)


def test_fetch_github_evidence_collects_ruleset_checks_and_head_artifact(monkeypatch) -> None:
    archive_bytes = _governance_artifact_archive(["tools/agent_policy/governance_gate.py"])
    archive_digest = "sha256:" + hashlib.sha256(archive_bytes).hexdigest()
    archive_size = len(archive_bytes)

    def fake_github_json(path: str, *, token: str) -> dict[str, Any]:
        assert token == "token"
        assert path == "repos/PaulKov/dpone/rulesets/18806829"
        return {
            "rules": [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "required_status_checks": [
                            {"context": "Agent PR receipt"},
                            {"context": "Quality checks (3.11)"},
                        ]
                    },
                }
            ]
        }

    def fake_paginated_items(path: str, *, token: str, array_key: str | None = None) -> list[dict[str, Any]]:
        assert token == "token"
        if path == "repos/PaulKov/dpone/commits/abc1234/check-runs":
            assert array_key == "check_runs"
            return [
                {
                    "id": 123,
                    "name": "Quality checks (3.11)",
                    "status": "completed",
                    "conclusion": "success",
                    "html_url": "https://github.example/PaulKov/dpone/actions/runs/77/job/88",
                    "completed_at": "2026-07-12T00:00:00Z",
                }
            ]
        if path == "repos/PaulKov/dpone/commits/abc1234/statuses":
            assert array_key is None
            return []
        if path == "repos/PaulKov/dpone/actions/artifacts?name=agent-governance-gate&per_page=100":
            assert array_key == "artifacts"
            return [
                {
                    "name": "agent-governance-gate",
                    "id": 42,
                    "expired": False,
                    "archive_download_url": "https://github.example/artifact.zip",
                    "digest": archive_digest,
                    "size_in_bytes": archive_size,
                    "created_at": "2026-07-13T00:00:00Z",
                    "expires_at": "2026-10-11T00:00:00Z",
                    "workflow_run": {"id": 77, "head_sha": "abc1234"},
                },
                {
                    "name": "agent-governance-gate",
                    "id": 41,
                    "expired": False,
                    "archive_download_url": "https://github.example/old.zip",
                    "digest": "sha256:" + "b" * 64,
                    "workflow_run": {"id": 41, "head_sha": "old"},
                },
            ]
        raise AssertionError(path)

    monkeypatch.setattr(pr_receipt.github_receipt, "_github_json", fake_github_json)
    monkeypatch.setattr(pr_receipt.github_receipt, "_github_paginated_items", fake_paginated_items)
    monkeypatch.setattr(
        pr_receipt.github_receipt,
        "_github_bytes",
        lambda path, *, token: archive_bytes,
    )

    evidence = pr_receipt.fetch_github_evidence(
        repo="PaulKov/dpone",
        head_sha="abc1234",
        token="token",
        ruleset_id=18806829,
    )

    assert evidence.required_checks == ["Agent PR receipt", "Quality checks (3.11)"]
    assert evidence.check_runs[0].successful is True
    assert evidence.check_runs[0].id == 123
    assert evidence.check_runs[0].workflow_run_id == 77
    assert [artifact.workflow_run_id for artifact in evidence.artifacts] == [77]
    assert [artifact.artifact_id for artifact in evidence.artifacts] == [42]
    assert [artifact.digest for artifact in evidence.artifacts] == [archive_digest]
    assert [artifact.archive_sha256 for artifact in evidence.artifacts] == [archive_digest]
    assert [artifact.size_in_bytes for artifact in evidence.artifacts] == [archive_size]
    assert [artifact.archive_size_bytes for artifact in evidence.artifacts] == [archive_size]
    assert evidence.artifacts[0].content is not None
    assert evidence.artifacts[0].content.status == "PASS"
    assert evidence.artifacts[0].content.changed_paths == ["tools/agent_policy/governance_gate.py"]
