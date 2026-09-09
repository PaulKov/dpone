from __future__ import annotations

import hashlib
import importlib.util
import inspect
import io
import json
import sys
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SIGNER_WORKFLOW = "PaulKov/dpone/.github/workflows/ci.yml"
REVIEWED_HEAD = "a" * 40
SYNTHETIC_MERGE = "b" * 40


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pr_receipt = _load("dpone_agent_pr_receipt_attestation_evidence_test", "tools/agent_policy/pr_receipt.py")
governance_artifact_content = _load(
    "dpone_agent_governance_artifact_content_attestation_evidence_test",
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
        "head_commit": REVIEWED_HEAD,
        "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
    }
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zip_file:
        zip_file.writestr("agent_governance_gate.json", json.dumps(payload))
    return archive.getvalue()


def _verified_attestation() -> object:
    return pr_receipt.GitHubArtifactAttestationEvidence(
        status="PASS",
        predicate_type="https://slsa.dev/provenance/v1",
        subject_sha256="c" * 64,
        source_repository="PaulKov/dpone",
        source_ref="refs/pull/320/merge",
        source_digest=SYNTHETIC_MERGE,
        signer_workflow=SIGNER_WORKFLOW,
        issuer="https://token.actions.githubusercontent.com",
        verified_timestamps_count=1,
        runner_environment="github-hosted",
        errors=[],
    )


def test_pr_receipt_payload_includes_compact_attestation_evidence_chain() -> None:
    assert hasattr(pr_receipt, "GitHubArtifactAttestationEvidence")

    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha=REVIEWED_HEAD,
            required_checks=["Agent PR receipt", "Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/PaulKov/dpone/actions/runs/77/job/88",
                    workflow_run_id=77,
                    completed_at="2026-07-13T00:00:00Z",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha=REVIEWED_HEAD,
                    workflow_run_id=77,
                    digest="sha256:" + "a" * 64,
                    size_in_bytes=200,
                    archive_sha256="sha256:" + "a" * 64,
                    archive_size_bytes=200,
                    expired=False,
                    url="https://github.example/artifact",
                    content=governance_artifact_content.content_from_payload(
                        {
                            "schema_version": 1,
                            "status": "PASS",
                            "control_surface_changed": True,
                            "changed_paths": ["tools/agent_policy/governance_gate.py"],
                            "head_commit": REVIEWED_HEAD,
                            "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
                        },
                        subject_sha256="c" * 64,
                    ),
                    attestation=_verified_attestation(),
                )
            ],
        ),
        require_github_evidence=True,
        repository="PaulKov/dpone",
        pull_request_number=320,
    )

    payload = pr_receipt.result_payload(result)

    assert payload["evidence_chain"]["governance_artifact"]["attestation"] == {
        "status": "PASS",
        "predicate_type": "https://slsa.dev/provenance/v1",
        "subject_sha256": "c" * 64,
        "source_repository": "PaulKov/dpone",
        "source_ref": "refs/pull/320/merge",
        "source_digest": SYNTHETIC_MERGE,
        "signer_workflow": SIGNER_WORKFLOW,
        "issuer": "https://token.actions.githubusercontent.com",
        "verified_timestamps_count": 1,
        "runner_environment": "github-hosted",
        "errors": [],
    }
    Draft202012Validator(json.loads((ROOT / "evals/agent/pr-receipt.schema.json").read_text())).validate(payload)


def test_fetch_github_evidence_verifies_governance_artifact_attestation_when_required(monkeypatch) -> None:
    assert "require_attestation" in inspect.signature(pr_receipt.fetch_github_evidence).parameters
    archive_bytes = _governance_artifact_archive(["tools/agent_policy/governance_gate.py"])
    archive_digest = "sha256:" + hashlib.sha256(archive_bytes).hexdigest()
    archive_size = len(archive_bytes)
    verified_subjects: list[Path] = []

    def fake_github_json(path: str, *, token: str) -> dict[str, Any]:
        assert path == "repos/PaulKov/dpone/rulesets/18806829"
        return {
            "rules": [
                {
                    "type": "required_status_checks",
                    "parameters": {"required_status_checks": [{"context": "Quality checks (3.12)"}]},
                }
            ]
        }

    def fake_paginated_items(path: str, *, token: str, array_key: str | None = None) -> list[dict[str, Any]]:
        if path == f"repos/PaulKov/dpone/commits/{REVIEWED_HEAD}/check-runs":
            return [{"name": "Quality checks (3.12)", "status": "completed", "conclusion": "success"}]
        if path == f"repos/PaulKov/dpone/commits/{REVIEWED_HEAD}/statuses":
            return []
        if path == "repos/PaulKov/dpone/actions/artifacts?name=agent-governance-gate&per_page=100":
            return [
                {
                    "name": "agent-governance-gate",
                    "id": 42,
                    "expired": False,
                    "archive_download_url": "https://github.example/artifact.zip",
                    "digest": archive_digest,
                    "size_in_bytes": archive_size,
                    "workflow_run": {"id": 77, "head_sha": REVIEWED_HEAD},
                }
            ]
        raise AssertionError(path)

    def fake_verify(
        subject_path: Path,
        *,
        repo: str,
        token: str,
        signer_workflow: str,
    ) -> object:
        assert repo == "PaulKov/dpone"
        assert token == "token"
        assert signer_workflow == SIGNER_WORKFLOW
        assert json.loads(subject_path.read_text(encoding="utf-8"))["status"] == "PASS"
        verified_subjects.append(subject_path)
        return _verified_attestation()

    monkeypatch.setattr(pr_receipt.github_receipt, "_github_json", fake_github_json)
    monkeypatch.setattr(pr_receipt.github_receipt, "_github_paginated_items", fake_paginated_items)
    monkeypatch.setattr(pr_receipt.github_receipt, "_github_bytes", lambda path, *, token: archive_bytes)
    monkeypatch.setattr(
        pr_receipt.github_receipt.github_attestation,
        "verify_governance_subject",
        fake_verify,
    )

    evidence = pr_receipt.fetch_github_evidence(
        repo="PaulKov/dpone",
        head_sha=REVIEWED_HEAD,
        token="token",
        ruleset_id=18806829,
        require_attestation=True,
        signer_workflow=SIGNER_WORKFLOW,
    )

    assert len(verified_subjects) == 1
    assert evidence.artifacts[0].attestation.status == "PASS"
