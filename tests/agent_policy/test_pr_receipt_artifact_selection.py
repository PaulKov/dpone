from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pr_receipt = _load("dpone_agent_pr_receipt_artifact_selection", "tools/agent_policy/pr_receipt.py")
governance_content = _load(
    "dpone_agent_governance_artifact_content_selection",
    "tools/agent_policy/governance_artifact_content.py",
)
COMPLETE_BODY = """
## Summary

- Problem: agent governance controls changed.

## Design and scope

- Approved specification or issue: #275
- In scope: exact validated artifact selection.
- Non-goals: Runtime ETL behavior.

## Validation evidence

| Check | Status | Command/workflow | Artifact/notes |
| --- | --- | --- | --- |
| Focused tests | PASS | `pytest` | test output |

## Owner attestation

- [x] Owner reviewed the final diff and accepts the change.
- [x] Required GitHub checks are green on the reviewed head commit.
- [x] Admin bypass was not used.
- [x] Agent governance receipt is attached when agent controls changed.

Evidence: agent_governance_gate.json
""".strip()


def _artifact(*, artifact_id: int, digest: str | None) -> object:
    return pr_receipt.GitHubArtifactEvidence(
        name="agent-governance-gate",
        artifact_id=artifact_id,
        workflow_run_head_sha="abc1234",
        workflow_run_id=10 + artifact_id,
        digest=digest,
        size_in_bytes=200,
        archive_sha256=digest,
        archive_size_bytes=200,
        expired=False,
        url=f"https://github.example/artifact/{artifact_id}",
        content=governance_content.content_from_payload(
            {
                "schema_version": 1,
                "status": "PASS",
                "control_surface_changed": True,
                "changed_paths": ["tools/agent_policy/governance_gate.py"],
                "head_commit": "abc1234",
                "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
            }
        ),
    )


def test_pr_receipt_projects_the_exact_artifact_that_validation_accepts() -> None:
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
                )
            ],
            statuses=[],
            artifacts=[
                _artifact(artifact_id=1, digest=None),
                _artifact(artifact_id=2, digest="sha256:" + "a" * 64),
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "PASS", result.errors
    assert result.evidence_chain["governance_artifact"]["artifact_id"] == 2


def test_pr_receipt_does_not_fallback_when_newest_artifact_is_invalid() -> None:
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
                )
            ],
            statuses=[],
            artifacts=[
                _artifact(artifact_id=1, digest="sha256:" + "a" * 64),
                _artifact(artifact_id=2, digest=None),
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert result.evidence_chain["governance_artifact"] is None
    assert any("digest" in error for error in result.errors)
