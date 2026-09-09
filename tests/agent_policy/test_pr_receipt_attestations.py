from __future__ import annotations

import importlib.util
import inspect
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]
REVIEWED_HEAD = "a" * 40
SYNTHETIC_MERGE = "b" * 40


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pr_receipt = _load("dpone_agent_pr_receipt_attestation_test", "tools/agent_policy/pr_receipt.py")
governance_artifact_content = _load(
    "dpone_agent_governance_artifact_content_pr_receipt_attestation_test",
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


def _governance_content(changed_paths: list[str]) -> object:
    return governance_artifact_content.content_from_payload(
        {
            "schema_version": 1,
            "status": "PASS",
            "control_surface_changed": True,
            "changed_paths": changed_paths,
            "head_commit": REVIEWED_HEAD,
            "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
        },
        subject_sha256="c" * 64,
    )


def _verified_attestation() -> object:
    assert hasattr(pr_receipt, "GitHubArtifactAttestationEvidence")
    return pr_receipt.GitHubArtifactAttestationEvidence(
        status="PASS",
        predicate_type="https://slsa.dev/provenance/v1",
        subject_sha256="c" * 64,
        source_repository="PaulKov/dpone",
        source_ref="refs/pull/320/merge",
        source_digest=SYNTHETIC_MERGE,
        signer_workflow="PaulKov/dpone/.github/workflows/ci.yml",
        issuer="https://token.actions.githubusercontent.com",
        verified_timestamps_count=1,
        runner_environment="github-hosted",
        errors=[],
    )


def _github_evidence(*, attestation: object | None = None) -> object:
    return pr_receipt.GitHubEvidence(
        head_sha=REVIEWED_HEAD,
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
                workflow_run_head_sha=REVIEWED_HEAD,
                workflow_run_id=10,
                digest="sha256:" + "a" * 64,
                size_in_bytes=200,
                archive_sha256="sha256:" + "a" * 64,
                archive_size_bytes=200,
                expired=False,
                url="https://github.example/artifact",
                content=_governance_content(["tools/agent_policy/governance_gate.py"]),
                attestation=attestation,
            )
        ],
    )


def test_pr_receipt_fails_when_required_governance_attestation_is_missing() -> None:
    assert "require_github_attestation" in inspect.signature(pr_receipt.validate_pr_receipt).parameters

    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=_github_evidence(),
        require_github_evidence=True,
        require_github_attestation=True,
        repository="PaulKov/dpone",
        pull_request_number=320,
    )

    assert result.status == "FAIL"
    assert any("attestation" in error for error in result.errors)


def test_pr_receipt_accepts_verified_governance_attestation_when_required() -> None:
    assert "require_github_attestation" in inspect.signature(pr_receipt.validate_pr_receipt).parameters

    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=_github_evidence(attestation=_verified_attestation()),
        require_github_evidence=True,
        require_github_attestation=True,
        repository="PaulKov/dpone",
        pull_request_number=320,
    )

    assert result.status == "PASS"
    assert result.errors == []


def test_pr_receipt_rejects_incomplete_pass_attestation() -> None:
    incomplete = pr_receipt.GitHubArtifactAttestationEvidence(status="PASS")

    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=_github_evidence(attestation=incomplete),
        require_github_evidence=True,
        require_github_attestation=True,
        repository="PaulKov/dpone",
        pull_request_number=320,
    )

    assert result.status == "FAIL"
    assert any("subject_sha256" in error for error in result.errors)
    assert any("source_digest" in error for error in result.errors)
    assert any("runner_environment" in error for error in result.errors)


def test_pr_receipt_rejects_attestation_for_different_governance_json() -> None:
    attestation = _verified_attestation()
    mismatched = pr_receipt.GitHubArtifactAttestationEvidence(**{**attestation.__dict__, "subject_sha256": "d" * 64})

    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=_github_evidence(attestation=mismatched),
        require_github_evidence=True,
        require_github_attestation=True,
        repository="PaulKov/dpone",
        pull_request_number=320,
    )

    assert result.status == "FAIL"
    assert any("extracted governance JSON" in error for error in result.errors)
