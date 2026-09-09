from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "tools/agent_policy/governance_artifact_attestation.py"
SIGNER_WORKFLOW = "PaulKov/dpone/.github/workflows/ci.yml"
REVIEWED_HEAD = "a" * 40
SYNTHETIC_MERGE = "b" * 40


def _load() -> ModuleType:
    assert MODULE_PATH.is_file(), "governance artifact attestation verifier module is missing"
    spec = importlib.util.spec_from_file_location("dpone_agent_governance_artifact_attestation", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _verified_payload() -> list[dict[str, Any]]:
    return [
        {
            "verificationResult": {
                "statement": {
                    "predicateType": "https://slsa.dev/provenance/v1",
                    "subject": [
                        {
                            "name": "agent_governance_gate.json",
                            "digest": {"sha256": "c" * 64},
                        }
                    ],
                },
                "signature": {
                    "certificate": {
                        "issuer": "https://token.actions.githubusercontent.com",
                        "sourceRepositoryURI": "https://github.com/PaulKov/dpone",
                        "sourceRepositoryDigest": SYNTHETIC_MERGE,
                        "sourceRepositoryRef": "refs/pull/320/merge",
                        "subjectAlternativeName": (
                            "https://github.com/PaulKov/dpone/.github/workflows/ci.yml@refs/pull/320/merge"
                        ),
                        "runnerEnvironment": "github-hosted",
                    }
                },
                "verifiedTimestamps": [{"timestamp": "2026-07-13T00:00:00Z"}],
            }
        }
    ]


def test_attestation_payload_normalizes_verified_github_cli_output() -> None:
    attestation = _load()

    evidence = attestation.evidence_from_gh_payload(_verified_payload(), signer_workflow=SIGNER_WORKFLOW)

    assert evidence.status == "PASS"
    assert evidence.predicate_type == "https://slsa.dev/provenance/v1"
    assert evidence.subject_sha256 == "c" * 64
    assert evidence.source_repository == "PaulKov/dpone"
    assert evidence.source_ref == "refs/pull/320/merge"
    assert evidence.source_digest == SYNTHETIC_MERGE
    assert evidence.signer_workflow == SIGNER_WORKFLOW
    assert evidence.issuer == "https://token.actions.githubusercontent.com"
    assert evidence.verified_timestamps_count == 1
    assert evidence.runner_environment == "github-hosted"
    assert evidence.errors == []


def test_attestation_verifier_invokes_gh_with_fail_closed_policy(tmp_path: Path) -> None:
    attestation = _load()
    subject = tmp_path / "agent_governance_gate.json"
    subject.write_text("{}", encoding="utf-8")
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_runner(command: list[str], **kwargs: Any) -> SimpleNamespace:
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=json.dumps(_verified_payload()), stderr="")

    evidence = attestation.verify_governance_subject(
        subject,
        repo="PaulKov/dpone",
        token="token",
        signer_workflow=SIGNER_WORKFLOW,
        runner=fake_runner,
    )

    assert evidence.status == "PASS"
    command, kwargs = calls[0]
    assert command == [
        "gh",
        "attestation",
        "verify",
        str(subject),
        "--repo",
        "PaulKov/dpone",
        "--signer-workflow",
        SIGNER_WORKFLOW,
        "--cert-oidc-issuer",
        "https://token.actions.githubusercontent.com",
        "--deny-self-hosted-runners",
        "--predicate-type",
        "https://slsa.dev/provenance/v1",
        "--format",
        "json",
    ]
    assert kwargs["env"]["GH_TOKEN"] == "token"


def test_attestation_verifier_fails_closed_on_nonzero_gh_exit(tmp_path: Path) -> None:
    attestation = _load()
    subject = tmp_path / "agent_governance_gate.json"
    subject.write_text("{}", encoding="utf-8")

    def fake_runner(command: list[str], **kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(returncode=1, stdout="", stderr="no attestations found for subject")

    evidence = attestation.verify_governance_subject(
        subject,
        repo="PaulKov/dpone",
        token="token",
        signer_workflow=SIGNER_WORKFLOW,
        runner=fake_runner,
    )

    assert evidence.status == "FAIL"
    assert any("no attestations found" in error for error in evidence.errors)


def test_receipt_accepts_pull_request_merge_provenance_for_reviewed_head() -> None:
    attestation = _load()
    evidence = attestation.evidence_from_gh_payload(_verified_payload(), signer_workflow=SIGNER_WORKFLOW)

    errors = attestation.validate_attestation_for_receipt(
        attestation=evidence,
        governance_artifact_name="agent-governance-gate",
        head_sha=REVIEWED_HEAD,
        expected_subject_sha256="c" * 64,
        expected_repository="PaulKov/dpone",
        expected_source_ref="refs/pull/320/merge",
    )

    assert errors == []


def test_receipt_rejects_different_pull_request_source_ref() -> None:
    attestation = _load()
    evidence = attestation.evidence_from_gh_payload(_verified_payload(), signer_workflow=SIGNER_WORKFLOW)
    invalid = attestation.GitHubArtifactAttestationEvidence(
        **{**evidence.__dict__, "source_ref": "refs/pull/999/merge"}
    )

    errors = attestation.validate_attestation_for_receipt(
        attestation=invalid,
        governance_artifact_name="agent-governance-gate",
        head_sha=REVIEWED_HEAD,
        expected_subject_sha256="c" * 64,
        expected_repository="PaulKov/dpone",
        expected_source_ref="refs/pull/320/merge",
    )

    assert any("refs/pull/320/merge" in error and "refs/pull/999/merge" in error for error in errors)


@pytest.mark.parametrize("invalid_digest", ["a" * 39, "a" * 41, "A" * 40, "g" * 40])
def test_receipt_rejects_noncanonical_source_digest(invalid_digest: str) -> None:
    attestation = _load()
    evidence = attestation.evidence_from_gh_payload(_verified_payload(), signer_workflow=SIGNER_WORKFLOW)
    invalid = attestation.GitHubArtifactAttestationEvidence(**{**evidence.__dict__, "source_digest": invalid_digest})

    errors = attestation.validate_attestation_for_receipt(
        attestation=invalid,
        governance_artifact_name="agent-governance-gate",
        head_sha=REVIEWED_HEAD,
        expected_subject_sha256="c" * 64,
        expected_repository="PaulKov/dpone",
        expected_source_ref="refs/pull/320/merge",
    )

    assert any("40 lowercase hexadecimal" in error for error in errors)


def test_receipt_rejects_different_source_repository() -> None:
    attestation = _load()
    evidence = attestation.evidence_from_gh_payload(_verified_payload(), signer_workflow=SIGNER_WORKFLOW)
    invalid = attestation.GitHubArtifactAttestationEvidence(
        **{**evidence.__dict__, "source_repository": "other/repository"}
    )

    errors = attestation.validate_attestation_for_receipt(
        attestation=invalid,
        governance_artifact_name="agent-governance-gate",
        head_sha=REVIEWED_HEAD,
        expected_subject_sha256="c" * 64,
        expected_repository="PaulKov/dpone",
        expected_source_ref="refs/pull/320/merge",
    )

    assert any("source_repository must equal PaulKov/dpone" in error for error in errors)
