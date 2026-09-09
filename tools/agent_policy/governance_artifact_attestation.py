"""Verify GitHub Artifact Attestations for governance evidence subjects."""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SLSA_PROVENANCE_V1 = "https://slsa.dev/provenance/v1"
GITHUB_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
DEFAULT_VERIFIER = "gh attestation verify"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
PULL_REQUEST_MERGE_REF = re.compile(r"^refs/pull/[1-9][0-9]*/merge$")

Runner = Callable[..., Any]


@dataclass(frozen=True)
class GitHubArtifactAttestationEvidence:
    """Compact evidence from `gh attestation verify --format json`."""

    status: str
    predicate_type: str | None = None
    subject_sha256: str | None = None
    source_repository: str | None = None
    source_ref: str | None = None
    source_digest: str | None = None
    signer_workflow: str | None = None
    issuer: str | None = None
    verified_timestamps_count: int | None = None
    runner_environment: str | None = None
    errors: list[str] = field(default_factory=list)


def verify_governance_subject(
    subject_path: Path,
    *,
    repo: str,
    token: str,
    signer_workflow: str,
    gh_bin: str = "gh",
    runner: Runner = subprocess.run,
) -> GitHubArtifactAttestationEvidence:
    """Verify a governance JSON subject with GitHub CLI attestation policy."""

    command = [
        gh_bin,
        "attestation",
        "verify",
        str(subject_path),
        "--repo",
        repo,
        "--signer-workflow",
        signer_workflow,
        "--cert-oidc-issuer",
        GITHUB_OIDC_ISSUER,
        "--deny-self-hosted-runners",
        "--predicate-type",
        SLSA_PROVENANCE_V1,
        "--format",
        "json",
    ]
    env = os.environ.copy()
    env["GH_TOKEN"] = token
    try:
        result = runner(command, capture_output=True, text=True, check=False, env=env)
    except FileNotFoundError as exc:
        return error_evidence(f"GitHub CLI attestation verifier is unavailable: {exc}.")
    except OSError as exc:
        return error_evidence(f"GitHub CLI attestation verifier failed to start: {exc}.")

    if getattr(result, "returncode", 1) != 0:
        return error_evidence(_command_errors(result))
    try:
        payload = json.loads(getattr(result, "stdout", "") or "null")
    except json.JSONDecodeError as exc:
        return error_evidence(f"GitHub CLI attestation verifier returned invalid JSON: {exc.msg}.")
    return evidence_from_gh_payload(payload, signer_workflow=signer_workflow)


def evidence_from_gh_payload(payload: Any, *, signer_workflow: str) -> GitHubArtifactAttestationEvidence:
    """Normalize successful GitHub CLI attestation JSON output."""

    if not isinstance(payload, list):
        return error_evidence("GitHub CLI attestation verifier output must be a JSON array.")
    if not payload:
        return error_evidence("GitHub CLI attestation verifier returned no verified attestations.")
    first = payload[0]
    if not isinstance(first, dict):
        return error_evidence("GitHub CLI attestation verifier entry must be a JSON object.")
    verification = _mapping(first.get("verificationResult"))
    statement = _mapping(verification.get("statement"))
    signature = _mapping(verification.get("signature"))
    certificate = _mapping(signature.get("certificate"))
    timestamps = verification.get("verifiedTimestamps")
    return GitHubArtifactAttestationEvidence(
        status="PASS",
        predicate_type=_optional_string(statement.get("predicateType")),
        subject_sha256=_subject_sha256(statement),
        source_repository=_source_repository(certificate),
        source_ref=_find_string(certificate, {"sourceRepositoryRef", "sourceRef", "githubRef"}),
        source_digest=_find_string(
            certificate, {"sourceRepositoryDigest", "sourceDigest", "githubSha", "githubWorkflowSHA"}
        ),
        signer_workflow=signer_workflow,
        issuer=_find_string(certificate, {"issuer"}),
        verified_timestamps_count=len(timestamps) if isinstance(timestamps, list) else None,
        runner_environment=_find_string(certificate, {"runnerEnvironment"}),
        errors=[],
    )


def validate_attestation_for_receipt(
    *,
    attestation: Any | None,
    governance_artifact_name: str,
    head_sha: str,
    expected_subject_sha256: str | None,
    expected_repository: str,
    expected_source_ref: str,
) -> list[str]:
    """Return fail-closed receipt errors for required attestation evidence."""

    if attestation is None:
        return [
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "is missing a verified GitHub artifact attestation."
        ]
    errors = [
        f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} attestation is invalid: {error}"
        for error in getattr(attestation, "errors", [])
    ]
    if getattr(attestation, "status", None) != "PASS":
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            f"must have attestation PASS (observed {getattr(attestation, 'status', None) or 'missing'})."
        )
    predicate_type = getattr(attestation, "predicate_type", None)
    if predicate_type != SLSA_PROVENANCE_V1:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            f"must have SLSA provenance predicate {SLSA_PROVENANCE_V1} "
            f"(observed {predicate_type or 'missing'})."
        )
    subject_sha256 = getattr(attestation, "subject_sha256", None)
    if not isinstance(subject_sha256, str) or SHA256.fullmatch(subject_sha256) is None:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation must contain subject_sha256 as 64 lowercase hexadecimal characters."
        )
    elif subject_sha256 != expected_subject_sha256:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation subject_sha256 does not match the extracted governance JSON."
        )
    source_repository = getattr(attestation, "source_repository", None)
    if source_repository != expected_repository:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            f"attestation source_repository must equal {expected_repository} "
            f"(observed {source_repository or 'missing'})."
        )
    source_ref = getattr(attestation, "source_ref", None)
    if not isinstance(source_ref, str) or PULL_REQUEST_MERGE_REF.fullmatch(source_ref) is None:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation must contain a pull-request merge ref in refs/pull/<number>/merge form."
        )
    elif source_ref != expected_source_ref:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            f"attestation source_ref must equal {expected_source_ref} (observed {source_ref})."
        )
    source_digest = getattr(attestation, "source_digest", None)
    if not isinstance(source_digest, str) or GIT_SHA.fullmatch(source_digest) is None:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation source_digest must contain 40 lowercase hexadecimal characters "
            f"(observed {source_digest or 'missing'})."
        )
    signer_workflow = getattr(attestation, "signer_workflow", None)
    if not isinstance(signer_workflow, str) or "/.github/workflows/" not in signer_workflow:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation must contain signer_workflow."
        )
    if source_repository and signer_workflow and not signer_workflow.startswith(f"{source_repository}/"):
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation signer_workflow must belong to source_repository."
        )
    issuer = getattr(attestation, "issuer", None)
    if issuer != GITHUB_OIDC_ISSUER:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            f"attestation issuer must equal {GITHUB_OIDC_ISSUER} (observed {issuer or 'missing'})."
        )
    timestamp_count = getattr(attestation, "verified_timestamps_count", None)
    if not isinstance(timestamp_count, int) or isinstance(timestamp_count, bool) or timestamp_count <= 0:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation must contain at least one verified timestamp."
        )
    runner_environment = getattr(attestation, "runner_environment", None)
    if runner_environment != "github-hosted":
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "attestation runner_environment must be github-hosted "
            f"(observed {runner_environment or 'missing'})."
        )
    return errors


def error_evidence(error: str | list[str]) -> GitHubArtifactAttestationEvidence:
    """Build failed attestation evidence with deterministic error text."""

    errors = error if isinstance(error, list) else [error]
    return GitHubArtifactAttestationEvidence(status="FAIL", errors=[item for item in errors if item])


def _command_errors(result: Any) -> list[str]:
    stderr = _short_output(getattr(result, "stderr", ""))
    stdout = _short_output(getattr(result, "stdout", ""))
    errors: list[str] = []
    if stderr:
        errors.append(f"GitHub CLI attestation verifier failed: {stderr}")
    if stdout:
        errors.append(f"GitHub CLI attestation verifier stdout: {stdout}")
    return errors or ["GitHub CLI attestation verifier failed without diagnostic output."]


def _short_output(value: Any, *, limit: int = 500) -> str:
    if not isinstance(value, str):
        return ""
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _subject_sha256(statement: dict[str, Any]) -> str | None:
    subjects = statement.get("subject")
    if not isinstance(subjects, list):
        return None
    for item in subjects:
        if not isinstance(item, dict):
            continue
        digest = item.get("digest")
        if isinstance(digest, dict):
            value = _optional_string(digest.get("sha256"))
            if value:
                return value
    return None


def _source_repository(certificate: dict[str, Any]) -> str | None:
    value = _find_string(certificate, {"sourceRepositoryURI", "sourceRepositoryUri", "sourceRepository"})
    if value is None:
        return None
    prefix = "https://github.com/"
    if value.startswith(prefix):
        return value[len(prefix) :]
    return value


def _find_string(value: Any, keys: set[str]) -> str | None:
    normalized = {_normalize_key(item) for item in keys}
    return _find_string_by_normalized_key(value, normalized)


def _find_string_by_normalized_key(value: Any, keys: set[str]) -> str | None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if _normalize_key(str(key)) in keys:
                candidate = _optional_string(nested)
                if candidate:
                    return candidate
        for nested in value.values():
            candidate = _find_string_by_normalized_key(nested, keys)
            if candidate:
                return candidate
    if isinstance(value, list):
        for nested in value:
            candidate = _find_string_by_normalized_key(nested, keys)
            if candidate:
                return candidate
    return None


def _normalize_key(value: str) -> str:
    return "".join(char.lower() for char in value if char.isalnum())


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
