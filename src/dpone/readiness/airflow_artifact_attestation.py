"""CLI-facing offline verification of one exact runtime release subject."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone.adapters.github_artifact_attestation import (
    GitHubArtifactAttestationError,
    GitHubArtifactAttestationVerifier,
)
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.runtime_artifact_attestation import (
    RuntimeArtifactTrustPolicyError,
    parse_runtime_artifact_trust_policy,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix
from dpone.readiness.error_contract import error_docs_url

_MAX_POLICY_BYTES = 1024 * 1024


def verify_runtime_artifact_attestation(
    *,
    subject_path: str,
    bundle_path: str,
    trust_policy_path: str,
    expected_trust_policy_sha256: str | None,
    expected_trust_tier: str,
    expected_subject_sha256: str | None,
    verifier: GitHubArtifactAttestationVerifier | None = None,
    now: datetime | None = None,
) -> SelfServiceResult:
    """Apply the same closed policy used by stock runtime init-fetch."""

    try:
        policy_payload, policy_sha256 = _policy_payload(Path(trust_policy_path))
        if (
            not is_canonical_sha256_digest(expected_trust_policy_sha256)
            or policy_sha256 != expected_trust_policy_sha256
        ):
            raise RuntimeArtifactTrustPolicyError(
                "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
                "artifact trust policy checksum does not match the pinned deployment input",
            )
        policy = parse_runtime_artifact_trust_policy(policy_payload, now=now)
        if expected_trust_tier not in {"production", "non_production"}:
            raise RuntimeArtifactTrustPolicyError(
                "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
                "expected artifact trust tier is invalid",
            )
        if policy.trust_tier != expected_trust_tier:
            raise RuntimeArtifactTrustPolicyError(
                "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
                "artifact trust policy does not match the expected trust tier",
            )
        if policy.verifier is None:
            raise RuntimeArtifactTrustPolicyError(
                "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE",
                "artifact trust policy does not define a concrete offline verifier",
            )
        result = (verifier or GitHubArtifactAttestationVerifier()).verify_files(
            subject_path=Path(subject_path),
            bundle_path=Path(bundle_path),
            policy=policy.verifier,
            expected_subject_sha256=expected_subject_sha256,
        )
    except (RuntimeArtifactTrustPolicyError, GitHubArtifactAttestationError) as exc:
        return SelfServiceResult(
            passed=False,
            errors=(_attestation_error(exc.code, str(exc)),),
            details={
                "schema": "dpone.runtime-artifact-attestation-verification.v1",
                "status": "failed",
            },
            exit_code=4,
        )
    except OSError:
        return SelfServiceResult(
            passed=False,
            errors=(
                _attestation_error(
                    "DPONE_ARTIFACT_ATTESTATION_INPUT_INVALID",
                    "offline attestation input is missing or unsafe",
                ),
            ),
            details={
                "schema": "dpone.runtime-artifact-attestation-verification.v1",
                "status": "failed",
            },
            exit_code=4,
        )
    return SelfServiceResult(
        passed=True,
        details={
            "schema": "dpone.runtime-artifact-attestation-verification.v1",
            "status": "passed",
            "subject_sha256": result.subject_sha256,
            "verifier": "github_artifact_attestation_v1",
            "verifier_version": result.verifier_version,
            "verified_attestations": result.verified_attestations,
        },
        exit_code=0,
    )


def _policy_payload(path: Path) -> tuple[Mapping[str, Any], str]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= _MAX_POLICY_BYTES:
            raise RuntimeArtifactTrustPolicyError(
                "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
                "artifact trust policy is missing or unsafe",
            )
        chunks: list[bytes] = []
        total = 0
        while total <= _MAX_POLICY_BYTES:
            chunk = os.read(descriptor, min(64 * 1024, _MAX_POLICY_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    if total > _MAX_POLICY_BYTES:
        raise RuntimeArtifactTrustPolicyError(
            "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
            "artifact trust policy exceeds its byte limit",
        )
    payload = b"".join(chunks)
    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise RuntimeArtifactTrustPolicyError(
            "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
            "artifact trust policy is not valid JSON",
        ) from exc
    if not isinstance(value, Mapping):
        raise RuntimeArtifactTrustPolicyError(
            "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
            "artifact trust policy must be an object",
        )
    return value, "sha256:" + hashlib.sha256(payload).hexdigest()


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _attestation_error(code: str, message: str) -> dict[str, Any]:
    fix_id = {
        "DPONE_ARTIFACT_TRUST_POLICY_INVALID": "publish_reviewed_runtime_trust_policy",
        "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH": "restore_pinned_runtime_trust_policy",
        "DPONE_ARTIFACT_TRUST_POLICY_EXPIRED": "rotate_runtime_trusted_root",
        "DPONE_ARTIFACT_ATTESTATION_INPUT_INVALID": "restore_pinned_attestation_inputs",
        "DPONE_ARTIFACT_ATTESTATION_SUBJECT_INVALID": "rebuild_release_projection",
        "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH": "quarantine_release_subject",
        "DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID": "republish_immutable_attestation_bundle",
        "DPONE_ARTIFACT_ATTESTATION_BUNDLE_TOO_LARGE": "correct_attestation_producer",
        "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE": "use_certified_runtime_image",
        "DPONE_ARTIFACT_ATTESTATION_VERIFIER_VERSION_UNSUPPORTED": "use_supported_verifier_version",
        "DPONE_ARTIFACT_ATTESTATION_VERIFICATION_FAILED": "investigate_attestation_trust_chain",
        "DPONE_ARTIFACT_ATTESTATION_RESULT_INVALID": "repair_certified_attestation_verifier",
    }.get(code, "inspect_runtime_attestation_failure")
    return dpone_error(
        code,
        message,
        stage="airflow_artifact_attestation_verify",
        fixes=[manual_fix(fix_id)],
        docs_url=error_docs_url(code),
    )


__all__ = ["verify_runtime_artifact_attestation"]
