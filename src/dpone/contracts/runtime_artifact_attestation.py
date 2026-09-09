"""Closed, secret-free policy for offline runtime artifact attestations."""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest

RUNTIME_ARTIFACT_TRUST_POLICY_V1 = "dpone.runtime-artifact-trust-policy.v1"
RUNTIME_ARTIFACT_TRUST_POLICY_V2 = "dpone.runtime-artifact-trust-policy.v2"
GITHUB_ATTESTATION_BACKEND = "github_artifact_attestation_v1"
GITHUB_PROVENANCE_PREDICATE = "https://slsa.dev/provenance/v1"
GITHUB_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
MAX_TRUSTED_ROOT_BYTES = 512 * 1024
MAX_ATTESTATION_BUNDLE_BYTES = 8 * 1024 * 1024
MAX_TRUSTED_ROOT_VALIDITY = timedelta(days=90)
MAX_TRUSTED_ROOT_FUTURE_SKEW = timedelta(minutes=5)

_REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_SIGNER_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_SECURITY_MINIMUM = (2, 93, 0)
_SECURITY_MAXIMUM = (3, 0, 0)


class RuntimeArtifactTrustPolicyError(ValueError):
    """Stable policy error safe to cross the runtime CLI boundary."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class RuntimeArtifactAttestationError(RuntimeError):
    """Stable adapter-neutral verification failure for runtime translation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class RuntimeArtifactAttestationSubject:
    """Exact verified local release-set passed across the runtime adapter port."""

    release_id: str
    subject_path: Path
    subject_sha256: str

    def __post_init__(self) -> None:
        if not is_canonical_sha256_digest(self.release_id):
            raise ValueError("attestation subject release_id must be a canonical sha256 digest")
        if not is_canonical_sha256_digest(self.subject_sha256):
            raise ValueError("attestation subject sha256 must be a canonical sha256 digest")
        if not self.subject_path.is_absolute():
            raise ValueError("attestation subject path must be absolute")


@dataclass(frozen=True, slots=True)
class GitHubCliPolicy:
    minimum_version: str
    maximum_version_exclusive: str
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class GitHubArtifactAttestationPolicy:
    repository: str
    signer_workflow: str
    signer_digest: str
    predicate_type: str
    cert_oidc_issuer: str
    deny_self_hosted_runners: bool
    trusted_root: bytes
    trusted_root_sha256: str
    trusted_root_generated_at: datetime
    trusted_root_refresh_after: datetime
    gh: GitHubCliPolicy


@dataclass(frozen=True, slots=True)
class RuntimeArtifactTrustPolicy:
    schema: str
    trust_tier: str
    attestations: str
    verifier: GitHubArtifactAttestationPolicy | None

    @property
    def requires_attestation(self) -> bool:
        return self.trust_tier == "production" or self.attestations == "required_for_prod"


def parse_runtime_artifact_trust_policy(
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> RuntimeArtifactTrustPolicy:
    """Parse one exact policy snapshot without coercion or permissive fallback."""

    schema = payload.get("schema")
    if schema == RUNTIME_ARTIFACT_TRUST_POLICY_V1:
        return _parse_v1(payload)
    if schema != RUNTIME_ARTIFACT_TRUST_POLICY_V2:
        raise _invalid("artifact trust policy schema is unsupported")
    _require_exact_keys(payload, {"schema", "trust_tier", "attestations", "verifier"})
    trust_tier, attestations = _common_policy_values(payload)
    verifier = _parse_github_verifier(
        _mapping(payload.get("verifier"), "verifier"),
        now=_utc_now(now),
    )
    return RuntimeArtifactTrustPolicy(
        schema=RUNTIME_ARTIFACT_TRUST_POLICY_V2,
        trust_tier=trust_tier,
        attestations=attestations,
        verifier=verifier,
    )


def runtime_attestation_bundle_key(
    *,
    release_id: str,
    release_set_sha256: str,
) -> PurePosixPath:
    """Return the immutable registry key for one exact release-set attestation."""

    if not is_canonical_sha256_digest(release_id) or not is_canonical_sha256_digest(release_set_sha256):
        raise RuntimeArtifactTrustPolicyError(
            "DPONE_ARTIFACT_ATTESTATION_SUBJECT_INVALID",
            "runtime attestation identity must use canonical sha256 digests",
        )
    return PurePosixPath(
        "attestations",
        "releases",
        release_id.replace(":", "-", 1),
        release_set_sha256.replace(":", "-", 1),
        "github.sigstore.jsonl",
    )


def github_cli_version_supported(version: str, policy: GitHubCliPolicy) -> bool:
    actual = _version_tuple(version)
    minimum = _version_tuple(policy.minimum_version)
    maximum = _version_tuple(policy.maximum_version_exclusive)
    if actual is None or minimum is None or maximum is None:
        return False
    return max(minimum, _SECURITY_MINIMUM) <= actual < maximum <= _SECURITY_MAXIMUM


def _parse_v1(payload: Mapping[str, Any]) -> RuntimeArtifactTrustPolicy:
    _require_exact_keys(payload, {"schema", "trust_tier", "attestations"})
    trust_tier, attestations = _common_policy_values(payload)
    return RuntimeArtifactTrustPolicy(
        schema=RUNTIME_ARTIFACT_TRUST_POLICY_V1,
        trust_tier=trust_tier,
        attestations=attestations,
        verifier=None,
    )


def _common_policy_values(payload: Mapping[str, Any]) -> tuple[str, str]:
    trust_tier = payload.get("trust_tier")
    attestations = payload.get("attestations")
    if trust_tier not in {"production", "non_production"}:
        raise _invalid("artifact trust policy trust_tier is invalid")
    if attestations not in {"optional", "required_for_prod"}:
        raise _invalid("artifact trust policy attestations value is invalid")
    if trust_tier == "production" and attestations != "required_for_prod":
        raise _invalid("production artifact trust policy must require attestations")
    return str(trust_tier), str(attestations)


def _parse_github_verifier(
    payload: Mapping[str, Any],
    *,
    now: datetime,
) -> GitHubArtifactAttestationPolicy:
    _require_exact_keys(
        payload,
        {
            "backend",
            "repository",
            "signer_workflow",
            "signer_digest",
            "predicate_type",
            "cert_oidc_issuer",
            "deny_self_hosted_runners",
            "trusted_root",
            "gh",
        },
    )
    repository = _matching_text(payload.get("repository"), _REPOSITORY_PATTERN, "repository")
    signer_workflow = _signer_workflow(payload.get("signer_workflow"), repository)
    signer_digest = _matching_text(payload.get("signer_digest"), _SIGNER_DIGEST_PATTERN, "signer_digest")
    if payload.get("backend") != GITHUB_ATTESTATION_BACKEND:
        raise _invalid("artifact attestation verifier backend is unsupported")
    if payload.get("predicate_type") != GITHUB_PROVENANCE_PREDICATE:
        raise _invalid("artifact attestation predicate_type is unsupported")
    if payload.get("cert_oidc_issuer") != GITHUB_OIDC_ISSUER:
        raise _invalid("artifact attestation OIDC issuer is unsupported")
    if payload.get("deny_self_hosted_runners") is not True:
        raise _invalid("artifact attestation policy must deny self-hosted runners")
    trusted_root, trusted_root_sha256, generated_at, refresh_after = _parse_trusted_root(
        _mapping(payload.get("trusted_root"), "trusted_root"),
        now=now,
    )
    gh = _parse_gh(_mapping(payload.get("gh"), "gh"))
    return GitHubArtifactAttestationPolicy(
        repository=repository,
        signer_workflow=signer_workflow,
        signer_digest=signer_digest,
        predicate_type=GITHUB_PROVENANCE_PREDICATE,
        cert_oidc_issuer=GITHUB_OIDC_ISSUER,
        deny_self_hosted_runners=True,
        trusted_root=trusted_root,
        trusted_root_sha256=trusted_root_sha256,
        trusted_root_generated_at=generated_at,
        trusted_root_refresh_after=refresh_after,
        gh=gh,
    )


def _parse_trusted_root(
    payload: Mapping[str, Any],
    *,
    now: datetime,
) -> tuple[bytes, str, datetime, datetime]:
    _require_exact_keys(payload, {"encoding", "content", "sha256", "generated_at", "refresh_after"})
    if payload.get("encoding") != "base64":
        raise _invalid("trusted root encoding must be base64")
    content = payload.get("content")
    if not isinstance(content, str) or not content:
        raise _invalid("trusted root content is invalid")
    try:
        decoded = base64.b64decode(content, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise _invalid("trusted root content is invalid") from exc
    if not decoded or len(decoded) > MAX_TRUSTED_ROOT_BYTES:
        raise _invalid("trusted root content exceeds its byte contract")
    expected = payload.get("sha256")
    actual = "sha256:" + hashlib.sha256(decoded).hexdigest()
    if not is_canonical_sha256_digest(expected) or expected != actual:
        raise _invalid("trusted root checksum does not match its content")
    generated_at = _timestamp(payload.get("generated_at"), "generated_at")
    refresh_after = _timestamp(payload.get("refresh_after"), "refresh_after")
    if generated_at > now + MAX_TRUSTED_ROOT_FUTURE_SKEW:
        raise _invalid("trusted root generated_at is unreasonably far in the future")
    if refresh_after <= generated_at:
        raise _invalid("trusted root refresh_after must follow generated_at")
    if refresh_after - generated_at > MAX_TRUSTED_ROOT_VALIDITY:
        raise _invalid("trusted root refresh interval exceeds the supported maximum")
    if refresh_after <= now:
        raise RuntimeArtifactTrustPolicyError(
            "DPONE_ARTIFACT_TRUST_POLICY_EXPIRED",
            "artifact trust policy trusted root refresh is overdue",
        )
    return decoded, actual, generated_at, refresh_after


def _parse_gh(payload: Mapping[str, Any]) -> GitHubCliPolicy:
    _require_exact_keys(payload, {"minimum_version", "maximum_version_exclusive", "timeout_seconds"})
    minimum = _version(payload.get("minimum_version"), "minimum_version")
    maximum = _version(payload.get("maximum_version_exclusive"), "maximum_version_exclusive")
    timeout = payload.get("timeout_seconds")
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 60:
        raise _invalid("GitHub CLI timeout_seconds must be between 1 and 60")
    policy = GitHubCliPolicy(minimum, maximum, timeout)
    maximum_tuple = _version_tuple(maximum)
    if maximum_tuple is None or not github_cli_version_supported(minimum, policy) or maximum_tuple > _SECURITY_MAXIMUM:
        raise _invalid("GitHub CLI version range is outside the supported security window")
    return policy


def _signer_workflow(value: object, repository: str) -> str:
    if not isinstance(value, str) or len(value) > 300:
        raise _invalid("artifact attestation signer_workflow is invalid")
    prefix = f"{repository}/.github/workflows/"
    relative = value.removeprefix(prefix)
    path = PurePosixPath(relative)
    if (
        not value.startswith(prefix)
        or not relative
        or path.as_posix() != relative
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.suffix not in {".yml", ".yaml"}
    ):
        raise _invalid("artifact attestation signer_workflow is invalid")
    return value


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise _invalid(f"trusted root {field} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise _invalid(f"trusted root {field} must be an RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):  # noqa: UP017
        raise _invalid(f"trusted root {field} must be an RFC3339 UTC timestamp")
    return parsed


def _version(value: object, field: str) -> str:
    if not isinstance(value, str) or _version_tuple(value) is None:
        raise _invalid(f"GitHub CLI {field} is invalid")
    return value


def _version_tuple(value: str) -> tuple[int, int, int] | None:
    match = _VERSION_PATTERN.fullmatch(value)
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def _matching_text(value: object, pattern: re.Pattern[str], field: str) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise _invalid(f"artifact attestation {field} is invalid")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid(f"artifact trust policy {field} must be an object")
    return value


def _require_exact_keys(value: Mapping[str, Any], expected: set[str]) -> None:
    if {str(key) for key in value} != expected:
        raise _invalid("artifact trust policy contains missing or unsupported fields")


def _utc_now(value: datetime | None) -> datetime:
    current = value or datetime.now(timezone.utc)  # noqa: UP017
    if current.tzinfo is None:
        raise _invalid("artifact trust policy validation clock must be timezone-aware")
    return current.astimezone(timezone.utc)  # noqa: UP017


def _invalid(message: str) -> RuntimeArtifactTrustPolicyError:
    return RuntimeArtifactTrustPolicyError("DPONE_ARTIFACT_TRUST_POLICY_INVALID", message)


__all__ = [
    "GITHUB_ATTESTATION_BACKEND",
    "GITHUB_OIDC_ISSUER",
    "GITHUB_PROVENANCE_PREDICATE",
    "GitHubArtifactAttestationPolicy",
    "GitHubCliPolicy",
    "MAX_ATTESTATION_BUNDLE_BYTES",
    "MAX_TRUSTED_ROOT_FUTURE_SKEW",
    "MAX_TRUSTED_ROOT_BYTES",
    "MAX_TRUSTED_ROOT_VALIDITY",
    "RUNTIME_ARTIFACT_TRUST_POLICY_V1",
    "RUNTIME_ARTIFACT_TRUST_POLICY_V2",
    "RuntimeArtifactTrustPolicy",
    "RuntimeArtifactAttestationError",
    "RuntimeArtifactAttestationSubject",
    "RuntimeArtifactTrustPolicyError",
    "github_cli_version_supported",
    "parse_runtime_artifact_trust_policy",
    "runtime_attestation_bundle_key",
]
