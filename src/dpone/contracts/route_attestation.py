"""Pure contracts for signed route authorization attestations."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.blob_signature import BlobVerificationCommandResult, CosignVerificationPolicy

ATTESTATION_SCHEMA = "dpone.route-attestation.v1"
POLICY_SCHEMA = "dpone.route-attestation-policy.v1"
VERIFICATION_SCHEMA = "dpone.route-attestation-verification.v1"


def sha256_bytes(value: bytes) -> str:
    """Return a canonical SHA-256 digest for exact bytes."""

    return "sha256:" + hashlib.sha256(value).hexdigest()


def route_attestation_id(claims: dict[str, Any]) -> str:
    """Return the self-reference-free identity of canonical claims."""

    return canonical_fingerprint(claims)


def parse_aware_datetime(value: object) -> datetime | None:
    """Parse an offset-aware ISO-8601 timestamp without assuming a timezone."""

    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


@dataclass(frozen=True, slots=True)
class RouteAttestationArtifact:
    """Deterministic unsigned blob that external CI signs verbatim."""

    attestation_id: str
    claims: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": ATTESTATION_SCHEMA,
            "attestation_id": self.attestation_id,
            "claims": self.claims,
        }

    def to_bytes(self) -> bytes:
        return (json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


@dataclass(frozen=True, slots=True)
class RouteAttestationExpectedSubject:
    """Actual immutable runtime identity that signed claims must equal."""

    route_id: str
    release_id: str
    deployment_id: str
    environment: str
    runtime_image_digest: str
    authorization_profile: str

    def to_dict(self) -> dict[str, str]:
        return {
            "route_id": self.route_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "runtime_image_digest": self.runtime_image_digest,
            "authorization_profile": self.authorization_profile,
        }


@dataclass(frozen=True, slots=True)
class SignatureVerificationResult:
    """Infrastructure result before route subject policy is evaluated."""

    status: str
    code: str
    message: str
    verifier_version: str | None = None

    @classmethod
    def verified(cls, verifier_version: str) -> SignatureVerificationResult:
        return cls(
            "verified",
            "DPONE_ROUTE_ATTESTATION_SIGNATURE_VERIFIED",
            "Route attestation signature was verified.",
            verifier_version,
        )

    @classmethod
    def invalid(cls, code: str, message: str, verifier_version: str | None = None) -> SignatureVerificationResult:
        return cls("invalid", code, message, verifier_version)

    @classmethod
    def unverified(cls, code: str, message: str) -> SignatureVerificationResult:
        return cls("unverified", code, message)


RouteAttestationCommandResult = BlobVerificationCommandResult


@dataclass(frozen=True, slots=True)
class RouteAttestationVerification:
    """Safe public receipt for one complete trust decision."""

    decision: str
    code: str
    message: str
    attestation_id: str | None
    attestation_sha256: str
    certification_bundle_sha256: str
    policy_fingerprint: str
    route_id: str | None
    release_id: str | None
    deployment_id: str | None
    environment: str | None
    authorization_profile: str | None
    signer: dict[str, Any]
    validity: dict[str, Any]
    verified_at: str

    @property
    def is_verified(self) -> bool:
        return self.decision == "verified"

    @property
    def verified_route_ids(self) -> tuple[str, ...]:
        return (self.route_id,) if self.is_verified and self.route_id else ()

    def to_dict(self) -> dict[str, Any]:
        errors = [] if self.is_verified else [{"code": self.code, "message": self.message}]
        return {
            "schema": VERIFICATION_SCHEMA,
            "decision": self.decision,
            "code": self.code,
            "message": self.message,
            "attestation_id": self.attestation_id,
            "attestation_sha256": self.attestation_sha256,
            "certification_bundle_sha256": self.certification_bundle_sha256,
            "policy_fingerprint": self.policy_fingerprint,
            "route_id": self.route_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "authorization_profile": self.authorization_profile,
            "signer": dict(self.signer),
            "validity": dict(self.validity),
            "verified_at": self.verified_at,
            "errors": errors,
        }


__all__ = [
    "ATTESTATION_SCHEMA",
    "POLICY_SCHEMA",
    "VERIFICATION_SCHEMA",
    "CosignVerificationPolicy",
    "RouteAttestationArtifact",
    "RouteAttestationCommandResult",
    "RouteAttestationExpectedSubject",
    "RouteAttestationVerification",
    "SignatureVerificationResult",
    "canonical_fingerprint",
    "is_canonical_sha256_digest",
    "parse_aware_datetime",
    "route_attestation_id",
    "sha256_bytes",
]
