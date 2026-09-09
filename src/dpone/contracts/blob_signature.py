"""Vendor-neutral contracts for bounded external blob verification."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CosignVerificationPolicy:
    """Exact signer identity and certified verifier process bounds."""

    certificate_identity: str
    certificate_oidc_issuer: str
    minimum_version: str
    maximum_version_exclusive: str
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class BlobSignatureVerification:
    """Safe infrastructure result without verifier stdout or secret material."""

    status: str
    reason: str
    verifier_version: str | None = None

    @classmethod
    def verified(cls, verifier_version: str) -> BlobSignatureVerification:
        return cls(status="verified", reason="verified", verifier_version=verifier_version)

    @classmethod
    def invalid(cls, verifier_version: str | None = None) -> BlobSignatureVerification:
        return cls(status="invalid", reason="invalid_signature", verifier_version=verifier_version)

    @classmethod
    def unverified(cls, reason: str) -> BlobSignatureVerification:
        return cls(status="unverified", reason=reason)


@dataclass(frozen=True, slots=True)
class BlobVerificationCommandResult:
    """Bounded output returned by an injected verifier command runner."""

    returncode: int
    stdout: str
    stderr: str


__all__ = [
    "BlobSignatureVerification",
    "BlobVerificationCommandResult",
    "CosignVerificationPolicy",
]
