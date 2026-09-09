"""Infrastructure ports for route-attestation signature verification."""

from __future__ import annotations

from typing import Protocol

from dpone.contracts.route_attestation import (
    CosignVerificationPolicy,
    RouteAttestationCommandResult,
    SignatureVerificationResult,
)


class RouteAttestationCommandRunner(Protocol):
    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> RouteAttestationCommandResult: ...


class RouteAttestationSignatureVerifier(Protocol):
    def verify_blob(
        self,
        *,
        blob: bytes,
        sigstore_bundle: bytes,
        trusted_root: bytes,
        policy: CosignVerificationPolicy,
    ) -> SignatureVerificationResult: ...


__all__ = [
    "RouteAttestationCommandResult",
    "RouteAttestationCommandRunner",
    "RouteAttestationSignatureVerifier",
]
