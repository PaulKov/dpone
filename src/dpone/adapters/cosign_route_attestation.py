"""Backward-compatible route facade over generic cosign blob verification."""

from __future__ import annotations

from typing import Protocol

from dpone.adapters.cosign_blob_signature import (
    CosignBlobSignatureVerifier,
    SubprocessBlobVerificationCommandRunner,
)
from dpone.contracts.route_attestation import (
    CosignVerificationPolicy,
    RouteAttestationCommandResult,
    SignatureVerificationResult,
)


class _RouteCommandRunner(Protocol):
    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> RouteAttestationCommandResult: ...


SubprocessRouteAttestationCommandRunner = SubprocessBlobVerificationCommandRunner


class CosignRouteAttestationSignatureVerifier:
    """Preserve route-specific codes while sharing the bounded cosign adapter."""

    def __init__(
        self,
        *,
        runner: _RouteCommandRunner | None = None,
        executable: str = "cosign",
    ) -> None:
        self._delegate = CosignBlobSignatureVerifier(runner=runner, executable=executable)

    def verify_blob(
        self,
        *,
        blob: bytes,
        sigstore_bundle: bytes,
        trusted_root: bytes,
        policy: CosignVerificationPolicy,
    ) -> SignatureVerificationResult:
        result = self._delegate.verify_blob(
            blob=blob,
            sigstore_bundle=sigstore_bundle,
            trusted_root=trusted_root,
            policy=policy,
        )
        if result.status == "verified" and result.verifier_version is not None:
            return SignatureVerificationResult.verified(result.verifier_version)
        if result.status == "invalid":
            return SignatureVerificationResult.invalid(
                "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID",
                "The route-attestation signature or signer identity is invalid.",
                result.verifier_version,
            )
        if result.reason == "version_unsupported":
            return SignatureVerificationResult.unverified(
                "DPONE_ROUTE_ATTESTATION_VERIFIER_VERSION_UNSUPPORTED",
                "The installed route-attestation verifier version is outside the certified range.",
            )
        return SignatureVerificationResult.unverified(
            "DPONE_ROUTE_ATTESTATION_VERIFIER_UNAVAILABLE",
            "The route-attestation verifier is unavailable or timed out.",
        )


__all__ = [
    "CosignRouteAttestationSignatureVerifier",
    "SubprocessRouteAttestationCommandRunner",
]
