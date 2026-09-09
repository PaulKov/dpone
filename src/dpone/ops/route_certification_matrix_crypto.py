"""Cryptographic re-verification gate for production route proofs."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from dpone.contracts.route_attestation import RouteAttestationExpectedSubject
from dpone.readiness.route_attestation import verify_route_attestation_files

_SIGSTORE_FILE = "route-attestation.sigstore.json"
_POLICY_FILE = "route-attestation-policy.json"

ProductionAttestationCryptoGate = Callable[..., str | None]


class SupportsProductionAttestationCryptoGate(Protocol):
    def __call__(
        self,
        *,
        directory: Path,
        attestation_path: Path,
        certification_bundle_path: Path,
        expected: RouteAttestationExpectedSubject,
    ) -> str | None:
        """Return a blocker code, or ``None`` when cryptographic verification passed."""


def require_production_attestation_crypto(
    *,
    directory: Path,
    attestation_path: Path,
    certification_bundle_path: Path,
    expected: RouteAttestationExpectedSubject,
) -> str | None:
    """Fail closed unless Sigstore artifacts re-verify the attestation bytes."""

    sigstore_path = directory / _SIGSTORE_FILE
    policy_path = directory / _POLICY_FILE
    if not sigstore_path.is_file() or sigstore_path.is_symlink():
        return "route_matrix.attestation_crypto_proof_missing"
    if not policy_path.is_file() or policy_path.is_symlink():
        return "route_matrix.attestation_crypto_proof_missing"
    try:
        verification = verify_route_attestation_files(
            attestation_path=attestation_path,
            sigstore_bundle_path=sigstore_path,
            certification_bundle_path=certification_bundle_path,
            policy_path=policy_path,
            expected=expected,
        )
    except Exception:  # noqa: BLE001 - aggregator must never raise into matrix publish.
        return "route_matrix.attestation_crypto_verify_failed"
    if getattr(verification, "decision", None) != "verified":
        return "route_matrix.attestation_crypto_unverified"
    return None


__all__ = [
    "ProductionAttestationCryptoGate",
    "SupportsProductionAttestationCryptoGate",
    "require_production_attestation_crypto",
]
