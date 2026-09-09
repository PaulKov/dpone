"""Cosign adapter for exact blob verification with a pinned public key."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from dpone.adapters.cosign_blob_signature import (
    CosignBlobSignatureVerifier,
    SubprocessBlobVerificationCommandRunner,
    write_private_verification_file,
)
from dpone.contracts.blob_signature import (
    BlobSignatureVerification,
    BlobVerificationCommandResult,
    CosignVerificationPolicy,
)

if TYPE_CHECKING:
    from dpone.contracts.airflow_deployment_trust_policy import CosignPublicKeyPolicy


class _CommandRunner(Protocol):
    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> BlobVerificationCommandResult: ...


class CosignPublicKeyBlobSignatureVerifier:
    """Verify one statement with one exact public key and detached bundle."""

    def __init__(
        self,
        *,
        runner: _CommandRunner | None = None,
        executable: str = "cosign",
    ) -> None:
        self._runner = runner or SubprocessBlobVerificationCommandRunner()
        self._executable = executable
        self._version_verifier = CosignBlobSignatureVerifier(runner=self._runner, executable=executable)

    def verify_blob(
        self,
        *,
        blob: bytes,
        sigstore_bundle: bytes,
        public_key: bytes,
        policy: CosignPublicKeyPolicy,
    ) -> BlobSignatureVerification:
        version = self._version_verifier.certified_version(_version_policy(policy))
        if isinstance(version, BlobSignatureVerification):
            return version
        try:
            with tempfile.TemporaryDirectory(prefix="dpone-artifact-attestation-") as directory:
                root = Path(directory)
                blob_path = write_private_verification_file(root / "artifact-attestation.json", blob)
                bundle_path = write_private_verification_file(
                    root / "artifact-attestation.sigstore.json",
                    sigstore_bundle,
                )
                key_path = write_private_verification_file(root / "cosign.pub", public_key)
                result = self._runner.run(
                    (
                        self._executable,
                        "verify-blob",
                        "--private-infrastructure",
                        "--bundle",
                        str(bundle_path),
                        "--key",
                        str(key_path),
                        "--timeout",
                        f"{policy.timeout_seconds}s",
                        str(blob_path),
                    ),
                    timeout_seconds=policy.timeout_seconds,
                )
        except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError):
            return BlobSignatureVerification.unverified("verifier_unavailable")
        if result.returncode != 0:
            return BlobSignatureVerification.invalid(version)
        return BlobSignatureVerification.verified(version)


def _version_policy(policy: CosignPublicKeyPolicy) -> CosignVerificationPolicy:
    return CosignVerificationPolicy(
        certificate_identity="not-used-for-public-key-verification",
        certificate_oidc_issuer="not-used-for-public-key-verification",
        minimum_version=policy.minimum_version,
        maximum_version_exclusive=policy.maximum_version_exclusive,
        timeout_seconds=policy.timeout_seconds,
    )


__all__ = ["CosignPublicKeyBlobSignatureVerifier"]
