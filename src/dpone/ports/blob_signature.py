"""Infrastructure boundary for exact-byte signature verification."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.blob_signature import (
        BlobSignatureVerification,
        BlobVerificationCommandResult,
        CosignVerificationPolicy,
    )


class BlobVerificationCommandRunner(Protocol):
    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> BlobVerificationCommandResult: ...


class BlobSignatureVerifier(Protocol):
    def verify_blob(
        self,
        *,
        blob: bytes,
        sigstore_bundle: bytes,
        trusted_root: bytes,
        policy: CosignVerificationPolicy,
    ) -> BlobSignatureVerification: ...


__all__ = ["BlobSignatureVerifier", "BlobVerificationCommandRunner"]
