"""Capability port for verifying one pinned runtime artifact subject."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.runtime_artifact_attestation import (
        RuntimeArtifactAttestationSubject,
    )


class RuntimeArtifactAttestationVerifier(Protocol):
    """Verify one exact release-set without depending on runtime internals."""

    def verify(self, *, subject: RuntimeArtifactAttestationSubject) -> None: ...


__all__ = ["RuntimeArtifactAttestationVerifier"]
