"""Port for reading one exact version-pinned semantic-refresh artifact."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_artifact_manifest import (
        SemanticRefreshSealedArtifactManifest,
    )
    from dpone.contracts.semantic_refresh_seal_authorization import (
        SemanticRefreshSealAuthorizationReceipt,
    )


@dataclass(frozen=True, slots=True)
class VersionPinnedSealedArtifact:
    """Authenticated canonical manifest and exact chunk bytes."""

    manifest: SemanticRefreshSealedArtifactManifest
    chunk_bytes: tuple[bytes, ...]


class SemanticRefreshSealedArtifactReader(Protocol):
    """Read one exact immutable sealed artifact."""

    def read(
        self,
        *,
        manifest_key: str,
        manifest_version: str,
        artifact_manifest_sha256: str,
        operation_id: str,
        operation_plan_sha256: str,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
        seal_authorization: SemanticRefreshSealAuthorizationReceipt,
    ) -> VersionPinnedSealedArtifact:
        """Authenticate and return only the requested immutable versions."""


__all__ = [
    "SemanticRefreshSealedArtifactReader",
    "VersionPinnedSealedArtifact",
]
