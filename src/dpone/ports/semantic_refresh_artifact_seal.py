"""Typed artifact sealing boundary for semantic-refresh publication."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.semantic_refresh_artifact_manifest import (
    SEALED_ARTIFACT_MANIFEST_SCHEMA,
    SemanticRefreshSealedArtifactManifest,
)
from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef

ARTIFACT_FORMAT = "dpone_parquet_v1"
ARTIFACT_MANIFEST_SCHEMA = SEALED_ARTIFACT_MANIFEST_SCHEMA
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class ArtifactChunk:
    """One deterministic Parquet chunk and its logical row count."""

    ordinal: int
    content: bytes
    row_count: int

    def __post_init__(self) -> None:
        if self.ordinal <= 0:
            raise ValueError("artifact chunk ordinal must be positive")
        if not isinstance(self.content, bytes) or not self.content:
            raise ValueError("artifact chunk content must be non-empty bytes")
        if len(self.content) < 8 or not self.content.startswith(b"PAR1") or not self.content.endswith(b"PAR1"):
            raise ValueError("artifact chunk must be a dpone_parquet_v1 Parquet container")
        if self.row_count < 0:
            raise ValueError("artifact chunk row count must be non-negative")

    @property
    def sha256(self) -> str:
        return "sha256:" + sha256(self.content).hexdigest()


@dataclass(frozen=True)
class ArtifactSealPlan:
    """Closed typed authorization and deterministic layout for one seal."""

    seal_authorization: SemanticRefreshSealAuthorizationReceipt
    artifact_prefix: str
    provider: str
    encryption_policy_sha256: str
    retention_policy_sha256: str
    encryption_scope: str
    retention_until: str
    chunks: tuple[ArtifactChunk, ...]
    artifact_format: str = ARTIFACT_FORMAT

    def __post_init__(self) -> None:
        if not isinstance(self.seal_authorization, SemanticRefreshSealAuthorizationReceipt):
            raise TypeError("seal_authorization must be a canonical typed receipt")
        for field_name in ("encryption_policy_sha256", "retention_policy_sha256"):
            if _DIGEST_RE.fullmatch(getattr(self, field_name)) is None:
                raise ValueError(f"{field_name} must be a canonical sha256 digest")
        if self.artifact_format != ARTIFACT_FORMAT:
            raise ValueError("artifact format is not supported")
        if (
            not self.artifact_prefix
            or self.artifact_prefix.startswith("/")
            or self.artifact_prefix.endswith("/")
            or ".." in self.artifact_prefix.split("/")
        ):
            raise ValueError("artifact prefix must be a safe relative object prefix")
        if self.artifact_prefix.split("/")[-1] != self.operation_id:
            raise ValueError("artifact prefix must be operation-scoped to the exact operation_id")
        if not self.provider.strip():
            raise ValueError("artifact provider must be non-empty")
        if not self.encryption_scope.strip():
            raise ValueError("artifact encryption scope must be non-empty")
        try:
            retention = datetime.fromisoformat(self.retention_until.replace("Z", "+00:00"))
        except ValueError:
            retention = None
        if retention is None or retention.tzinfo is None:
            raise ValueError("artifact retention_until must be timezone-aware")
        ordinals = tuple(chunk.ordinal for chunk in self.chunks)
        if ordinals != tuple(range(1, len(self.chunks) + 1)):
            raise ValueError("artifact chunks must use contiguous deterministic ordinals from one")
        if sum(chunk.row_count for chunk in self.chunks) != self.seal_authorization.after_image_row_count:
            raise ValueError("artifact chunk rows must equal the authorized committed after-image row count")

    @property
    def operation_id(self) -> str:
        return self.seal_authorization.operation_id

    @property
    def operation_plan_sha256(self) -> str:
        return self.seal_authorization.operation_plan_sha256

    @property
    def workflow_execution_id(self) -> str:
        return self.seal_authorization.workflow_execution_id

    @property
    def workflow_execution_binding_sha256(self) -> str:
        return self.seal_authorization.workflow_execution_binding_sha256

    @property
    def attempt_binding_sha256(self) -> str:
        return self.seal_authorization.attempt_binding_sha256

    @property
    def serializer_sha256(self) -> str:
        return self.seal_authorization.serializer_sha256

    @property
    def effective_key_mapping_sha256(self) -> str:
        return self.seal_authorization.effective_key_mapping_sha256

    @property
    def route_certification_receipt_sha256(self) -> str:
        return self.seal_authorization.route_certification_receipt_sha256

    def authority_inventory(self) -> dict[str, Any]:
        """Return the closed source-authorized bytes/inventory/policy projection."""

        return {
            "seal_authorization": self.seal_authorization.to_dict(),
            "artifact_prefix": self.artifact_prefix,
            "artifact_format": self.artifact_format,
            "provider": self.provider,
            "encryption_policy_sha256": self.encryption_policy_sha256,
            "retention_policy_sha256": self.retention_policy_sha256,
            "encryption_scope": self.encryption_scope,
            "retention_until": self.retention_until,
            "total_rows": sum(chunk.row_count for chunk in self.chunks),
            "total_bytes": sum(len(chunk.content) for chunk in self.chunks),
            "chunks": [
                {
                    "ordinal": chunk.ordinal,
                    "sha256": chunk.sha256,
                    "size_bytes": len(chunk.content),
                    "row_count": chunk.row_count,
                }
                for chunk in self.chunks
            ],
        }


@dataclass(frozen=True)
class SealedArtifactReceipt:
    """Version-pinned receipt returned only after exact manifest publication."""

    operation_id: str
    status: str
    manifest: ArtifactObjectRef
    chunks: tuple[ArtifactObjectRef, ...]
    manifest_payload: dict[str, object]
    sealed_manifest: SemanticRefreshSealedArtifactManifest


class SemanticRefreshSealAuthorizationPort(Protocol):
    """Load one authenticated MSSQL seal receipt by immutable run identity."""

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> SemanticRefreshSealAuthorizationReceipt:
        """Return one canonical receipt or fail closed."""


class SemanticRefreshSealAuthorizationStorePort(SemanticRefreshSealAuthorizationPort, Protocol):
    """Create-once durable authority used by the issuer and the sealer."""

    def persist_exact(self, receipt: SemanticRefreshSealAuthorizationReceipt) -> None:
        """Create the receipt or acknowledge only canonical byte-equivalent replay."""


__all__ = [
    "ARTIFACT_FORMAT",
    "ARTIFACT_MANIFEST_SCHEMA",
    "ArtifactChunk",
    "ArtifactSealPlan",
    "SealedArtifactReceipt",
    "SemanticRefreshSealAuthorizationPort",
    "SemanticRefreshSealAuthorizationStorePort",
]
