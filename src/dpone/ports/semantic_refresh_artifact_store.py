"""Ports for immutable semantic-refresh artifact publication.

The runtime owns sealing policy. Infrastructure implementations provide only
atomic create-if-absent, exact-version reads, and operation authorization.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class ArtifactCreateConflict(RuntimeError):
    """Raised when a create-only object key already exists."""


class ArtifactStoreUnavailable(RuntimeError):
    """Raised when an object-store outcome requires reconciliation."""


@dataclass(frozen=True, slots=True)
class ArtifactStoreBinding:
    """Exact protected provider/policy coordinates for one operation prefix."""

    provider: str
    provider_profile: str
    endpoint_authority_id: str
    bucket_or_container_authority_id: str
    kms_key_authority_id: str
    capability_evidence_sha256: str
    writer_scope: str
    artifact_prefix: str
    encryption_policy_sha256: str
    retention_policy_id: str
    retention_policy_sha256: str
    retention_days: int
    retention_issued_at: str
    retention_until: str
    max_artifact_bytes: int


class SemanticRefreshArtifactStoreAuthority(Protocol):
    """Structural protected authority used to derive an exact store binding."""

    @property
    def provider(self) -> str: ...

    @property
    def provider_profile(self) -> str: ...

    @property
    def endpoint_authority_id(self) -> str: ...

    @property
    def bucket_or_container_authority_id(self) -> str: ...

    @property
    def kms_key_authority_id(self) -> str: ...

    @property
    def capability_evidence_sha256(self) -> str: ...

    @property
    def writer_scope(self) -> str: ...

    @property
    def artifact_prefix(self) -> str: ...

    @property
    def encryption_policy_sha256(self) -> str: ...

    @property
    def retention_policy_id(self) -> str: ...

    @property
    def retention_policy_sha256(self) -> str: ...

    @property
    def retention_days(self) -> int: ...

    @property
    def retention_issued_at(self) -> str: ...

    @property
    def retention_until(self) -> str: ...

    @property
    def max_artifact_bytes(self) -> int: ...


@dataclass(frozen=True)
class ArtifactObjectRef:
    """Provider-pinned identity for one immutable object version."""

    key: str
    version: str
    size_bytes: int
    sha256: str
    encryption_scope: str
    retention_until: str


class CreateOnlyVersionedArtifactStore(Protocol):
    """Minimal versioned store used by the manifest-last sealing service."""

    def create(
        self,
        *,
        key: str,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
    ) -> ArtifactObjectRef:
        """Atomically create ``key`` or raise ``ArtifactCreateConflict``."""

    def head(self, *, key: str) -> ArtifactObjectRef | None:
        """Return the sole create-only version currently bound to ``key``."""

    def read_version(self, *, key: str, version: str) -> bytes:
        """Read exact bytes by provider version/generation, never by latest."""


class SemanticRefreshArtifactStoreResolver(Protocol):
    """Resolve one store that is exactly bound to protected operation policy."""

    def resolve(self, binding: ArtifactStoreBinding) -> CreateOnlyVersionedArtifactStore:
        """Return an exact operation-scoped store or fail before object I/O."""


def artifact_store_binding(authority: SemanticRefreshArtifactStoreAuthority) -> ArtifactStoreBinding:
    """Project the closed provider coordinates authorized for one operation."""

    return ArtifactStoreBinding(
        provider=authority.provider,
        provider_profile=authority.provider_profile,
        endpoint_authority_id=authority.endpoint_authority_id,
        bucket_or_container_authority_id=authority.bucket_or_container_authority_id,
        kms_key_authority_id=authority.kms_key_authority_id,
        capability_evidence_sha256=authority.capability_evidence_sha256,
        writer_scope=authority.writer_scope,
        artifact_prefix=authority.artifact_prefix,
        encryption_policy_sha256=authority.encryption_policy_sha256,
        retention_policy_id=authority.retention_policy_id,
        retention_policy_sha256=authority.retention_policy_sha256,
        retention_days=authority.retention_days,
        retention_issued_at=authority.retention_issued_at,
        retention_until=authority.retention_until,
        max_artifact_bytes=authority.max_artifact_bytes,
    )


def operation_artifact_prefix(base_prefix: str, operation_id: str) -> str:
    """Derive the sole operation-scoped prefix from protected deployment policy."""

    if (
        not isinstance(base_prefix, str)
        or not base_prefix
        or base_prefix.startswith("/")
        or base_prefix.endswith("/")
        or ".." in base_prefix.split("/")
        or _DIGEST_RE.fullmatch(operation_id) is None
    ):
        raise ValueError("artifact base prefix or operation identity is invalid")
    return f"{base_prefix}/{operation_id}"


class SemanticRefreshArtifactAuthority(Protocol):
    """Authorize sealing against independent durable journal/source evidence."""

    def assert_authorized(
        self,
        *,
        inventory: Mapping[str, object],
    ) -> None:
        """Raise before I/O unless the exact inventory and policies are authorized."""


__all__ = [
    "ArtifactCreateConflict",
    "ArtifactObjectRef",
    "ArtifactStoreBinding",
    "ArtifactStoreUnavailable",
    "CreateOnlyVersionedArtifactStore",
    "SemanticRefreshArtifactStoreAuthority",
    "SemanticRefreshArtifactStoreResolver",
    "SemanticRefreshArtifactAuthority",
    "artifact_store_binding",
    "operation_artifact_prefix",
]
