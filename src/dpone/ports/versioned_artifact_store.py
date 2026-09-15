"""Bounded versioned artifact capabilities; clients and clocks are injected."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef


@dataclass(frozen=True, slots=True)
class VersionedArtifactIoBudget:
    """One operation's byte allowance and absolute monotonic deadline.

    SDK connect/read/retry limits are additional composition obligations; this
    cooperative deadline does not interrupt an already blocking SDK operation.
    """

    max_bytes: int
    chunk_bytes: int
    deadline_monotonic: float

    def __post_init__(self) -> None:
        for value in (self.max_bytes, self.chunk_bytes):
            if type(value) is not int or value <= 0:
                raise ValueError("artifact byte budgets must be exact positive integers")
        if self.chunk_bytes > self.max_bytes:
            raise ValueError("artifact chunk_bytes exceeds max_bytes")
        if type(self.deadline_monotonic) is not float or not math.isfinite(self.deadline_monotonic):
            raise ValueError("artifact deadline must be a finite float")


class S3VersionedArtifactPolicy(Protocol):
    """Protected capability coordinates for shared create-only S3 operations."""

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

    @property
    def conditional_create_authorized(self) -> bool: ...

    @property
    def require_object_lock(self) -> bool: ...

    @property
    def object_lock_mode(self) -> str | None: ...


class S3ClientMetadata(Protocol):
    @property
    def endpoint_url(self) -> str: ...


class S3ObjectBody(Protocol):
    """SDK response body whose read enforces framing or raises on truncation."""

    def read(self, size: int) -> bytes: ...

    def close(self) -> None: ...


class S3VersionedObjectClient(Protocol):
    """Already configured client; methods must use finite SDK wait/retry limits."""

    @property
    def meta(self) -> S3ClientMetadata: ...

    def put_object(self, **kwargs: object) -> Mapping[str, object]: ...
    def head_object(self, **kwargs: object) -> Mapping[str, object]: ...
    def get_object(self, **kwargs: object) -> Mapping[str, object]: ...
    def list_object_versions(self, **kwargs: object) -> Mapping[str, object]: ...
    def get_bucket_versioning(self, **kwargs: object) -> Mapping[str, object]: ...
    def get_object_lock_configuration(self, **kwargs: object) -> Mapping[str, object]: ...


class BoundedVersionedArtifactStore(Protocol):
    """Immutable object operations sharing one explicit caller-owned budget."""

    def create(
        self,
        *,
        key: str,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
        budget: VersionedArtifactIoBudget,
    ) -> ArtifactObjectRef: ...

    def head(self, *, key: str, budget: VersionedArtifactIoBudget) -> ArtifactObjectRef | None: ...

    def read_version(self, *, key: str, version: str, budget: VersionedArtifactIoBudget) -> bytes: ...


# Preserve the historical protocol identity for legacy imports and pickles.
S3VersionedArtifactPolicy.__module__ = "dpone.ports.semantic_refresh_s3_policy"
S3VersionedArtifactPolicy.__name__ = "SemanticRefreshS3ArtifactStorePolicy"
S3VersionedArtifactPolicy.__qualname__ = "SemanticRefreshS3ArtifactStorePolicy"
