"""Compatibility API for the shared create-only S3 artifact implementation."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from dpone.adapters.versioned_artifact_s3 import S3ArtifactOperations
from dpone.adapters.versioned_artifact_s3_proof import retention_datetime as retention_datetime
from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef
from dpone.ports.semantic_refresh_s3_policy import SemanticRefreshS3ArtifactStorePolicy

_UTC = timezone.utc  # noqa: UP017 - supported Python 3.10


class S3CreateOnlyArtifactStore:
    """Preserve legacy signatures and GET identity fallback without native claims."""

    def __init__(
        self,
        *,
        client: Any,
        bucket: str,
        operation_prefix: str,
        policy: SemanticRefreshS3ArtifactStorePolicy,
        encryption_algorithm: str = "aws:kms",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._operations = S3ArtifactOperations(
            client=client,
            bucket=bucket,
            operation_prefix=operation_prefix,
            policy=policy,
            encryption_algorithm=encryption_algorithm,
            clock=clock or (lambda: datetime.now(_UTC)),
            monotonic_clock=monotonic,
        )

    def create(
        self, *, key: str, content: bytes, sha256: str, encryption_scope: str, retention_until: str
    ) -> ArtifactObjectRef:
        return self._operations.create(
            key=key, content=content, sha256=sha256, encryption_scope=encryption_scope, retention_until=retention_until
        )

    def head(self, *, key: str) -> ArtifactObjectRef | None:
        return self._operations.head(key=key)

    def read_version(self, *, key: str, version: str) -> bytes:
        return self._operations.read_version(key=key, version=version)


__all__ = ["S3CreateOnlyArtifactStore"]
