"""Capability policy consumed by the semantic-refresh S3 artifact adapter."""

from __future__ import annotations

from typing import Protocol


class SemanticRefreshS3ArtifactStorePolicy(Protocol):
    """Closed protected storage policy required by create-only S3 writes."""

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
