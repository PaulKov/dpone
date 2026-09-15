"""Operation-authority-bound S3 artifact store resolution."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from dpone.adapters.semantic_refresh_artifact_s3 import S3CreateOnlyArtifactStore
from dpone.contracts.s3_artifact_store_policy import S3ArtifactStorePolicy


class S3CreateOnlyArtifactStoreResolver:
    """Build one exact operation-scoped S3 store from protected authority."""

    def __init__(
        self,
        *,
        client: Any,
        provider_policy: S3ArtifactStorePolicy,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if (
            provider_policy.conditional_create_authorized is not True
            or provider_policy.require_object_lock is not True
            or provider_policy.object_lock_mode != "COMPLIANCE"
        ):
            raise ValueError("semantic-refresh V2 resolver requires COMPLIANCE create-only policy")
        self._client = client
        self._provider_policy = provider_policy
        self._clock = clock

    def resolve(self, binding: object) -> S3CreateOnlyArtifactStore:
        """Validate provider coordinates and return an exact protected store."""

        required = (
            "provider",
            "provider_profile",
            "endpoint_authority_id",
            "bucket_or_container_authority_id",
            "kms_key_authority_id",
            "capability_evidence_sha256",
            "writer_scope",
            "artifact_prefix",
            "encryption_policy_sha256",
            "retention_policy_id",
            "retention_policy_sha256",
            "retention_days",
            "retention_issued_at",
            "retention_until",
            "max_artifact_bytes",
        )
        values = {field_name: getattr(binding, field_name, None) for field_name in required}
        if values["provider"] != "s3":
            raise ValueError("semantic-refresh S3 resolver requires provider s3")
        expected_provider = (
            *(values[field_name] for field_name in required[1:6]),
            values["writer_scope"],
            *(values[field_name] for field_name in required[8:11]),
            values["retention_days"],
            values["retention_issued_at"],
            values["retention_until"],
            values["max_artifact_bytes"],
        )
        configured_provider = (
            self._provider_policy.provider_profile,
            self._provider_policy.endpoint_authority_id,
            self._provider_policy.bucket_or_container_authority_id,
            self._provider_policy.kms_key_authority_id,
            self._provider_policy.capability_evidence_sha256,
            self._provider_policy.writer_scope,
            self._provider_policy.encryption_policy_sha256,
            self._provider_policy.retention_policy_id,
            self._provider_policy.retention_policy_sha256,
            self._provider_policy.retention_days,
            self._provider_policy.retention_issued_at,
            self._provider_policy.retention_until,
            self._provider_policy.max_artifact_bytes,
        )
        if configured_provider != expected_provider:
            raise ValueError("configured S3 provider policy differs from protected operation authority")
        policy = S3ArtifactStorePolicy(
            provider_profile=str(values["provider_profile"]),
            endpoint_authority_id=str(values["endpoint_authority_id"]),
            bucket_or_container_authority_id=str(values["bucket_or_container_authority_id"]),
            kms_key_authority_id=str(values["kms_key_authority_id"]),
            capability_evidence_sha256=str(values["capability_evidence_sha256"]),
            writer_scope=str(values["writer_scope"]),
            artifact_prefix=str(values["artifact_prefix"]),
            encryption_policy_sha256=str(values["encryption_policy_sha256"]),
            retention_policy_id=str(values["retention_policy_id"]),
            retention_policy_sha256=str(values["retention_policy_sha256"]),
            retention_days=_positive_int(values["retention_days"], "retention_days"),
            retention_issued_at=str(values["retention_issued_at"]),
            retention_until=str(values["retention_until"]),
            max_artifact_bytes=_positive_int(values["max_artifact_bytes"], "max_artifact_bytes"),
            conditional_create_authorized=True,
            require_object_lock=True,
            object_lock_mode="COMPLIANCE",
        )
        return S3CreateOnlyArtifactStore(
            client=self._client,
            bucket=str(values["bucket_or_container_authority_id"]),
            operation_prefix=str(values["artifact_prefix"]),
            policy=policy,
            clock=self._clock,
        )


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"S3 {field_name} must be positive")
    return value


__all__ = ["S3ArtifactStorePolicy", "S3CreateOnlyArtifactStoreResolver"]
