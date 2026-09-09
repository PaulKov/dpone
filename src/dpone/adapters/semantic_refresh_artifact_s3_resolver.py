"""Operation-authority-bound S3 artifact store resolution."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from dpone.adapters.semantic_refresh_artifact_s3 import S3CreateOnlyArtifactStore

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_KMS_KEY_ARN_RE = re.compile(r"^arn:(aws|aws-us-gov|aws-cn):kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$")
_MINIO_KMS_KEY_ARN_RE = re.compile(r"^arn:aws:kms:[A-Za-z0-9._/-]+$")
_PRODUCTION_PROFILE = "s3_create_only_versioned_kms_object_lock_v1"
_PROVIDER_PROFILES = frozenset({_PRODUCTION_PROFILE, "MINIO_LOCAL_UNVERIFIED"})
_LOCK_MODES = frozenset({"GOVERNANCE", "COMPLIANCE"})
_UTC = timezone.utc  # noqa: UP017 - datetime.UTC is absent from the supported Python 3.10 API


@dataclass(frozen=True)
class S3ArtifactStorePolicy:
    """Protected provider coordinates, capabilities, and artifact limits."""

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
    conditional_create_authorized: bool
    require_object_lock: bool
    object_lock_mode: str | None = None

    def __post_init__(self) -> None:
        if self.provider_profile not in _PROVIDER_PROFILES:
            raise ValueError("S3 provider profile is unsupported")
        key_pattern = _KMS_KEY_ARN_RE if self.provider_profile == _PRODUCTION_PROFILE else _MINIO_KMS_KEY_ARN_RE
        if not isinstance(self.kms_key_authority_id, str) or key_pattern.fullmatch(self.kms_key_authority_id) is None:
            raise ValueError("S3 KMS key authority differs from the provider profile")
        for field_name in (
            "endpoint_authority_id",
            "bucket_or_container_authority_id",
            "writer_scope",
            "retention_policy_id",
        ):
            if not isinstance(getattr(self, field_name), str) or not getattr(self, field_name).strip():
                raise ValueError(f"S3 {field_name} must be non-empty")
        for field_name in (
            "capability_evidence_sha256",
            "encryption_policy_sha256",
            "retention_policy_sha256",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
                raise ValueError(f"S3 {field_name} must be a canonical digest")
        if (
            not self.artifact_prefix
            or self.artifact_prefix.startswith("/")
            or self.artifact_prefix.endswith("/")
            or ".." in self.artifact_prefix.split("/")
        ):
            raise ValueError("S3 artifact_prefix is invalid")
        _require_positive_int(self.retention_days, "retention_days")
        _require_positive_int(self.max_artifact_bytes, "max_artifact_bytes")
        issued_at = _utc_timestamp(self.retention_issued_at, "retention_issued_at")
        retention_until = _utc_timestamp(self.retention_until, "retention_until")
        if retention_until < issued_at + timedelta(days=self.retention_days):
            raise ValueError("S3 retention authority is shorter than retention_days")
        if not isinstance(self.conditional_create_authorized, bool):
            raise TypeError("S3 conditional-create authority must be boolean")
        if not isinstance(self.require_object_lock, bool):
            raise TypeError("S3 object-lock requirement must be boolean")
        if self.require_object_lock and self.object_lock_mode not in _LOCK_MODES:
            raise ValueError("required S3 object lock mode is unsupported")
        if not self.require_object_lock and self.object_lock_mode is not None:
            raise ValueError("S3 object lock mode requires object-lock enforcement")


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


def _require_positive_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"S3 {field_name} must be positive")


def _utc_timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"S3 {field_name} must be canonical UTC")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"S3 {field_name} must be canonical UTC") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"S3 {field_name} must be canonical UTC")
    return parsed.astimezone(_UTC)


__all__ = ["S3ArtifactStorePolicy", "S3CreateOnlyArtifactStoreResolver"]
