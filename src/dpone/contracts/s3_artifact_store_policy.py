"""Reusable S3 artifact policy invariants without provider or runtime imports."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_KMS_KEY_ARN_RE = re.compile(r"^arn:(aws|aws-us-gov|aws-cn):kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$")
_MINIO_KMS_KEY_ARN_RE = re.compile(r"^arn:aws:kms:[A-Za-z0-9._/-]+$")
_PRODUCTION_PROFILE = "s3_create_only_versioned_kms_object_lock_v1"
_PROVIDER_PROFILES = frozenset({_PRODUCTION_PROFILE, "MINIO_LOCAL_UNVERIFIED"})
_LOCK_MODES = frozenset({"GOVERNANCE", "COMPLIANCE"})
_UTC = timezone.utc  # noqa: UP017 - datetime.UTC is absent from the supported Python 3.10 API


@dataclass(frozen=True)
class S3ArtifactStorePolicy:
    """Protected provider coordinates, capabilities, and artifact limits.

    The historical module identifier preserves pickle compatibility in both
    directions; the resolver reexports this exact canonical class object.
    """

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


# Postponed dataclass annotations must resolve against this canonical module
# during decoration. Restore the historical pickle identifier only afterwards.
S3ArtifactStorePolicy.__module__ = "dpone.adapters.semantic_refresh_artifact_s3_resolver"

__all__ = ["S3ArtifactStorePolicy"]
