"""Shared protected authority value objects for MSSQL semantic refresh."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

_SHA256_LENGTH = 71


def _digest(value: str) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == _SHA256_LENGTH
        and all(item in "0123456789abcdef" for item in value[7:])
    )


@dataclass(frozen=True, order=True, slots=True)
class MssqlProtectedWritableColumn:
    """One ordered writable SQL Server/ClickHouse column contract."""

    name: str
    source_type: str
    target_type: str
    nullable: bool
    writable_role: str

    def __post_init__(self) -> None:
        if any(not isinstance(value, str) or not value for value in (self.name, self.source_type, self.target_type)):
            raise ValueError("protected writable column fields must be non-empty")
        if not isinstance(self.nullable, bool):
            raise ValueError("protected writable column nullable must be boolean")
        if self.writable_role not in {"EFFECTIVE_KEY", "EFFECTIVE_KEY_EVENT_TIME", "MUTABLE_VALUE"}:
            raise ValueError("protected writable column role is unsupported")


@dataclass(frozen=True, order=True, slots=True)
class MssqlProtectedResourcePolicy:
    """Exact per-model row/image/log limits projected into runtime authority."""

    max_source_scope_rows: int
    max_target_scope_rows: int
    max_before_image_rows: int
    max_before_image_bytes: int
    max_after_image_rows: int
    max_after_image_bytes: int
    max_transaction_log_bytes: int
    min_mssql_log_free_bytes: int
    max_mssql_scope_lock_seconds: int
    max_mssql_version_store_bytes: int
    max_scope_image_total_bytes: int
    max_dbt_temp_rows: int
    max_dbt_temp_bytes: int
    max_statement_seconds: int
    max_clickhouse_staging_bytes: int
    max_clickhouse_shadow_bytes: int
    max_clickhouse_retained_backup_bytes: int
    max_clickhouse_total_transient_bytes: int
    resource_policy_sha256: str

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if field_name == "resource_policy_sha256":
                if not _digest(value):
                    raise ValueError("resource_policy_sha256 is invalid")
            elif isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")


@dataclass(frozen=True, order=True, slots=True)
class MssqlProtectedArtifactAuthority:
    """Protected create-only object-store coordinates and limits."""

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

    def __post_init__(self) -> None:
        if self.provider != "s3":
            raise ValueError("artifact provider must equal s3")
        for field_name in (
            "provider_profile",
            "endpoint_authority_id",
            "bucket_or_container_authority_id",
            "kms_key_authority_id",
            "writer_scope",
            "retention_policy_id",
            "retention_issued_at",
            "retention_until",
        ):
            if not isinstance(getattr(self, field_name), str) or not getattr(self, field_name):
                raise ValueError(f"{field_name} must be non-empty")
        if not self.artifact_prefix or self.artifact_prefix.startswith("/") or ".." in self.artifact_prefix.split("/"):
            raise ValueError("artifact prefix is invalid")
        if any(
            not _digest(value)
            for value in (
                self.capability_evidence_sha256,
                self.encryption_policy_sha256,
                self.retention_policy_sha256,
            )
        ):
            raise ValueError("artifact policy digest is invalid")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in (self.retention_days, self.max_artifact_bytes)
        ):
            raise ValueError("artifact retention and byte limits must be positive")
        issued_at = _utc(self.retention_issued_at, "retention_issued_at")
        retention_until = _utc(self.retention_until, "retention_until")
        if retention_until < issued_at + timedelta(days=self.retention_days):
            raise ValueError("artifact retention window is shorter than protected retention_days")


def _utc(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"{field_name} must be a UTC timestamp") from exc
    if not value.endswith("Z") or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be a UTC timestamp")
    return parsed


def mssql_artifact_retention_authorizes(
    authority: MssqlProtectedArtifactAuthority,
    *,
    as_of: datetime,
) -> bool:
    """Check the immutable issuance window at a trusted runtime instant."""

    if not isinstance(authority, MssqlProtectedArtifactAuthority):
        raise TypeError("artifact authority must be protected and typed")
    if not isinstance(as_of, datetime) or as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
        raise ValueError("as_of must be a timezone-aware UTC datetime")
    issued_at = _utc(authority.retention_issued_at, "retention_issued_at")
    retention_until = _utc(authority.retention_until, "retention_until")
    current = as_of.astimezone(timezone.utc)  # noqa: UP017 - package supports Python 3.10
    return issued_at <= current < retention_until


@dataclass(frozen=True, slots=True)
class MssqlCanonicalAuthorityRecord:
    """One create-only canonical authority document loaded from control state."""

    workflow_execution_binding_sha256: str
    workflow_execution_id: str
    authority_sha256: str
    authority_json: str
    status: str


__all__ = [
    "MssqlCanonicalAuthorityRecord",
    "MssqlProtectedArtifactAuthority",
    "MssqlProtectedResourcePolicy",
    "MssqlProtectedWritableColumn",
    "mssql_artifact_retention_authorizes",
]
