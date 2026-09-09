"""Closed writable, resource, and artifact policy for semantic-refresh plans."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from urllib.parse import urlsplit

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_effective_key_identity import EffectiveKeyTemplateColumn
from dpone.contracts.semantic_refresh_evidence_common import parse_utc_timestamp, require_utc_timestamp

RESOURCE_POLICY_SCHEMA = "dpone.dbt-semantic-refresh-resource-policy.v1"
_PRODUCTION_ARTIFACT_PROFILE = "s3_create_only_versioned_kms_object_lock_v1"
_AWS_KMS_KEY_ARN = re.compile(r"arn:aws(?:-[a-z0-9-]+)?:kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+")


class SemanticRefreshWritableRole(str, Enum):  # noqa: UP042
    """Closed SQL Server mutation role for one ordered contract column."""

    EFFECTIVE_KEY = "EFFECTIVE_KEY"
    EFFECTIVE_KEY_EVENT_TIME = "EFFECTIVE_KEY_EVENT_TIME"
    MUTABLE_VALUE = "MUTABLE_VALUE"


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshWritableColumn:
    """One exact ordered SQL Server-to-ClickHouse writable column."""

    name: str
    source_type: str
    target_type: str
    nullable: bool
    writable_role: SemanticRefreshWritableRole

    def __post_init__(self) -> None:
        require_text(self.name, "writable column name")
        require_text(self.source_type, "writable column source_type")
        require_text(self.target_type, "writable column target_type")
        if not isinstance(self.nullable, bool):
            raise SemanticRefreshContractError("writable column nullable must be boolean")
        if not isinstance(self.writable_role, SemanticRefreshWritableRole):
            raise SemanticRefreshContractError("writable column role is unsupported")
        if self.writable_role is not SemanticRefreshWritableRole.MUTABLE_VALUE and self.nullable:
            raise SemanticRefreshContractError("effective-key writable columns must be non-null")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "nullable": self.nullable,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "writable_role": self.writable_role.value,
        }


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshResourcePolicy:
    """Required bounded SQL Server, dbt-temp, and publication budgets."""

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
    resource_policy_sha256: str = field(init=False, compare=True)
    schema: str = field(default=RESOURCE_POLICY_SCHEMA, init=False, compare=True)

    def __post_init__(self) -> None:
        values = {name: require_positive_int(getattr(self, name), name) for name in _RESOURCE_FIELDS}
        object.__setattr__(
            self,
            "resource_policy_sha256",
            semantic_refresh_sha256({"schema": RESOURCE_POLICY_SCHEMA, **values}),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            **{name: getattr(self, name) for name in sorted(_RESOURCE_FIELDS)},
            "resource_policy_sha256": self.resource_policy_sha256,
            "schema": self.schema,
        }


_RESOURCE_FIELDS = (
    "max_source_scope_rows",
    "max_target_scope_rows",
    "max_before_image_rows",
    "max_before_image_bytes",
    "max_after_image_rows",
    "max_after_image_bytes",
    "max_transaction_log_bytes",
    "min_mssql_log_free_bytes",
    "max_mssql_scope_lock_seconds",
    "max_mssql_version_store_bytes",
    "max_scope_image_total_bytes",
    "max_dbt_temp_rows",
    "max_dbt_temp_bytes",
    "max_statement_seconds",
    "max_clickhouse_staging_bytes",
    "max_clickhouse_shadow_bytes",
    "max_clickhouse_retained_backup_bytes",
    "max_clickhouse_total_transient_bytes",
)


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshArtifactAuthority:
    """Protected create-only object-store authority for one model artifact set."""

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
            raise SemanticRefreshContractError("semantic-refresh artifact provider must equal s3")
        for field_name in (
            "provider_profile",
            "endpoint_authority_id",
            "bucket_or_container_authority_id",
            "kms_key_authority_id",
            "writer_scope",
            "retention_policy_id",
        ):
            require_text(getattr(self, field_name), field_name)
        if self.provider_profile == _PRODUCTION_ARTIFACT_PROFILE:
            _require_production_s3_endpoint_authority(self.endpoint_authority_id)
            if _AWS_KMS_KEY_ARN.fullmatch(self.kms_key_authority_id) is None:
                raise SemanticRefreshContractError("production artifact authority requires a full AWS KMS key ARN")
        require_text(self.artifact_prefix, "artifact_prefix")
        if self.artifact_prefix.startswith("/") or ".." in self.artifact_prefix.split("/"):
            raise SemanticRefreshContractError("artifact_prefix must be relative and confined")
        require_digest(self.encryption_policy_sha256, "encryption_policy_sha256")
        require_digest(self.capability_evidence_sha256, "capability_evidence_sha256")
        require_digest(self.retention_policy_sha256, "retention_policy_sha256")
        require_positive_int(self.retention_days, "retention_days")
        issued_at = parse_utc_timestamp(self.retention_issued_at, "retention_issued_at")
        require_utc_timestamp(self.retention_until, "retention_until")
        if parse_utc_timestamp(self.retention_until, "retention_until") < issued_at + timedelta(
            days=self.retention_days
        ):
            raise SemanticRefreshContractError(
                "retention_until must cover retention_days from the immutable issuance time"
            )
        require_positive_int(self.max_artifact_bytes, "max_artifact_bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_prefix": self.artifact_prefix,
            "bucket_or_container_authority_id": self.bucket_or_container_authority_id,
            "capability_evidence_sha256": self.capability_evidence_sha256,
            "endpoint_authority_id": self.endpoint_authority_id,
            "encryption_policy_sha256": self.encryption_policy_sha256,
            "kms_key_authority_id": self.kms_key_authority_id,
            "max_artifact_bytes": self.max_artifact_bytes,
            "provider": self.provider,
            "provider_profile": self.provider_profile,
            "retention_days": self.retention_days,
            "retention_issued_at": self.retention_issued_at,
            "retention_policy_id": self.retention_policy_id,
            "retention_policy_sha256": self.retention_policy_sha256,
            "retention_until": self.retention_until,
            "writer_scope": self.writer_scope,
        }


def _require_production_s3_endpoint_authority(value: str) -> None:
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise SemanticRefreshContractError(
            "production artifact authority requires a normalized HTTPS S3 endpoint"
        ) from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise SemanticRefreshContractError("production artifact authority requires a normalized HTTPS S3 endpoint")
    canonical = f"https://{parsed.hostname.lower()}"
    if port is not None:
        canonical = f"{canonical}:{port}"
    if value != canonical:
        raise SemanticRefreshContractError("production artifact authority requires a normalized HTTPS S3 endpoint")


def validate_writable_columns(
    values: tuple[SemanticRefreshWritableColumn, ...],
    *,
    effective_keys: tuple[EffectiveKeyTemplateColumn, ...],
    event_time_column: str,
) -> None:
    """Require one ordered writable schema consistent with effective-key policy."""

    if not values or any(not isinstance(item, SemanticRefreshWritableColumn) for item in values):
        raise SemanticRefreshContractError("writable_columns must be a non-empty typed tuple")
    names = tuple(item.name for item in values)
    if len(names) != len(set(names)):
        raise SemanticRefreshContractError("writable_columns must have unique ordered names")
    keys = {item.name: item for item in effective_keys}
    if not set(keys).issubset(names):
        raise SemanticRefreshContractError("effective key is absent from writable columns")
    for column in values:
        key = keys.get(column.name)
        role = _expected_role(column.name, event_time_column, key is not None)
        if column.writable_role is not role:
            raise SemanticRefreshContractError("writable column role differs from effective-key policy")
        if key is not None and (column.source_type, column.target_type) != (key.source_type, key.target_type):
            raise SemanticRefreshContractError("writable effective-key types differ from certified mapping")


def _expected_role(name: str, event_time: str, is_key: bool) -> SemanticRefreshWritableRole:
    if name == event_time:
        return SemanticRefreshWritableRole.EFFECTIVE_KEY_EVENT_TIME
    return SemanticRefreshWritableRole.EFFECTIVE_KEY if is_key else SemanticRefreshWritableRole.MUTABLE_VALUE


__all__ = [
    "RESOURCE_POLICY_SCHEMA",
    "SemanticRefreshArtifactAuthority",
    "SemanticRefreshResourcePolicy",
    "SemanticRefreshWritableColumn",
    "SemanticRefreshWritableRole",
    "validate_writable_columns",
]
