"""Protected canonical target/scope authority for ClickHouse publication."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Protocol

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RELATION_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")


@dataclass(frozen=True)
class ClickHousePublicationAuthority:
    """Immutable operation-to-target/scope/head authorization projection."""

    authority_sha256: str
    workflow_execution_id: str
    operation_id: str
    operation_plan_sha256: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    owner_id: str
    guard_resource_id: str
    guard_status: str
    target_resource_id: str
    target_authority_id: str
    clickhouse_cluster_authority_id: str
    database: str
    target_table: str
    scope_id: str
    scope_start: str
    scope_end: str
    scope_revision: int
    target_predecessor_generation_id: str
    scope_predecessor_operation_id: str | None
    predecessor_target_generation: int
    predecessor_target_uuid: str
    predecessor_target_operation_id: str
    predecessor_scope_revision: int | None
    predecessor_checkpoint_sha256: str | None
    predecessor_checkpoint_operation_id: str | None
    predecessor_checkpoint_version: int | None
    expected_target_uuid: str
    expected_schema_sha256: str
    expected_physical_sha256: str
    business_columns: tuple[str, ...]
    effective_key_columns: tuple[str, ...]
    event_time_column: str
    effective_key_mapping_sha256: str
    route_certification_receipt_sha256: str
    artifact_prefix: str
    artifact_provider: str
    encryption_policy_sha256: str
    retention_policy_sha256: str
    max_artifact_bytes: int
    max_staging_rows: int
    max_target_scope_rows: int
    max_staging_bytes: int
    max_shadow_bytes: int
    max_retained_backup_bytes: int
    max_total_transient_bytes: int

    def __post_init__(self) -> None:
        for field_name in (
            "authority_sha256",
            "operation_plan_sha256",
            "workflow_plan_sha256",
            "workflow_execution_binding_sha256",
            "attempt_binding_sha256",
            "target_predecessor_generation_id",
            "expected_schema_sha256",
            "expected_physical_sha256",
            "effective_key_mapping_sha256",
            "route_certification_receipt_sha256",
            "encryption_policy_sha256",
            "retention_policy_sha256",
        ):
            if _DIGEST_RE.fullmatch(getattr(self, field_name)) is None:
                raise ValueError(f"ClickHouse publication {field_name} is invalid")
        for field_name in (
            "workflow_execution_id",
            "operation_id",
            "owner_id",
            "guard_resource_id",
            "target_resource_id",
            "target_authority_id",
            "clickhouse_cluster_authority_id",
            "scope_id",
            "scope_start",
            "scope_end",
            "artifact_prefix",
            "artifact_provider",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"ClickHouse publication {field_name} is empty")
        if _IDENTIFIER_RE.fullmatch(self.database) is None:
            raise ValueError("ClickHouse publication database is invalid")
        if _RELATION_IDENTIFIER_RE.fullmatch(self.target_table) is None:
            raise ValueError("ClickHouse publication target_table is invalid")
        if self.target_authority_id != (
            f"clickhouse://{self.clickhouse_cluster_authority_id}/{self.database}/{self.target_table}"
        ):
            raise ValueError("ClickHouse target authority differs from publication relation")
        if self.guard_resource_id != self.target_resource_id or self.guard_status != "HELD":
            raise ValueError("ClickHouse publication guard authority is invalid")
        if isinstance(self.fencing_epoch, bool) or not isinstance(self.fencing_epoch, int) or self.fencing_epoch <= 0:
            raise ValueError("ClickHouse publication fencing_epoch must be positive")
        if (
            isinstance(self.scope_revision, bool)
            or not isinstance(self.scope_revision, int)
            or self.scope_revision <= 0
        ):
            raise ValueError("ClickHouse publication scope_revision must be positive")
        if (
            self.scope_predecessor_operation_id is not None
            and _DIGEST_RE.fullmatch(self.scope_predecessor_operation_id) is None
        ):
            raise ValueError("ClickHouse scope predecessor operation is invalid")
        if (
            isinstance(self.predecessor_target_generation, bool)
            or not isinstance(self.predecessor_target_generation, int)
            or self.predecessor_target_generation < 0
        ):
            raise ValueError("ClickHouse predecessor target generation is invalid")
        if _DIGEST_RE.fullmatch(self.predecessor_target_operation_id) is None:
            raise ValueError("ClickHouse predecessor target operation is invalid")
        if (
            not _canonical_uuid(self.predecessor_target_uuid)
            or self.expected_target_uuid != self.predecessor_target_uuid
        ):
            raise ValueError("ClickHouse expected target UUID is invalid")
        checkpoint_values = (
            self.predecessor_checkpoint_sha256,
            self.predecessor_checkpoint_operation_id,
            self.predecessor_checkpoint_version,
        )
        if self.scope_predecessor_operation_id is None:
            if self.predecessor_scope_revision is not None or any(value is not None for value in checkpoint_values):
                raise ValueError("first-scope publication predecessors must be absent")
        elif (
            isinstance(self.predecessor_scope_revision, bool)
            or not isinstance(self.predecessor_scope_revision, int)
            or self.predecessor_scope_revision <= 0
            or not all(value is not None for value in checkpoint_values)
            or _DIGEST_RE.fullmatch(str(self.predecessor_checkpoint_sha256)) is None
            or _DIGEST_RE.fullmatch(str(self.predecessor_checkpoint_operation_id)) is None
            or isinstance(self.predecessor_checkpoint_version, bool)
            or not isinstance(self.predecessor_checkpoint_version, int)
            or self.predecessor_checkpoint_version <= 0
        ):
            raise ValueError("existing-scope publication predecessors are incomplete")
        if self.scope_revision != (self.predecessor_scope_revision or 0) + 1:
            raise ValueError("ClickHouse scope revision is not the exact protected successor")
        if (
            not self.business_columns
            or len(set(self.business_columns)) != len(self.business_columns)
            or not self.effective_key_columns
            or not set(self.effective_key_columns).issubset(self.business_columns)
            or self.event_time_column not in self.effective_key_columns
        ):
            raise ValueError("ClickHouse protected column closure is invalid")
        if self.artifact_provider != "s3":
            raise ValueError("ClickHouse artifact provider authority is invalid")
        for field_name in (
            "max_artifact_bytes",
            "max_staging_rows",
            "max_target_scope_rows",
            "max_staging_bytes",
            "max_shadow_bytes",
            "max_retained_backup_bytes",
            "max_total_transient_bytes",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"ClickHouse protected {field_name} must be positive")


class SemanticRefreshClickHousePublicationAuthorityPort(Protocol):
    """Load protected publication authority by immutable execution/operation identity."""

    def load(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> ClickHousePublicationAuthority:
        """Return one immutable authority record or fail closed."""


def clickhouse_authority_rows_sha256(rows: tuple[tuple[object, ...], ...]) -> str:
    """Digest normalized engine observations used by protected authority."""

    raw = json.dumps(rows, allow_nan=False, ensure_ascii=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def clickhouse_physical_authority_rows(
    *,
    database_engine: str,
    shard_count: int,
    replica_count: int,
    table_observation: tuple[object, ...],
) -> tuple[tuple[object, ...], ...]:
    """Normalize the closed V1 physical-design observation into tagged rows."""

    if len(table_observation) != 15:
        raise ValueError("ClickHouse physical table observation must contain 15 fields")
    return (
        ("topology", database_engine, shard_count, replica_count),
        ("table", *table_observation[:7]),
        ("forbidden_feature_counts", *table_observation[7:]),
    )


def clickhouse_plain_mergetree_physical_authority_rows(
    sorting_key: str,
) -> tuple[tuple[object, ...], ...]:
    """Return the exact supported Atomic/single-node/plain-MergeTree authority."""

    return clickhouse_physical_authority_rows(
        database_engine="Atomic",
        shard_count=1,
        replica_count=1,
        table_observation=(
            "MergeTree",
            "",
            sorting_key,
            sorting_key,
            "",
            "default",
            f"MergeTree ORDER BY ({sorting_key}) SETTINGS index_granularity = 8192",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ),
    )


def clickhouse_operation_table_names(target_table: str, operation_id: str) -> tuple[str, str]:
    """Derive the sole mutation-capable staging/shadow names for one operation."""

    if _RELATION_IDENTIFIER_RE.fullmatch(target_table) is None or _DIGEST_RE.fullmatch(operation_id) is None:
        raise ValueError("ClickHouse operation table identity is invalid")
    suffix = operation_id.removeprefix("sha256:")[:12]
    return (
        f"{target_table}__dpone_stage__{suffix}",
        f"{target_table}__dpone_shadow__{suffix}",
    )


def _canonical_uuid(value: object) -> bool:
    try:
        parsed = uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return False
    return str(parsed) == value


__all__ = [
    "ClickHousePublicationAuthority",
    "SemanticRefreshClickHousePublicationAuthorityPort",
    "clickhouse_authority_rows_sha256",
    "clickhouse_physical_authority_rows",
    "clickhouse_plain_mergetree_physical_authority_rows",
    "clickhouse_operation_table_names",
]
