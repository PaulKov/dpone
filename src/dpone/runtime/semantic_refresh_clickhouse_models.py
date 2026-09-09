"""Closed plans and receipts for semantic-refresh ClickHouse publication."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from dpone.runtime.semantic_refresh_clickhouse_receipts import (
    ClickHouseExchangeReceipt,
    ClickHouseHeadPublicationPlan,
    ClickHousePreparedReceipt,
    ClickHousePublicationReceipt,
    ClickHouseUuidMap,
    semantic_refresh_fingerprint,
)
from dpone.runtime.semantic_refresh_clickhouse_receipts import (
    require_digest as _require_digest,
)
from dpone.runtime.semantic_refresh_clickhouse_receipts import (
    require_uuid as _require_uuid,
)
from dpone.runtime.semantic_refresh_clickhouse_receipts import (
    terminal_receipt_sha256 as terminal_receipt_sha256,
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RELATION_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
_TRANSPORT_PREFIX = "__dpone_seal_"


@dataclass(frozen=True)
class ShadowEquation:
    """Closed target equation used to populate the full shadow table."""

    retained_target_rule: str
    append_rule: str

    def __post_init__(self) -> None:
        if self.retained_target_rule != "TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY":
            raise ValueError("ClickHouse shadow retained-target rule is unsupported")
        if self.append_rule != "APPEND_ALL_STAGING_ROWS":
            raise ValueError("ClickHouse shadow append rule is unsupported")

    def to_mapping(self) -> dict[str, str]:
        return {
            "retained_target_rule": self.retained_target_rule,
            "append_rule": self.append_rule,
        }


@dataclass(frozen=True)
class GroupedMultisetConformance:
    """Exact comparison mode over every business column and multiplicity."""

    mode: str

    def __post_init__(self) -> None:
        if self.mode != "BIDIRECTIONAL_GROUPED_MULTISET":
            raise ValueError("ClickHouse conformance mode is unsupported")

    def to_mapping(self) -> dict[str, str]:
        return {"mode": self.mode}


@dataclass(frozen=True)
class ClickHousePreparePlan:
    """Immutable PREPARE identity, topology, equation, proof, and budgets."""

    operation_id: str
    operation_plan_sha256: str
    workflow_plan_sha256: str
    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    fence_epoch: int
    artifact_manifest_key: str
    artifact_manifest_version: str
    artifact_manifest_sha256: str
    target_resource_id: str
    target_authority_id: str
    clickhouse_cluster_authority_id: str
    database: str
    target_table: str
    scope_id: str
    scope_start: str
    scope_end: str
    scope_revision: int
    event_time_column: str
    staging_table: str
    shadow_table: str
    expected_target_uuid: str
    expected_schema_sha256: str
    expected_physical_sha256: str
    business_columns: tuple[str, ...]
    effective_key_columns: tuple[str, ...]
    max_staging_rows: int
    max_target_scope_rows: int
    max_staging_bytes: int
    max_shadow_bytes: int
    max_retained_backup_bytes: int
    max_total_transient_bytes: int
    shadow_equation: ShadowEquation
    conformance: GroupedMultisetConformance
    database_engine: str = "Atomic"
    table_engine: str = "MergeTree"
    shard_count: int = 1
    replica_count: int = 1
    artifact_chunk_count: int = 1
    artifact_total_rows: int = 1

    def __post_init__(self) -> None:
        for field_name in (
            "operation_id",
            "operation_plan_sha256",
            "workflow_plan_sha256",
            "workflow_execution_binding_sha256",
            "attempt_binding_sha256",
            "artifact_manifest_sha256",
            "expected_schema_sha256",
            "expected_physical_sha256",
        ):
            _require_digest(getattr(self, field_name), field_name)
        if isinstance(self.fence_epoch, bool) or not isinstance(self.fence_epoch, int) or self.fence_epoch <= 0:
            raise ValueError("ClickHouse fence_epoch must be positive")
        if (
            self.database_engine != "Atomic"
            or self.table_engine != "MergeTree"
            or type(self.shard_count) is not int
            or type(self.replica_count) is not int
            or self.shard_count != 1
            or self.replica_count != 1
        ):
            raise ValueError("ClickHouse V2 requires Atomic, plain MergeTree, single-node topology")
        if not self.artifact_manifest_key or not self.artifact_manifest_version:
            raise ValueError("ClickHouse artifact manifest must be version-pinned")
        for field_name in (
            "workflow_execution_id",
            "target_resource_id",
            "target_authority_id",
            "clickhouse_cluster_authority_id",
            "scope_id",
            "scope_start",
            "scope_end",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"ClickHouse {field_name} must be non-empty")
        if self.target_authority_id != (
            f"clickhouse://{self.clickhouse_cluster_authority_id}/{self.database}/{self.target_table}"
        ):
            raise ValueError("ClickHouse target authority differs from plan relation")
        if (
            isinstance(self.scope_revision, bool)
            or not isinstance(self.scope_revision, int)
            or self.scope_revision <= 0
        ):
            raise ValueError("ClickHouse scope_revision must be positive")
        for value in (self.database, *self.business_columns, *self.effective_key_columns):
            if _IDENTIFIER_RE.fullmatch(value) is None:
                raise ValueError(f"ClickHouse identifier is outside the closed subset: {value}")
        for value in (self.target_table, self.staging_table, self.shadow_table):
            if _RELATION_IDENTIFIER_RE.fullmatch(value) is None:
                raise ValueError(f"ClickHouse relation identifier is outside the closed subset: {value}")
        table_names = (
            self.target_table,
            self.staging_table,
            self.shadow_table,
        )
        if len(set(table_names)) != len(table_names):
            raise ValueError("ClickHouse publication tables must be distinct")
        if not self.business_columns or len(set(self.business_columns)) != len(self.business_columns):
            raise ValueError("ClickHouse business columns must be unique and non-empty")
        if any(column.startswith(_TRANSPORT_PREFIX) for column in self.business_columns):
            raise ValueError("transport-only seal columns cannot enter business conformance")
        if (
            not self.effective_key_columns
            or len(set(self.effective_key_columns)) != len(self.effective_key_columns)
            or not set(self.effective_key_columns).issubset(self.business_columns)
        ):
            raise ValueError("ClickHouse effective keys must be unique business columns")
        if self.event_time_column not in self.effective_key_columns:
            raise ValueError("ClickHouse event-time column must be an effective key")
        _require_uuid(self.expected_target_uuid, "expected_target_uuid")
        for field_name in (
            "max_staging_rows",
            "max_target_scope_rows",
            "max_staging_bytes",
            "max_shadow_bytes",
            "max_retained_backup_bytes",
            "max_total_transient_bytes",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be positive")
        for field_name in ("artifact_chunk_count", "artifact_total_rows"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field_name} must be non-negative")
        if (self.artifact_chunk_count == 0) != (self.artifact_total_rows == 0):
            raise ValueError("empty artifact chunk and row closure must agree")

    @property
    def is_empty_scope(self) -> bool:
        """Return whether the protected sealed manifest is the canonical empty closure."""

        return self.artifact_chunk_count == 0

    @property
    def sha256(self) -> str:
        return semantic_refresh_fingerprint(self.to_mapping())

    def to_mapping(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
            "workflow_plan_sha256": self.workflow_plan_sha256,
            "workflow_execution_id": self.workflow_execution_id,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "fence_epoch": self.fence_epoch,
            "artifact_manifest_key": self.artifact_manifest_key,
            "artifact_manifest_version": self.artifact_manifest_version,
            "artifact_manifest_sha256": self.artifact_manifest_sha256,
            "target_resource_id": self.target_resource_id,
            "target_authority_id": self.target_authority_id,
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "database": self.database,
            "target_table": self.target_table,
            "scope_id": self.scope_id,
            "scope_start": self.scope_start,
            "scope_end": self.scope_end,
            "scope_revision": self.scope_revision,
            "event_time_column": self.event_time_column,
            "staging_table": self.staging_table,
            "shadow_table": self.shadow_table,
            "expected_target_uuid": self.expected_target_uuid,
            "expected_schema_sha256": self.expected_schema_sha256,
            "expected_physical_sha256": self.expected_physical_sha256,
            "business_columns": self.business_columns,
            "effective_key_columns": self.effective_key_columns,
            "max_staging_rows": self.max_staging_rows,
            "max_target_scope_rows": self.max_target_scope_rows,
            "max_staging_bytes": self.max_staging_bytes,
            "max_shadow_bytes": self.max_shadow_bytes,
            "max_retained_backup_bytes": self.max_retained_backup_bytes,
            "max_total_transient_bytes": self.max_total_transient_bytes,
            "shadow_equation": self.shadow_equation.to_mapping(),
            "conformance": self.conformance.to_mapping(),
            "database_engine": self.database_engine,
            "table_engine": self.table_engine,
            "shard_count": self.shard_count,
            "replica_count": self.replica_count,
            "artifact_chunk_count": self.artifact_chunk_count,
            "artifact_total_rows": self.artifact_total_rows,
        }

    def exchange_request(self, prepared: ClickHousePreparedReceipt) -> dict[str, object]:
        """Return the exact guarded exchange command projection."""

        return {
            "workflow_execution_id": self.workflow_execution_id,
            "operation_id": self.operation_id,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "fence_epoch": self.fence_epoch,
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "database": self.database,
            "target_table": self.target_table,
            "shadow_table": self.shadow_table,
            "expected_target_uuid": prepared.target_uuid,
            "expected_shadow_uuid": prepared.shadow_uuid,
            "prepare_receipt_sha256": prepared.receipt_sha256,
        }

    def cleanup_request(
        self,
        prepared: ClickHousePreparedReceipt,
        receipt: ClickHousePublicationReceipt,
    ) -> dict[str, object]:
        """Return the exact post-COMPLETE temporary-relation cleanup projection."""

        return {
            "workflow_execution_id": self.workflow_execution_id,
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "fence_epoch": self.fence_epoch,
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "database": self.database,
            "target_table": self.target_table,
            "staging_table": self.staging_table,
            "shadow_table": self.shadow_table,
            "expected_target_uuid": prepared.shadow_uuid,
            "expected_staging_uuid": prepared.staging_uuid,
            "expected_retained_uuid": prepared.target_uuid,
            "terminal_receipt_sha256": receipt.terminal_receipt_sha256,
        }


__all__ = [
    "ClickHouseExchangeReceipt",
    "ClickHouseHeadPublicationPlan",
    "ClickHousePreparePlan",
    "ClickHousePreparedReceipt",
    "ClickHousePublicationReceipt",
    "ClickHouseUuidMap",
    "GroupedMultisetConformance",
    "ShadowEquation",
    "semantic_refresh_fingerprint",
]
