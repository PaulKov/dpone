"""Closed PREPARE SQL, engine evidence, failures, and conformance checks."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.semantic_refresh_clickhouse_models import ClickHousePreparePlan

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class ClickHouseConformanceError(RuntimeError):
    """Raised when PREPARE evidence cannot authorize publication."""


class ClickHouseUuidAmbiguityError(RuntimeError):
    """Raised when UUID ownership proves neither old nor exchanged state."""


class ClickHouseExchangeNotCommittedError(RuntimeError):
    """Raised when reconciliation proves the original UUID ownership remains."""


class ClickHouseCommittedIncompleteError(RuntimeError):
    """Raised after target mutation when terminal state publication is unproved."""

    def __init__(self, message: str, *, exchange_receipt: object) -> None:
        super().__init__(message)
        self.exchange_receipt = exchange_receipt


class ClickHousePostCommitCleanupError(RuntimeError):
    """Raised when COMPLETE is durable but exact temporary-relation cleanup is unproved."""

    def __init__(self, message: str, *, publication_receipt: object) -> None:
        super().__init__(message)
        self.publication_receipt = publication_receipt


@dataclass(frozen=True)
class ClickHousePrepareEvidence:
    """Exact engine observations required before exchange can be attempted."""

    target_uuid: str
    staging_uuid: str
    shadow_uuid: str
    staging_rows: int
    target_scope_rows: int
    shadow_rows: int
    desired_rows: int
    staging_null_key_rows: int
    staging_duplicate_key_groups: int
    target_null_key_rows: int
    target_duplicate_key_groups: int
    forward_difference_groups: int
    reverse_difference_groups: int
    schema_sha256: str
    physical_sha256: str
    staging_bytes: int
    shadow_bytes: int
    retained_backup_bytes: int
    total_transient_bytes: int
    guard_operation_id: str
    guard_attempt_binding_sha256: str
    guard_fence_epoch: int
    database_engine: str
    table_engine: str
    shard_count: int
    replica_count: int

    @classmethod
    def from_mapping(cls, raw: Any) -> ClickHousePrepareEvidence:
        if not isinstance(raw, dict) or set(raw) != set(cls.__dataclass_fields__):
            raise ValueError("ClickHouse PREPARE evidence must contain the exact closed fields")
        evidence = cls(**raw)
        for field_name in ("target_uuid", "staging_uuid", "shadow_uuid"):
            _require_uuid(getattr(evidence, field_name), field_name)
        for field_name in ("schema_sha256", "physical_sha256", "guard_attempt_binding_sha256"):
            _require_digest(getattr(evidence, field_name), field_name)
        for field_name in (
            "staging_rows",
            "target_scope_rows",
            "shadow_rows",
            "desired_rows",
            "staging_null_key_rows",
            "staging_duplicate_key_groups",
            "target_null_key_rows",
            "target_duplicate_key_groups",
            "forward_difference_groups",
            "reverse_difference_groups",
            "staging_bytes",
            "shadow_bytes",
            "retained_backup_bytes",
            "total_transient_bytes",
        ):
            value = getattr(evidence, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
        _require_positive_int(evidence.guard_fence_epoch, "guard_fence_epoch")
        _require_positive_int(evidence.shard_count, "shard_count")
        _require_positive_int(evidence.replica_count, "replica_count")
        return evidence


@dataclass(frozen=True)
class ClickHouseEmptyScopeEvidence:
    """Current target proof authorizing a no-swap empty-scope publication."""

    target_uuid: str
    target_scope_rows: int
    schema_sha256: str
    physical_sha256: str
    guard_operation_id: str
    guard_attempt_binding_sha256: str
    guard_fence_epoch: int
    database_engine: str
    table_engine: str
    shard_count: int
    replica_count: int

    @classmethod
    def from_mapping(cls, raw: Any) -> ClickHouseEmptyScopeEvidence:
        if not isinstance(raw, dict) or set(raw) != set(cls.__dataclass_fields__):
            raise ValueError("ClickHouse empty-scope evidence must contain the exact closed fields")
        evidence = cls(**raw)
        _require_uuid(evidence.target_uuid, "target_uuid")
        for field_name in ("schema_sha256", "physical_sha256", "guard_attempt_binding_sha256"):
            _require_digest(getattr(evidence, field_name), field_name)
        if (
            isinstance(evidence.target_scope_rows, bool)
            or not isinstance(evidence.target_scope_rows, int)
            or evidence.target_scope_rows < 0
        ):
            raise ValueError("target_scope_rows is invalid")
        _require_positive_int(evidence.guard_fence_epoch, "guard_fence_epoch")
        _require_positive_int(evidence.shard_count, "shard_count")
        _require_positive_int(evidence.replica_count, "replica_count")
        return evidence


@dataclass(frozen=True)
class ClickHouseTargetAuthorityEvidence:
    """Full target authority observed after EXCHANGE and before COMPLETE."""

    target_uuid: str
    schema_sha256: str
    physical_sha256: str
    guard_operation_id: str
    guard_attempt_binding_sha256: str
    guard_fence_epoch: int
    database_engine: str
    table_engine: str
    shard_count: int
    replica_count: int

    @classmethod
    def from_mapping(cls, raw: Any) -> ClickHouseTargetAuthorityEvidence:
        if not isinstance(raw, dict) or set(raw) != set(cls.__dataclass_fields__):
            raise ValueError("ClickHouse target authority evidence must contain the exact closed fields")
        evidence = cls(**raw)
        _require_uuid(evidence.target_uuid, "target_uuid")
        for field_name in ("schema_sha256", "physical_sha256", "guard_attempt_binding_sha256"):
            _require_digest(getattr(evidence, field_name), field_name)
        _require_positive_int(evidence.guard_fence_epoch, "guard_fence_epoch")
        _require_positive_int(evidence.shard_count, "shard_count")
        _require_positive_int(evidence.replica_count, "replica_count")
        return evidence


@dataclass(frozen=True)
class ClickHousePrepareSql:
    """Closed SQL statements executed by a thin ClickHouse adapter."""

    populate_shadow_retained: str
    append_staging: str
    forward_multiset_difference: str
    reverse_multiset_difference: str

    def to_mapping(self) -> dict[str, str]:
        return {
            "populate_shadow_retained": self.populate_shadow_retained,
            "append_staging": self.append_staging,
            "forward_multiset_difference": self.forward_multiset_difference,
            "reverse_multiset_difference": self.reverse_multiset_difference,
        }


class ClickHousePrepareSqlBuilder:
    """Build the anti-join-plus-staging shadow equation and exact proof."""

    def build(self, plan: ClickHousePreparePlan) -> ClickHousePrepareSql:
        columns = _columns(plan.business_columns)
        target = _table(plan.database, plan.target_table)
        staging = _table(plan.database, plan.staging_table)
        shadow = _table(plan.database, plan.shadow_table)
        target_columns = _qualified_columns("target", plan.business_columns)
        staged_columns = _qualified_columns("staged", plan.business_columns)
        join = " AND ".join(
            f"target.{_identifier(column)} = staged.{_identifier(column)}" for column in plan.effective_key_columns
        )
        desired = (
            f"SELECT {target_columns} FROM {target} AS target "
            f"LEFT ANTI JOIN {staging} AS staged ON {join} "
            f"UNION ALL SELECT {staged_columns} FROM {staging} AS staged"
        )
        return ClickHousePrepareSql(
            populate_shadow_retained=(
                f"INSERT INTO {shadow} ({columns}) SELECT {target_columns} FROM {target} AS target "
                f"LEFT ANTI JOIN {staging} AS staged ON {join}"
            ),
            append_staging=f"INSERT INTO {shadow} ({columns}) SELECT {columns} FROM {staging}",
            forward_multiset_difference=_difference_query(
                left=f"SELECT {columns} FROM {shadow}", right=desired, columns=columns
            ),
            reverse_multiset_difference=_difference_query(
                left=desired, right=f"SELECT {columns} FROM {shadow}", columns=columns
            ),
        )


def assert_prepare_evidence(plan: ClickHousePreparePlan, evidence: ClickHousePrepareEvidence) -> None:
    """Require exact topology, identity, key, resource, and multiset evidence."""

    if evidence.target_uuid != plan.expected_target_uuid:
        raise ClickHouseConformanceError("ClickHouse target UUID drifted before PREPARE")
    if len({evidence.target_uuid, evidence.staging_uuid, evidence.shadow_uuid}) != 3:
        raise ClickHouseConformanceError("ClickHouse PREPARE table UUIDs must be distinct")
    if evidence.schema_sha256 != plan.expected_schema_sha256:
        raise ClickHouseConformanceError("ClickHouse schema digest does not conform")
    if evidence.physical_sha256 != plan.expected_physical_sha256:
        raise ClickHouseConformanceError("ClickHouse physical digest does not conform")
    if (
        evidence.database_engine != plan.database_engine
        or evidence.table_engine != plan.table_engine
        or evidence.shard_count != plan.shard_count
        or evidence.replica_count != plan.replica_count
    ):
        raise ClickHouseConformanceError("ClickHouse Atomic/MergeTree topology does not conform")
    if (
        evidence.guard_operation_id != plan.operation_id
        or evidence.guard_attempt_binding_sha256 != plan.attempt_binding_sha256
        or evidence.guard_fence_epoch != plan.fence_epoch
    ):
        raise ClickHouseConformanceError("ClickHouse guard/fence evidence does not conform")
    if any(
        value != 0
        for value in (
            evidence.staging_null_key_rows,
            evidence.staging_duplicate_key_groups,
            evidence.target_null_key_rows,
            evidence.target_duplicate_key_groups,
        )
    ):
        raise ClickHouseConformanceError("ClickHouse effective-key gates did not pass")
    if evidence.staging_rows > plan.max_staging_rows:
        raise ClickHouseConformanceError("ClickHouse staging row budget exceeded")
    if evidence.target_scope_rows > plan.max_target_scope_rows:
        raise ClickHouseConformanceError("ClickHouse target scope row budget exceeded")
    if evidence.total_transient_bytes != (
        evidence.staging_bytes + evidence.shadow_bytes + evidence.retained_backup_bytes
    ):
        raise ClickHouseConformanceError("ClickHouse total transient byte evidence is inconsistent")
    for observed, maximum, label in (
        (evidence.staging_bytes, plan.max_staging_bytes, "staging"),
        (evidence.shadow_bytes, plan.max_shadow_bytes, "shadow"),
        (evidence.retained_backup_bytes, plan.max_retained_backup_bytes, "retained backup"),
        (evidence.total_transient_bytes, plan.max_total_transient_bytes, "total transient"),
    ):
        if observed > maximum:
            raise ClickHouseConformanceError(f"ClickHouse {label} byte budget exceeded")
    if evidence.shadow_rows != evidence.desired_rows:
        raise ClickHouseConformanceError("ClickHouse shadow/desired row count invariant failed")
    if evidence.forward_difference_groups or evidence.reverse_difference_groups:
        raise ClickHouseConformanceError("ClickHouse exact grouped-multiset conformance failed")


def assert_target_authority_evidence(
    plan: ClickHousePreparePlan,
    evidence: ClickHouseTargetAuthorityEvidence,
    *,
    expected_target_uuid: str,
) -> None:
    """Require the same closed physical authority after the name exchange."""

    if (
        evidence.target_uuid != expected_target_uuid
        or evidence.schema_sha256 != plan.expected_schema_sha256
        or evidence.physical_sha256 != plan.expected_physical_sha256
        or evidence.database_engine != plan.database_engine
        or evidence.table_engine != plan.table_engine
        or evidence.shard_count != plan.shard_count
        or evidence.replica_count != plan.replica_count
        or evidence.guard_operation_id != plan.operation_id
        or evidence.guard_attempt_binding_sha256 != plan.attempt_binding_sha256
        or evidence.guard_fence_epoch != plan.fence_epoch
    ):
        raise ClickHouseConformanceError("ClickHouse post-exchange target authority differs")


def _difference_query(*, left: str, right: str, columns: str) -> str:
    return (
        "SELECT count() AS difference_groups FROM ("
        f"SELECT {columns}, count() AS multiplicity FROM ({left}) GROUP BY {columns} "
        "EXCEPT DISTINCT "
        f"SELECT {columns}, count() AS multiplicity FROM ({right}) GROUP BY {columns}"
        ")"
    )


def _columns(columns: tuple[str, ...]) -> str:
    return ", ".join(_identifier(column) for column in columns)


def _qualified_columns(alias: str, columns: tuple[str, ...]) -> str:
    return ", ".join(f"{alias}.{_identifier(column)}" for column in columns)


def _table(database: str, table: str) -> str:
    return f"{_identifier(database)}.{_identifier(table)}"


def _identifier(value: str) -> str:
    return f"`{value}`"


def _require_digest(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a canonical sha256 digest")


def _require_uuid(value: object, field_name: str) -> None:
    try:
        parsed = uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        parsed = None
    if parsed is None or str(parsed) != value:
        raise ValueError(f"{field_name} must be a canonical UUID")


def _require_positive_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be positive")


__all__ = [
    "ClickHouseCommittedIncompleteError",
    "ClickHouseConformanceError",
    "ClickHouseEmptyScopeEvidence",
    "ClickHouseExchangeNotCommittedError",
    "ClickHousePostCommitCleanupError",
    "ClickHousePrepareEvidence",
    "ClickHousePrepareSql",
    "ClickHousePrepareSqlBuilder",
    "ClickHouseUuidAmbiguityError",
    "assert_prepare_evidence",
]
