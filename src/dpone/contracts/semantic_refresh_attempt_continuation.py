"""Protected reconciliation receipt for a new worker try after dbt acknowledgement loss."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_evidence_common import require_utc_timestamp

ATTEMPT_CONTINUATION_RECEIPT_SCHEMA = "dpone.semantic-refresh-attempt-continuation-receipt.v1"
_DIGEST_FIELD = "continuation_receipt_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "status",
        "workflow_execution_binding_sha256",
        "operation_id",
        "operation_plan_sha256",
        "original_attempt_binding_sha256",
        "fencing_epoch",
        "guard_resource_id",
        "build_receipt_sha256",
        "after_image_sha256",
        "clickhouse_cluster_authority_id",
        "clickhouse_query_id_prefix",
        "clickhouse_quiescence_observation_sha256",
        "engine_quiescence_receipt_sha256",
        "mssql_active_session_count",
        "mssql_guard_lock_status",
        "original_attempt_termination_receipt_sha256",
        "task_id",
        "try_number",
        "pod_uid",
        "verified_at",
    }
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshAttemptContinuationReceipt(SemanticRefreshDocumentCodec):
    """Bind a later task try to an unchanged committed target and original fence."""

    workflow_execution_binding_sha256: str
    operation_id: str
    operation_plan_sha256: str
    original_attempt_binding_sha256: str
    fencing_epoch: int
    guard_resource_id: str
    build_receipt_sha256: str
    after_image_sha256: str
    original_attempt_termination_receipt_sha256: str
    clickhouse_cluster_authority_id: str
    clickhouse_query_id_prefix: str
    clickhouse_quiescence_observation_sha256: str
    mssql_active_session_count: int
    mssql_guard_lock_status: str
    engine_quiescence_receipt_sha256: str
    task_id: str
    try_number: int
    pod_uid: str
    verified_at: str
    continuation_receipt_sha256: str
    status: str = "VERIFIED"
    schema: str = ATTEMPT_CONTINUATION_RECEIPT_SCHEMA

    schema_id: ClassVar[str] = ATTEMPT_CONTINUATION_RECEIPT_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        for field_name in (
            "workflow_execution_binding_sha256",
            "operation_id",
            "operation_plan_sha256",
            "original_attempt_binding_sha256",
            "build_receipt_sha256",
            "after_image_sha256",
            "original_attempt_termination_receipt_sha256",
            "clickhouse_quiescence_observation_sha256",
            "engine_quiescence_receipt_sha256",
        ):
            require_digest(getattr(self, field_name), field_name)
        for field_name in (
            "guard_resource_id",
            "task_id",
            "pod_uid",
            "clickhouse_cluster_authority_id",
            "clickhouse_query_id_prefix",
        ):
            require_text(getattr(self, field_name), field_name)
        require_positive_int(self.fencing_epoch, "fencing_epoch")
        require_positive_int(self.try_number, "try_number")
        if self.mssql_active_session_count != 0:
            raise SemanticRefreshContractError("continuation requires zero active MSSQL attempt sessions")
        if self.mssql_guard_lock_status != "EXCLUSIVE_ACQUIRED":
            raise SemanticRefreshContractError("continuation MSSQL guard lock is not exclusive")
        require_utc_timestamp(self.verified_at, "verified_at")
        if self.status != "VERIFIED":
            raise SemanticRefreshContractError("continuation status must equal VERIFIED")
        if self.clickhouse_quiescence_observation_sha256 != semantic_refresh_sha256(
            self._clickhouse_quiescence_subject()
        ):
            raise SemanticRefreshContractError("ClickHouse quiescence digest differs")
        if self.engine_quiescence_receipt_sha256 != semantic_refresh_sha256(self._engine_quiescence_subject()):
            raise SemanticRefreshContractError("engine quiescence digest differs")
        validate_digest(self._unsigned(), self.continuation_receipt_sha256, self.digest_field)

    @classmethod
    def build(cls, **values: object) -> SemanticRefreshAttemptContinuationReceipt:
        """Build one canonical receipt after protected current-target reconciliation."""

        unsigned = {"schema": cls.schema_id, "status": "VERIFIED", **values}
        return cls.from_mapping({**unsigned, cls.digest_field: semantic_refresh_sha256(unsigned)})

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshAttemptContinuationReceipt:
        """Parse a closed receipt and recompute its identity."""

        raw = require_closed_mapping(value, "attempt_continuation_receipt", required=_FIELDS)
        return cls(
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"), "workflow_execution_binding_sha256"
            ),
            operation_id=require_digest(raw.get("operation_id"), "operation_id"),
            operation_plan_sha256=require_digest(raw.get("operation_plan_sha256"), "operation_plan_sha256"),
            original_attempt_binding_sha256=require_digest(
                raw.get("original_attempt_binding_sha256"), "original_attempt_binding_sha256"
            ),
            fencing_epoch=require_positive_int(raw.get("fencing_epoch"), "fencing_epoch"),
            guard_resource_id=require_text(raw.get("guard_resource_id"), "guard_resource_id"),
            build_receipt_sha256=require_digest(raw.get("build_receipt_sha256"), "build_receipt_sha256"),
            after_image_sha256=require_digest(raw.get("after_image_sha256"), "after_image_sha256"),
            original_attempt_termination_receipt_sha256=require_digest(
                raw.get("original_attempt_termination_receipt_sha256"),
                "original_attempt_termination_receipt_sha256",
            ),
            clickhouse_cluster_authority_id=require_text(
                raw.get("clickhouse_cluster_authority_id"),
                "clickhouse_cluster_authority_id",
            ),
            clickhouse_query_id_prefix=require_text(
                raw.get("clickhouse_query_id_prefix"),
                "clickhouse_query_id_prefix",
            ),
            clickhouse_quiescence_observation_sha256=require_digest(
                raw.get("clickhouse_quiescence_observation_sha256"),
                "clickhouse_quiescence_observation_sha256",
            ),
            mssql_active_session_count=_zero(
                raw.get("mssql_active_session_count"),
                "mssql_active_session_count",
            ),
            mssql_guard_lock_status=require_text(
                raw.get("mssql_guard_lock_status"),
                "mssql_guard_lock_status",
            ),
            engine_quiescence_receipt_sha256=require_digest(
                raw.get("engine_quiescence_receipt_sha256"),
                "engine_quiescence_receipt_sha256",
            ),
            task_id=require_text(raw.get("task_id"), "task_id"),
            try_number=require_positive_int(raw.get("try_number"), "try_number"),
            pod_uid=require_text(raw.get("pod_uid"), "pod_uid"),
            verified_at=require_text(raw.get("verified_at"), "verified_at"),
            continuation_receipt_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            status=require_text(raw.get("status"), "status"),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return {
            "after_image_sha256": self.after_image_sha256,
            "build_receipt_sha256": self.build_receipt_sha256,
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "clickhouse_query_id_prefix": self.clickhouse_query_id_prefix,
            "clickhouse_quiescence_observation_sha256": self.clickhouse_quiescence_observation_sha256,
            "engine_quiescence_receipt_sha256": self.engine_quiescence_receipt_sha256,
            "fencing_epoch": self.fencing_epoch,
            "guard_resource_id": self.guard_resource_id,
            "mssql_active_session_count": self.mssql_active_session_count,
            "mssql_guard_lock_status": self.mssql_guard_lock_status,
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
            "original_attempt_binding_sha256": self.original_attempt_binding_sha256,
            "original_attempt_termination_receipt_sha256": (self.original_attempt_termination_receipt_sha256),
            "pod_uid": self.pod_uid,
            "schema": self.schema,
            "status": self.status,
            "task_id": self.task_id,
            "try_number": self.try_number,
            "verified_at": self.verified_at,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
        }

    def _clickhouse_quiescence_subject(self) -> dict[str, object]:
        return {
            "active_query_ids": [],
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "observed_at": self.verified_at,
            "operation_id": self.operation_id,
            "original_attempt_binding_sha256": self.original_attempt_binding_sha256,
            "query_id_prefix": self.clickhouse_query_id_prefix,
            "schema": "dpone.semantic-refresh-clickhouse-attempt-quiescence.v1",
            "status": "VERIFIED",
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
        }

    def _engine_quiescence_subject(self) -> dict[str, object]:
        return {
            "clickhouse_quiescence_observation_sha256": (self.clickhouse_quiescence_observation_sha256),
            "mssql_active_session_count": self.mssql_active_session_count,
            "mssql_guard_lock_status": self.mssql_guard_lock_status,
            "observed_at": self.verified_at,
            "operation_id": self.operation_id,
            "original_attempt_binding_sha256": self.original_attempt_binding_sha256,
            "original_attempt_termination_receipt_sha256": (self.original_attempt_termination_receipt_sha256),
            "schema": "dpone.semantic-refresh-attempt-engine-quiescence.v1",
            "status": "VERIFIED",
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
        }

    def to_dict(self) -> dict[str, object]:
        """Return the canonical durable evidence mapping."""

        return {**self._unsigned(), self.digest_field: self.continuation_receipt_sha256}


def semantic_refresh_engine_quiescence_sha256(
    *,
    workflow_execution_binding_sha256: str,
    operation_id: str,
    original_attempt_binding_sha256: str,
    original_attempt_termination_receipt_sha256: str,
    clickhouse_quiescence_observation_sha256: str,
    observed_at: str,
) -> str:
    """Digest the exact termination, MSSQL lock/session, and ClickHouse closure."""

    return semantic_refresh_sha256(
        {
            "clickhouse_quiescence_observation_sha256": clickhouse_quiescence_observation_sha256,
            "mssql_active_session_count": 0,
            "mssql_guard_lock_status": "EXCLUSIVE_ACQUIRED",
            "observed_at": observed_at,
            "operation_id": operation_id,
            "original_attempt_binding_sha256": original_attempt_binding_sha256,
            "original_attempt_termination_receipt_sha256": (original_attempt_termination_receipt_sha256),
            "schema": "dpone.semantic-refresh-attempt-engine-quiescence.v1",
            "status": "VERIFIED",
            "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
        }
    )


def _zero(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value != 0:
        raise SemanticRefreshContractError(f"{field} must equal zero")
    return value


__all__ = [
    "ATTEMPT_CONTINUATION_RECEIPT_SCHEMA",
    "SemanticRefreshAttemptContinuationReceipt",
    "semantic_refresh_engine_quiescence_sha256",
]
