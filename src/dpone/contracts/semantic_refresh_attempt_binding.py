"""Attempt-local runtime binding for one semantic-refresh operation."""

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

ATTEMPT_BINDING_SCHEMA = "dpone.semantic-refresh-attempt-binding.v1"
_DIGEST_FIELD = "attempt_binding_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "workflow_execution_id",
        "workflow_execution_binding_sha256",
        "operation_id",
        "operation_plan_sha256",
        "dag_run_id",
        "task_id",
        "try_number",
        "pod_uid",
        "fencing_epoch",
        "owner_id",
    }
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshAttemptBinding(SemanticRefreshDocumentCodec):
    """Bind DagRun/try/pod/fence facts after semantic identity is complete."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    operation_id: str
    operation_plan_sha256: str
    dag_run_id: str
    task_id: str
    try_number: int
    pod_uid: str
    fencing_epoch: int
    owner_id: str
    attempt_binding_sha256: str
    schema: str = ATTEMPT_BINDING_SCHEMA

    schema_id: ClassVar[str] = ATTEMPT_BINDING_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        for field_name in (
            "workflow_execution_binding_sha256",
            "operation_id",
            "operation_plan_sha256",
        ):
            require_digest(getattr(self, field_name), field_name)
        for field_name in ("dag_run_id", "task_id", "pod_uid", "owner_id"):
            require_text(getattr(self, field_name), field_name)
        require_text(self.workflow_execution_id, "workflow_execution_id")
        if self.workflow_execution_id != self.dag_run_id:
            raise SemanticRefreshContractError("workflow_execution_id must equal dag_run_id")
        require_positive_int(self.try_number, "try_number")
        require_positive_int(self.fencing_epoch, "fencing_epoch")
        validate_digest(self._unsigned(), self.attempt_binding_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        operation_id: str,
        operation_plan_sha256: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        pod_uid: str,
        fencing_epoch: int,
        owner_id: str,
    ) -> SemanticRefreshAttemptBinding:
        """Build one attempt identity without altering its operation identity."""

        values = (
            workflow_execution_id,
            workflow_execution_binding_sha256,
            operation_id,
            operation_plan_sha256,
            dag_run_id,
            task_id,
            try_number,
            pod_uid,
            fencing_epoch,
            owner_id,
        )
        return cls(*values, semantic_refresh_sha256(_unsigned_mapping(*values)))

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshAttemptBinding:
        """Parse a strict attempt binding and recompute its digest."""

        raw = require_closed_mapping(value, "attempt_binding", required=_FIELDS)
        return cls(
            workflow_execution_id=require_text(raw.get("workflow_execution_id"), "workflow_execution_id"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"),
                "workflow_execution_binding_sha256",
            ),
            operation_id=require_digest(raw.get("operation_id"), "operation_id"),
            operation_plan_sha256=require_digest(raw.get("operation_plan_sha256"), "operation_plan_sha256"),
            dag_run_id=require_text(raw.get("dag_run_id"), "dag_run_id"),
            task_id=require_text(raw.get("task_id"), "task_id"),
            try_number=require_positive_int(raw.get("try_number"), "try_number"),
            pod_uid=require_text(raw.get("pod_uid"), "pod_uid"),
            fencing_epoch=require_positive_int(raw.get("fencing_epoch"), "fencing_epoch"),
            owner_id=require_text(raw.get("owner_id"), "owner_id"),
            attempt_binding_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.workflow_execution_id,
            self.workflow_execution_binding_sha256,
            self.operation_id,
            self.operation_plan_sha256,
            self.dag_run_id,
            self.task_id,
            self.try_number,
            self.pod_uid,
            self.fencing_epoch,
            self.owner_id,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.attempt_binding_sha256}


def _unsigned_mapping(
    workflow_execution_id: str,
    workflow_execution_binding_sha256: str,
    operation_id: str,
    operation_plan_sha256: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
    pod_uid: str,
    fencing_epoch: int,
    owner_id: str,
) -> dict[str, object]:
    return {
        "dag_run_id": dag_run_id,
        "fencing_epoch": fencing_epoch,
        "operation_id": operation_id,
        "operation_plan_sha256": operation_plan_sha256,
        "owner_id": owner_id,
        "pod_uid": pod_uid,
        "schema": ATTEMPT_BINDING_SCHEMA,
        "task_id": task_id,
        "try_number": try_number,
        "workflow_execution_id": workflow_execution_id,
        "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
    }


__all__ = ["ATTEMPT_BINDING_SCHEMA", "SemanticRefreshAttemptBinding"]
