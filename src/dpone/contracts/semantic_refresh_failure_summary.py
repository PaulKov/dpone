"""Durable failed-precommit workflow summary for semantic refresh V2."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    canonical_string_set,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_sorted_unique_strings,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_types import SqlServerModelOutcome

FAILED_WORKFLOW_SUMMARY_SCHEMA = "dpone.semantic-refresh-failed-workflow-summary.v1"
_DIGEST_FIELD = "terminal_summary_sha256"
_FIELDS = frozenset(
    {
        "schema",
        "workflow_id",
        "workflow_plan_sha256",
        "workflow_execution_binding_sha256",
        "expected_operation_ids",
        "models",
        "status",
        _DIGEST_FIELD,
    }
)
_MODEL_FIELDS = frozenset(
    {
        "operation_id",
        "attempt_binding_sha256",
        "mssql_outcome",
        "mssql_evidence_sha256",
    }
)
_TERMINAL_OUTCOMES = frozenset(
    {
        SqlServerModelOutcome.NOT_INVOKED,
        SqlServerModelOutcome.ROLLED_BACK,
        SqlServerModelOutcome.COMMITTED_WITH_IMAGES,
    }
)


@dataclass(frozen=True, order=True, slots=True)
class FailedModelOutcome:
    """One reconciled MSSQL model outcome in a failed workflow."""

    operation_id: str
    attempt_binding_sha256: str
    mssql_outcome: SqlServerModelOutcome
    mssql_evidence_sha256: str

    def __post_init__(self) -> None:
        require_digest(self.operation_id, "operation_id")
        require_digest(self.attempt_binding_sha256, "attempt_binding_sha256")
        require_digest(self.mssql_evidence_sha256, "mssql_evidence_sha256")
        if self.mssql_outcome not in _TERMINAL_OUTCOMES:
            raise SemanticRefreshContractError("unresolved MSSQL outcome blocks failed-workflow terminalization")

    @classmethod
    def from_mapping(cls, value: object) -> FailedModelOutcome:
        """Parse one closed model outcome."""

        raw = require_closed_mapping(value, "failed_model_outcome", required=_MODEL_FIELDS)
        return cls(
            operation_id=require_digest(raw.get("operation_id"), "operation_id"),
            attempt_binding_sha256=require_digest(raw.get("attempt_binding_sha256"), "attempt_binding_sha256"),
            mssql_outcome=require_enum(raw.get("mssql_outcome"), "mssql_outcome", SqlServerModelOutcome),
            mssql_evidence_sha256=require_digest(raw.get("mssql_evidence_sha256"), "mssql_evidence_sha256"),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed public mapping."""

        return {
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "mssql_evidence_sha256": self.mssql_evidence_sha256,
            "mssql_outcome": self.mssql_outcome.value,
            "operation_id": self.operation_id,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshFailedWorkflowSummary(SemanticRefreshDocumentCodec):
    """Terminal failed-precommit evidence with an exact model closure."""

    workflow_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    expected_operation_ids: tuple[str, ...]
    models: tuple[FailedModelOutcome, ...]
    terminal_summary_sha256: str
    status: str = "FAILED_PRE_COMMIT"
    schema: str = FAILED_WORKFLOW_SUMMARY_SCHEMA

    schema_id: ClassVar[str] = FAILED_WORKFLOW_SUMMARY_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        require_text(self.workflow_id, "workflow_id")
        require_digest(self.workflow_plan_sha256, "workflow_plan_sha256")
        require_digest(
            self.workflow_execution_binding_sha256,
            "workflow_execution_binding_sha256",
        )
        if self.status != "FAILED_PRE_COMMIT":
            raise SemanticRefreshContractError("failed workflow status is unsupported")
        expected = require_sorted_unique_strings(list(self.expected_operation_ids), "expected_operation_ids")
        if not isinstance(self.models, tuple) or any(not isinstance(item, FailedModelOutcome) for item in self.models):
            raise SemanticRefreshContractError("models must be a tuple of failed model outcomes")
        if tuple(sorted(self.models)) != self.models:
            raise SemanticRefreshContractError("models must be canonically ordered")
        if tuple(item.operation_id for item in self.models) != expected:
            raise SemanticRefreshContractError("failed workflow model closure differs")
        validate_digest(self._unsigned(), self.terminal_summary_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        workflow_id: str,
        workflow_plan_sha256: str,
        workflow_execution_binding_sha256: str,
        expected_operation_ids: tuple[str, ...],
        models: tuple[FailedModelOutcome, ...],
    ) -> SemanticRefreshFailedWorkflowSummary:
        """Build one canonical terminal failed-workflow summary."""

        expected = canonical_string_set(expected_operation_ids, "expected_operation_ids")
        canonical_models = _canonical_models(models)
        unsigned = _unsigned_mapping(
            workflow_id,
            workflow_plan_sha256,
            workflow_execution_binding_sha256,
            expected,
            canonical_models,
        )
        return cls(
            workflow_id=workflow_id,
            workflow_plan_sha256=workflow_plan_sha256,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            expected_operation_ids=expected,
            models=canonical_models,
            terminal_summary_sha256=semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshFailedWorkflowSummary:
        """Parse a closed summary and recompute its digest."""

        raw = require_closed_mapping(value, "failed_workflow_summary", required=_FIELDS)
        raw_models = raw.get("models")
        if not isinstance(raw_models, Sequence) or isinstance(raw_models, str | bytes):
            raise SemanticRefreshContractError("models must be an array")
        return cls(
            workflow_id=require_text(raw.get("workflow_id"), "workflow_id"),
            workflow_plan_sha256=require_digest(raw.get("workflow_plan_sha256"), "workflow_plan_sha256"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"),
                "workflow_execution_binding_sha256",
            ),
            expected_operation_ids=require_sorted_unique_strings(
                raw.get("expected_operation_ids"), "expected_operation_ids"
            ),
            models=tuple(FailedModelOutcome.from_mapping(item) for item in raw_models),
            terminal_summary_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            status=require_text(raw.get("status"), "status"),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.workflow_id,
            self.workflow_plan_sha256,
            self.workflow_execution_binding_sha256,
            self.expected_operation_ids,
            self.models,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.terminal_summary_sha256}


def _canonical_models(values: tuple[FailedModelOutcome, ...]) -> tuple[FailedModelOutcome, ...]:
    if not isinstance(values, tuple) or not values or any(not isinstance(item, FailedModelOutcome) for item in values):
        raise SemanticRefreshContractError("models must be a non-empty tuple")
    if len({item.operation_id for item in values}) != len(values):
        raise SemanticRefreshContractError("model operation IDs must be unique")
    return tuple(sorted(values))


def _unsigned_mapping(
    workflow_id: str,
    workflow_plan_sha256: str,
    workflow_execution_binding_sha256: str,
    expected_operation_ids: tuple[str, ...],
    models: tuple[FailedModelOutcome, ...],
) -> dict[str, object]:
    return {
        "expected_operation_ids": list(expected_operation_ids),
        "models": [item.to_dict() for item in models],
        "schema": FAILED_WORKFLOW_SUMMARY_SCHEMA,
        "status": "FAILED_PRE_COMMIT",
        "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
        "workflow_id": workflow_id,
        "workflow_plan_sha256": workflow_plan_sha256,
    }


__all__ = [
    "FAILED_WORKFLOW_SUMMARY_SCHEMA",
    "FailedModelOutcome",
    "SemanticRefreshFailedWorkflowSummary",
]
