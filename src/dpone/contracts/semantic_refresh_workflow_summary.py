"""Durable complete-only workflow summary authority."""

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
    require_positive_int,
    require_sorted_unique_strings,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)

DURABLE_WORKFLOW_SUMMARY_SCHEMA = "dpone.semantic-refresh-durable-workflow-summary.v1"
_DIGEST_FIELD = "terminal_summary_sha256"
_SUMMARY_STATUS = "FULLY_COMPLETE"
_PUBLICATION_STATUS = "COMPLETE"
_PUBLICATION_FIELDS = frozenset(
    {
        "operation_id",
        "operation_plan_sha256",
        "workflow_execution_binding_sha256",
        "attempt_binding_sha256",
        "artifact_manifest_sha256",
        "clickhouse_terminal_receipt_sha256",
        "terminal_receipt_sha256",
        "target_generation",
        "scope_revision",
        "status",
    }
)
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "workflow_execution_id",
        "workflow_plan_sha256",
        "workflow_execution_binding_sha256",
        "expected_operation_ids",
        "publications",
        "status",
    }
)


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshDurableModelPublication:
    """Exact terminal artifact and ClickHouse receipt closure for one model."""

    operation_id: str
    operation_plan_sha256: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    artifact_manifest_sha256: str
    clickhouse_terminal_receipt_sha256: str
    terminal_receipt_sha256: str
    target_generation: int
    scope_revision: int
    status: str = _PUBLICATION_STATUS

    def __post_init__(self) -> None:
        for field in (
            "operation_id",
            "operation_plan_sha256",
            "workflow_execution_binding_sha256",
            "attempt_binding_sha256",
            "artifact_manifest_sha256",
            "clickhouse_terminal_receipt_sha256",
            "terminal_receipt_sha256",
        ):
            require_digest(getattr(self, field), f"publication.{field}")
        require_positive_int(self.target_generation, "publication.target_generation")
        require_positive_int(self.scope_revision, "publication.scope_revision")
        if self.status != _PUBLICATION_STATUS:
            raise SemanticRefreshContractError("durable model publication must be COMPLETE")

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshDurableModelPublication:
        """Parse one closed complete publication."""

        raw = require_closed_mapping(value, "publication", required=_PUBLICATION_FIELDS)
        return cls(
            operation_id=require_digest(raw.get("operation_id"), "publication.operation_id"),
            operation_plan_sha256=require_digest(raw.get("operation_plan_sha256"), "publication.operation_plan_sha256"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"),
                "publication.workflow_execution_binding_sha256",
            ),
            attempt_binding_sha256=require_digest(
                raw.get("attempt_binding_sha256"), "publication.attempt_binding_sha256"
            ),
            artifact_manifest_sha256=require_digest(
                raw.get("artifact_manifest_sha256"), "publication.artifact_manifest_sha256"
            ),
            clickhouse_terminal_receipt_sha256=require_digest(
                raw.get("clickhouse_terminal_receipt_sha256"),
                "publication.clickhouse_terminal_receipt_sha256",
            ),
            terminal_receipt_sha256=require_digest(
                raw.get("terminal_receipt_sha256"), "publication.terminal_receipt_sha256"
            ),
            target_generation=require_positive_int(raw.get("target_generation"), "publication.target_generation"),
            scope_revision=require_positive_int(raw.get("scope_revision"), "publication.scope_revision"),
            status=require_text(raw.get("status"), "publication.status"),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the canonical terminal publication mapping."""

        return {
            "artifact_manifest_sha256": self.artifact_manifest_sha256,
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "clickhouse_terminal_receipt_sha256": self.clickhouse_terminal_receipt_sha256,
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
            "scope_revision": self.scope_revision,
            "status": self.status,
            "target_generation": self.target_generation,
            "terminal_receipt_sha256": self.terminal_receipt_sha256,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshDurableWorkflowSummary(SemanticRefreshDocumentCodec):
    """Fully complete durable closure used as the only workflow-success authority."""

    workflow_execution_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    expected_operation_ids: tuple[str, ...]
    publications: tuple[SemanticRefreshDurableModelPublication, ...]
    terminal_summary_sha256: str
    status: str = _SUMMARY_STATUS
    schema: str = DURABLE_WORKFLOW_SUMMARY_SCHEMA

    schema_id: ClassVar[str] = DURABLE_WORKFLOW_SUMMARY_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        require_text(self.workflow_execution_id, "workflow_execution_id")
        require_digest(self.workflow_plan_sha256, "workflow_plan_sha256")
        require_digest(self.workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
        expected = require_sorted_unique_strings(list(self.expected_operation_ids), "expected_operation_ids")
        publications = _canonical_publications(self.publications)
        if publications != self.publications:
            raise SemanticRefreshContractError("durable workflow publications must be canonically ordered")
        if tuple(item.operation_id for item in publications) != expected:
            raise SemanticRefreshContractError("durable workflow publication closure differs from expected operations")
        if any(
            item.workflow_execution_binding_sha256 != self.workflow_execution_binding_sha256 for item in publications
        ):
            raise SemanticRefreshContractError("durable publication uses another workflow execution binding")
        if self.status != _SUMMARY_STATUS:
            raise SemanticRefreshContractError("durable workflow summary must be FULLY_COMPLETE")
        validate_digest(self._unsigned(), self.terminal_summary_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        workflow_execution_id: str,
        workflow_plan_sha256: str,
        workflow_execution_binding_sha256: str,
        expected_operation_ids: tuple[str, ...],
        publications: tuple[SemanticRefreshDurableModelPublication, ...],
    ) -> SemanticRefreshDurableWorkflowSummary:
        """Build only a fully complete exact model-publication closure."""

        expected = canonical_string_set(expected_operation_ids, "expected_operation_ids")
        ordered = _canonical_publications(publications)
        unsigned = _unsigned_mapping(
            workflow_execution_id,
            workflow_plan_sha256,
            workflow_execution_binding_sha256,
            expected,
            ordered,
        )
        return cls(
            workflow_execution_id,
            workflow_plan_sha256,
            workflow_execution_binding_sha256,
            expected,
            ordered,
            semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshDurableWorkflowSummary:
        """Parse and recompute a complete-only durable summary."""

        raw = require_closed_mapping(value, "durable_workflow_summary", required=_FIELDS)
        raw_publications = raw.get("publications")
        if not isinstance(raw_publications, Sequence) or isinstance(raw_publications, str | bytes):
            raise SemanticRefreshContractError("publications must be an array")
        return cls(
            workflow_execution_id=require_text(raw.get("workflow_execution_id"), "workflow_execution_id"),
            workflow_plan_sha256=require_digest(raw.get("workflow_plan_sha256"), "workflow_plan_sha256"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"), "workflow_execution_binding_sha256"
            ),
            expected_operation_ids=require_sorted_unique_strings(
                raw.get("expected_operation_ids"), "expected_operation_ids"
            ),
            publications=tuple(SemanticRefreshDurableModelPublication.from_mapping(item) for item in raw_publications),
            terminal_summary_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            status=require_text(raw.get("status"), "status"),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.workflow_execution_id,
            self.workflow_plan_sha256,
            self.workflow_execution_binding_sha256,
            self.expected_operation_ids,
            self.publications,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the complete durable summary mapping."""

        return {**self._unsigned(), self.digest_field: self.terminal_summary_sha256}


def _canonical_publications(
    values: tuple[SemanticRefreshDurableModelPublication, ...],
) -> tuple[SemanticRefreshDurableModelPublication, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(item, SemanticRefreshDurableModelPublication) for item in values)
    ):
        raise SemanticRefreshContractError("publications must be a non-empty tuple")
    ordered = tuple(sorted(values, key=lambda item: item.operation_id))
    if len({item.operation_id for item in ordered}) != len(ordered):
        raise SemanticRefreshContractError("durable workflow publications must be unique")
    return ordered


def _unsigned_mapping(
    workflow_execution_id: str,
    workflow_plan_sha256: str,
    workflow_execution_binding_sha256: str,
    expected_operation_ids: tuple[str, ...],
    publications: tuple[SemanticRefreshDurableModelPublication, ...],
) -> dict[str, object]:
    return {
        "expected_operation_ids": list(expected_operation_ids),
        "publications": [item.to_dict() for item in publications],
        "schema": DURABLE_WORKFLOW_SUMMARY_SCHEMA,
        "status": _SUMMARY_STATUS,
        "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
        "workflow_execution_id": workflow_execution_id,
        "workflow_plan_sha256": workflow_plan_sha256,
    }


__all__ = [
    "DURABLE_WORKFLOW_SUMMARY_SCHEMA",
    "SemanticRefreshDurableModelPublication",
    "SemanticRefreshDurableWorkflowSummary",
]
