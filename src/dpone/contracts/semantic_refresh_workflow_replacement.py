"""Failed-precommit semantic workflow replacement plan."""

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
from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
from dpone.contracts.semantic_refresh_types import WorkflowMode

WORKFLOW_REPLACEMENT_PLAN_SCHEMA = "dpone.semantic-refresh-workflow-replacement-plan.v1"
_DIGEST_FIELD = "workflow_replacement_plan_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "workflow_mode",
        "workflow_plan_sha256",
        "predecessor_workflow_execution_id",
        "predecessor_workflow_execution_binding_sha256",
        "predecessor_workflow_summary_sha256",
        "recovery_plan_digest",
        "selected_mutating_node_ids",
        "model_operation_plan_ids",
        "expected_model_outcome_ids",
        "replacement_action_ids",
        "replacement_actions",
    }
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshWorkflowReplacementPlan(SemanticRefreshDocumentCodec):
    """One recovery-authorized replacement referencing an existing workflow plan."""

    workflow_plan_sha256: str
    predecessor_workflow_execution_id: str
    predecessor_workflow_execution_binding_sha256: str
    predecessor_workflow_summary_sha256: str
    recovery_plan_digest: str
    selected_mutating_node_ids: tuple[str, ...]
    model_operation_plan_ids: tuple[str, ...]
    expected_model_outcome_ids: tuple[str, ...]
    replacement_action_ids: tuple[str, ...]
    replacement_actions: tuple[ReplacementActionBinding, ...]
    workflow_replacement_plan_sha256: str
    workflow_mode: WorkflowMode = WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT
    schema: str = WORKFLOW_REPLACEMENT_PLAN_SCHEMA

    schema_id: ClassVar[str] = WORKFLOW_REPLACEMENT_PLAN_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if self.workflow_mode is not WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT:
            raise SemanticRefreshContractError("replacement plan mode is unsupported")
        require_digest(self.workflow_plan_sha256, "workflow_plan_sha256")
        require_text(
            self.predecessor_workflow_execution_id,
            "predecessor_workflow_execution_id",
        )
        require_digest(
            self.predecessor_workflow_execution_binding_sha256,
            "predecessor_workflow_execution_binding_sha256",
        )
        require_digest(
            self.predecessor_workflow_summary_sha256,
            "predecessor_workflow_summary_sha256",
        )
        require_digest(self.recovery_plan_digest, "recovery_plan_digest")
        _validate_replacement_closure(self)
        validate_digest(
            self._unsigned(),
            self.workflow_replacement_plan_sha256,
            self.digest_field,
        )

    @classmethod
    def build(
        cls,
        *,
        workflow_plan_sha256: str,
        predecessor_workflow_execution_id: str,
        predecessor_workflow_execution_binding_sha256: str,
        predecessor_workflow_summary_sha256: str,
        recovery_plan_digest: str,
        selected_mutating_node_ids: tuple[str, ...],
        model_operation_plan_ids: tuple[str, ...],
        expected_model_outcome_ids: tuple[str, ...],
        replacement_action_ids: tuple[str, ...],
        replacement_actions: tuple[ReplacementActionBinding, ...],
    ) -> SemanticRefreshWorkflowReplacementPlan:
        """Build a canonical failed-precommit replacement plan."""

        selected = canonical_string_set(selected_mutating_node_ids, "selected_mutating_node_ids")
        plans = canonical_string_set(model_operation_plan_ids, "model_operation_plan_ids")
        outcomes = canonical_string_set(expected_model_outcome_ids, "expected_model_outcome_ids")
        action_ids = canonical_string_set(replacement_action_ids, "replacement_action_ids")
        actions = _canonical_actions(replacement_actions)
        unsigned = _unsigned_mapping(
            workflow_plan_sha256,
            predecessor_workflow_execution_id,
            predecessor_workflow_execution_binding_sha256,
            predecessor_workflow_summary_sha256,
            recovery_plan_digest,
            selected,
            plans,
            outcomes,
            action_ids,
            actions,
        )
        return cls(
            workflow_plan_sha256=workflow_plan_sha256,
            predecessor_workflow_execution_id=predecessor_workflow_execution_id,
            predecessor_workflow_execution_binding_sha256=predecessor_workflow_execution_binding_sha256,
            predecessor_workflow_summary_sha256=predecessor_workflow_summary_sha256,
            recovery_plan_digest=recovery_plan_digest,
            selected_mutating_node_ids=selected,
            model_operation_plan_ids=plans,
            expected_model_outcome_ids=outcomes,
            replacement_action_ids=action_ids,
            replacement_actions=actions,
            workflow_replacement_plan_sha256=semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshWorkflowReplacementPlan:
        """Parse a strict replacement plan and recompute its digest."""

        raw = require_closed_mapping(value, "workflow_replacement_plan", required=_FIELDS)
        raw_actions = raw.get("replacement_actions")
        if not isinstance(raw_actions, Sequence) or isinstance(raw_actions, str | bytes):
            raise SemanticRefreshContractError("replacement_actions must be an array")
        return cls(
            workflow_plan_sha256=require_digest(raw.get("workflow_plan_sha256"), "workflow_plan_sha256"),
            predecessor_workflow_execution_id=require_text(
                raw.get("predecessor_workflow_execution_id"),
                "predecessor_workflow_execution_id",
            ),
            predecessor_workflow_execution_binding_sha256=require_digest(
                raw.get("predecessor_workflow_execution_binding_sha256"),
                "predecessor_workflow_execution_binding_sha256",
            ),
            predecessor_workflow_summary_sha256=require_digest(
                raw.get("predecessor_workflow_summary_sha256"),
                "predecessor_workflow_summary_sha256",
            ),
            recovery_plan_digest=require_digest(raw.get("recovery_plan_digest"), "recovery_plan_digest"),
            selected_mutating_node_ids=require_sorted_unique_strings(
                raw.get("selected_mutating_node_ids"), "selected_mutating_node_ids"
            ),
            model_operation_plan_ids=require_sorted_unique_strings(
                raw.get("model_operation_plan_ids"), "model_operation_plan_ids"
            ),
            expected_model_outcome_ids=require_sorted_unique_strings(
                raw.get("expected_model_outcome_ids"), "expected_model_outcome_ids"
            ),
            replacement_action_ids=require_sorted_unique_strings(
                raw.get("replacement_action_ids"), "replacement_action_ids"
            ),
            replacement_actions=tuple(ReplacementActionBinding.from_mapping(item) for item in raw_actions),
            workflow_replacement_plan_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            workflow_mode=require_enum(raw.get("workflow_mode"), "workflow_mode", WorkflowMode),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.workflow_plan_sha256,
            self.predecessor_workflow_execution_id,
            self.predecessor_workflow_execution_binding_sha256,
            self.predecessor_workflow_summary_sha256,
            self.recovery_plan_digest,
            self.selected_mutating_node_ids,
            self.model_operation_plan_ids,
            self.expected_model_outcome_ids,
            self.replacement_action_ids,
            self.replacement_actions,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {
            **self._unsigned(),
            self.digest_field: self.workflow_replacement_plan_sha256,
        }


def _canonical_actions(values: tuple[ReplacementActionBinding, ...]) -> tuple[ReplacementActionBinding, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(item, ReplacementActionBinding) for item in values)
    ):
        raise SemanticRefreshContractError("replacement_actions must be a non-empty tuple")
    if len(values) != len(set(values)):
        raise SemanticRefreshContractError("replacement_actions must be unique")
    return tuple(sorted(values))


def _validate_replacement_closure(plan: SemanticRefreshWorkflowReplacementPlan) -> None:
    identifiers = (
        plan.selected_mutating_node_ids,
        plan.model_operation_plan_ids,
        plan.expected_model_outcome_ids,
        plan.replacement_action_ids,
    )
    for values, field_name in zip(
        identifiers,
        (
            "selected_mutating_node_ids",
            "model_operation_plan_ids",
            "expected_model_outcome_ids",
            "replacement_action_ids",
        ),
        strict=True,
    ):
        require_sorted_unique_strings(list(values), field_name)
    if len(set(identifiers)) != 1:
        raise SemanticRefreshContractError("replacement model closure identifiers must be equal")
    actions = _canonical_actions(plan.replacement_actions)
    if actions != plan.replacement_actions:
        raise SemanticRefreshContractError("replacement_actions must be canonically ordered")
    if tuple(action.action_id for action in actions) != plan.replacement_action_ids:
        raise SemanticRefreshContractError("replacement actions must cover the exact model closure")


def _unsigned_mapping(
    workflow_plan_sha256: str,
    predecessor_workflow_execution_id: str,
    predecessor_workflow_execution_binding_sha256: str,
    predecessor_workflow_summary_sha256: str,
    recovery_plan_digest: str,
    selected_mutating_node_ids: tuple[str, ...],
    model_operation_plan_ids: tuple[str, ...],
    expected_model_outcome_ids: tuple[str, ...],
    replacement_action_ids: tuple[str, ...],
    replacement_actions: tuple[ReplacementActionBinding, ...],
) -> dict[str, object]:
    return {
        "expected_model_outcome_ids": list(expected_model_outcome_ids),
        "model_operation_plan_ids": list(model_operation_plan_ids),
        "predecessor_workflow_execution_id": predecessor_workflow_execution_id,
        "predecessor_workflow_execution_binding_sha256": predecessor_workflow_execution_binding_sha256,
        "predecessor_workflow_summary_sha256": predecessor_workflow_summary_sha256,
        "recovery_plan_digest": recovery_plan_digest,
        "replacement_action_ids": list(replacement_action_ids),
        "replacement_actions": [action.to_dict() for action in replacement_actions],
        "schema": WORKFLOW_REPLACEMENT_PLAN_SCHEMA,
        "selected_mutating_node_ids": list(selected_mutating_node_ids),
        "workflow_mode": WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value,
        "workflow_plan_sha256": workflow_plan_sha256,
    }


__all__ = ["SemanticRefreshWorkflowReplacementPlan", "WORKFLOW_REPLACEMENT_PLAN_SCHEMA"]
