"""Normal and complete-scope-replay semantic workflow plans."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    canonical_string_set,
    require_closed_mapping,
    require_daily_scope,
    require_digest,
    require_enum,
    require_positive_int,
    require_sorted_unique_strings,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_plan_refs import OperationPlanReference
from dpone.contracts.semantic_refresh_types import WorkflowMode

WORKFLOW_PLAN_SCHEMA = "dpone.semantic-refresh-workflow-plan.v1"
_DIGEST_FIELD = "workflow_plan_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "workflow_name",
        "workflow_mode",
        "scope_start",
        "scope_end",
        "scope_revision",
        "mutation_closure_sha256",
        "selected_mutating_node_ids",
        "model_operation_plan_ids",
        "expected_model_outcome_ids",
        "replacement_action_ids",
        "operation_plan_refs",
    }
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshWorkflowPlan(SemanticRefreshDocumentCodec):
    """Semantic plan that intentionally has no recovery-plan field."""

    workflow_name: str
    workflow_mode: WorkflowMode
    scope_start: str
    scope_end: str
    scope_revision: int
    mutation_closure_sha256: str
    selected_mutating_node_ids: tuple[str, ...]
    model_operation_plan_ids: tuple[str, ...]
    expected_model_outcome_ids: tuple[str, ...]
    replacement_action_ids: tuple[str, ...]
    operation_plan_refs: tuple[OperationPlanReference, ...]
    workflow_plan_sha256: str
    schema: str = WORKFLOW_PLAN_SCHEMA

    schema_id: ClassVar[str] = WORKFLOW_PLAN_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        require_text(self.workflow_name, "workflow_name")
        if not isinstance(self.workflow_mode, WorkflowMode):
            raise SemanticRefreshContractError("workflow plan mode is unsupported")
        require_daily_scope(self.scope_start, self.scope_end)
        require_positive_int(self.scope_revision, "scope_revision")
        if self.workflow_mode is WorkflowMode.NORMAL and self.scope_revision != 1:
            raise SemanticRefreshContractError("normal workflow requires scope revision 1")
        if self.workflow_mode is WorkflowMode.COMPLETE_SCOPE_REPLAY and self.scope_revision < 2:
            raise SemanticRefreshContractError("complete-scope replay requires scope revision n+1")
        require_digest(self.mutation_closure_sha256, "mutation_closure_sha256")
        _validate_model_closure(self)
        validate_digest(self._unsigned(), self.workflow_plan_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        workflow_name: str,
        workflow_mode: WorkflowMode,
        scope_start: str,
        scope_end: str,
        scope_revision: int,
        mutation_closure_sha256: str,
        selected_mutating_node_ids: tuple[str, ...],
        model_operation_plan_ids: tuple[str, ...],
        expected_model_outcome_ids: tuple[str, ...],
        replacement_action_ids: tuple[str, ...],
        operation_plan_refs: tuple[OperationPlanReference, ...],
    ) -> SemanticRefreshWorkflowPlan:
        """Build a canonical workflow plan from model-keyed references."""

        model_sets = _canonical_model_sets(
            selected_mutating_node_ids,
            model_operation_plan_ids,
            expected_model_outcome_ids,
            replacement_action_ids,
        )
        refs = _canonical_operation_refs(operation_plan_refs)
        unsigned = _unsigned_mapping(
            workflow_name,
            workflow_mode,
            scope_start,
            scope_end,
            scope_revision,
            mutation_closure_sha256,
            model_sets[0],
            model_sets[1],
            model_sets[2],
            model_sets[3],
            refs,
        )
        return cls(
            workflow_name=workflow_name,
            workflow_mode=workflow_mode,
            scope_start=scope_start,
            scope_end=scope_end,
            scope_revision=scope_revision,
            mutation_closure_sha256=mutation_closure_sha256,
            selected_mutating_node_ids=model_sets[0],
            model_operation_plan_ids=model_sets[1],
            expected_model_outcome_ids=model_sets[2],
            replacement_action_ids=model_sets[3],
            operation_plan_refs=refs,
            workflow_plan_sha256=semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshWorkflowPlan:
        """Parse a strict recovery-free semantic workflow plan."""

        raw = require_closed_mapping(value, "workflow_plan", required=_FIELDS)
        raw_refs = raw.get("operation_plan_refs")
        if not isinstance(raw_refs, Sequence) or isinstance(raw_refs, str | bytes):
            raise SemanticRefreshContractError("operation_plan_refs must be an array")
        return cls(
            workflow_name=require_text(raw.get("workflow_name"), "workflow_name"),
            workflow_mode=require_enum(raw.get("workflow_mode"), "workflow_mode", WorkflowMode),
            scope_start=require_text(raw.get("scope_start"), "scope_start"),
            scope_end=require_text(raw.get("scope_end"), "scope_end"),
            scope_revision=require_positive_int(raw.get("scope_revision"), "scope_revision"),
            mutation_closure_sha256=require_digest(raw.get("mutation_closure_sha256"), "mutation_closure_sha256"),
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
                raw.get("replacement_action_ids"), "replacement_action_ids", allow_empty=True
            ),
            operation_plan_refs=tuple(OperationPlanReference.from_mapping(item) for item in raw_refs),
            workflow_plan_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.workflow_name,
            self.workflow_mode,
            self.scope_start,
            self.scope_end,
            self.scope_revision,
            self.mutation_closure_sha256,
            self.selected_mutating_node_ids,
            self.model_operation_plan_ids,
            self.expected_model_outcome_ids,
            self.replacement_action_ids,
            self.operation_plan_refs,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.workflow_plan_sha256}


def _validate_model_closure(plan: SemanticRefreshWorkflowPlan) -> None:
    identifiers = (
        plan.selected_mutating_node_ids,
        plan.model_operation_plan_ids,
        plan.expected_model_outcome_ids,
    )
    for values, field_name in zip(
        identifiers,
        ("selected_mutating_node_ids", "model_operation_plan_ids", "expected_model_outcome_ids"),
        strict=True,
    ):
        require_sorted_unique_strings(list(values), field_name)
    if len(set(identifiers)) != 1:
        raise SemanticRefreshContractError("workflow model closure identifiers must be equal")
    is_replacement = plan.workflow_mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT
    if is_replacement and plan.replacement_action_ids != plan.selected_mutating_node_ids:
        raise SemanticRefreshContractError("replacement workflow plan requires equal action identifiers")
    if not is_replacement and plan.replacement_action_ids:
        raise SemanticRefreshContractError("normal/replay workflow plans require empty replacement_action_ids")
    refs = _canonical_operation_refs(plan.operation_plan_refs)
    if refs != plan.operation_plan_refs:
        raise SemanticRefreshContractError("operation_plan_refs must be canonically ordered")
    if tuple(ref.model_unique_id for ref in refs) != plan.model_operation_plan_ids:
        raise SemanticRefreshContractError("operation plan references must cover the exact model closure")


def _canonical_model_sets(
    selected: tuple[str, ...],
    plans: tuple[str, ...],
    outcomes: tuple[str, ...],
    actions: tuple[str, ...],
) -> tuple[tuple[str, ...], ...]:
    return (
        canonical_string_set(selected, "selected_mutating_node_ids"),
        canonical_string_set(plans, "model_operation_plan_ids"),
        canonical_string_set(outcomes, "expected_model_outcome_ids"),
        canonical_string_set(actions, "replacement_action_ids", allow_empty=True),
    )


def _canonical_operation_refs(values: tuple[OperationPlanReference, ...]) -> tuple[OperationPlanReference, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(item, OperationPlanReference) for item in values)
    ):
        raise SemanticRefreshContractError("operation_plan_refs must be a non-empty tuple")
    if len(values) != len(set(values)):
        raise SemanticRefreshContractError("operation_plan_refs must be unique")
    return tuple(sorted(values))


def _unsigned_mapping(
    workflow_name: str,
    workflow_mode: WorkflowMode,
    scope_start: str,
    scope_end: str,
    scope_revision: int,
    mutation_closure_sha256: str,
    selected_mutating_node_ids: tuple[str, ...],
    model_operation_plan_ids: tuple[str, ...],
    expected_model_outcome_ids: tuple[str, ...],
    replacement_action_ids: tuple[str, ...],
    operation_plan_refs: tuple[OperationPlanReference, ...],
) -> dict[str, object]:
    return {
        "expected_model_outcome_ids": list(expected_model_outcome_ids),
        "model_operation_plan_ids": list(model_operation_plan_ids),
        "mutation_closure_sha256": mutation_closure_sha256,
        "operation_plan_refs": [reference.to_dict() for reference in operation_plan_refs],
        "replacement_action_ids": list(replacement_action_ids),
        "schema": WORKFLOW_PLAN_SCHEMA,
        "scope_end": scope_end,
        "scope_revision": scope_revision,
        "scope_start": scope_start,
        "selected_mutating_node_ids": list(selected_mutating_node_ids),
        "workflow_mode": workflow_mode.value,
        "workflow_name": workflow_name,
    }


__all__ = ["SemanticRefreshWorkflowPlan", "WORKFLOW_PLAN_SCHEMA"]
