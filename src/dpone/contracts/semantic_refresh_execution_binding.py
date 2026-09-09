"""Deployment-bound semantic-refresh workflow execution contract."""

from __future__ import annotations

from collections.abc import Mapping
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
from dpone.contracts.semantic_refresh_types import WorkflowMode

WORKFLOW_EXECUTION_BINDING_SCHEMA = "dpone.semantic-refresh-workflow-execution-binding.v1"
_DIGEST_FIELD = "workflow_execution_binding_sha256"
_REQUIRED_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "workflow_execution_id",
        "workflow_mode",
        "workflow_plan_sha256",
        "selected_mutating_node_ids",
        "model_operation_plan_ids",
        "expected_model_outcome_ids",
        "replacement_action_ids",
        "deployment_id",
        "binding_set_ref",
        "connection_registry_ref",
        "credential_runtime_ref",
    }
)
_OPTIONAL_FIELDS = frozenset({"workflow_replacement_plan_sha256", "recovery_plan_digest"})


@dataclass(frozen=True, slots=True)
class SemanticRefreshWorkflowExecutionBinding(SemanticRefreshDocumentCodec):
    """Bind a semantic workflow branch to immutable deployment authorities."""

    workflow_execution_id: str
    workflow_mode: WorkflowMode
    workflow_plan_sha256: str
    selected_mutating_node_ids: tuple[str, ...]
    model_operation_plan_ids: tuple[str, ...]
    expected_model_outcome_ids: tuple[str, ...]
    replacement_action_ids: tuple[str, ...]
    deployment_id: str
    binding_set_ref: str
    connection_registry_ref: str
    credential_runtime_ref: str
    workflow_execution_binding_sha256: str
    workflow_replacement_plan_sha256: str | None = None
    recovery_plan_digest: str | None = None
    schema: str = WORKFLOW_EXECUTION_BINDING_SCHEMA

    schema_id: ClassVar[str] = WORKFLOW_EXECUTION_BINDING_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if not isinstance(self.workflow_mode, WorkflowMode):
            raise SemanticRefreshContractError("workflow_mode is unsupported")
        require_text(self.workflow_execution_id, "workflow_execution_id")
        require_digest(self.workflow_plan_sha256, "workflow_plan_sha256")
        require_digest(self.deployment_id, "deployment_id")
        for field_name in ("binding_set_ref", "connection_registry_ref", "credential_runtime_ref"):
            require_text(getattr(self, field_name), field_name)
        _validate_mode_closure(self)
        validate_digest(
            self._unsigned(),
            self.workflow_execution_binding_sha256,
            self.digest_field,
        )

    @classmethod
    def build(
        cls,
        *,
        workflow_execution_id: str,
        workflow_mode: WorkflowMode,
        workflow_plan_sha256: str,
        selected_mutating_node_ids: tuple[str, ...],
        model_operation_plan_ids: tuple[str, ...],
        expected_model_outcome_ids: tuple[str, ...],
        replacement_action_ids: tuple[str, ...],
        deployment_id: str,
        binding_set_ref: str,
        connection_registry_ref: str,
        credential_runtime_ref: str,
        workflow_replacement_plan_sha256: str | None = None,
        recovery_plan_digest: str | None = None,
    ) -> SemanticRefreshWorkflowExecutionBinding:
        """Build an execution binding for exactly one workflow mode branch."""

        selected = canonical_string_set(selected_mutating_node_ids, "selected_mutating_node_ids")
        plans = canonical_string_set(model_operation_plan_ids, "model_operation_plan_ids")
        outcomes = canonical_string_set(expected_model_outcome_ids, "expected_model_outcome_ids")
        action_ids = canonical_string_set(
            replacement_action_ids,
            "replacement_action_ids",
            allow_empty=True,
        )
        unsigned = _unsigned_mapping(
            workflow_execution_id,
            workflow_mode,
            workflow_plan_sha256,
            selected,
            plans,
            outcomes,
            action_ids,
            deployment_id,
            binding_set_ref,
            connection_registry_ref,
            credential_runtime_ref,
            workflow_replacement_plan_sha256,
            recovery_plan_digest,
        )
        return cls(
            workflow_execution_id=workflow_execution_id,
            workflow_mode=workflow_mode,
            workflow_plan_sha256=workflow_plan_sha256,
            selected_mutating_node_ids=selected,
            model_operation_plan_ids=plans,
            expected_model_outcome_ids=outcomes,
            replacement_action_ids=action_ids,
            deployment_id=deployment_id,
            binding_set_ref=binding_set_ref,
            connection_registry_ref=connection_registry_ref,
            credential_runtime_ref=credential_runtime_ref,
            workflow_execution_binding_sha256=semantic_refresh_sha256(unsigned),
            workflow_replacement_plan_sha256=workflow_replacement_plan_sha256,
            recovery_plan_digest=recovery_plan_digest,
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshWorkflowExecutionBinding:
        """Parse a strict execution binding with conditional branch fields."""

        raw = require_closed_mapping(
            value,
            "workflow_execution_binding",
            required=_REQUIRED_FIELDS,
            optional=_OPTIONAL_FIELDS,
        )
        return cls(
            workflow_execution_id=require_text(raw.get("workflow_execution_id"), "workflow_execution_id"),
            workflow_mode=require_enum(raw.get("workflow_mode"), "workflow_mode", WorkflowMode),
            workflow_plan_sha256=require_digest(raw.get("workflow_plan_sha256"), "workflow_plan_sha256"),
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
            deployment_id=require_digest(raw.get("deployment_id"), "deployment_id"),
            binding_set_ref=require_text(raw.get("binding_set_ref"), "binding_set_ref"),
            connection_registry_ref=require_text(raw.get("connection_registry_ref"), "connection_registry_ref"),
            credential_runtime_ref=require_text(raw.get("credential_runtime_ref"), "credential_runtime_ref"),
            workflow_execution_binding_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            workflow_replacement_plan_sha256=_optional_digest(raw, "workflow_replacement_plan_sha256"),
            recovery_plan_digest=_optional_digest(raw, "recovery_plan_digest"),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.workflow_execution_id,
            self.workflow_mode,
            self.workflow_plan_sha256,
            self.selected_mutating_node_ids,
            self.model_operation_plan_ids,
            self.expected_model_outcome_ids,
            self.replacement_action_ids,
            self.deployment_id,
            self.binding_set_ref,
            self.connection_registry_ref,
            self.credential_runtime_ref,
            self.workflow_replacement_plan_sha256,
            self.recovery_plan_digest,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.workflow_execution_binding_sha256}


def _optional_digest(raw: Mapping[str, object], field_name: str) -> str | None:
    if field_name not in raw:
        return None
    return require_digest(raw[field_name], field_name)


def _validate_mode_closure(binding: SemanticRefreshWorkflowExecutionBinding) -> None:
    identifiers = (
        binding.selected_mutating_node_ids,
        binding.model_operation_plan_ids,
        binding.expected_model_outcome_ids,
    )
    for values, field_name in zip(
        identifiers,
        ("selected_mutating_node_ids", "model_operation_plan_ids", "expected_model_outcome_ids"),
        strict=True,
    ):
        require_sorted_unique_strings(list(values), field_name)
    if len(set(identifiers)) != 1:
        raise SemanticRefreshContractError("execution model closure identifiers must be equal")
    is_replacement = binding.workflow_mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT
    if is_replacement:
        if binding.replacement_action_ids != binding.selected_mutating_node_ids:
            raise SemanticRefreshContractError("replacement execution requires equal action identifiers")
        require_digest(binding.workflow_replacement_plan_sha256, "workflow_replacement_plan_sha256")
        require_digest(binding.recovery_plan_digest, "recovery_plan_digest")
    elif (
        binding.replacement_action_ids
        or binding.workflow_replacement_plan_sha256 is not None
        or binding.recovery_plan_digest is not None
    ):
        raise SemanticRefreshContractError("normal/replay execution cannot contain replacement fields")


def _unsigned_mapping(
    workflow_execution_id: str,
    workflow_mode: WorkflowMode,
    workflow_plan_sha256: str,
    selected_mutating_node_ids: tuple[str, ...],
    model_operation_plan_ids: tuple[str, ...],
    expected_model_outcome_ids: tuple[str, ...],
    replacement_action_ids: tuple[str, ...],
    deployment_id: str,
    binding_set_ref: str,
    connection_registry_ref: str,
    credential_runtime_ref: str,
    workflow_replacement_plan_sha256: str | None,
    recovery_plan_digest: str | None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "binding_set_ref": binding_set_ref,
        "connection_registry_ref": connection_registry_ref,
        "credential_runtime_ref": credential_runtime_ref,
        "deployment_id": deployment_id,
        "expected_model_outcome_ids": list(expected_model_outcome_ids),
        "model_operation_plan_ids": list(model_operation_plan_ids),
        "replacement_action_ids": list(replacement_action_ids),
        "schema": WORKFLOW_EXECUTION_BINDING_SCHEMA,
        "selected_mutating_node_ids": list(selected_mutating_node_ids),
        "workflow_execution_id": workflow_execution_id,
        "workflow_mode": workflow_mode.value,
        "workflow_plan_sha256": workflow_plan_sha256,
    }
    if workflow_replacement_plan_sha256 is not None:
        result["workflow_replacement_plan_sha256"] = workflow_replacement_plan_sha256
    if recovery_plan_digest is not None:
        result["recovery_plan_digest"] = recovery_plan_digest
    return result


__all__ = ["SemanticRefreshWorkflowExecutionBinding", "WORKFLOW_EXECUTION_BINDING_SCHEMA"]
