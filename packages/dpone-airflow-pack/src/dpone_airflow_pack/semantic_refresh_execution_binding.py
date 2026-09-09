"""Parse-safe authentication for semantic-refresh execution bindings."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REQUIRED_FIELDS = frozenset(
    {
        "binding_set_ref",
        "connection_registry_ref",
        "credential_runtime_ref",
        "deployment_id",
        "expected_model_outcome_ids",
        "model_operation_plan_ids",
        "replacement_action_ids",
        "schema",
        "selected_mutating_node_ids",
        "workflow_execution_binding_sha256",
        "workflow_execution_id",
        "workflow_mode",
        "workflow_plan_sha256",
    }
)
_OPTIONAL_FIELDS = frozenset({"recovery_plan_digest", "workflow_replacement_plan_sha256"})
_MODES = frozenset({"normal", "failed_precommit_replacement", "complete_scope_replay"})


class SemanticRefreshExecutionBindingError(ValueError):
    """Raised when a deployment-bound execution document is not authentic."""


@dataclass(frozen=True)
class SemanticRefreshExecutionBinding:
    """Minimal authenticated projection of the canonical public binding."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    workflow_plan_sha256: str
    model_unique_ids: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> SemanticRefreshExecutionBinding:
        """Authenticate a closed canonical execution binding without importing dpone."""

        if not isinstance(value, Mapping):
            raise SemanticRefreshExecutionBindingError("execution binding must be a mapping")
        fields = set(value)
        if not _REQUIRED_FIELDS.issubset(fields) or fields - _REQUIRED_FIELDS - _OPTIONAL_FIELDS:
            raise SemanticRefreshExecutionBindingError("execution binding fields are not closed")
        if value["schema"] != "dpone.semantic-refresh-workflow-execution-binding.v1":
            raise SemanticRefreshExecutionBindingError("execution binding schema is invalid")
        digest = _digest(value["workflow_execution_binding_sha256"], "workflow_execution_binding_sha256")
        unsigned = {key: raw for key, raw in value.items() if key != "workflow_execution_binding_sha256"}
        if _canonical_digest(unsigned) != digest:
            raise SemanticRefreshExecutionBindingError("execution binding digest differs")
        workflow_execution_id = _text(value["workflow_execution_id"], "workflow_execution_id")
        workflow_plan_sha256 = _digest(value["workflow_plan_sha256"], "workflow_plan_sha256")
        _digest(value["deployment_id"], "deployment_id")
        for field in ("binding_set_ref", "connection_registry_ref", "credential_runtime_ref"):
            _text(value[field], field)
        mode = value["workflow_mode"]
        if mode not in _MODES:
            raise SemanticRefreshExecutionBindingError("execution binding workflow_mode is invalid")
        selected = _canonical_text_sequence(value["selected_mutating_node_ids"], "selected_mutating_node_ids")
        plans = _canonical_text_sequence(value["model_operation_plan_ids"], "model_operation_plan_ids")
        outcomes = _canonical_text_sequence(value["expected_model_outcome_ids"], "expected_model_outcome_ids")
        actions = _canonical_text_sequence(value["replacement_action_ids"], "replacement_action_ids", allow_empty=True)
        if selected != plans or selected != outcomes:
            raise SemanticRefreshExecutionBindingError("execution binding model closure differs")
        is_replacement = mode == "failed_precommit_replacement"
        has_replacement_digests = all(
            field in value and _is_digest(value[field])
            for field in ("workflow_replacement_plan_sha256", "recovery_plan_digest")
        )
        if is_replacement and (actions != selected or not has_replacement_digests):
            raise SemanticRefreshExecutionBindingError("replacement execution binding closure is invalid")
        if not is_replacement and (
            actions or "workflow_replacement_plan_sha256" in value or "recovery_plan_digest" in value
        ):
            raise SemanticRefreshExecutionBindingError("non-replacement binding contains replacement authority")
        return cls(
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=digest,
            workflow_plan_sha256=workflow_plan_sha256,
            model_unique_ids=selected,
        )


def _canonical_text_sequence(value: object, field: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise SemanticRefreshExecutionBindingError(f"{field} must be an array")
    result = tuple(_text(item, field) for item in value)
    if (not result and not allow_empty) or result != tuple(sorted(set(result))):
        raise SemanticRefreshExecutionBindingError(f"{field} must be sorted and unique")
    return result


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SemanticRefreshExecutionBindingError(f"{field} must be non-empty text")
    return value


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST_RE.fullmatch(value) is not None


def _digest(value: object, field: str) -> str:
    if not _is_digest(value):
        raise SemanticRefreshExecutionBindingError(f"{field} must be a canonical digest")
    return str(value)


def _canonical_digest(value: Mapping[str, object]) -> str:
    try:
        raw = json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshExecutionBindingError("execution binding is not canonical JSON") from exc
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = ["SemanticRefreshExecutionBinding", "SemanticRefreshExecutionBindingError"]
