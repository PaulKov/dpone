"""Closed nested references shared by semantic-refresh workflow plans."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_text,
)
from dpone.contracts.semantic_refresh_types import (
    ReplacementAction,
    SqlServerModelOutcome,
    replacement_action_for,
)

_OPERATION_REF_FIELDS = frozenset({"model_unique_id", "operation_id", "operation_plan_sha256"})
_ACTION_FIELDS = frozenset({"action_id", "outcome", "action"})


@dataclass(frozen=True, order=True, slots=True)
class OperationPlanReference:
    """One model-keyed operation identity and immutable plan digest."""

    model_unique_id: str
    operation_id: str
    operation_plan_sha256: str

    def __post_init__(self) -> None:
        require_text(self.model_unique_id, "model_unique_id")
        require_digest(self.operation_id, "operation_id")
        require_digest(self.operation_plan_sha256, "operation_plan_sha256")

    @classmethod
    def from_mapping(cls, value: object) -> OperationPlanReference:
        """Parse one closed operation-plan reference."""

        raw = require_closed_mapping(value, "operation_plan_ref", required=_OPERATION_REF_FIELDS)
        return cls(
            require_text(raw.get("model_unique_id"), "model_unique_id"),
            require_digest(raw.get("operation_id"), "operation_id"),
            require_digest(raw.get("operation_plan_sha256"), "operation_plan_sha256"),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed public reference mapping."""

        return {
            "model_unique_id": self.model_unique_id,
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
        }


@dataclass(frozen=True, order=True, slots=True)
class ReplacementActionBinding:
    """A model outcome and its only safe failed-precommit action."""

    action_id: str
    outcome: SqlServerModelOutcome
    action: ReplacementAction

    def __post_init__(self) -> None:
        require_text(self.action_id, "action_id")
        if not isinstance(self.outcome, SqlServerModelOutcome):
            raise SemanticRefreshContractError("replacement outcome is unsupported")
        if not isinstance(self.action, ReplacementAction):
            raise SemanticRefreshContractError("replacement action is unsupported")
        if self.action is not replacement_action_for(self.outcome):
            raise SemanticRefreshContractError("replacement action differs from its durable MSSQL outcome")

    @classmethod
    def from_mapping(cls, value: object) -> ReplacementActionBinding:
        """Parse one closed replacement action."""

        raw = require_closed_mapping(value, "replacement_action", required=_ACTION_FIELDS)
        return cls(
            action_id=require_text(raw.get("action_id"), "action_id"),
            outcome=require_enum(raw.get("outcome"), "outcome", SqlServerModelOutcome),
            action=require_enum(raw.get("action"), "action", ReplacementAction),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed public action mapping."""

        return {
            "action": self.action.value,
            "action_id": self.action_id,
            "outcome": self.outcome.value,
        }


__all__ = ["OperationPlanReference", "ReplacementActionBinding"]
