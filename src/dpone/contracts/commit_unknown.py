"""Public connector-neutral outcome for an unproven target mutation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CommitUnknownBoundary = Literal["target_invocation", "checkpoint_persistence"]
CommitUnknownCheckpointState = Literal["not_advanced", "incomplete"]


@dataclass(frozen=True, slots=True)
class CommitUnknownOutcome:
    """Bounded facts known after mutation may have occurred without proof."""

    failure_boundary: CommitUnknownBoundary
    checkpoint_state: CommitUnknownCheckpointState
    status: Literal["COMMIT_UNKNOWN"] = "COMMIT_UNKNOWN"
    target_state: Literal["unknown"] = "unknown"
    source_state: Literal["not_advanced"] = "not_advanced"
    safe_to_retry: Literal[False] = False
    operator_verification_required: Literal[True] = True
    recovery_action: Literal["operator_verification_required"] = "operator_verification_required"

    def to_jsonable(self) -> dict[str, object]:
        """Return the stable redaction-safe failure payload."""

        return {
            "status": self.status,
            "failure_boundary": self.failure_boundary,
            "target_state": self.target_state,
            "checkpoint_state": self.checkpoint_state,
            "source_state": self.source_state,
            "safe_to_retry": self.safe_to_retry,
            "operator_verification_required": self.operator_verification_required,
            "recovery_action": self.recovery_action,
        }


__all__ = [
    "CommitUnknownBoundary",
    "CommitUnknownCheckpointState",
    "CommitUnknownOutcome",
]
