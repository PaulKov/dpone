"""Connector-neutral outcome for an unproven target mutation."""

from __future__ import annotations

from typing import Any

from dpone.contracts.commit_unknown import (
    CommitUnknownBoundary,
    CommitUnknownCheckpointState,
    CommitUnknownOutcome,
)
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome


class CommitUnknownError(RuntimeError):
    """Stop automatic retry when target completion cannot be proven."""

    code = "COMMIT_UNKNOWN"
    safe_to_retry = False
    operator_verification_required = True
    artifact_terminal_outcome = ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN

    def __init__(self, outcome: CommitUnknownOutcome) -> None:
        self.outcome = outcome
        super().__init__("target commit outcome is unknown; operator verification is required")


def classify_commit_unknown(
    *,
    target_invocation_started: bool,
    target_returned_success: bool,
    expected_checkpoint_ids: frozenset[str],
    committed_checkpoint_ids: frozenset[str],
) -> CommitUnknownError | None:
    """Classify only the interval between target invocation and durable proof."""

    if not target_invocation_started:
        return None
    checkpoint_complete = bool(expected_checkpoint_ids) and expected_checkpoint_ids.issubset(committed_checkpoint_ids)
    if target_returned_success and checkpoint_complete:
        return None
    checkpoint_state: CommitUnknownCheckpointState = "incomplete" if committed_checkpoint_ids else "not_advanced"
    boundary: CommitUnknownBoundary = "checkpoint_persistence" if target_returned_success else "target_invocation"
    return CommitUnknownError(
        CommitUnknownOutcome(
            failure_boundary=boundary,
            checkpoint_state=checkpoint_state,
        )
    )


def classify_runtime_commit_unknown(
    runtime_service: Any,
    context: Any,
    exception: Exception,
) -> CommitUnknownError | None:
    """Adapt an optional runtime capability to the canonical failure outcome."""

    if isinstance(exception, CommitUnknownError):
        return exception
    classify = getattr(runtime_service, "commit_unknown_error", None)
    if not callable(classify):
        return None
    failure = classify(context)
    return failure if isinstance(failure, CommitUnknownError) else None


__all__ = [
    "CommitUnknownBoundary",
    "CommitUnknownCheckpointState",
    "CommitUnknownError",
    "CommitUnknownOutcome",
    "classify_commit_unknown",
    "classify_runtime_commit_unknown",
]
