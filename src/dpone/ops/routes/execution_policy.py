"""Idempotent route commit protocol policy."""

from __future__ import annotations

from dpone.ops.routes.execution_models import (
    RouteExecutionDecision,
    RouteExecutionStage,
    RouteExecutionStatus,
    RouteExecutionStep,
)

_DURABLE_SINK_STAGES = {
    RouteExecutionStage.LOADED_TO_STAGING,
    RouteExecutionStage.FINALIZED,
    RouteExecutionStage.QUALITY_CHECKED,
}

_NEXT_ACTIONS = {
    "route_execution.idempotency_conflict": "Use a new idempotency key or replay the exact same step payload.",
    "route_execution.lease_held_by_another_runner": (
        "Wait for the active runner lease to expire or rerun with the same runner id after confirming ownership."
    ),
    "route_execution.stage_regression": "Resume from the latest recorded stage or start a new approved run id.",
    "route_execution.step_failed": "Fix the failed stage before appending later protocol steps.",
    "route_execution.state_commit_before_sink_success": (
        "Record a successful `loaded_to_staging`, `finalized`, or `quality_checked` step before committing state."
    ),
    "route_execution.concurrent_write_conflict": (
        "Re-read the shared ledger and retry with the latest step version before appending another side effect."
    ),
}


class RouteCommitProtocolPolicy:
    """Evaluate one append attempt against prior route ledger steps."""

    def evaluate(
        self,
        *,
        existing_steps: tuple[RouteExecutionStep, ...],
        candidate: RouteExecutionStep,
        blockers: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
    ) -> RouteExecutionDecision:
        all_blockers = [*blockers]
        all_warnings = [*warnings]

        if existing_steps and not existing_steps[-1].stage.can_transition_to(candidate.stage):
            all_blockers.append("route_execution.stage_regression")
        if candidate.status == RouteExecutionStatus.FAILED:
            all_blockers.append("route_execution.step_failed")
        if candidate.stage == RouteExecutionStage.STATE_COMMITTED and not self._has_durable_sink_success(
            existing_steps, candidate
        ):
            all_blockers.append("route_execution.state_commit_before_sink_success")

        unique_blockers = tuple(dict.fromkeys(all_blockers))
        unique_warnings = tuple(dict.fromkeys(all_warnings))
        return RouteExecutionDecision(
            passed=not unique_blockers,
            level=self._level(candidate, unique_blockers),
            blockers=unique_blockers,
            warnings=unique_warnings,
            next_actions=tuple(_NEXT_ACTIONS[item] for item in unique_blockers if item in _NEXT_ACTIONS),
        )

    @staticmethod
    def _has_durable_sink_success(
        existing_steps: tuple[RouteExecutionStep, ...],
        candidate: RouteExecutionStep,
    ) -> bool:
        steps = (*existing_steps, candidate)
        return any(step.stage in _DURABLE_SINK_STAGES and step.status.is_successful for step in steps)

    @staticmethod
    def _level(candidate: RouteExecutionStep, blockers: tuple[str, ...]) -> str:
        if blockers:
            return "blocked"
        if candidate.stage == RouteExecutionStage.STATE_COMMITTED:
            return "committed"
        if candidate.status == RouteExecutionStatus.RUNNING:
            return "in_progress"
        return "recorded"
