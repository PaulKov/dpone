"""Fail-closed policy for route release-candidate execution receipts."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.ops.routes.rc_executor_models import (
    RouteRcExecutionArtifact,
    RouteRcExecutionDecision,
    RouteRcExecutionStep,
)


class RouteRcExecutionPolicy:
    """Evaluate command execution and expected artifact collection."""

    def evaluate(
        self,
        *,
        mode: str,
        orchestration_passed: bool,
        orchestration_blockers: Sequence[str],
        steps: Sequence[RouteRcExecutionStep],
        artifacts: Sequence[RouteRcExecutionArtifact],
    ) -> RouteRcExecutionDecision:
        blockers = list(orchestration_blockers)
        if not orchestration_passed:
            blockers.append("orchestration.not_passed")
        blockers.extend(_step_blockers(steps))
        if mode == "execute":
            blockers.extend(_artifact_blockers(artifacts))
        warnings = tuple(
            dict.fromkeys(
                (
                    *(f"{step.name}.not_passed" for step in steps if not step.required and not step.passed),
                    *(
                        f"{artifact.name}.missing"
                        for artifact in artifacts
                        if not artifact.required and not artifact.exists
                    ),
                )
            )
        )
        normalized_blockers = tuple(dict.fromkeys(blockers))
        return RouteRcExecutionDecision(
            passed=not normalized_blockers,
            level="executed"
            if mode == "execute" and not normalized_blockers
            else ("planned" if not normalized_blockers else "blocked"),
            score=_score(mode=mode, steps=tuple(steps), artifacts=tuple(artifacts)),
            blockers=normalized_blockers,
            warnings=warnings,
            next_actions=_next_actions(normalized_blockers),
        )


def _step_blockers(steps: Sequence[RouteRcExecutionStep]) -> tuple[str, ...]:
    blockers: list[str] = []
    for step in steps:
        if not step.required:
            continue
        if step.status == "failed":
            blockers.append(f"{step.name}.command_failed")
        elif step.status == "timed_out":
            blockers.append(f"{step.name}.timed_out")
        elif step.status == "skipped":
            blockers.append(f"{step.name}.skipped")
        blockers.extend(step.blockers)
    return tuple(blockers)


def _artifact_blockers(artifacts: Sequence[RouteRcExecutionArtifact]) -> tuple[str, ...]:
    return tuple(
        f"{artifact.name}.artifact_missing" for artifact in artifacts if artifact.required and not artifact.exists
    )


def _score(
    *,
    mode: str,
    steps: tuple[RouteRcExecutionStep, ...],
    artifacts: tuple[RouteRcExecutionArtifact, ...],
) -> float:
    required_units = [step.passed for step in steps if step.required]
    if mode == "execute":
        required_units.extend(artifact.exists for artifact in artifacts if artifact.required)
    if not required_units:
        return 100.0
    return round((sum(1 for passed in required_units if passed) / len(required_units)) * 100.0, 2)


def _next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        name, _, reason = blocker.partition(".")
        if reason == "command_failed":
            actions.append(f"Open `{name}` captured stderr/stdout and rerun after fixing the failing command.")
        elif reason == "timed_out":
            actions.append(f"Increase timeout or reduce the workload for `{name}` before rerunning.")
        elif reason == "artifact_missing":
            actions.append(f"Regenerate `{name}` and ensure the expected artifact path is written.")
        elif reason == "skipped":
            actions.append(f"Re-run the route RC executor after earlier required steps pass so `{name}` can execute.")
        elif blocker == "orchestration.not_passed":
            actions.append("Regenerate `route_rc_orchestration.json` from a passing route release train.")
        else:
            actions.append(f"Resolve `{blocker}` before release review.")
    return tuple(dict.fromkeys(actions))


__all__ = ["RouteRcExecutionPolicy"]
