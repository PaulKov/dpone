"""Route release candidate orchestration policy."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.ops.routes.rc_orchestrator_models import RouteRcOrchestrationDecision, RouteRcOrchestrationStep


class RouteRcOrchestrationPolicy:
    """Evaluate the ordered route release candidate train."""

    def evaluate(self, *, steps: Sequence[RouteRcOrchestrationStep]) -> RouteRcOrchestrationDecision:
        required = tuple(step for step in steps if step.required)
        failed_required = tuple(step for step in required if not step.passed)
        blockers = tuple(
            dict.fromkeys(
                (
                    *(f"{step.name}.not_passed" for step in failed_required),
                    *(blocker for step in steps for blocker in step.blockers),
                )
            )
        )
        warnings = tuple(f"{step.name}.not_passed" for step in steps if not step.required and not step.passed)
        score = _score(required)
        return RouteRcOrchestrationDecision(
            passed=not blockers,
            level="release_ready" if not blockers else "blocked",
            score=score,
            blockers=blockers,
            warnings=warnings,
            next_actions=_next_actions(blockers),
        )


def _score(required_steps: tuple[RouteRcOrchestrationStep, ...]) -> float:
    if not required_steps:
        return 100.0
    passed = sum(1 for step in required_steps if step.passed)
    return round((passed / len(required_steps)) * 100.0, 2)


def _next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        name, _, suffix = blocker.partition(".")
        if suffix == "missing":
            actions.append(f"Attach or regenerate required release candidate evidence `{name}`.")
        elif suffix == "not_passed":
            actions.append(f"Open `{name}` output and fix the upstream release-train step.")
        elif suffix == "route_mismatch":
            actions.append(f"Regenerate `{name}` for the same source, sink, and strategy.")
        else:
            actions.append(f"Resolve `{blocker}` before release review.")
    return tuple(dict.fromkeys(actions))
