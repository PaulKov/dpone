"""Pure policy for route release certification bundles."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RouteCertificationDecision:
    """Go/no-go decision for the route certification bundle."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteCertificationPolicy:
    """Evaluate certification stages without knowing route-specific mechanics."""

    def evaluate(self, *, stages: Sequence[_CertificationStage]) -> RouteCertificationDecision:
        required = tuple(stage for stage in stages if stage.required)
        failed_required = tuple(stage for stage in required if not stage.passed)
        blockers = tuple(
            dict.fromkeys(
                (
                    *(f"{stage.name}.not_passed" for stage in failed_required),
                    *(blocker for stage in stages for blocker in stage.blockers),
                )
            )
        )
        warnings = tuple(f"{stage.name}.not_passed" for stage in stages if not stage.required and not stage.passed)
        score = _score(required)
        return RouteCertificationDecision(
            passed=not blockers,
            level=_level(blockers=blockers, warnings=warnings),
            score=score,
            blockers=blockers,
            warnings=warnings,
            next_actions=_next_actions(blockers=blockers, warnings=warnings),
        )


def _score(required: tuple[_CertificationStage, ...]) -> float:
    if not required:
        return 100.0
    passed = sum(1 for stage in required if stage.passed)
    return round((passed / len(required)) * 100.0, 2)


def _level(*, blockers: tuple[str, ...], warnings: tuple[str, ...]) -> str:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "certified"


def _next_actions(*, blockers: tuple[str, ...], warnings: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        name, _, suffix = blocker.partition(".")
        if suffix == "missing":
            actions.append(f"Attach or regenerate required route certification evidence `{name}`.")
        elif suffix == "route_mismatch":
            actions.append(f"Regenerate `{name}` for the same source, sink, and strategy.")
        elif suffix == "not_passed":
            actions.append(f"Open `{name}` output and fix the upstream certification stage.")
        else:
            actions.append(f"Resolve `{blocker}` before release certification.")
    for warning in warnings:
        actions.append(f"Review optional route certification warning `{warning}` before publishing.")
    return tuple(dict.fromkeys(actions))


class _CertificationStage(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def passed(self) -> bool: ...

    @property
    def required(self) -> bool: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...
