"""Route release gate policy."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RouteReleaseGateDecision:
    """Pure go/no-go decision for route release promotion."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteReleaseGatePolicy:
    """Evaluate route release evidence against required domains."""

    def evaluate(
        self,
        *,
        required_evidence: tuple[str, ...],
        evidence: Sequence[_ReleaseEvidence],
        base_blockers: tuple[str, ...] = (),
    ) -> RouteReleaseGateDecision:
        by_name = {item.name: item for item in evidence}
        blockers = [*base_blockers]
        for name in required_evidence:
            item = by_name.get(name)
            if item is None or item.missing:
                blockers.append(f"{name}.missing")
                continue
            if not item.route_matched:
                blockers.append(f"{name}.route_mismatch")
            if not item.passed:
                blockers.append(f"{name}.not_passed")
        warnings = tuple(
            f"{item.name}.not_passed"
            for item in evidence
            if not item.required and not item.missing and (not item.passed or not item.route_matched)
        )
        unique_blockers = tuple(dict.fromkeys(blockers))
        score = _score(required_evidence=required_evidence, evidence=evidence)
        return RouteReleaseGateDecision(
            passed=not unique_blockers,
            level=_level(score=score, blockers=unique_blockers),
            score=score,
            blockers=unique_blockers,
            warnings=warnings,
            next_actions=_next_actions(unique_blockers),
        )


def _score(*, required_evidence: tuple[str, ...], evidence: Sequence[_ReleaseEvidence]) -> float:
    required = tuple(dict.fromkeys(required_evidence))
    if not required:
        return 100.0
    by_name = {item.name: item for item in evidence}
    passed = sum(
        1
        for name in required
        if (item := by_name.get(name)) is not None and not item.missing and item.passed and item.route_matched
    )
    return round((passed / len(required)) * 100.0, 2)


def _level(*, score: float, blockers: tuple[str, ...]) -> str:
    if not blockers:
        return "release_ready"
    if score >= 80.0:
        return "release_candidate"
    return "blocked"


def _next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        name, _, suffix = blocker.partition(".")
        if blocker.startswith("route.unsupported"):
            actions.append("Add the route to the source -> sink matrix before release gating.")
        elif suffix == "missing":
            actions.append(f"Attach or regenerate required route release evidence `{name}`.")
        elif suffix == "route_mismatch":
            actions.append(f"Use `{name}` evidence produced for the same source, sink, and strategy.")
        else:
            actions.append(f"Open `{name}` and follow its upstream runbook before release review.")
    return tuple(dict.fromkeys(actions))


class _ReleaseEvidence(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def required(self) -> bool: ...

    @property
    def missing(self) -> bool: ...

    @property
    def passed(self) -> bool: ...

    @property
    def route_matched(self) -> bool: ...
