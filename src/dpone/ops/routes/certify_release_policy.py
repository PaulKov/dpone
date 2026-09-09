"""Pure policy for release-level route certification decisions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RouteCertificationReleaseDecision:
    """Go/no-go decision for all required route certification bundles."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteCertificationReleasePolicy:
    """Evaluate route certification bundle artifacts without route-specific logic."""

    def evaluate(
        self,
        *,
        required_routes: tuple[str, ...],
        routes: Sequence[_RouteCertificationReleaseItem],
    ) -> RouteCertificationReleaseDecision:
        by_route = {item.route_case_id: item for item in routes}
        blockers: list[str] = []
        for route in required_routes:
            item = by_route.get(route)
            if item is None or item.missing:
                blockers.append(f"{route}.missing")
                continue
            if not item.route_matched:
                blockers.append(f"{route}.route_mismatch")
            if not item.profile_matched:
                blockers.append(f"{route}.profile_mismatch")
            if not item.passed or item.level != "certified":
                blockers.append(f"{route}.not_certified")
            blockers.extend(item.blockers)
        warnings = tuple(
            f"{item.route_case_id}.not_certified"
            for item in routes
            if not item.required and not item.missing and (not item.passed or item.level != "certified")
        )
        unique_blockers = tuple(dict.fromkeys(blockers))
        score = _score(required_routes=required_routes, routes=routes)
        return RouteCertificationReleaseDecision(
            passed=not unique_blockers,
            level=_level(blockers=unique_blockers, warnings=warnings),
            score=score,
            blockers=unique_blockers,
            warnings=warnings,
            next_actions=_next_actions(blockers=unique_blockers, warnings=warnings),
        )


def _score(*, required_routes: tuple[str, ...], routes: Sequence[_RouteCertificationReleaseItem]) -> float:
    required = tuple(dict.fromkeys(required_routes))
    if not required:
        return 100.0
    by_route = {item.route_case_id: item for item in routes}
    passed = sum(
        1
        for route in required
        if (item := by_route.get(route)) is not None
        and not item.missing
        and item.passed
        and item.level == "certified"
        and item.route_matched
        and item.profile_matched
    )
    return round((passed / len(required)) * 100.0, 2)


def _level(*, blockers: tuple[str, ...], warnings: tuple[str, ...]) -> str:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "release_ready"


def _next_actions(*, blockers: tuple[str, ...], warnings: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        route, _, suffix = blocker.partition(".")
        if suffix == "missing":
            actions.append(f"Run `dpone ops route-certify` for `{route}` and attach its bundle.")
        elif suffix == "profile_mismatch":
            actions.append(f"Regenerate `{route}` with the release profile requested by this gate.")
        elif suffix == "route_mismatch":
            actions.append(f"Attach a bundle whose embedded route identity matches `{route}`.")
        elif suffix == "not_certified":
            actions.append(f"Open `{route}` route certification bundle and fix upstream blockers.")
        else:
            actions.append(f"Resolve `{blocker}` before tagging the release.")
    for warning in warnings:
        actions.append(f"Review optional route certification warning `{warning}` before publishing.")
    return tuple(dict.fromkeys(actions))


class _RouteCertificationReleaseItem(Protocol):
    @property
    def route_case_id(self) -> str: ...

    @property
    def required(self) -> bool: ...

    @property
    def missing(self) -> bool: ...

    @property
    def passed(self) -> bool: ...

    @property
    def level(self) -> str: ...

    @property
    def route_matched(self) -> bool: ...

    @property
    def profile_matched(self) -> bool: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...
