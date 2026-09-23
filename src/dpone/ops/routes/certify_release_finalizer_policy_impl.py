"""Pure policy for route certification release finalization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from dpone.ops.routes.certify_release_finalizer_models import RouteCertificationReleaseFinalizerCheck


@dataclass(frozen=True, slots=True)
class RouteCertificationReleaseFinalizerDecision:
    """Final go/no-go decision for a route-certified release."""

    passed: bool
    level: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    checks: tuple[RouteCertificationReleaseFinalizerCheck, ...]


class RouteCertificationReleaseFinalizerPolicy:
    """Evaluate release finalization checks without filesystem or CLI logic."""

    def evaluate(
        self,
        *,
        release: str,
        release_report: _RouteCertificationReleaseReport,
        baseline_scores: Mapping[str, float],
        max_age_hours: float,
        now: datetime,
    ) -> RouteCertificationReleaseFinalizerDecision:
        checks: list[RouteCertificationReleaseFinalizerCheck] = []
        blockers: list[str] = []
        if not release_report.passed:
            blockers.append("route_certification_release.not_passed")
            blockers.extend(release_report.blockers)
            checks.append(
                _check(
                    name="route_certification_release",
                    route_case_id="",
                    passed=False,
                    summary=f"level={release_report.level}",
                    blocker="route_certification_release.not_passed",
                )
            )
        else:
            checks.append(
                _check(
                    name="route_certification_release",
                    route_case_id="",
                    passed=True,
                    summary=f"level={release_report.level}",
                    blocker="",
                )
            )

        for route in release_report.routes:
            if route.missing:
                continue
            blockers.extend(_route_blockers(route, release=release, baseline_scores=baseline_scores))
            checks.extend(_route_checks(route, release=release, baseline_scores=baseline_scores))
            stale = _stale_blocker(route, max_age_hours=max_age_hours, now=now)
            if stale:
                blockers.append(stale)
                checks.append(
                    _check(
                        name="freshness",
                        route_case_id=route.route_case_id,
                        passed=False,
                        summary=f"modified_at={route.modified_at}; max_age_hours={max_age_hours}",
                        blocker=stale,
                    )
                )
            elif route.modified_at:
                checks.append(
                    _check(
                        name="freshness",
                        route_case_id=route.route_case_id,
                        passed=True,
                        summary=f"modified_at={route.modified_at}",
                        blocker="",
                    )
                )

        unique_blockers = tuple(dict.fromkeys(blockers))
        warnings: tuple[str, ...] = tuple(release_report.warnings)
        return RouteCertificationReleaseFinalizerDecision(
            passed=not unique_blockers,
            level=_level(blockers=unique_blockers, warnings=warnings),
            blockers=unique_blockers,
            warnings=warnings,
            next_actions=_next_actions(unique_blockers, warnings),
            checks=tuple(checks),
        )


def _route_blockers(
    route: _RouteCertificationReleaseItem,
    *,
    release: str,
    baseline_scores: Mapping[str, float],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if route.required and route.bundle_release != release:
        blockers.append(f"{route.route_case_id}.release_mismatch")
    baseline_score = baseline_scores.get(route.route_case_id)
    if baseline_score is not None and route.score is not None and route.score < baseline_score:
        blockers.append(f"{route.route_case_id}.score_regression")
    return tuple(blockers)


def _route_checks(
    route: _RouteCertificationReleaseItem,
    *,
    release: str,
    baseline_scores: Mapping[str, float],
) -> tuple[RouteCertificationReleaseFinalizerCheck, ...]:
    checks = [
        _check(
            name="provenance",
            route_case_id=route.route_case_id,
            passed=route.bundle_release == release,
            summary=f"bundle_release={route.bundle_release}; expected={release}",
            blocker=f"{route.route_case_id}.release_mismatch",
        )
    ]
    baseline_score = baseline_scores.get(route.route_case_id)
    if baseline_score is not None:
        checks.append(
            _check(
                name="regression",
                route_case_id=route.route_case_id,
                passed=route.score is not None and route.score >= baseline_score,
                summary=f"score={route.score}; baseline={baseline_score}",
                blocker=f"{route.route_case_id}.score_regression",
            )
        )
    return tuple(checks)


def _stale_blocker(
    route: _RouteCertificationReleaseItem,
    *,
    max_age_hours: float,
    now: datetime,
) -> str:
    if max_age_hours <= 0 or not route.modified_at:
        return ""
    modified_at = datetime.fromisoformat(route.modified_at)
    age_hours = (now - modified_at).total_seconds() / 3600
    if age_hours > max_age_hours:
        return f"{route.route_case_id}.stale"
    return ""


def _check(
    *,
    name: str,
    route_case_id: str,
    passed: bool,
    summary: str,
    blocker: str,
) -> RouteCertificationReleaseFinalizerCheck:
    return RouteCertificationReleaseFinalizerCheck(
        name=name,
        route_case_id=route_case_id,
        passed=passed,
        required=True,
        summary=summary,
        blocker="" if passed else blocker,
    )


def _level(*, blockers: tuple[str, ...], warnings: tuple[str, ...]) -> str:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "final_ready"


def _next_actions(blockers: tuple[str, ...], warnings: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        route, _, suffix = blocker.partition(".")
        if blocker == "route_certification_release.not_passed":
            actions.append("Open nested `route_certification_release.json` and fix route bundle blockers.")
        elif suffix == "stale":
            actions.append(f"Regenerate `{route}` route certification bundle for this release candidate.")
        elif suffix == "release_mismatch":
            actions.append(f"Regenerate `{route}` with the release id requested by this finalizer.")
        elif suffix == "score_regression":
            actions.append(f"Review `{route}` route score regression against the baseline before release.")
        else:
            actions.append(f"Resolve `{blocker}` before tagging the release.")
    for warning in warnings:
        actions.append(f"Review warning `{warning}` before publishing.")
    return tuple(dict.fromkeys(actions))


class _RouteCertificationReleaseItem(Protocol):
    @property
    def route_case_id(self) -> str: ...

    @property
    def required(self) -> bool: ...

    @property
    def missing(self) -> bool: ...

    @property
    def bundle_release(self) -> str: ...

    @property
    def score(self) -> float | None: ...

    @property
    def modified_at(self) -> str: ...


class _RouteCertificationReleaseReport(Protocol):
    @property
    def passed(self) -> bool: ...

    @property
    def level(self) -> str: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...

    @property
    def warnings(self) -> tuple[str, ...]: ...

    @property
    def routes(self) -> Sequence[_RouteCertificationReleaseItem]: ...


__all__ = [
    "RouteCertificationReleaseFinalizerDecision",
    "RouteCertificationReleaseFinalizerPolicy",
]
