"""Pure route data quality scorecard policy."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

RouteDataQualityStatus = Literal["passed", "warning", "blocked", "waiver_required", "quarantine_sla_breached"]


@dataclass(frozen=True, slots=True)
class RouteDataQualityDecision:
    """Pure policy decision for one route data quality scorecard."""

    status: RouteDataQualityStatus
    passed: bool
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteDataQualityPolicy:
    """Classify route data quality from normalized evidence only."""

    def evaluate(
        self,
        *,
        route_colon_id: str,
        profile_exists: bool,
        required_evidence: tuple[str, ...],
        evidence: Sequence[_QualityEvidence],
        thresholds: _QualityThresholds,
    ) -> RouteDataQualityDecision:
        score = _score(evidence)
        blockers = _blockers(
            route_colon_id=route_colon_id,
            profile_exists=profile_exists,
            required_evidence=required_evidence,
            evidence=evidence,
            thresholds=thresholds,
            score=score,
        )
        warnings = _warnings(evidence=evidence, thresholds=thresholds, score=score)
        status = _status(blockers=blockers, warnings=warnings)
        return RouteDataQualityDecision(
            status=status,
            passed=status in {"passed", "warning"},
            score=score,
            blockers=blockers,
            warnings=warnings,
            next_actions=_next_actions(status=status, blockers=blockers, warnings=warnings),
        )


def _score(evidence: Sequence[_QualityEvidence]) -> float:
    scored = tuple(item for item in evidence if not item.missing)
    if not scored:
        return 0.0
    weighted_total = 0.0
    weight_total = 0.0
    for item in scored:
        weight = sum(dimension.weight for dimension in item.dimensions) or 1.0
        weighted_total += item.score * weight
        weight_total += weight
    return round(weighted_total / weight_total, 2) if weight_total else 0.0


def _blockers(
    *,
    route_colon_id: str,
    profile_exists: bool,
    required_evidence: tuple[str, ...],
    evidence: Sequence[_QualityEvidence],
    thresholds: _QualityThresholds,
    score: float,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if not profile_exists:
        blockers.append(f"route.unsupported:{route_colon_id}")
    by_name = {item.name: item for item in evidence}
    for name in required_evidence:
        item = by_name.get(name)
        if item is None or item.missing:
            blockers.append(f"{name}.missing")
            continue
        if not item.route_matched:
            blockers.append(f"{name}.route_mismatch")
        if not item.passed and not item.blockers:
            blockers.append(f"{name}.not_passed")
    for item in evidence:
        blockers.extend(item.blockers)
        if item.waiver_required and not item.waiver_approved:
            blockers.append(f"{item.name}.waiver_required")
        blockers.extend(_exception_blockers(item=item, thresholds=thresholds))
    if score < thresholds.min_score:
        blockers.append(f"route_data_quality.score_below_min:{score}<{thresholds.min_score}")
    return tuple(dict.fromkeys(blocker for blocker in blockers if blocker))


def _exception_blockers(
    *,
    item: _QualityEvidence,
    thresholds: _QualityThresholds,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if item.exception_count > thresholds.max_quarantine_rows:
        blockers.append(f"{item.name}.exception_count_exceeded")
    if item.exception_ratio > thresholds.max_exception_ratio:
        blockers.append(f"{item.name}.exception_ratio_exceeded")
    if item.max_exception_age_hours > thresholds.max_exception_age_hours:
        blockers.append(f"{item.name}.exception_age_exceeded")
    return tuple(blockers)


def _warnings(
    *,
    evidence: Sequence[_QualityEvidence],
    thresholds: _QualityThresholds,
    score: float,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if thresholds.min_score <= score < thresholds.warning_score:
        warnings.append(f"route_data_quality.score_below_warning:{score}<{thresholds.warning_score}")
    for item in evidence:
        warnings.extend(f"{item.name}.{warning}" if "." not in warning else warning for warning in item.warnings)
        if item.missing and not item.required:
            warnings.append(f"{item.name}.optional_missing")
        if not item.required and not item.missing and (not item.passed or not item.route_matched):
            warnings.append(f"{item.name}.optional_not_passed")
    return tuple(dict.fromkeys(warnings))


def _status(
    *,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> RouteDataQualityStatus:
    if not blockers:
        return "warning" if warnings else "passed"
    if any(blocker.endswith(".exception_count_exceeded") for blocker in blockers):
        return "quarantine_sla_breached"
    if any(
        blocker.endswith(".exception_ratio_exceeded") or blocker.endswith(".exception_age_exceeded")
        for blocker in blockers
    ):
        return "quarantine_sla_breached"
    if any(blocker.endswith(".waiver_required") for blocker in blockers):
        return "waiver_required"
    return "blocked"


def _next_actions(
    *,
    status: RouteDataQualityStatus,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> tuple[str, ...]:
    if status == "passed":
        return tuple()
    actions: list[str] = []
    if status == "warning":
        actions.append("Review route data quality warnings before release review.")
    elif status == "quarantine_sla_breached":
        actions.append("Drain, replay, or approve the exception backlog before promoting this route.")
    elif status == "waiver_required":
        actions.append("Capture data steward waiver approval before route release.")
    else:
        actions.append("Resolve required route data quality evidence blockers before continuing.")
    for blocker in blockers:
        if blocker.startswith("route.unsupported"):
            actions.append("Add the route to the source -> sink matrix before evaluating route data quality.")
        elif blocker.endswith(".missing"):
            actions.append(f"Attach `{blocker.removesuffix('.missing')}` evidence with --artifact.")
        elif blocker.endswith(".route_mismatch"):
            actions.append("Regenerate the mismatched artifact for the same source, sink, and strategy.")
        elif blocker.endswith(".invalid_json") or blocker.endswith(".invalid_shape"):
            actions.append(f"Regenerate malformed evidence `{blocker}`.")
        elif blocker.endswith(".exception_count_exceeded"):
            actions.append(
                "Export quarantine rows, triage root causes, and reduce the exception count below threshold."
            )
        elif blocker.endswith(".exception_ratio_exceeded"):
            actions.append("Reduce failed-row ratio or raise an explicit release waiver.")
        elif blocker.endswith(".exception_age_exceeded"):
            actions.append("Clear stale exceptions before the quarantine SLA expires.")
        elif blocker.endswith(".waiver_required"):
            actions.append("Attach approved waiver evidence and rerun route-data-quality.")
        else:
            actions.append(f"Open upstream evidence and resolve `{blocker}`.")
    for warning in warnings:
        actions.append(f"Review warning `{warning}`.")
    return tuple(dict.fromkeys(actions))


class _QualityDimension(Protocol):
    @property
    def weight(self) -> float: ...


class _QualityEvidence(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def required(self) -> bool: ...

    @property
    def missing(self) -> bool: ...

    @property
    def passed(self) -> bool: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...

    @property
    def warnings(self) -> tuple[str, ...]: ...

    @property
    def route_matched(self) -> bool: ...

    @property
    def score(self) -> float: ...

    @property
    def dimensions(self) -> tuple[_QualityDimension, ...]: ...

    @property
    def exception_count(self) -> int: ...

    @property
    def exception_ratio(self) -> float: ...

    @property
    def max_exception_age_hours(self) -> float: ...

    @property
    def waiver_required(self) -> bool: ...

    @property
    def waiver_approved(self) -> bool: ...


class _QualityThresholds(Protocol):
    @property
    def min_score(self) -> float: ...

    @property
    def warning_score(self) -> float: ...

    @property
    def max_exception_ratio(self) -> float: ...

    @property
    def max_quarantine_rows(self) -> int: ...

    @property
    def max_exception_age_hours(self) -> float: ...


__all__ = ["RouteDataQualityDecision", "RouteDataQualityPolicy"]
