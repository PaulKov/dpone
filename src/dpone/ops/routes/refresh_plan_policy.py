"""Pure route refresh planning policy."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

RouteRefreshStatus = Literal["ready", "approval_required", "blocked"]


@dataclass(frozen=True, slots=True)
class RouteRefreshDecision:
    """Go/no-go decision for one route refresh plan."""

    status: RouteRefreshStatus
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteRefreshPlanPolicy:
    """Classify route refresh readiness from normalized plan inputs."""

    def evaluate(
        self,
        *,
        route_colon_id: str,
        profile_exists: bool,
        required_evidence: tuple[str, ...],
        evidence: Sequence[_RefreshEvidence],
        approval: _Approval,
        hard_blockers: Sequence[str] = (),
        warnings: Sequence[str] = (),
    ) -> RouteRefreshDecision:
        blockers = _blockers(
            route_colon_id=route_colon_id,
            profile_exists=profile_exists,
            required_evidence=required_evidence,
            evidence=evidence,
            approval=approval,
            hard_blockers=hard_blockers,
        )
        soft_warnings = tuple(dict.fromkeys(item for item in warnings if item))
        status = _status(blockers)
        return RouteRefreshDecision(
            status=status,
            passed=status == "ready",
            blockers=blockers,
            warnings=soft_warnings,
            next_actions=_next_actions(status=status, blockers=blockers, warnings=soft_warnings),
        )


def _blockers(
    *,
    route_colon_id: str,
    profile_exists: bool,
    required_evidence: tuple[str, ...],
    evidence: Sequence[_RefreshEvidence],
    approval: _Approval,
    hard_blockers: Sequence[str],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if not profile_exists:
        blockers.append(f"route.unsupported:{route_colon_id}")
    blockers.extend(hard_blockers)
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
    if approval.required and not approval.approved:
        blockers.append("route_refresh.approval_required")
    return tuple(dict.fromkeys(item for item in blockers if item))


def _status(blockers: tuple[str, ...]) -> RouteRefreshStatus:
    if not blockers:
        return "ready"
    if blockers == ("route_refresh.approval_required",) or all(
        item == "route_refresh.approval_required" for item in blockers
    ):
        return "approval_required"
    if "route_refresh.approval_required" in blockers and not any(_hard_blocker(item) for item in blockers):
        return "approval_required"
    return "blocked"


def _hard_blocker(blocker: str) -> bool:
    return blocker != "route_refresh.approval_required"


def _next_actions(
    *,
    status: RouteRefreshStatus,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> tuple[str, ...]:
    if status == "ready":
        return tuple()
    actions: list[str] = []
    if status == "approval_required":
        actions.append("Capture refresh approval and rerun with approval evidence before executing chunks.")
    else:
        actions.append("Resolve route refresh blockers before executing any backfill, replay, or resync.")
    for blocker in blockers:
        if blocker.startswith("route.unsupported"):
            actions.append("Add the route to the source -> sink matrix before planning refresh operations.")
        elif blocker.endswith(".missing"):
            actions.append(f"Attach `{blocker.removesuffix('.missing')}` evidence with --artifact or --require.")
        elif blocker.endswith(".route_mismatch"):
            actions.append("Regenerate mismatched evidence for the same source, sink, and strategy.")
        elif blocker.endswith(".invalid_json") or blocker.endswith(".invalid_shape"):
            actions.append(f"Regenerate malformed evidence `{blocker}`.")
        elif blocker == "route_refresh.window_invalid":
            actions.append("Fix the refresh window boundaries and chunk size.")
        elif blocker == "route_refresh.chunk_count_exceeded":
            actions.append("Increase --chunk-size, narrow the window, or raise the chunk cap explicitly.")
        elif blocker == "route_refresh.approval_required":
            actions.append("Attach manual approval before executing a destructive or state-rewinding refresh.")
        else:
            actions.append(f"Open upstream evidence and resolve `{blocker}`.")
    for warning in warnings:
        actions.append(f"Review warning `{warning}`.")
    return tuple(dict.fromkeys(actions))


class _RefreshEvidence(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def missing(self) -> bool: ...

    @property
    def passed(self) -> bool: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...

    @property
    def route_matched(self) -> bool: ...


class _Approval(Protocol):
    @property
    def required(self) -> bool: ...

    @property
    def approved(self) -> bool: ...


__all__ = ["RouteRefreshDecision", "RouteRefreshPlanPolicy"]
