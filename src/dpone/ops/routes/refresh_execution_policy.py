"""Pure policy for route refresh execution receipts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

RouteRefreshExecutionStatus = Literal["dry_run", "succeeded", "partial_failure", "blocked", "approval_required"]


@dataclass(frozen=True, slots=True)
class RouteRefreshExecutionDecision:
    """Pure go/no-go decision for route refresh execution."""

    status: RouteRefreshExecutionStatus
    passed: bool
    ready_for_state_promotion: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteRefreshExecutionPolicy:
    """Classify refresh execution from plan state, route state, and chunk results."""

    def evaluate(
        self,
        *,
        execute: bool,
        profile_exists: bool,
        route_matched: bool,
        plan_passed: bool,
        plan_status: str,
        executor_available: bool,
        chunks: Sequence[_ChunkResult],
        hard_blockers: Sequence[str] = (),
        warnings: Sequence[str] = (),
    ) -> RouteRefreshExecutionDecision:
        blockers = _blockers(
            execute=execute,
            profile_exists=profile_exists,
            route_matched=route_matched,
            plan_passed=plan_passed,
            plan_status=plan_status,
            executor_available=executor_available,
            chunks=chunks,
            hard_blockers=hard_blockers,
        )
        normalized_warnings = tuple(dict.fromkeys(item for item in warnings if item))
        status = _status(execute=execute, blockers=blockers, chunks=chunks)
        return RouteRefreshExecutionDecision(
            status=status,
            passed=status in {"dry_run", "succeeded"},
            ready_for_state_promotion=status == "succeeded",
            blockers=blockers,
            warnings=normalized_warnings,
            next_actions=_next_actions(status=status, blockers=blockers, warnings=normalized_warnings),
        )


def _blockers(
    *,
    execute: bool,
    profile_exists: bool,
    route_matched: bool,
    plan_passed: bool,
    plan_status: str,
    executor_available: bool,
    chunks: Sequence[_ChunkResult],
    hard_blockers: Sequence[str],
) -> tuple[str, ...]:
    blockers: list[str] = [item for item in hard_blockers if item]
    if not profile_exists:
        blockers.append("route_refresh_execution.route_unsupported")
    if not route_matched:
        blockers.append("route_refresh_execution.route_mismatch")
    if plan_status == "approval_required":
        blockers.append("route_refresh_execution.plan_approval_required")
    elif not plan_passed or plan_status == "blocked":
        blockers.append("route_refresh_execution.plan_blocked")
    if execute and not executor_available:
        blockers.append("route_refresh_execution.executor_unavailable")
    for chunk in chunks:
        if chunk.status == "failed" or not chunk.passed:
            blockers.append(f"route_refresh_execution.chunk_failed:{chunk.ordinal}")
            blockers.extend(chunk.blockers)
    return tuple(dict.fromkeys(blockers))


def _status(
    *,
    execute: bool,
    blockers: tuple[str, ...],
    chunks: Sequence[_ChunkResult],
) -> RouteRefreshExecutionStatus:
    if any(item == "route_refresh_execution.plan_approval_required" for item in blockers):
        return "approval_required"
    if any(item.startswith("route_refresh_execution.chunk_failed:") for item in blockers):
        return "partial_failure"
    if blockers:
        return "blocked"
    return "succeeded" if execute and chunks else "dry_run"


def _next_actions(
    *,
    status: RouteRefreshExecutionStatus,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> tuple[str, ...]:
    if status == "dry_run":
        return ("Review the dry-run receipt, then rerun with an approved executor before moving data.",)
    if status == "succeeded":
        return tuple()
    actions: list[str] = []
    if status == "approval_required":
        actions.append("Capture refresh approval and regenerate the route refresh plan before execution.")
    elif status == "partial_failure":
        actions.append("Stop source-state promotion, inspect failed chunks, and rerun only idempotent failed chunks.")
    else:
        actions.append("Resolve route refresh execution blockers before executing chunks.")
    for blocker in blockers:
        if blocker == "route_refresh_execution.executor_unavailable":
            actions.append("Configure a concrete RouteRefreshExecutor before using --execute.")
        elif blocker == "route_refresh_execution.plan_blocked":
            actions.append("Regenerate a ready route_refresh_plan.json before execution.")
        elif blocker == "route_refresh_execution.plan_approval_required":
            actions.append("Attach approval evidence and rerun route-refresh-plan with approval granted.")
        elif blocker == "route_refresh_execution.route_mismatch":
            actions.append("Use a refresh plan for the same source, sink, and strategy.")
        elif blocker.startswith("route_refresh_execution.chunk_failed:"):
            actions.append(f"Inspect and replay `{blocker}` with the same idempotency key.")
        else:
            actions.append(f"Resolve `{blocker}`.")
    for warning in warnings:
        actions.append(f"Review warning `{warning}`.")
    return tuple(dict.fromkeys(actions))


class _ChunkResult(Protocol):
    @property
    def ordinal(self) -> int: ...

    @property
    def status(self) -> str: ...

    @property
    def passed(self) -> bool: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...


__all__ = ["RouteRefreshExecutionDecision", "RouteRefreshExecutionPolicy"]
