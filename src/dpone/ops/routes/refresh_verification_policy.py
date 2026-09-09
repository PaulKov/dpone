"""Pure policy for route refresh verification receipts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from dpone.ops.routes.refresh_verification_models import RouteRefreshVerificationStatus


@dataclass(frozen=True, slots=True)
class RouteRefreshChunkVerificationDecision:
    """Pure decision for one source/sink chunk snapshot comparison."""

    ordinal: int
    idempotency_key: str
    status: str
    passed: bool
    summary: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RouteRefreshVerificationDecision:
    """Pure route-level verification decision."""

    status: RouteRefreshVerificationStatus
    passed: bool
    ready_for_state_promotion: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteRefreshVerificationPolicy:
    """Classify post-load verification from source and sink snapshots."""

    def evaluate_chunk(
        self,
        *,
        ordinal: int,
        idempotency_key: str,
        source: _SideSnapshot,
        sink: _SideSnapshot,
    ) -> RouteRefreshChunkVerificationDecision:
        blockers = _chunk_blockers(ordinal=ordinal, source=source, sink=sink)
        return RouteRefreshChunkVerificationDecision(
            ordinal=ordinal,
            idempotency_key=idempotency_key,
            status="failed" if blockers else "verified",
            passed=not blockers,
            summary="source and sink snapshots match" if not blockers else "source and sink snapshots differ",
            blockers=blockers,
        )

    def evaluate_report(
        self,
        *,
        chunks: Sequence[_ChunkVerification],
        base_blockers: Sequence[str] = (),
        warnings: Sequence[str] = (),
    ) -> RouteRefreshVerificationDecision:
        blockers = [item for item in base_blockers if item]
        for chunk in chunks:
            if not chunk.passed:
                blockers.append(f"route_refresh_verification.chunk_failed:{chunk.ordinal}")
                blockers.extend(chunk.blockers)
        unique_blockers = tuple(dict.fromkeys(blockers))
        unique_warnings = tuple(dict.fromkeys(item for item in warnings if item))
        status = _status(chunks=chunks, blockers=unique_blockers)
        return RouteRefreshVerificationDecision(
            status=status,
            passed=status == "verified",
            ready_for_state_promotion=status == "verified",
            blockers=unique_blockers,
            warnings=unique_warnings,
            next_actions=_next_actions(status=status, blockers=unique_blockers, warnings=unique_warnings),
        )


def _chunk_blockers(
    *,
    ordinal: int,
    source: _SideSnapshot,
    sink: _SideSnapshot,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if source.row_count != sink.row_count:
        blockers.append(f"route_refresh_verification.row_count_mismatch:{ordinal}")
    if source.min_boundary != sink.min_boundary:
        blockers.append(f"route_refresh_verification.min_boundary_mismatch:{ordinal}")
    if source.max_boundary != sink.max_boundary:
        blockers.append(f"route_refresh_verification.max_boundary_mismatch:{ordinal}")
    if source.typed_hash != sink.typed_hash:
        blockers.append(f"route_refresh_verification.typed_hash_mismatch:{ordinal}")
    if sink.duplicate_keys:
        blockers.append(f"route_refresh_verification.duplicate_keys:{ordinal}")
    if sink.null_keys:
        blockers.append(f"route_refresh_verification.null_keys:{ordinal}")
    return tuple(dict.fromkeys(blockers))


def _status(
    *,
    chunks: Sequence[_ChunkVerification],
    blockers: tuple[str, ...],
) -> RouteRefreshVerificationStatus:
    if any(item.startswith("route_refresh_verification.chunk_failed:") for item in blockers):
        return "failed"
    if blockers:
        return "blocked"
    return "verified" if chunks else "blocked"


def _next_actions(
    *,
    status: RouteRefreshVerificationStatus,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> tuple[str, ...]:
    if status == "verified":
        return tuple()
    actions: list[str] = []
    if status == "failed":
        actions.append("Stop state promotion and replay or repair failed chunks before advancing source state.")
    else:
        actions.append("Resolve route refresh verification blockers before state promotion.")
    for blocker in blockers:
        if blocker == "route_refresh_verification.execution_not_succeeded":
            actions.append("Verify only a succeeded route_refresh_execution.json receipt.")
        elif blocker == "route_refresh_verification.reader_unavailable":
            actions.append("Configure source and sink verification readers or provide snapshot artifacts.")
        elif blocker.startswith("route_refresh_verification.typed_hash_mismatch:"):
            actions.append(f"Inspect typed hash reconciliation for `{blocker}`.")
        elif blocker.startswith("route_refresh_verification.row_count_mismatch:"):
            actions.append(f"Compare source and sink row counts for `{blocker}`.")
        else:
            actions.append(f"Resolve `{blocker}`.")
    for warning in warnings:
        actions.append(f"Review warning `{warning}`.")
    return tuple(dict.fromkeys(actions))


class _SideSnapshot(Protocol):
    @property
    def row_count(self) -> int: ...

    @property
    def min_boundary(self) -> str: ...

    @property
    def max_boundary(self) -> str: ...

    @property
    def typed_hash(self) -> str: ...

    @property
    def duplicate_keys(self) -> int: ...

    @property
    def null_keys(self) -> int: ...


class _ChunkVerification(Protocol):
    @property
    def ordinal(self) -> int: ...

    @property
    def passed(self) -> bool: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...


__all__ = [
    "RouteRefreshChunkVerificationDecision",
    "RouteRefreshVerificationDecision",
    "RouteRefreshVerificationPolicy",
]
