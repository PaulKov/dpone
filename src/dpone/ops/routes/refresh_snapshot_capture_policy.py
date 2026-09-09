"""Pure policy for route refresh snapshot capture receipts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

RouteRefreshSnapshotCaptureStatus = Literal["captured", "failed", "blocked"]


@dataclass(frozen=True, slots=True)
class RouteRefreshSnapshotCaptureDecision:
    """Route-level snapshot capture decision."""

    status: RouteRefreshSnapshotCaptureStatus
    passed: bool
    ready_for_verification: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteRefreshSnapshotCapturePolicy:
    """Classify snapshot capture receipts from base blockers and chunk results."""

    def evaluate(
        self,
        *,
        chunks: Sequence[_ChunkCapture],
        base_blockers: Sequence[str] = (),
        warnings: Sequence[str] = (),
    ) -> RouteRefreshSnapshotCaptureDecision:
        blockers = [item for item in base_blockers if item]
        for chunk in chunks:
            if not chunk.passed:
                blockers.append(f"route_refresh_snapshot_capture.chunk_failed:{chunk.ordinal}")
                blockers.extend(chunk.blockers)
        unique_blockers = tuple(dict.fromkeys(blockers))
        unique_warnings = tuple(dict.fromkeys(item for item in warnings if item))
        status = _status(chunks=chunks, blockers=unique_blockers)
        return RouteRefreshSnapshotCaptureDecision(
            status=status,
            passed=status == "captured",
            ready_for_verification=status == "captured",
            blockers=unique_blockers,
            warnings=unique_warnings,
            next_actions=_next_actions(status=status, blockers=unique_blockers),
        )


def _status(
    *,
    chunks: Sequence[_ChunkCapture],
    blockers: tuple[str, ...],
) -> RouteRefreshSnapshotCaptureStatus:
    if any(item.startswith("route_refresh_snapshot_capture.chunk_failed:") for item in blockers):
        return "failed"
    if blockers:
        return "blocked"
    return "captured" if chunks else "blocked"


def _next_actions(
    *,
    status: RouteRefreshSnapshotCaptureStatus,
    blockers: tuple[str, ...],
) -> tuple[str, ...]:
    if status == "captured":
        return tuple()
    actions: list[str] = []
    if status == "failed":
        actions.append("Stop route refresh verification until every source and sink snapshot chunk is captured.")
    else:
        actions.append("Resolve route refresh snapshot capture blockers before verification.")
    for blocker in blockers:
        if blocker == "route_refresh_snapshot_capture.execution_not_succeeded":
            actions.append("Capture snapshots only for a succeeded route_refresh_execution.json receipt.")
        elif blocker == "route_refresh_snapshot_capture.execution_not_run":
            actions.append("Run route-refresh-execute with `--execute` before snapshot capture.")
        elif blocker == "route_refresh_snapshot_capture.reader_unavailable":
            actions.append("Configure source and sink row readers or provide rows JSON files.")
        else:
            actions.append(f"Resolve `{blocker}`.")
    return tuple(dict.fromkeys(actions))


class _ChunkCapture(Protocol):
    @property
    def ordinal(self) -> int: ...

    @property
    def passed(self) -> bool: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...


__all__ = ["RouteRefreshSnapshotCaptureDecision", "RouteRefreshSnapshotCapturePolicy"]
