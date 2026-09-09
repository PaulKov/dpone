"""Heartbeat and control-error helpers for the process-lane parent."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol

from dpone.backfill.process_lane_contracts import ProcessLaneDispatch

_CHUNK_LEASE_ERROR = "DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST"


class _RunningLane(Protocol):
    worker_id: int
    running: ProcessLaneDispatch | None
    control_error: str | None


def _heartbeat_running(
    lanes: Sequence[_RunningLane],
    heartbeat: Callable[[ProcessLaneDispatch], bool],
    *,
    after_each: Callable[[], bool] | None = None,
) -> bool:
    """Renew active chunk leases and latch the first failed owner proof."""

    healthy = True
    for lane in lanes:
        if lane.running is None or lane.control_error is not None:
            continue
        try:
            renewed = bool(heartbeat(lane.running))
        except Exception:
            renewed = False
        if not renewed:
            lane.control_error = _CHUNK_LEASE_ERROR
            healthy = False
        if after_each is not None and after_each():
            break
    return healthy


def _mark_all_running_control_error(lanes: Sequence[_RunningLane], error: str) -> None:
    """Latch one parent control failure onto every active lane."""

    for lane in lanes:
        if lane.running is not None:
            lane.control_error = lane.control_error or error


def _mark_exact_running_control_error(
    lanes: Sequence[_RunningLane],
    key: tuple[int, str],
    error: str,
) -> None:
    """Latch an exact command failure before any co-ready terminal IPC drain."""

    worker_id, command_id = key
    for lane in lanes:
        running = lane.running
        if lane.worker_id == worker_id and running is not None and running.command_id == command_id:
            lane.control_error = lane.control_error or error
            return


__all__ = [
    "_CHUNK_LEASE_ERROR",
    "_heartbeat_running",
    "_mark_all_running_control_error",
    "_mark_exact_running_control_error",
]
