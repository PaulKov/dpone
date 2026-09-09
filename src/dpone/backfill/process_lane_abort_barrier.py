"""Fail-closed all-peer process-tree barrier for parent abort cleanup."""

from __future__ import annotations

import signal
import time
from typing import Any

from dpone.backfill.process_lane_group import (
    PROCESS_GROUP_ERROR,
    join_lane_groups,
    lane_or_group_is_alive,
    signal_lane,
)


def terminate_lane_trees_before(lanes: list[Any], *, deadline: float) -> None:
    """Quiesce all trees within one absolute graceful-plus-force deadline.

    Abort cleanup is itself exposed to process-control signals and OS errors.
    Every phase therefore attempts every peer and aggregates failures instead
    of allowing one broken signal or join to skip the remaining process trees.
    The hard phase is an unconditional ``finally`` boundary.
    """

    pending = [lane for lane in lanes if not lane.tree_quiesced]
    barrier_failed = False
    active = list(pending)
    try:
        barrier_failed = _signal_lanes_best_effort(pending, force=False)
        remaining_budget = max(0.0, deadline - time.monotonic())
        graceful_deadline = min(deadline, time.monotonic() + remaining_budget * 0.75)
        barrier_failed = _wait_for_process_trees_best_effort(pending, graceful_deadline) or barrier_failed
    finally:
        # An asynchronous BaseException between graceful helpers cannot bypass
        # SIGKILL, final observation, or the per-lane quiescence proof.
        try:
            barrier_failed = _signal_lanes_best_effort(pending, force=True) or barrier_failed
        finally:
            try:
                barrier_failed = _wait_for_process_trees_best_effort(pending, deadline) or barrier_failed
            finally:
                active, classification_failed = _active_lanes_best_effort(pending)
                barrier_failed = classification_failed or barrier_failed
                active_ids = {lane.worker_id for lane in active}
                for lane in lanes:
                    lane.tree_quiesced = lane.worker_id not in active_ids
    cleanup_failed = [lane.worker_id for lane in active]
    if cleanup_failed or barrier_failed:
        workers = ",".join(str(worker_id) for worker_id in cleanup_failed)
        suffix = f": worker_ids={workers}" if workers else ""
        raise RuntimeError(f"{PROCESS_GROUP_ERROR}{suffix}")


def _signal_lanes_best_effort(lanes: list[Any], *, force: bool) -> bool:
    """Signal every peer even when another peer raises a fatal cleanup error."""

    failed = False
    requested_signal = signal.SIGKILL if force else signal.SIGTERM
    for lane in lanes:
        try:
            signal_lane(lane, requested_signal)
        except BaseException:
            failed = True
    return failed


def _wait_for_process_trees_best_effort(lanes: list[Any], deadline: float) -> bool:
    """Observe every peer against one deadline while aggregating interruptions."""

    failed = False
    for lane in lanes:
        try:
            if getattr(lane.process, "exitcode", None) is None:
                join = getattr(lane.process, "join", None)
                if callable(join):
                    join(timeout=max(0.0, deadline - time.monotonic()))
        except BaseException:
            failed = True
    for lane in lanes:
        try:
            join_lane_groups([lane], timeout_seconds=max(0.0, deadline - time.monotonic()))
        except BaseException:
            failed = True
    return failed


def _active_lanes_best_effort(lanes: list[Any]) -> tuple[list[Any], bool]:
    """Classify every tree, treating unreadable evidence as still active."""

    active: list[Any] = []
    failed = False
    for lane in lanes:
        try:
            is_active = lane.process.is_alive() or (lane.ready is not None and lane_or_group_is_alive(lane))
        except BaseException:
            is_active = True
            failed = True
        if is_active:
            active.append(lane)
    return active, failed


__all__ = ["terminate_lane_trees_before"]
