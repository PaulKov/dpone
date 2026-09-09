"""Spawned backfill lane lifecycle and bounded native diagnostics."""

from __future__ import annotations

import os
import signal
import time
from dataclasses import dataclass
from multiprocessing import get_context
from multiprocessing.connection import Connection, wait
from pickle import UnpicklingError
from typing import Any, cast

from dpone.backfill.process_lane_contracts import (
    PROCESS_TREE_ISOLATION_ERROR,
    PROCESS_TREE_SIGNAL_ERROR,
    BackfillProcessLaneBootstrap,
    ProcessLaneBootstrapFailed,
    ProcessLaneContainmentAck,
    ProcessLaneDispatch,
    ProcessLaneReady,
    ProcessLaneRuntimeReady,
    ProcessLaneStop,
)
from dpone.backfill.process_lane_diagnostics import (
    NATIVE_EXIT_ERROR as NATIVE_EXIT_ERROR,
)
from dpone.backfill.process_lane_diagnostics import (
    diagnostics,
    handle_native_exits,
    native_exit_message,
    render_dispatch_failure,
)
from dpone.backfill.process_lane_group import (
    PROCESS_GROUP_ERROR,
    join_lane_groups,
    lane_or_group_is_alive,
    ready_identity_is_valid,
    signal_lane,
)
from dpone.backfill.process_lane_worker import run_process_lane

BOOTSTRAP_ERROR = "DPONE_BACKFILL_PROCESS_BOOTSTRAP_FAILED"
CHANNEL_RECEIVE_ERROR = "DPONE_BACKFILL_PROCESS_CHANNEL_RECEIVE_FAILED"
_CHANNEL_RECEIVE_EXCEPTIONS = (
    EOFError,
    OSError,
    UnpicklingError,
    AttributeError,
    ImportError,
    IndexError,
    TypeError,
    ValueError,
)


def dispatch_failure_message(stage: str, error: BaseException) -> str:
    """Return one bounded, redacted parent dispatch diagnostic."""

    return render_dispatch_failure(stage, error)


@dataclass(slots=True)
class ProcessLaneHandle:
    """Parent-owned process, IPC endpoint, and current command state."""

    worker_id: int
    process: Any
    connection: Connection
    ready: ProcessLaneReady | None = None
    runtime_ready: bool = False
    running: ProcessLaneDispatch | None = None
    control_error: str | None = None
    stopping: bool = False
    tree_quiesced: bool = False
    final_exitcode: int | None = None


def start_lanes(
    runtime: BackfillProcessLaneBootstrap,
    count: int,
    *,
    ownership: list[ProcessLaneHandle] | None = None,
) -> list[ProcessLaneHandle]:
    """Start lanes while publishing each live handle to parent ownership."""

    context = get_context("spawn")
    lanes = ownership if ownership is not None else []
    if lanes:
        raise ValueError("backfill.process_lane_start_ownership_not_empty")
    try:
        for worker_id in range(count):
            parent_connection, child_connection = context.Pipe(duplex=True)
            process = context.Process(
                target=run_process_lane,
                args=(worker_id, runtime, child_connection),
                name=f"dpone-backfill-{worker_id}",
                daemon=False,
            )
            handle = ProcessLaneHandle(worker_id, process, parent_connection)
            try:
                process.start()
                lanes.append(handle)
                child_connection.close()
            except BaseException:
                # A signal after successful spawn but before the normal append
                # still transfers the live handle to the caller-owned list.
                if process.pid is not None and not any(item is handle for item in lanes):
                    lanes.append(handle)
                child_connection.close()
                raise
    except BaseException:
        # A caller-owned list is covered by its one shared-deadline lifecycle
        # barrier. Standalone callers retain the legacy local cleanup contract.
        if ownership is None:
            shutdown_lanes(lanes, timeout_seconds=2.0, terminate=True)
        raise
    return lanes


def await_ready(lanes: list[ProcessLaneHandle], *, timeout_seconds: float) -> list[str]:
    """Prove containment before allowing a child to open its runtime."""

    errors: list[str] = []
    deadline = time.monotonic() + timeout_seconds
    waiting = {lane.worker_id for lane in lanes}
    while waiting and time.monotonic() < deadline:
        connections = [lane.connection for lane in lanes if lane.worker_id in waiting]
        timeout = min(0.05, max(0.0, deadline - time.monotonic()))
        for ready_connection in wait(connections, timeout=timeout):
            connection = cast(Connection, ready_connection)
            lane = lane_for_connection(lanes, connection)
            try:
                message = connection.recv()
            except _CHANNEL_RECEIVE_EXCEPTIONS:
                lane.process.join(timeout=0.05)
                error = (
                    native_exit_message(lane)
                    if lane.process.exitcode is not None
                    else f"{BOOTSTRAP_ERROR}: worker_id={lane.worker_id} channel_receive_failed=true"
                )
                errors.append(error)
                waiting.discard(lane.worker_id)
                continue
            if isinstance(message, ProcessLaneReady) and message.worker_id == lane.worker_id:
                if lane.ready is None and _ready_identity_is_valid(lane, message):
                    lane.ready = message
                    try:
                        connection.send(
                            ProcessLaneContainmentAck(
                                worker_id=message.worker_id,
                                pid=message.pid,
                                process_group_id=message.process_group_id,
                                session_id=message.session_id,
                            )
                        )
                    except (BrokenPipeError, EOFError, OSError):
                        errors.append(f"{BOOTSTRAP_ERROR}: worker_id={lane.worker_id} containment_ack_failed=true")
                        waiting.discard(lane.worker_id)
                else:
                    errors.append(f"{BOOTSTRAP_ERROR}: worker_id={lane.worker_id} error={PROCESS_TREE_ISOLATION_ERROR}")
                    waiting.discard(lane.worker_id)
            elif isinstance(message, ProcessLaneRuntimeReady) and message.worker_id == lane.worker_id:
                if lane.ready is None or lane.runtime_ready:
                    errors.append(f"{BOOTSTRAP_ERROR}: worker_id={lane.worker_id} readiness_order_invalid=true")
                else:
                    lane.runtime_ready = True
                waiting.discard(lane.worker_id)
            elif isinstance(message, ProcessLaneBootstrapFailed) and message.worker_id == lane.worker_id:
                errors.append(f"{BOOTSTRAP_ERROR}: worker_id={lane.worker_id} error={message.error}")
                waiting.discard(lane.worker_id)
        for lane in lanes:
            if lane.worker_id in waiting and lane.process.exitcode is not None:
                errors.append(native_exit_message(lane))
                waiting.discard(lane.worker_id)
    for worker_id in sorted(waiting):
        errors.append(f"{BOOTSTRAP_ERROR}: worker_id={worker_id} timeout=true")
    return errors


def _ready_identity_is_valid(lane: ProcessLaneHandle, ready: ProcessLaneReady) -> bool:
    if not ready.process_tree_isolated or not ready_identity_is_valid(lane, ready):
        return False
    try:
        return ready.session_id == ready.pid and os.getsid(ready.pid) == ready.session_id
    except (OSError, ProcessLookupError):
        return False


def stop_idle_lanes(lanes: list[ProcessLaneHandle]) -> None:
    """Ask only idle lanes to stop; running lanes remain bounded by their call."""

    for lane in lanes:
        if lane.running is None:
            stop_lane(lane)


def stop_lane(lane: ProcessLaneHandle) -> None:
    """Send an idempotent graceful stop request."""

    if lane.stopping or lane.process.exitcode is not None:
        return
    try:
        lane.connection.send(ProcessLaneStop())
    except (BrokenPipeError, EOFError, OSError):
        pass
    lane.stopping = True


def shutdown_lanes(
    lanes: list[ProcessLaneHandle],
    *,
    timeout_seconds: float,
    terminate: bool,
) -> None:
    """Bound cleanup and verify that Python plus every native descendant exited."""

    if terminate:
        for lane in lanes:
            signal_lane(lane, signal.SIGTERM)
    else:
        stop_idle_lanes(lanes)
    deadline = time.monotonic() + timeout_seconds
    for lane in lanes:
        lane.process.join(timeout=max(0.0, deadline - time.monotonic()))
    for lane in lanes:
        if lane_or_group_is_alive(lane):
            signal_lane(lane, signal.SIGTERM)
    join_lane_groups(lanes, timeout_seconds=1.0)
    for lane in lanes:
        if lane_or_group_is_alive(lane):
            signal_lane(lane, signal.SIGKILL)
    join_lane_groups(lanes, timeout_seconds=1.0)
    cleanup_failed = [lane.worker_id for lane in lanes if lane_or_group_is_alive(lane)]
    close_lane_handles(lanes)
    if cleanup_failed:
        workers = ",".join(str(worker_id) for worker_id in cleanup_failed)
        raise RuntimeError(f"{PROCESS_GROUP_ERROR}: worker_ids={workers}")


def terminate_running_lanes(
    lanes: list[ProcessLaneHandle],
    *,
    timeout_seconds: float,
) -> list[ProcessLaneHandle]:
    """Stop complete in-flight process trees, then return receipt candidates."""

    running = [lane for lane in lanes if lane.running is not None]
    terminate_lane_trees(running, timeout_seconds=timeout_seconds)
    return running


def terminate_lane_trees(
    lanes: list[ProcessLaneHandle],
    *,
    timeout_seconds: float,
) -> list[ProcessLaneHandle]:
    """Stop and prove cleanup of the complete native tree for each lane."""

    pending = [lane for lane in lanes if not lane.tree_quiesced]
    _terminate_lanes(pending, force=False)
    _wait_for_process_trees(pending, deadline=time.monotonic() + max(0.0, timeout_seconds))
    remaining = [lane for lane in pending if _lane_tree_is_active(lane)]
    if remaining:
        _terminate_lanes(remaining, force=True)
        _wait_for_process_trees(remaining, deadline=time.monotonic() + 1.0)
    cleanup_failed = [lane.worker_id for lane in pending if _lane_tree_is_active(lane)]
    for lane in lanes:
        lane.tree_quiesced = lane.worker_id not in cleanup_failed
    if cleanup_failed:
        workers = ",".join(str(worker_id) for worker_id in cleanup_failed)
        raise RuntimeError(f"{PROCESS_GROUP_ERROR}: worker_ids={workers}")
    return pending


def close_lane_handles(lanes: list[ProcessLaneHandle]) -> None:
    """Close quiesced IPC/process handles without another wait or signal phase."""

    failed: list[int] = []
    for lane in lanes:
        try:
            if _lane_tree_is_active(lane) or lane.process.exitcode is None:
                failed.append(lane.worker_id)
                continue
            lane.final_exitcode = lane.process.exitcode
            lane.connection.close()
            lane.process.close()
        except BaseException:
            failed.append(lane.worker_id)
    if failed:
        workers = ",".join(str(worker_id) for worker_id in failed)
        raise RuntimeError(f"{PROCESS_GROUP_ERROR}: worker_ids={workers}")


def _terminate_lanes(lanes: list[ProcessLaneHandle], *, force: bool) -> None:
    requested_signal = signal.SIGKILL if force else signal.SIGTERM
    for lane in lanes:
        if _lane_tree_is_active(lane):
            signal_lane(lane, requested_signal)


def _wait_for_process_trees(lanes: list[ProcessLaneHandle], *, deadline: float) -> None:
    """Observe every peer against one shared cleanup deadline."""

    for lane in lanes:
        if getattr(lane.process, "exitcode", None) is None:
            join = getattr(lane.process, "join", None)
            if callable(join):
                join(timeout=max(0.0, deadline - time.monotonic()))
    remaining = max(0.0, deadline - time.monotonic())
    if remaining > 0:
        join_lane_groups(lanes, timeout_seconds=remaining)


def _lane_tree_is_active(lane: ProcessLaneHandle) -> bool:
    if lane.process.is_alive():
        return True
    return lane.ready is not None and lane_or_group_is_alive(lane)


def quiesce_lane_process_tree(
    lane: ProcessLaneHandle,
    *,
    timeout_seconds: float,
) -> bool:
    """Prove the complete native tree stopped before a target receipt probe."""

    if lane.tree_quiesced:
        return True
    lane.stopping = True
    try:
        terminate_lane_trees([lane], timeout_seconds=timeout_seconds)
    except RuntimeError:
        lane.control_error = PROCESS_TREE_SIGNAL_ERROR
        return False
    return lane.tree_quiesced


def lane_for_connection(
    lanes: list[ProcessLaneHandle],
    connection: Connection,
) -> ProcessLaneHandle:
    """Resolve the unique lane owning an IPC endpoint."""

    return next(lane for lane in lanes if lane.connection is connection)


def receive_lane_message(lane: ProcessLaneHandle) -> tuple[Any | None, str | None]:
    """Decode one child frame or return one bounded ambiguous-failure code."""

    try:
        return lane.connection.recv(), None
    except _CHANNEL_RECEIVE_EXCEPTIONS:
        lane.process.join(timeout=0.05)
        error = native_exit_message(lane) if lane.process.exitcode is not None else CHANNEL_RECEIVE_ERROR
        return None, error


def handle_idle_native_exits(
    lanes: list[ProcessLaneHandle],
    ready_events: list[Any],
    *,
    group_cleanup_timeout_seconds: float,
    errors: list[str],
) -> bool:
    """Reject a dead redispatch candidate before another chunk lease claim."""

    found = False
    for lane in lanes:
        if lane.running is not None or lane.stopping:
            continue
        if lane.process.sentinel not in ready_events and lane.process.exitcode is None:
            continue
        lane.process.join(timeout=0)
        if lane.process.exitcode is None:
            continue
        errors.append(native_exit_message(lane))
        try:
            terminate_lane_trees([lane], timeout_seconds=group_cleanup_timeout_seconds)
        except RuntimeError:
            errors.append(PROCESS_TREE_SIGNAL_ERROR)
        lane.stopping = True
        found = True
    return found


__all__ = [
    "CHANNEL_RECEIVE_ERROR",
    "ProcessLaneHandle",
    "await_ready",
    "close_lane_handles",
    "diagnostics",
    "dispatch_failure_message",
    "handle_idle_native_exits",
    "handle_native_exits",
    "lane_for_connection",
    "native_exit_message",
    "quiesce_lane_process_tree",
    "receive_lane_message",
    "shutdown_lanes",
    "start_lanes",
    "stop_idle_lanes",
    "stop_lane",
    "terminate_lane_trees",
    "terminate_running_lanes",
]
