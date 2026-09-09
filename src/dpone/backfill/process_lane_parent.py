"""Parent-owned event loop for spawned, fixed backfill lanes."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Mapping
from multiprocessing.connection import Connection, wait
from typing import Any, cast

from dpone.backfill.process_lane_abort_barrier import terminate_lane_trees_before
from dpone.backfill.process_lane_contracts import (
    PROCESS_TREE_SIGNAL_ERROR,
    BackfillProcessLaneBootstrap,
    ProcessLaneClaimHandoff,
    ProcessLaneDiagnostic,
    ProcessLaneDispatch,
    ProcessLaneExecutionSummary,
)
from dpone.backfill.process_lane_dispatch import (
    DispatchBatchOutcome,
    DispatchInterruptionHandoff,
    abandon_prepared_batch,
    dispatch_prepared_batch,
    identity_dispatch,
)
from dpone.backfill.process_lane_messages import (
    ParentLaneFailureResolver,
    handle_lane_message,
)
from dpone.backfill.process_lane_operation import (
    OPERATION_LEASE_ERROR,
    ParentOperationLeaseCoordinator,
)
from dpone.backfill.process_lane_parent_abort import cleanup_parent_abort
from dpone.backfill.process_lane_parent_heartbeat import (
    _CHUNK_LEASE_ERROR,
    _heartbeat_running,
    _mark_all_running_control_error,
    _mark_exact_running_control_error,
)
from dpone.backfill.process_lane_preflight import process_lane_bootstrap_serialization_error
from dpone.backfill.process_lane_processes import (
    ProcessLaneHandle,
    await_ready,
    close_lane_handles,
    handle_idle_native_exits,
    handle_native_exits,
    lane_for_connection,
    shutdown_lanes,
    start_lanes,
    stop_idle_lanes,
    terminate_running_lanes,
)
from dpone.backfill.process_lane_processes import (
    diagnostics as lane_diagnostics,
)
from dpone.backfill.process_lane_receipt import ParentReceiptReplayCoordinator

_COORDINATOR_ERROR = "DPONE_BACKFILL_PROCESS_COORDINATOR_LOST"
_DISPATCH_CANCELLED = "DPONE_BACKFILL_PROCESS_DISPATCH_CANCELLED"
_DEFERRED_ABORTED = "DPONE_BACKFILL_PROCESS_DEFERRED_CLAIM_ABORTED"
_PARENT_ABORTED = "DPONE_BACKFILL_PROCESS_PARENT_ABORTED"
_PEER_DRAIN_ERROR = "DPONE_BACKFILL_PROCESS_PEER_DRAIN_TIMEOUT"


def run_process_chunk_lanes(
    pending: list[Any],
    *,
    workers: int,
    runtime: BackfillProcessLaneBootstrap,
    claim_chunk: Callable[[int, Any, ProcessLaneClaimHandoff], None],
    complete_chunk: Callable[[ProcessLaneDispatch, Mapping[str, Any]], bool],
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    heartbeat_chunk: Callable[[ProcessLaneDispatch], bool],
    heartbeat_interval_seconds: float,
    renew_operation_lease: Callable[[Any, Any], bool] | None = None,
    validate_operation_binding: Callable[..., bool] | None = None,
    issue_receipt_probe: Callable[..., tuple[Any, Any] | None] | None = None,
    validate_replay_result: Callable[..., bool] | None = None,
    coordinator_tick: Callable[[], None] | None = None,
    coordinator_reprove: Callable[[], None] | None = None,
    recover_committed_receipt: Callable[[ProcessLaneDispatch, Any], bool] | None = None,
    refresh_dispatch: Callable[[ProcessLaneDispatch], ProcessLaneDispatch] | None = None,
    startup_timeout_seconds: float = 20.0,
    poll_interval_seconds: float = 0.05,
    shutdown_timeout_seconds: float = 5.0,
) -> ProcessLaneExecutionSummary:
    """Dispatch chunks without waiting on one child or sharing an interpreter."""

    if not pending:
        return ProcessLaneExecutionSummary(errors=(), lanes=())
    if workers < 1 or heartbeat_interval_seconds <= 0:
        raise ValueError("backfill.process_lane_policy_invalid")
    bootstrap_error = process_lane_bootstrap_serialization_error(runtime)
    if bootstrap_error is not None:
        return ProcessLaneExecutionSummary(errors=(bootstrap_error,), lanes=())

    operation_coordinator = ParentOperationLeaseCoordinator(
        renew=renew_operation_lease,
        validate_binding=validate_operation_binding,
    )
    receipt_coordinator = ParentReceiptReplayCoordinator(
        issue_probe=issue_receipt_probe,
        validate_result=validate_replay_result,
    )
    lane_count = min(workers, len(pending))
    lanes: list[ProcessLaneHandle] = []
    errors: list[str] = []
    diagnostics: list[ProcessLaneDiagnostic] = []
    queue = deque(pending)
    stop_dispatch = False
    failure_deadline: float | None = None
    next_heartbeat = time.monotonic() + heartbeat_interval_seconds
    coordinator_failed = False
    dispatch_ownership = DispatchInterruptionHandoff()

    resolve_failure = ParentLaneFailureResolver(
        fail_chunk,
        recover_committed_receipt,
        operation_coordinator,
        receipt_coordinator,
        errors,
        shutdown_timeout_seconds,
    )

    def tick_coordinator() -> str | None:
        nonlocal coordinator_failed
        if coordinator_failed:
            return _COORDINATOR_ERROR
        if coordinator_tick is None:
            return None
        try:
            coordinator_tick()
        except Exception:
            coordinator_failed = True
            _mark_all_running_control_error(lanes, _COORDINATOR_ERROR)
            return _COORDINATOR_ERROR
        return None

    def reprove_coordinator() -> str | None:
        nonlocal coordinator_failed
        if coordinator_failed:
            return _COORDINATOR_ERROR
        if coordinator_reprove is None:
            if coordinator_tick is None:
                return None
            coordinator_failed = True
            _mark_all_running_control_error(lanes, _COORDINATOR_ERROR)
            return _COORDINATOR_ERROR
        try:
            coordinator_reprove()
        except Exception:
            coordinator_failed = True
            _mark_all_running_control_error(lanes, _COORDINATOR_ERROR)
            return _COORDINATOR_ERROR
        return None

    def prove_prepared_dispatch(dispatch: ProcessLaneDispatch) -> str | None:
        try:
            healthy = bool(heartbeat_chunk(dispatch))
        except Exception:
            healthy = False
        return None if healthy else _CHUNK_LEASE_ERROR

    def drain_lane_events(timeout_seconds: float) -> bool:
        """Serve child IPC and native exits at every parent control safe point."""

        nonlocal stop_dispatch
        active_lanes = [lane for lane in lanes if lane.running is not None]
        if not active_lanes:
            return stop_dispatch
        active_connections = [lane.connection for lane in active_lanes]
        process_sentinels = [lane.process.sentinel for lane in active_lanes]
        ready_events = list(wait([*active_connections, *process_sentinels], timeout=timeout_seconds))
        if handle_native_exits(
            lanes,
            ready_events,
            resolve_failure=resolve_failure,
            errors=errors,
        ):
            stop_dispatch = True
        for ready_connection in (event for event in ready_events if event in active_connections):
            connection = cast(Connection, ready_connection)
            lane = lane_for_connection(lanes, connection)
            if lane.running is None:
                continue
            stop_dispatch = (
                handle_lane_message(
                    lane,
                    complete_chunk=complete_chunk,
                    fail_chunk=fail_chunk,
                    operation_coordinator=operation_coordinator,
                    receipt_coordinator=receipt_coordinator,
                    recover_committed_receipt=recover_committed_receipt,
                    errors=errors,
                    stop_dispatch=stop_dispatch,
                    quiesce_timeout_seconds=shutdown_timeout_seconds,
                )
                or stop_dispatch
            )
        remaining_sentinels = [lane.process.sentinel for lane in lanes if lane.running is not None]
        ready_exits = list(wait(remaining_sentinels, timeout=0)) if remaining_sentinels else []
        if handle_native_exits(
            lanes,
            ready_exits,
            resolve_failure=resolve_failure,
            errors=errors,
        ):
            stop_dispatch = True
        return stop_dispatch

    def service_dispatch_control() -> str | None:
        return _DISPATCH_CANCELLED if drain_lane_events(0) else None

    def gate_dispatch_control() -> bool:
        """Reject co-ready peer activity without executing callbacks after the fence."""

        if stop_dispatch:
            return True
        active_lanes = [lane for lane in lanes if lane.running is not None]
        if not active_lanes:
            return False
        ready = wait(
            [
                *(lane.connection for lane in active_lanes),
                *(lane.process.sentinel for lane in active_lanes),
            ],
            timeout=0,
        )
        return bool(ready)

    def dispatch_batch(candidates: list[ProcessLaneHandle]) -> DispatchBatchOutcome:
        return dispatch_prepared_batch(
            candidates,
            queue,
            claim_chunk=claim_chunk,
            fail_chunk=fail_chunk,
            control_tick=tick_coordinator,
            final_control_tick=reprove_coordinator,
            final_control_gate=gate_dispatch_control,
            refresh_dispatch=refresh_dispatch or identity_dispatch,
            prove_dispatch=prove_prepared_dispatch,
            errors=errors,
            service_control=service_dispatch_control,
            interruption_handoff=dispatch_ownership,
        ).outcome

    def abandon_deferred(error: str) -> bool:
        prepared = dispatch_ownership.unsent()
        if not prepared:
            return True
        healthy = abandon_prepared_batch(
            prepared,
            fail_chunk=fail_chunk,
            error=error,
            errors=errors,
        )
        if healthy:
            dispatch_ownership.release(prepared)
        return healthy

    try:
        start_lanes(runtime, lane_count, ownership=lanes)
        startup_errors = await_ready(lanes, timeout_seconds=startup_timeout_seconds)
        errors.extend(startup_errors)
        if startup_errors:
            stop_dispatch = True
        else:
            initial_exits = list(wait([lane.process.sentinel for lane in lanes], timeout=0))
            stop_dispatch = handle_idle_native_exits(
                lanes,
                initial_exits,
                group_cleanup_timeout_seconds=shutdown_timeout_seconds,
                errors=errors,
            )
            if not stop_dispatch:
                stop_dispatch = dispatch_batch(lanes) is DispatchBatchOutcome.FAILED

        while any(lane.running is not None for lane in lanes):
            drain_lane_events(poll_interval_seconds)

            coordinator_error = tick_coordinator()
            if coordinator_error is not None:
                if coordinator_error not in errors:
                    errors.append(coordinator_error)
                stop_dispatch = True
            drain_lane_events(0)
            now = time.monotonic()
            if now >= next_heartbeat:
                if not _heartbeat_running(
                    lanes,
                    heartbeat_chunk,
                    after_each=lambda: drain_lane_events(0),
                ):
                    if _CHUNK_LEASE_ERROR not in errors:
                        errors.append(_CHUNK_LEASE_ERROR)
                    stop_dispatch = True
                next_heartbeat = now + heartbeat_interval_seconds
            running = {
                (lane.worker_id, dispatch.command_id): dispatch
                for lane in lanes
                if (dispatch := lane.running) is not None
            }
            failed_operation_leases = operation_coordinator.tick(
                running,
                on_failure=lambda key: _mark_exact_running_control_error(
                    lanes,
                    key,
                    OPERATION_LEASE_ERROR,
                ),
                after_each=lambda: drain_lane_events(0),
            )
            if failed_operation_leases:
                if OPERATION_LEASE_ERROR not in errors:
                    errors.append(OPERATION_LEASE_ERROR)
                stop_dispatch = True

            redispatch_candidates = [
                lane for lane in lanes if lane.running is None and not lane.stopping and lane.runtime_ready
            ]
            redispatch_exits = (
                list(wait([lane.process.sentinel for lane in redispatch_candidates], timeout=0))
                if redispatch_candidates
                else []
            )
            if handle_idle_native_exits(
                redispatch_candidates,
                redispatch_exits,
                group_cleanup_timeout_seconds=shutdown_timeout_seconds,
                errors=errors,
            ):
                stop_dispatch = True
            if not stop_dispatch:
                # When peers are active, claim only one new command before the
                # loop returns to its IPC-first boundary.  If every lane is
                # idle, the complete batch is prepared before any child starts.
                running_now = any(lane.running is not None for lane in lanes)
                candidates = redispatch_candidates[:1] if running_now else redispatch_candidates
                stop_dispatch = dispatch_batch(candidates) is DispatchBatchOutcome.FAILED
            if stop_dispatch:
                stop_idle_lanes(lanes)
                if failure_deadline is None:
                    failure_deadline = time.monotonic() + shutdown_timeout_seconds
                elif time.monotonic() >= failure_deadline:
                    terminated = terminate_running_lanes(lanes, timeout_seconds=shutdown_timeout_seconds)
                    for lane in terminated:
                        if lane.control_error == PROCESS_TREE_SIGNAL_ERROR:
                            errors.append(PROCESS_TREE_SIGNAL_ERROR)
                        error = f"{_PEER_DRAIN_ERROR}: worker_id={lane.worker_id}"
                        errors.append(error)
                        resolve_failure(lane, error)

        abandon_deferred(errors[-1] if errors else _DEFERRED_ABORTED)
        stop_idle_lanes(lanes)
        shutdown_lanes(lanes, timeout_seconds=shutdown_timeout_seconds, terminate=False)
        diagnostics.extend(lane_diagnostics(lanes))
        return ProcessLaneExecutionSummary(errors=tuple(errors), lanes=tuple(diagnostics))
    except BaseException as exc:
        cleanup_error = cleanup_parent_abort(
            lanes,
            abandon_deferred=lambda: abandon_deferred(errors[-1] if errors else _DEFERRED_ABORTED),
            resolve_quiesced=lambda lane, error: resolve_failure.resolve(
                lane,
                error,
                prequiesced=True,
            ),
            terminate_before=terminate_lane_trees_before,
            close_handles=close_lane_handles,
            default_error=_PARENT_ABORTED,
            timeout_seconds=shutdown_timeout_seconds,
            errors=errors,
        )
        if cleanup_error is not None:
            raise RuntimeError(cleanup_error) from exc
        raise


__all__ = ["run_process_chunk_lanes"]
