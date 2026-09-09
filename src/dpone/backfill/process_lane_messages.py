"""Terminal and authority messages for parent-owned process lanes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from dpone.backfill.process_lane_contracts import (
    PROCESS_TREE_SIGNAL_ERROR,
    ProcessLaneDispatch,
    ProcessLaneFailed,
    ProcessLaneOperationLeaseRequest,
    ProcessLaneReceiptProbeRequest,
    ProcessLaneSucceeded,
)
from dpone.backfill.process_lane_dispatch_support import LEDGER_FAILURE_WRITE_ERROR
from dpone.backfill.process_lane_operation import ParentOperationLeaseCoordinator
from dpone.backfill.process_lane_processes import (
    ProcessLaneHandle,
    quiesce_lane_process_tree,
    receive_lane_message,
    stop_lane,
)
from dpone.backfill.process_lane_receipt import (
    PARENT_RECEIPT_VALIDATION_ERROR,
    ParentReceiptReplayCoordinator,
)


@dataclass(slots=True)
class ParentLaneFailureResolver:
    """Resolve exact claims with shared parent coordinators and policy."""

    fail_chunk: Callable[[ProcessLaneDispatch, str], None]
    recover_committed_receipt: Callable[[ProcessLaneDispatch, Any], bool] | None
    operation_coordinator: ParentOperationLeaseCoordinator
    receipt_coordinator: ParentReceiptReplayCoordinator
    errors: list[str]
    timeout_seconds: float

    def __call__(self, lane: ProcessLaneHandle, error: str) -> None:
        self.resolve(lane, error)

    def resolve(
        self,
        lane: ProcessLaneHandle,
        error: str,
        *,
        prequiesced: bool = False,
    ) -> bool:
        return resolve_ambiguous_failure(
            lane,
            error,
            fail_chunk=self.fail_chunk,
            recover_committed_receipt=self.recover_committed_receipt,
            operation_coordinator=self.operation_coordinator,
            receipt_coordinator=self.receipt_coordinator,
            errors=self.errors,
            quiesce_timeout_seconds=self.timeout_seconds,
            prequiesced=prequiesced,
        )


def handle_lane_message(
    lane: ProcessLaneHandle,
    *,
    complete_chunk: Callable[[ProcessLaneDispatch, Mapping[str, Any]], bool],
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    operation_coordinator: ParentOperationLeaseCoordinator,
    receipt_coordinator: ParentReceiptReplayCoordinator,
    recover_committed_receipt: Callable[[ProcessLaneDispatch, Any], bool] | None,
    errors: list[str],
    stop_dispatch: bool,
    quiesce_timeout_seconds: float,
) -> bool:
    """Validate one child frame and perform at most one terminal transition."""

    message, receive_error = receive_lane_message(lane)
    if receive_error is not None:
        errors.append(receive_error)
        resolve_ambiguous_failure(
            lane,
            receive_error,
            fail_chunk=fail_chunk,
            recover_committed_receipt=recover_committed_receipt,
            operation_coordinator=operation_coordinator,
            receipt_coordinator=receipt_coordinator,
            errors=errors,
            quiesce_timeout_seconds=quiesce_timeout_seconds,
        )
        return True
    if isinstance(message, ProcessLaneOperationLeaseRequest):
        return (
            not operation_coordinator.handle(
                lane.connection,
                message,
                worker_id=lane.worker_id,
                dispatch=lane.running,
                control_error=lane.control_error,
            )
            or stop_dispatch
        )
    if isinstance(message, ProcessLaneReceiptProbeRequest):
        return (
            not receipt_coordinator.handle(
                lane.connection,
                message,
                worker_id=lane.worker_id,
                dispatch=lane.running,
                control_error=lane.control_error,
            )
            or stop_dispatch
        )
    running = lane.running
    if running is None:
        errors.append("DPONE_BACKFILL_PROCESS_UNEXPECTED_CHILD_MESSAGE")
        return True
    if isinstance(message, ProcessLaneSucceeded) and _matches_result(lane, message.worker_id, message.command_id):
        if lane.control_error is not None:
            resolve_ambiguous_failure(
                lane,
                lane.control_error,
                fail_chunk=fail_chunk,
                recover_committed_receipt=recover_committed_receipt,
                operation_coordinator=operation_coordinator,
                receipt_coordinator=receipt_coordinator,
                errors=errors,
                quiesce_timeout_seconds=quiesce_timeout_seconds,
            )
            return True
        operation_registered = operation_coordinator.operation_for(lane.worker_id, running) is not None
        if not receipt_coordinator.validate_success(
            lane.worker_id,
            running,
            message.result,
            operation_registered=operation_registered,
        ):
            errors.append(PARENT_RECEIPT_VALIDATION_ERROR)
            resolve_ambiguous_failure(
                lane,
                PARENT_RECEIPT_VALIDATION_ERROR,
                fail_chunk=fail_chunk,
                recover_committed_receipt=recover_committed_receipt,
                operation_coordinator=operation_coordinator,
                receipt_coordinator=receipt_coordinator,
                errors=errors,
                quiesce_timeout_seconds=quiesce_timeout_seconds,
            )
            return True
        try:
            keep_running = complete_chunk(running, message.result)
        except Exception:
            error = "DPONE_BACKFILL_PROCESS_LEDGER_COMPLETION_FAILED"
            errors.append(error)
            resolve_ambiguous_failure(
                lane,
                error,
                fail_chunk=fail_chunk,
                recover_committed_receipt=recover_committed_receipt,
                operation_coordinator=operation_coordinator,
                receipt_coordinator=receipt_coordinator,
                errors=errors,
                quiesce_timeout_seconds=quiesce_timeout_seconds,
            )
            return True
        operation_coordinator.clear(lane.worker_id, running)
        receipt_coordinator.clear(lane.worker_id, running)
        _clear_running(lane)
        if stop_dispatch or not keep_running:
            stop_lane(lane)
            return not keep_running
        return False
    if isinstance(message, ProcessLaneFailed) and _matches_result(lane, message.worker_id, message.command_id):
        errors.append(message.error)
        resolve_ambiguous_failure(
            lane,
            message.error,
            fail_chunk=fail_chunk,
            recover_committed_receipt=recover_committed_receipt,
            operation_coordinator=operation_coordinator,
            receipt_coordinator=receipt_coordinator,
            errors=errors,
            quiesce_timeout_seconds=quiesce_timeout_seconds,
        )
        return True
    errors.append("DPONE_BACKFILL_PROCESS_CHILD_MESSAGE_IDENTITY_MISMATCH")
    lane.control_error = "DPONE_BACKFILL_PROCESS_CHILD_MESSAGE_IDENTITY_MISMATCH"
    return True


def resolve_ambiguous_failure(
    lane: ProcessLaneHandle,
    error: str,
    *,
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    recover_committed_receipt: Callable[[ProcessLaneDispatch, Any], bool] | None,
    operation_coordinator: ParentOperationLeaseCoordinator,
    receipt_coordinator: ParentReceiptReplayCoordinator,
    errors: list[str],
    quiesce_timeout_seconds: float,
    prequiesced: bool = False,
) -> bool:
    """Quiesce the lane, recover a registered receipt, or fail its claim."""

    running = lane.running
    if running is None:
        return True
    tree_quiesced = (
        lane.tree_quiesced
        if prequiesced
        else quiesce_lane_process_tree(
            lane,
            timeout_seconds=quiesce_timeout_seconds,
        )
    )
    if not tree_quiesced:
        if PROCESS_TREE_SIGNAL_ERROR not in errors:
            errors.append(PROCESS_TREE_SIGNAL_ERROR)
        operation_coordinator.clear(lane.worker_id, running)
        receipt_coordinator.clear(lane.worker_id, running)
        _clear_running(lane)
        return True
    recovered = False
    operation = operation_coordinator.operation_for(lane.worker_id, running)
    if recover_committed_receipt is not None and operation is not None:
        try:
            recovered = bool(recover_committed_receipt(running, operation))
        except BaseException:
            errors.append("DPONE_BACKFILL_PROCESS_RECEIPT_RECOVERY_FAILED")
    ledger_healthy = True
    if not recovered:
        try:
            fail_chunk(running, error)
        except BaseException:
            errors.append(LEDGER_FAILURE_WRITE_ERROR)
            ledger_healthy = False
    operation_coordinator.clear(lane.worker_id, running)
    receipt_coordinator.clear(lane.worker_id, running)
    _clear_running(lane)
    return ledger_healthy


def _matches_result(lane: ProcessLaneHandle, worker_id: int, command_id: str) -> bool:
    return lane.running is not None and worker_id == lane.worker_id and command_id == lane.running.command_id


def _clear_running(lane: ProcessLaneHandle) -> None:
    lane.running = None
    lane.control_error = None


__all__ = [
    "ParentLaneFailureResolver",
    "handle_lane_message",
    "resolve_ambiguous_failure",
]
