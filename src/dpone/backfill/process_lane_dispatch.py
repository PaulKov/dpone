"""Two-phase parent dispatch for fixed spawned backfill lanes.

Claims and parent-only preflight may perform catalog or state I/O.  The parent
therefore prepares and re-proves the complete startup batch before sending the
first child command.  Once publication starts, only bounded pipe writes remain;
no child starts an authority handshake while the parent is still preparing a
peer.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NamedTuple

from dpone._compat import StrEnum
from dpone.backfill.process_lane_contracts import (
    ProcessLaneBinding,
    ProcessLaneClaimHandoff,
    ProcessLaneDispatch,
    ProcessLaneRun,
)
from dpone.backfill.process_lane_dispatch_support import (
    LEDGER_FAILURE_WRITE_ERROR,
    _abandon,
    _abort_after_service,
    _abort_prepared,
    _append_once,
    _prepared_lane_exit_error,
    _same_claim,
    identity_dispatch,
)
from dpone.backfill.process_lane_processes import (
    ProcessLaneHandle,
    dispatch_failure_message,
    native_exit_message,
)


class DispatchBatchOutcome(StrEnum):
    """Exact result of one two-phase parent dispatch attempt."""

    PUBLISHED = "published"
    DEFERRED = "deferred"
    FAILED = "failed"


class PreparedLaneDispatch(NamedTuple):
    """One claimed command that has not crossed the child IPC boundary."""

    lane: ProcessLaneHandle
    chunk: Any
    dispatch: ProcessLaneDispatch
    message: ProcessLaneRun | None = None


class DispatchBatchResult(NamedTuple):
    """Outcome plus exact claims retained across a non-servicing deferral."""

    outcome: DispatchBatchOutcome
    prepared: tuple[PreparedLaneDispatch, ...] = ()


@dataclass(slots=True)
class DispatchInterruptionHandoff:
    """Persistent parent ownership of claims not yet terminal or published.

    The same cell survives every deferred retry.  The dispatcher updates it
    before returning, so an asynchronous parent interruption cannot lose a
    batch between a local tuple swap and the surrounding abort barrier.
    """

    prepared: tuple[PreparedLaneDispatch, ...] = ()

    def capture(self, item: PreparedLaneDispatch) -> None:
        """Publish one newly durable exact claim before its callback returns."""

        self.prepared = (*self.prepared, item)

    def unsent(self) -> tuple[PreparedLaneDispatch, ...]:
        """Return claims not represented by an exact running-lane identity."""

        return tuple(
            item
            for item in self.prepared
            if item.lane.running is None or not _same_claim(item.dispatch, item.lane.running)
        )

    def release(self, released: tuple[PreparedLaneDispatch, ...]) -> None:
        """Forget claims only after their exact terminal callback completed."""

        released_ids = {id(item) for item in released}
        self.prepared = tuple(item for item in self.prepared if id(item) not in released_ids)

    def release_dispatch(self, dispatch: ProcessLaneDispatch) -> None:
        """Forget one exact claim after its durable terminal callback returns."""

        self.prepared = tuple(item for item in self.prepared if not _same_claim(item.dispatch, dispatch))


@dataclass(frozen=True, slots=True)
class _DispatchCallbacks:
    claim_chunk: Callable[[int, Any, ProcessLaneClaimHandoff], None]
    fail_chunk: Callable[[ProcessLaneDispatch, str], None]
    control_tick: Callable[[], str | None]
    final_control_tick: Callable[[], str | None]
    final_control_gate: Callable[[], bool]
    refresh_dispatch: Callable[[ProcessLaneDispatch], ProcessLaneDispatch]
    prove_dispatch: Callable[[ProcessLaneDispatch], str | None]
    service_control: Callable[[], str | None] | None
    errors: list[str]


def dispatch_prepared_batch(
    lanes: list[ProcessLaneHandle],
    queue: deque[Any],
    *,
    claim_chunk: Callable[[int, Any, ProcessLaneClaimHandoff], None],
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    control_tick: Callable[[], str | None],
    final_control_tick: Callable[[], str | None],
    final_control_gate: Callable[[], bool],
    refresh_dispatch: Callable[[ProcessLaneDispatch], ProcessLaneDispatch],
    prove_dispatch: Callable[[ProcessLaneDispatch], str | None],
    errors: list[str],
    service_control: Callable[[], str | None] | None = None,
    prepared_batch: tuple[PreparedLaneDispatch, ...] = (),
    interruption_handoff: DispatchInterruptionHandoff | None = None,
) -> DispatchBatchResult:
    """Prepare, re-prove, then publish one exact batch without interleaved I/O."""

    ownership = interruption_handoff or DispatchInterruptionHandoff(prepared_batch)
    prepared = list(ownership.prepared)
    callbacks = _DispatchCallbacks(
        claim_chunk=claim_chunk,
        fail_chunk=fail_chunk,
        control_tick=control_tick,
        final_control_tick=final_control_tick,
        final_control_gate=final_control_gate,
        refresh_dispatch=refresh_dispatch,
        prove_dispatch=prove_dispatch,
        service_control=service_control,
        errors=errors,
    )
    result: DispatchBatchResult | None = None
    try:
        result = _dispatch_prepared_lifecycle(lanes, queue, prepared, ownership, callbacks)
        if interruption_handoff is not None:
            interruption_handoff.prepared = (
                interruption_handoff.unsent() if result.outcome is DispatchBatchOutcome.FAILED else result.prepared
            )
        return result
    except BaseException as exc:
        # Ownership is the first fatal-path write. Diagnostics may stringify an
        # arbitrary BaseException and are intentionally best-effort afterwards.
        if interruption_handoff is not None and result is not None:
            interruption_handoff.prepared = (
                interruption_handoff.unsent() if result.outcome is DispatchBatchOutcome.FAILED else result.prepared
            )
        try:
            error = dispatch_failure_message("lifecycle", exc)
        except BaseException:
            error = "DPONE_BACKFILL_PROCESS_DISPATCH_FAILED: stage=lifecycle"
        _append_once(errors, error)
        if interruption_handoff is None and not _abandon(
            ownership.prepared,
            fail_chunk=fail_chunk,
            error=error,
            errors=errors,
        ):
            raise RuntimeError(LEDGER_FAILURE_WRITE_ERROR) from exc
        raise


def _dispatch_prepared_lifecycle(
    lanes: list[ProcessLaneHandle],
    queue: deque[Any],
    prepared: list[PreparedLaneDispatch],
    ownership: DispatchInterruptionHandoff,
    callbacks: _DispatchCallbacks,
) -> DispatchBatchResult:
    claim_chunk = callbacks.claim_chunk
    terminal_fail_chunk = callbacks.fail_chunk

    def fail_chunk(dispatch: ProcessLaneDispatch, error: str) -> None:
        terminal_fail_chunk(dispatch, error)
        ownership.release_dispatch(dispatch)

    control_tick = callbacks.control_tick
    final_control_tick = callbacks.final_control_tick
    final_control_gate = callbacks.final_control_gate
    refresh_dispatch = callbacks.refresh_dispatch
    prove_dispatch = callbacks.prove_dispatch
    service_control = callbacks.service_control
    errors = callbacks.errors
    if not prepared:
        for lane in lanes:
            if not queue:
                break
            if lane.running is not None or lane.stopping or not lane.runtime_ready:
                continue
            if lane.process.exitcode is not None:
                error = native_exit_message(lane)
                _append_once(errors, error)
                _abandon(prepared, fail_chunk=fail_chunk, error=error, errors=errors)
                return DispatchBatchResult(DispatchBatchOutcome.FAILED)
            chunk = queue.popleft()
            stage = "claim"
            try:
                handoff = _LaneClaimHandoff(lane, chunk, prepared, ownership)
                claim_chunk(lane.worker_id, chunk, handoff)
                dispatch = handoff.dispatch
                if dispatch is None:
                    raise RuntimeError("backfill.process_lane_claim_handoff_missing")
                stage = "serialize"
                ProcessLaneRun.from_dispatch(dispatch)
            except Exception as exc:
                error = dispatch_failure_message(stage, exc)
                _append_once(errors, error)
                _abandon(
                    ownership.prepared,
                    fail_chunk=fail_chunk,
                    error=error,
                    errors=errors,
                )
                return DispatchBatchResult(DispatchBatchOutcome.FAILED)
            if _abort_after_service(
                service_control,
                prepared,
                fail_chunk=fail_chunk,
                errors=errors,
            ):
                return DispatchBatchResult(DispatchBatchOutcome.FAILED)
            control_error = control_tick()
            if _abort_prepared(control_error, prepared, fail_chunk=fail_chunk, errors=errors):
                return DispatchBatchResult(DispatchBatchOutcome.FAILED)
            if _abort_after_service(
                service_control,
                prepared,
                fail_chunk=fail_chunk,
                errors=errors,
            ):
                return DispatchBatchResult(DispatchBatchOutcome.FAILED)

    if not prepared:
        return DispatchBatchResult(DispatchBatchOutcome.PUBLISHED)

    refreshed: list[PreparedLaneDispatch] = []
    for item in prepared:
        stage = "refresh"
        try:
            dispatch = refresh_dispatch(item.dispatch)
            if not _same_claim(item.dispatch, dispatch):
                raise RuntimeError("backfill.process_lane_dispatch_refresh_rebound")
            stage = "serialize"
            message = ProcessLaneRun.from_dispatch(dispatch)
        except Exception as exc:
            error = dispatch_failure_message(stage, exc)
            _append_once(errors, error)
            _abandon(prepared, fail_chunk=fail_chunk, error=error, errors=errors)
            return DispatchBatchResult(DispatchBatchOutcome.FAILED)
        refreshed.append(PreparedLaneDispatch(item.lane, item.chunk, dispatch, message))
        remaining = prepared[len(refreshed) :]
        if _abort_after_service(
            service_control,
            refreshed,
            remaining,
            fail_chunk=fail_chunk,
            errors=errors,
        ):
            return DispatchBatchResult(DispatchBatchOutcome.FAILED)
        control_error = control_tick()
        if _abort_prepared(
            control_error,
            refreshed,
            remaining,
            fail_chunk=fail_chunk,
            errors=errors,
        ):
            return DispatchBatchResult(DispatchBatchOutcome.FAILED)
        if _abort_after_service(
            service_control,
            refreshed,
            remaining,
            fail_chunk=fail_chunk,
            errors=errors,
        ):
            return DispatchBatchResult(DispatchBatchOutcome.FAILED)
        proof_error = prove_dispatch(dispatch)
        if _abort_prepared(
            proof_error,
            refreshed,
            remaining,
            fail_chunk=fail_chunk,
            errors=errors,
        ):
            return DispatchBatchResult(DispatchBatchOutcome.FAILED)
        if _abort_after_service(
            service_control,
            refreshed,
            remaining,
            fail_chunk=fail_chunk,
            errors=errors,
        ):
            return DispatchBatchResult(DispatchBatchOutcome.FAILED)

    # Heartbeat proofs and peer callbacks may themselves wait on SQL.  Drain
    # those callbacks first, then force the campaign session proof.  The final
    # gate may inspect readiness only: no callback or database I/O is allowed
    # between that proof and the first irreversible child IPC send.
    if _abort_after_service(
        service_control,
        refreshed,
        fail_chunk=fail_chunk,
        errors=errors,
    ):
        return DispatchBatchResult(DispatchBatchOutcome.FAILED)
    control_error = final_control_tick()
    if _abort_prepared(control_error, refreshed, fail_chunk=fail_chunk, errors=errors):
        return DispatchBatchResult(DispatchBatchOutcome.FAILED)
    try:
        deferred = bool(final_control_gate())
    except Exception as exc:
        error = dispatch_failure_message("final_gate", exc)
        _append_once(errors, error)
        _abandon(refreshed, fail_chunk=fail_chunk, error=error, errors=errors)
        return DispatchBatchResult(DispatchBatchOutcome.FAILED)
    if deferred:
        return DispatchBatchResult(DispatchBatchOutcome.DEFERRED, tuple(refreshed))
    exit_error = _prepared_lane_exit_error(refreshed)
    if _abort_prepared(exit_error, refreshed, fail_chunk=fail_chunk, errors=errors):
        return DispatchBatchResult(DispatchBatchOutcome.FAILED)

    for index, item in enumerate(refreshed):
        lane = item.lane
        if lane.process.exitcode is not None:
            error = native_exit_message(lane)
            _append_once(errors, error)
            _abandon(refreshed[index:], fail_chunk=fail_chunk, error=error, errors=errors)
            return DispatchBatchResult(DispatchBatchOutcome.FAILED)
        # Once a write starts, Python cannot distinguish a zero-byte failure
        # from a complete frame followed by an acknowledgement-side error.
        # Publish durable ownership to the parent state machine first so every
        # such outcome is quiesced and receipt-recovered, never re-claimed.
        lane.running = item.dispatch
        lane.control_error = None
        try:
            assert item.message is not None
            lane.connection.send(item.message)
        except BaseException as exc:
            error = dispatch_failure_message("ipc_send", exc)
            lane.control_error = error
            _append_once(errors, error)
            if isinstance(exc, Exception):
                _abandon(refreshed[index + 1 :], fail_chunk=fail_chunk, error=error, errors=errors)
                return DispatchBatchResult(DispatchBatchOutcome.FAILED)
            raise
    return DispatchBatchResult(DispatchBatchOutcome.PUBLISHED)


@dataclass(slots=True)
class _LaneClaimHandoff:
    """Bind one claim continuation to its exact lane and ownership cell."""

    lane: ProcessLaneHandle
    chunk: Any
    prepared: list[PreparedLaneDispatch]
    ownership: DispatchInterruptionHandoff

    def __call__(self, dispatch: ProcessLaneDispatch) -> None:
        if self.dispatch is not None:
            raise RuntimeError("backfill.process_lane_claim_handoff_repeated")
        item = PreparedLaneDispatch(self.lane, self.chunk, dispatch)
        self.ownership.capture(item)
        self.prepared.append(item)

    @property
    def dispatch(self) -> ProcessLaneDispatch | None:
        for item in reversed(self.ownership.prepared):
            if item.lane is self.lane and item.chunk is self.chunk:
                return item.dispatch
        return None

    def owns(self, binding: ProcessLaneBinding) -> bool:
        return any(item.dispatch.binding == binding for item in self.ownership.prepared)


def abandon_prepared_batch(
    prepared: tuple[PreparedLaneDispatch, ...],
    *,
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    error: str,
    errors: list[str],
) -> bool:
    """Terminally release retained unsent claims through their exact owner CAS."""

    return _abandon(prepared, fail_chunk=fail_chunk, error=error, errors=errors)


__all__ = [
    "DispatchBatchOutcome",
    "DispatchBatchResult",
    "DispatchInterruptionHandoff",
    "PreparedLaneDispatch",
    "abandon_prepared_batch",
    "dispatch_prepared_batch",
    "identity_dispatch",
]
