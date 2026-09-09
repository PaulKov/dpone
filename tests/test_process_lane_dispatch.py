"""Two-phase process-lane dispatch and rollback contracts."""

from __future__ import annotations

from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.backfill.process_lane_contracts import (
    ProcessLaneBinding,
    ProcessLaneDispatch,
)
from dpone.backfill.process_lane_dispatch import (
    DispatchBatchOutcome,
    DispatchInterruptionHandoff,
    abandon_prepared_batch,
    dispatch_prepared_batch,
)


class _Connection:
    def __init__(
        self,
        *,
        fail: bool = False,
        signal: BaseException | None = None,
        append_before_failure: bool = False,
    ) -> None:
        self.fail = fail
        self.signal = signal
        self.append_before_failure = append_before_failure
        self.sent: list[object] = []

    def send(self, message: object) -> None:
        if self.append_before_failure:
            self.sent.append(message)
        if self.signal is not None:
            raise self.signal
        if self.fail:
            raise BrokenPipeError("test pipe closed")
        if not self.append_before_failure:
            self.sent.append(message)


def _lane(
    worker_id: int,
    *,
    fail_send: bool = False,
    send_signal: BaseException | None = None,
    append_before_failure: bool = False,
) -> Any:
    return SimpleNamespace(
        worker_id=worker_id,
        process=SimpleNamespace(exitcode=None),
        connection=_Connection(
            fail=fail_send,
            signal=send_signal,
            append_before_failure=append_before_failure,
        ),
        ready=None,
        running=None,
        control_error=None,
        stopping=False,
        runtime_ready=True,
    )


def _dispatch(
    worker_id: int,
    chunk: int,
    capture=None,
) -> ProcessLaneDispatch | None:
    dispatch = ProcessLaneDispatch(
        command_id=f"chunk-{chunk}",
        load_config=chunk,
        binding=ProcessLaneBinding("campaign-a", chunk, f"owner-{worker_id}"),
        parent_context=chunk,
    )
    if capture is None:
        return dispatch
    capture(dispatch)
    return None


def test_control_loss_abandons_only_claimed_unsent_commands() -> None:
    lanes = [_lane(worker_id) for worker_id in range(3)]
    queue = deque([1, 2, 3])
    failed: list[int] = []
    ticks = 0

    def tick() -> str | None:
        nonlocal ticks
        ticks += 1
        return "DPONE_BACKFILL_PROCESS_COORDINATOR_LOST" if ticks == 2 else None

    healthy = dispatch_prepared_batch(
        lanes,
        queue,
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=tick,
        final_control_tick=tick,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        errors=(errors := []),
    )

    assert healthy.outcome is DispatchBatchOutcome.FAILED
    assert failed == [1, 2]
    assert list(queue) == [3]
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert errors == ["DPONE_BACKFILL_PROCESS_COORDINATOR_LOST"]


def test_proof_failure_abandons_the_complete_prepared_batch_before_send() -> None:
    lanes = [_lane(worker_id) for worker_id in range(2)]
    failed: list[int] = []

    healthy = dispatch_prepared_batch(
        lanes,
        deque([1, 2]),
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=lambda: None,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda dispatch: (
            "DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST" if dispatch.parent_context == 2 else None
        ),
        errors=(errors := []),
    )

    assert healthy.outcome is DispatchBatchOutcome.FAILED
    assert failed == [1, 2]
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert errors == ["DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST"]


def test_ambiguous_pipe_failure_keeps_current_lane_for_receipt_drain_and_releases_later_claims() -> None:
    lanes = [_lane(0), _lane(1, fail_send=True), _lane(2)]
    failed: list[int] = []

    healthy = dispatch_prepared_batch(
        lanes,
        deque([1, 2, 3]),
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=lambda: None,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        errors=(errors := []),
    )

    assert healthy.outcome is DispatchBatchOutcome.FAILED
    assert lanes[0].running is not None
    assert len(lanes[0].connection.sent) == 1
    assert lanes[1].running is not None
    assert lanes[1].control_error is not None
    assert lanes[2].running is None
    assert failed == [3]
    assert len(errors) == 1
    assert "stage=ipc_send" in errors[0]


def test_final_campaign_fence_loss_after_all_chunk_proofs_sends_nothing() -> None:
    lanes = [_lane(worker_id) for worker_id in range(2)]
    failed: list[int] = []
    proofs = 0

    def prove(_dispatch: ProcessLaneDispatch) -> str | None:
        nonlocal proofs
        proofs += 1
        return None

    def tick() -> str | None:
        return "DPONE_BACKFILL_PROCESS_COORDINATOR_LOST" if proofs == 2 else None

    healthy = dispatch_prepared_batch(
        lanes,
        deque([1, 2]),
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=tick,
        final_control_tick=tick,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=prove,
        errors=(errors := []),
    )

    assert healthy.outcome is DispatchBatchOutcome.FAILED
    assert failed == [1, 2]
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert errors == ["DPONE_BACKFILL_PROCESS_COORDINATOR_LOST"]


def test_final_campaign_fence_is_forced_after_proofs_and_before_first_send() -> None:
    lanes = [_lane(worker_id) for worker_id in range(2)]
    proofs = 0

    class _Lease:
        calls: list[tuple[bool, int, int]] = []

        def tick(self, *, force: bool = False) -> None:
            sent = sum(len(lane.connection.sent) for lane in lanes)
            self.calls.append((force, proofs, sent))

    lease = _Lease()

    def prove(_dispatch: ProcessLaneDispatch) -> str | None:
        nonlocal proofs
        proofs += 1
        return None

    healthy = dispatch_prepared_batch(
        lanes,
        deque([1, 2]),
        claim_chunk=_dispatch,
        fail_chunk=lambda _dispatch, _error: None,
        control_tick=lambda: lease.tick(),
        final_control_tick=lambda: lease.tick(force=True),
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=prove,
        errors=[],
    )

    assert healthy.outcome is DispatchBatchOutcome.PUBLISHED
    assert lease.calls[-1] == (True, 2, 0)
    assert all(len(lane.connection.sent) == 1 for lane in lanes)


def test_prepared_lane_exit_after_peer_gate_aborts_whole_batch_without_send() -> None:
    lanes = [_lane(worker_id) for worker_id in range(4)]
    failed: list[int] = []

    def final_tick() -> str | None:
        lanes[-1].process.exitcode = 91
        return None

    result = dispatch_prepared_batch(
        lanes,
        deque([1, 2, 3, 4]),
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=final_tick,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        errors=(errors := []),
    )

    assert result.outcome is DispatchBatchOutcome.FAILED
    assert failed == [1, 2, 3, 4]
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert len(errors) == 1
    assert "worker_id=3" in errors[0]


def test_ready_peer_after_final_fence_defers_and_requeues_without_callbacks() -> None:
    lanes = [_lane(worker_id) for worker_id in range(2)]
    queue = deque([1, 2])
    proofs = 0
    final_fence_proved = False
    failed: list[int] = []
    ownership = DispatchInterruptionHandoff()

    def prove(_dispatch: ProcessLaneDispatch) -> str | None:
        nonlocal proofs
        proofs += 1
        return None

    def final_tick() -> str | None:
        nonlocal final_fence_proved
        final_fence_proved = True
        return None

    def service() -> str | None:
        if final_fence_proved:
            raise AssertionError("final gate must not execute peer callbacks")
        return None

    def gate() -> bool:
        assert final_fence_proved is True
        return True

    healthy = dispatch_prepared_batch(
        lanes,
        queue,
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=final_tick,
        final_control_gate=gate,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=prove,
        service_control=service,
        errors=(errors := []),
        interruption_handoff=ownership,
    )

    assert proofs == 2
    assert healthy.outcome is DispatchBatchOutcome.DEFERRED
    assert failed == []
    assert list(queue) == []
    assert len(healthy.prepared) == 2
    assert ownership.prepared == healthy.prepared
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert errors == []

    resumed = dispatch_prepared_batch(
        lanes,
        queue,
        claim_chunk=lambda _worker_id, _chunk, _capture: pytest.fail("retained claim must not be reacquired"),
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=lambda: None,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        errors=errors,
        interruption_handoff=ownership,
    )

    assert resumed.outcome is DispatchBatchOutcome.PUBLISHED
    assert resumed.prepared == ()
    assert ownership.prepared == ()
    assert failed == []
    assert all(len(lane.connection.sent) == 1 for lane in lanes)


def test_retained_cleanup_reports_new_write_failure_when_diagnostic_is_preseeded() -> None:
    lanes = [_lane(worker_id) for worker_id in range(2)]
    deferred = dispatch_prepared_batch(
        lanes,
        deque([1, 2]),
        claim_chunk=_dispatch,
        fail_chunk=lambda _dispatch, _error: None,
        control_tick=lambda: None,
        final_control_tick=lambda: None,
        final_control_gate=lambda: True,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        errors=[],
    )
    errors = ["DPONE_BACKFILL_PROCESS_LEDGER_FAILURE_WRITE_FAILED"]

    def unavailable_ledger(_dispatch: ProcessLaneDispatch, _error: str) -> None:
        raise RuntimeError("state unavailable")

    healthy = abandon_prepared_batch(
        deferred.prepared,
        fail_chunk=unavailable_ledger,
        error="DPONE_BACKFILL_PROCESS_COORDINATOR_LOST",
        errors=errors,
    )

    assert deferred.outcome is DispatchBatchOutcome.DEFERRED
    assert healthy is False
    assert errors == ["DPONE_BACKFILL_PROCESS_LEDGER_FAILURE_WRITE_FAILED"]


@pytest.mark.parametrize(
    ("failure_call", "expected_failed", "expected_queued"),
    ((1, [1], [2, 3]), (3, [1, 2], [3])),
)
def test_service_callback_exception_releases_every_exact_unsent_claim_once(
    failure_call: int,
    expected_failed: list[int],
    expected_queued: list[int],
) -> None:
    lanes = [_lane(worker_id) for worker_id in range(3)]
    queue = deque([1, 2, 3])
    failed: list[int] = []
    service_calls = 0

    def service() -> str | None:
        nonlocal service_calls
        service_calls += 1
        if service_calls == failure_call:
            raise RuntimeError("control callback failed")
        return None

    result = dispatch_prepared_batch(
        lanes,
        queue,
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=lambda: None,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        service_control=service,
        errors=(errors := []),
    )

    assert result.outcome is DispatchBatchOutcome.FAILED
    assert failed == expected_failed
    assert list(queue) == expected_queued
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert len(errors) == 1
    assert "stage=service_control" in errors[0]


def test_service_callback_process_signal_releases_claims_before_reraise() -> None:
    class _ProcessSignal(BaseException):
        pass

    lanes = [_lane(worker_id) for worker_id in range(2)]
    failed: list[int] = []
    service_calls = 0

    def service() -> str | None:
        nonlocal service_calls
        service_calls += 1
        if service_calls == 3:
            raise _ProcessSignal("stop")
        return None

    with pytest.raises(_ProcessSignal, match="stop"):
        dispatch_prepared_batch(
            lanes,
            deque([1, 2]),
            claim_chunk=_dispatch,
            fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
            control_tick=lambda: None,
            final_control_tick=lambda: None,
            final_control_gate=lambda: False,
            refresh_dispatch=lambda dispatch: dispatch,
            prove_dispatch=lambda _dispatch: None,
            service_control=service,
            errors=(errors := []),
        )

    assert failed == [1, 2]
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert len(errors) == 1
    assert "stage=lifecycle" in errors[0]


def test_process_signal_surfaces_unsent_ledger_cleanup_failure() -> None:
    class _ProcessSignal(BaseException):
        pass

    lanes = [_lane(worker_id) for worker_id in range(2)]
    failed: list[int] = []

    def unavailable_ledger(dispatch: ProcessLaneDispatch, _error: str) -> None:
        failed.append(dispatch.parent_context)
        raise RuntimeError("state unavailable")

    def interrupt_refresh(_dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
        raise _ProcessSignal("stop")

    with pytest.raises(
        RuntimeError,
        match="^DPONE_BACKFILL_PROCESS_LEDGER_FAILURE_WRITE_FAILED$",
    ) as caught:
        dispatch_prepared_batch(
            lanes,
            deque([1, 2]),
            claim_chunk=_dispatch,
            fail_chunk=unavailable_ledger,
            control_tick=lambda: None,
            final_control_tick=lambda: None,
            final_control_gate=lambda: False,
            refresh_dispatch=interrupt_refresh,
            prove_dispatch=lambda _dispatch: None,
            errors=[],
        )

    assert isinstance(caught.value.__cause__, _ProcessSignal)
    assert failed == [1, 2]
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)


def test_process_signal_hands_unsent_cleanup_to_parent_barrier() -> None:
    class _ProcessSignal(BaseException):
        pass

    lanes = [_lane(worker_id) for worker_id in range(2)]
    failed: list[int] = []
    handoff = DispatchInterruptionHandoff()

    with pytest.raises(_ProcessSignal, match="stop"):
        dispatch_prepared_batch(
            lanes,
            deque([1, 2]),
            claim_chunk=_dispatch,
            fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
            control_tick=lambda: None,
            final_control_tick=lambda: None,
            final_control_gate=lambda: False,
            refresh_dispatch=lambda _dispatch: (_ for _ in ()).throw(_ProcessSignal("stop")),
            prove_dispatch=lambda _dispatch: None,
            errors=[],
            interruption_handoff=handoff,
        )

    assert failed == []
    assert len(handoff.prepared) == 2
    assert abandon_prepared_batch(
        handoff.prepared,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        error="parent-aborted",
        errors=[],
    )
    assert failed == [1, 2]


def test_claim_continuation_publishes_ownership_before_fatal_callback_unwind() -> None:
    class _Fatal(BaseException):
        def __str__(self) -> str:
            raise BaseException("diagnostic formatting interrupted")

    lane = _lane(0)
    ownership = DispatchInterruptionHandoff()

    def claim(worker_id: int, chunk: int, capture) -> None:
        dispatch = _dispatch(worker_id, chunk)
        assert dispatch is not None
        capture(dispatch)
        assert capture.owns(dispatch.binding)
        raise _Fatal()

    with pytest.raises(_Fatal):
        dispatch_prepared_batch(
            [lane],
            deque([1]),
            claim_chunk=claim,
            fail_chunk=lambda _dispatch, _error: pytest.fail("parent barrier owns cleanup"),
            control_tick=lambda: None,
            final_control_tick=lambda: None,
            final_control_gate=lambda: False,
            refresh_dispatch=lambda dispatch: dispatch,
            prove_dispatch=lambda _dispatch: None,
            errors=[],
            interruption_handoff=ownership,
        )

    assert len(ownership.prepared) == 1
    assert ownership.prepared[0].dispatch.parent_context == 1
    assert lane.running is None


def test_ordinary_claim_error_after_transfer_terminally_releases_owned_cell() -> None:
    lane = _lane(0)
    ownership = DispatchInterruptionHandoff()
    failed: list[int] = []

    def claim(worker_id: int, chunk: int, capture) -> None:
        dispatch = _dispatch(worker_id, chunk)
        assert dispatch is not None
        capture(dispatch)
        raise RuntimeError("post-transfer callback failure")

    result = dispatch_prepared_batch(
        [lane],
        deque([1]),
        claim_chunk=claim,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=lambda: None,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        errors=[],
        interruption_handoff=ownership,
    )

    assert result.outcome is DispatchBatchOutcome.FAILED
    assert failed == [1]
    assert ownership.prepared == ()


@pytest.mark.parametrize("failure_stage", ("refresh", "proof", "final_tick", "final_gate"))
def test_process_signal_at_any_pre_send_stage_releases_all_claims(
    failure_stage: str,
) -> None:
    class _ProcessSignal(BaseException):
        pass

    lanes = [_lane(worker_id) for worker_id in range(2)]
    failed: list[int] = []

    def signal() -> None:
        raise _ProcessSignal(failure_stage)

    with pytest.raises(_ProcessSignal, match=failure_stage):
        dispatch_prepared_batch(
            lanes,
            deque([1, 2]),
            claim_chunk=_dispatch,
            fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
            control_tick=lambda: None,
            final_control_tick=signal if failure_stage == "final_tick" else lambda: None,
            final_control_gate=signal if failure_stage == "final_gate" else lambda: False,
            refresh_dispatch=(
                (lambda _dispatch: signal()) if failure_stage == "refresh" else lambda dispatch: dispatch
            ),
            prove_dispatch=((lambda _dispatch: signal()) if failure_stage == "proof" else lambda _dispatch: None),
            errors=(errors := []),
        )

    assert failed == [1, 2]
    assert all(not lane.connection.sent and lane.running is None for lane in lanes)
    assert len(errors) == 1
    assert "stage=lifecycle" in errors[0]


def test_process_signal_during_partial_send_preserves_sent_lane_for_recovery() -> None:
    class _ProcessSignal(BaseException):
        pass

    lanes = [_lane(0), _lane(1, send_signal=_ProcessSignal("send")), _lane(2)]
    failed: list[int] = []
    ownership = DispatchInterruptionHandoff()

    with pytest.raises(_ProcessSignal, match="send"):
        dispatch_prepared_batch(
            lanes,
            deque([1, 2, 3]),
            claim_chunk=_dispatch,
            fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
            control_tick=lambda: None,
            final_control_tick=lambda: None,
            final_control_gate=lambda: False,
            refresh_dispatch=lambda dispatch: dispatch,
            prove_dispatch=lambda _dispatch: None,
            errors=[],
            interruption_handoff=ownership,
        )

    assert lanes[0].running is not None
    assert len(lanes[0].connection.sent) == 1
    assert lanes[1].running is not None
    assert lanes[1].control_error is not None
    assert lanes[2].running is None
    unsent = ownership.unsent()
    assert failed == []
    assert [item.dispatch.parent_context for item in unsent] == [3]
    assert abandon_prepared_batch(
        unsent,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        error="parent-aborted",
        errors=[],
    )
    assert failed == [3]


def test_full_frame_then_send_error_is_still_ambiguous_running() -> None:
    lanes = [_lane(0, fail_send=True, append_before_failure=True), _lane(1)]
    failed: list[int] = []

    result = dispatch_prepared_batch(
        lanes,
        deque([1, 2]),
        claim_chunk=_dispatch,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        control_tick=lambda: None,
        final_control_tick=lambda: None,
        final_control_gate=lambda: False,
        refresh_dispatch=lambda dispatch: dispatch,
        prove_dispatch=lambda _dispatch: None,
        errors=[],
    )

    assert result.outcome is DispatchBatchOutcome.FAILED
    assert len(lanes[0].connection.sent) == 1
    assert lanes[0].running is not None
    assert lanes[0].control_error is not None
    assert lanes[1].running is None
    assert failed == [2]
