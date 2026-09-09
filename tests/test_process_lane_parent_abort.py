"""Shared-deadline parent abort and fail-closed cleanup contracts."""

from __future__ import annotations

import signal
import time
from types import SimpleNamespace

import pytest

from dpone.backfill import process_lane_abort_barrier, process_lane_processes
from dpone.backfill.process_lane_contracts import BackfillProcessLaneBootstrap
from dpone.backfill.process_lane_dispatch_support import LEDGER_FAILURE_WRITE_ERROR
from dpone.backfill.process_lane_group import PROCESS_GROUP_ERROR
from dpone.backfill.process_lane_parent_abort import cleanup_parent_abort


def _lane(worker_id: int):
    return SimpleNamespace(
        worker_id=worker_id,
        running=object(),
        control_error=None,
        tree_quiesced=False,
    )


def test_abort_quiesces_every_tree_before_any_receipt_or_ledger_callback() -> None:
    lanes = [_lane(worker_id) for worker_id in range(4)]
    events: list[str] = []

    def terminate(captured, *, deadline: float) -> None:
        assert captured == lanes
        assert deadline > 0
        events.append("terminate-all")
        for lane in captured:
            lane.tree_quiesced = True

    def abandon() -> bool:
        assert all(lane.tree_quiesced for lane in lanes)
        events.append("abandon-deferred")
        return True

    def resolve(lane, error: str) -> bool:
        assert all(peer.tree_quiesced for peer in lanes)
        assert error == "parent-aborted"
        events.append(f"resolve-{lane.worker_id}")
        lane.running = None
        return True

    def close(captured) -> None:
        assert captured == lanes
        events.append("close-all")

    outcome = cleanup_parent_abort(
        lanes,
        abandon_deferred=abandon,
        resolve_quiesced=resolve,
        terminate_before=terminate,
        close_handles=close,
        default_error="parent-aborted",
        timeout_seconds=0.25,
        errors=[],
    )

    assert outcome is None
    assert events == [
        "terminate-all",
        "abandon-deferred",
        "resolve-0",
        "resolve-1",
        "resolve-2",
        "resolve-3",
        "close-all",
    ]


def test_abort_aggregates_fatal_callbacks_and_always_processes_peers_and_shutdown() -> None:
    class _Fatal(BaseException):
        pass

    lanes = [_lane(worker_id) for worker_id in range(2)]
    resolved: list[int] = []
    shutdown_called = False

    def terminate(captured, *, deadline: float) -> None:
        del deadline
        for lane in captured:
            lane.tree_quiesced = True

    def resolve(lane, _error: str) -> bool:
        resolved.append(lane.worker_id)
        if lane.worker_id == 0:
            raise _Fatal("cleanup callback interrupted")
        return False

    def close(_captured) -> None:
        nonlocal shutdown_called
        shutdown_called = True
        raise _Fatal("final close interrupted")

    errors: list[str] = []

    outcome = cleanup_parent_abort(
        lanes,
        abandon_deferred=lambda: (_ for _ in ()).throw(_Fatal("deferred cleanup interrupted")),
        resolve_quiesced=resolve,
        terminate_before=terminate,
        close_handles=close,
        default_error="parent-aborted",
        timeout_seconds=0.25,
        errors=errors,
    )

    assert outcome == LEDGER_FAILURE_WRITE_ERROR
    assert resolved == [0, 1]
    assert shutdown_called is True
    assert errors == [LEDGER_FAILURE_WRITE_ERROR, PROCESS_GROUP_ERROR]


def test_partial_quiescence_failure_runs_no_ledger_callback_until_global_barrier() -> None:
    class _Fatal(BaseException):
        pass

    lanes = [_lane(worker_id) for worker_id in range(2)]
    callbacks: list[str] = []

    def partial_quiescence(captured, *, deadline: float) -> None:
        del deadline
        captured[0].tree_quiesced = True
        raise RuntimeError("peer group still executable")

    def failed_global_close(_captured) -> None:
        raise _Fatal("hard barrier unavailable")

    errors: list[str] = []

    outcome = cleanup_parent_abort(
        lanes,
        abandon_deferred=lambda: callbacks.append("abandon") is None,
        resolve_quiesced=lambda lane, _error: callbacks.append(f"resolve-{lane.worker_id}") is None,
        terminate_before=partial_quiescence,
        close_handles=failed_global_close,
        default_error="parent-aborted",
        timeout_seconds=0.25,
        errors=errors,
    )

    assert outcome == PROCESS_GROUP_ERROR
    assert callbacks == []
    assert errors == [PROCESS_GROUP_ERROR]


def test_hard_phase_cannot_be_skipped_by_interruption_between_graceful_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Fatal(BaseException):
        pass

    class _Process:
        exitcode: int | None = None

        def is_alive(self) -> bool:
            return self.exitcode is None

        def join(self, *, timeout: float) -> None:
            del timeout

    lanes = [
        SimpleNamespace(
            worker_id=worker_id,
            process=_Process(),
            ready=None,
            tree_quiesced=False,
        )
        for worker_id in range(4)
    ]
    signals: list[tuple[int, int]] = []
    real_monotonic = time.monotonic

    def signal_peer(lane, signal_number: int) -> None:
        signals.append((lane.worker_id, signal_number))
        if signal_number == signal.SIGKILL:
            lane.process.exitcode = -signal.SIGKILL

    monotonic_calls = 0

    def interrupted_clock() -> float:
        nonlocal monotonic_calls
        monotonic_calls += 1
        if monotonic_calls == 1:
            raise _Fatal("parent interrupted after graceful signal")
        return real_monotonic()

    monkeypatch.setattr(process_lane_abort_barrier, "signal_lane", signal_peer)
    monkeypatch.setattr(process_lane_abort_barrier.time, "monotonic", interrupted_clock)

    with pytest.raises(_Fatal, match="parent interrupted"):
        process_lane_abort_barrier.terminate_lane_trees_before(lanes, deadline=real_monotonic() + 1.0)

    assert [worker_id for worker_id, number in signals if number == signal.SIGTERM] == [0, 1, 2, 3]
    assert [worker_id for worker_id, number in signals if number == signal.SIGKILL] == [0, 1, 2, 3]
    assert all(lane.tree_quiesced for lane in lanes)


def test_live_process_handles_are_not_closed_after_failed_barrier() -> None:
    class _Connection:
        closed = False

        def close(self) -> None:
            self.closed = True

    process = SimpleNamespace(
        exitcode=None,
        is_alive=lambda: True,
    )
    connection = _Connection()
    lane = SimpleNamespace(
        worker_id=7,
        process=process,
        connection=connection,
        ready=None,
        final_exitcode=None,
    )

    with pytest.raises(RuntimeError, match=PROCESS_GROUP_ERROR):
        process_lane_processes.close_lane_handles([lane])

    assert connection.closed is False


def test_caller_owned_start_defers_all_cleanup_to_parent_lifecycle_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Fatal(BaseException):
        pass

    class _Connection:
        def close(self) -> None:
            return None

    class _Process:
        pid: int | None = None
        exitcode: int | None = None

        def start(self) -> None:
            self.pid = 41_001
            raise _Fatal("interrupted after spawn")

    process = _Process()
    context = SimpleNamespace(
        Pipe=lambda **_kwargs: (_Connection(), _Connection()),
        Process=lambda **_kwargs: process,
    )
    local_shutdowns: list[object] = []
    monkeypatch.setattr(process_lane_processes, "get_context", lambda _method: context)
    monkeypatch.setattr(
        process_lane_processes,
        "shutdown_lanes",
        lambda *_args, **_kwargs: local_shutdowns.append(object()),
    )
    owned: list[object] = []

    with pytest.raises(_Fatal, match="interrupted after spawn"):
        process_lane_processes.start_lanes(
            BackfillProcessLaneBootstrap(entrypoint="tests:unused", payload=None),
            1,
            ownership=owned,
        )

    assert len(owned) == 1
    assert owned[0].process is process
    assert local_shutdowns == []
