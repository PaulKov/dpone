"""Thread admission linearization and synchronous failure boundaries."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from queue import SimpleQueue
from threading import Event, Lock

import pytest

from dpone.backfill.parallel_execution import _run_lane, _run_parallel_chunk_lanes
from dpone.runtime._backfill_admission import _ChunkAdmission


class _FatalLaneError(BaseException):
    pass


def test_closed_admission_performs_no_claim_or_false_lease_rejection() -> None:
    gate = _ChunkAdmission()
    gate.close()
    gate.close()
    calls = []
    assert gate.acquire(lambda: calls.append("claimed") or True) is None
    assert calls == []


def test_claim_inside_boundary_finishes_before_competing_closure() -> None:
    gate = _ChunkAdmission()
    claim_entered = Event()
    close_requested = Event()
    close_lock_entered = Event()
    release_claim = Event()

    class _ObservedLock:
        def __init__(self):
            self.lock = Lock()

        def __enter__(self):
            if close_requested.is_set():
                close_lock_entered.set()
            self.lock.acquire()

        def __exit__(self, *_args):
            self.lock.release()

    gate._lock = _ObservedLock()

    order = []

    def claim():
        claim_entered.set()
        assert release_claim.wait(5)
        order.append("durable claim")
        return True

    def close():
        close_requested.set()
        gate.close()
        order.append("closed")

    with ThreadPoolExecutor(max_workers=2) as pool:
        acquired = pool.submit(gate.acquire, claim)
        try:
            assert claim_entered.wait(5)
            closed = pool.submit(close)
            assert close_lock_entered.wait(5)
            assert not gate.closed
        finally:
            release_claim.set()
        assert acquired.result(timeout=5) is True
        closed.result(timeout=5)
    assert order == ["durable claim", "closed"]
    assert gate.acquire(lambda: pytest.fail("claim after closure")) is None


@pytest.mark.parametrize("outcome", [False, RuntimeError("claim failed"), _FatalLaneError("fatal claim")])
def test_rejected_or_exceptional_claim_closes_before_next_store_access(outcome) -> None:
    gate = _ChunkAdmission()

    def claim():
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    if isinstance(outcome, BaseException):
        with pytest.raises(type(outcome)) as raised:
            gate.acquire(claim)
        assert raised.value is outcome
    else:
        assert gate.acquire(claim) is False
    assert gate.closed
    assert gate.acquire(lambda: pytest.fail("second store access")) is None


@pytest.mark.parametrize("failure_at", ["runner_entry", "runner_exit", "callback", "store_exit"])
@pytest.mark.parametrize("fatal", [False, True])
def test_lane_failure_closes_before_enclosing_context_cleanup(failure_at, fatal) -> None:
    gate = _ChunkAdmission()
    error = _FatalLaneError("fatal") if fatal else RuntimeError("ordinary")
    exits = []
    queue = SimpleQueue()
    queue.put(1)

    @contextmanager
    def store_factory(_worker):
        try:
            yield object()
        finally:
            exits.append("store")
            if failure_at == "store_exit":
                raise error
            assert gate.closed  # runner teardown cannot postpone closure until here.

    @contextmanager
    def runner_factory(_worker):
        if failure_at == "runner_entry":
            raise error
        try:
            yield None
        finally:
            exits.append("runner")
            if failure_at == "runner_exit":
                raise error
            if failure_at == "callback":
                assert gate.closed

    def run_chunk(*_args):
        if failure_at == "callback":
            raise error
        return True

    with pytest.raises(type(error)) as raised:
        _run_lane(
            0,
            queue,
            admission=gate,
            state_store_factory=store_factory,
            chunk_runner_factory=runner_factory,
            run_chunk=run_chunk,
        )
    assert raised.value is error
    assert gate.closed
    assert exits == (["store"] if failure_at == "runner_entry" else ["runner", "store"])


@pytest.mark.parametrize("fatal", [False, True])
def test_coordinator_failure_closes_before_pool_wait_without_cancelling_admitted_peer(fatal) -> None:
    released = Event()
    first_running = Event()
    calls = []
    error = _FatalLaneError("coordinator") if fatal else RuntimeError("coordinator")

    class _ObservedAdmission(_ChunkAdmission):
        def close(self):
            super().close()
            released.set()

    gate = _ObservedAdmission()

    @contextmanager
    def store_factory(_worker):
        yield object()

    def run_chunk(chunk, _store, _errors, _runner):
        if gate.acquire(lambda: True) is None:
            return False
        calls.append(chunk)
        first_running.set()
        assert released.wait(5)
        return True

    def coordinator():
        assert first_running.wait(5)
        raise error

    def run():
        return _run_parallel_chunk_lanes(
            [1, 2, 3],
            workers=2,
            state_store_factory=store_factory,
            chunk_runner_factory=None,
            run_chunk=run_chunk,
            coordinator_tick=coordinator,
            admission=gate,
        )

    try:
        if fatal:
            with pytest.raises(_FatalLaneError) as raised:
                run()
            assert raised.value is error
        else:
            assert run() == ["DPONE_BACKFILL_PROCESS_COORDINATOR_LOST"]
    finally:
        released.set()
    assert gate.closed
    assert 1 <= len(calls) <= 2
    assert 3 not in calls
