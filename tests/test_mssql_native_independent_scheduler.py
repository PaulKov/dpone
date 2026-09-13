"""Controlled futures exercise stage bounds without timing or live SQL claims."""

from concurrent.futures import FIRST_COMPLETED, Future
from dataclasses import replace
from threading import Event

import pytest

import dpone.runtime.mssql_native_chunks as chunks
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError
from tests.test_mssql_native_chunks_execution import Target, setup


class ControlledPool:
    """Hold submitted work; the driver completes only an eligible worker slot."""

    def __init__(self, max_workers, **kwargs):
        self.max_workers = max_workers
        self.tasks = {}
        self.shutdown_calls = []
        self.peak = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown(wait=True)

    def submit(self, function, *args):
        future = Future()
        self.tasks[future] = (function, args)
        self.peak = max(self.peak, len(self.eligible()))
        return future

    def eligible(self):
        return [future for future in self.tasks if not future.done()][: self.max_workers]

    def finish(self, future):
        assert future in self.eligible()
        function, args = self.tasks[future]
        try:
            future.set_result(function(*args))
        except BaseException as error:
            future.set_exception(error)

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdown_calls.append((wait, cancel_futures))
        for future in self.tasks:
            if not future.done():
                future.cancel()


class Source:
    """One acquired source, with observable pulls and unconditional closure."""

    def __init__(self, count):
        self.count, self.pulled, self.closed = count, 0, False

    def __iter__(self):
        return self

    def __next__(self):
        if self.pulled == self.count:
            raise StopIteration
        self.pulled += 1
        return (self.pulled,)

    def close(self):
        self.closed = True


class Driver:
    """Progress encoders first, then newest eligible import, at scheduler waits."""

    def __init__(self, monkeypatch, source, capacity):
        self.source, self.capacity = source, capacity
        self.encoders = self.imports = None
        self.snapshots = []
        self.before_complete = lambda pool, future, pending: None
        self.import_order = []
        monkeypatch.setattr(chunks, "ProcessPoolExecutor", self.encoding_pool)
        monkeypatch.setattr(chunks, "ThreadPoolExecutor", self.import_pool)
        monkeypatch.setattr(chunks, "wait", self.wait)

    def encoding_pool(self, **kwargs):
        assert kwargs["mp_context"].get_start_method() == "spawn"
        self.encoders = ControlledPool(**kwargs)
        return self.encoders

    def import_pool(self, **kwargs):
        self.imports = ControlledPool(**kwargs)
        return self.imports

    def wait(self, pending, *, timeout, return_when):
        assert return_when == FIRST_COMPLETED
        assert len(pending) <= self.capacity
        # Every retained slot has exactly one future across both pools.
        unfinished = {f for pool in (self.encoders, self.imports) for f in pool.tasks if not f.done()}
        assert set(pending) == unfinished
        self.snapshots.append((len(pending), self.source.pulled))
        assert len(self.snapshots) < 200, "scheduler failed to make bounded progress"
        pool = self.encoders if self.encoders.eligible() else self.imports
        future = getattr(self, "preferred_future", None) or pool.eligible()[-1]
        self.preferred_future = None
        self.before_complete(pool, future, pending)
        if pool is self.imports:
            self.import_order.append(pool.tasks[future][1][1].ordinal)
        pool.finish(future)
        return {future}, set(pending) - {future}


def configured(tmp_path, target, encoding, imports):
    executor, plan, lease, contract = setup(tmp_path, target)
    executor.limits = replace(executor.limits, encoding_parallelism=encoding, import_parallelism=imports, max_pending=2)
    assert executor.limits.effective_encoding_parallelism == encoding
    assert executor.limits.effective_import_parallelism == imports
    assert executor.limits.retained_work_capacity == max(encoding, imports) + 2
    return executor, plan, lease, contract


@pytest.mark.parametrize("encoding,imports", [(1, 3), (3, 1)])
def test_independent_stage_saturation_preserves_one_shared_capacity(tmp_path, monkeypatch, encoding, imports):
    target = Target()
    executor, plan, lease, contract = configured(tmp_path, target, encoding, imports)
    capacity = max(encoding, imports) + 2
    source = Source(capacity * 2)
    driver = Driver(monkeypatch, source, capacity)
    result = executor.stage(plan, source, contract, lease)
    assert driver.encoders.max_workers == encoding
    assert driver.imports.max_workers == imports
    assert driver.encoders.peak == encoding
    assert driver.imports.peak == imports
    # No release/refill during any encode -> import transition, even with
    # all encoders blocked initially or all imports blocked subsequently.
    assert driver.snapshots[: capacity + 1] == [(capacity, capacity + 1)] * (capacity + 1)
    assert source.closed and source.pulled == source.count
    assert result.rows == source.count
    assert [receipt.ordinal for receipt in result.receipts] == list(range(source.count))
    assert driver.import_order != sorted(driver.import_order)
    assert NativeChunkJournal(executor.store, lease, plan).completed() == result
    assert not list(executor.work_dir.rglob("*.native"))


@pytest.mark.parametrize("encoding,imports", [(1, 3), (3, 1)])
def test_saturated_retry_reuses_slot_file_bytes_and_durable_attempt(tmp_path, monkeypatch, encoding, imports):
    target = Target(failures=2)
    executor, plan, lease, contract = configured(tmp_path, target, encoding, imports)
    capacity = max(encoding, imports) + 2
    source = Source(capacity + 3)
    driver = Driver(monkeypatch, source, capacity)
    attempts = []

    def inspect_attempt(pool, future, pending):
        if pool is driver.imports and len(attempts) < 3:
            _, file, attempt, _, _ = pool.tasks[future][1]
            # Finish the first import's retries before any other import.
            attempts.append((file, file.path.read_bytes(), attempt))
            assert attempt in NativeChunkJournal(executor.store, lease, plan).attempts()
            assert len(pending) == capacity
            assert source.pulled == capacity + 1

    # Retry submits at the tail; select it even when earlier imports remain
    # queued. max_workers still governs eligibility for ordinary progression.
    original_wait = driver.wait

    def retry_first(pending, **kwargs):
        if attempts and len(attempts) < 3:
            retry = next(
                f
                for f, (_, args) in driver.imports.tasks.items()
                if not f.done() and args[1].ordinal == attempts[0][0].ordinal
            )
            driver.preferred_future = retry
            tasks = driver.imports.tasks
            driver.imports.tasks = {retry: tasks[retry], **{f: task for f, task in tasks.items() if f is not retry}}
        return original_wait(pending, **kwargs)

    driver.before_complete = inspect_attempt
    monkeypatch.setattr(chunks, "wait", retry_first)
    executor.stage(plan, source, contract, lease)
    assert len(attempts) == 3
    assert attempts[0][0] is attempts[1][0] is attempts[2][0]
    assert attempts[0][1] == attempts[1][1] == attempts[2][1]
    assert [attempt for _, _, attempt in attempts] == [f"run-{attempts[0][0].ordinal}-{n}" for n in range(3)]
    assert target.settled[:2] == [attempts[0][2], attempts[1][2]]
    assert source.closed


@pytest.mark.parametrize("encoding,imports", [(1, 3), (3, 1)])
@pytest.mark.parametrize("failure", ["encode", "import", "cancel"])
def test_failure_and_cancellation_close_source_without_completion(tmp_path, monkeypatch, encoding, imports, failure):
    target = Target()
    executor, plan, lease, contract = configured(tmp_path, target, encoding, imports)
    capacity = max(encoding, imports) + 2
    source = Source(capacity + 3)
    driver = Driver(monkeypatch, source, capacity)
    cancelled = Event()

    def fail(pool, future, pending):
        chosen = driver.encoders if failure == "encode" else driver.imports
        if pool is chosen:
            if failure == "cancel":
                cancelled.set()
            else:

                def broken(*args):
                    raise RuntimeError(f"controlled {failure} failure")

                _, args = pool.tasks[future]
                pool.tasks[future] = (broken, args)

    driver.before_complete = fail
    expected = WindowContractError if failure == "cancel" else RuntimeError
    with pytest.raises(expected, match="cancelled" if failure == "cancel" else f"controlled {failure} failure"):
        executor.stage(plan, source, contract, lease, cancelled)
    assert source.closed
    assert source.pulled == capacity + 1
    assert NativeChunkJournal(executor.store, lease, plan).completed() is None
    for pool in (driver.encoders, driver.imports):
        assert (True, True) in pool.shutdown_calls
        assert all(future.done() for future in pool.tasks)
