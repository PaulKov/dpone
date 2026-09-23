"""Real lifecycle CAS transitions with deterministic process/clock seams."""

from __future__ import annotations

from uuid import uuid4

import pytest

from dpone.adapters.mssql_tds_journal_actor import TdsJournalActor
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.contracts.bounded_window import WindowContractError, WindowLease, WindowOutcomeUnknown, WindowRecord
from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_tds_result import TdsWorkerResult, attempt_identity_digest
from dpone.contracts.mssql_tds_worker import (
    LaunchIntent,
    Prepared,
    TdsAttemptIdentity,
    TdsAttemptPhase,
    TdsChildExit,
    TdsObjectIdentity,
    TdsProcessIdentity,
)
from dpone.services.mssql_tds_supervisor import run_tds_worker


class Store:
    def __init__(self):
        self.records = {}
        self.lease = WindowLease("target", "owner", 1)
        self.fail_phase = None

    def assert_lease(self, lease):
        if lease != self.lease:
            raise WindowContractError("lease lost")

    def load(self, key):
        return self.records.get(key)

    def save(self, key, expected, payload, lease):
        self.assert_lease(lease)
        existing = self.load(key)
        if (None if existing is None else existing.revision) != expected:
            raise WindowContractError("CAS failed")
        if self.fail_phase and '"phase":"' + self.fail_phase + '"' in payload:
            raise OSError("private storage failure")
        record = WindowRecord((expected or 0) + 1, payload)
        self.records[key] = record
        return record


class Worker:
    identity = TdsProcessIdentity("a" * 64, str(uuid4()), 123, 10)

    def __init__(self, writer, receipt):
        self.writer = writer
        self.calls = []
        self.result = TdsWorkerResult(attempt_identity_digest(writer.snapshot.state.identity), receipt, None)
        self.exit = TdsChildExit(self.identity, 0, True)
        self.fail = None

    def startup(self, *, deadline):
        self.calls.append(("startup", deadline))

    def send(self, body, *, deadline):
        assert self.writer.snapshot.state.phase == TdsAttemptPhase.RUNNING
        self.calls.append(("send", deadline))
        if self.fail == "send":
            raise ValueError("private SDK error")

    def receive(self, **kwargs):
        self.calls.append(("receive", kwargs["deadline"]))
        return self.result

    def wait(self, *, deadline):
        self.calls.append(("wait", deadline))
        return self.exit

    def terminate(self, *, deadline):
        self.calls.append(("terminate", deadline))
        if self.fail == "terminate":
            raise TimeoutError("private process state")
        return TdsChildExit(self.identity, -9, True)

    def close(self):
        self.calls.append(("close", None))


class Launcher:
    def __init__(self, worker):
        self.worker = worker
        self.calls = []

    def spawn(self, **kwargs):
        self.calls.append(kwargs)
        return self.worker


def environment():
    identity = TdsAttemptIdentity(
        "target", "run", 0, 0, "1" * 64, "2" * 64, "3" * 64, "4" * 64, "db", "test", "owned", "5" * 64
    )
    store = Store()
    writer = TdsAttemptJournal(store).create(identity, store.lease, supervisor_token=str(uuid4()))
    writer.advance(Prepared(TdsObjectIdentity(123, "6" * 64), "7" * 64), expected_phase=TdsAttemptPhase.CREATION_INTENT)
    writer.advance(LaunchIntent("8" * 64), expected_phase=TdsAttemptPhase.PREPARED)
    receipt = TdsInputReceipt(2, 16, "4" * 64)
    worker = Worker(writer, receipt)
    return store, writer, receipt, worker, Launcher(worker)


class Gateway:
    """Deterministic bounded-port seam for pure ordering unit tests."""

    def __init__(self, writer):
        self.writer = writer

    @property
    def snapshot(self):
        return self.writer.snapshot

    def assert_authority(self, *, deadline):
        self.writer.assert_authority()

    def advance(self, event, *, expected_phase, deadline):
        return self.writer.advance(event, expected_phase=expected_phase)


def run(writer, receipt, launcher, request=lambda: b"private"):
    return run_tds_worker(
        Gateway(writer),
        launcher,
        receipt,
        request,
        operation_deadline=100,
        startup_timeout=10,
        termination_timeout=5,
        clock=lambda: 1,
    )


def test_success_requires_durable_running_before_supplier_and_exited_before_return():
    _, writer, receipt, worker, launcher = environment()

    def request():
        assert writer.snapshot.state.phase == TdsAttemptPhase.RUNNING
        return b"private"

    assert run(writer, receipt, launcher, request) == worker.result
    assert writer.snapshot.state.phase == TdsAttemptPhase.EXITED
    assert [c[0] for c in worker.calls] == ["startup", "send", "receive", "wait", "close"]
    assert launcher.calls == [{"startup_deadline": 11, "operation_deadline": 100}]


@pytest.mark.parametrize("phase", ["spawned_waiting", "running", "exited"])
def test_unknown_cas_always_contains_and_withholds_credentials(phase):
    store, writer, receipt, worker, launcher = environment()
    store.fail_phase = phase
    requested = []
    with pytest.raises(WindowOutcomeUnknown):
        run(writer, receipt, launcher, lambda: requested.append(True) or b"private")
    assert requested == ([True] if phase == "exited" else [])
    if phase != "exited":
        assert any(c[0] == "terminate" for c in worker.calls)
    assert worker.calls[-1][0] == "close"


@pytest.mark.parametrize("stage", ["startup", "send", "receive", "wait"])
def test_failures_contain_and_never_mark_verified(stage):
    _, writer, receipt, worker, launcher = environment()

    def fail(*args, **kwargs):
        worker.calls.append((stage, None))
        raise ValueError("private-data")

    setattr(worker, stage, fail)
    with pytest.raises(RuntimeError, match="mssql_native.tds_worker_failed"):
        run(writer, receipt, launcher)
    assert writer.snapshot.state.phase == TdsAttemptPhase.CONTAINED
    assert any(c[0] == "terminate" for c in worker.calls)
    assert worker.calls[-1][0] == "close"
    if stage == "startup":
        assert not any(c[0] == "send" for c in worker.calls)


@pytest.mark.parametrize(
    "kind", ["wrong_attempt", "wrong_receipt", "not_result", "worker_error", "nonzero", "unreaped", "wrong_process"]
)
def test_no_false_success(kind):
    from dataclasses import replace

    from dpone.contracts.mssql_tds_worker import TdsAttemptError

    _, writer, receipt, worker, launcher = environment()
    if kind == "wrong_attempt":
        worker.result = replace(worker.result, attempt_sha256="f" * 64)
    elif kind == "wrong_receipt":
        worker.result = replace(worker.result, receipt=replace(receipt, rows=3))
    elif kind == "not_result":
        worker.result = object()
    elif kind == "worker_error":
        worker.result = replace(worker.result, receipt=None, error=TdsAttemptError.DECODER)
    elif kind == "nonzero":
        worker.exit = replace(worker.exit, exit_code=1)
    elif kind == "unreaped":
        worker.exit = replace(worker.exit, reaped=False)
    else:
        worker.exit = replace(worker.exit, identity=replace(worker.identity, start_ticks=11))
    with pytest.raises(RuntimeError, match="tds_worker_failed"):
        run(writer, receipt, launcher)
    assert writer.snapshot.state.phase == TdsAttemptPhase.CONTAINED


def test_uncertain_containment_preserves_live_handle_and_never_closes():
    _, writer, receipt, worker, launcher = environment()
    worker.fail = "terminate"

    def fail_request():
        raise ValueError("private")

    with pytest.raises(WindowOutcomeUnknown) as caught:
        run(writer, receipt, launcher, fail_request)
    assert caught.value.worker is worker
    assert "private" not in str(caught.value)
    assert not any(c[0] == "close" for c in worker.calls)
    assert writer.snapshot.state.phase == TdsAttemptPhase.CONTAINMENT_REQUIRED


@pytest.mark.parametrize("phase", ["containment_required", "contained"])
def test_cleanup_cas_failure_does_not_skip_process_settlement(phase):
    store, writer, receipt, worker, launcher = environment()
    store.fail_phase = phase
    worker.fail = "send"
    with pytest.raises(WindowOutcomeUnknown):
        run(writer, receipt, launcher)
    assert any(c[0] == "terminate" for c in worker.calls)
    assert worker.calls[-1][0] == "close"


def test_keyboardinterrupt_preserved_only_after_confirmed_containment():
    _, writer, receipt, worker, launcher = environment()

    def interrupt():
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run(writer, receipt, launcher, interrupt)
    assert writer.snapshot.state.phase == TdsAttemptPhase.CONTAINED
    assert worker.calls[-1][0] == "close"


def test_keyboardinterrupt_with_unconfirmed_containment_becomes_unknown():
    _, writer, receipt, worker, launcher = environment()
    worker.fail = "terminate"

    def interrupt():
        raise KeyboardInterrupt

    with pytest.raises(WindowOutcomeUnknown):
        run(writer, receipt, launcher, interrupt)
    assert not any(c[0] == "close" for c in worker.calls)


def test_lost_fence_during_request_prevents_send():
    store, writer, receipt, worker, launcher = environment()

    def request():
        store.lease = WindowLease("target", "successor", 2)
        return b"private"

    with pytest.raises(WindowOutcomeUnknown):
        run(writer, receipt, launcher, request)
    assert not any(c[0] == "send" for c in worker.calls)
    assert any(c[0] == "terminate" for c in worker.calls)


def test_operation_deadline_shared_and_termination_has_one_fresh_budget():
    _, writer, receipt, worker, launcher = environment()
    now = [1.0]

    def request():
        now[0] = 100.0
        return b"private"

    with pytest.raises(RuntimeError, match="operation_timeout"):
        run_tds_worker(
            Gateway(writer),
            launcher,
            receipt,
            request,
            operation_deadline=100,
            startup_timeout=10,
            termination_timeout=5,
            clock=lambda: now[0],
        )
    assert ("terminate", 105.0) in worker.calls
    assert not any(c[0] == "send" for c in worker.calls)


def test_late_startup_is_contained_without_request():
    _, writer, receipt, worker, launcher = environment()
    now = [1.0]
    requested = []

    def startup(**kwargs):
        now[0] = 12

    worker.startup = startup
    with pytest.raises(RuntimeError, match="startup_timeout"):
        run_tds_worker(
            Gateway(writer),
            launcher,
            receipt,
            lambda: requested.append(True),
            operation_deadline=100,
            startup_timeout=10,
            termination_timeout=5,
            clock=lambda: now[0],
        )
    assert not requested
    assert ("terminate", 17) in worker.calls


def test_spawn_failure_with_typed_launch_attempts_containment():
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    _, writer, receipt, worker, launcher = environment()

    class Launch:
        def contain(self, *, deadline):
            worker.terminate(deadline=deadline)

        def close(self):
            worker.close()

    launch = Launch()

    def fail(**kwargs):
        raise TdsLaunchUnknown(launch)

    launcher.spawn = fail
    with pytest.raises(WindowOutcomeUnknown) as caught:
        run(writer, receipt, launcher)
    assert caught.value.unresolved_launch is None
    assert any(c[0] == "terminate" for c in worker.calls)
    assert worker.calls[-1][0] == "close"


def test_unknown_spawn_without_handle_cannot_manufacture_containment():
    _, writer, receipt, worker, launcher = environment()

    def fail(**kwargs):
        raise RuntimeError("unknown spawn")

    launcher.spawn = fail
    with pytest.raises(WindowOutcomeUnknown):
        run(writer, receipt, launcher)
    assert writer.snapshot.state.phase == TdsAttemptPhase.LAUNCH_INTENT
    assert worker.calls == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation_deadline", True),
        ("startup_timeout", 0),
        ("termination_timeout", float("inf")),
        ("operation_deadline", float("nan")),
    ],
)
def test_invalid_bounds_prevent_all_effects(field, value):
    _, writer, receipt, worker, launcher = environment()
    kwargs = dict(operation_deadline=100, startup_timeout=10, termination_timeout=5, clock=lambda: 1)
    kwargs[field] = value
    with pytest.raises(ValueError):
        run_tds_worker(Gateway(writer), launcher, receipt, lambda: b"private", **kwargs)
    assert launcher.calls == [] and worker.calls == []


def test_success_cannot_be_replayed_without_new_attempt():
    _, writer, receipt, worker, launcher = environment()
    run(writer, receipt, launcher)
    previous = list(worker.calls)
    with pytest.raises(ValueError, match="launch_intent_required"):
        run(writer, receipt, launcher)
    assert worker.calls == previous


@pytest.mark.parametrize("phase", ["spawned_waiting", "running", "exited"])
def test_committed_cas_lost_ack_never_reloads_and_replays(phase):
    store, writer, receipt, worker, launcher = environment()
    original = store.save

    def lost_ack(key, expected, payload, lease):
        result = original(key, expected, payload, lease)
        if '"phase":"' + phase + '"' in payload:
            raise OSError("lost commit reply")
        return result

    store.save = lost_ack
    requested = []
    with pytest.raises(WindowOutcomeUnknown):
        run(writer, receipt, launcher, lambda: requested.append(True) or b"private")
    persisted = TdsAttemptJournal(store).read(writer.snapshot.state.identity)
    assert persisted.state.phase.value == phase
    assert requested == ([True] if phase == "exited" else [])
    assert worker.calls[-1][0] == "close"


def test_process_termination_precedes_delayed_failed_journal_cleanup():
    store, writer, receipt, worker, launcher = environment()
    now = [1.0]
    original = store.save

    def delayed(key, expected, payload, lease):
        if '"phase":"containment_required"' in payload:
            assert ("terminate", 6.0) in worker.calls
            now[0] = 7.0
            raise OSError("late failed save")
        return original(key, expected, payload, lease)

    store.save = delayed
    worker.fail = "send"
    with pytest.raises(WindowOutcomeUnknown) as caught:
        run_tds_worker(
            Gateway(writer),
            launcher,
            receipt,
            lambda: b"private",
            operation_deadline=100,
            startup_timeout=10,
            termination_timeout=5,
            clock=lambda: now[0],
        )
    assert caught.value.worker is None
    assert ("terminate", 6.0) in worker.calls
    assert worker.calls[-1][0] == "close"


def test_late_termination_observation_stays_unknown():
    _, writer, receipt, worker, launcher = environment()
    now = [1.0]

    def late(**kwargs):
        now[0] = 7
        return TdsChildExit(worker.identity, -9, True)

    worker.terminate = late
    worker.fail = "send"
    with pytest.raises(WindowOutcomeUnknown) as caught:
        run_tds_worker(
            Gateway(writer),
            launcher,
            receipt,
            lambda: b"private",
            operation_deadline=100,
            startup_timeout=10,
            termination_timeout=5,
            clock=lambda: now[0],
        )
    assert caught.value.worker is worker
    assert not any(c[0] == "close" for c in worker.calls)


def test_failed_descriptor_close_retains_settled_handle():
    _, writer, receipt, worker, launcher = environment()

    def close():
        raise OSError("private fd info")

    worker.close = close
    with pytest.raises(WindowOutcomeUnknown) as caught:
        run(writer, receipt, launcher)
    assert caught.value.worker is worker
    assert str(caught.value) == "mssql_native.tds_supervision_unknown"
    assert writer.snapshot.state.phase == TdsAttemptPhase.EXITED


def test_result_and_wait_use_same_operation_deadline():
    _, writer, receipt, worker, launcher = environment()
    run(writer, receipt, launcher)
    assert [v for k, v in worker.calls if k in {"send", "receive", "wait"}] == [100, 100, 100]


@pytest.mark.parametrize("body", [None, b"", bytearray(b"private"), b"x" * ((1 << 20) + 1)])
def test_invalid_request_not_sent_and_contained(body):
    _, writer, receipt, worker, launcher = environment()
    with pytest.raises(RuntimeError, match="tds_worker_failed:protocol"):
        run(writer, receipt, launcher, lambda: body)
    assert not any(c[0] == "send" for c in worker.calls)
    assert writer.snapshot.state.phase == TdsAttemptPhase.CONTAINED


def test_changed_handle_identity_cannot_match_a_different_exit_proof():
    from dataclasses import replace

    _, writer, receipt, worker, launcher = environment()
    initial = worker.identity
    original_send = worker.send

    def changed(body, **kwargs):
        original_send(body, **kwargs)
        worker.identity = replace(initial, start_ticks=11)
        worker.exit = replace(worker.exit, identity=worker.identity)

    worker.send = changed
    with pytest.raises(WindowOutcomeUnknown) as caught:
        run(writer, receipt, launcher)
    assert caught.value.worker is worker
    assert writer.snapshot.state.process == initial
    assert writer.snapshot.state.phase == TdsAttemptPhase.CONTAINMENT_REQUIRED
    assert not any(c[0] == "close" for c in worker.calls)


@pytest.mark.parametrize("blocked_stage", ["running", "after_send", "exited"])
def test_real_blocked_actor_cannot_hold_process_past_budget(blocked_stage):
    import threading
    import time
    from contextlib import contextmanager

    from dpone.adapters.mssql_tds_actor_core import TdsActorPool

    entered = threading.Event()
    release = threading.Event()
    box = []

    @contextmanager
    def factory():
        store, writer, receipt, _, _ = environment()
        original_save = store.save
        original_load = store.load
        store.block_read = False

        def save(key, expected, payload, lease):
            if blocked_stage in {"running", "exited"} and '"phase":"' + blocked_stage + '"' in payload:
                entered.set()
                assert release.wait(5)
            return original_save(key, expected, payload, lease)

        def load(key):
            if store.block_read:
                entered.set()
                assert release.wait(5)
            return original_load(key)

        store.save = save
        store.load = load
        box.append((store, writer, receipt))
        yield writer

    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(factory, actor_deadline, actor_clock),
        deadline=time.monotonic() + 1,
    )
    store, writer, receipt = box[0]
    worker = Worker(gateway, receipt)
    launcher = Launcher(worker)
    requested = []
    if blocked_stage == "after_send":
        original_send = worker.send

        def send(body, **kwargs):
            original_send(body, **kwargs)
            store.block_read = True

        worker.send = send
    old = gateway.snapshot
    start = time.monotonic()
    try:
        with pytest.raises(WindowOutcomeUnknown):
            run_tds_worker(
                gateway,
                launcher,
                receipt,
                lambda: requested.append(True) or b"private",
                operation_deadline=start + 0.15,
                startup_timeout=0.1,
                termination_timeout=0.15,
            )
        elapsed = time.monotonic() - start
        assert entered.is_set() and elapsed < 0.6
        assert requested == ([] if blocked_stage == "running" else [True])
        assert worker.calls[-1][0] == "close"
        if blocked_stage != "exited":
            assert any(c[0] == "terminate" for c in worker.calls)
        else:
            assert any(c[0] == "wait" for c in worker.calls)
        assert pool.live_count == 1
        assert gateway.snapshot.state.phase != TdsAttemptPhase.EXITED
    finally:
        release.set()
        gateway.close(deadline=time.monotonic() + 1)
    if blocked_stage in {"running", "exited"}:
        assert writer.snapshot.state.phase.value == blocked_stage
    requested_before = list(requested)
    calls_before = list(worker.calls)
    with pytest.raises((WindowOutcomeUnknown, ValueError)):
        run_tds_worker(
            gateway,
            launcher,
            receipt,
            lambda: requested.append(True) or b"private",
            operation_deadline=time.monotonic() + 1,
            startup_timeout=0.1,
            termination_timeout=0.15,
        )
    assert requested == requested_before and worker.calls == calls_before
    assert old.state.phase == TdsAttemptPhase.LAUNCH_INTENT


def test_unknown_launch_without_identity_retains_exact_capability():
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    _, writer, receipt, worker, launcher = environment()

    class Launch:
        def contain(self, *, deadline):
            assert deadline == 6
            raise TimeoutError("unresolved private launch")

        def close(self):
            pytest.fail("cannot close uncontained launch")

    launch = Launch()

    def fail(**kwargs):
        raise TdsLaunchUnknown(launch)

    launcher.spawn = fail
    with pytest.raises(WindowOutcomeUnknown) as caught:
        run(writer, receipt, launcher)
    assert caught.value.unresolved_launch is launch
    assert caught.value.worker is None
    assert worker.calls == []


def test_arbitrary_exception_worker_attribute_is_not_a_capability():
    _, writer, receipt, worker, launcher = environment()
    error = RuntimeError("unknown")
    error.worker = worker

    def fail(**kwargs):
        raise error

    launcher.spawn = fail
    with pytest.raises(WindowOutcomeUnknown) as caught:
        run(writer, receipt, launcher)
    assert caught.value.worker is None and worker.calls == []
