"""Actual thread/deadline/capacity checks; no SQL or process certification."""

import threading
from contextlib import contextmanager
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_evidence_actor import SqlClientEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceObservation
from tests.mssql_sqlclient_evidence_fixtures import evidence_record, registration

H = registration().binding.attempt_sha256


def record(kind=Kind.CREDENTIAL_INTENT):
    return evidence_record(kind)


def opened(pool, factory, deadline=None):
    return pool.open(
        lambda d, clock: SqlClientEvidenceActor(factory, H, d, clock),
        deadline=monotonic() + 2 if deadline is None else deadline,
    )


@contextmanager
def sink():
    yield SimpleNamespace(write=lambda name, payload: None)


def test_real_filesystem_writer_construction_write_and_teardown_on_actor(tmp_path):
    events = []
    supervisor = threading.get_ident()

    @contextmanager
    def factory():
        events.append(("construct", threading.get_ident()))
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, payload):
            events.append(("write", threading.get_ident()))
            writer.write(name, payload)

        try:
            yield SimpleNamespace(write=write)
        finally:
            events.append(("teardown", threading.get_ident()))

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    item = record()
    receipt = gateway.write(item, deadline=monotonic() + 2)
    assert receipt == item.receipt
    assert gateway.observation == SqlClientEvidenceObservation(H, receipt)
    assert gateway.observation.receipt is receipt
    assert (tmp_path / receipt.relative_name).read_bytes() == item.payload
    gateway.close(deadline=monotonic() + 2)
    assert [name for name, _ in events] == ["construct", "write", "teardown"]
    assert len({owner for _, owner in events}) == 1 and events[0][1] != supervisor
    assert pool.live_count == 0


def test_each_kind_is_one_shot_and_only_seven_backend_writes():
    writes = []

    @contextmanager
    def factory():
        yield SimpleNamespace(write=lambda name, payload: writes.append(name))

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    try:
        for kind in Kind:
            gateway.write(record(kind), deadline=monotonic() + 1)
            with pytest.raises(ValueError, match="consumed"):
                gateway.write(record(kind), deadline=monotonic() + 1)
        assert len(writes) == len(set(writes)) == 7
    finally:
        gateway.close(deadline=monotonic() + 1)


def test_wrong_operation_or_forged_shape_cannot_reach_backend():
    writes = []

    @contextmanager
    def factory():
        yield SimpleNamespace(write=lambda *args: writes.append(args))

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    try:
        wrong = record()
        object.__setattr__(wrong, "attempt_sha256", "b" * 64)
        with pytest.raises(ValueError, match="attempt_invalid"):
            gateway.write(wrong, deadline=monotonic() + 1)
        forged = record()
        object.__setattr__(forged, "payload", b"{}")
        with pytest.raises(ValueError, match="evidence_invalid"):
            gateway.write(forged, deadline=monotonic() + 1)
        assert not writes and gateway.observation.receipt is None
    finally:
        gateway.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("phase", ["initialize", "write", "fsync", "teardown"])
def test_stalled_backend_retains_capacity_and_late_ack_is_excluded(tmp_path, monkeypatch, phase):
    from dpone.adapters import filesystem_evidence

    entered, release = threading.Event(), threading.Event()
    persisted = []

    def block():
        entered.set()
        assert release.wait(5)

    if phase == "fsync":
        original = filesystem_evidence._fsync

        def fsync(*args):
            block()
            original(*args)

        monkeypatch.setattr(filesystem_evidence, "_fsync", fsync)

    @contextmanager
    def factory():
        if phase == "initialize":
            block()
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, payload):
            if phase == "write":
                block()
            writer.write(name, payload)
            persisted.append(name)

        try:
            yield SimpleNamespace(write=write)
        finally:
            if phase == "teardown":
                block()

    pool = TdsActorPool(capacity=1)
    gateway = None
    try:
        if phase == "initialize":
            with pytest.raises(TdsJournalActorUnknown) as caught:
                opened(pool, factory, monotonic() + 0.05)
            gateway = caught.value.gateway
        else:
            gateway = opened(pool, factory)
            with pytest.raises(TdsJournalActorUnknown):
                if phase == "teardown":
                    gateway.close(deadline=monotonic() + 0.05)
                else:
                    gateway.write(record(), deadline=monotonic() + 0.05)
        assert entered.is_set() and pool.live_count == 1
        with pytest.raises(TdsJournalActorUnknown):
            opened(pool, sink)
        if phase != "initialize":
            assert gateway.observation.receipt is None
            with pytest.raises((TdsJournalActorUnknown, ValueError)):
                gateway.write(record(Kind.REGISTRATION), deadline=monotonic() + 1)
    finally:
        release.set()
        if gateway is not None:
            gateway.close(deadline=monotonic() + 2)
    assert pool.live_count == 0
    if phase in {"write", "fsync"}:
        assert persisted and gateway.observation.receipt is None
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(record(Kind.RESULT), deadline=monotonic() + 1)


def test_backend_exception_never_acknowledges_or_allows_next_kind():
    def fail(*args):
        raise OSError("private backend diagnostics")

    @contextmanager
    def factory():
        yield SimpleNamespace(write=fail)

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    try:
        with pytest.raises(TdsJournalActorUnknown) as caught:
            gateway.write(record(), deadline=monotonic() + 1)
        assert "private" not in str(caught.value)
        assert caught.value.gateway is gateway and gateway.observation.receipt is None
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(record(Kind.REGISTRATION), deadline=monotonic() + 1)
    finally:
        gateway.close(deadline=monotonic() + 1)


def test_foreign_thread_cannot_submit_or_close():
    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, sink)
    failures = []

    def foreign():
        for action in (
            lambda: gateway.write(record(), deadline=monotonic() + 1),
            lambda: gateway.close(deadline=monotonic() + 1),
        ):
            try:
                action()
            except TdsJournalActorUnknown:
                failures.append(True)

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join()
    assert failures == [True, True] and gateway.observation.receipt is None
    gateway.close(deadline=monotonic() + 1)


def test_existing_identical_artifact_verified_by_fresh_actor(tmp_path):
    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1)
    for _ in range(2):
        gateway = opened(pool, factory)
        assert gateway.write(record(), deadline=monotonic() + 1) == record().receipt
        gateway.close(deadline=monotonic() + 1)
    assert len(list(tmp_path.iterdir())) == 1


def test_corrupt_ack_is_rejected_without_exposing_new_observation(monkeypatch):
    from dataclasses import replace

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, sink)
    previous = gateway.observation
    wrong = replace(record().receipt, byte_count=1)
    monkeypatch.setattr(gateway, "_dispatch", lambda *args: SqlClientEvidenceObservation(H, wrong))
    try:
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(record(), deadline=monotonic() + 1)
        assert gateway.observation == previous
    finally:
        gateway.close(deadline=monotonic() + 1)


def test_concurrent_admission_never_exceeds_shared_capacity():
    start = threading.Barrier(5)
    release = threading.Event()
    factory_entries = []
    failures = []
    lock = threading.Lock()
    pool = TdsActorPool(capacity=2)

    @contextmanager
    def blocked():
        with lock:
            factory_entries.append(threading.get_ident())
        assert release.wait(5)
        yield SimpleNamespace(write=lambda *args: None)

    def contender():
        gateway = None
        start.wait()
        try:
            opened(pool, blocked, monotonic() + 0.05)
        except TdsJournalActorUnknown as error:
            with lock:
                failures.append(error)
            gateway = error.gateway
        finally:
            assert release.wait(5)
            if gateway is not None:
                gateway.close(deadline=monotonic() + 2)

    threads = [threading.Thread(target=contender) for _ in range(4)]
    for thread in threads:
        thread.start()
    start.wait()
    try:
        # Bounded main-thread wait observes four attempts, never actor internals.
        deadline = monotonic() + 2
        while monotonic() < deadline:
            with lock:
                if len(failures) == 4:
                    break
            threading.Event().wait(0.005)
        assert len(failures) == 4 and len(factory_entries) == pool.live_count == 2
        assert sum(error.gateway is not None for error in failures) == 2
    finally:
        release.set()
        for thread in threads:
            thread.join(timeout=2)
    assert not any(thread.is_alive() for thread in threads) and pool.live_count == 0


def test_failed_context_exit_retains_teardown_uncertainty():
    @contextmanager
    def factory():
        try:
            yield SimpleNamespace(write=lambda *args: None)
        finally:
            raise OSError("private close details")

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    gateway.write(record(), deadline=monotonic() + 1)
    with pytest.raises(TdsJournalActorUnknown) as caught:
        gateway.close(deadline=monotonic() + 1)
    assert caught.value.gateway is gateway and "private" not in str(caught.value)
    assert gateway.observation.receipt == record().receipt
    assert pool.live_count == 0  # Actual termination frees capacity; teardown is still unknown.


def test_conflicting_existing_evidence_never_receives_ack(tmp_path):
    item = record()
    (tmp_path / item.receipt.relative_name).write_bytes(b"different bytes")

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    try:
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(item, deadline=monotonic() + 1)
        assert gateway.observation.receipt is None
        assert (tmp_path / item.receipt.relative_name).read_bytes() == b"different bytes"
    finally:
        gateway.close(deadline=monotonic() + 1)


def test_actor_boundary_revalidates_context_before_backend(monkeypatch):
    writes = []

    @contextmanager
    def factory():
        yield SimpleNamespace(write=lambda *args: writes.append(args))

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    item = record(Kind.RESULT)
    original = gateway._call

    def mutate_then_enqueue(command):
        object.__setattr__(command.event.result_context.expected_input, "rows", True)
        return original(command)

    monkeypatch.setattr(gateway, "_call", mutate_then_enqueue)
    try:
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(item, deadline=monotonic() + 1)
        assert not writes and gateway.observation.receipt is None
    finally:
        gateway.close(deadline=monotonic() + 1)


@pytest.mark.parametrize(
    "field", ["byte_count", "kind", "relative_name", "payload_sha256", "receipt", "attempt_sha256"]
)
def test_equal_value_ack_alias_rejects_and_restores_previous(monkeypatch, field):
    class TextAlias(str):
        pass

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, sink)
    gateway.write(record(Kind.REGISTRATION), deadline=monotonic() + 1)
    previous = gateway.observation
    item = record()
    receipt = item.receipt
    observed = SqlClientEvidenceObservation(H, receipt)
    if field == "receipt":
        from dataclasses import asdict

        object.__setattr__(observed, "receipt", SimpleNamespace(**asdict(receipt)))
    elif field == "attempt_sha256":
        object.__setattr__(observed, field, TextAlias(H))
    else:
        value = getattr(receipt, field)
        alias = float(value) if field == "byte_count" else str(value) if field == "kind" else TextAlias(value)
        object.__setattr__(receipt, field, alias)
    monkeypatch.setattr(gateway, "_dispatch", lambda *args: observed)
    try:
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(item, deadline=monotonic() + 1)
        assert gateway.observation is previous
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(record(Kind.LOCAL_EXIT), deadline=monotonic() + 1)
    finally:
        gateway.close(deadline=monotonic() + 1)
