"""Actual local containment persistence; no original process or SQL certification."""

import threading
from contextlib import contextmanager
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_observe_containment_evidence_actor import SqlClientObserveContainmentEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.contracts.mssql_sqlclient_observe_departure_codec import encode_observe_containment
from tests.test_mssql_sqlclient_observe_departure_codec import containment


def opened(pool, factory, value=None):
    c = containment() if value is None else value
    return pool.open(
        lambda d, clock: SqlClientObserveContainmentEvidenceActor(
            factory, c.observe_operation, c.exit.identity, d, clock
        ),
        deadline=monotonic() + 2,
    )


def payload(value=None):
    c = containment() if value is None else value
    return encode_observe_containment(c, process=c.exit.identity)


@contextmanager
def sink():
    yield SimpleNamespace(write=lambda *args: None)


def test_actual_nonzero_reaped_containment_file_and_thread(tmp_path):
    events = []

    @contextmanager
    def factory():
        events.append(("enter", threading.get_ident()))
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, body):
            events.append(("write", threading.get_ident()))
            writer.write(name, body)

        yield SimpleNamespace(write=write)
        events.append(("exit", threading.get_ident()))

    pool = TdsActorPool(capacity=1)
    actor = opened(pool, factory)
    assert actor.observation.receipt is None
    raw = payload()
    receipt = actor.write(raw, deadline=monotonic() + 2)
    assert (tmp_path / receipt.relative_name).read_bytes() == raw
    assert actor.observation.receipt == receipt
    with pytest.raises(ValueError, match="consumed"):
        actor.write(raw, deadline=monotonic() + 2)
    actor.close(deadline=monotonic() + 2)
    assert [event for event, _ in events] == ["enter", "write", "exit"]
    assert len({thread for _, thread in events}) == 1 and events[0][1] != threading.get_ident()
    assert pool.live_count == 0


@pytest.mark.parametrize("mutation", ["operation", "process", "unreaped", "bool", "whitespace", "cap", "type"])
def test_invalid_payload_never_writes_or_consumes(mutation):
    import json

    from dpone.contracts.strict_json import canonical_json_bytes

    raw = payload()
    body = json.loads(raw)
    if mutation == "operation":
        body["observe_operation"]["slot_index"] += 1
    elif mutation == "process":
        body["exit"]["identity"]["pid"] += 1
    elif mutation == "unreaped":
        body["exit"]["reaped"] = False
    elif mutation == "bool":
        body["exit"]["exit_code"] = True
    raw = canonical_json_bytes(body)
    if mutation == "whitespace":
        raw += b" "
    elif mutation == "cap":
        raw = b"x" * 16385
    elif mutation == "type":
        raw = bytearray(raw)
    calls = []

    @contextmanager
    def factory():
        yield SimpleNamespace(write=lambda *args: calls.append(args))

    actor = opened(TdsActorPool(capacity=1), factory)
    try:
        with pytest.raises(ValueError):
            actor.write(raw, deadline=monotonic() + 2)
        assert not calls and not actor._attempted
        assert actor.observation.receipt is None
    finally:
        actor.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("field", ["operation", "process"])
@pytest.mark.parametrize("boundary", ["constructor", "caller", "dispatch"])
def test_independent_original_nested_aliases_and_snapshots(field, boundary, monkeypatch):
    c = containment()
    operation, process = c.observe_operation, c.exit.identity
    target, name = (operation.parent, "ordinal") if field == "operation" else (process, "pid")

    def mutate():
        object.__setattr__(target, name, float(getattr(target, name)))

    if boundary == "constructor":
        mutate()
        with pytest.raises(ValueError):
            SqlClientObserveContainmentEvidenceActor(sink, operation, process, 1.0, monotonic)
        return
    actor = opened(TdsActorPool(capacity=1), sink, c)
    raw = payload(c)
    if boundary == "caller":
        # A different, still valid scalar also cannot rewrite held identity.
        object.__setattr__(target, name, getattr(target, name) + 1)
    else:
        original = actor._dispatch

        def dispatch(*args):
            mutate()
            return original(*args)

        monkeypatch.setattr(actor, "_dispatch", dispatch)
    try:
        with pytest.raises(ValueError if boundary == "caller" else TdsJournalActorUnknown):
            actor.write(raw, deadline=monotonic() + 2)
        assert not actor._persisted and actor.observation.receipt is None
    finally:
        actor.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("mutation", ["count", "bool", "subject", "name", "receipt", "snapshot", "type"])
def test_wrong_ack_never_becomes_receipt(mutation, monkeypatch):
    from dpone.contracts.mssql_sqlclient_observe_departure import SqlClientObserveContainmentObservation
    from dpone.contracts.mssql_sqlclient_observe_departure_codec import observe_containment_receipt

    actor = opened(TdsActorPool(capacity=1), sink)
    raw = payload()
    receipt = observe_containment_receipt(raw, process=containment().exit.identity)
    observed = SqlClientObserveContainmentObservation(receipt.operation_sha256, receipt)
    previous = actor.observation
    if mutation in ("count", "bool"):
        object.__setattr__(receipt, "byte_count", float(receipt.byte_count) if mutation == "count" else True)
    elif mutation == "subject":
        object.__setattr__(observed, "operation_sha256", "9" * 64)
    elif mutation == "name":
        object.__setattr__(receipt, "relative_name", "other.json")
    elif mutation == "receipt":
        object.__setattr__(observed, "receipt", {})
    elif mutation == "type":
        observed = SimpleNamespace()
    if mutation == "snapshot":
        monkeypatch.setattr(actor, "_call", lambda *args: observed)
    else:
        monkeypatch.setattr(actor, "_dispatch", lambda *args: observed)
    with pytest.raises(TdsJournalActorUnknown) as caught:
        actor.write(raw, deadline=monotonic() + 2)
    assert caught.value.gateway is actor and actor.observation is previous
    with pytest.raises(ValueError, match="consumed"):
        actor.write(raw, deadline=monotonic() + 2)
    actor.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("mode", ["late", "partial", "close"])
def test_uncertain_write_close_and_late_file_never_replay(mode, tmp_path):
    release, entered = threading.Event(), threading.Event()
    writes = []

    @contextmanager
    def factory():
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, raw):
            writes.append(name)
            entered.set()
            if mode == "late":
                assert release.wait(2)
            writer.write(name, raw)
            if mode == "partial":
                raise OSError("synthetic post-write error")

        yield SimpleNamespace(write=write)
        if mode == "close":
            raise OSError("synthetic close error")

    pool = TdsActorPool(capacity=1)
    actor = opened(pool, factory)
    previous = actor.observation
    if mode == "close":
        receipt = actor.write(payload(), deadline=monotonic() + 2)
        previous = actor.observation
        assert (tmp_path / receipt.relative_name).read_bytes() == payload()
        with pytest.raises(TdsJournalActorUnknown):
            actor.close(deadline=monotonic() + 2)
    else:
        try:
            with pytest.raises(TdsJournalActorUnknown) as caught:
                actor.write(payload(), deadline=monotonic() + (0.05 if mode == "late" else 2))
            assert caught.value.gateway is actor
            if mode == "late":
                assert entered.is_set() and pool.live_count == 1
                with pytest.raises(TdsJournalActorUnknown):
                    opened(pool, sink)
                with pytest.raises(TdsJournalActorUnknown):
                    actor.close(deadline=monotonic() + 0.01)
        finally:
            release.set()
            actor.close(deadline=monotonic() + 2)
        assert (tmp_path / writes[0]).read_bytes() == payload()
    assert actor._persisted and actor.observation is previous
    with pytest.raises(ValueError, match="consumed"):
        actor.write(payload(), deadline=monotonic() + 2)
    assert len(writes) == 1 and pool.live_count == 0


@pytest.mark.parametrize("mode", ["reentry", "expired", "lost"])
def test_reentry_expiry_and_lost_ack_keep_empty_snapshot(mode, monkeypatch, tmp_path):
    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    actor = opened(TdsActorPool(capacity=1), factory)
    original = actor._call

    def call(command):
        if mode == "reentry":
            actor.write(payload(), deadline=monotonic() + 2)
        result = original(command)
        if mode == "lost":
            raise TdsJournalActorUnknown(actor)
        return result

    monkeypatch.setattr(actor, "_call", call)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(payload(), deadline=monotonic() + (-1 if mode == "expired" else 2))
    assert actor.observation.receipt is None and actor._attempted
    actor.close(deadline=monotonic() + 2)
    assert len(list(tmp_path.glob("*.json"))) == (1 if mode == "lost" else 0)


@pytest.mark.parametrize("mode", ["ready", "close"])
def test_blocked_entry_or_teardown_retains_original_pool(mode):
    release = threading.Event()

    @contextmanager
    def factory():
        if mode == "ready":
            assert release.wait(2)
        yield SimpleNamespace(write=lambda *args: None)
        if mode == "close":
            assert release.wait(2)

    pool = TdsActorPool(capacity=1)
    c = containment()
    actor = None
    try:
        if mode == "ready":
            with pytest.raises(TdsJournalActorUnknown) as caught:
                pool.open(
                    lambda d, clock: SqlClientObserveContainmentEvidenceActor(
                        factory, c.observe_operation, c.exit.identity, d, clock
                    ),
                    deadline=monotonic() + 0.05,
                )
            actor = caught.value.gateway
        else:
            actor = opened(pool, factory)
            with pytest.raises(TdsJournalActorUnknown):
                actor.close(deadline=monotonic() + 0.05)
        assert actor is not None and pool.live_count == 1
        with pytest.raises(TdsJournalActorUnknown):
            opened(pool, sink)
    finally:
        release.set()
        if actor is not None:
            actor.close(deadline=monotonic() + 2)
    assert pool.live_count == 0
