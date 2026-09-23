"""Actor ordering and actual local create-only persistence checks."""

import threading
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_permission_grant_evidence_actor import (
    SqlClientPermissionGrantEvidenceActor,
)
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    PermissionGrantParentEvidenceContext,
    PermissionGrantParentEvidenceObservation,
    PermissionGrantParentEvidenceRecord,
    decode_permission_grant_held_ready_evidence,
    encode_permission_grant_held_ready_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    PermissionGrantParentEvidenceKind as K,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionBoundary,
    PermissionWireKind,
    encode_permission_message,
)
from dpone.contracts.mssql_tds_coordinator_authority import encode_authority
from dpone.contracts.strict_json import strict_json_object
from tests.test_mssql_sqlclient_permission_grant_parent_evidence import evidence_fixture, records
from tests.test_mssql_sqlclient_permission_grant_wire import fixture


def opened(pool, factory, *, clock=None):
    binding, subject, _ = evidence_fixture()
    return pool.open(
        lambda deadline, actor_clock: SqlClientPermissionGrantEvidenceActor(
            factory, subject, binding, deadline, actor_clock
        ),
        deadline=(clock or monotonic)() + 2,
    )


@contextmanager
def sink(writes=None):
    yield SimpleNamespace(write=lambda name, payload: None if writes is None else writes.append((name, payload)))


def test_actual_descriptor_pinned_writer_persists_all_eight_on_one_actor_thread(tmp_path):
    events = []
    caller = threading.get_ident()

    @contextmanager
    def factory():
        events.append(("enter", threading.get_ident()))
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, payload):
            events.append(("write", threading.get_ident()))
            writer.write(name, payload)

        try:
            yield SimpleNamespace(write=write)
        finally:
            events.append(("exit", threading.get_ident()))

    pool = TdsActorPool(capacity=1)
    actor = opened(pool, factory)
    expected = records()
    for record in expected:
        assert actor.write(record, deadline=monotonic() + 2) == record.receipt
    assert len(tuple(tmp_path.iterdir())) == 8
    assert all((tmp_path / record.receipt.relative_name).read_bytes() == record.payload for record in expected)
    actor.close(deadline=monotonic() + 2)
    assert len({owner for _, owner in events}) == 1 and events[0][1] != caller
    assert [name for name, _ in events] == ["enter", *("write" for _ in range(8)), "exit"]


def test_wrong_order_does_not_consume_expected_kind_or_reach_backend():
    writes = []
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, lambda: sink(writes))
    items = records()
    try:
        with pytest.raises(ValueError):
            actor.write(items[1], deadline=monotonic() + 1)
        assert actor.write(items[0], deadline=monotonic() + 1) == items[0].receipt
        assert len(writes) == 1
    finally:
        actor.close(deadline=monotonic() + 1)


def test_backend_failure_consumes_kind_and_retains_previous_observation():
    writes = 0

    @contextmanager
    def factory():
        def write(name, payload):
            nonlocal writes
            writes += 1
            if writes == 2:
                raise OSError("private backend detail")

        yield SimpleNamespace(write=write)

    pool = TdsActorPool(capacity=1)
    actor = opened(pool, factory)
    items = records()
    first = actor.write(items[0], deadline=monotonic() + 1)
    previous = actor.observation
    assert previous == PermissionGrantParentEvidenceObservation(items[0].subject, first)
    with pytest.raises(TdsJournalActorUnknown) as caught:
        actor.write(items[1], deadline=monotonic() + 1)
    assert "private" not in str(caught.value) and actor.observation is previous
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[1], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)


def test_dispatch_revalidates_mutated_frozen_record_before_io():
    writes = []
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, lambda: sink(writes))
    record = records()[0]
    object.__setattr__(record, "payload", b"{}")
    try:
        with pytest.raises(ValueError):
            actor.write(record, deadline=monotonic() + 1)
        assert writes == []
    finally:
        actor.close(deadline=monotonic() + 1)


def test_actor_context_rejects_binding_drift_at_caller_boundary_before_io():
    writes = []
    binding, subject, payloads = evidence_fixture()
    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda deadline, clock: SqlClientPermissionGrantEvidenceActor(
            lambda: sink(writes), subject, binding, deadline, clock
        ),
        deadline=monotonic() + 2,
    )
    record = PermissionGrantParentEvidenceRecord(subject, K.REQUEST, payloads[K.REQUEST], binding=binding)
    original_deadline = binding.operation_deadline_ns
    object.__setattr__(binding, "operation_deadline_ns", original_deadline + 1)
    try:
        with pytest.raises(ValueError):
            actor.write(record, deadline=monotonic() + 1)
        assert writes == []
    finally:
        object.__setattr__(binding, "operation_deadline_ns", original_deadline)
        actor.close(deadline=monotonic() + 1)


def test_actor_context_rejects_binding_drift_again_at_dispatch_boundary(monkeypatch):
    writes = []
    binding, subject, payloads = evidence_fixture()
    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda deadline, clock: SqlClientPermissionGrantEvidenceActor(
            lambda: sink(writes), subject, binding, deadline, clock
        ),
        deadline=monotonic() + 2,
    )
    record = PermissionGrantParentEvidenceRecord(subject, K.REQUEST, payloads[K.REQUEST], binding=binding)
    original_record = PermissionGrantParentEvidenceContext.record
    original_deadline = binding.operation_deadline_ns
    calls = 0

    def drift_on_dispatch(context, value):
        nonlocal calls
        calls += 1
        if calls == 2:
            object.__setattr__(binding, "operation_deadline_ns", original_deadline + 1)
        return original_record(context, value)

    monkeypatch.setattr(PermissionGrantParentEvidenceContext, "record", drift_on_dispatch)
    try:
        with pytest.raises(TdsJournalActorUnknown):
            actor.write(record, deadline=monotonic() + 1)
        assert calls == 2 and writes == []
    finally:
        object.__setattr__(binding, "operation_deadline_ns", original_deadline)
        actor.close(deadline=monotonic() + 1)


def test_malformed_ack_restores_previous_and_poison_is_sticky(monkeypatch):
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, sink)
    items = records()
    first = actor.write(items[0], deadline=monotonic() + 1)
    previous = actor.observation
    wrong = PermissionGrantParentEvidenceObservation(items[0].subject, first)
    monkeypatch.setattr(actor, "_dispatch", lambda *args: wrong)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[1], deadline=monotonic() + 1)
    assert actor.observation is previous
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[1], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)


def test_registration_claim_must_match_prior_admission_ack():
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, sink)
    items = list(records())
    actor.write(items[0], deadline=monotonic() + 1)
    actor.write(items[1], deadline=monotonic() + 1)
    bad_payload = items[2].payload.replace(items[1].receipt.payload_sha256.encode(), b"f" * 64)
    items[2] = replace(items[2], payload=bad_payload)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[2], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)


def test_dispatch_consumes_kind_before_backend_write():
    actor = None
    observed = []

    @contextmanager
    def factory():
        def write(name, payload):
            observed.append(actor._persisted_index)

        yield SimpleNamespace(write=write)

    pool = TdsActorPool(capacity=1)
    actor = opened(pool, factory)
    actor.write(records()[0], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)
    assert observed == [1]


def test_caught_clock_reentry_with_invalid_kind_poison_is_sticky():
    actor = None
    nested = []
    enabled = False

    def clock():
        if enabled and not nested:
            try:
                actor.write(records()[1], deadline=monotonic() + 1)
            except TdsJournalActorUnknown:
                nested.append(True)
        return monotonic()

    writes = []
    pool = TdsActorPool(capacity=1, clock=clock)
    actor = opened(pool, lambda: sink(writes), clock=clock)
    enabled = True
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(records()[0], deadline=monotonic() + 1)
    assert nested == [True] and writes == [] and actor.observation.receipt is None
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(records()[0], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)


def test_receipt_failure_restores_previous_and_consumes_kind(monkeypatch):
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, sink)
    items = records()
    actor.write(items[0], deadline=monotonic() + 1)
    previous = actor.observation

    def fail_receipt(self):
        raise ValueError("private receipt detail")

    monkeypatch.setattr(PermissionGrantParentEvidenceRecord, "receipt", property(fail_receipt))
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[1], deadline=monotonic() + 1)
    assert actor.observation is previous
    actor.close(deadline=monotonic() + 1)


def test_malformed_authority_ack_never_publishes_authority_pin(monkeypatch):
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, sink)
    items = records()
    for item in items[:4]:
        actor.write(item, deadline=monotonic() + 1)
    previous = actor.observation
    monkeypatch.setattr(actor, "_dispatch", lambda *args: previous)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[4], deadline=monotonic() + 1)
    assert actor._authority_ack is None and actor.observation is previous
    actor.close(deadline=monotonic() + 1)


def test_lost_authority_ack_after_durable_write_never_publishes_authority_pin():
    durable = threading.Event()
    release = threading.Event()
    writes = []

    @contextmanager
    def factory():
        def write(name, payload):
            writes.append((name, payload))
            if len(writes) == 5:
                durable.set()
                assert release.wait(5)

        yield SimpleNamespace(write=write)

    pool = TdsActorPool(capacity=1)
    actor = opened(pool, factory)
    items = records()
    for item in items[:4]:
        actor.write(item, deadline=monotonic() + 1)
    previous = actor.observation
    try:
        with pytest.raises(TdsJournalActorUnknown):
            actor.write(items[4], deadline=monotonic() + 0.05)
        assert durable.is_set() and len(writes) == 5
        assert actor.observation is previous and actor._authority_ack is None
        with pytest.raises(TdsJournalActorUnknown):
            actor.write(items[5], deadline=monotonic() + 1)
    finally:
        release.set()
        actor.close(deadline=monotonic() + 2)
    assert actor.observation is previous and actor._authority_ack is None


def test_held_ready_claim_must_match_prior_result_receipt():
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, sink)
    items = list(records())
    for item in items[:7]:
        actor.write(item, deadline=monotonic() + 1)
    binding, authority, *_ = fixture()
    wrong = "f" * 64
    check = encode_permission_message(
        binding,
        PermissionWireKind.CHECK_HELD,
        4,
        {"boundary": PermissionBoundary.READY.value, "evidence_sha256": wrong},
    )
    held = encode_permission_message(
        binding,
        PermissionWireKind.HELD,
        4,
        {
            "boundary": PermissionBoundary.READY.value,
            "evidence_sha256": wrong,
            "authority": strict_json_object(encode_authority(authority)),
        },
    )
    original = decode_permission_grant_held_ready_evidence(items[7].payload, binding=binding)
    forged = replace(
        original,
        permission_evidence_sha256=wrong,
        check_payload_sha256=sha256(check).hexdigest(),
        check_payload=check,
        held_payload_sha256=sha256(held).hexdigest(),
        held_payload=held,
    )
    payload = encode_permission_grant_held_ready_evidence(forged, binding=binding)
    items[7] = PermissionGrantParentEvidenceRecord(items[7].subject, K.HELD_READY, payload, binding=binding)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[7], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)


def test_held_ready_authority_must_equal_acknowledged_authority():
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, sink)
    items = list(records())
    for item in items[:7]:
        actor.write(item, deadline=monotonic() + 1)
    binding, authority, *_ = fixture()
    changed_session = replace(authority.session, session_id=authority.session.session_id + 1)
    changed_authority = replace(authority, session=changed_session)
    original = decode_permission_grant_held_ready_evidence(items[7].payload, binding=binding)
    held = encode_permission_message(
        binding,
        PermissionWireKind.HELD,
        4,
        {
            "boundary": PermissionBoundary.READY.value,
            "evidence_sha256": original.permission_evidence_sha256,
            "authority": strict_json_object(encode_authority(changed_authority)),
        },
    )
    forged = replace(
        original,
        held_payload_sha256=sha256(held).hexdigest(),
        held_payload=held,
    )
    payload = encode_permission_grant_held_ready_evidence(forged, binding=binding)
    items[7] = PermissionGrantParentEvidenceRecord(items[7].subject, K.HELD_READY, payload, binding=binding)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(items[7], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)


def test_no_ninth_write_after_complete_sequence():
    pool = TdsActorPool(capacity=1)
    actor = opened(pool, sink)
    items = records()
    for item in items:
        actor.write(item, deadline=monotonic() + 1)
    with pytest.raises(ValueError):
        actor.write(items[0], deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)
