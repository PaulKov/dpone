"""Real local persistence and actor containment; no SQL or producer authentication."""

import threading
from contextlib import contextmanager
from dataclasses import replace
from enum import StrEnum
from time import monotonic
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceObservation as Observation,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from tests.test_mssql_sqlclient_departure_evidence import cases, record
from tests.test_mssql_sqlclient_departure_ipc import request

HELPER = request().plan.helper_id
ATTEMPT = attempt_identity_digest(request().plan.attempt)


def item(index=0):
    kind, value, encode, _ = cases()[index]
    return record(kind, encode(value))


def opened(pool, factory, deadline=None):
    return pool.open(
        lambda d, c: SqlClientDepartureEvidenceActor(factory, HELPER, ATTEMPT, d, c),
        deadline=monotonic() + 2 if deadline is None else deadline,
    )


@contextmanager
def sink():
    yield SimpleNamespace(write=lambda *args: None)


@pytest.fixture
def actor():
    gateway = opened(TdsActorPool(capacity=1), sink)
    yield gateway
    gateway.close(deadline=monotonic() + 2)


def test_actual_writer_all_six_exact_bytes_on_one_actor_thread(tmp_path):
    events = []

    @contextmanager
    def factory():
        events.append(("enter", threading.get_ident()))
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, payload):
            events.append(("write", threading.get_ident()))
            assert len(("." + name + ".stage").encode()) <= 255
            writer.write(name, payload)

        yield SimpleNamespace(write=write)
        events.append(("exit", threading.get_ident()))

    pool = TdsActorPool(capacity=1)
    gateway = opened(pool, factory)
    assert gateway.observation == Observation(HELPER, ATTEMPT)
    for index in range(6):
        value = item(index)
        receipt = gateway.write(value, deadline=monotonic() + 2)
        assert receipt == value.receipt
        assert (tmp_path / receipt.relative_name).read_bytes() == value.payload
        assert gateway.observation == Observation(HELPER, ATTEMPT, receipt)
    gateway.close(deadline=monotonic() + 2)
    assert len({owner for _, owner in events}) == 1
    assert events[0][1] != threading.get_ident()
    assert pool.live_count == 0
    assert [event for event, _ in events] == ["enter", *(["write"] * 6), "exit"]


def test_actual_grant_context_all_six_persist_on_same_nominal_actor(tmp_path):
    from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
    from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
        PermissionGrantDepartureEvidenceContext,
    )
    from tests.test_mssql_sqlclient_permission_grant_departure import grant_evidence_payloads

    request, _, payloads = grant_evidence_payloads()
    attempt = attempt_identity_digest(request.plan.attempt)
    pool = TdsActorPool(capacity=1)

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    gateway = pool.open(
        lambda d, c: SqlClientDepartureEvidenceActor(factory, request.plan.helper_id, attempt, d, c),
        deadline=monotonic() + 2,
    )
    context = PermissionGrantDepartureEvidenceContext(request.plan)
    for kind in Kind:
        record = SqlClientDepartureEvidenceRecord(
            request.plan.helper_id,
            attempt,
            kind,
            payloads[kind],
            permission_grant_context=context,
        )
        assert gateway.write(record, deadline=monotonic() + 2) == record.receipt
    gateway.close(deadline=monotonic() + 2)


def test_constructor_is_allocation_only_and_validates_subject():
    def forbidden():
        pytest.fail("allocation called a callback")

    gateway = SqlClientDepartureEvidenceActor(forbidden, HELPER, ATTEMPT, 1.0, forbidden)
    assert gateway._thread.ident is None
    with pytest.raises(ValueError, match="sqlclient_departure_evidence_invalid"):
        SqlClientDepartureEvidenceActor(sink, cast(Any, str(HELPER)), ATTEMPT, 1.0, monotonic)


@pytest.mark.parametrize("index", range(6))
def test_duplicate_kind_never_resubmits_even_changed_bytes(actor, index, monkeypatch):
    calls = []
    original = actor._dispatch

    def counted(*args):
        calls.append(True)
        return original(*args)

    monkeypatch.setattr(actor, "_dispatch", counted)
    value = item(index)
    actor.write(value, deadline=monotonic() + 2)
    kind, dto, encode, _ = cases()[index]
    if kind is Kind.LAUNCH_INTENT:
        changed = replace(dto, plan=replace(dto.plan, operation_deadline=21.0))
    else:
        field = next(name for name in dto.__slots__ if name.endswith("sha256") and name != "attempt_sha256")
        changed = replace(dto, **{field: "9" * 64})
    for duplicate in (value, record(kind, encode(changed))):
        with pytest.raises(ValueError, match="^mssql_native.sqlclient_departure_evidence_kind_consumed$"):
            actor.write(duplicate, deadline=monotonic() + 2)
    assert len(calls) == 1


@pytest.mark.parametrize("variation", ["helper", "attempt", "both"])
@pytest.mark.parametrize("source", range(3))
def test_three_subjects_cannot_bind(actor, variation, source):
    r = request()
    attempt = replace(r.plan.attempt, ordinal=source + 1) if variation != "helper" else r.plan.attempt
    helper = UUID(int=source + 1) if variation != "attempt" else HELPER
    r = replace(
        r,
        plan=replace(
            r.plan, helper_id=helper, attempt=attempt, create_operation=replace(r.plan.create_operation, parent=attempt)
        ),
    )
    kind, dto, encode, _ = cases(r)[0]
    with pytest.raises(ValueError, match="sqlclient_departure_evidence_invalid"):
        actor.write(record(kind, encode(dto), r), deadline=monotonic() + 2)
    assert actor.observation.receipt is None


@pytest.mark.parametrize(
    "mutation", ["payload", "kind", "helper", "attempt", "missing_context", "extra_context", "nested", "type"]
)
def test_invalid_input_never_consumes_kind_or_calls_backend(actor, mutation, monkeypatch):
    value = item(3 if mutation in {"missing_context", "nested"} else 0)
    if mutation == "nested":
        object.__setattr__(value.result_context.startup.process, "pid", 124.0)
    elif mutation == "type":
        value = SimpleNamespace()
    else:
        field, replacement = {
            "payload": ("payload", b"{}"),
            "kind": ("kind", "launch_intent"),
            "helper": ("helper_id", str(HELPER)),
            "attempt": ("attempt_sha256", "A" * 64),
            "missing_context": ("result_context", None),
            "extra_context": ("result_context", request()),
        }[mutation]
        object.__setattr__(value, field, replacement)
    calls = []
    monkeypatch.setattr(actor, "_dispatch", lambda *args: calls.append(args))
    with pytest.raises(ValueError, match="sqlclient_departure_evidence_invalid"):
        actor.write(value, deadline=monotonic() + 2)
    assert not calls and not actor._attempted


def test_context_mutation_after_enqueue_rejected_on_backend(actor, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = actor._dispatch
    value = item(3)

    def dispatch(*args):
        entered.set()
        assert release.wait(2)
        return original(*args)

    def mutate():
        assert entered.wait(2)
        object.__setattr__(value.result_context.startup.process, "pid", 124.0)
        release.set()

    monkeypatch.setattr(actor, "_dispatch", dispatch)
    mutator = threading.Thread(target=mutate)
    mutator.start()
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(value, deadline=monotonic() + 2)
    mutator.join(timeout=2)
    assert not mutator.is_alive() and actor.observation.receipt is None and not actor._persisted


@pytest.mark.parametrize(
    "mutation",
    [
        "helper",
        "attempt",
        "receipt",
        "float",
        "bool",
        "kind",
        "uuid_alias",
        "uuid_subclass",
        "enum_alias",
        "int_alias",
        "hash",
        "name",
        "count",
        "snapshot",
    ],
)
def test_provisional_bad_ack_restores_previous_nonempty(actor, mutation, monkeypatch):
    class TextAlias(str):
        pass

    class UUIDAlias(UUID):
        pass

    class KindAlias(StrEnum):
        REGISTRATION = "registration"

    class IntegerAlias(int):
        pass

    actor.write(item(0), deadline=monotonic() + 2)
    previous = actor.observation
    expected = item(1).receipt
    observed = Observation(HELPER, ATTEMPT, expected)
    if mutation in {"helper", "attempt", "receipt", "uuid_alias", "uuid_subclass"}:
        field, value = {
            "helper": ("helper_id", UUID(int=7)),
            "attempt": ("attempt_sha256", TextAlias(ATTEMPT)),
            "receipt": ("receipt", {}),
            "uuid_alias": ("helper_id", str(HELPER)),
            "uuid_subclass": ("helper_id", UUIDAlias(str(HELPER))),
        }[mutation]
        object.__setattr__(observed, field, value)
    elif mutation != "snapshot":
        field, value = {
            "float": ("byte_count", float(expected.byte_count)),
            "bool": ("byte_count", True),
            "kind": ("kind", expected.kind.value),
            "hash": ("payload_sha256", "9" * 64),
            "name": ("relative_name", TextAlias(expected.relative_name)),
            "count": ("byte_count", expected.byte_count + 1),
            "enum_alias": ("kind", KindAlias.REGISTRATION),
            "int_alias": ("byte_count", IntegerAlias(expected.byte_count)),
        }[mutation]
        object.__setattr__(expected, field, value)
    if mutation == "snapshot":

        def wrong_stored(command):
            actor._snapshot = previous
            return observed

        monkeypatch.setattr(actor, "_call", wrong_stored)
    else:
        monkeypatch.setattr(actor, "_dispatch", lambda *args: observed)
    with pytest.raises(TdsJournalActorUnknown) as caught:
        actor.write(item(1), deadline=monotonic() + 2)
    assert caught.value.gateway is actor and actor.observation is previous
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(item(2), deadline=monotonic() + 2)


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_post_submission_exception_preserves_control_flow_and_prior_ack(actor, monkeypatch, error):
    actor.write(item(0), deadline=monotonic() + 2)
    previous, original = actor.observation, actor._call

    def fail_after_call(command):
        original(command)
        raise error("private-canary")

    monkeypatch.setattr(actor, "_call", fail_after_call)
    with pytest.raises(error):
        actor.write(item(1), deadline=monotonic() + 2)
    assert actor.observation is previous and actor._stop.is_set()


def test_expired_submission_consumes_kind(actor):
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(item(0), deadline=monotonic() - 1)
    with pytest.raises(ValueError, match="kind_consumed"):
        actor.write(item(0), deadline=monotonic() + 2)
    assert not actor._persisted


@pytest.mark.parametrize("mutation", ["duplicate", "command_kind"])
def test_backend_rejects_duplicate_kind_or_foreign_command(actor, monkeypatch, mutation):
    from dpone.adapters.mssql_tds_actor_core import _ActorCommand

    actor.write(item(0), deadline=monotonic() + 2)
    previous = actor.observation
    original = actor._call

    def corrupt_command(command):
        event = item(0) if mutation == "duplicate" else command.event
        return original(_ActorCommand("other" if mutation == "command_kind" else "evidence", command.deadline, event))

    monkeypatch.setattr(actor, "_call", corrupt_command)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(item(1), deadline=monotonic() + 2)
    assert actor.observation is previous and actor._persisted == {Kind.LAUNCH_INTENT}


def test_observe_actual_writer_retains_all_six_contexts(tmp_path):
    from tests.test_mssql_sqlclient_departure_evidence import observe_record

    first = observe_record()

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda d, c: SqlClientDepartureEvidenceActor(factory, first.helper_id, first.attempt_sha256, d, c),
        deadline=monotonic() + 2,
    )
    try:
        for index in range(6):
            value = observe_record(index)
            receipt = actor.write(value, deadline=monotonic() + 2)
            assert receipt == value.receipt
            assert (tmp_path / receipt.relative_name).read_bytes() == value.payload
            assert actor.observation.receipt == receipt
    finally:
        actor.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("index", range(6))
def test_observe_dispatch_revalidates_retained_nested_context(index, monkeypatch):
    from tests.test_mssql_sqlclient_departure_evidence import observe_record

    value = observe_record(index)
    pool = TdsActorPool(capacity=1)
    writes = []

    @contextmanager
    def factory():
        yield SimpleNamespace(write=lambda *args: writes.append(args))

    actor = pool.open(
        lambda d, c: SqlClientDepartureEvidenceActor(factory, value.helper_id, value.attempt_sha256, d, c),
        deadline=monotonic() + 2,
    )
    original = actor._dispatch

    def dispatch(*args):
        context = value.observe_plan if index == 0 else value.observe_request.plan
        object.__setattr__(context.observe_process, "pid", float(context.observe_process.pid))
        return original(*args)

    monkeypatch.setattr(actor, "_dispatch", dispatch)
    with pytest.raises(TdsJournalActorUnknown) as caught:
        actor.write(value, deadline=monotonic() + 2)
    assert caught.value.gateway is actor and not writes and actor.observation.receipt is None
    assert actor._attempted == {value.kind}
    actor.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("mode", ["late", "partial", "close"])
def test_observe_uncertain_effect_keeps_prior_ack_and_attempt(mode, tmp_path):
    from tests.test_mssql_sqlclient_departure_evidence import observe_record

    entered, release = threading.Event(), threading.Event()
    count = []

    @contextmanager
    def factory():
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, payload):
            count.append(name)
            if len(count) == 2:
                entered.set()
                if mode == "late":
                    assert release.wait(2)
            writer.write(name, payload)
            if len(count) == 2 and mode == "partial":
                raise OSError("synthetic uncertain durability")

        yield SimpleNamespace(write=write)
        if mode == "close":
            raise OSError("synthetic close failure")

    first, second = observe_record(), observe_record(1)
    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda d, c: SqlClientDepartureEvidenceActor(factory, first.helper_id, first.attempt_sha256, d, c),
        deadline=monotonic() + 2,
    )
    actor.write(first, deadline=monotonic() + 2)
    previous = actor.observation
    if mode == "close":
        with pytest.raises(TdsJournalActorUnknown):
            actor.close(deadline=monotonic() + 2)
    else:
        try:
            with pytest.raises(TdsJournalActorUnknown) as caught:
                actor.write(second, deadline=monotonic() + (0.05 if mode == "late" else 2))
            assert caught.value.gateway is actor
            if mode == "late":
                assert entered.is_set() and pool.live_count == 1
                with pytest.raises(TdsJournalActorUnknown):
                    actor.close(deadline=monotonic() + 0.01)
        finally:
            release.set()
            actor.close(deadline=monotonic() + 2)
        assert (tmp_path / second.receipt.relative_name).read_bytes() == second.payload
        assert second.kind in actor._persisted
    assert actor.observation is previous
    with pytest.raises((ValueError, TdsJournalActorUnknown)):
        actor.write(second, deadline=monotonic() + 2)
    assert len(count) == (1 if mode == "close" else 2)


@pytest.mark.parametrize("mode", ["reentry", "lost", "wrong"])
def test_observe_unknown_ack_preserves_prior_receipt(mode, monkeypatch, tmp_path):
    from tests.test_mssql_sqlclient_departure_evidence import observe_record

    first, second = observe_record(), observe_record(1)

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    actor = TdsActorPool(capacity=1).open(
        lambda d, c: SqlClientDepartureEvidenceActor(factory, first.helper_id, first.attempt_sha256, d, c),
        deadline=monotonic() + 2,
    )
    actor.write(first, deadline=monotonic() + 2)
    previous = actor.observation
    original = actor._call

    def call(command):
        if mode == "reentry":
            actor.write(second, deadline=monotonic() + 2)
        observed = original(command)
        if mode == "lost":
            raise TdsJournalActorUnknown(actor)
        object.__setattr__(observed.receipt, "byte_count", float(observed.receipt.byte_count))
        return observed

    monkeypatch.setattr(actor, "_call", call)
    with pytest.raises(TdsJournalActorUnknown) as caught:
        actor.write(second, deadline=monotonic() + 2)
    assert caught.value.gateway is actor and actor.observation is previous
    with pytest.raises(ValueError, match="consumed"):
        actor.write(second, deadline=monotonic() + 2)
    actor.close(deadline=monotonic() + 2)
    assert len(list(tmp_path.glob("*.json"))) == (1 if mode == "reentry" else 2)
