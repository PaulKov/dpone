"""Finite producer retention; actual evidence actor persistence is exercised below."""

from contextlib import contextmanager
from copy import deepcopy
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.services.mssql_sqlclient_observe_helper_evidence import ObserveHelperEvidence
from tests.test_mssql_sqlclient_observe_departure_evidence import chain


@pytest.fixture
def captured_actor(tmp_path):
    request, objects, payloads = chain()
    helper = ObserveHelperEvidence()
    facts = SimpleNamespace(
        child=None,
        unresolved_launch=None,
        process=None,
        startup=None,
        local_exit=None,
        raw_result=None,
        child_closed=False,
        helper_evidence=None,
        helper_evidence_closed=False,
    )
    helper.bind_custody(facts)
    helper.bind_plan(request.plan)

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda d, c: SqlClientDepartureEvidenceActor(
            factory, request.plan.helper_id, attempt_identity_digest(request.plan.attempt), d, c
        ),
        deadline=monotonic() + 2,
    )
    facts.helper_evidence = actor
    helper.bind_gateway(actor)
    records = tuple(
        SqlClientDepartureEvidenceRecord(
            request.plan.helper_id,
            attempt_identity_digest(request.plan.attempt),
            kind,
            payload,
            observe_plan=request.plan if i == 0 else None,
            observe_request=request if i else None,
            observer_admission=request.plan.observer_admission,
        )
        for i, (kind, payload) in enumerate(zip(Kind, payloads, strict=True))
    )
    value = SimpleNamespace(
        helper=helper, facts=facts, actor=actor, request=request, objects=objects, records=records, root=tmp_path
    )
    try:
        yield value
    finally:
        actor.close(deadline=monotonic() + 2)
        pool.close(deadline=monotonic() + 2)


def persist(helper, record):
    helper.write_attempt(record, deadline=monotonic() + 2)
    helper.capture_observation()
    return helper.validate_ack()


def through(f, count=6):
    for index, record in enumerate(f.records[:count]):
        if index == 1:
            from dataclasses import replace

            f.facts.child = object()
            f.helper.capture_child()
            f.facts.process = replace(f.request.startup.process)
            f.helper.capture_process()
            f.facts.startup = f.request.startup
            f.helper.capture_startup()
            f.helper.capture_request(f.request)
        if index == 3:
            from dpone.contracts.mssql_sqlclient_observe_departure_codec import encode_observe_departure_result

            f.facts.raw_result = encode_observe_departure_result(
                f.objects[3].result, request=f.request, observer_admission=f.request.plan.observer_admission
            )
            f.helper.capture_raw_result()
            f.helper.capture_result(f.objects[3].result)
        if index == 4:
            f.facts.local_exit = f.objects[4].exit
            f.helper.capture_exit(f.objects[4].exit)
            f.facts.child_closed = True
        persist(f.helper, record)


def test_six_real_acks_preserve_canonical_bytes(captured_actor):
    f = captured_actor
    through(f)
    f.helper.validate_complete()
    for record in f.records:
        entry = f.helper.records[record.kind]
        assert entry[0] is record and entry[2] == entry[3].receipt and entry[4] is not None
        assert (f.root / entry[2].relative_name).read_bytes() == record.payload
    with pytest.raises(TypeError):
        f.helper.records[Kind.RESULT] = None


@pytest.mark.parametrize("phase", range(6))
def test_repeated_or_premature_phase_is_sticky(captured_actor, phase):
    f = captured_actor
    through(f, phase + 1)
    with pytest.raises(Exception):
        persist(f.helper, f.records[phase])
    assert f.helper.failed
    with pytest.raises(Exception):
        f.helper.validate_complete()


@pytest.mark.parametrize("fault", ["write_after_effect", "observation", "reentrant_write"])
def test_unknown_retains_actual_partial_producer_returns(captured_actor, monkeypatch, fault):
    f = captured_actor
    write = f.actor.write
    observation = type(f.actor).observation

    def failed_write(record, *, deadline):
        receipt = write(record, deadline=deadline)
        if fault == "reentrant_write":
            with pytest.raises(Exception):
                f.helper.write_attempt(record, deadline=deadline)
            return receipt
        raise RuntimeError("lost actual return")

    if fault != "observation":
        monkeypatch.setattr(f.actor, "write", failed_write)
    else:

        def failed_observation(actor):
            raise RuntimeError("observation unavailable")

        monkeypatch.setattr(type(f.actor), "observation", property(failed_observation))
    with pytest.raises(Exception):
        persist(f.helper, f.records[0])
    entry = f.helper.records[Kind.LAUNCH_INTENT]
    assert entry[0] is f.records[0] and entry[4] is None
    assert (entry[2] is None) == (fault == "write_after_effect")
    assert entry[3] is None
    assert (f.root / f.records[0].receipt.relative_name).read_bytes() == f.records[0].payload
    monkeypatch.setattr(type(f.actor), "observation", observation)
    with pytest.raises(Exception):
        f.helper.capture_observation()
    assert f.helper.records[Kind.LAUNCH_INTENT] is entry


@pytest.mark.parametrize("subject", ["request", "result", "exit", "startup", "process", "raw_alias"])
def test_equal_looking_original_substitution_cannot_use_valid_chain(captured_actor, subject):
    f = captured_actor
    through(f)
    if subject == "request":
        f.helper.request = deepcopy(f.request)
    elif subject == "result":
        f.helper.result = deepcopy(f.objects[3].result)
    elif subject == "exit":
        f.helper.local_exit = deepcopy(f.objects[4].exit)
    elif subject == "startup":
        object.__setattr__(f.request, "startup", deepcopy(f.request.startup))
    elif subject == "process":
        object.__setattr__(f.request.startup, "process", deepcopy(f.request.startup.process))
    else:
        object.__setattr__(f.objects[4].exit, "exit_code", False)
    with pytest.raises(Exception):
        f.helper.validate_complete()
    assert f.helper.failed


def test_request_capture_repetition_preserves_first_original(captured_actor):
    f = captured_actor
    through(f, 2)
    with pytest.raises(Exception):
        f.helper.capture_request(deepcopy(f.request))
    assert f.helper.request is f.request and f.helper.failed


def test_gateway_replacement_cannot_redirect_write(captured_actor):
    f = captured_actor
    f.helper._gateway = SimpleNamespace(write=lambda *a, **kw: pytest.fail("redirected write"))
    with pytest.raises(Exception):
        persist(f.helper, f.records[0])
    assert not f.helper.records


def test_unbound_helper_cannot_publish_a_complete_chain():
    helper = ObserveHelperEvidence()
    with pytest.raises(Exception):
        helper.validate_complete()
    assert helper.failed


def test_wrong_thread_rejects_before_actor_write(captured_actor):
    from threading import Thread

    f = captured_actor
    failures = []

    def other():
        try:
            persist(f.helper, f.records[0])
        except Exception as exc:
            failures.append(exc)

    thread = Thread(target=other)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and failures and not f.helper.records


@pytest.mark.parametrize("complete", [False, True])
def test_raw_capture_only_repeats_same_original_reference(captured_actor, complete):
    f = captured_actor
    if complete:
        through(f)
    f.helper.capture_raw_result()
    original = f.facts.raw_result
    f.helper.capture_raw_result()
    f.facts.raw_result = bytes(bytearray(original)) if complete else b"late bytes"
    with pytest.raises(Exception):
        f.helper.capture_raw_result()
    assert f.helper._raw_capture[0] is original and f.helper.failed
