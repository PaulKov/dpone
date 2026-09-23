"""Original SQLite/file producer tests; synthetic process/SQL rows are not live proof."""

from dataclasses import replace
from time import monotonic
from uuid import uuid4

import pytest

from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
from dpone.services.mssql_tds_observe_settlement import ObserveSettlement
from tests.test_mssql_sqlclient_launch import typed_descriptor
from tests.test_mssql_sqlclient_preparation_composition import composed_preparation as composed_preparation


@pytest.fixture
def prepared(composed_preparation, tmp_path):
    h = composed_preparation
    path = tmp_path / "settlement-input"
    path.write_bytes(b"synthetic")
    with path.open("rb") as stream:
        descriptor = typed_descriptor(stream.fileno())
        descriptor = replace(
            descriptor, expected=replace(descriptor.expected, file_sha256=h.request.parent.file_sha256)
        )
        h.preparation_api.prepare_sqlclient_attempt(
            h.handle,
            parent_input=descriptor,
            input_fd=stream.fileno(),
            baseline=h.baseline,
            baseline_authority=h.baseline_authority,
            policy=h.policy,
            build=h.build,
            reader_factory=PinnedEvidenceReadFactory(h.evidence_root),
            evidence_root=h.evidence_root,
            expected_typed_digest="d" * 64,
        )
    yield h
    # Only fixture teardown releases the deliberately retained failed association.
    h.attempt._observe_settlement = None


def new_owner(*args, **kwargs):
    """Synthetic read-only helper facts; original journal/file producers remain real."""
    from types import SimpleNamespace

    owner = ObserveSettlement(*args, **kwargs)
    owner.test_facts = SimpleNamespace(
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
    owner.bind_helper_custody(owner.test_facts)
    return owner


def test_unfinished_sequence_consumes_original_and_retains_prepared(prepared):
    h = prepared
    original = h.attempt._prepared_origin
    before = h.attempt._lifecycle.snapshot
    owner = new_owner(h.attempt, original, uuid4(), deadline=monotonic() + 2)
    with pytest.raises(Exception):
        with owner.sequence():
            assert h.attempt._observe_settlement is owner
            assert owner.pending and not owner.complete and h.attempt._busy
    assert owner.failed and not owner.complete and owner.pending
    assert h.attempt._poisoned and not h.attempt._busy
    assert h.attempt._lifecycle.snapshot == before
    assert h.attempt._prepared_origin is original


def start_containment(h, owner):
    from contextlib import contextmanager

    from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
    from dpone.adapters.mssql_sqlclient_observe_containment_evidence_actor import (
        SqlClientObserveContainmentEvidenceActor,
    )

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(h.evidence_root)

    origin = owner.origin
    actor = h.pool.open(
        lambda d, c: SqlClientObserveContainmentEvidenceActor(factory, origin.identity, origin.startup.process, d, c),
        deadline=owner.deadline,
    )
    owner.retain_containment_gateway(actor)
    return actor


def plan_for(owner):
    from dpone.contracts.mssql_sqlclient_observe_departure import SqlClientObserveDeparturePlan
    from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
    from dpone.contracts.mssql_tds_coordinator_authority import authority_digest

    o = owner.origin
    resolution = o.management_incarnation.authority.principal_resolution
    return SqlClientObserveDeparturePlan(
        helper_id=owner.helper_id,
        attempt=o.parent.state.identity,
        ownership=o.parent.state.ownership,
        observe_operation=o.identity,
        observe_process=o.startup.process,
        original_registration_artifact_sha256=o.original_evidence[1][1].payload_sha256,
        original_authority_artifact_sha256=o.original_evidence[2][1].payload_sha256,
        original_authority_sha256=authority_digest(o.authority),
        original_containment_artifact_sha256=owner.containment_capture[2].payload_sha256,
        preparation_artifact_sha256=o.evidence_receipt.payload_sha256,
        original=o.authority.session,
        database=o.authority.database,
        management_admission=o.management_admission,
        principal=SqlClientDatabasePrincipal(resolution.principal_id, resolution.name, resolution.sid),
        observer_admission=o.management_admission,
        implementation_sha256=o.startup.implementation_sha256,
        package_root=o.startup.package_root,
        admission_sha256="e" * 64,
        startup_deadline=owner.deadline,
        operation_deadline=owner.deadline,
        max_address_space_bytes=8 << 30,
    )


def helper_chain(owner):
    """Controlled helper observations; genuine original authority is never replaced."""
    from datetime import datetime

    from dpone.contracts.mssql_sqlclient_observe_departure import SqlClientObserveDepartureRequest
    from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
    from tests.mssql_sqlclient_departure_v2_fixtures import sample
    from tests.test_mssql_sqlclient_observe_departure_evidence import chain

    plan = owner.plan
    startup = replace(
        owner.origin.startup, process=replace(owner.origin.startup.process, pid=owner.origin.startup.process.pid + 1)
    )
    request = SqlClientObserveDepartureRequest(plan=plan, startup=startup)
    old = sample(reused=False)
    own = replace(
        old.observer,
        connection_id=uuid4(),
        connect_time=datetime.max,
        login_time=datetime.max,
        session_id=plan.original.session_id + 1,
        authority=owner.origin.management_incarnation.authority,
        visibility=replace(old.observer.visibility, database_id=plan.database.database_id),
    )
    assert own.authority.server == plan.management_admission.server
    assert own.authority.database == plan.management_admission.database
    assert own.connect_time > plan.original.connect_time, (own.connect_time, plan.original.connect_time)
    assert own.login_time > plan.original.login_time, (own.login_time, plan.original.login_time)
    digest = observer_incarnation_digest(own)
    departure = replace(
        old,
        original=plan.original,
        database=plan.database,
        admission=plan.management_admission,
        principal=plan.principal,
        observer=own,
        samples=tuple(replace(s, before_sha256=digest, after_sha256=digest) for s in old.samples),
    )
    return chain(request, departure)


def write_chain(owner, actor, values):
    from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
    from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
    from dpone.contracts.mssql_tds_result import attempt_identity_digest

    request, objects, payloads = values
    owner.test_facts.helper_evidence = actor
    owner.bind_helper_evidence(actor)
    for index, (kind, payload) in enumerate(zip(Kind, payloads, strict=True)):
        if index == 1:
            owner.test_facts.child = object()
            owner.capture_helper_child()
            owner.test_facts.process = replace(request.startup.process)
            owner.capture_helper_process()
            owner.test_facts.startup = request.startup
            owner.capture_helper_startup()
            owner.capture_helper_request(request)
        elif index == 3:
            from dpone.contracts.mssql_sqlclient_observe_departure_codec import encode_observe_departure_result

            owner.test_facts.raw_result = encode_observe_departure_result(
                objects[3].result, request=request, observer_admission=owner.plan.observer_admission
            )
            owner.capture_helper_raw_result()
            owner.capture_helper_result(objects[3].result)
        elif index == 4:
            owner.test_facts.local_exit = objects[4].exit
            owner.capture_helper_exit(objects[4].exit)
            owner.test_facts.child_closed = True
        record = SqlClientDepartureEvidenceRecord(
            owner.helper_id,
            attempt_identity_digest(owner.plan.attempt),
            kind,
            payload,
            observe_plan=owner.plan if index == 0 else None,
            observe_request=request if index else None,
            observer_admission=owner.plan.observer_admission,
        )
        receipt = owner.write_helper_record(record, deadline=owner.deadline)
        assert receipt is owner.helper_records[kind][2]


def helper_actor(h, owner):
    from contextlib import contextmanager

    from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
    from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
    from dpone.contracts.mssql_tds_result import attempt_identity_digest

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(h.evidence_root)

    return h.pool.open(
        lambda d, c: SqlClientDepartureEvidenceActor(
            factory, owner.helper_id, attempt_identity_digest(owner.origin.identity.parent), d, c
        ),
        deadline=owner.deadline,
    )


def test_actual_original_containment_six_acks_and_provisional_release(prepared):
    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    parent = h.attempt.lifecycle
    with owner.sequence():
        start_containment(h, owner)
        local = owner.acknowledge_containment(deadline=owner.deadline)
        owner.bind_plan(plan_for(owner))
        actor = helper_actor(h, owner)
        try:
            write_chain(owner, actor, helper_chain(owner))
            remote = owner.acknowledge_settlement(deadline=owner.deadline)
            assert remote.state.slots[-1].settled and not local.state.slots[-1].settled
            assert owner.pending and not owner.complete
            actor.close(deadline=owner.deadline)
            owner.test_facts.helper_evidence_closed = True
            owner.close_containment(deadline=owner.deadline)
            owner.finish(deadline=owner.deadline)
            assert owner.pending and not owner.complete
        finally:
            actor.close(deadline=monotonic() + 2)
            owner.close_containment(deadline=monotonic() + 2)
    assert owner.complete and not owner.pending and not owner.failed
    assert h.attempt.lifecycle == parent


@pytest.mark.parametrize("fault", ["clock_raises", "clock_reentry", "owner_swap", "origin_swap"])
def test_entry_or_original_replacement_is_sticky_before_allocation(prepared, monkeypatch, fault):
    from copy import deepcopy

    from dpone.services import mssql_tds_observe_settlement as service

    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)

    def clock():
        assert h.attempt._observe_settlement is owner and h.attempt._busy
        if fault == "clock_reentry":
            with pytest.raises(Exception):
                h.attempt._owned()
            return monotonic()
        raise RuntimeError("clock unknown")

    if fault.startswith("clock"):
        monkeypatch.setattr(service, "monotonic", clock)
    with pytest.raises(Exception):
        with owner.sequence():
            if fault == "owner_swap":
                owner._helper = type(owner._helper)()
            else:
                owner.origin = deepcopy(owner.origin)  # Copying actor owners itself rejects.
            owner.assert_current(deadline=owner.deadline)
    assert h.attempt._poisoned and owner.pending and not owner.complete
    assert owner.containment_capture is None


@pytest.mark.parametrize("fault", ["lost_write", "post_write_guard", "directory_return"])
def test_containment_raw_returns_survive_unknown(prepared, monkeypatch, fault):
    from dpone.ports.mssql_tds_directory import RecordDirectoryContainment

    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    actor = None
    try:
        with pytest.raises(Exception):
            with owner.sequence():
                actor = start_containment(h, owner)
                original_write = actor.write
                original_execute = h.attempt._directory.execute

                def write(payload, *, deadline):
                    receipt = original_write(payload, deadline=deadline)
                    if fault == "lost_write":
                        raise RuntimeError("lost return")
                    if fault == "post_write_guard":
                        h.attempt._poisoned = True
                    return receipt

                def execute(request, *, deadline):
                    result = original_execute(request, deadline=deadline)
                    if isinstance(request, RecordDirectoryContainment):
                        h.attempt._poisoned = True
                    return result

                monkeypatch.setattr(actor, "write", write)
                if fault == "directory_return":
                    monkeypatch.setattr(h.attempt._directory, "execute", execute)
                owner.acknowledge_containment(deadline=owner.deadline)
        assert owner.failed and owner.pending and not owner.complete
        assert owner.containment_capture is not None
        assert (owner.containment_capture[2] is None) == (fault == "lost_write")
        assert (owner.local_return is not None) == (fault == "directory_return")
        assert owner.local_ack is None
    finally:
        if actor is not None:
            actor.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("fault", ["post_write_guard", "observation"])
def test_helper_raw_return_precedes_owner_guard_and_observation(prepared, monkeypatch, fault):
    from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
    from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
    from dpone.contracts.mssql_tds_result import attempt_identity_digest

    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    actor = containment = None
    try:
        with pytest.raises(Exception):
            with owner.sequence():
                containment = start_containment(h, owner)
                owner.acknowledge_containment(deadline=owner.deadline)
                owner.bind_plan(plan_for(owner))
                request, _, payloads = helper_chain(owner)
                actor = helper_actor(h, owner)
                owner.test_facts.helper_evidence = actor
                owner.bind_helper_evidence(actor)
                write = actor.write

                def lost(record, *, deadline):
                    receipt = write(record, deadline=deadline)
                    if fault == "post_write_guard":
                        h.attempt._poisoned = True
                    return receipt

                monkeypatch.setattr(actor, "write", lost)
                if fault == "observation":

                    def missing(actor):
                        raise RuntimeError("missing observation")

                    monkeypatch.setattr(type(actor), "observation", property(missing))
                record = SqlClientDepartureEvidenceRecord(
                    owner.helper_id,
                    attempt_identity_digest(owner.plan.attempt),
                    Kind.LAUNCH_INTENT,
                    payloads[0],
                    observe_plan=owner.plan,
                    observer_admission=owner.plan.observer_admission,
                )
                owner.write_helper_record(record, deadline=owner.deadline)
        entry = owner.helper_records[Kind.LAUNCH_INTENT]
        assert entry[2] is not None and entry[3] is None and entry[4] is None
        assert owner.failed and owner.pending
    finally:
        for gateway in (actor, containment):
            if gateway is not None:
                gateway.close(deadline=monotonic() + 2)


@pytest.mark.parametrize(
    "field",
    [
        "child",
        "unresolved_launch",
        "helper_evidence",
        "helper_evidence_closed",
        "child_closed",
        "process",
        "startup",
        "local_exit",
        "raw_result",
        "owner_origin",
        "owner_helper",
        "owner_directory",
        "owner_deadline",
        "actor_lifecycle",
        "actor_directory",
        "actor_lifecycle_alias",
        "actor_directory_alias",
    ],
)
def test_last_pool_callback_cannot_replace_actual_custody_before_publication(prepared, monkeypatch, field):
    from copy import deepcopy

    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    actor = containment = None
    try:
        with pytest.raises(Exception):
            with owner.sequence():
                containment = start_containment(h, owner)
                owner.acknowledge_containment(deadline=owner.deadline)
                owner.bind_plan(plan_for(owner))
                actor = helper_actor(h, owner)
                write_chain(owner, actor, helper_chain(owner))
                owner.acknowledge_settlement(deadline=owner.deadline)
                actor.close(deadline=owner.deadline)
                owner.test_facts.helper_evidence_closed = True
                owner.close_containment(deadline=owner.deadline)
                owner.finish(deadline=owner.deadline)
                original = h.pool.assert_deadline

                def changed(*, deadline):
                    original(deadline=deadline)
                    if field.startswith("actor_"):
                        gateway = h.attempt._lifecycle if "lifecycle" in field else h.attempt._directory
                        snapshot = deepcopy(gateway.snapshot)
                        value = snapshot if "lifecycle" in field else snapshot.snapshot
                        object.__setattr__(
                            value, "revision", float(value.revision) if field.endswith("alias") else value.revision + 1
                        )
                        gateway._snapshot = snapshot
                        return
                    if field.startswith("owner_"):
                        from copy import copy

                        name = field.removeprefix("owner_")
                        name = "_helper" if name == "helper" else name
                        value = getattr(owner, name)
                        value = (
                            value + 1
                            if name == "deadline"
                            else replace(value, revision=value.revision + 1)
                            if name == "directory"
                            else copy(value)
                        )
                        setattr(owner, name, value)
                        return
                    value = getattr(owner.test_facts, field)
                    if field in ("child", "unresolved_launch", "helper_evidence"):
                        value = object()
                    elif field.endswith("_closed"):
                        value = False
                    elif field == "raw_result":
                        value = bytes(bytearray(value))
                    else:
                        value = deepcopy(value)
                    setattr(owner.test_facts, field, value)

                monkeypatch.setattr(h.pool, "assert_deadline", changed)
        assert owner.remote_ack is not None and owner.remote_ack.state.slots[-1].settled
        assert owner.failed and owner.pending and not owner.complete and h.attempt._poisoned
    finally:
        for gateway in (actor, containment):
            if gateway is not None:
                gateway.close(deadline=monotonic() + 2)


@pytest.mark.parametrize(
    "field", ["helper_id", "original_authority_sha256", "preparation_artifact_sha256", "principal"]
)
def test_plan_cannot_replace_original_producer_facts(prepared, field):
    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    actor = None
    try:
        with pytest.raises(Exception):
            with owner.sequence():
                actor = start_containment(h, owner)
                owner.acknowledge_containment(deadline=owner.deadline)
                plan = plan_for(owner)
                value = (
                    uuid4()
                    if field == "helper_id"
                    else replace(plan.principal, name="substituted")
                    if field == "principal"
                    else "9" * 64
                )
                owner.bind_plan(replace(plan, **{field: value}))
        assert owner.failed and owner.plan is None
    finally:
        if actor is not None:
            actor.close(deadline=monotonic() + 2)


def test_containment_close_reentry_cannot_claim_ack(prepared, monkeypatch):
    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    actor = start_containment(h, owner)
    original = actor.close
    calls = []

    def reentered(*, deadline):
        calls.append(deadline)
        with pytest.raises(Exception):
            owner.close_containment(deadline=deadline + 10)
        original(deadline=deadline)

    monkeypatch.setattr(actor, "close", reentered)
    with pytest.raises(Exception):
        owner.close_containment(deadline=monotonic() + 1)
    assert len(calls) == 1 and not owner._containment_closed and owner.failed


def test_missing_containment_is_not_observed_close(prepared):
    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    owner.close_containment(deadline=monotonic() + 1)
    assert not owner._containment_closed


def test_actual_actor_shutdown_can_rewait_without_renewing_ceiling(prepared, monkeypatch):
    from contextlib import contextmanager
    from threading import Event

    from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
    from dpone.adapters.mssql_sqlclient_observe_containment_evidence_actor import (
        SqlClientObserveContainmentEvidenceActor,
    )

    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    release = Event()

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(h.evidence_root)
        release.wait(2)

    # This teardown probe needs wall time; retain the original pool and admit its same clock.
    monkeypatch.setattr(h.pool, "_clock", monotonic)
    actor = h.pool.open(
        lambda d, c: SqlClientObserveContainmentEvidenceActor(
            factory, owner.origin.identity, owner.origin.startup.process, d, c
        ),
        deadline=owner.deadline,
    )
    owner.retain_containment_gateway(actor)
    wait = actor._wait_stopped
    bounds = []

    def bounded(deadline):
        bounds.append(deadline)
        # Real actor thread is blocked; inject an earlier UNKNOWN wait return.
        return wait(min(deadline, monotonic() + 0.02) if len(bounds) == 1 else deadline)

    monkeypatch.setattr(actor, "_wait_stopped", bounded)
    ceiling = monotonic() + 1
    try:
        with pytest.raises(Exception):
            owner.close_containment(deadline=ceiling)
        assert owner.failed and not owner._containment_closed and actor._thread.is_alive()
        release.set()
        owner.close_containment(deadline=ceiling + 50)
        assert bounds == [ceiling, ceiling]
        assert owner._containment_closed and owner.failed and h.attempt._poisoned and not owner.complete
    finally:
        release.set()
        actor.close(deadline=monotonic() + 1)


def test_remote_ack_followed_by_actual_close_lost_return_stays_pending(prepared, monkeypatch):
    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    actor = containment = None
    try:
        with pytest.raises(Exception):
            with owner.sequence():
                containment = start_containment(h, owner)
                owner.acknowledge_containment(deadline=owner.deadline)
                owner.bind_plan(plan_for(owner))
                actor = helper_actor(h, owner)
                write_chain(owner, actor, helper_chain(owner))
                owner.acknowledge_settlement(deadline=owner.deadline)
                actor.close(deadline=owner.deadline)
                owner.test_facts.helper_evidence_closed = True
                original = containment.close

                def lost(*, deadline):
                    original(deadline=deadline)
                    raise RuntimeError("lost close return")

                monkeypatch.setattr(containment, "close", lost)
                owner.close_containment(deadline=owner.deadline)
        assert owner.remote_ack.state.slots[-1].settled
        assert owner.failed and owner.pending and not owner.complete and not owner._containment_closed
    finally:
        if actor is not None:
            actor.close(deadline=monotonic() + 2)
        if containment is not None:
            original(deadline=monotonic() + 2)


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), 0.0, -1.0, False, 1])
def test_failed_first_cleanup_budget_is_consumed_without_actor_call(prepared, monkeypatch, invalid):
    h = prepared
    owner = new_owner(h.attempt, h.attempt._prepared_origin, uuid4(), deadline=h.handle.operation_deadline)
    actor = start_containment(h, owner)
    original = actor.close
    calls = []

    def counted(*, deadline):
        calls.append(deadline)
        return original(deadline=deadline)

    monkeypatch.setattr(actor, "close", counted)
    try:
        with pytest.raises(Exception):
            owner.close_containment(deadline=invalid)
        assert owner.failed and not calls and not owner._containment_closed
        with pytest.raises(Exception):
            owner.close_containment(deadline=monotonic() + 2)
        assert not calls and owner._cleanup_deadline[0] is invalid
    finally:
        original(deadline=monotonic() + 2)
