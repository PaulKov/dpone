"""The P7 service reaches HELD_READY without releasing any retained owner."""

import math
from contextlib import contextmanager
from dataclasses import replace
from time import monotonic
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_permission_grant_evidence_actor import SqlClientPermissionGrantEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    PermissionGrantParentEvidenceKind as E,
)
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    decode_permission_grant_held_ready_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import PermissionWireKind as K
from dpone.contracts.mssql_sqlclient_permission_grant_wire import decode_permission_message, encode_permission_message
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorPhase,
    TdsCoordinatorSnapshot,
    TdsCoordinatorState,
    advance_coordinator_state,
)
from dpone.contracts.mssql_tds_coordinator_ipc import encode_startup
from dpone.contracts.strict_json import strict_json_object
from dpone.ports.mssql_tds_coordinator import AdvanceCoordinator
from dpone.services.mssql_tds_permission_grant import (
    PermissionGrantHeldUnknown,
    hold_permission_grant,
    permission_evidence_subject,
)
from tests.test_mssql_sqlclient_permission_grant_parent_evidence import evidence_fixture
from tests.test_mssql_sqlclient_permission_grant_wire import fixture


class Association:
    def __init__(self, binding, events):
        self._identity, self._reservation, self.events = binding.operation, object(), events
        self.failed = False

    def assert_ready(self):
        self.events.append("association")
        if self.failed:
            raise ValueError("changed")

    @property
    def identity(self):
        return self._identity

    @property
    def reservation(self):
        return self._reservation


class Observation:
    def __init__(self, *, snapshot=None, receipt=None):
        self._snapshot, self._receipt = snapshot, receipt
        self.snapshot_hook = self.receipt_hook = None

    @staticmethod
    def _fire(instance, name):
        hook = getattr(instance, name)
        if hook is not None:
            setattr(instance, name, None)
            hook()

    @property
    def snapshot(self):
        self._fire(self, "snapshot_hook")
        return self._snapshot

    @snapshot.setter
    def snapshot(self, value):
        self._snapshot = value

    @property
    def receipt(self):
        self._fire(self, "receipt_hook")
        return self._receipt

    @receipt.setter
    def receipt(self, value):
        self._receipt = value


class Coordinator:
    def __init__(self, binding, events, fail_on=None):
        state = TdsCoordinatorState(
            binding.operation, binding.execution_owner, binding.execution_owner, TdsCoordinatorPhase.INTENT, 0
        )
        self.snapshot = TdsCoordinatorSnapshot(state, 1)
        self._observation = Observation(snapshot=self.snapshot)
        self.observation_hook = None
        self.events = events
        self.fail_on = fail_on

    @property
    def observation(self):
        Observation._fire(self, "observation_hook")
        return self._observation

    def execute(self, request, *, deadline):
        if isinstance(request, AdvanceCoordinator):
            self.events.append("coordinator:" + type(request.event).__name__)
            state = advance_coordinator_state(self.snapshot.state, request.event, expected_phase=request.expected_phase)
            self.snapshot = TdsCoordinatorSnapshot(state, self.snapshot.revision + 1)
            self._observation.snapshot = self.snapshot
            if type(request.event).__name__ == self.fail_on:
                raise OSError("lost ack")
        return self.snapshot

    def close(self, *, deadline):
        raise AssertionError("coordinator close forbidden in P7")


class Evidence:
    def __init__(self, events, fail=None):
        self.events, self.fail, self.records = events, fail, []
        self._observation = Observation(receipt=None)
        self.observation_hook = None

    @property
    def observation(self):
        Observation._fire(self, "observation_hook")
        return self._observation

    def write(self, record, *, deadline):
        self.events.append("evidence:" + record.kind.value)
        if record.kind is self.fail:
            raise OSError("lost ack")
        assert record.kind is tuple(E)[len(self.records)]
        self.records.append(record)
        self._observation.receipt = record.receipt
        return record.receipt


class Process:
    def __init__(self, binding, payloads, events, fail_on=None):
        self.binding, self.payloads, self.events = binding, payloads, events
        self.fail_on = fail_on
        self.calls = []
        self.fail_send = self.fail_credentials = False
        ready = decode_permission_grant_held_ready_evidence(payloads[E.HELD_READY], binding=binding)
        self.inbound = {
            K.STARTUP: encode_permission_message(
                binding, K.STARTUP, 0, {"startup": strict_json_object(encode_startup(binding.startup))}
            ),
            K.REQUEST_ACCEPTED: payloads[E.REQUEST_ACCEPTED],
            K.AUTHORITY: payloads[E.AUTHORITY],
            K.PERMISSION_HELD: encode_permission_message(
                binding, K.PERMISSION_HELD, 3, {"evidence": strict_json_object(payloads[E.RESULT])}
            ),
            K.HELD: ready.held_payload,
        }

    def receive_public(self, kind, ordinal, *, deadline=None):
        if kind is self.fail_on:
            raise OSError("process died")
        self.events.append("receive:" + kind.value)
        self.calls.append(("receive", kind))
        return decode_permission_message(self.inbound[kind], binding=self.binding, kind=kind, ordinal=ordinal)

    def send_public(self, kind, ordinal, body, *, deadline=None):
        self.events.append("send:" + kind.value)
        self.calls.append(("send", kind))
        if self.fail_send is kind:
            raise OSError("partial public write")
        payload = encode_permission_message(self.binding, kind, ordinal, body)
        return decode_permission_message(payload, binding=self.binding, kind=kind, ordinal=ordinal)

    def send_credentials(self, payload, *, deadline=None):
        self.events.append("credentials")
        self.calls.append(("credentials", len(payload)))
        if self.fail_credentials:
            raise OSError("partial private write")

    def observe_eof(self, **kwargs):
        raise AssertionError("EOF forbidden in P7")

    def settle(self, **kwargs):
        raise AssertionError("settlement forbidden in P7")

    def contain(self, **kwargs):
        raise AssertionError("success containment forbidden in P7")


def setup_run(
    *,
    evidence_failure=None,
    credential=None,
    coordinator_failure=None,
    process_failure=None,
    clock=None,
    factory_hook=None,
    evidence_factory=None,
    deadline=1.0,
):
    binding, _, payloads = evidence_fixture()
    _, _, grant, _ = fixture()
    events: list[str] = []
    association = Association(binding, events)
    coordinator = Coordinator(binding, events, coordinator_failure)
    process = Process(binding, payloads, events, process_failure)
    evidence = Evidence(events, evidence_failure)

    def launch(inputs):
        events.append("launch")
        return process

    launcher = SimpleNamespace(launch=launch)
    inputs = SimpleNamespace(
        request=binding.request,
        operation=binding.operation,
        execution_owner=binding.execution_owner,
        operation_deadline=math.nextafter(binding.operation_deadline_ns / 1_000_000_000, math.inf),
    )

    def credentials(*args):
        events.append("credential_supplier")
        return b"PRIVATE_CANARY" if credential is None else credential(*args)

    def invoke():
        def open_evidence(value):
            events.append("evidence_open")
            if factory_hook is not None:
                factory_hook(association)
            return evidence if evidence_factory is None else evidence_factory(value)

        return hold_permission_grant(
            association,
            coordinator,
            launcher,
            inputs,
            payloads[E.ADMISSION],
            credentials,
            open_evidence,
            grant_id=lambda: grant.grant_id,
            deadline=deadline,
            clock=clock or (lambda: 0.0),
        )

    return invoke, association, coordinator, process, evidence, events


def test_exact_order_reaches_held_ready_and_retains_same_capabilities():
    invoke, association, coordinator, process, evidence, events = setup_run()
    owner = invoke()
    assert owner.association is association and owner.process is process
    assert owner.coordinator is coordinator and owner.evidence is evidence
    assert owner.phase == "HELD_READY" and len(owner.receipts) == 8
    assert not hasattr(owner, "writer")
    assert coordinator.snapshot.state.phase is TdsCoordinatorPhase.RESULT_RECEIVED
    assert [record.kind for record in evidence.records] == list(E)
    assert [call for call in process.calls if call[0] == "send"] == [
        ("send", K.REQUEST),
        ("send", K.EXECUTE),
        ("send", K.CHECK_HELD),
    ]
    assert all(kind not in (K.RELEASE, K.RELEASED) for _, kind in process.calls if isinstance(kind, K))
    assert "PRIVATE_CANARY" not in repr(owner)
    assert (
        events.index("evidence:execution_intent")
        < events.index("coordinator:CoordinatorGrantIntent")
        < events.index("send:EXECUTE")
    )
    assert (
        events.index("evidence:result")
        < events.index("coordinator:CoordinatorResultReceived")
        < events.index("send:CHECK_HELD")
    )


@pytest.mark.parametrize("kind", list(E))
def test_every_lost_evidence_ack_is_unknown_and_never_releases(kind):
    invoke, _, _, process, evidence, _ = setup_run(evidence_failure=kind)
    with pytest.raises(PermissionGrantHeldUnknown) as caught:
        invoke()
    assert caught.value.owner.phase == "UNKNOWN"
    assert [record.kind for record in evidence.records] == list(E)[: list(E).index(kind)]
    assert all(call[1] is not K.RELEASE for call in process.calls if call[0] == "send")


def test_association_drift_after_first_effect_is_sticky_unknown():
    invoke, association, _, process, _, events = setup_run()
    original = process.receive_public

    def drift(kind, ordinal, **kwargs):
        value = original(kind, ordinal, **kwargs)
        if kind is K.STARTUP:
            association.failed = True
        return value

    process.receive_public = drift
    with pytest.raises(PermissionGrantHeldUnknown) as caught:
        invoke()
    with pytest.raises(PermissionGrantHeldUnknown):
        caught.value.owner.assert_held_ready()
    assert "send:REQUEST" not in events


def test_lost_coordinator_ack_retains_unknown_owner_without_release():
    invoke, _, coordinator, process, _, _ = setup_run(coordinator_failure="CoordinatorCredentialIntent")
    with pytest.raises(PermissionGrantHeldUnknown) as caught:
        invoke()
    assert caught.value.owner.coordinator is coordinator
    assert all(call[1] is not K.RELEASE for call in process.calls if call[0] == "send")


@pytest.mark.parametrize("failure", [K.AUTHORITY, K.PERMISSION_HELD])
def test_process_death_or_partial_transcript_is_sticky_unknown(failure):
    invoke, _, _, process, _, _ = setup_run(process_failure=failure)
    with pytest.raises(PermissionGrantHeldUnknown) as caught:
        invoke()
    assert caught.value.owner.process is process
    assert caught.value.owner.phase == "UNKNOWN"


def test_expired_deadline_before_launch_has_no_process_effect():
    invoke, _, _, process, _, events = setup_run(clock=lambda: 2.0)
    with pytest.raises(PermissionGrantHeldUnknown):
        invoke()
    assert process.calls == [] and "launch" not in events


def test_caught_credential_callback_reentry_still_poisoned():
    association: Any = None

    def reenter(*args):
        try:
            association._held_ready_owner.assert_held_ready()
        except PermissionGrantHeldUnknown:
            pass
        return b"PRIVATE_CANARY"

    invoke, association, *_ = setup_run(credential=reenter)
    with pytest.raises(PermissionGrantHeldUnknown):
        invoke()


def test_equal_value_process_cannot_replace_retained_capability():
    invoke, *_ = setup_run()
    owner = invoke()
    owner.process = SimpleNamespace(**owner.process.__dict__)
    with pytest.raises(PermissionGrantHeldUnknown):
        owner.assert_held_ready()


@pytest.mark.parametrize(
    "change",
    ["binding", "startup", "owner", "authority", "grant", "result", "receipt", "coordinator", "evidence"],
)
def test_equal_value_substitution_poison_is_sticky(change):
    invoke, *_ = setup_run()
    owner = invoke()
    if change == "binding":
        owner.binding = replace(owner.binding)
    elif change == "startup":
        object.__setattr__(owner.binding, "startup", replace(owner.binding.startup))
    elif change == "owner":
        object.__setattr__(owner.binding, "execution_owner", replace(owner.binding.execution_owner))
    elif change == "authority":
        owner.authority = replace(owner.authority)
    elif change == "grant":
        owner.grant = replace(owner.grant)
    elif change == "result":
        owner.result = replace(owner.result)
    elif change == "receipt":
        owner.receipts = (replace(owner.receipts[0]), *owner.receipts[1:])
    elif change == "coordinator":
        owner.coordinator.observation.snapshot = replace(owner.coordinator.observation.snapshot)
    else:
        owner.evidence.observation.receipt = replace(owner.evidence.observation.receipt)
    with pytest.raises(PermissionGrantHeldUnknown):
        owner.assert_held_ready()
    with pytest.raises(PermissionGrantHeldUnknown):
        owner.assert_held_ready()


@pytest.mark.parametrize("private", [False, True])
def test_partial_public_or_private_write_is_unknown(private):
    invoke, _, _, process, _, _ = setup_run()
    process.fail_credentials = private
    process.fail_send = False if private else K.REQUEST
    with pytest.raises(PermissionGrantHeldUnknown):
        invoke()
    assert ("credentials", len(b"PRIVATE_CANARY")) in process.calls if private else ("send", K.REQUEST) in process.calls


@pytest.mark.parametrize("source", ["clock", "factory"])
def test_callback_reentry_poison_prevents_next_effect(source):
    association: Any = None

    def reenter(*args):
        try:
            association._held_ready_owner.assert_held_ready()
        except PermissionGrantHeldUnknown:
            pass
        return 0.0

    kwargs = {"clock": reenter} if source == "clock" else {"factory_hook": reenter}
    invoke, association, _, process, evidence, events = setup_run(**kwargs)
    with pytest.raises(PermissionGrantHeldUnknown):
        invoke()
    if source == "clock":
        assert "launch" not in events
    else:
        assert evidence.records == [] and process.calls == [("receive", K.STARTUP)]


@pytest.mark.parametrize("source", ["coordinator_observation", "coordinator_snapshot"])
def test_initial_observation_getter_reentry_prevents_launch_and_is_sticky(source):
    invoke, association, coordinator, _, _, events = setup_run()

    def reenter():
        try:
            association._held_ready_owner.assert_held_ready()
        except PermissionGrantHeldUnknown:
            pass

    if source == "coordinator_observation":
        coordinator.observation_hook = reenter
    else:
        coordinator._observation.snapshot_hook = reenter
    with pytest.raises(PermissionGrantHeldUnknown) as caught:
        invoke()
    with pytest.raises(PermissionGrantHeldUnknown):
        caught.value.owner.assert_held_ready()
    assert "launch" not in events


@pytest.mark.parametrize("source", ["evidence_observation", "evidence_receipt"])
def test_evidence_getter_reentry_prevents_commit_and_next_effect(source):
    invoke, association, _, process, evidence, events = setup_run()

    def reenter():
        try:
            association._held_ready_owner.assert_held_ready()
        except PermissionGrantHeldUnknown:
            pass

    if source == "evidence_observation":
        evidence.observation_hook = reenter
    else:
        evidence._observation.receipt_hook = reenter
    with pytest.raises(PermissionGrantHeldUnknown) as caught:
        invoke()
    with pytest.raises(PermissionGrantHeldUnknown):
        caught.value.owner.assert_held_ready()
    assert caught.value.owner.receipts == ()
    assert [record.kind for record in evidence.records] == [E.REQUEST]
    assert process.calls == [("receive", K.STARTUP)]
    assert "evidence:admission" not in events


def test_descriptor_pinned_success_contains_exactly_eight_files(tmp_path):
    pool = TdsActorPool(capacity=1)
    target = monotonic() + 5

    @contextmanager
    def writer():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    def factory(binding):
        return pool.open(
            lambda deadline, clock: SqlClientPermissionGrantEvidenceActor(
                writer,
                permission_evidence_subject(binding),
                binding,
                deadline,
                clock,
            ),
            deadline=target,
        )

    invoke, *_ = setup_run(evidence_factory=factory, deadline=target, clock=monotonic)
    owner = invoke()
    assert len(tuple(tmp_path.iterdir())) == 8
    owner.evidence.close(deadline=target)
    pool.close(deadline=target)
