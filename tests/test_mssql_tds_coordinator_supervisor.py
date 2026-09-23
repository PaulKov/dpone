"""CREATE ordering through pure journal transitions and real envelope codecs."""

from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsCoordinatorBuild,
    encode_connection_admission,
)
from dpone.app.mssql_tds_coordinator_request import (
    decode_credentials,
    decode_grant,
    encode_create_response,
    failed_response,
    successful_response,
)
from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorFailure, TdsCoordinatorSupervisionUnknown
from dpone.app.mssql_tds_coordinator_supervisor import run_tds_coordinator
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorPhase,
    TdsCoordinatorSnapshot,
    TdsCoordinatorState,
    advance_coordinator_state,
    coordinator_grant_digest,
)
from dpone.contracts.mssql_tds_coordinator_authority import authority_digest, encode_authority
from dpone.contracts.mssql_tds_coordinator_evidence import (
    TdsCoordinatorEvidenceKind as Kind,
)
from dpone.contracts.mssql_tds_coordinator_evidence import (
    TdsCoordinatorEvidenceObservation,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsChildExit
from dpone.ports.mssql_tds_coordinator import AssertCoordinatorAuthority, CoordinatorObservation
from tests.test_mssql_tds_coordinator_request import values


class Harness:
    """Deterministic ports; SQL result fixtures are explicitly synthetic."""

    def __init__(self):
        credentials, self.startup, self.authority, _, self.proof = values()
        self.request, self.material, self.profile = (
            credentials.request,
            credentials.connection_material,
            credentials.driver_profile,
        )
        self.identity, self.owner = credentials.identity, credentials.execution_owner
        self.current = self.durable = TdsCoordinatorSnapshot(
            TdsCoordinatorState(self.identity, self.owner, self.owner, TdsCoordinatorPhase.INTENT, 0), 1
        )
        self.events, self.deadlines, self.saved = [], [], {}
        self.fail_at = None
        self.hook = lambda label: None
        self.now = 10.0
        self.lost = False
        self.grant = self.received_result = None
        self.startup_receipt = self.startup
        self.exit_code = 0
        self.failed_result = False
        self.mutate_authority = lambda value: value
        self.mutate_response = lambda value: value
        self.pool = TdsActorPool(capacity=2, clock=lambda: self.now)
        self.evidence_observation = TdsCoordinatorEvidenceObservation(self.proof.operation_sha256)
        pin = TdsBinaryPin(Path("/admitted/binary"), "a" * 64)
        self.admission = encode_connection_admission(TdsCoordinatorBuild(pin, pin, pin, pin), self.profile)
        self.launcher = SimpleNamespace(admission_sha256=sha256(self.admission).hexdigest(), spawn=self.spawn)
        self.child = SimpleNamespace(
            identity=self.startup.process,
            startup_receipt=self.startup,
            startup=self.observe_startup,
            deliver_credentials=self.credentials,
            observe_authority=self.observe_authority,
            deliver_grant=self.deliver_grant,
            receive_result=self.receive_result,
            received_result=None,
            wait=self.wait,
            terminate=self.terminate,
            close=self.close_child,
        )
        self.writer = SimpleNamespace(
            execute=self.execute,
            observation=CoordinatorObservation(self.current),
            close=lambda **kw: self.hit("writer.close", kw["deadline"]),
        )
        self.evidence = SimpleNamespace(
            write=self.write,
            observation=self.evidence_observation,
            close=lambda **kw: self.hit("evidence.close", kw["deadline"]),
        )

    def hit(self, label, deadline=None):
        self.events.append(label)
        if deadline is not None:
            self.deadlines.append((label, deadline))
            if self.now >= deadline:
                raise TimeoutError
        self.hook(label)
        if self.fail_at == label:
            raise WindowOutcomeUnknown("synthetic_ack_lost")

    def execute(self, command, *, deadline):
        if self.lost or self.durable != self.current:
            self.hit("lease_lost", deadline)
            raise WindowOutcomeUnknown("lost")
        if isinstance(command, AssertCoordinatorAuthority):
            self.hit("assert:" + self.current.state.phase.value, deadline)
        else:
            expected = advance_coordinator_state(
                self.current.state, command.event, expected_phase=command.expected_phase
            )
            self.durable = TdsCoordinatorSnapshot(expected, self.current.revision + (expected != self.current.state))
            self.hit("ack:" + type(command.event).__name__, deadline)
            self.current = self.durable
            self.writer.observation = CoordinatorObservation(self.current)
        return self.current

    def write(self, record, *, deadline):
        self.saved[record.kind] = record
        self.hit("evidence:" + record.kind.value, deadline)
        self.evidence.observation = TdsCoordinatorEvidenceObservation(record.operation_sha256, record.receipt)
        return record.receipt

    def spawn(self, **kw):
        self.hit("spawn", kw["startup_deadline"])
        return self.child

    def observe_startup(self, *, deadline):
        self.hit("startup", deadline)
        return self.startup

    def credentials(self, body, *, deadline):
        self.hit("credentials", deadline)
        value = decode_credentials(body, startup=self.startup, profile=self.profile)
        assert value.execution_owner == self.owner and value.request == self.request
        self.authority = replace(self.authority, session=replace(self.authority.session, nonce=value.session_nonce))

    def observe_authority(self, *, deadline):
        self.hit("authority", deadline)
        self.authority = self.mutate_authority(self.authority)
        return encode_authority(self.authority)

    def deliver_grant(self, body, *, deadline):
        self.hit("grant", deadline)
        self.grant = decode_grant(body, identity=self.identity, authority=self.authority)
        self.proof = replace(
            self.proof,
            session=self.authority.session,
            authority_sha256=authority_digest(self.authority),
            grant_sha256=coordinator_grant_digest(self.grant),
        )

    def receive_result(self, *, deadline):
        response = (
            failed_response(self.identity, self.grant, self.authority, TdsAttemptError.DRIVER)
            if self.failed_result
            else successful_response(self.proof)
        )
        self.received_result = self.child.received_result = encode_create_response(self.mutate_response(response))
        self.hit("result", deadline)
        return self.received_result

    def wait(self, *, deadline):
        self.hit("wait", deadline)
        return TdsChildExit(self.startup.process, self.exit_code, True)

    def terminate(self, *, deadline):
        self.hit("terminate", deadline)
        return TdsChildExit(self.startup.process, -9, True)

    def close_child(self):
        self.hit("child.close")

    def supply(self):
        self.hit("supplier")
        return self.material

    def run(self, **kwargs):
        return run_tds_coordinator(
            self.writer,
            self.evidence,
            self.launcher,
            self.request,
            self.admission,
            self.supply,
            pool=self.pool,
            operation_deadline=100.0,
            startup_timeout=10.0,
            termination_timeout=5.0,
            clock=lambda: self.now,
            **kwargs,
        )


def test_success_orders_every_durable_barrier_and_never_sets_remote():
    h = Harness()
    outcome = h.run()
    assert h.events == [
        "assert:intent",
        "evidence:create_request",
        "evidence:admission",
        "assert:intent",
        "spawn",
        "startup",
        "evidence:registration",
        "ack:CoordinatorProcessRegistered",
        "ack:CoordinatorCredentialIntent",
        "supplier",
        "assert:credential_intent",
        "credentials",
        "authority",
        "evidence:authority",
        "ack:CoordinatorSessionRegistered",
        "ack:CoordinatorGrantIntent",
        "assert:grant_intent",
        "grant",
        "result",
        "evidence:result",
        "wait",
        "child.close",
        "assert:grant_intent",
        "ack:CoordinatorResultReceived",
        "evidence:local_exit",
        "ack:CoordinatorLocalObserved",
        "evidence.close",
        "writer.close",
    ]
    assert outcome.response.evidence == h.proof and outcome.local_exit.exit_code == 0
    assert len(outcome.receipts) == 6 and set(h.saved) == set(Kind)
    assert h.current.state.result == outcome.response.result and h.current.state.local is not None
    assert h.current.state.remote is None and h.current.state.error is None
    assert "privatepassword" not in repr(outcome) and "privatepassword" not in repr(h.saved)
    assert dict(h.deadlines)["credentials"] == 20.0 and dict(h.deadlines)["grant"] == 100.0


@pytest.mark.parametrize(
    "fault,forbidden",
    [
        ("evidence:create_request", "spawn"),
        ("evidence:admission", "spawn"),
        ("evidence:registration", "credentials"),
        ("ack:CoordinatorProcessRegistered", "credentials"),
        ("ack:CoordinatorCredentialIntent", "credentials"),
        ("evidence:authority", "grant"),
        ("ack:CoordinatorSessionRegistered", "grant"),
        ("ack:CoordinatorGrantIntent", "grant"),
    ],
)
def test_failed_ack_never_opens_next_effect(fault, forbidden):
    h = Harness()
    h.fail_at = fault
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run()
    assert forbidden not in h.events
    if "spawn" in h.events:
        assert "terminate" in h.events
        index = h.events.index(fault)
        assert h.events[index + 1] == "terminate"


@pytest.mark.parametrize("binding", ["owner", "supervisor", "nonce", "process", "source", "operation"])
def test_full_authority_binding_rejected_before_persistence_or_grant(binding):
    h = Harness()

    def mutate(value):
        if binding == "owner":
            return replace(value, execution_owner=replace(value.execution_owner, owner="other"))
        if binding == "supervisor":
            return replace(
                value,
                execution_owner=replace(value.execution_owner, supervisor_id="00000000-0000-0000-0000-000000000099"),
            )
        if binding == "nonce":
            return replace(value, session=replace(value.session, nonce=b"z" * 32))
        if binding == "process":
            return replace(value, process=replace(value.process, pid=999))
        if binding == "source":
            return replace(value, implementation_sha256="f" * 64)
        return replace(value, operation_sha256="f" * 64)

    h.mutate_authority = mutate
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run()
    assert Kind.AUTHORITY not in h.saved and "grant" not in h.events


def test_failed_result_is_locally_contained_then_result_and_local_ack_in_valid_order():
    h = Harness()
    h.failed_result = True
    with pytest.raises(TdsCoordinatorFailure) as caught:
        h.run()
    trace = h.events[h.events.index("result") + 1 :]
    assert trace[0] == "terminate"
    assert trace.index("ack:CoordinatorResultReceived") < trace.index("ack:CoordinatorLocalObserved")
    assert caught.value.retained.response.failure.error is TdsAttemptError.DRIVER
    assert h.current.state.remote is None and h.current.state.error is TdsAttemptError.DRIVER


def test_recovering_intent_cannot_launch():
    from dpone.contracts.mssql_tds_coordinator import take_over_coordinator_state

    h = Harness()
    recovered = take_over_coordinator_state(
        h.current.state, replace(h.owner, fence=h.owner.fence + 1, supervisor_id="00000000-0000-0000-0000-000000000098")
    )
    h.writer.observation = CoordinatorObservation(TdsCoordinatorSnapshot(recovered, 2))
    with pytest.raises(ValueError, match="intent_required"):
        h.run()
    assert not h.events


@pytest.mark.parametrize("binding", ["process", "source", "receipt"])
def test_startup_mismatch_never_produces_registration_or_credentials(binding):
    h = Harness()
    if binding == "process":
        h.startup = replace(h.startup, process=replace(h.startup.process, pid=999))
        h.child.startup_receipt = h.startup
    elif binding == "source":
        h.startup = replace(h.startup, implementation_sha256="f" * 64)
        h.child.startup_receipt = h.startup
    else:
        h.child.startup_receipt = None
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run()
    assert Kind.REGISTRATION not in h.saved and "credentials" not in h.events


def test_unbound_admission_fails_before_any_io():
    h = Harness()
    h.launcher.admission_sha256 = "f" * 64
    with pytest.raises(ValueError, match="admission_binding"):
        h.run()
    assert not h.events


def test_supplier_budget_expiry_cannot_release_credentials():
    h = Harness()
    h.hook = lambda label: setattr(h, "now", 21.0) if label == "supplier" else None
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    assert "credentials" not in h.events and caught.value.retained.containment_deadline == 26.0


def test_same_fence_wrong_current_owner_never_launches():
    h = Harness()
    # Normal contract cannot even represent changed owner without takeover.
    with pytest.raises(ValueError):
        replace(h.current.state, ownership=replace(h.owner, owner="other"))
    assert not h.events


@pytest.mark.parametrize(
    "field,value", [("name", "other_database"), ("database_id", 123456), ("database_guid", UUID(int=99))]
)
def test_expected_database_rejects_before_authority_ack_or_create_grant(field, value):
    h = Harness()
    expected = replace(h.authority.database, **{field: value})
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run(_expected_database=expected)
    assert "grant" not in h.events
    assert "evidence:authority" not in h.events
    assert "terminate" in h.events


def test_expected_database_accepts_original_observation():
    h = Harness()
    assert h.run(_expected_database=replace(h.authority.database)).local_exit.reaped


def test_expected_schema_rejects_before_authority_ack_or_create_grant():
    h = Harness()
    expected = replace(h.authority.database)
    h.mutate_authority = lambda value: replace(
        value, schema_observation=replace(value.schema_observation, name="other_schema")
    )
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run(_expected_database=expected)
    assert "grant" not in h.events
    assert "evidence:authority" not in h.events
    assert "terminate" in h.events


def test_expected_database_nested_uuid_snapshot_precedes_spawn():
    h = Harness()
    expected = replace(h.authority.database, database_guid=UUID(str(h.authority.database.database_guid)))
    h.hook = lambda label: object.__setattr__(expected.database_guid, "int", 99) if label == "spawn" else None
    h.mutate_authority = lambda value: replace(value, database=replace(value.database, database_guid=UUID(int=99)))
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run(_expected_database=expected)
    assert "grant" not in h.events
    assert "evidence:authority" not in h.events


@pytest.mark.parametrize("value", [True, 1.0, -1, 2**128])
def test_expected_database_nested_uuid_alias_rejects_before_effects(value):
    h = Harness()
    expected = replace(h.authority.database, database_guid=UUID(int=5))
    object.__setattr__(expected.database_guid, "int", value)
    with pytest.raises(ValueError):
        h.run(_expected_database=expected)
    assert not h.events
