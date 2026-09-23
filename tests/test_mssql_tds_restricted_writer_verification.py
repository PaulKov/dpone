from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest

from dpone.app.mssql_sqlclient_restricted_writer_verify_request import (
    RestrictedWriterCredentialSupplier,
    RestrictedWriterVerificationOperations,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    RestrictedWriterVerifyRegistration,
    RestrictedWriterVerifyReservation,
    verify_request_digest,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_codec import encode_verify_request
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_evidence import RestrictedWriterVerifyEvidenceObservation
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_handshake import RestrictedWriterVerifyOpening
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorSnapshot,
    advance_coordinator_state,
    initial_coordinator_state,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership
from dpone.contracts.strict_json import strict_json_object
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from dpone.services.mssql_tds_restricted_writer_verification import (
    RestrictedWriterVerifyLocalUnknown,
    RestrictedWriterVerifyOrigin,
    RestrictedWriterVerifyRetained,
    verify_restricted_writer,
)
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_result
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request
from tests.test_mssql_tds_coordinator import PROCESS, identity, reservation
from tests.test_mssql_tds_directory_journal import OWNER


class Association(RestrictedWriterVerifyOrigin):
    def __init__(self):
        self.reservation = None
        self.reserve_count = 0
        self._verify_identity = identity()

    def reserve_verify(self, request, *, request_sha256, deadline):
        self.reserve_count += 1
        self.reservation = RestrictedWriterVerifyReservation(request.operation_id, request_sha256, "b" * 64, OWNER)
        return self.reservation

    def assert_verify_reservation(self, reservation, request, *, deadline):
        if reservation is not self.reservation or reservation.operation_id != request.operation_id:
            raise ValueError


class Evidence:
    def __init__(self, *, substitute=False, fault_at=None, mismatch_at=None, rewrite_prefix=False):
        self.receipts = ()
        self.substitute = substitute
        self.fault_at = fault_at
        self.mismatch_at = mismatch_at
        self.rewrite_prefix = rewrite_prefix
        self.records = []

    @property
    def observation(self):
        return RestrictedWriterVerifyEvidenceObservation(launch_request().request.operation_id, self.receipts)

    def write(self, record, *, deadline):
        self.records.append(record)
        receipt = record.receipt
        if len(self.receipts) == self.fault_at:
            return receipt
        changed = self.substitute or len(self.receipts) == self.mismatch_at
        if self.rewrite_prefix:
            self.receipts = tuple(replace(previous) for previous in self.receipts)
        self.receipts += (replace(receipt) if changed else receipt,)
        return receipt


class Process:
    def __init__(self, reservation, request, events, *, substitute=False, exit_fault=None, result_fault=False):
        self.reservation, self.events, self.substitute = reservation, events, substitute
        self.request = request
        self.exit_fault = exit_fault
        self.result_fault = result_fault
        self.send_count = 0
        self.scrub_count = 0
        self.cached_credentials = None
        self.credential_public_sha256 = None

    def registration(self, *, deadline):
        self.events.append("registration")
        reservation = replace(self.reservation) if self.substitute else self.reservation
        return RestrictedWriterVerifyRegistration(reservation, PROCESS, OWNER)

    def send_credentials(self, payload, *, deadline):
        self.events.append("credentials")
        self.send_count += 1
        self.cached_credentials = bytes(payload)
        self.credential_public_sha256 = strict_json_object(payload)["public_sha256"]

    def scrub_credentials(self, payload):
        self.scrub_count += 1
        assert not any(payload)
        self.cached_credentials = None

    def assert_credentials_scrubbed(self):
        if self.cached_credentials is not None:
            raise ValueError

    def writer_session(self, *, deadline):
        self.events.append("opening")
        request_sha256 = verify_request_digest(encode_verify_request(self.request))
        self.opening = RestrictedWriterVerifyOpening(request_sha256, verify_result(self.request).opening)
        return self.opening

    def authorize_probe(self, opening, *, deadline):
        assert opening is self.opening
        self.events.append("authorized")

    def result(self, *, deadline):
        self.events.append("result")
        if self.result_fault:
            raise RuntimeError("private result failure")
        return verify_result(self.request)

    def require_eof_and_zero(self, *, deadline):
        self.events.append("exit")
        return SimpleNamespace(
            identity=replace(PROCESS, pid=999) if self.exit_fault == "substituted" else PROCESS,
            reaped=self.exit_fault != "unreaped",
            exit_code=1 if self.exit_fault == "nonzero" else 0,
        )

    def cleanup(self):
        self.events.append("cleanup")


class Launcher:
    def __init__(self, events, *, substitute=False, exit_fault=None, result_fault=False):
        self.events, self.substitute, self.exit_fault, self.result_fault = events, substitute, exit_fault, result_fault
        self.last_process = None

    def launch(self, inputs, reservation, *, public_payload):
        self.events.append("launch")
        self.public_payload = public_payload
        self.last_process = Process(
            reservation,
            inputs.request,
            self.events,
            substitute=self.substitute,
            exit_fault=self.exit_fault,
            result_fault=self.result_fault,
        )
        return self.last_process


class Coordinator:
    def __init__(self, association, events):
        self.events = events
        self.close_count = 0
        state = initial_coordinator_state(identity(), reservation(), OWNER)
        state = replace(state, identity=association._verify_identity)
        self.observation = SimpleNamespace(snapshot=TdsCoordinatorSnapshot(state, 1))

    def execute(self, request, *, deadline):
        self.events.append(type(request.event).__name__)
        state = advance_coordinator_state(
            self.observation.snapshot.state,
            request.event,
            expected_phase=request.expected_phase,
        )
        self.observation.snapshot = TdsCoordinatorSnapshot(state, self.observation.snapshot.revision + 1)
        return self.observation.snapshot

    def close(self, *, deadline):
        self.close_count += 1


def execute(*, evidence=None, launcher=None, association=None, supplier=None, launch=None, coordinator_factory=None):
    events = []
    launch = launch_request() if launch is None else launch
    association = Association() if association is None else association
    evidence = Evidence() if evidence is None else evidence
    launcher = Launcher(events) if launcher is None else launcher
    supplier = supplier or RestrictedWriterCredentialSupplier(
        lambda: (
            TdsConnectionMaterial("localhost", 1433, launch.request.stage.database_name, "writer", "secret-value"),
            b"n" * 32,
        )
    )
    coordinator = None

    def default_coordinator_factory():
        nonlocal coordinator
        coordinator = Coordinator(association, events)
        return coordinator

    result = verify_restricted_writer(
        association,
        evidence,
        launcher,
        launch,
        supplier,
        coordinator_factory or default_coordinator_factory,
        RestrictedWriterVerificationOperations(),
        deadline=300.0,
        clock=lambda: 1.0,
    )
    return result, association, evidence, events


def test_exact_happy_trace_returns_only_retained_owner():
    retained, association, evidence, events = execute()
    request_bytes = encode_verify_request(launch_request().request)
    expected_digest = verify_request_digest(request_bytes)
    assert type(retained) is RestrictedWriterVerifyRetained
    retained.assert_retained()
    assert association.reserve_count == 1
    assert association.reservation.request_sha256 == expected_digest
    assert strict_json_object(evidence.records[0].payload)["request_sha256"] == expected_digest
    assert expected_digest != sha256(request_bytes).hexdigest()
    assert len(evidence.receipts) == 6
    assert events == [
        "launch",
        "registration",
        "CoordinatorProcessRegistered",
        "CoordinatorCredentialIntent",
        "credentials",
        "opening",
        "CoordinatorSessionRegistered",
        "authorized",
        "result",
        "exit",
    ]
    assert not hasattr(retained, "write") and not hasattr(retained, "bulk")


def test_invalid_initial_coordinator_is_closed_once_before_helper_launch():
    association, evidence, events = Association(), Evidence(), []

    class InvalidCoordinator:
        observation = SimpleNamespace(snapshot=object())

        def __init__(self):
            self.close_count = 0

        def close(self, *, deadline):
            self.close_count += 1

    coordinator = InvalidCoordinator()
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        verify_restricted_writer(
            association,
            evidence,
            Launcher(events),
            launch_request(),
            RestrictedWriterCredentialSupplier(
                lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret"), b"n" * 32)
            ),
            lambda: coordinator,
            RestrictedWriterVerificationOperations(),
            deadline=300.0,
            clock=lambda: 1.0,
        )
    assert coordinator.close_count == 1 and events == []


def test_durable_equal_but_distinct_execution_owner_is_admitted():
    association, evidence, events = Association(), Evidence(), []
    coordinator = Coordinator(association, events)
    reconstructed = TdsAttemptOwnership(OWNER.owner, OWNER.fence, OWNER.supervisor_id)
    assert reconstructed == OWNER and reconstructed is not OWNER
    snapshot = coordinator.observation.snapshot
    coordinator.observation.snapshot = replace(
        snapshot,
        state=replace(snapshot.state, execution_owner=reconstructed, ownership=reconstructed),
    )

    retained, _, _, _ = execute(
        association=association,
        evidence=evidence,
        launcher=Launcher(events),
        coordinator_factory=lambda: coordinator,
    )
    retained.assert_retained()


def test_unequal_reconstructed_execution_owner_fails_before_launch_or_evidence():
    association, evidence, events = Association(), Evidence(), []
    coordinator = Coordinator(association, events)
    foreign = TdsAttemptOwnership("foreign-owner", OWNER.fence, OWNER.supervisor_id)
    snapshot = coordinator.observation.snapshot
    coordinator.observation.snapshot = replace(
        snapshot,
        state=replace(snapshot.state, execution_owner=foreign, ownership=foreign),
    )

    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(
            association=association,
            evidence=evidence,
            launcher=Launcher(events),
            coordinator_factory=lambda: coordinator,
        )
    assert coordinator.close_count == 1
    assert events == [] and evidence.records == []


def test_equal_observation_gateway_wrapper_is_sticky_unknown_and_replay_has_no_effects():
    retained, _, _, events = execute()
    custody = retained._owner._coordinator_custody
    exact = custody.gateway
    foreign_effects = []
    foreign = SimpleNamespace(
        observation=exact.observation,
        execute=lambda *args, **kwargs: foreign_effects.append("execute"),
        close=lambda **kwargs: foreign_effects.append("close"),
    )
    for name in ("gateway", "_gateway_ref", "_VerifyCoordinatorCustody__gateway"):
        with pytest.raises(AttributeError):
            setattr(custody, name, foreign)
        with pytest.raises((AttributeError, TypeError)):
            object.__setattr__(custody, name, foreign)
    with pytest.raises(TypeError):
        deepcopy(custody)

    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        retained.assert_retained()
    before = list(events)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        retained.assert_retained()

    assert exact.close_count == 1
    assert foreign_effects == []
    assert events == before


def test_retained_public_projection_exposes_no_request_or_raw_name_canaries():
    base = launch_request()
    canaries = (
        "p9-canary-login-47291",
        "p9-canary-user-58302",
        "p9-canary-database-69413",
        "p9-canary-schema-70524",
        "p9-canary-table-81635",
        "p9-canary-column-92746",
    )
    login, user, database, schema, table, column = canaries
    request = replace(
        base.request,
        parent=replace(base.request.parent, database=database, schema=schema, table=table),
        writer_login=replace(base.request.writer_login, name=login, original_name=login),
        writer=replace(base.request.writer, name=user),
        login_token=tuple(replace(value, name=login) for value in base.request.login_token),
        user_token=tuple(replace(value, name=user) for value in base.request.user_token),
        stage=replace(
            base.request.stage,
            database_name=database,
            schema_name=schema,
            table_name=table,
            columns=tuple(replace(value, name=column) for value in base.request.stage.columns),
        ),
    )
    retained, _, _, _ = execute(launch=replace(base, request=request))
    public_names = tuple(name for name in dir(retained) if not name.startswith("_"))
    public_projection = repr((retained.reservation, retained.result, retained.receipts, public_names))
    assert not hasattr(retained, "request") and "request" not in public_names
    assert all(canary not in repr(retained) and canary not in public_projection for canary in canaries)


def test_equal_value_reservation_substitution_is_unknown_before_credentials():
    events = []
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(launcher=Launcher(events, substitute=True))
    assert events == ["launch", "registration", "cleanup"]


def test_equal_value_evidence_ack_substitution_is_unknown():
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(evidence=Evidence(substitute=True))


def test_rewriting_every_prior_receipt_identity_is_sticky_unknown():
    evidence = Evidence(rewrite_prefix=True)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(evidence=evidence)
    assert len(evidence.receipts) == 2


def test_retained_owner_reasserts_every_receipt_identity():
    retained, _, evidence, _ = execute()
    evidence.receipts = tuple(replace(receipt) for receipt in evidence.receipts)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        retained.assert_retained()


def test_association_substitution_after_reservation_is_sticky_unknown():
    association = Association()
    original = association.reserve_verify

    def reserve(*args, **kwargs):
        result = original(*args, **kwargs)
        association.reservation = replace(result)
        return result

    association.reserve_verify = reserve
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(association=association)


def test_supplier_runs_only_after_fourth_durable_ack_and_is_not_retained():
    evidence = Evidence()
    observed = []
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (
            observed.append(len(evidence.receipts))
            or TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"),
            b"n" * 32,
        )
    )
    retained, _, _, _ = execute(evidence=evidence, supplier=supplier)
    assert observed == [4] and supplier.consumed
    assert all(value is not supplier for value in vars(retained._owner).values())


@pytest.mark.parametrize("fault_at", range(6))
def test_lost_ack_at_each_boundary_is_sticky_and_never_retried(fault_at):
    evidence = Evidence(fault_at=fault_at)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(evidence=evidence)
    assert len(evidence.receipts) == fault_at


@pytest.mark.parametrize("fault_at", range(6))
def test_equal_value_ack_substitution_at_each_boundary_is_rejected(fault_at):
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(evidence=Evidence(mismatch_at=fault_at))


@pytest.mark.parametrize("fault", ("nonzero", "unreaped", "substituted"))
def test_invalid_exit_cannot_produce_retained_owner(fault):
    events = []
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(launcher=Launcher(events, exit_fault=fault))
    assert events.count("credentials") == 1 and events.count("exit") == 1 and events.count("cleanup") == 1


def test_structural_fake_origin_is_rejected_before_reservation():
    fake = SimpleNamespace(reserve_verify=lambda *a, **k: None, assert_verify_reservation=lambda *a, **k: None)
    with pytest.raises(ValueError):
        execute(association=fake)


def test_unknown_owner_does_not_retain_supplier_or_private_material():
    calls = []
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (
            calls.append(True) or TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"),
            b"n" * 32,
        )
    )
    events = []
    with pytest.raises(RestrictedWriterVerifyLocalUnknown) as caught:
        execute(launcher=Launcher(events, result_fault=True), supplier=supplier)
    assert calls == [True] and supplier.consumed
    assert all(value is not supplier for value in vars(caught.value.retained).values())
    assert "secret-value" not in repr(vars(caught.value.retained))


def test_post_launch_failure_sends_credentials_and_cleanup_at_most_once():
    events = []
    launcher = Launcher(events, exit_fault="nonzero")
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        execute(launcher=launcher)
    assert launcher.last_process.send_count == 1
    assert launcher.last_process.scrub_count == 1
    assert events.count("cleanup") == 1


def test_success_and_unknown_process_graphs_do_not_retain_private_frame():
    retained, _, _, _ = execute()
    assert retained._owner._process.cached_credentials is None
    events = []
    launcher = Launcher(events, result_fault=True)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown) as caught:
        execute(launcher=launcher)
    error = caught.value
    assert error.__traceback__ is not None
    assert error.__cause__ is None and error.__context__ is None
    assert launcher.last_process.cached_credentials is None
    assert b"secret-value" not in repr(vars(error.retained)).encode()
    traceback = error.__traceback__
    while traceback is not None:
        if "/src/dpone/" in traceback.tb_frame.f_code.co_filename:
            assert "secret-value" not in repr(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next


def test_launcher_unknown_retains_exact_unresolved_capability():
    class Unresolved:
        def __init__(self):
            self.contain_count = self.close_count = 0

        def contain(self, *, deadline):
            assert deadline == 2.0
            self.contain_count += 1

        def close(self):
            self.close_count += 1

    unresolved = Unresolved()

    class UnknownLauncher:
        def launch(self, inputs, reservation, *, public_payload):
            del inputs, reservation, public_payload
            raise TdsLaunchUnknown(unresolved)

    with pytest.raises(RestrictedWriterVerifyLocalUnknown) as caught:
        execute(launcher=UnknownLauncher())
    caught.value.retained.contain_unknown(deadline=2.0)
    assert caught.value.retained._unresolved_launch is unresolved
    assert unresolved.contain_count == unresolved.close_count == 1
    assert caught.value.__cause__ is None and caught.value.__context__ is None


def test_lost_containment_ack_is_sticky_and_never_retried():
    class Unresolved:
        def __init__(self):
            self.contain_count = self.close_count = 0

        def contain(self, *, deadline):
            del deadline
            self.contain_count += 1
            raise RuntimeError("lost containment acknowledgement")

        def close(self):
            self.close_count += 1

    unresolved = Unresolved()

    class UnknownLauncher:
        def launch(self, inputs, reservation, *, public_payload):
            del inputs, reservation, public_payload
            raise TdsLaunchUnknown(unresolved)

    with pytest.raises(RestrictedWriterVerifyLocalUnknown) as caught:
        execute(launcher=UnknownLauncher())
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        caught.value.retained.contain_unknown(deadline=2.0)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        caught.value.retained.contain_unknown(deadline=2.0)
    assert unresolved.contain_count == 1 and unresolved.close_count == 0


def test_external_deadline_bounds_registration_and_all_operation_effects():
    deadlines = []

    class DeadlineProcess(Process):
        def registration(self, *, deadline):
            deadlines.append(deadline)
            return super().registration(deadline=deadline)

        def send_credentials(self, payload, *, deadline):
            deadlines.append(deadline)
            return super().send_credentials(payload, deadline=deadline)

        def result(self, *, deadline):
            deadlines.append(deadline)
            return super().result(deadline=deadline)

        def require_eof_and_zero(self, *, deadline):
            deadlines.append(deadline)
            return super().require_eof_and_zero(deadline=deadline)

    class DeadlineLauncher(Launcher):
        def launch(self, inputs, reservation, *, public_payload):
            self.public_payload = public_payload
            self.last_process = DeadlineProcess(reservation, inputs.request, self.events)
            return self.last_process

    launch = launch_request()
    deadline = 75.0
    events = []
    association = Association()
    retained = verify_restricted_writer(
        association,
        Evidence(),
        DeadlineLauncher(events),
        launch,
        RestrictedWriterCredentialSupplier(
            lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
        ),
        lambda: Coordinator(association, events),
        RestrictedWriterVerificationOperations(),
        deadline=deadline,
        clock=lambda: 1.0,
    )
    retained.assert_retained()
    assert deadlines == [deadline] * 4


def test_public_payload_is_captured_once_for_evidence_launch_and_credentials(monkeypatch):
    launch = launch_request()
    original = type(launch).public_payload
    calls = []

    def alternating(value):
        calls.append(True)
        return original(value) if len(calls) == 1 else b"substituted-public-payload"

    monkeypatch.setattr(type(launch), "public_payload", alternating)
    events = []
    evidence = Evidence()
    launcher = Launcher(events)
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
    )
    association = Association()
    retained = verify_restricted_writer(
        association,
        evidence,
        launcher,
        launch,
        supplier,
        lambda: Coordinator(association, events),
        RestrictedWriterVerificationOperations(),
        deadline=300.0,
        clock=lambda: 1.0,
    )
    retained.assert_retained()
    digest = sha256(launcher.public_payload).hexdigest()
    assert calls == [True]
    assert strict_json_object(evidence.records[1].payload)["public_sha256"] == digest
    assert launcher.last_process.credential_public_sha256 == digest
