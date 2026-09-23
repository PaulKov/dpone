"""One original PREPARED attempt owns one fail-closed GRANT reservation."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from time import monotonic
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.app.mssql_sqlclient_restricted_writer_verify_composition import (
    bind_mssql_sqlclient_restricted_writer_verify,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_request import (
    RestrictedWriterCredentialSupplier,
    RestrictedWriterVerificationOperations,
)
from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown
from dpone.contracts.mssql_sqlclient_observation import SqlClientPrincipalResolution
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    RestrictedWriterVerifyRegistration,
    SqlClientEffectivePermission,
    SqlClientRestrictedWriterVerifyRequest,
    SqlClientTokenRow,
    verify_request_digest,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_codec import encode_verify_request
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_evidence import (
    RestrictedWriterVerifyEvidenceObservation,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorSnapshot,
    advance_coordinator_state,
    initial_coordinator_state,
)
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    record_local_containment,
    record_remote_settlement,
)
from dpone.contracts.mssql_tds_directory_codec import process_identity_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
from dpone.ports.mssql_tds_directory import (
    AssertDirectoryAuthority,
    RecordDirectoryContainment,
    RecordDirectorySettlement,
    ReserveDirectoryOperation,
)
from dpone.services.mssql_tds_permission_grant_association import PermissionGrantAssociation
from dpone.services.mssql_tds_restricted_writer_verification import verify_restricted_writer
from tests.test_mssql_sqlclient_observe_departure_composition import (
    composed_preparation as composed_preparation,
)
from tests.test_mssql_sqlclient_observe_departure_composition import (
    prepared as prepared,
)
from tests.test_mssql_sqlclient_observe_departure_composition import (
    scripted_helper,
)
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request
from tests.test_mssql_tds_attempt import create
from tests.test_mssql_tds_attempt import setup as setup
from tests.test_mssql_tds_coordinator import PROCESS
from tests.test_mssql_tds_restricted_writer_verification import Evidence, Launcher


def _settled(prepared, monkeypatch):
    from dpone.app.mssql_sqlclient_observe_departure_composition import settle_prepared_observe

    helper = scripted_helper(prepared, monkeypatch)
    origin = prepared.attempt._prepared_origin
    settle_prepared_observe(
        prepared.attempt,
        admitted_factory=prepared.factory,
        pool=prepared.pool,
        evidence_root=prepared.evidence_root,
        departure_launcher=helper.launcher,
        observer_admission=helper.admission,
        management_credentials=lambda: helper.material,
        deadline=origin.deadline,
        helper_startup_timeout=10.0,
        termination_timeout=10.0,
    )
    return prepared.attempt


def _grant(attempt):
    origin = attempt._prepared_origin
    return attempt._permission_grant(
        UUID(int=701), "a" * 64, origin.identity.implementation_sha256, deadline=origin.deadline
    )


def _settled_grant_for_verify(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    association = _grant(attempt).reserve()
    operation = association.identity
    process = attempt._prepared_origin.startup.process
    association.record_local_containment(
        TdsLocalContainment(
            attempt_identity_digest(operation.parent),
            operation.operation_id,
            process_identity_digest(process),
            "b" * 64,
        )
    )
    directory = association.record_remote_settlement(
        TdsRemoteSettlement(
            attempt_identity_digest(operation.parent),
            operation.operation_id,
            "c" * 64,
            "d" * 64,
        )
    )
    origin = attempt._prepared_origin
    observed = origin.handle.request
    writer = observed.writer_principal
    login = observed.writer_admission.login
    grant = SimpleNamespace(
        parent=origin.expected.state.identity,
        stage=observed.selected_stage,
        writer=SqlClientPrincipalResolution("mapped_user", writer.principal_id, writer.name, writer.sid),
        writer_login=login,
    )
    result = SimpleNamespace(request=grant)
    held = SimpleNamespace(result=result, _result_ref=result)
    settled = SimpleNamespace(directory=directory)
    remote_owner = SimpleNamespace(
        local=SimpleNamespace(held_owner=held),
        _settled_ref=settled,
        _unknown=False,
        _busy=False,
        phase="SETTLED",
        integrity=lambda: None,
    )
    association._remote_settlement_owner = remote_owner
    association._settled_capability = settled
    return attempt, association, settled, grant


def _verify_request(attempt, grant):
    effective = json.loads(attempt._prepared_origin.preparation_payload)["profile_open"]["PREP_EFFECTIVE"][0]
    return SqlClientRestrictedWriterVerifyRequest(
        parent=grant.parent,
        stage=grant.stage,
        writer=grant.writer,
        writer_login=grant.writer_login,
        login_token=tuple(SqlClientTokenRow(*row) for row in effective["login_token"]),
        user_token=tuple(SqlClientTokenRow(*row) for row in effective["user_token"]),
        server_permissions=tuple(SqlClientEffectivePermission(*row) for row in effective["server_permissions"]),
        database_permissions=tuple(SqlClientEffectivePermission(*row) for row in effective["database_permissions"]),
        operation_id=UUID(int=702),
        implementation_sha256="e" * 64,
    )


def _retained_verify(attempt, association, settled, grant):
    deadline = attempt._prepared_origin.deadline
    bound = bind_mssql_sqlclient_restricted_writer_verify(attempt, settled, deadline=deadline)
    request = _verify_request(attempt, grant)
    launch = replace(
        launch_request(),
        request=request,
        startup_deadline=deadline,
        operation_deadline=deadline,
    )
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (
            TdsConnectionMaterial(
                "localhost",
                1433,
                request.stage.database_name,
                request.writer_login.name,
                "secret",
            ),
            b"n" * 32,
        )
    )

    class ExactEvidence(Evidence):
        @property
        def observation(self):
            return RestrictedWriterVerifyEvidenceObservation(request.operation_id, self.receipts)

    class ExactLauncher(Launcher):
        def launch(self, inputs, reservation, *, public_payload):
            process = super().launch(inputs, reservation, public_payload=public_payload)

            def registration(*, deadline):
                self.events.append("registration")
                return RestrictedWriterVerifyRegistration(reservation, PROCESS, reservation.execution_owner)

            process.registration = registration
            return process

    events = []
    evidence = ExactEvidence()

    def coordinator():
        class ExactCoordinator:
            def __init__(self):
                state = initial_coordinator_state(
                    bound._verify_identity,
                    bound._verify_directory_ref.state,
                    bound._verify_reservation.execution_owner,
                )
                self.observation = SimpleNamespace(snapshot=TdsCoordinatorSnapshot(state, 1))

            def execute(self, request, *, deadline):
                events.append(type(request.event).__name__)
                state = advance_coordinator_state(
                    self.observation.snapshot.state,
                    request.event,
                    expected_phase=request.expected_phase,
                )
                self.observation.snapshot = TdsCoordinatorSnapshot(state, self.observation.snapshot.revision + 1)
                return self.observation.snapshot

            def close(self, *, deadline):
                return None

        return ExactCoordinator()

    return verify_restricted_writer(
        bound,
        evidence,
        ExactLauncher(events),
        launch,
        supplier,
        coordinator,
        RestrictedWriterVerificationOperations(),
        deadline=deadline,
        clock=lambda: 0.0,
    )


def test_verify_origin_retains_distinct_exact_request_and_evidence(prepared, monkeypatch):
    attempt, association, settled, grant_request = _settled_grant_for_verify(prepared, monkeypatch)
    grant_evidence = association._remote_settlement_owner.local.held_owner.result
    bound = bind_mssql_sqlclient_restricted_writer_verify(
        attempt,
        settled,
        deadline=attempt._prepared_origin.deadline,
    )
    assert bound is association
    assert association._verify_grant_ref is grant_evidence
    assert association._verify_grant_request_ref is grant_request
    association._assert_verify_origin(
        association._verify_directory_ref,
        deadline=attempt._prepared_origin.deadline,
        phase="bound",
    )


def test_exact_retained_verify_settles_same_directory_slot(prepared, monkeypatch):
    attempt, association, settled, grant = _settled_grant_for_verify(prepared, monkeypatch)
    retained = _retained_verify(attempt, association, settled, grant)
    deadline = attempt._prepared_origin.deadline
    association.assert_verify_settlement(retained, deadline=deadline)
    identity = association._verify_identity
    process = retained._owner._registration.process
    local = TdsLocalContainment(
        attempt_identity_digest(identity.parent),
        identity.operation_id,
        process_identity_digest(process),
        retained.receipts[-1].payload_sha256,
    )
    local_directory = association.record_verify_local_containment(local, deadline=deadline)
    remote = TdsRemoteSettlement(
        attempt_identity_digest(identity.parent),
        identity.operation_id,
        "c" * 64,
        "d" * 64,
    )
    final_directory = association.record_verify_remote_settlement(remote, deadline=deadline)
    slot = final_directory.state.slots[identity.slot_index]
    assert local_directory is association._verify_local_directory_ref
    assert final_directory is association._verify_remote_directory_ref
    assert slot.local_containment == local and slot.remote_settlement == remote
    retained.assert_retained()


def test_unbound_verify_call_is_defined_sticky_unknown_without_gateway():
    class Host:
        calls = 0

        def _permission_grant_unknown(self):
            return WindowOutcomeUnknown("mssql_native.test_verify_unbound")

    association = object.__new__(PermissionGrantAssociation)
    association._host = Host()
    with pytest.raises(WindowOutcomeUnknown, match="test_verify_unbound"):
        association.reserve_verify(object(), request_sha256="a" * 64, deadline=monotonic() + 1)
    assert association._verify_phase == "unknown"
    assert association._host.calls == 0


def test_real_prepared_attempt_reserves_one_original_grant(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    calls = []
    original = attempt._directory.execute

    def execute(request, *, deadline):
        if isinstance(request, ReserveDirectoryOperation):
            calls.append(request)
            assert attempt._permission_grant_owner is owner
            assert owner._phase == "reservation_attempted"
        return original(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", execute)
    owner = _grant(attempt)
    reservation = owner.reserve().reservation
    assert owner.identity.command is TdsCoordinatorCommand.GRANT
    assert reservation.state.slots[-1].operation_id == UUID(int=701)
    assert len(calls) == 1


def test_grant_association_records_exact_local_then_remote_settlement(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    association = _grant(attempt).reserve()
    operation = association.identity
    original = association.reservation
    process = attempt._prepared_origin.startup.process
    local = TdsLocalContainment(
        attempt_identity_digest(operation.parent),
        operation.operation_id,
        process_identity_digest(process),
        "b" * 64,
    )
    remote = TdsRemoteSettlement(attempt_identity_digest(operation.parent), operation.operation_id, "c" * 64, "d" * 64)
    calls = []
    execute = attempt._directory.execute

    def traced(request, *, deadline):
        if isinstance(request, (RecordDirectoryContainment, RecordDirectorySettlement)):
            calls.append(type(request))
        return execute(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", traced)
    local_ack = association.record_local_containment(local)
    remote_ack = association.record_remote_settlement(remote)

    assert calls == [RecordDirectoryContainment, RecordDirectorySettlement]
    assert local_ack.state == record_local_containment(original.state, operation.slot_index, local)
    assert remote_ack.state == record_remote_settlement(local_ack.state, operation.slot_index, remote)
    assert remote_ack.revision > local_ack.revision > original.revision
    assert attempt._directory.observation.snapshot == remote_ack


def test_exact_p8_settlement_reserves_one_digest_bound_verify(prepared, monkeypatch):
    attempt, association, settled, grant = _settled_grant_for_verify(prepared, monkeypatch)
    calls = []
    execute = attempt._directory.execute

    def traced(request, *, deadline):
        if isinstance(request, ReserveDirectoryOperation):
            calls.append(request)
        return execute(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", traced)
    deadline = monotonic() + 10.0
    bound = bind_mssql_sqlclient_restricted_writer_verify(attempt, settled, deadline=deadline)
    request = _verify_request(attempt, grant)
    payload = encode_verify_request(request)
    digest = verify_request_digest(payload)
    reservation = bound.reserve_verify(request, request_sha256=digest, deadline=deadline)

    assert association is bound
    assert reservation.operation_id == request.operation_id
    assert reservation.request_sha256 == digest
    assert reservation.execution_owner is attempt._directory.observation.snapshot.ownership
    assert calls[-1].command is TdsCoordinatorCommand.VERIFY
    assert [call.command for call in calls].count(TdsCoordinatorCommand.VERIFY) == 1
    bound.assert_verify_reservation(reservation, request, deadline=deadline)


def test_verify_rejects_substitute_settlement_before_reserve(prepared, monkeypatch):
    attempt, _, settled, _ = _settled_grant_for_verify(prepared, monkeypatch)
    calls = []
    execute = attempt._directory.execute

    def traced(request, *, deadline):
        if isinstance(request, ReserveDirectoryOperation):
            calls.append(request)
        return execute(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", traced)
    deadline = monotonic() + 10.0
    with pytest.raises(WindowOutcomeUnknown):
        bind_mssql_sqlclient_restricted_writer_verify(
            attempt,
            SimpleNamespace(directory=settled.directory),
            deadline=deadline,
        )
    assert calls == []


def test_verify_rejects_wrong_digest_before_reserve(prepared, monkeypatch):
    attempt, association, settled, grant = _settled_grant_for_verify(prepared, monkeypatch)
    calls = []
    execute = attempt._directory.execute

    def traced(request, *, deadline):
        if isinstance(request, ReserveDirectoryOperation):
            calls.append(request)
        return execute(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", traced)
    deadline = monotonic() + 10.0
    bound = bind_mssql_sqlclient_restricted_writer_verify(attempt, settled, deadline=deadline)
    request = _verify_request(attempt, grant)
    with pytest.raises(WindowOutcomeUnknown):
        bound.reserve_verify(request, request_sha256="f" * 64, deadline=deadline)
    assert calls == []
    assert association._verify_phase == "unknown"


def test_lost_verify_ack_is_sticky_and_never_replayed(prepared, monkeypatch):
    attempt, association, settled, grant = _settled_grant_for_verify(prepared, monkeypatch)
    deadline = monotonic() + 10.0
    bound = bind_mssql_sqlclient_restricted_writer_verify(attempt, settled, deadline=deadline)
    request = _verify_request(attempt, grant)
    digest = verify_request_digest(encode_verify_request(request))
    execute = attempt._directory.execute
    calls = []

    def lost(request_value, *, deadline):
        if (
            isinstance(request_value, ReserveDirectoryOperation)
            and request_value.command is TdsCoordinatorCommand.VERIFY
        ):
            calls.append(request_value)
            execute(request_value, deadline=deadline)
            raise OSError("lost ack")
        return execute(request_value, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", lost)
    with pytest.raises(WindowOutcomeUnknown):
        bound.reserve_verify(request, request_sha256=digest, deadline=deadline)
    with pytest.raises(WindowOutcomeUnknown):
        bound.reserve_verify(request, request_sha256=digest, deadline=deadline)
    assert len(calls) == 1
    assert association._verify_phase == "unknown"


@pytest.mark.parametrize("hook", ["validate_origin_request", "encode_request", "request_digest"])
def test_verify_callback_reentry_has_no_reservation_effect(prepared, monkeypatch, hook):
    attempt, association, settled, grant = _settled_grant_for_verify(prepared, monkeypatch)
    deadline = monotonic() + 10.0
    operations = RestrictedWriterVerificationOperations()
    bound = association.bind_restricted_writer_verify(settled, operations, deadline=deadline)
    request = _verify_request(attempt, grant)
    digest = verify_request_digest(encode_verify_request(request))
    original = getattr(operations, hook)
    entered = False
    calls = []
    execute = attempt._directory.execute

    def reenter(*args):
        nonlocal entered
        if not entered:
            entered = True
            bound.reserve_verify(request, request_sha256=digest, deadline=deadline)
        return original(*args)

    def traced(request_value, *, deadline):
        if (
            isinstance(request_value, ReserveDirectoryOperation)
            and request_value.command is TdsCoordinatorCommand.VERIFY
        ):
            calls.append(request_value)
        return execute(request_value, deadline=deadline)

    monkeypatch.setattr(operations, hook, reenter)
    monkeypatch.setattr(attempt._directory, "execute", traced)
    with pytest.raises(WindowOutcomeUnknown):
        bound.reserve_verify(request, request_sha256=digest, deadline=deadline)
    assert calls == []
    assert association._verify_phase == "unknown"


@pytest.mark.parametrize("substitute", ["request", "reservation"])
def test_ready_verify_substitutes_are_rejected_before_gateway(prepared, monkeypatch, substitute):
    attempt, association, settled, grant = _settled_grant_for_verify(prepared, monkeypatch)
    deadline = monotonic() + 10.0
    bound = bind_mssql_sqlclient_restricted_writer_verify(attempt, settled, deadline=deadline)
    request = _verify_request(attempt, grant)
    digest = verify_request_digest(encode_verify_request(request))
    reservation = bound.reserve_verify(request, request_sha256=digest, deadline=deadline)
    calls = []
    execute = attempt._directory.execute
    monkeypatch.setattr(
        attempt._directory,
        "execute",
        lambda request_value, *, deadline: calls.append(request_value) or execute(request_value, deadline=deadline),
    )
    checked_request = replace(request) if substitute == "request" else request
    checked_reservation = replace(reservation) if substitute == "reservation" else reservation
    with pytest.raises(WindowOutcomeUnknown):
        bound.assert_verify_reservation(checked_reservation, checked_request, deadline=deadline)
    assert calls == []
    assert association._verify_phase == "unknown"


def test_grant_association_lost_local_ack_is_sticky_and_never_replayed(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    association = _grant(attempt).reserve()
    operation = association.identity
    process = attempt._prepared_origin.startup.process
    local = TdsLocalContainment(
        attempt_identity_digest(operation.parent),
        operation.operation_id,
        process_identity_digest(process),
        "b" * 64,
    )
    execute = attempt._directory.execute
    calls = []

    def lost(request, *, deadline):
        if isinstance(request, RecordDirectoryContainment):
            calls.append(type(request))
            execute(request, deadline=deadline)
            raise OSError("lost ack")
        return execute(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", lost)
    with pytest.raises(WindowOutcomeUnknown):
        association.record_local_containment(local)
    with pytest.raises(WindowOutcomeUnknown):
        association.record_local_containment(local)
    assert calls == [RecordDirectoryContainment]


def test_grant_association_rejects_remote_before_local_without_directory_mutation(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    association = _grant(attempt).reserve()
    operation = association.identity
    remote = TdsRemoteSettlement(attempt_identity_digest(operation.parent), operation.operation_id, "c" * 64, "d" * 64)
    calls = []
    execute = attempt._directory.execute

    def traced(request, *, deadline):
        if isinstance(request, (RecordDirectoryContainment, RecordDirectorySettlement)):
            calls.append(type(request))
        return execute(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", traced)
    with pytest.raises(WindowOutcomeUnknown):
        association.record_remote_settlement(remote)
    assert calls == []


def test_grant_association_lost_remote_ack_is_sticky_and_never_replayed(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    association = _grant(attempt).reserve()
    operation = association.identity
    process = attempt._prepared_origin.startup.process
    local = TdsLocalContainment(
        attempt_identity_digest(operation.parent),
        operation.operation_id,
        process_identity_digest(process),
        "b" * 64,
    )
    remote = TdsRemoteSettlement(attempt_identity_digest(operation.parent), operation.operation_id, "c" * 64, "d" * 64)
    association.record_local_containment(local)
    execute = attempt._directory.execute
    calls = []

    def lost(request, *, deadline):
        if isinstance(request, RecordDirectorySettlement):
            calls.append(type(request))
            execute(request, deadline=deadline)
            raise OSError("lost ack")
        return execute(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", lost)
    with pytest.raises(WindowOutcomeUnknown):
        association.record_remote_settlement(remote)
    with pytest.raises(WindowOutcomeUnknown):
        association.record_remote_settlement(remote)
    assert calls == [RecordDirectorySettlement]


def test_generic_grant_rejects_before_directory_io(setup, monkeypatch):
    attempt = create(setup)
    calls = []
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: calls.append((a, k)))
    with pytest.raises(ValueError, match="ordinary_command_required"):
        attempt.reserve_operation(UUID(int=702), TdsCoordinatorCommand.GRANT, "a" * 64, deadline=monotonic() + 1)
    assert calls == []


@pytest.mark.parametrize("path", ["ordinary", "seal", "retire", "close_admission", "observe"])
def test_registered_owner_blocks_every_competing_forward_path_before_io(setup, monkeypatch, path):
    attempt = create(setup)
    owner = attempt._permission_grant_owner = object()
    calls = []
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: calls.append((a, k)))
    actions = {
        "ordinary": lambda: attempt.reserve_operation(
            UUID(int=703), TdsCoordinatorCommand.VERIFY, "a" * 64, deadline=monotonic() + 1
        ),
        "seal": lambda: attempt.seal_work(deadline=monotonic() + 1),
        "retire": lambda: attempt.reserve_retirement(UUID(int=704), "a" * 64, deadline=monotonic() + 1),
        "close_admission": lambda: attempt.close_admission(deadline=monotonic() + 1),
        "observe": lambda: attempt._begin_observe(UUID(int=705), object(), deadline=monotonic() + 1),
    }
    with pytest.raises(WindowContractError, match="permission_grant_active"):
        actions[path]()
    assert attempt._permission_grant_owner is owner and calls == []


def test_close_remains_available_while_association_exists(setup):
    attempt = create(setup)
    attempt._permission_grant_owner = object()
    attempt.close(deadline=monotonic() + 2)
    assert attempt._closed is True


@pytest.mark.parametrize(
    ("operation_id", "command_sha256", "implementation_sha256", "deadline_kind"),
    [
        (UUID(int=0), "a" * 64, "b" * 64, "future"),
        (UUID(int=1), "z" * 64, "b" * 64, "future"),
        (UUID(int=1), "a" * 64, "b" * 63, "future"),
        (UUID(int=1), "a" * 64, "b" * 64, "nan"),
        (UUID(int=1), "a" * 64, "b" * 64, "past"),
    ],
)
def test_invalid_scalars_install_no_owner_or_effect(
    setup, monkeypatch, operation_id, command_sha256, implementation_sha256, deadline_kind
):
    attempt = create(setup)
    deadline = {"future": monotonic() + 30, "nan": float("nan"), "past": monotonic() - 1}[deadline_kind]
    calls = []
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: calls.append((a, k)))
    with pytest.raises(ValueError):
        attempt._permission_grant(operation_id, command_sha256, implementation_sha256, deadline=deadline)
    assert attempt._permission_grant_owner is None and calls == []


@pytest.mark.parametrize(
    "failure",
    [
        "implementation",
        "deadline",
        "lifecycle",
        "directory",
        "origin_type",
        "expected_type",
        "settlement_type",
        "identity_implementation",
        "origin_deadline",
        "settlement_deadline",
        "flag",
        "attempt_state",
        "phase",
        "directory_state",
        "settlement_snapshot",
        "attempt_identity_leaf",
        "directory_limit_leaf",
        "directory_slot_leaf",
    ],
)
def test_exact_preflight_rejects_without_owner_callback_or_io(prepared, monkeypatch, failure):
    attempt = _settled(prepared, monkeypatch)
    origin, settlement = attempt._prepared_origin, attempt._observe_settlement
    lifecycle_snapshot, directory_observation = attempt._lifecycle._snapshot, attempt._directory._snapshot
    callbacks, io = [], []

    class Explosive:
        def __getattribute__(self, name):
            callbacks.append(name)
            raise AssertionError("preflight invoked a substituted property")

    class ExplosiveEquality:
        def __eq__(self, other):
            callbacks.append("eq")
            raise AssertionError("preflight invoked substituted equality")

    implementation_sha256, deadline = origin.identity.implementation_sha256, origin.deadline
    saved = (
        origin.identity.implementation_sha256,
        origin.deadline,
        settlement.deadline,
        origin.failed,
        origin.expected.state,
        origin.expected.state.phase,
        directory_observation.snapshot.state,
        settlement.directory,
        origin.expected.state.identity.target_key,
        directory_observation.snapshot.state.limits.max_entries,
        directory_observation.snapshot.state.slots[0].command_sha256,
    )
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: io.append((a, k)))
    if failure == "implementation":
        implementation_sha256 = "c" * 64
    elif failure == "deadline":
        deadline = min(origin.deadline, settlement.deadline) + 1.0
    elif failure == "lifecycle":
        attempt._lifecycle._snapshot = replace(lifecycle_snapshot, revision=lifecycle_snapshot.revision + 1)
    elif failure == "directory":
        changed = replace(directory_observation.snapshot, revision=directory_observation.snapshot.revision + 1)
        attempt._directory._snapshot = replace(directory_observation, snapshot=changed)
    elif failure == "origin_type":
        attempt._prepared_origin = Explosive()
    elif failure == "expected_type":
        origin.expected = Explosive()
    elif failure == "settlement_type":
        attempt._observe_settlement = Explosive()
    elif failure == "identity_implementation":
        object.__setattr__(origin.identity, "implementation_sha256", Explosive())
    elif failure == "origin_deadline":
        origin.deadline = Explosive()
    elif failure == "settlement_deadline":
        settlement.deadline = Explosive()
    elif failure == "flag":
        origin.failed = Explosive()
    elif failure == "attempt_state":
        object.__setattr__(origin.expected, "state", Explosive())
    elif failure == "phase":
        object.__setattr__(origin.expected.state, "phase", Explosive())
    elif failure == "directory_state":
        object.__setattr__(directory_observation.snapshot, "state", Explosive())
    elif failure == "settlement_snapshot":
        settlement.directory = Explosive()
    elif failure == "attempt_identity_leaf":
        object.__setattr__(origin.expected.state.identity, "target_key", ExplosiveEquality())
    elif failure == "directory_limit_leaf":
        object.__setattr__(directory_observation.snapshot.state.limits, "max_entries", ExplosiveEquality())
    else:
        object.__setattr__(directory_observation.snapshot.state.slots[0], "command_sha256", ExplosiveEquality())
    try:
        with pytest.raises(WindowContractError):
            attempt._permission_grant(UUID(int=707), "a" * 64, implementation_sha256, deadline=deadline)
        assert attempt._permission_grant_owner is None and callbacks == [] and io == []
    finally:
        attempt._prepared_origin, attempt._observe_settlement = origin, settlement
        origin.expected = lifecycle_snapshot
        attempt._lifecycle._snapshot, attempt._directory._snapshot = lifecycle_snapshot, directory_observation
        object.__setattr__(origin.identity, "implementation_sha256", saved[0])
        origin.deadline, settlement.deadline, origin.failed = saved[1:4]
        object.__setattr__(origin.expected, "state", saved[4])
        object.__setattr__(origin.expected.state, "phase", saved[5])
        object.__setattr__(directory_observation.snapshot, "state", saved[6])
        settlement.directory = saved[7]
        object.__setattr__(origin.expected.state.identity, "target_key", saved[8])
        object.__setattr__(directory_observation.snapshot.state.limits, "max_entries", saved[9])
        object.__setattr__(directory_observation.snapshot.state.slots[0], "command_sha256", saved[10])


@pytest.mark.parametrize("gateway_name", ["_lifecycle", "_directory"])
def test_registered_owner_rejects_substituted_gateway_without_callback_or_io(prepared, monkeypatch, gateway_name):
    attempt = _settled(prepared, monkeypatch)
    owner = _grant(attempt)
    original, callbacks = getattr(attempt, gateway_name), []

    class ExplosiveGateway:
        def __getattribute__(self, name):
            callbacks.append(name)
            raise AssertionError("substituted gateway callback")

    setattr(attempt, gateway_name, ExplosiveGateway())
    try:
        with pytest.raises(WindowOutcomeUnknown):
            owner.reserve()
        assert owner._phase == "unknown" and attempt._permission_grant_owner is owner and callbacks == []
    finally:
        setattr(attempt, gateway_name, original)


@pytest.mark.parametrize("callback", ["validator", "binding"])
@pytest.mark.parametrize("gateway_name", ["_lifecycle", "_directory"])
def test_after_ack_callback_gateway_substitution_is_unknown_before_access(
    prepared, monkeypatch, callback, gateway_name
):
    import dpone.services.mssql_tds_attempt as attempt_module

    attempt = _settled(prepared, monkeypatch)
    original_gateway, callbacks = getattr(attempt, gateway_name), []

    class ExplosiveGateway:
        def __getattribute__(self, name):
            callbacks.append(name)
            raise AssertionError("substituted gateway callback")

    if callback == "validator":
        original_callback = attempt_module.validate_original_record

        def injected(value):
            original_callback(value)
            if type(value) is TdsDirectorySnapshot and value.state.slots[-1].command is TdsCoordinatorCommand.GRANT:
                setattr(attempt, gateway_name, ExplosiveGateway())

        monkeypatch.setattr(attempt_module, "validate_original_record", injected)
    else:
        original_callback = attempt_module.validate_prepared_grant_binding

        def injected(*args):
            original_callback(*args)
            setattr(attempt, gateway_name, ExplosiveGateway())

        monkeypatch.setattr(attempt_module, "validate_prepared_grant_binding", injected)
    owner = _grant(attempt)
    try:
        with pytest.raises(WindowOutcomeUnknown):
            owner.reserve()
        assert owner._phase == "unknown" and callbacks == []
    finally:
        setattr(attempt, gateway_name, original_gateway)


@pytest.mark.parametrize("gateway_name", ["_lifecycle", "_directory"])
@pytest.mark.parametrize("property_name", ["identity", "reservation"])
def test_ready_property_gateway_substitution_is_unknown_before_access(
    prepared, monkeypatch, gateway_name, property_name
):
    attempt = _settled(prepared, monkeypatch)
    owner = _grant(attempt).reserve()
    original_gateway, callbacks = getattr(attempt, gateway_name), []

    class ExplosiveGateway:
        def __getattribute__(self, name):
            callbacks.append(name)
            raise AssertionError("substituted gateway callback")

    setattr(attempt, gateway_name, ExplosiveGateway())
    try:
        with pytest.raises(WindowOutcomeUnknown):
            getattr(owner, property_name)
        assert owner._phase == "unknown" and callbacks == []
    finally:
        setattr(attempt, gateway_name, original_gateway)


@pytest.mark.parametrize(
    ("target_name", "attribute", "changed"),
    [
        ("attempt", "_prepared_origin", None),
        ("attempt", "_observe_settlement", None),
        ("origin", "expected", None),
        ("origin", "expected", "wrong_phase"),
        ("origin", "failed", True),
        ("origin", "cleaned", False),
        ("settlement", "complete", False),
        ("settlement", "pending", True),
        ("settlement", "failed", True),
        ("settlement", "directory", None),
        ("settlement", "local_ack", None),
        ("settlement", "remote_ack", None),
    ],
)
def test_incomplete_visible_prerequisite_installs_no_owner_or_effect(
    prepared, monkeypatch, target_name, attribute, changed
):
    attempt = _settled(prepared, monkeypatch)
    targets = {"attempt": attempt, "origin": attempt._prepared_origin, "settlement": attempt._observe_settlement}
    implementation_sha256, deadline = (
        attempt._prepared_origin.identity.implementation_sha256,
        attempt._prepared_origin.deadline,
    )
    calls = []
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: calls.append((a, k)))
    if changed == "wrong_phase":
        changed = deepcopy(attempt._prepared_origin.expected)
        object.__setattr__(changed.state, "phase", TdsAttemptPhase.CREATION_INTENT)
    setattr(targets[target_name], attribute, changed)
    with pytest.raises(WindowContractError):
        attempt._permission_grant(UUID(int=701), "a" * 64, implementation_sha256, deadline=deadline)
    assert attempt._permission_grant_owner is None and calls == []


@pytest.mark.parametrize(
    ("target_name", "attribute", "changed"),
    [
        ("attempt", "_composition_origin", None),
        ("origin", "factory", None),
        ("origin", "pool", None),
        ("origin", "attempt", None),
        ("settlement", "attempt", None),
        ("settlement", "origin", None),
        ("handle", "factory", None),
        ("handle", "pool", None),
        ("settlement", "_references", ()),
        ("settlement", "local_return", None),
        ("settlement", "remote_return", None),
    ],
)
def test_deep_original_drift_poison_registered_owner_before_io(prepared, monkeypatch, target_name, attribute, changed):
    attempt = _settled(prepared, monkeypatch)
    origin, settlement = attempt._prepared_origin, attempt._observe_settlement
    calls = []
    original_execute = attempt._directory.execute

    def execute(*args, **kwargs):
        calls.append(args[0])
        return original_execute(*args, **kwargs)

    monkeypatch.setattr(attempt._directory, "execute", execute)
    target = {"attempt": attempt, "origin": origin, "settlement": settlement, "handle": origin.handle}[target_name]
    setattr(target, attribute, changed)
    with pytest.raises(WindowOutcomeUnknown):
        _grant(attempt)
    owner = attempt._permission_grant_owner
    assert owner is not None and owner._phase == "unknown" and calls == []


def test_registration_callback_sees_owner_and_reentry_stops_before_io(prepared, monkeypatch):
    import dpone.services.mssql_tds_attempt as attempt_module

    attempt = _settled(prepared, monkeypatch)
    original_validate = attempt_module.validate_original_record
    calls = []
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: calls.append((a, k)))

    def validate(value):
        assert attempt._permission_grant_owner is not None
        with pytest.raises(WindowContractError, match="permission_grant_active"):
            attempt.reserve_operation(UUID(int=706), TdsCoordinatorCommand.VERIFY, "c" * 64, deadline=monotonic() + 1)
        original_validate(value)

    monkeypatch.setattr(attempt_module, "validate_original_record", validate)
    owner = _grant(attempt)
    assert attempt._permission_grant_owner is owner and owner._phase == "registered" and calls == []


def test_registration_callback_failure_is_sticky_unknown_with_owner(prepared, monkeypatch):
    import dpone.services.mssql_tds_attempt as attempt_module

    attempt = _settled(prepared, monkeypatch)
    calls = []
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setattr(attempt_module, "validate_original_record", lambda value: (_ for _ in ()).throw(RuntimeError()))
    with pytest.raises(WindowOutcomeUnknown):
        _grant(attempt)
    owner = attempt._permission_grant_owner
    assert owner is not None and owner._phase == "unknown" and calls == []
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    assert calls == []


@pytest.mark.parametrize(
    "failure",
    [
        "lost_return",
        "prior_prefix",
        "limits",
        "sequence",
        "revision",
        "ownership",
        "fence",
        "digest",
        "tail_operation",
        "tail_index",
        "tail_command",
        "tail_owner_fence",
        "implementation_identity",
        "final_assertion",
    ],
)
def test_bad_or_lost_ack_is_unknown_once_and_late_observation_cannot_heal(prepared, monkeypatch, failure):
    attempt = _settled(prepared, monkeypatch)
    owner = _grant(attempt)
    if failure == "implementation_identity":
        identity_factory = owner._operations[2]
        operations = list(owner._operations)
        operations[2] = lambda *args: replace(identity_factory(*args), implementation_sha256="c" * 64)
        owner._operations = tuple(operations)
    original = attempt._directory.execute
    calls = []

    def execute(request, *, deadline):
        observed = original(request, deadline=deadline)
        if isinstance(request, ReserveDirectoryOperation):
            calls.append(request)
            if failure == "lost_return":
                raise TimeoutError("lost")
            if failure == "prior_prefix":
                prefix = replace(observed.state.slots[0], command_sha256="c" * 64)
                return replace(observed, state=replace(observed.state, slots=(prefix, *observed.state.slots[1:])))
            if failure == "limits":
                limits = replace(observed.state.limits, max_entries=observed.state.limits.max_entries + 1)
                return replace(observed, state=replace(observed.state, limits=limits))
            if failure == "sequence":
                return replace(observed, state=replace(observed.state, sequence=observed.state.sequence + 1))
            if failure == "revision":
                return replace(observed, revision=owner._capture[8].revision)
            if failure == "ownership":
                return replace(observed, ownership=replace(observed.ownership, owner="foreign"))
            if failure == "fence":
                return replace(observed, ownership=replace(observed.ownership, fence=observed.ownership.fence + 1))
            if failure == "digest":
                tail = replace(observed.state.slots[-1], command_sha256="c" * 64)
                return replace(observed, state=replace(observed.state, slots=(*observed.state.slots[:-1], tail)))
            tail_changes = {
                "tail_operation": ("operation_id", UUID(int=999)),
                "tail_index": ("index", observed.state.slots[-1].index + 1),
                "tail_command": ("command", TdsCoordinatorCommand.VERIFY),
                "tail_owner_fence": ("owner_fence", 0),
            }
            if failure in tail_changes:
                changed = deepcopy(observed)
                field, value = tail_changes[failure]
                object.__setattr__(changed.state.slots[-1], field, value)
                return changed
        return observed

    monkeypatch.setattr(attempt._directory, "execute", execute)
    if failure == "final_assertion":
        monkeypatch.setattr(
            attempt._prepared_origin.pool,
            "assert_deadline",
            lambda *, deadline: (_ for _ in ()).throw(RuntimeError("final assertion failed")),
        )
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    expected_calls = 0 if failure == "implementation_identity" else 1
    assert owner._phase == "unknown" and attempt._permission_grant_owner is owner and len(calls) == expected_calls
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    assert len(calls) == expected_calls


@pytest.mark.parametrize(
    "failure",
    ["deadline_expired", "lifecycle_changed", "directory_changed", "initial_observation", "final_observation"],
)
def test_observation_fault_matrix_is_sticky_and_never_retries_grant(prepared, monkeypatch, failure):
    import dpone.services.mssql_tds_permission_grant_association as association_module

    attempt = _settled(prepared, monkeypatch)
    owner = _grant(attempt)
    lifecycle_assert = attempt._lifecycle.assert_authority
    directory_execute = attempt._directory.execute
    grants = []
    assertions = 0

    def assert_authority(*, deadline):
        lifecycle_assert(deadline=deadline)
        if failure == "lifecycle_changed":
            attempt._lifecycle._snapshot = replace(
                attempt._lifecycle._snapshot, revision=attempt._lifecycle._snapshot.revision + 1
            )

    def execute(request, *, deadline):
        nonlocal assertions
        if isinstance(request, ReserveDirectoryOperation):
            grants.append(request)
        if isinstance(request, AssertDirectoryAuthority):
            assertions += 1
            if failure == "initial_observation" and assertions == 1:
                raise RuntimeError("initial observation failed")
            if failure == "final_observation" and assertions == 2:
                raise RuntimeError("final observation failed")
        observed = directory_execute(request, deadline=deadline)
        if failure == "directory_changed" and assertions == 1:
            return replace(observed, revision=observed.revision + 1)
        return observed

    monkeypatch.setattr(attempt._lifecycle, "assert_authority", assert_authority)
    monkeypatch.setattr(attempt._directory, "execute", execute)
    if failure == "deadline_expired":
        monkeypatch.setattr(association_module, "monotonic", lambda: owner._deadline)
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    expected = 1 if failure == "final_observation" else 0
    assert owner._phase == "unknown" and attempt._permission_grant_owner is owner and len(grants) == expected
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    assert len(grants) == expected


def test_ack_returned_is_visible_before_ack_validation(prepared, monkeypatch):
    import dpone.services.mssql_tds_attempt as attempt_module

    attempt = _settled(prepared, monkeypatch)
    validate_original = attempt_module.validate_original_record
    holder, phases = {}, []

    def validate(value):
        owner = holder.get("owner")
        if owner is not None and type(value) is TdsDirectorySnapshot:
            phases.append(owner._phase)
        validate_original(value)

    monkeypatch.setattr(attempt_module, "validate_original_record", validate)
    holder["owner"] = _grant(attempt)
    holder["owner"].reserve()
    assert phases == ["ack_returned"]


def test_grant_path_has_no_sql_process_credential_evidence_writer_or_settlement_effects(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    origin, settlement = attempt._prepared_origin, attempt._observe_settlement
    calls = []

    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("forbidden side effect")

    candidates = [
        (origin.handle, "collect_authenticated"),
        (origin.handle, "observe_selected"),
        (origin.handle, "close"),
        (origin.handle.catalog, "preparation"),
        (origin.handle.catalog, "observe_selected"),
        (origin.handle.child, "contain"),
        (origin.handle.writer, "execute"),
        (origin.handle.evidence, "close"),
        (settlement, "sequence"),
        (settlement, "retain_containment_gateway"),
    ]
    with monkeypatch.context() as patch:
        for target, name in candidates:
            if target is not None and hasattr(target, name):
                patch.setattr(target, name, forbidden)
        owner = _grant(attempt)
        owner.reserve()
    assert calls == []


@pytest.mark.parametrize("boundary", ["thread", "fork"])
def test_registered_owner_rejects_foreign_execution_before_io(prepared, monkeypatch, boundary):
    import os
    from concurrent.futures import ThreadPoolExecutor

    import dpone.services.mssql_tds_attempt as attempt_module

    attempt = _settled(prepared, monkeypatch)
    owner = _grant(attempt)
    calls = []
    monkeypatch.setattr(attempt._directory, "execute", lambda *a, **k: calls.append((a, k)))
    if boundary == "thread":
        with ThreadPoolExecutor(max_workers=1) as executor, pytest.raises(WindowOutcomeUnknown):
            executor.submit(owner.reserve).result()
    else:
        pid = os.getpid()
        with monkeypatch.context() as patch:
            patch.setattr(attempt_module.os, "getpid", lambda: pid + 1)
            with pytest.raises(WindowOutcomeUnknown):
                owner.reserve()
    assert owner._phase == "unknown" and attempt._permission_grant_owner is owner and calls == []


def test_second_association_and_second_reserve_make_no_second_effect(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    owner = _grant(attempt)
    calls = []
    original = attempt._directory.execute

    def execute(request, *, deadline):
        if isinstance(request, ReserveDirectoryOperation):
            calls.append(request)
        return original(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", execute)
    owner.reserve()
    with pytest.raises(WindowContractError, match="permission_grant_active"):
        _grant(attempt)
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    assert len(calls) == 1 and attempt._permission_grant_owner is owner


def test_callback_reentry_poison_is_sticky_and_makes_no_second_grant(prepared, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    owner = _grant(attempt)
    calls = []
    original = attempt._directory.execute

    def execute(request, *, deadline):
        if isinstance(request, ReserveDirectoryOperation):
            calls.append(request)
            with pytest.raises(WindowContractError, match="reentrant"):
                attempt.seal_work(deadline=deadline)
        return original(request, deadline=deadline)

    monkeypatch.setattr(attempt._directory, "execute", execute)
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    with pytest.raises(WindowOutcomeUnknown):
        owner.reserve()
    assert len(calls) == 1 and owner._phase == "unknown" and owner._reservation is not None
