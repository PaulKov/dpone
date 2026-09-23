"""P10a consumes exact P9 authority without performing external effects."""

import os
from dataclasses import replace
from hashlib import sha256
from threading import Lock, get_ident
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
from dpone.contracts.mssql_sqlclient_input import encode_input_descriptor
from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
from dpone.contracts.mssql_sqlclient_writer_settlement import SqlClientStageContentExpectation
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand,
    TdsCoordinatorDirectory,
    TdsDirectoryLimits,
    TdsDirectorySlot,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    parent_digest,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase, TdsAttemptSnapshot, TdsAttemptState
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.services import mssql_tds_writer_admission as admission_module
from dpone.services.mssql_tds_original_continuation import PreparationTransition
from dpone.services.mssql_tds_permission_grant_association import PermissionGrantAssociation
from dpone.services.mssql_tds_permission_grant_release import PermissionGrantLocallyReleased
from dpone.services.mssql_tds_permission_grant_settlement import _SettlementOwner
from dpone.services.mssql_tds_preparation_origin import PreparationOrigin, PreparationWriterInputs
from dpone.services.mssql_tds_restricted_writer_settlement import _Owner, _SettlementClaim
from dpone.services.mssql_tds_restricted_writer_verification import _VerifyOwner
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import RestrictedWriterVerifyRetained
from dpone.services.mssql_tds_writer_admission import (
    RestrictedWriterVerified,
    SqlClientWriterAdmissionUnknown,
    admit_sqlclient_writer,
)
from tests.test_mssql_sqlclient_launch import typed_descriptor
from tests.test_mssql_sqlclient_permission_grant_departure import grant_departure_fixture
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_request
from tests.test_mssql_tds_directory_journal import OWNER
from tests.test_mssql_tds_permission_grant_release import held_setup

_REAL_CLAIM = RestrictedWriterVerified._claim_p10a_once
_REAL_ASSERT = RestrictedWriterVerified._assert_p10a_claim
_REAL_ADMITTED = RestrictedWriterVerified._mark_p10a_admitted
_REAL_UNKNOWN = RestrictedWriterVerified._mark_p10a_unknown


def _captured(values):
    return tuple.__new__(PreparationWriterInputs, values)


def _expectation():
    return SqlClientStageContentExpectation(1, "0" * 64, "1" * 64)


@pytest.fixture
def admission(tmp_path, monkeypatch):
    path = tmp_path / "input.bin"
    path.write_bytes(b"synthetic")
    fd = os.open(path, os.O_RDONLY)
    descriptor = typed_descriptor(fd)
    policy = NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30)
    policy_bytes = canonical_json_bytes(policy.to_dict())
    request = verify_request()
    identity = replace(
        request.parent,
        policy_sha256=sha256(policy_bytes).hexdigest(),
        file_sha256=descriptor.expected.file_sha256,
        implementation_sha256=request.implementation_sha256,
    )
    request = replace(request, parent=identity)
    stage = request.stage
    attempt = TdsAttemptSnapshot(
        TdsAttemptState(
            identity,
            OWNER,
            TdsAttemptPhase.PREPARED,
            2,
            object_identity=stage_object_identity(stage),
            observation_sha256="7" * 64,
        ),
        3,
    )
    binding = parent_digest(identity)
    local = TdsLocalContainment(binding, request.operation_id, "1" * 64, "2" * 64)
    remote = TdsRemoteSettlement(binding, request.operation_id, "3" * 64, "4" * 64)
    slot = TdsDirectorySlot(
        0,
        request.operation_id,
        TdsCoordinatorCommand.VERIFY,
        "5" * 64,
        attempt.state.ownership.fence,
        local_containment=local,
        remote_settlement=remote,
    )
    limits = TdsDirectoryLimits(8, 2, 1 << 20, 1 << 18)
    directory = TdsDirectorySnapshot(
        TdsCoordinatorDirectory(identity, limits, (slot,), sequence=1), attempt.state.ownership, 4
    )
    _, departure, _ = grant_departure_fixture()
    grant_request = replace(departure.plan.grant_evidence.request, parent=identity, stage=stage)
    installation = SimpleNamespace(build_sha256="6" * 64)
    preparation = object.__new__(PreparationTransition)
    preparation.attempt = host = SimpleNamespace()
    preparation.expected = attempt
    preparation.deadline = 10.0
    preparation.policy = policy
    preparation.input = descriptor
    preparation.build = installation
    preparation.handle = SimpleNamespace(request=SimpleNamespace(selected_stage=stage))
    host._prepared_origin = preparation
    association = object.__new__(PermissionGrantAssociation)
    association._host = host
    association._verify_grant_ref = SimpleNamespace(request=grant_request)
    association._verify_directory_ref = directory
    association._verify_remote_directory_ref = directory
    association._capture = (host, preparation)
    host._permission_grant_owner = association
    grant_held, _, _ = held_setup()
    grant_held.association = association
    grant_local = object.__new__(PermissionGrantLocallyReleased)
    grant_local.held_owner = grant_held
    grant_owner = object.__new__(_SettlementOwner)
    grant_owner.local = grant_local
    association._verify_owner_ref = association._remote_settlement_owner = grant_owner
    verify = object.__new__(_VerifyOwner)
    verify._association = association
    verify._request = request
    verify._result = result = object()
    verify._reservation = reservation = object()
    verify._registration = registration = object()
    verify._receipts = receipts = (object(),)
    retained = tuple.__new__(RestrictedWriterVerifyRetained, (verify, object(), object(), object(), None, None, None))
    claim_owner = object.__new__(_SettlementClaim)
    claim_owner._token = object()
    claim_owner.retained = retained
    operations = object()
    claim_owner.operations = operations
    claim_owner._plan = object()
    owner = object.__new__(_Owner)
    owner.claim = claim_owner
    owner.retained = retained
    owner.origin = association
    owner.coordinator = coordinator = object()
    owner.evidence = evidence = object()
    owner.operations = operations
    owner.phase = "SETTLED"
    owner.unknown = False
    owner.busy = False
    owner._receipt = SimpleNamespace(payload_sha256="9" * 64)
    owner._refs = (
        claim_owner,
        retained,
        verify,
        association,
        request,
        result,
        reservation,
        registration,
        receipts,
        association,
        coordinator,
        evidence,
        operations,
    )
    origin = object.__new__(PreparationOrigin)
    inputs = _captured(
        (
            preparation,
            policy,
            descriptor,
            installation,
            policy_bytes,
            encode_input_descriptor(descriptor),
            installation.build_sha256,
            10_000_000_000,
            SimpleNamespace,
            SqlClientStageContentExpectation(
                descriptor.expected.rows,
                descriptor.expected.file_sha256,
                "1" * 64,
            ),
        )
    )
    origin._writer_inputs = inputs
    preparation._origin = preparation._captured_origin = origin
    lock = Lock()
    gate = admission_module._WriterAdmissionGate(lock)
    bind_identity, identity_witness = admission_module._make_identity_witness()
    terminal = tuple.__new__(
        RestrictedWriterVerified,
        (
            directory,
            object(),
            (),
            "8" * 64,
            owner,
            gate,
            lock,
            os.getpid(),
            get_ident(),
            association._capture,
            preparation,
            origin,
            inputs,
            identity_witness,
        ),
    )
    gate.owner = terminal
    bind_identity(terminal)
    owner._terminal = terminal
    claim = object()
    marks = []
    monkeypatch.setattr(RestrictedWriterVerified, "_claim_p10a_once", lambda self: claim)
    monkeypatch.setattr(RestrictedWriterVerified, "_assert_p10a_claim", lambda self, exact: owner)
    monkeypatch.setattr(
        RestrictedWriterVerified, "_mark_p10a_admitted", lambda self, exact: marks.append(("admitted", exact))
    )
    monkeypatch.setattr(
        RestrictedWriterVerified, "_mark_p10a_unknown", lambda self, exact: marks.append(("unknown", exact))
    )
    monkeypatch.setattr(PreparationOrigin, "writer_inputs", lambda self: self._writer_inputs)
    yield SimpleNamespace(
        terminal=terminal,
        owner=owner,
        preparation=preparation,
        inputs=inputs,
        marks=marks,
        claim=claim,
        policy=policy,
    )
    os.close(fd)


def _admit(h, **changes):
    values = dict(
        now=1.0,
        startup_deadline=2.0,
        operation_deadline=3.0,
        termination_timeout_seconds=5,
        max_worker_address_space_bytes=8 << 30,
    )
    values.update(changes)
    return admit_sqlclient_writer(h.terminal, **values)


def test_actual_terminal_claim_is_exact_and_one_shot():
    owner = object.__new__(_Owner)
    owner.phase, owner.unknown, owner.busy, owner._terminal = "SETTLED", False, False, None
    preparation = object.__new__(PreparationTransition)
    origin = object.__new__(PreparationOrigin)
    inputs = _captured((preparation, object(), object(), object(), b"{}", b"{}", "0" * 64, 1, object, _expectation()))
    terminal = RestrictedWriterVerified(
        admission_module._TERMINAL_TOKEN,
        object(),
        object(),
        (),
        "a" * 64,
        owner=owner,
        association_capture=(object(), preparation),
        preparation=preparation,
        origin=origin,
        writer_inputs=inputs,
    )
    owner._terminal = terminal
    assert repr(terminal) == "RestrictedWriterVerified(<opaque>)"
    assert repr(inputs) == "PreparationWriterInputs(<opaque>)"
    assert admission_module._TERMINAL_TOKEN not in terminal

    with pytest.raises(ValueError, match="writer_admission_unknown"):
        type(terminal)(
            terminal[9],
            terminal[0],
            terminal[1],
            terminal[2],
            terminal[3],
            owner=terminal[4],
            association_capture=terminal[9],
            preparation=terminal[10],
            origin=terminal[11],
            writer_inputs=terminal[12],
        )

    claim = terminal._claim_p10a_once()

    assert terminal._assert_p10a_claim(claim) is owner
    terminal._mark_p10a_admitted(claim)
    with pytest.raises(ValueError, match="writer_admission_unknown"):
        terminal._claim_p10a_once()


def test_actual_terminal_reconstruction_fails_after_coordinated_owner_substitution():
    owner = object.__new__(_Owner)
    owner.phase, owner.unknown, owner.busy, owner._terminal = "SETTLED", False, False, None
    preparation = object.__new__(PreparationTransition)
    origin = object.__new__(PreparationOrigin)
    inputs = _captured((preparation, object(), object(), object(), b"{}", b"{}", "0" * 64, 1, object, _expectation()))
    terminal = RestrictedWriterVerified(
        admission_module._TERMINAL_TOKEN,
        object(),
        object(),
        (),
        "a" * 64,
        owner=owner,
        association_capture=(object(), preparation),
        preparation=preparation,
        origin=origin,
        writer_inputs=inputs,
    )
    reconstructed = tuple.__new__(RestrictedWriterVerified, terminal)
    terminal[5].owner = reconstructed
    owner._terminal = reconstructed

    with pytest.raises(ValueError, match="writer_admission_unknown"):
        reconstructed._claim_p10a_once()


def test_actual_terminal_claim_rejects_replaced_gate_lock_without_using_it():
    owner = object.__new__(_Owner)
    owner.phase, owner.unknown, owner.busy, owner._terminal = "SETTLED", False, False, None
    preparation = object.__new__(PreparationTransition)
    origin = object.__new__(PreparationOrigin)
    inputs = _captured((preparation, object(), object(), object(), b"{}", b"{}", "0" * 64, 1, object, _expectation()))
    terminal = RestrictedWriterVerified(
        admission_module._TERMINAL_TOKEN,
        object(),
        object(),
        (),
        "a" * 64,
        owner=owner,
        association_capture=(object(), preparation),
        preparation=preparation,
        origin=origin,
        writer_inputs=inputs,
    )
    owner._terminal = terminal
    terminal[5].lock = Lock()

    with pytest.raises(ValueError, match="writer_admission_unknown"):
        terminal._claim_p10a_once()

    assert terminal[5].state == "UNKNOWN"


def test_exact_terminal_yields_opaque_one_shot_p10b_capability(admission):
    admitted = _admit(admission)

    assert repr(admitted) == "SqlClientWriterAdmitted(<opaque>)"
    assert admission.marks == [("admitted", admission.claim)]
    reconstructed = object.__new__(type(admitted))
    with pytest.raises(ValueError, match="writer_admission_unknown"):
        reconstructed._claim_p10b_once()
    claim = admitted._claim_p10b_once()
    plan = admitted._assert_p10b_claim(claim)
    assert plan.terminal is admission.terminal
    assert plan.p9_owner is admission.owner
    assert plan.transition is admission.preparation
    assert plan.input_descriptor is admission.inputs.input_descriptor
    assert plan.policy is admission.policy
    assert plan.installation is admission.inputs.installation
    assert (
        plan.grant_result_receipt
        is admission.terminal._preparation.attempt._permission_grant_owner._verify_owner_ref.local.held_owner.result_receipt
    )
    assert plan.grant_result_sha256 == plan.grant_result_receipt.payload_sha256
    with pytest.raises(ValueError, match="writer_admission_unknown"):
        admitted._claim_p10b_once()


def test_permission_grant_result_receipt_substitution_is_sticky_unknown(admission):
    held = admission.terminal._preparation.attempt._permission_grant_owner._verify_owner_ref.local.held_owner
    held.result_receipt = object()

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert admission.marks == [("unknown", admission.claim)]


def test_p10b_rejects_every_reconstructed_instance(admission):
    admitted = _admit(admission)
    for reconstructed in (object.__new__(type(admitted)), type(admitted)()):
        with pytest.raises(ValueError, match="writer_admission_unknown"):
            reconstructed._claim_p10b_once()
    assert not isinstance(admitted, tuple)
    assert not hasattr(admitted, "plan")


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("now", 2.0),
        ("startup_deadline", 3),
        ("operation_deadline", float("inf")),
        ("termination_timeout_seconds", True),
        ("max_worker_address_space_bytes", 1),
    ),
)
def test_invalid_scalar_shape_is_rejected_before_terminal_claim(admission, monkeypatch, field, value):
    calls = []
    monkeypatch.setattr(RestrictedWriterVerified, "_claim_p10a_once", lambda self: calls.append("claim"))

    with pytest.raises(ValueError, match="writer_admission_unknown"):
        _admit(admission, **{field: value})

    assert calls == [] and admission.marks == []


def test_postclaim_binding_mismatch_is_sticky_unknown(admission):
    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission, max_worker_address_space_bytes=16 << 30)

    assert admission.marks == [("unknown", admission.claim)]


def test_owner_mutation_is_detected_only_after_irreversible_claim(admission, monkeypatch):
    monkeypatch.setattr(RestrictedWriterVerified, "_claim_p10a_once", _REAL_CLAIM)
    monkeypatch.setattr(RestrictedWriterVerified, "_assert_p10a_claim", _REAL_ASSERT)
    monkeypatch.setattr(RestrictedWriterVerified, "_mark_p10a_admitted", _REAL_ADMITTED)
    monkeypatch.setattr(RestrictedWriterVerified, "_mark_p10a_unknown", _REAL_UNKNOWN)
    admission.owner.phase = "BROKEN"

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert admission.terminal[5].state == "UNKNOWN"
    admission.owner.phase = "SETTLED"
    with pytest.raises(ValueError, match="writer_admission_unknown"):
        _admit(admission)


@pytest.mark.parametrize(
    ("startup_deadline", "operation_deadline"),
    ((10.1, 10.1), (9.0, 10.1)),
)
def test_both_writer_deadlines_are_bounded_by_preparation_ceilings(admission, startup_deadline, operation_deadline):
    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission, startup_deadline=startup_deadline, operation_deadline=operation_deadline)

    assert admission.marks == [("unknown", admission.claim)]


def test_postclaim_cancellation_is_sticky_unknown(admission, monkeypatch):
    def cancel(_origin):
        raise KeyboardInterrupt

    monkeypatch.setattr(PreparationOrigin, "writer_inputs", cancel)

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert admission.marks == [("unknown", admission.claim)]


def test_unknown_latch_survives_callback_reset_and_blocks_replay(admission, monkeypatch):
    monkeypatch.setattr(RestrictedWriterVerified, "_claim_p10a_once", _REAL_CLAIM)
    monkeypatch.setattr(RestrictedWriterVerified, "_assert_p10a_claim", _REAL_ASSERT)
    monkeypatch.setattr(RestrictedWriterVerified, "_mark_p10a_admitted", _REAL_ADMITTED)
    monkeypatch.setattr(RestrictedWriterVerified, "_mark_p10a_unknown", _REAL_UNKNOWN)

    def reset_then_cancel(_origin):
        gate = admission.terminal[5]
        gate.state, gate.claim = "AVAILABLE", None
        raise KeyboardInterrupt

    monkeypatch.setattr(PreparationOrigin, "writer_inputs", reset_then_cancel)

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert admission.terminal[5].state == "UNKNOWN"
    with pytest.raises(ValueError, match="writer_admission_unknown"):
        _admit(admission)


def test_policy_instance_method_shadow_is_inert(admission):
    callbacks = []

    class ShadowFields:
        def __iter__(self):
            callbacks.append("fields")
            return iter(())

    object.__setattr__(admission.policy, "to_dict", lambda: callbacks.append("called"))
    object.__setattr__(admission.policy, "__dataclass_fields__", ShadowFields())

    admitted = _admit(admission)

    claim = admitted._claim_p10b_once()
    assert admitted._assert_p10b_claim(claim).policy is admission.policy
    assert callbacks == []


@pytest.mark.parametrize("field", ("backend", "max_worker_address_space_bytes", "terminate_timeout_seconds"))
def test_policy_scalar_substitution_rejects_without_comparison_callback(admission, field):
    callbacks = []

    class CallbackScalar:
        def __eq__(self, other):
            callbacks.append(("eq", other))
            return True

        def __ne__(self, other):
            callbacks.append(("ne", other))
            return False

        def __gt__(self, other):
            callbacks.append(("gt", other))
            return False

    object.__setattr__(admission.policy, field, CallbackScalar())

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert callbacks == []


def test_installation_build_digest_drift_is_sticky_unknown(admission):
    admission.inputs.installation.build_sha256 = "f" * 64

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert admission.marks == [("unknown", admission.claim)]


def test_installation_build_scalar_substitution_rejects_without_callback(admission):
    callbacks = []

    class CallbackDigest:
        def __ne__(self, other):
            callbacks.append(other)
            return True

    admission.inputs.installation.build_sha256 = CallbackDigest()

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert callbacks == []


def test_captured_input_snapshot_substitution_fails_closed(admission):
    changed = bytearray(admission.inputs.input_snapshot)
    changed[-1] ^= 1
    admission.inputs = _captured((*admission.inputs[:5], bytes(changed), *admission.inputs[6:]))
    admission.preparation._origin._writer_inputs = admission.inputs

    with pytest.raises(SqlClientWriterAdmissionUnknown, match="writer_admission_unknown"):
        _admit(admission)

    assert admission.marks == [("unknown", admission.claim)]
