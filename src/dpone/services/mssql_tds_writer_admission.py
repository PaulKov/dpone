"""Pure one-shot association of a settled P9 terminal with future P10b work."""

from __future__ import annotations

import os
from copy import deepcopy
from threading import Lock, get_ident
from typing import TYPE_CHECKING, Any, Never, cast

from dpone.services.mssql_tds_original_continuation import (
    PreparationOrigin,
    PreparationTransition,
    PreparationWriterInputs,
)
from dpone.services.mssql_tds_permission_grant_association import PermissionGrantAssociation
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import RestrictedWriterVerifyRetained
from dpone.services.mssql_tds_writer_admission_gate import (
    SqlClientWriterAdmissionUnknown as SqlClientWriterAdmissionUnknown,
)
from dpone.services.mssql_tds_writer_admission_gate import WriterAdmissionClaimMixin
from dpone.services.mssql_tds_writer_admission_gate import (
    WriterAdmissionGate as _WriterAdmissionGate,
)
from dpone.services.mssql_tds_writer_authority import (
    _ADMITTED_TOKEN,
    SqlClientWriterAdmitted,
    _AdmissionPlan,
    _create_sqlclient_writer_admitted,
    _make_identity_witness,
)
from dpone.services.mssql_tds_writer_contracts import (
    validate_writer_admission_limits,
    validate_writer_admission_values,
)
from dpone.services.mssql_tds_writer_grant_origin import capture_writer_grant_result

if TYPE_CHECKING:
    from dpone.services.mssql_tds_restricted_writer_settlement import _Owner

ERROR = "mssql_native.sqlclient_writer_admission_unknown"
_TERMINAL_TOKEN = object()
_LOWER_HEX = frozenset("0123456789abcdef")


class RestrictedWriterVerified(WriterAdmissionClaimMixin, tuple):
    """Credential-free immutable P9 terminal with one exact P10a claim."""

    __slots__ = ()

    def __new__(
        cls,
        token,
        directory,
        receipt,
        departure_receipts,
        authority_sha256,
        *,
        owner=None,
        association_capture=None,
        preparation=None,
        origin=None,
        writer_inputs=None,
    ):
        from dpone.services.mssql_tds_restricted_writer_settlement import _Owner

        if (
            token is not _TERMINAL_TOKEN
            or type(owner) is not _Owner
            or type(association_capture) is not tuple
            or type(preparation) is not PreparationTransition
            or type(origin) is not PreparationOrigin
            or type(writer_inputs) is not PreparationWriterInputs
        ):
            raise ValueError(ERROR)
        lock, pid, thread_id = Lock(), os.getpid(), get_ident()
        gate = _WriterAdmissionGate(lock)
        bind_identity, identity_witness = _make_identity_witness()
        terminal = tuple.__new__(
            cls,
            (
                directory,
                receipt,
                departure_receipts,
                authority_sha256,
                owner,
                gate,
                lock,
                pid,
                thread_id,
                association_capture,
                preparation,
                origin,
                writer_inputs,
                identity_witness,
            ),
        )
        gate.owner = terminal
        bind_identity(terminal)
        return terminal

    def __repr__(self) -> str:
        return "RestrictedWriterVerified(<opaque>)"

    directory = property(lambda self: self[0])
    _owner = property(lambda self: self[4])
    _association_capture = property(lambda self: self[9])
    _preparation = property(lambda self: self[10])
    _preparation_origin = property(lambda self: self[11])
    _writer_inputs = property(lambda self: self[12])


def _create_restricted_writer_verified(directory, receipt, departure_receipts, authority_sha256, *, owner):
    association = owner.retained._owner._association
    capture = association._capture
    preparation = capture[1]
    origin = preparation._origin
    writer_inputs = origin._writer_inputs
    return RestrictedWriterVerified(
        _TERMINAL_TOKEN,
        directory,
        receipt,
        departure_receipts,
        authority_sha256,
        owner=owner,
        association_capture=capture,
        preparation=preparation,
        origin=origin,
        writer_inputs=writer_inputs,
    )


def _invalid() -> Never:
    raise ValueError(ERROR)


def _exact_chain(terminal: RestrictedWriterVerified, claim: object) -> tuple[_Owner, Any, PreparationTransition]:
    from dpone.services.mssql_tds_restricted_writer_settlement import _Owner, _SettlementClaim

    owner = RestrictedWriterVerified._assert_p10a_claim(terminal, claim)
    if (
        type(owner) is not _Owner
        or owner._terminal is not terminal
        or owner.phase != "SETTLED"
        or owner.unknown
        or owner.busy
    ):
        _invalid()
    retained = owner.retained
    verify = retained._owner
    association = verify._association
    association_capture = terminal._association_capture
    host = association_capture[0]
    preparation = terminal._preparation
    refs = owner._refs
    if (
        type(owner.claim) is not _SettlementClaim
        or type(retained) is not RestrictedWriterVerifyRetained
        or type(association) is not PermissionGrantAssociation
        or len(refs) != 13
        or owner.claim.retained is not retained
        or owner.claim.operations is not owner.operations
        or owner.claim is not refs[0]
        or retained is not refs[1]
        or verify is not refs[2]
        or association is not refs[3]
        or verify._request is not refs[4]
        or verify._result is not refs[5]
        or verify._reservation is not refs[6]
        or verify._registration is not refs[7]
        or type(refs[8]) is not tuple
        or len(verify._receipts) != len(refs[8])
        or any(value is not exact for value, exact in zip(verify._receipts, refs[8], strict=True))
        or owner.origin is not refs[9]
        or owner.coordinator is not refs[10]
        or owner.evidence is not refs[11]
        or owner.operations is not refs[12]
        or owner.origin is not association
        or association._capture is not association_capture
        or len(association_capture) < 2
        or association_capture[1] is not preparation
        or type(preparation) is not PreparationTransition
        or preparation.attempt is not host
        or host._permission_grant_owner is not association
        or host._prepared_origin is not preparation
        or preparation._origin is not preparation._captured_origin
        or preparation._origin is not terminal._preparation_origin
        or preparation._origin._writer_inputs is not terminal._writer_inputs
    ):
        _invalid()
    return owner, verify, preparation


def _build_plan(
    terminal: RestrictedWriterVerified,
    owner: _Owner,
    verify: Any,
    preparation: PreparationTransition,
    *,
    startup_deadline: float,
    operation_deadline: float,
    termination_timeout_seconds: int,
    max_worker_address_space_bytes: int,
) -> _AdmissionPlan:
    origin = terminal._preparation_origin
    if type(origin) is not PreparationOrigin:
        _invalid()
    captured = terminal._writer_inputs
    if (
        type(captured) is not PreparationWriterInputs
        or captured is not origin._writer_inputs
        or PreparationOrigin.writer_inputs(origin) is not captured
    ):
        _invalid()
    attempt, directory = preparation.expected, terminal.directory
    verify_request = verify._request
    association = verify._association
    grant_request = association._verify_grant_ref.request
    try:
        grant_result_receipt, grant_result_sha256 = capture_writer_grant_result(association)
    except (ValueError, TypeError, AttributeError):
        _invalid()
    stage = preparation.handle.request.selected_stage
    input_descriptor, policy = captured.input_descriptor, captured.policy
    expectation = captured.content_expectation
    if directory is not association._verify_directory_ref or directory is not association._verify_remote_directory_ref:
        _invalid()
    try:
        stage_snapshot, input_binding_sha256 = validate_writer_admission_values(
            attempt=attempt,
            directory=directory,
            grant_request=grant_request,
            verify_request=verify_request,
            stage=stage,
            input_descriptor=input_descriptor,
            input_snapshot=captured.input_snapshot,
            policy=policy,
            policy_snapshot=captured.policy_snapshot,
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            preparation_deadline=preparation.deadline,
            captured_operation_deadline_ns=captured.operation_deadline_ns,
            termination_timeout_seconds=termination_timeout_seconds,
            max_worker_address_space_bytes=max_worker_address_space_bytes,
        )
    except ValueError:
        _invalid()
    attempt_exact = cast(Any, attempt)
    current_build_sha256 = object.__getattribute__(captured.installation, "build_sha256")
    if (
        captured.transition is not preparation
        or captured.policy is not preparation.policy
        or captured.input_descriptor is not preparation.input
        or captured.installation is not preparation.build
        or type(captured.installation) is not captured.installation_type
        or expectation.rows != input_descriptor.expected.rows
        or expectation.file_sha256 != input_descriptor.expected.file_sha256
        or type(current_build_sha256) is not str
        or len(current_build_sha256) != 64
        or any(character not in _LOWER_HEX for character in current_build_sha256)
        or type(captured.build_sha256) is not str
        or current_build_sha256 != captured.build_sha256
    ):
        _invalid()
    fence = object()
    plan = _AdmissionPlan(
        preparation,
        attempt_exact,
        deepcopy(attempt_exact),
        directory,
        deepcopy(directory),
        stage,
        stage_snapshot,
        input_descriptor,
        captured.input_snapshot,
        input_binding_sha256,
        policy,
        captured.policy_snapshot,
        captured.installation,
        captured.build_sha256,
        attempt_exact.state.identity.implementation_sha256,
        startup_deadline,
        operation_deadline,
        captured.operation_deadline_ns,
        termination_timeout_seconds,
        max_worker_address_space_bytes,
        grant_result_receipt,
        grant_result_sha256,
        expectation,
        terminal,
        owner,
        fence,
        None,
    )
    from dpone.services.mssql_tds_writer_authority_proof import mint_writer_authority_proof

    proof = mint_writer_authority_proof(terminal, owner, verify, preparation, fence=fence)
    return plan._replace(authority_proof=proof)


def admit_sqlclient_writer(
    terminal: RestrictedWriterVerified,
    *,
    now: float,
    startup_deadline: float,
    operation_deadline: float,
    termination_timeout_seconds: int,
    max_worker_address_space_bytes: int,
) -> SqlClientWriterAdmitted:
    """Consume the exact P9 terminal and return zero-effect P10b authority."""
    if type(terminal) is not RestrictedWriterVerified:
        _invalid()
    validate_writer_admission_limits(
        now,
        startup_deadline,
        operation_deadline,
        termination_timeout_seconds,
        max_worker_address_space_bytes,
    )
    claim = RestrictedWriterVerified._claim_p10a_once(terminal)
    try:
        owner, verify, preparation = _exact_chain(terminal, claim)
        plan = _build_plan(
            terminal,
            owner,
            verify,
            preparation,
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            termination_timeout_seconds=termination_timeout_seconds,
            max_worker_address_space_bytes=max_worker_address_space_bytes,
        )
        admitted = _create_sqlclient_writer_admitted(_ADMITTED_TOKEN, plan)
        RestrictedWriterVerified._mark_p10a_admitted(terminal, claim)
        return admitted
    except BaseException:
        try:
            RestrictedWriterVerified._mark_p10a_unknown(terminal, claim)
        except BaseException:
            pass
        raise SqlClientWriterAdmissionUnknown() from None
