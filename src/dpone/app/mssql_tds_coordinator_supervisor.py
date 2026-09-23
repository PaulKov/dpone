"""One original CREATE through acknowledged journal, evidence and process ports.

This app service supplies no SQL effects. Success describes only this CREATE,
its durable evidence and local reaping; reconciliation and parent progression
remain separate. Both actor gateways must be admitted before calling this entry.
"""

from __future__ import annotations

import math
import secrets
from collections.abc import Callable
from dataclasses import dataclass, replace
from hashlib import sha256
from time import monotonic
from uuid import UUID, uuid4

from dpone.adapters.mssql_coordinator_worker_capabilities import (
    TdsActorPool,
    TdsConnectionMaterial,
    TdsConnectionProfile,
    decode_connection_admission,
    encode_connection_admission,
)
from dpone.app.mssql_tds_coordinator_lifecycle import (
    deliver_coordinator_credentials as _credentials,
)
from dpone.app.mssql_tds_coordinator_lifecycle import (
    receive_coordinator_authority as _receive_authority,
)
from dpone.app.mssql_tds_coordinator_request import (
    TdsCreateResponse,
    encode_create_response,
    encode_grant,
    validate_grant,
)
from dpone.app.mssql_tds_coordinator_supervision import (
    TdsCoordinatorFailure,
    TdsCoordinatorRetention,
    before,
    checked_exit,
    fail_coordinator,
)
from dpone.app.mssql_tds_create_provenance import TdsCoordinatorCreateProvenance, validate_create_provenance
from dpone.contracts.mssql_coordinator_worker_capabilities import (
    CoordinatorCredentialIntent,
    CoordinatorGrantIntent,
    CoordinatorLocalObserved,
    CoordinatorProcessRegistered,
    CoordinatorResultReceived,
    TdsAttemptError,
    TdsChildExit,
    TdsCoordinatorCommand,
    TdsCoordinatorEvidenceObservation,
    TdsCoordinatorEvidenceReceipt,
    TdsCoordinatorGrant,
    TdsCoordinatorPhase,
    TdsCoordinatorResultKind,
    TdsCoordinatorSnapshot,
    TdsCoordinatorStartup,
    TdsCreateRequest,
    TdsDatabaseObservation,
    TdsProcessIdentity,
    authority_digest,
    create_command_digest,
    encode_create_request,
    encode_local_exit,
    encode_registration,
    local_exit_observation,
    require_session_nonce,
)
from dpone.contracts.mssql_coordinator_worker_capabilities import (
    TdsCoordinatorEvidenceKind as Kind,
)
from dpone.ports.mssql_coordinator_worker_capabilities import (
    TdsCoordinatorEvidenceGateway,
    TdsCoordinatorGateway,
    TdsCoordinatorProcessLauncher,
    TdsLaunchUnknown,
)


@dataclass(frozen=True)
class TdsCoordinatorCreateOutcome:
    """Durable CREATE component result, never a remote-settlement or route receipt."""

    response: TdsCreateResponse
    local_exit: TdsChildExit
    receipts: tuple[TdsCoordinatorEvidenceReceipt, ...]
    provenance: TdsCoordinatorCreateProvenance | None = None

    def __post_init__(self) -> None:
        if (
            type(self.response) is not TdsCreateResponse
            or self.response.result.outcome is not TdsCoordinatorResultKind.SUCCEEDED
            or type(self.local_exit) is not TdsChildExit
            or self.local_exit.reaped is not True
            or self.local_exit.exit_code != 0
            or type(self.receipts) is not tuple
            or len(self.receipts) != len(Kind)
            or any(type(r) is not TdsCoordinatorEvidenceReceipt for r in self.receipts)
            or {r.kind for r in self.receipts} != set(Kind)
            or any(r.operation_sha256 != self.response.result.operation_sha256 for r in self.receipts)
        ):
            raise ValueError("mssql_native.tds_coordinator_outcome_invalid")
        # Frozen instances can still arrive with mutated nested scalar fields.
        # Reject equal-valued bool/int/enum aliases before accepting the result.
        self.local_exit.__post_init__()
        self.local_exit.identity.__post_init__()
        for receipt in self.receipts:
            receipt.__post_init__()
        if self.provenance is not None:
            if type(self.provenance) is not TdsCoordinatorCreateProvenance:
                raise ValueError("mssql_native.tds_coordinator_outcome_invalid")
            self.provenance.__post_init__()


def _run(
    retained: TdsCoordinatorRetention,
    launcher: TdsCoordinatorProcessLauncher,
    supplier: Callable[[], TdsConnectionMaterial],
    profile: TdsConnectionProfile,
    *,
    expected_database: TdsDatabaseObservation | None,
    operation_deadline: float,
    startup_timeout: float,
    clock: Callable[[], float],
) -> TdsCoordinatorCreateOutcome:
    retained.assert_authority(operation_deadline)
    retained.persist(Kind.CREATE_REQUEST, encode_create_request(retained.request), operation_deadline)
    retained.persist(Kind.ADMISSION, retained.admission, operation_deadline)
    retained.assert_authority(operation_deadline)
    before(operation_deadline, clock)
    startup_deadline = min(operation_deadline, clock() + startup_timeout)
    retained.code = TdsAttemptError.STARTUP_TIMEOUT
    retained.assert_parent(startup_deadline)
    retained.child = launcher.spawn(startup_deadline=startup_deadline, operation_deadline=operation_deadline)
    retained.process = retained.child.identity
    if type(retained.process) is not TdsProcessIdentity:
        raise ValueError("mssql_native.tds_coordinator_process_invalid")
    retained.assert_parent(startup_deadline)
    startup = retained.child.startup(deadline=startup_deadline)
    if (
        type(startup) is not TdsCoordinatorStartup
        or startup != retained.child.startup_receipt
        or startup.process != retained.process
        or startup.implementation_sha256 != retained.current.state.identity.implementation_sha256
    ):
        raise ValueError("mssql_native.tds_coordinator_startup_binding")
    retained.startup = startup
    before(startup_deadline, clock)
    retained.registration = encode_registration(startup, sha256(retained.admission).hexdigest())
    retained.persist(Kind.REGISTRATION, retained.registration, startup_deadline)
    retained.code = TdsAttemptError.FENCING
    retained.advance(
        CoordinatorProcessRegistered(retained.process, sha256(retained.registration).hexdigest()), startup_deadline
    )
    retained.advance(CoordinatorCredentialIntent(), startup_deadline)
    retained.code = TdsAttemptError.PROTOCOL
    _credentials(retained, supplier, profile, startup_deadline, clock)
    before(operation_deadline, clock)
    _receive_authority(retained, operation_deadline, expected_database)
    assert retained.authority is not None
    retained.grant = TdsCoordinatorGrant(
        retained.operation_sha256,
        retained.current.state.execution_owner,
        retained.process,
        retained.authority.session,
        authority_digest(retained.authority),
        uuid4(),
    )
    validate_grant(retained.current.state.identity, retained.grant, retained.authority)
    retained.code = TdsAttemptError.FENCING
    retained.advance(CoordinatorGrantIntent(retained.grant), operation_deadline)
    retained.assert_authority(operation_deadline)
    before(operation_deadline, clock)
    retained.code = TdsAttemptError.PROTOCOL
    retained.assert_parent(operation_deadline)
    retained.child.deliver_grant(
        encode_grant(retained.current.state.identity, retained.grant), deadline=operation_deadline
    )
    retained.assert_parent(operation_deadline)
    retained.raw_result = retained.child.receive_result(deadline=operation_deadline)
    retained.capture_result()  # Retain typed evidence before any deadline/I/O/teardown.
    assert retained.response is not None
    before(operation_deadline, clock)
    if retained.response.result.outcome is TdsCoordinatorResultKind.FAILED:
        assert retained.response.result.error is not None
        retained.code = retained.response.result.error
        raise TdsCoordinatorFailure(retained.code, retained)
    retained.persist(Kind.RESULT, encode_create_response(retained.response), operation_deadline)
    retained.code = TdsAttemptError.PROCESS_UNKNOWN
    retained.assert_parent(operation_deadline)
    retained.local_exit = checked_exit(retained.child.wait(deadline=operation_deadline), retained.process)
    before(operation_deadline, clock)
    if retained.local_exit.exit_code != 0:
        raise ValueError("mssql_native.tds_coordinator_nonzero_exit")
    retained.code = TdsAttemptError.CLEANUP
    retained.close_child()
    retained.code = TdsAttemptError.FENCING
    retained.assert_authority(operation_deadline)
    retained.advance(CoordinatorResultReceived(retained.response.result), operation_deadline)
    before(operation_deadline, clock)
    local = retained.local_proof()
    retained.persist(Kind.LOCAL_EXIT, encode_local_exit(local), operation_deadline)
    retained.advance(CoordinatorLocalObserved(local_exit_observation(local)), operation_deadline)
    retained.code = TdsAttemptError.CLEANUP
    retained.close_evidence(operation_deadline)
    retained.close_writer(operation_deadline)
    retained.pool.assert_deadline(deadline=operation_deadline)
    before(operation_deadline, clock)
    assert retained.startup is not None and retained.authority is not None and retained.grant is not None
    provenance = TdsCoordinatorCreateProvenance(
        request=retained.request,
        admission=retained.admission,
        startup=retained.startup,
        authority=retained.authority,
        grant=retained.grant,
        local_proof=local,
        snapshot=retained.current,
    )
    receipts = tuple(retained.receipts.values())
    validate_create_provenance(
        provenance,
        response=retained.response,
        local_exit=retained.local_exit,
        receipts=receipts,
        request=retained.request,
        identity=retained.current.state.identity,
        ownership=retained.current.state.execution_owner,
    )
    outcome = TdsCoordinatorCreateOutcome(retained.response, retained.local_exit, receipts, provenance)
    before(operation_deadline, clock)
    retained.assert_parent(operation_deadline)
    before(operation_deadline, clock)
    return outcome


def run_tds_coordinator(
    writer: TdsCoordinatorGateway,
    evidence: TdsCoordinatorEvidenceGateway,
    launcher: TdsCoordinatorProcessLauncher,
    request: TdsCreateRequest,
    admission: bytes,
    connection_material: Callable[[], TdsConnectionMaterial],
    *,
    pool: TdsActorPool,
    operation_deadline: float,
    startup_timeout: float,
    termination_timeout: float,
    clock: Callable[[], float] = monotonic,
    _parent_assertion: Callable[[float], None] | None = None,
    _expected_database: TdsDatabaseObservation | None = None,
) -> TdsCoordinatorCreateOutcome:
    """Run one original INTENT using two already admitted actor gateways.

    Supply reviewed immutable admission pins and a bounded in-memory credential
    supplier. The app root binds the launcher source/root and shares this run's
    pool; no synchronous evidence writer or journal belongs on this call path.
    """
    if (
        any(
            type(v) not in (int, float) or not math.isfinite(v) or v <= 0
            for v in (operation_deadline, startup_timeout, termination_timeout)
        )
        or not callable(connection_material)
        or (_parent_assertion is not None and not callable(_parent_assertion))
    ):
        raise ValueError("mssql_native.tds_coordinator_arguments_invalid")
    if _expected_database is not None:
        if type(_expected_database) is not TdsDatabaseObservation:
            raise ValueError("mssql_native.tds_coordinator_database_binding")
        _expected_database.__post_init__()
        guid = _expected_database.database_guid.int
        if type(guid) is not int or not 0 < guid < 2**128:
            raise ValueError("mssql_native.tds_coordinator_database_binding")
        _expected_database = replace(_expected_database, database_guid=UUID(int=guid))
    initial = writer.observation.snapshot
    if type(initial) is not TdsCoordinatorSnapshot or type(request) is not TdsCreateRequest:
        raise ValueError("mssql_native.tds_coordinator_intent_required")
    state = initial.state
    if (
        state.phase is not TdsCoordinatorPhase.INTENT
        or state.recovering
        or state.execution_owner != state.ownership
        or state.error is not None
        or state.local is not None
        or state.remote is not None
        or state.identity.command is not TdsCoordinatorCommand.CREATE
        or request.parent != state.identity.parent
        or create_command_digest(request) != state.identity.command_sha256
    ):
        raise ValueError("mssql_native.tds_coordinator_intent_required")
    build, profile = decode_connection_admission(admission)
    canonical_admission = encode_connection_admission(build, profile)
    if sha256(canonical_admission).hexdigest() != launcher.admission_sha256:
        raise ValueError("mssql_native.tds_coordinator_admission_binding")
    retained = TdsCoordinatorRetention(
        writer,
        evidence,
        pool,
        initial,
        request,
        canonical_admission,
        secrets.token_bytes(32),
        parent_assertion=_parent_assertion,
    )
    require_session_nonce(retained.session_nonce)
    observation = evidence.observation
    if (
        type(observation) is not TdsCoordinatorEvidenceObservation
        or observation.operation_sha256 != retained.operation_sha256
        or observation.receipt is not None
    ):
        raise ValueError("mssql_native.tds_coordinator_evidence_operation_invalid")
    try:
        return _run(
            retained,
            launcher,
            connection_material,
            profile,
            expected_database=_expected_database,
            operation_deadline=operation_deadline,
            startup_timeout=startup_timeout,
            clock=clock,
        )
    except BaseException as error:
        if isinstance(error, TdsLaunchUnknown):
            retained.unresolved_launch = error.launch
        if isinstance(error, TimeoutError) and retained.code is not TdsAttemptError.STARTUP_TIMEOUT:
            retained.code = TdsAttemptError.OPERATION_TIMEOUT
        fail_coordinator(retained, error, termination_timeout=termination_timeout, clock=clock)
