"""Failure settlement and authenticated session exchange for one TDS CREATE."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, NoReturn

from dpone.app.mssql_tds_coordinator_request import (
    TdsCoordinatorCredentials,
    encode_create_response,
    encode_credentials,
)
from dpone.contracts.mssql_tds_api import (
    WindowOutcomeUnknown,
    authority_digest,
    decode_authority,
    encode_authority,
    encode_local_exit,
    local_exit_observation,
)
from dpone.contracts.mssql_tds_operation_models import (
    CoordinatorFailed,
    CoordinatorLocalObserved,
    CoordinatorResultReceived,
    CoordinatorSessionRegistered,
    TdsAttemptError,
    TdsConnectionMaterial,
    TdsConnectionProfile,
    TdsCoordinatorResultKind,
    TdsDatabaseObservation,
)
from dpone.contracts.mssql_tds_operation_models import (
    TdsCoordinatorEvidenceKind as Kind,
)

if TYPE_CHECKING:
    from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorRetention


class TdsCoordinatorSupervisionUnknown(WindowOutcomeUnknown):
    """Retain process, actor/pool authority and complete observed evidence."""

    def __init__(self, retained: TdsCoordinatorRetention) -> None:
        self.retained = retained
        super().__init__("mssql_native.tds_coordinator_supervision_unknown")


class TdsCoordinatorFailure(RuntimeError):
    """Acknowledged failed CREATE plus local containment; no rollback assertion."""

    def __init__(self, code: TdsAttemptError, retained: TdsCoordinatorRetention) -> None:
        self.code, self.retained = (code, retained)
        super().__init__("mssql_native.tds_coordinator_failed:" + code.value)


def fail_coordinator(
    retained: TdsCoordinatorRetention,
    original: BaseException,
    *,
    termination_timeout: float,
    clock: Callable[[], float],
) -> NoReturn:
    """Capture one cleanup budget and attempt containment before all backend waits."""
    from dpone.app.mssql_tds_coordinator_supervision import before

    try:
        retained.capture_cleanup_budget(termination_timeout, clock)
    except BaseException:
        pass
    try:
        retained.capture_result()
    except BaseException:
        pass
    try:
        deadline = retained.containment_deadline
        if deadline is None:
            raise ValueError("mssql_native.tds_coordinator_cleanup_budget_unknown")
        before(deadline, clock)
    except BaseException:
        raise TdsCoordinatorSupervisionUnknown(retained) from None
    uncertain = not isinstance(original, TdsCoordinatorFailure)
    try:
        retained.cleanup_local(deadline, clock, close=False)
    except BaseException:
        uncertain = True
    try:
        if retained.response is not None:
            retained.persist(Kind.RESULT, encode_create_response(retained.response), deadline)
            if (
                retained.response.result.outcome is TdsCoordinatorResultKind.FAILED
                and retained.current.state.result is None
            ):
                retained.assert_authority(deadline)
                retained.advance(CoordinatorResultReceived(retained.response.result), deadline)
    except BaseException:
        uncertain = True
    try:
        if retained.local_exit is not None:
            local = retained.local_proof()
            retained.persist(Kind.LOCAL_EXIT, encode_local_exit(local), deadline)
            state = retained.current.state
            if state.process != local.process or state.authentication_sha256 != local.authority_sha256:
                raise WindowOutcomeUnknown("mssql_native.tds_coordinator_registration_ack_unknown")
            retained.assert_authority(deadline)
            retained.advance(CoordinatorLocalObserved(local_exit_observation(local)), deadline)
        else:
            uncertain = True
    except BaseException:
        uncertain = True
    try:
        retained.assert_authority(deadline)
        retained.advance(CoordinatorFailed(retained.code), deadline)
    except BaseException:
        uncertain = True
    try:
        retained.close_child()
    except BaseException:
        uncertain = True
    for close in (retained.close_evidence, retained.close_writer):
        try:
            close(deadline)
        except BaseException:
            uncertain = True
    try:
        before(deadline, clock)
        retained.pool.assert_deadline(deadline=deadline)
    except BaseException:
        uncertain = True
    if uncertain:
        raise TdsCoordinatorSupervisionUnknown(retained) from None
    raise TdsCoordinatorFailure(retained.code, retained) from None


def deliver_coordinator_credentials(
    retained: TdsCoordinatorRetention,
    supplier: Callable[[], TdsConnectionMaterial],
    profile: TdsConnectionProfile,
    deadline: float,
    clock: Callable[[], float],
) -> None:
    """Release connection material once, after retained authority checks."""
    from dpone.app.mssql_tds_coordinator_supervision import before

    if retained.startup is None or retained.child is None:
        raise ValueError("mssql_native.tds_coordinator_startup_required")
    retained.assert_parent(deadline)
    material = supplier()
    try:
        value = TdsCoordinatorCredentials(
            retained.current.state.identity,
            retained.current.state.execution_owner,
            retained.startup.process,
            retained.startup.launch_nonce,
            retained.request,
            material,
            retained.session_nonce,
            profile,
        )
        body = encode_credentials(value)
        try:
            retained.assert_authority(deadline)
            before(deadline, clock)
            retained.assert_parent(deadline)
            retained.child.deliver_credentials(body, deadline=deadline)
        finally:
            del body, value
    finally:
        del material


def receive_coordinator_authority(
    retained: TdsCoordinatorRetention, deadline: float, expected_database: TdsDatabaseObservation | None
) -> None:
    """Bind the child session to the registered process and expected database."""
    assert retained.child is not None
    retained.assert_parent(deadline)
    observed = decode_authority(retained.child.observe_authority(deadline=deadline))
    state = retained.current.state
    if (
        observed.operation_sha256 != retained.operation_sha256
        or observed.execution_owner != state.execution_owner
        or observed.process != retained.process
        or (observed.implementation_sha256 != state.identity.implementation_sha256)
        or (observed.session.nonce != retained.session_nonce)
    ):
        raise ValueError("mssql_native.tds_coordinator_authority_binding")
    if expected_database is not None and observed.database != expected_database:
        raise ValueError("mssql_native.tds_coordinator_database_binding")
    if expected_database is not None and observed.schema_observation.name != retained.request.parent.schema:
        raise ValueError("mssql_native.tds_coordinator_schema_binding")
    retained.authority = observed
    retained.persist(Kind.AUTHORITY, encode_authority(observed), deadline)
    retained.advance(CoordinatorSessionRegistered(observed.session, authority_digest(observed)), deadline)
