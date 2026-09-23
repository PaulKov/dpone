"""Product-owned execution of one fresh SqlClient P7-P10f chunk."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from dpone.adapters.mssql_sqlclient_native_importer import SqlClientInputCustody
from dpone.app.mssql_sqlclient_authorization_composition import (
    SqlClientAuthorizationInputs,
    authorize_mssql_sqlclient_writer,
)
from dpone.app.mssql_sqlclient_prepared_attempt_factory import SqlClientPreparedAttempt
from dpone.app.mssql_sqlclient_writer_admission_composition import admit_mssql_sqlclient_writer
from dpone.app.mssql_sqlclient_writer_launch_composition import launch_mssql_sqlclient_writer
from dpone.app.mssql_sqlclient_writer_observation_composition import (
    SqlClientWriterObservationInputs,
    compose_sqlclient_writer_observation,
)
from dpone.app.mssql_sqlclient_writer_observer_composition import open_sqlclient_writer_observer
from dpone.app.mssql_sqlclient_writer_observer_request import sqlclient_writer_observer_request
from dpone.app.mssql_sqlclient_writer_pregrant_composition import prepare_mssql_sqlclient_writer
from dpone.contracts.bounded_window import WindowTransientError
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan
from dpone.contracts.mssql_sqlclient_credential_admission import SqlClientCredentialProfile
from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_tds_worker import TdsAttemptError
from dpone.ports.mssql_sqlclient_credentials import preload_sqlclient_credentials
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_writer_execution import execute_sqlclient_writer
from dpone.services.mssql_tds_writer_execution_custody import (
    SqlClientWriterHandledFailure,
    SqlClientWriterLocallyExited,
    handled_failure_error,
)
from dpone.services.mssql_tds_writer_observation_custody import SqlClientWriterGrantReady
from dpone.services.mssql_tds_writer_settlement import SqlClientWriterVerified, settle_sqlclient_writer

ERROR = "mssql_native.sqlclient_fresh_chunk_execution_invalid"
_RETRYABLE = frozenset(
    {
        TdsAttemptError.CONNECTION,
        TdsAttemptError.DRIVER,
        TdsAttemptError.STARTUP_TIMEOUT,
        TdsAttemptError.OPERATION_TIMEOUT,
    }
)


class SqlClientRetryPendingSettlement(WindowTransientError):
    """A closed retry candidate whose predecessor still requires settlement."""

    def __init__(self, code: TdsAttemptError) -> None:
        self.code = code
        super().__init__("mssql_native.sqlclient_retry_pending_settlement")


class SqlClientTerminalAttemptError(RuntimeError):
    """A deterministic acknowledged error that cannot auto-retry."""

    def __init__(self, code: TdsAttemptError) -> None:
        self.code = code
        super().__init__("mssql_native.sqlclient_terminal_input_or_policy")


@dataclass(frozen=True, slots=True, kw_only=True)
class FreshSqlClientChunkExecution:
    """Injected deployment facts for a fresh, already-prepared attempt."""

    prepared_attempt: Callable[..., SqlClientPreparedAttempt]
    authorization: Callable[[TdsAttempt], SqlClientAuthorizationInputs]
    pool: Any
    evidence_writer_factory: Callable[..., Any]
    startup_deadline: float
    operation_deadline: float
    termination_timeout_seconds: int
    max_worker_address_space_bytes: int
    writer_profile: SqlClientCredentialProfile
    writer_credentials: Callable[[], SqlClientCredentials]
    session_nonce: Callable[[], bytes | None]
    observer_admission: SqlClientObserverAdmission
    observer_profile: SqlClientCredentialProfile
    observer_credentials: Callable[[], SqlClientCredentials]
    observer_launcher: Any
    observer_startup_timeout: float
    observer_termination_timeout: float
    grant_id: Callable[[], UUID]
    settlement_verifier: Callable[[], object]
    clock_ns: Callable[[], int]
    retirement_custody: SqlClientAttemptRetirementCustody


class FreshSqlClientChunkExecutor:
    """Own the irreversible ordering from prepared-attempt acquisition to P10f."""

    def __init__(self, execution: FreshSqlClientChunkExecution) -> None:
        if type(execution) is not FreshSqlClientChunkExecution:
            raise ValueError(ERROR)
        self._execution = execution

    def execute(
        self,
        plan: NativeChunkPlan,
        file: EncodedNativeFile,
        attempt_id: str,
        lease: object,
        custody: SqlClientInputCustody,
    ) -> SqlClientWriterVerified:
        """Execute every phase once; ambiguity propagates without retry."""
        inputs = self._execution
        prepared = inputs.prepared_attempt(plan, file, attempt_id, lease, custody)
        if type(prepared) is not SqlClientPreparedAttempt:
            raise ValueError(ERROR)
        primary: BaseException | None = None
        try:
            if type(prepared.attempt) is not TdsAttempt:
                raise ValueError(ERROR)
            return self._execute_prepared(inputs, prepared.attempt)
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                prepared.release_input()
            except BaseException as release_error:
                if primary is None:
                    raise
                raise BaseExceptionGroup(
                    "mssql_native.sqlclient_execution_and_input_release_failed",
                    [primary, release_error],
                ) from None

    @staticmethod
    def _execute_prepared(inputs: FreshSqlClientChunkExecution, attempt: TdsAttempt) -> SqlClientWriterVerified:
        authorized = authorize_mssql_sqlclient_writer(attempt, inputs.authorization(attempt))
        admitted = admit_mssql_sqlclient_writer(
            authorized,
            startup_deadline=inputs.startup_deadline,
            operation_deadline=inputs.operation_deadline,
            termination_timeout_seconds=inputs.termination_timeout_seconds,
            max_worker_address_space_bytes=inputs.max_worker_address_space_bytes,
        )
        registered = launch_mssql_sqlclient_writer(
            admitted,
            pool=inputs.pool,
            evidence_writer_factory=inputs.evidence_writer_factory,
        )
        credentials: SqlClientCredentials | None = inputs.writer_credentials()
        try:
            if type(credentials) is not SqlClientCredentials:
                raise ValueError(ERROR)
            pregrant = prepare_mssql_sqlclient_writer(
                registered,
                profile=inputs.writer_profile,
                credentials=credentials,
                session_nonce=inputs.session_nonce(),
                clock_ns=inputs.clock_ns,
            )
        finally:
            credentials = None
        observer_request = sqlclient_writer_observer_request(
            pregrant,
            observer_admission=inputs.observer_admission,
        )

        def session_inputs() -> SqlClientWriterObservationInputs:
            if observer_request is None:
                raise ValueError(ERROR)
            observer_credentials: SqlClientCredentials | None = inputs.observer_credentials()
            try:
                if type(observer_credentials) is not SqlClientCredentials:
                    raise ValueError(ERROR)
                holder = preload_sqlclient_credentials(inputs.observer_profile, observer_credentials)
            finally:
                observer_credentials = None
            observer = open_sqlclient_writer_observer(
                observer_request,
                profile=inputs.observer_profile,
                credential_custody=holder,
                launcher=inputs.observer_launcher,
                startup_timeout=inputs.observer_startup_timeout,
                termination_timeout=inputs.observer_termination_timeout,
            )
            grant_id = inputs.grant_id()
            if type(grant_id) is not UUID or not grant_id.int:
                raise ValueError(ERROR)
            return SqlClientWriterObservationInputs(observer, str(grant_id), inputs.clock_ns())

        ready = compose_sqlclient_writer_observation(pregrant, session_inputs=session_inputs)
        if not isinstance(ready, SqlClientWriterGrantReady):
            raise ValueError(ERROR)
        exited = execute_sqlclient_writer(ready, clock_ns=inputs.clock_ns, retain_lifecycle=True)
        if type(exited) is SqlClientWriterHandledFailure:
            code = handled_failure_error(exited)
            inputs.retirement_custody.retain(attempt, deadline=inputs.operation_deadline)
            if code in _RETRYABLE:
                raise SqlClientRetryPendingSettlement(code)
            raise SqlClientTerminalAttemptError(code)
        if not isinstance(exited, SqlClientWriterLocallyExited):
            raise ValueError(ERROR)
        terminal = settle_sqlclient_writer(exited, inputs.settlement_verifier(), retain_lifecycle=True)
        if not isinstance(terminal, SqlClientWriterVerified):
            raise ValueError(ERROR)
        inputs.retirement_custody.retain(attempt, deadline=inputs.operation_deadline)
        return terminal


__all__ = (
    "FreshSqlClientChunkExecution",
    "FreshSqlClientChunkExecutor",
    "SqlClientRetryPendingSettlement",
    "SqlClientTerminalAttemptError",
)
