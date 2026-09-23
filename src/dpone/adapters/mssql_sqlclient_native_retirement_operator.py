"""Production SQL Server effects for exact, restartable native-chunk retirement."""

from __future__ import annotations

import time
from collections.abc import Callable
from hashlib import sha256
from typing import Any

from dpone.adapters.mssql_sqlclient_native_retirement_session import (
    OpenManagementSession,
    SqlClientRetirementSession,
    SqlClientRetirementSessionAuthority,
)
from dpone.adapters.mssql_sqlclient_native_retirement_sql import (
    DROP_EXACT_STAGE_SQL,
    OBSERVE_EXACT_STAGE_SQL,
    drop_exact_parameters,
    observe_stage_parameters,
    stage_catalog_status,
    stage_is_exact,
)
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkAbsenceProof,
    NativeChunkCapacityProof,
    NativeChunkDropIntent,
    NativeChunkDropObservation,
    NativeChunkDropOutcome,
    NativeChunkDropProof,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
    NativeChunkRetirementState,
    NativeChunkSettlementProof,
    bind_native_chunk_absence,
    bind_native_chunk_capacity,
    bind_native_chunk_containment,
    bind_native_chunk_drop,
    bind_native_chunk_settlement,
    native_chunk_object_incarnation_digest,
)
from dpone.ports.mssql_sqlclient_writer_settlement import SqlClientObserverAdmission
from dpone.ports.mssql_tds_directory import TdsDirectoryLimits, TdsDirectoryObserver
from dpone.ports.mssql_tds_journal import TdsAttemptObserver, TdsAttemptPhase

_ERROR = "mssql_native.sqlclient_retirement_effect_invalid"


def _digest(*parts: object) -> str:
    return sha256(":".join(str(part) for part in parts).encode()).hexdigest()


CapacityReleaseObserver = Callable[[NativeChunkRetirementRequest], bool]


class SqlClientNativeRetirementOperator:
    """Bind durable local facts and guarded SQL effects to existing P10g proofs."""

    def __init__(
        self,
        *,
        open_management_session: OpenManagementSession,
        admission: SqlClientObserverAdmission,
        expected_server: object,
        expected_database: object,
        attempts: TdsAttemptObserver,
        directories: TdsDirectoryObserver,
        directory_limits: TdsDirectoryLimits,
        progress: NativeChunkRetirementState,
        observe_capacity_release: CapacityReleaseObserver | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        operation_timeout: float = 30.0,
    ) -> None:
        if (
            type(admission) is not SqlClientObserverAdmission
            or admission.server != expected_server
            or admission.database != expected_database
            or type(directory_limits) is not TdsDirectoryLimits
        ):
            raise ValueError(_ERROR)
        admission.__post_init__()
        self._admission = admission
        self._attempts = attempts
        self._directories = directories
        self._limits = directory_limits
        self._progress = progress
        self._capacity = observe_capacity_release
        if not callable(monotonic) or type(operation_timeout) not in (int, float) or operation_timeout <= 0:
            raise ValueError(_ERROR)
        self._sessions = SqlClientRetirementSessionAuthority(
            open_management_session=open_management_session,
            admission=admission,
            monotonic=monotonic,
            operation_timeout=float(operation_timeout),
        )

    def _authority(self, request: NativeChunkRetirementRequest) -> SqlClientRetirementSession:
        self._validate(request)
        return self._sessions.acquire(request, self._assert_current)

    def _assert_current(self, request: NativeChunkRetirementRequest) -> None:
        progress = self._progress.observe(request)
        attempt = self._attempts.read(request.projection.attempt)
        directory = self._directories.read(request.projection.attempt, self._limits)
        lifecycle = progress.lifecycle[-1]
        if (
            attempt is None
            or directory is None
            or directory.state.retirement_authority != attempt.state
            or lifecycle.authority_digest != request.authority.digest
            or lifecycle.authority_fence != request.authority.fence
            or directory.ownership.fence != request.authority.fence
        ):
            raise ValueError(_ERROR)

    def _close(self, *, primary: BaseException | None = None) -> None:
        self._sessions.close(primary=primary)

    def _validate(self, request: NativeChunkRetirementRequest) -> None:
        if type(request) is not NativeChunkRetirementRequest:
            raise ValueError(_ERROR)
        request.__post_init__()
        stage, database = request.projection.stage, self._admission.database
        if (stage.database_id, str(stage.database_guid), stage.database_name) != (
            database.database_id,
            database.database_guid,
            database.database_name,
        ):
            raise ValueError(_ERROR)

    def _query(self, request: NativeChunkRetirementRequest) -> tuple[tuple[Any, ...], ...]:
        self._validate(request)
        try:
            session = self._authority(request)
            rows = session.query(
                OBSERVE_EXACT_STAGE_SQL,
                observe_stage_parameters(request.projection.stage),
                deadline=self._sessions.deadline,
            )
            self._assert_current(request)
            return rows
        except BaseException as error:
            self._close(primary=error)
            raise

    def prove_containment(self, request: NativeChunkRetirementRequest):
        self._validate(request)
        attempt = self._attempts.read(request.projection.attempt)
        directory = self._directories.read(request.projection.attempt, self._limits)
        if (
            attempt is None
            or directory is None
            or attempt.state.identity != request.projection.attempt
            or attempt.state.phase not in (TdsAttemptPhase.CONTAINED, TdsAttemptPhase.RETIREMENT_REQUIRED)
            or directory.state.parent != request.projection.attempt
            or not directory.state.work_sealed
            or directory.state.retirement_authority != attempt.state
            or any(not slot.settled for slot in directory.state.slots)
        ):
            raise ValueError(_ERROR)
        current = self._progress.observe(request)
        lifecycle = current.lifecycle[-1]
        return bind_native_chunk_containment(
            projection_sha256=request.projection.projection_sha256,
            authorization_sha256=request.authorization.authorization_sha256,
            directory_key=request.projection.directory_key,
            directory_revision=lifecycle.directory_revision,
            authority_digest=request.authority.digest,
            authority_fence=request.authority.fence,
            lifecycle_revision=lifecycle.lifecycle_revision,
            process_absence_sha256=_digest(
                request.projection.projection_sha256, attempt.revision, attempt.state.phase.value
            ),
            credential_channel_absence_sha256=sha256(
                f"{request.projection.directory_key}:{directory.revision}:{directory.state.sequence}".encode()
            ).hexdigest(),
        )

    def observe_exact_incarnation(self, request: NativeChunkRetirementRequest) -> str:
        if not stage_is_exact(self._query(request), request.projection.stage):
            self._close()
            raise ValueError(_ERROR)
        return native_chunk_object_incarnation_digest(request)

    def drop_exact(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        intent: NativeChunkDropIntent,
    ) -> NativeChunkDropProof:
        self._validate_operation(request, reservation, intent)
        outcome = NativeChunkDropOutcome.SUCCEEDED
        uncertainty: BaseException | None = None
        try:
            session = self._authority(request)
            try:
                session.execute(
                    DROP_EXACT_STAGE_SQL,
                    drop_exact_parameters(request.projection.stage),
                    deadline=self._sessions.deadline,
                )
                self._assert_current(request)
                session.commit(deadline=self._sessions.deadline)
            except BaseException as primary:
                try:
                    session.rollback(deadline=self._sessions.deadline)
                except BaseException as rollback_error:
                    primary.add_note(f"retirement rollback failed: {type(rollback_error).__name__}")
                raise
        except ValueError as error:
            if str(error) == _ERROR:
                raise
            uncertainty = error
            outcome = NativeChunkDropOutcome.UNKNOWN
        except Exception as error:
            uncertainty = error
            outcome = NativeChunkDropOutcome.UNKNOWN
        proof = bind_native_chunk_drop(
            operation_sha256=reservation.operation_sha256,
            effect_attempt=intent.effect_attempt,
            observation=NativeChunkDropObservation.INITIAL,
            outcome=outcome,
        )
        if outcome is NativeChunkDropOutcome.UNKNOWN:
            self._close(primary=uncertainty)
        return proof

    def reconcile_drop(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        intent: NativeChunkDropIntent,
        prior: NativeChunkDropProof | None,
    ) -> NativeChunkDropProof:
        self._validate_operation(request, reservation, intent)
        if prior is not None and (
            prior.operation_sha256 != reservation.operation_sha256 or prior.effect_attempt != intent.effect_attempt
        ):
            raise ValueError(_ERROR)
        rows = self._query(request)
        status = stage_catalog_status(rows, request.projection.stage)
        if status == "absent":
            outcome = NativeChunkDropOutcome.SUCCEEDED
        elif status == "exact":
            outcome = NativeChunkDropOutcome.NO_EFFECT
        else:
            self._close()
            raise ValueError(_ERROR)
        return bind_native_chunk_drop(
            operation_sha256=reservation.operation_sha256,
            effect_attempt=intent.effect_attempt,
            observation=NativeChunkDropObservation.RECONCILED,
            outcome=outcome,
        )

    def settle_drop(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        drop: NativeChunkDropProof,
        object_incarnation_sha256: str,
    ) -> NativeChunkSettlementProof:
        self._validate(request)
        if (
            drop.operation_sha256 != reservation.operation_sha256
            or drop.outcome not in (NativeChunkDropOutcome.SUCCEEDED, NativeChunkDropOutcome.FAILED)
            or object_incarnation_sha256 != native_chunk_object_incarnation_digest(request)
        ):
            raise ValueError(_ERROR)
        current = self._progress.observe(request)
        lifecycle = current.lifecycle[-1]
        return bind_native_chunk_settlement(
            projection_sha256=request.projection.projection_sha256,
            authorization_sha256=request.authorization.authorization_sha256,
            operation_id=reservation.operation_id,
            operation_sha256=reservation.operation_sha256,
            effect_attempt=drop.effect_attempt,
            drop_proof_sha256=drop.proof_sha256,
            object_incarnation_sha256=object_incarnation_sha256,
            directory_key=request.projection.directory_key,
            directory_revision=lifecycle.directory_revision,
            authority_digest=request.authority.digest,
            authority_fence=request.authority.fence,
            local_settlement_sha256=sha256(f"{current.projection_sha256}:{drop.proof_sha256}".encode()).hexdigest(),
            remote_settlement_sha256=sha256(f"{object_incarnation_sha256}:{drop.outcome.value}".encode()).hexdigest(),
            outcome=drop.outcome,
        )

    def observe_absence(self, request: NativeChunkRetirementRequest) -> NativeChunkAbsenceProof:
        self._validate(request)
        current = self._progress.observe(request)
        settlement = current.settlement
        if settlement is None:
            raise ValueError(_ERROR)
        lifecycle = current.lifecycle[-1]
        status = stage_catalog_status(self._query(request), request.projection.stage)
        if status == "foreign":
            self._close()
            raise ValueError(_ERROR)
        proof = bind_native_chunk_absence(
            object_incarnation_sha256=native_chunk_object_incarnation_digest(request),
            settlement_sha256=settlement.proof_sha256,
            directory_revision=lifecycle.directory_revision,
            lifecycle_revision=lifecycle.lifecycle_revision,
            absent=status == "absent",
        )
        if not proof.absent:
            self._close()
        return proof

    def observe_capacity(self, request: NativeChunkRetirementRequest) -> NativeChunkCapacityProof:
        self._validate(request)
        current = self._progress.observe(request)
        if current.directory is None:
            raise ValueError(_ERROR)
        released = self._capacity(request) if self._capacity is not None else self._directory_released(request)
        if type(released) is not bool:
            raise ValueError(_ERROR)
        proof = bind_native_chunk_capacity(directory_sha256=current.directory.proof_sha256, released=released)
        if released:
            self._close()
        return proof

    def _directory_released(self, request: NativeChunkRetirementRequest) -> bool:
        directory = self._directories.read(request.projection.attempt, self._limits)
        return bool(directory is not None and directory.state.admission_closed)

    @staticmethod
    def _validate_operation(
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        intent: NativeChunkDropIntent,
    ) -> None:
        expected = NativeChunkRetirementReservation.deterministic(request)
        if reservation != expected or intent.operation_sha256 != reservation.operation_sha256:
            raise ValueError(_ERROR)


__all__ = (
    "OpenManagementSession",
    "SqlClientNativeRetirementOperator",
    "SqlClientRetirementSession",
)
