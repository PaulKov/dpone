"""Compose failed-attempt retirement from retained or recovered TDS authority."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from uuid import UUID

from dpone.adapters.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedRetirementTerminal,
    WindowStoreSqlClientFailedRetirementProgress,
)
from dpone.adapters.mssql_sqlclient_failed_retirement_effects import ProductionSqlClientFailedRetirementEffects
from dpone.app.mssql_sqlclient_native_retirement_composition import SqlClientNativeRetirementDeployment
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import SqlClientFailedRetirementRequest
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand, TdsDirectoryLimits
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
from dpone.ports.bounded_window import WindowStore
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody
from dpone.services.mssql_tds_attempt import TdsAttempt

_ERROR = "mssql_native.sqlclient_failed_retirement_reservation_invalid"

AcquireFailedAttempt = Callable[[SqlClientFailedRetirementRequest], TdsAttempt]
ReleaseFailedAttempt = Callable[[SqlClientFailedRetirementRequest], None]


def failed_retirement_operation_id(request: SqlClientFailedRetirementRequest) -> UUID:
    """Return the stable directory-operation identity for an exact request."""
    if type(request) is not SqlClientFailedRetirementRequest:
        raise ValueError(_ERROR)
    value = bytearray(sha256(b"dpone.sqlclient.failed-retirement.v1\0" + request.request_sha256.encode()).digest()[:16])
    value[6] = (value[6] & 0x0F) | 0x40
    value[8] = (value[8] & 0x3F) | 0x80
    return UUID(bytes=bytes(value))


class SqlClientFailedRetirementReservation:
    """Reserve P10g through the real attempt lifecycle and directory actors."""

    def __init__(
        self,
        acquire_attempt: AcquireFailedAttempt,
        release_attempt: ReleaseFailedAttempt | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        operation_timeout: float = 30.0,
    ) -> None:
        if not callable(acquire_attempt) or not callable(monotonic) or type(operation_timeout) not in (int, float):
            raise ValueError(_ERROR)
        if operation_timeout <= 0:
            raise ValueError(_ERROR)
        self._acquire = acquire_attempt
        self._release = release_attempt or (lambda _request: None)
        self._clock = monotonic
        self._timeout = float(operation_timeout)

    def reserve(self, request: SqlClientFailedRetirementRequest) -> None:
        """Durably advance CONTAINED to RETIREMENT_REQUIRED, idempotently."""
        attempt = self._attempt(request)
        try:
            phase = attempt.lifecycle.state.phase
            if phase is TdsAttemptPhase.CONTAINED:
                attempt.reserve_retirement(
                    failed_retirement_operation_id(request),
                    request.request_sha256,
                    deadline=self._clock() + self._timeout,
                )
            elif phase not in (TdsAttemptPhase.RETIREMENT_REQUIRED, TdsAttemptPhase.RETIRED):
                raise RuntimeError(_ERROR)
            self._assert_bound(
                attempt,
                request,
                (TdsAttemptPhase.RETIREMENT_REQUIRED, TdsAttemptPhase.RETIRED),
            )
        finally:
            self._release(request)

    def acquire_terminal(self, request: SqlClientFailedRetirementRequest) -> TdsAttempt:
        """Acquire current writer authority for the terminal retirement suffix."""
        attempt = self._attempt(request)
        self._assert_bound(attempt, request, (TdsAttemptPhase.RETIREMENT_REQUIRED, TdsAttemptPhase.RETIRED))
        return attempt

    def release_terminal(self, request: SqlClientFailedRetirementRequest) -> None:
        """Return terminal authority to resource-free invocation custody."""
        self._release(request)

    def _attempt(self, request: SqlClientFailedRetirementRequest) -> TdsAttempt:
        if type(request) is not SqlClientFailedRetirementRequest:
            raise ValueError(_ERROR)
        request.__post_init__()
        attempt = self._acquire(request)
        if type(attempt) is not TdsAttempt:
            raise RuntimeError(_ERROR)

        return attempt

    @staticmethod
    def _assert_bound(
        attempt: TdsAttempt,
        request: SqlClientFailedRetirementRequest,
        phase: TdsAttemptPhase | tuple[TdsAttemptPhase, ...],
    ) -> None:
        lifecycle, directory, subject = attempt.lifecycle, attempt.directory, request.subject
        allowed = (phase,) if type(phase) is TdsAttemptPhase else phase
        operation_id = failed_retirement_operation_id(request)
        bound_slots = tuple(
            slot
            for slot in directory.state.slots
            if slot.command is TdsCoordinatorCommand.RETIRE
            and slot.operation_id == operation_id
            and slot.command_sha256 == request.request_sha256
        )
        if (
            lifecycle.state.identity != subject.attempt
            or lifecycle.state.phase not in allowed
            or lifecycle.state.object_identity != subject.object_identity
            or lifecycle.state.error is not subject.error
            or lifecycle.state.ownership.owner != subject.lease_owner
            or lifecycle.state.ownership.fence != subject.lease_fence
            or directory.state.parent != subject.attempt
            or directory.ownership.owner != subject.lease_owner
            or directory.ownership.fence != subject.lease_fence
            or not directory.state.work_sealed
            or len(bound_slots) != 1
            or (
                lifecycle.state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED
                and directory.state.retirement_authority != lifecycle.state
            )
            or (
                lifecycle.state.phase is TdsAttemptPhase.RETIRED
                and (
                    directory.state.retirement_authority is None
                    or directory.state.retirement_authority.phase is not TdsAttemptPhase.RETIREMENT_REQUIRED
                    or directory.state.retirement_authority.identity != lifecycle.state.identity
                )
            )
            or any(
                not slot.settled
                and not (
                    slot.command is TdsCoordinatorCommand.RETIRE
                    and slot.operation_id == operation_id
                    and slot.command_sha256 == request.request_sha256
                )
                for slot in directory.state.slots
            )
        ):
            raise RuntimeError(_ERROR)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientFailedRetirementDeployment:
    """Invocation-owned authorities for the failed P10g suffix."""

    store: WindowStore
    lease: WindowLease
    custody: SqlClientAttemptRetirementCustody
    retirement: SqlClientNativeRetirementDeployment
    directory_limits: TdsDirectoryLimits


def compose_sqlclient_failed_retirement(
    deployment: SqlClientFailedRetirementDeployment,
) -> tuple[SqlClientFailedRetirementReservation, SqlClientFailedRetirementTerminal]:
    """Bind custody, fenced progress, SQL effects, and terminal state."""
    if type(deployment) is not SqlClientFailedRetirementDeployment:
        raise ValueError(_ERROR)
    native = deployment.retirement

    def deadline() -> float:
        return native.monotonic() + native.operation_timeout

    reservation = SqlClientFailedRetirementReservation(
        lambda request: deployment.custody.acquire_failed(request, deadline=deadline()),
        lambda request: deployment.custody.release_failed(request, deadline=deadline()),
        monotonic=native.monotonic,
        operation_timeout=native.operation_timeout,
    )

    def assert_current(request: SqlClientFailedRetirementRequest) -> None:
        lifecycle = native.lifecycle_observer.read(request.subject.attempt)
        directory = native.directory_observer.read(request.subject.attempt, deployment.directory_limits)
        if (
            lifecycle is None
            or directory is None
            or lifecycle.state.phase not in (TdsAttemptPhase.RETIREMENT_REQUIRED, TdsAttemptPhase.RETIRED)
            or lifecycle.state.ownership.fence != request.subject.lease_fence
            or directory.ownership.fence != request.subject.lease_fence
            or directory.state.retirement_authority is None
        ):
            raise RuntimeError(_ERROR)

    def capacity(request: SqlClientFailedRetirementRequest) -> bool:
        lifecycle = native.lifecycle_observer.read(request.subject.attempt)
        directory = native.directory_observer.read(request.subject.attempt, deployment.directory_limits)
        return bool(
            lifecycle is not None
            and lifecycle.state.phase is TdsAttemptPhase.RETIRED
            and directory is not None
            and directory.state.admission_closed
            and deployment.custody.failed_released(request)
        )

    effects = ProductionSqlClientFailedRetirementEffects(
        open_management_session=native.open_management_session,
        admission=native.admission,
        expected_server=native.expected_server,
        expected_database=native.expected_database,
        assert_current=assert_current,
        observe_capacity_release=capacity,
        monotonic=native.monotonic,
        operation_timeout=native.operation_timeout,
    )
    terminal = SqlClientFailedRetirementTerminal(
        reservation,
        WindowStoreSqlClientFailedRetirementProgress(deployment.store, deployment.lease),
        effects,
        deadline=deadline,
    )
    return reservation, terminal


__all__ = (
    "SqlClientFailedRetirementDeployment",
    "SqlClientFailedRetirementReservation",
    "compose_sqlclient_failed_retirement",
    "failed_retirement_operation_id",
)
