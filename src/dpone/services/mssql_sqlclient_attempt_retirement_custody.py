"""Invocation-scoped custody for live SqlClient attempt retirement authority."""

from __future__ import annotations

import os
from collections.abc import Callable
from hashlib import sha256
from threading import Lock, get_ident

from dpone.contracts.mssql_native_chunk_retirement_authority import (
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
)
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import SqlClientFailedRetirementRequest
from dpone.contracts.mssql_tds_suspension import TdsAttemptSuspension
from dpone.contracts.mssql_tds_worker import (
    ParentAuthority,
    TdsAttemptIdentity,
    TdsAttemptPhase,
)
from dpone.services.mssql_tds_attempt import TdsAttempt

_ERROR = "mssql_native.sqlclient_attempt_retirement_custody_invalid"


class SqlClientAttemptRetirementCustody:
    """Transfer each live attempt once from P10f to same-fence P10g preparation."""

    def __init__(
        self,
        resume: Callable[[TdsAttemptSuspension, float], TdsAttempt],
    ) -> None:
        if not callable(resume):
            raise ValueError(_ERROR)
        self._owner = (os.getpid(), get_ident())
        self._lock = Lock()
        self._resume = resume
        self._attempts: dict[TdsAttemptIdentity, TdsAttemptSuspension] = {}
        self._active: dict[TdsAttemptIdentity, TdsAttempt] = {}

    def retain(self, attempt: TdsAttempt, *, deadline: float) -> None:
        self._assert_owner()
        if type(attempt) is not TdsAttempt:
            raise ValueError(_ERROR)
        snapshot = attempt.lifecycle
        if snapshot.state.phase not in (TdsAttemptPhase.VERIFIED, TdsAttemptPhase.CONTAINED):
            raise ValueError(_ERROR)
        with self._lock:
            if snapshot.state.identity in self._attempts:
                raise ValueError(_ERROR)
        suspension = attempt.suspend(deadline=deadline)
        with self._lock:
            self._attempts[snapshot.state.identity] = suspension

    def prepare_verified(self, request: NativeChunkRetirementRequest, *, deadline: float) -> None:
        """Initialize exact normal retirement under the still-current attempt fence."""
        self._assert_owner()
        if type(request) is not NativeChunkRetirementRequest:
            raise ValueError(_ERROR)
        identity = request.projection.attempt
        with self._lock:
            suspended = self._attempts.pop(identity, None)
        if suspended is None:
            raise LookupError("mssql_native.sqlclient_live_attempt_unavailable")
        attempt = self._resume(suspended, deadline)
        try:
            snapshot = attempt.lifecycle
            projection = request.projection
            if (
                snapshot.state.phase not in (TdsAttemptPhase.VERIFIED, TdsAttemptPhase.CONTAINED)
                or snapshot.state.object_identity != projection.object_identity
                or snapshot.state.verification_sha256 != projection.lifecycle_verification_sha256
                or snapshot.state.ownership.fence != request.authority.fence
            ):
                raise ValueError(_ERROR)
            parent = ParentAuthority(request.authority.kind, request.authority.digest)
            if snapshot.state.phase is TdsAttemptPhase.VERIFIED:
                if snapshot.revision != projection.lifecycle_revision:
                    raise ValueError(_ERROR)
                containment_sha256 = sha256(
                    (
                        "dpone.sqlclient.parent-containment.v1\0"
                        + projection.projection_sha256
                        + request.authority.digest
                        + str(request.authority.fence)
                    ).encode()
                ).hexdigest()
                attempt.contain_verified_parent(parent, containment_sha256, deadline=deadline)
            elif snapshot.state.parent_authority != parent:
                raise ValueError(_ERROR)
            reservation = NativeChunkRetirementReservation.deterministic(request)
            attempt.reserve_retirement(
                reservation.operation_id,
                reservation.operation_sha256,
                deadline=deadline,
            )
        finally:
            attempt.close(deadline=deadline)

    def discard(self, identity: TdsAttemptIdentity, *, deadline: float) -> None:
        """Bound teardown when the parent cannot consume retained authority."""
        self._assert_owner()
        with self._lock:
            self._attempts.pop(identity, None)

    def acquire_failed(self, request: SqlClientFailedRetirementRequest, *, deadline: float) -> TdsAttempt:
        """Consume one failed-attempt suspension for a bounded settlement step."""
        self._assert_owner()
        if type(request) is not SqlClientFailedRetirementRequest:
            raise ValueError(_ERROR)
        identity = request.subject.attempt
        with self._lock:
            active = self._active.get(identity)
            suspended = None if active is not None else self._attempts.pop(identity, None)
        if active is None:
            if suspended is None:
                raise LookupError("mssql_native.sqlclient_live_attempt_unavailable")
            try:
                active = self._resume(suspended, deadline)
            except BaseException:
                with self._lock:
                    self._attempts.setdefault(identity, suspended)
                raise
            with self._lock:
                self._active[identity] = active
        state = active.lifecycle.state
        if (
            state.identity != identity
            or state.phase
            not in (
                TdsAttemptPhase.CONTAINED,
                TdsAttemptPhase.RETIREMENT_REQUIRED,
                TdsAttemptPhase.RETIRED,
            )
            or state.object_identity != request.subject.object_identity
            or state.error is not request.subject.error
            or (state.ownership.owner, state.ownership.fence)
            != (request.subject.lease_owner, request.subject.lease_fence)
        ):
            raise ValueError(_ERROR)
        return active

    def release_failed(self, request: SqlClientFailedRetirementRequest, *, deadline: float) -> None:
        """Close actors and retain the exact current prefix for same-invocation replay."""
        self._assert_owner()
        identity = request.subject.attempt
        with self._lock:
            attempt = self._active.pop(identity, None)
        if attempt is None:
            return
        try:
            suspension = attempt.suspend(deadline=deadline)
        except BaseException:
            with self._lock:
                self._active.setdefault(identity, attempt)
            raise
        with self._lock:
            if identity in self._attempts:
                raise ValueError(_ERROR)
            self._attempts[identity] = suspension

    def failed_released(self, request: SqlClientFailedRetirementRequest) -> bool:
        """Observe that the current process holds no live actors for this attempt."""
        self._assert_owner()
        identity = request.subject.attempt
        with self._lock:
            return identity not in self._active

    def close_all(self) -> None:
        """Drop only resource-free suspended continuations at invocation teardown."""
        self._assert_owner()
        with self._lock:
            if self._active:
                raise RuntimeError("mssql_native.sqlclient_live_attempt_not_released")
            self._attempts.clear()

    def _assert_owner(self) -> None:
        if (os.getpid(), get_ident()) != self._owner:
            raise ValueError(_ERROR)


__all__ = ("SqlClientAttemptRetirementCustody",)
