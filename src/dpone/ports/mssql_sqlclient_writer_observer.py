"""Opaque one-shot custody for an independently contained SQL observer.

The backend owns the SQL connection and its containing process.  This port
publishes only exact immutable identity, one bounded target observation and
one bounded cleanup capability; it never exposes a cursor or connection.
"""

from __future__ import annotations

import math
import os
from copy import deepcopy
from dataclasses import dataclass
from inspect import getattr_static
from threading import Lock, get_ident
from typing import Protocol

from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientObserverAdmission,
    SqlClientWriterObservation,
)
from dpone.contracts.mssql_tds_api import SqlClientObserverIncarnation, deadline_seconds
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity

ERROR = "mssql_native.sqlclient_writer_observer_unknown"
_CLASS_TOKEN = object()


class SqlClientWriterObserverBackend(Protocol):
    """Already-contained observer backend supplied by the composition root."""

    @property
    def identity(self) -> TdsProcessIdentity: ...

    def observe_once(self, *, session_id: int, nonce: bytes, deadline: float) -> SqlClientWriterObservation: ...

    def contain(self, *, deadline: float) -> TdsChildExit: ...

    def close(self, *, deadline: float) -> None: ...


@dataclass(frozen=True, slots=True)
class _ObserverRefs:
    backend: SqlClientWriterObserverBackend
    identity_origin: TdsProcessIdentity
    identity: TdsProcessIdentity
    observer_admission_origin: SqlClientObserverAdmission
    observer_admission: SqlClientObserverAdmission
    target_admission: SqlClientObserverAdmission
    target_admission_snapshot: SqlClientObserverAdmission
    incarnation_origin: SqlClientObserverIncarnation
    incarnation: SqlClientObserverIncarnation
    attempt_sha256: str
    launch_sha256: str
    credential_custody: object
    operation_deadline_ns: int
    operation_deadline: float
    cleanup_deadline: float


def _call(value: object, name: str, /, *args: object, **kwargs: object):
    descriptor = getattr_static(type(value), name)
    return descriptor.__get__(value, type(value))(*args, **kwargs)


def _property(value: object, name: str):
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        raise ValueError(ERROR)
    return descriptor.__get__(value, type(value))


def _validated_copy(value, expected_type):
    if type(value) is not expected_type:
        raise ValueError(ERROR)
    try:
        value.__post_init__()
        if expected_type is SqlClientObserverAdmission:
            for nested in (value.server, value.database, value.login, value.transport):
                nested.__post_init__()
        copied = deepcopy(value)
        copied.__post_init__()
        if expected_type is SqlClientObserverAdmission:
            for nested in (copied.server, copied.database, copied.login, copied.transport):
                nested.__post_init__()
    except (ValueError, TypeError, AttributeError, RecursionError):
        raise ValueError(ERROR) from None
    if type(copied) is not expected_type or copied != value:
        raise ValueError(ERROR)
    return copied


def _digest(value: object) -> str:
    if type(value) is not str or len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(ERROR)
    return value


class SqlClientWriterObserverCleanup:
    """Closure-bound cleanup for the exact contained observer."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _CLASS_TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def _cleanup_once(self) -> None:
        raise ValueError(ERROR)

    def _settle_once(self) -> TdsChildExit:
        """Confirm exact observer reap and close for the P10e success path."""
        raise ValueError(ERROR)


class SqlClientWriterObserverCustody:
    """Opaque exact-origin observer capability consumed only by P10d."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _CLASS_TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def __repr__(self) -> str:
        return "SqlClientWriterObserverCustody(<opaque>)"

    def _p10d_identity(self) -> SqlClientWriterObserverCustody:
        raise ValueError(ERROR)

    def _prepare_cleanup(self, candidate: SqlClientWriterObserverCustody) -> SqlClientWriterObserverCleanup:
        raise ValueError(ERROR)

    def _claim_once(self, candidate: SqlClientWriterObserverCustody, cleanup: SqlClientWriterObserverCleanup) -> object:
        raise ValueError(ERROR)

    def _assert_claim(
        self,
        candidate: SqlClientWriterObserverCustody,
        claim: object,
        cleanup: SqlClientWriterObserverCleanup,
    ) -> _ObserverRefs:
        raise ValueError(ERROR)

    def _observe_once(
        self,
        candidate: SqlClientWriterObserverCustody,
        claim: object,
        cleanup: SqlClientWriterObserverCleanup,
        *,
        session_id: int,
        nonce: bytes,
        deadline: float,
    ) -> SqlClientWriterObservation:
        raise ValueError(ERROR)


def create_sqlclient_writer_observer_custody(
    backend: SqlClientWriterObserverBackend,
    *,
    observer_admission: SqlClientObserverAdmission,
    target_admission: SqlClientObserverAdmission,
    incarnation: SqlClientObserverIncarnation,
    attempt_sha256: str,
    launch_sha256: str,
    credential_custody: object,
    operation_deadline_ns: int,
    operation_deadline: float,
    cleanup_deadline: float,
) -> SqlClientWriterObserverCustody:
    """Capture an already-contained backend before P10c custody is claimed."""
    identity_origin = _property(backend, "identity")
    identity = _validated_copy(identity_origin, TdsProcessIdentity)
    observer_snapshot = _validated_copy(observer_admission, SqlClientObserverAdmission)
    target_snapshot = _validated_copy(target_admission, SqlClientObserverAdmission)
    incarnation_snapshot = _validated_copy(incarnation, SqlClientObserverIncarnation)
    attempt_digest, launch_digest = _digest(attempt_sha256), _digest(launch_sha256)
    if (
        credential_custody is None
        or type(operation_deadline_ns) is not int
        or deadline_seconds(operation_deadline_ns) != operation_deadline
        or type(operation_deadline) is not float
        or type(cleanup_deadline) is not float
        or not math.isfinite(operation_deadline)
        or not math.isfinite(cleanup_deadline)
        or operation_deadline <= 0.0
        or cleanup_deadline <= 0.0
        or incarnation_snapshot.authority
        != type(incarnation_snapshot.authority)(
            observer_snapshot.server,
            observer_snapshot.database,
            observer_snapshot.login,
            observer_snapshot.transport,
            incarnation_snapshot.authority.principal_resolution,
        )
    ):
        raise ValueError(ERROR)
    refs = _ObserverRefs(
        backend,
        identity_origin,
        identity,
        observer_admission,
        observer_snapshot,
        target_admission,
        target_snapshot,
        incarnation,
        incarnation_snapshot,
        attempt_digest,
        launch_digest,
        credential_custody,
        operation_deadline_ns,
        operation_deadline,
        cleanup_deadline,
    )
    lock, pid, thread_id = Lock(), os.getpid(), get_ident()
    state, exact_claim, observed, cleaned = "AVAILABLE", None, False, False
    exact: SqlClientWriterObserverCustody
    exact_cleanup: SqlClientWriterObserverCleanup

    def local() -> None:
        if os.getpid() != pid or get_ident() != thread_id:
            raise ValueError(ERROR)

    def current() -> None:
        if (
            _property(refs.backend, "identity") is not refs.identity_origin
            or _validated_copy(refs.identity_origin, TdsProcessIdentity) != refs.identity
            or _validated_copy(refs.observer_admission_origin, SqlClientObserverAdmission) != refs.observer_admission
            or _validated_copy(refs.target_admission, SqlClientObserverAdmission) != refs.target_admission_snapshot
            or _validated_copy(refs.incarnation_origin, SqlClientObserverIncarnation) != refs.incarnation
        ):
            raise ValueError(ERROR)

    class _Cleanup(SqlClientWriterObserverCleanup, _token=_CLASS_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _cleanup_once(self) -> None:
            nonlocal cleaned, state
            local()
            if self is not exact_cleanup:
                raise ValueError(ERROR)
            with lock:
                if cleaned:
                    return
                cleaned, state = True, "UNKNOWN"
            try:
                current()
            except BaseException:
                pass
            try:
                exited = _call(refs.backend, "contain", deadline=refs.cleanup_deadline)
                if type(exited) is not TdsChildExit or exited.identity != refs.identity or not exited.reaped:
                    raise ValueError(ERROR)
            except BaseException:
                pass
            try:
                _call(refs.backend, "close", deadline=refs.cleanup_deadline)
            except BaseException:
                pass

        def _settle_once(self) -> TdsChildExit:
            nonlocal cleaned, state
            local()
            if self is not exact_cleanup:
                raise ValueError(ERROR)
            with lock:
                if cleaned:
                    raise ValueError(ERROR)
                cleaned, state = True, "SETTLING"
            failed = False
            exited: object = None
            try:
                current()
            except BaseException:
                failed = True
            try:
                exited = _call(refs.backend, "contain", deadline=refs.cleanup_deadline)
                if type(exited) is not TdsChildExit:
                    raise ValueError(ERROR)
                exited.__post_init__()
                if exited.identity != refs.identity or exited.reaped is not True:
                    raise ValueError(ERROR)
            except BaseException:
                failed = True
            try:
                if _call(refs.backend, "close", deadline=refs.cleanup_deadline) is not None:
                    raise ValueError(ERROR)
            except BaseException:
                failed = True
            with lock:
                state = "UNKNOWN" if failed else "SETTLED"
            if failed or type(exited) is not TdsChildExit:
                raise ValueError(ERROR)
            return exited

    class _Custody(SqlClientWriterObserverCustody, _token=_CLASS_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _p10d_identity(self) -> SqlClientWriterObserverCustody:
            local()
            if self is not exact:
                raise ValueError(ERROR)
            return exact

        def _prepare_cleanup(self, candidate: SqlClientWriterObserverCustody) -> SqlClientWriterObserverCleanup:
            local()
            if candidate is not self or self is not exact or state != "AVAILABLE":
                raise ValueError(ERROR)
            return exact_cleanup

        def _claim_once(
            self, candidate: SqlClientWriterObserverCustody, cleanup: SqlClientWriterObserverCleanup
        ) -> object:
            nonlocal state, exact_claim
            local()
            if candidate is not self or self is not exact or cleanup is not exact_cleanup:
                raise ValueError(ERROR)
            current()
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    raise ValueError(ERROR)
                exact_claim, state = object(), "CLAIMED"
                return exact_claim

        def _assert_claim(
            self,
            candidate: SqlClientWriterObserverCustody,
            claim: object,
            cleanup: SqlClientWriterObserverCleanup,
        ) -> _ObserverRefs:
            local()
            if candidate is not self or self is not exact or cleanup is not exact_cleanup:
                raise ValueError(ERROR)
            current()
            with lock:
                if state != "CLAIMED" or claim is not exact_claim or cleaned:
                    raise ValueError(ERROR)
                return refs

        def _observe_once(
            self,
            candidate: SqlClientWriterObserverCustody,
            claim: object,
            cleanup: SqlClientWriterObserverCleanup,
            *,
            session_id: int,
            nonce: bytes,
            deadline: float,
        ) -> SqlClientWriterObservation:
            nonlocal observed
            retained = self._assert_claim(candidate, claim, cleanup)
            if type(deadline) is not float or deadline != retained.operation_deadline:
                raise ValueError(ERROR)
            with lock:
                if observed:
                    raise ValueError(ERROR)
                observed = True
            value = _call(retained.backend, "observe_once", session_id=session_id, nonce=nonce, deadline=deadline)
            if type(value) is not SqlClientWriterObservation:
                raise ValueError(ERROR)
            return value

    exact_cleanup = _Cleanup()
    exact = _Custody()
    return exact


__all__ = (
    "SqlClientWriterObserverBackend",
    "SqlClientWriterObserverCleanup",
    "SqlClientWriterObserverCustody",
    "create_sqlclient_writer_observer_custody",
)
