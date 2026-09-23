"""Opaque cleanup-bound successor custody for the P10d boundary."""

from __future__ import annotations

import os
from dataclasses import dataclass
from threading import Lock, get_ident
from typing import Never

from dpone.ports.mssql_sqlclient_writer_observer import (
    SqlClientWriterObserverCleanup,
    SqlClientWriterObserverCustody,
    _ObserverRefs,
)
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientBulkGrant,
    SqlClientEvidenceReceipt,
    SqlClientWriterObservationRecord,
    TdsAttemptSnapshot,
)
from dpone.services.mssql_tds_writer_observation_validation import _ClaimedRefs, reassert_claimed_refs
from dpone.services.mssql_tds_writer_registration_custody import _P10cCleanupCustody

ERROR = "mssql_native.sqlclient_writer_observation_unknown"
_TOKEN = object()


def _invalid() -> Never:
    raise ValueError(ERROR)


@dataclass(frozen=True, slots=True, repr=False)
class _ResultReadyOwner:
    refs: _ClaimedRefs


@dataclass(frozen=True, slots=True, repr=False)
class _GrantReadyOwner:
    refs: _ClaimedRefs
    observer: SqlClientWriterObserverCustody
    observer_claim: object
    observer_cleanup: SqlClientWriterObserverCleanup
    observer_refs: _ObserverRefs
    observation: SqlClientWriterObservationRecord
    observation_receipt: SqlClientEvidenceReceipt
    grant: SqlClientBulkGrant
    grant_bytes: bytes
    grant_receipt: SqlClientEvidenceReceipt
    state: TdsAttemptSnapshot


class SqlClientWriterContinuationCleanup:
    """Exact inherited writer and optional observer cleanup custody."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def _cleanup_once(self) -> None:
        _invalid()


def _continuation_cleanup(
    writer: _P10cCleanupCustody, observer: SqlClientWriterObserverCleanup | None
) -> SqlClientWriterContinuationCleanup:
    lock, pid, thread_id = Lock(), os.getpid(), get_ident()
    cleaned = False
    exact: SqlClientWriterContinuationCleanup

    class _Exact(SqlClientWriterContinuationCleanup, _token=_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _cleanup_once(self) -> None:
            nonlocal cleaned
            if self is not exact or os.getpid() != pid or get_ident() != thread_id:
                _invalid()
            with lock:
                if cleaned:
                    return
                cleaned = True
            type(writer)._cleanup_once(writer)
            if observer is not None:
                type(observer)._cleanup_once(observer)

    exact = _Exact()
    return exact


class SqlClientWriterResultReady:
    """Opaque exact P10c result for a later settlement stage."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def __repr__(self) -> str:
        return "SqlClientWriterResultReady(<opaque>)"

    def _prepare_settlement_cleanup(self, candidate: SqlClientWriterResultReady) -> SqlClientWriterContinuationCleanup:
        _invalid()

    def _claim_settlement_once(
        self, candidate: SqlClientWriterResultReady, cleanup: SqlClientWriterContinuationCleanup
    ) -> object:
        _invalid()

    def _assert_settlement_claim(
        self,
        candidate: SqlClientWriterResultReady,
        claim: object,
        cleanup: SqlClientWriterContinuationCleanup,
    ) -> _ResultReadyOwner:
        _invalid()


class SqlClientWriterGrantReady:
    """Opaque P10e custody; grant bytes have not been sent."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def __repr__(self) -> str:
        return "SqlClientWriterGrantReady(<opaque>)"

    def _prepare_p10e_cleanup(self, candidate: SqlClientWriterGrantReady) -> SqlClientWriterContinuationCleanup:
        _invalid()

    def _claim_p10e_once(
        self, candidate: SqlClientWriterGrantReady, cleanup: SqlClientWriterContinuationCleanup
    ) -> object:
        _invalid()

    def _assert_p10e_claim(
        self,
        candidate: SqlClientWriterGrantReady,
        claim: object,
        cleanup: SqlClientWriterContinuationCleanup,
    ) -> _GrantReadyOwner:
        _invalid()


def _result_ready(owner: _ResultReadyOwner) -> SqlClientWriterResultReady:
    lock, pid, thread_id = Lock(), os.getpid(), get_ident()
    state, exact_claim = "AVAILABLE", None
    exact: SqlClientWriterResultReady
    exact_cleanup = _continuation_cleanup(owner.refs.cleanup, None)

    class _Exact(SqlClientWriterResultReady, _token=_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _prepare_settlement_cleanup(self, candidate):
            if candidate is not self or self is not exact or (os.getpid(), get_ident()) != (pid, thread_id):
                _invalid()
            with lock:
                if state != "AVAILABLE":
                    _invalid()
                return exact_cleanup

        def _claim_settlement_once(self, candidate, cleanup):
            nonlocal state, exact_claim
            if candidate is not self or self is not exact or cleanup is not exact_cleanup:
                _invalid()
            with lock:
                if (os.getpid(), get_ident()) != (pid, thread_id) or state != "AVAILABLE" or exact_claim is not None:
                    _invalid()
                state, exact_claim = "CLAIMED", object()
                return exact_claim

        def _assert_settlement_claim(self, candidate, claim, cleanup):
            if candidate is not self or self is not exact or cleanup is not exact_cleanup:
                _invalid()
            with lock:
                if (os.getpid(), get_ident()) != (pid, thread_id) or state != "CLAIMED" or claim is not exact_claim:
                    _invalid()
                reassert_claimed_refs(owner.refs)
                return owner

    exact = _Exact()
    return exact


def _grant_ready(owner: _GrantReadyOwner) -> SqlClientWriterGrantReady:
    lock, pid, thread_id = Lock(), os.getpid(), get_ident()
    state, exact_claim = "AVAILABLE", None
    exact: SqlClientWriterGrantReady
    exact_cleanup = _continuation_cleanup(owner.refs.cleanup, owner.observer_cleanup)

    class _Exact(SqlClientWriterGrantReady, _token=_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _prepare_p10e_cleanup(self, candidate):
            if candidate is not self or self is not exact or (os.getpid(), get_ident()) != (pid, thread_id):
                _invalid()
            with lock:
                if state != "AVAILABLE":
                    _invalid()
                return exact_cleanup

        def _claim_p10e_once(self, candidate, cleanup):
            nonlocal state, exact_claim
            if candidate is not self or self is not exact or cleanup is not exact_cleanup:
                _invalid()
            with lock:
                if (os.getpid(), get_ident()) != (pid, thread_id) or state != "AVAILABLE" or exact_claim is not None:
                    _invalid()
                state, exact_claim = "CLAIMED", object()
                return exact_claim

        def _assert_p10e_claim(self, candidate, claim, cleanup):
            if candidate is not self or self is not exact or cleanup is not exact_cleanup:
                _invalid()
            with lock:
                if (os.getpid(), get_ident()) != (pid, thread_id) or state != "CLAIMED" or claim is not exact_claim:
                    _invalid()
                reassert_claimed_refs(owner.refs, owner.state)
                current = type(owner.observer)._assert_claim(
                    owner.observer, owner.observer, owner.observer_claim, owner.observer_cleanup
                )
                if current is not owner.observer_refs:
                    _invalid()
                return owner

    exact = _Exact()
    return exact


class SqlClientWriterObservationUnknown(RuntimeError):
    """Secret-free UNKNOWN retaining cleanup authority only."""

    __slots__ = ("_writer_cleanup", "_observer_cleanup")

    def __init__(
        self, writer_cleanup: _P10cCleanupCustody, observer_cleanup: SqlClientWriterObserverCleanup | None
    ) -> None:
        self._writer_cleanup = writer_cleanup
        self._observer_cleanup = observer_cleanup
        super().__init__(ERROR)

    def __repr__(self) -> str:
        return "SqlClientWriterObservationUnknown(<opaque>)"


__all__ = (
    "SqlClientWriterContinuationCleanup",
    "SqlClientWriterGrantReady",
    "SqlClientWriterObservationUnknown",
    "SqlClientWriterResultReady",
)
