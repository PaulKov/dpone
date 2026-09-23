"""Resource-free local-exit custody for the post-grant writer boundary."""

from __future__ import annotations

import os
from copy import deepcopy
from dataclasses import dataclass
from threading import Lock, get_ident
from typing import Any, Never

from dpone.contracts.mssql_tds_worker import TdsAttemptError
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientBulkGrant,
    SqlClientEvidenceReceipt,
    SqlClientObserverAdmission,
    SqlClientRegistration,
    SqlClientResult,
    SqlClientWriterObservationRecord,
    TdsAttemptSnapshot,
    TdsChildExit,
)

ERROR = "mssql_native.sqlclient_writer_execution_unknown"
_TOKEN = object()


def _invalid() -> Never:
    raise ValueError(ERROR)


@dataclass(frozen=True, slots=True, repr=False)
class _LocallyExitedOwner:
    """Immutable, credential-free facts retained for the later P10f claim."""

    writer_admission: SqlClientObserverAdmission
    registration: SqlClientRegistration
    observation: SqlClientWriterObservationRecord
    grant: SqlClientBulkGrant
    result: SqlClientResult
    result_bytes: bytes
    result_receipt: SqlClientEvidenceReceipt
    local_exit: TdsChildExit
    local_exit_receipt: SqlClientEvidenceReceipt
    state: TdsAttemptSnapshot
    evidence: Any
    lifecycle: Any
    operation_deadline: float
    stage: Any
    input_descriptor: Any
    content_expectation: Any
    p9_settlement_sha256: str


class SqlClientWriterLocallyExited:
    """Opaque local completion; it does not assert remote SQL settlement."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def __repr__(self) -> str:
        return "SqlClientWriterLocallyExited(<opaque>)"

    def _claim_p10f_once(self, candidate: SqlClientWriterLocallyExited) -> object:
        _invalid()

    def _assert_p10f_claim(self, candidate: SqlClientWriterLocallyExited, claim: object) -> _LocallyExitedOwner:
        _invalid()


class SqlClientWriterHandledFailure:
    """Credential-free terminal for acknowledged error+exit1 containment."""

    __slots__ = ("_error",)

    def __init__(self, error: TdsAttemptError | None = None) -> None:
        if error is not None and type(error) is not TdsAttemptError:
            _invalid()
        self._error = error

    def __repr__(self) -> str:
        return "SqlClientWriterHandledFailure(<opaque>)"


def _handled_failure(error: TdsAttemptError) -> SqlClientWriterHandledFailure:
    return SqlClientWriterHandledFailure(error)


def handled_failure_error(value: SqlClientWriterHandledFailure) -> TdsAttemptError:
    """Return the closed diagnostic enum; the value carries no retry authority."""
    if type(value) is not SqlClientWriterHandledFailure or type(value._error) is not TdsAttemptError:
        _invalid()
    return value._error


def _locally_exited(owner: _LocallyExitedOwner) -> SqlClientWriterLocallyExited:
    if type(owner) is not _LocallyExitedOwner:
        _invalid()
    references = (owner.evidence, owner.lifecycle, owner.input_descriptor, owner.content_expectation)

    def values() -> tuple:
        return (
            owner.writer_admission,
            owner.registration,
            owner.observation,
            owner.grant,
            owner.result,
            owner.result_bytes,
            owner.result_receipt,
            owner.local_exit,
            owner.local_exit_receipt,
            owner.state,
            owner.operation_deadline,
            owner.stage,
            owner.p9_settlement_sha256,
        )

    snapshot = deepcopy(values())
    if snapshot != values():
        _invalid()
    lock, pid, thread_id = Lock(), os.getpid(), get_ident()
    claim: object | None = None
    exact: SqlClientWriterLocallyExited

    class _Exact(SqlClientWriterLocallyExited, _token=_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _claim_p10f_once(self, candidate):
            nonlocal claim
            if candidate is not self or self is not exact or (os.getpid(), get_ident()) != (pid, thread_id):
                _invalid()
            with lock:
                if claim is not None:
                    _invalid()
                claim = object()
                return claim

        def _assert_p10f_claim(self, candidate, candidate_claim):
            if candidate is not self or self is not exact or (os.getpid(), get_ident()) != (pid, thread_id):
                _invalid()
            with lock:
                if (
                    claim is None
                    or candidate_claim is not claim
                    or values() != snapshot
                    or any(
                        current is not original
                        for current, original in zip(
                            references,
                            (
                                owner.evidence,
                                owner.lifecycle,
                                owner.input_descriptor,
                                owner.content_expectation,
                            ),
                            strict=True,
                        )
                    )
                ):
                    _invalid()
                return owner

    exact = _Exact()
    return exact


__all__ = (
    "SqlClientWriterHandledFailure",
    "SqlClientWriterLocallyExited",
    "handled_failure_error",
)
