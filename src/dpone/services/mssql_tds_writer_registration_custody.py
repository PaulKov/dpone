"""Closure-bound P10b registration and exact P10c cleanup custody."""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from inspect import getattr_static
from threading import Lock, get_ident
from typing import NamedTuple, Protocol

from dpone.ports.mssql_sqlclient_process import SqlClientProcess
from dpone.ports.mssql_tds_journal import TdsJournalGateway
from dpone.ports.mssql_tds_worker import TdsUnresolvedLaunch
from dpone.services.mssql_tds_writer_authority import SqlClientWriterAdmitted, _AdmissionPlan
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientEvidenceReceipt,
    SqlClientEvidenceRecord,
    SqlClientRegistration,
    TdsChildExit,
)

ERROR = "mssql_native.sqlclient_writer_launch_unknown"
_REGISTERED_TOKEN = object()
_REGISTERED_CLASS_TOKEN = object()
_CLEANUP_CLASS_TOKEN = object()


def _class_call(value: object, name: str, /, *args: object, **kwargs: object):
    descriptor = getattr_static(type(value), name)
    bound = descriptor.__get__(value, type(value))
    return bound(*args, **kwargs)


def _class_property(value: object, name: str):
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        raise ValueError(ERROR)
    return descriptor.__get__(value, type(value))


class EvidenceGateway(Protocol):
    @property
    def observation(self) -> object: ...

    def write(self, record: SqlClientEvidenceRecord, *, deadline: float) -> SqlClientEvidenceReceipt: ...
    def close(self, *, deadline: float) -> None: ...


@dataclass(slots=True, repr=False)
class _LaunchOwner:
    admitted: SqlClientWriterAdmitted
    claim: object | None
    plan: _AdmissionPlan | None
    lifecycle: TdsJournalGateway | None = None
    containment_deadline: float | None = None
    evidence: EvidenceGateway | None = None
    process: SqlClientProcess | None = None
    unresolved: TdsUnresolvedLaunch | None = None
    registration: SqlClientRegistration | None = None
    receipt: SqlClientEvidenceReceipt | None = None
    _cleanup_attempted: bool = False
    _pid: int = field(default_factory=os.getpid)
    _thread_id: int = field(default_factory=get_ident)

    def require_plan(self) -> _AdmissionPlan:
        if type(self.plan) is not _AdmissionPlan:
            raise ValueError(ERROR)
        return self.plan

    def capture_deadline(self, clock: Callable[[], float]) -> None:
        now = clock()
        timeout = self.require_plan().termination_timeout_seconds
        if type(now) is not float or not math.isfinite(now) or type(timeout) is not int:
            raise ValueError(ERROR)
        deadline = now + timeout
        if not math.isfinite(deadline):
            raise ValueError(ERROR)
        self.containment_deadline = deadline

    def cleanup_once(self) -> None:
        if self._cleanup_attempted:
            return
        self._cleanup_attempted = True
        deadline = self.containment_deadline if self.containment_deadline is not None else 0.0
        if os.getpid() != self._pid or get_ident() != self._thread_id:
            return
        if self.process is not None:
            try:
                exited = _class_call(self.process, "terminate", deadline=deadline)
                if (
                    type(exited) is not TdsChildExit
                    or exited.identity != _class_property(self.process, "identity")
                    or exited.reaped is not True
                ):
                    raise ValueError(ERROR)
                _class_call(self.process, "close")
            except BaseException:
                pass
        if self.unresolved is not None:
            try:
                _class_call(self.unresolved, "contain", deadline=deadline)
                _class_call(self.unresolved, "close")
            except BaseException:
                pass
        gateways = (self.evidence, self.lifecycle)
        seen: set[int] = set()
        for gateway in gateways:
            if gateway is None or id(gateway) in seen:
                continue
            seen.add(id(gateway))
            try:
                _class_call(gateway, "close", deadline=deadline)
            except BaseException:
                pass


class _P10cLaunchRefs(NamedTuple):
    """Immutable exact P10b resources captured before capability publication."""

    admitted: SqlClientWriterAdmitted
    claim: object
    plan: _AdmissionPlan
    lifecycle: TdsJournalGateway
    containment_deadline: float
    evidence: EvidenceGateway
    process: SqlClientProcess
    registration: SqlClientRegistration
    receipt: SqlClientEvidenceReceipt


class _P10cCleanupCustody:
    """Opaque cleanup authority retained before the irreversible P10c claim."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _CLEANUP_CLASS_TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def _cleanup_once(self) -> None:
        raise ValueError(ERROR)


class SqlClientWriterRegistered:
    """Opaque live custody; only the future P10c service can claim it once."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _REGISTERED_CLASS_TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def __repr__(self) -> str:
        return "SqlClientWriterRegistered(<opaque>)"

    def _p10c_identity(self) -> SqlClientWriterRegistered:
        raise ValueError(ERROR)

    def _prepare_p10c_cleanup(self, candidate: SqlClientWriterRegistered) -> _P10cCleanupCustody:
        raise ValueError(ERROR)

    def _claim_p10c_once(self, candidate: SqlClientWriterRegistered, cleanup: _P10cCleanupCustody) -> object:
        raise ValueError(ERROR)

    def _assert_p10c_claim(
        self, candidate: SqlClientWriterRegistered, claim: object, cleanup: _P10cCleanupCustody
    ) -> _P10cLaunchRefs:
        raise ValueError(ERROR)


def _create_registered_writer(token: object, owner: _LaunchOwner) -> SqlClientWriterRegistered:
    if token is not _REGISTERED_TOKEN or type(owner) is not _LaunchOwner:
        raise ValueError(ERROR)
    if (
        not isinstance(owner.admitted, SqlClientWriterAdmitted)
        or owner.claim is None
        or type(owner.plan) is not _AdmissionPlan
        or owner.lifecycle is None
        or type(owner.containment_deadline) is not float
        or owner.evidence is None
        or owner.process is None
        or type(owner.registration) is not SqlClientRegistration
        or type(owner.receipt) is not SqlClientEvidenceReceipt
    ):
        raise ValueError(ERROR)
    refs = _P10cLaunchRefs(
        owner.admitted,
        owner.claim,
        owner.plan,
        owner.lifecycle,
        owner.containment_deadline,
        owner.evidence,
        owner.process,
        owner.registration,
        owner.receipt,
    )
    lock = Lock()
    pid, thread_id = os.getpid(), get_ident()
    state = "AVAILABLE"
    exact_claim: object | None = None
    exact: SqlClientWriterRegistered
    exact_cleanup: _P10cCleanupCustody

    def refs_are_current() -> bool:
        return (
            owner.admitted is refs.admitted
            and owner.claim is refs.claim
            and owner.plan is refs.plan
            and owner.lifecycle is refs.lifecycle
            and owner.containment_deadline == refs.containment_deadline
            and owner.evidence is refs.evidence
            and owner.process is refs.process
            and owner.unresolved is None
            and owner.registration is refs.registration
            and owner.receipt is refs.receipt
        )

    cleanup_attempted = False

    class _ExactCleanup(_P10cCleanupCustody, _token=_CLEANUP_CLASS_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _cleanup_once(self) -> None:
            nonlocal cleanup_attempted, state
            if self is not exact_cleanup or os.getpid() != pid or get_ident() != thread_id:
                raise ValueError(ERROR)
            with lock:
                if state != "CLAIMED" or cleanup_attempted:
                    return
                cleanup_attempted = True
                state = "UNKNOWN"
                owner._cleanup_attempted = True
            _cleanup_exact_launch(refs)

    class _ExactWriterRegistered(SqlClientWriterRegistered, _token=_REGISTERED_CLASS_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _p10c_identity(self) -> SqlClientWriterRegistered:
            if self is not exact or os.getpid() != pid or get_ident() != thread_id:
                raise ValueError(ERROR)
            return exact

        def _prepare_p10c_cleanup(self, candidate: SqlClientWriterRegistered) -> _P10cCleanupCustody:
            if candidate is not self or self is not exact or os.getpid() != pid or get_ident() != thread_id:
                raise ValueError(ERROR)
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    raise ValueError(ERROR)
                return exact_cleanup

        def _claim_p10c_once(self, candidate: SqlClientWriterRegistered, cleanup: _P10cCleanupCustody) -> object:
            nonlocal state, exact_claim
            if (
                candidate is not self
                or self is not exact
                or cleanup is not exact_cleanup
                or os.getpid() != pid
                or get_ident() != thread_id
            ):
                raise ValueError(ERROR)
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    raise ValueError(ERROR)
                exact_claim = object()
                state = "CLAIMED"
                return exact_claim

        def _assert_p10c_claim(
            self, candidate: SqlClientWriterRegistered, claim: object, cleanup: _P10cCleanupCustody
        ) -> _P10cLaunchRefs:
            if (
                candidate is not self
                or self is not exact
                or cleanup is not exact_cleanup
                or os.getpid() != pid
                or get_ident() != thread_id
            ):
                raise ValueError(ERROR)
            with lock:
                if state != "CLAIMED" or claim is not exact_claim or not refs_are_current():
                    raise ValueError(ERROR)
                return refs

    exact_cleanup = _ExactCleanup()
    exact = _ExactWriterRegistered()
    return exact


def _cleanup_exact_launch(refs: _P10cLaunchRefs) -> None:
    """Contain only the exact resources captured when P10b published custody."""
    deadline = refs.containment_deadline
    try:
        exited = _class_call(refs.process, "terminate", deadline=deadline)
        if (
            type(exited) is not TdsChildExit
            or exited.identity != _class_property(refs.process, "identity")
            or exited.reaped is not True
        ):
            raise ValueError(ERROR)
        _class_call(refs.process, "close")
    except BaseException:
        pass
    seen: set[int] = set()
    for gateway in (refs.evidence, refs.lifecycle):
        if id(gateway) in seen:
            continue
        seen.add(id(gateway))
        try:
            _class_call(gateway, "close", deadline=deadline)
        except BaseException:
            pass
