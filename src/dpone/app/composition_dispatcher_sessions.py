"""Bounded private issuance sessions over the real durable ClickHouse gates.

The required root factory validates staged parent against current SQL authority
and constructs real gates and bounded transports. These methods never grant a
permit from readiness or reconstruct issued passwords after a restart. Absolute
deadlines are checked around collaborator calls; late effects are UNKNOWN, not
cancelled. The factory must bound I/O to the remaining deadline.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock

from dpone.adapters.composition_clickhouse_dispatch_store import MssqlClickHouseDispatchStore
from dpone.adapters.composition_clickhouse_gate import MssqlClickHouseGate
from dpone.adapters.composition_clickhouse_principal import IssuedClickHouseCredentials
from dpone.adapters.composition_clickhouse_transport import ClickHouseDispatchTransport
from dpone.app.composition_dispatcher_context import StagedDispatcherAttempt, StagedDispatcherContextLoader
from dpone.contracts.composition_clickhouse_dispatch import (
    ClickHouseDispatch,
    ClickHouseDispatchObservation,
    CreateGenerationDispatch,
    ExchangeSnapshotDispatch,
    InsertGenerationDispatch,
)
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import CompositionAttemptIdentity, CompositionAttemptProof


@dataclass(frozen=True, slots=True, repr=False)
class DispatcherGateComponents:
    """Root-bound gate and private transport builder; never serialize this value."""

    gate: MssqlClickHouseGate
    bind_transport: Callable[
        [IssuedClickHouseCredentials, MssqlClickHouseDispatchStore, float], ClickHouseDispatchTransport
    ]


@dataclass(frozen=True, slots=True)
class DispatcherOpenedGate:
    """Public issued identity only; SQL evidence remains independently required."""

    attempt_sha256: str
    purpose: str
    gate_id: str


@dataclass(frozen=True, slots=True)
class DispatcherClosedGate:
    closed_gates: CompositionAttemptProof
    quiescence: CompositionAttemptProof


@dataclass(slots=True, repr=False)
class _Session:
    phase: str = "OPENING"
    issuance_finished: bool = False
    closing_call: bool = False
    selected: StagedDispatcherAttempt | None = None
    components: DispatcherGateComponents | None = None
    credentials: IssuedClickHouseCredentials | None = field(default=None, repr=False)


def _same_subject(left: StagedDispatcherAttempt, right: StagedDispatcherAttempt) -> bool:
    return (
        left.attempt,
        left.write,
        left.manifest_document,
        left.context.occurrence,
        left.context.binding,
        left.context.target_binding_ref,
    ) == (
        right.attempt,
        right.write,
        right.manifest_document,
        right.context.occurrence,
        right.context.binding,
        right.context.target_binding_ref,
    )


class DispatcherSessions:
    """In-memory issuance ownership; durable SQL claims remain the send authority.

    Slots, including failed and closed tombstones, are never automatically
    evicted. Capacity exhaustion requires explicit operator lifecycle handling;
    creating a fresh instance never makes a durable gate eligible for reissue.
    """

    def __init__(
        self,
        *,
        loader: StagedDispatcherContextLoader,
        factory: Callable[[StagedDispatcherAttempt, str, float], DispatcherGateComponents],
        max_sessions: int = 256,
        max_inflight: int = 8,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if (
            type(max_sessions) is not int
            or not 0 < max_sessions <= 4096
            or type(max_inflight) is not int
            or not 0 < max_inflight <= 64
            or not callable(factory)
        ):
            raise CompositionAdmissionError("dispatcher_sessions_configuration")
        self._loader, self._factory, self._clock = loader, factory, clock
        self._maximum, self._max_inflight, self._inflight = max_sessions, max_inflight, 0
        self._lock = Lock()
        self._sessions: dict[tuple[str, str, str], _Session] = {}

    def _deadline(self, deadline: float) -> None:
        if type(deadline) not in {int, float} or not math.isfinite(deadline) or not 0 < deadline - self._clock() <= 60:
            raise CompositionAdmissionError("dispatcher_session_deadline")

    def _key(self, authority: str, attempt: CompositionAttemptIdentity, purpose: str) -> tuple[str, str, str]:
        require_digest(authority)
        attempt.__post_init__()
        if purpose not in {"INGEST", "PUBLISHER"}:
            raise CompositionAdmissionError("dispatcher_session_purpose")
        return authority, attempt.attempt_sha256, purpose

    def open(
        self, authority: str, attempt: CompositionAttemptIdentity, purpose: str, *, deadline: float
    ) -> DispatcherOpenedGate:
        self._deadline(deadline)
        key = self._key(authority, attempt, purpose)
        with self._lock:
            if key in self._sessions or len(self._sessions) >= self._maximum:
                raise CompositionAdmissionError("dispatcher_session_unavailable")
            session = self._sessions[key] = _Session()
        try:
            selected = self._loader.load_attempt(authority, attempt)
            self._deadline(deadline)
            components = self._factory(selected, purpose, deadline)
            with self._lock:
                session.selected, session.components = selected, components
                if session.phase != "OPENING":
                    raise CompositionAdmissionError("dispatcher_session_closing")
            self._deadline(deadline)
            credentials = components.gate.issue_once(attempt)
            self._deadline(deadline)
            with self._lock:
                session.issuance_finished = True
                if session.phase != "OPENING":
                    raise CompositionAdmissionError("dispatcher_session_closing")
                session.credentials, session.phase = credentials, "READY"
                return DispatcherOpenedGate(attempt.attempt_sha256, purpose, credentials.user_id)
        except Exception:
            with self._lock:
                session.issuance_finished = True
                session.credentials = None
                if session.phase != "CLOSING":
                    session.phase = "UNKNOWN"
            raise CompositionAdmissionError("dispatcher_open_unknown") from None

    def dispatch(
        self, authority: str, dispatch: ClickHouseDispatch, *, payload: bytes = b"", deadline: float
    ) -> ClickHouseDispatchObservation:
        self._deadline(deadline)
        if type(dispatch) not in {CreateGenerationDispatch, InsertGenerationDispatch, ExchangeSnapshotDispatch}:
            raise CompositionAdmissionError("dispatcher_dispatch_shape")
        dispatch.__post_init__()
        purpose = "PUBLISHER" if isinstance(dispatch, ExchangeSnapshotDispatch) else "INGEST"
        key = self._key(authority, dispatch.attempt, purpose)
        selected = self._loader.load_attempt(authority, dispatch.attempt)
        with self._lock:
            session = self._sessions.get(key)
            if (
                session is None
                or session.phase != "READY"
                or session.credentials is None
                or session.components is None
                or session.selected is None
                or not _same_subject(session.selected, selected)
                or self._inflight >= self._max_inflight
            ):
                raise CompositionAdmissionError("dispatcher_session_unavailable")
            credentials, components = session.credentials, session.components
            self._inflight += 1
        try:
            self._deadline(deadline)
            journal = components.gate.journal(dispatch.attempt, credentials.user_id)
            transport = components.bind_transport(credentials, journal, deadline)
            self._deadline(deadline)
            observation = transport.execute(dispatch, payload=payload)
            # Persist even if the HTTP response exhausted the caller deadline.
            journal.record_completed(dispatch, observation)
            self._deadline(deadline)
            return observation
        except Exception:
            with self._lock:
                session.credentials = None
                if session.phase == "READY":
                    session.phase = "UNKNOWN"
            raise CompositionAdmissionError("dispatcher_dispatch_unknown") from None
        finally:
            with self._lock:
                self._inflight -= 1

    def close(
        self, authority: str, attempt: CompositionAttemptIdentity, purpose: str, *, deadline: float
    ) -> DispatcherClosedGate:
        self._deadline(deadline)
        key = self._key(authority, attempt, purpose)
        with self._lock:
            session = self._sessions.get(key)
            if session is None:
                raise CompositionAdmissionError("dispatcher_session_unavailable")
            session.phase, session.credentials = "CLOSING", None
            if not session.issuance_finished or session.components is None or session.closing_call:
                raise CompositionAdmissionError("dispatcher_close_unknown")
            session.closing_call = True
            components = session.components
        try:
            selected = self._loader.load_attempt(authority, attempt)
            if session.selected is None or not _same_subject(session.selected, selected):
                raise ValueError
            self._deadline(deadline)
            closed = components.gate.close(attempt)
            self._deadline(deadline)
            quiet = components.gate.prove_quiescence(attempt)
            self._deadline(deadline)
            with self._lock:
                session.phase = "CLOSED"
            return DispatcherClosedGate(closed, quiet)
        except Exception:
            raise CompositionAdmissionError("dispatcher_close_unknown") from None
        finally:
            with self._lock:
                session.closing_call = False
