"""Restartable exact SQL and durable terminal suffix for failed P10g retirement."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from hashlib import sha256
from typing import Any, Literal, Protocol

from dpone.ports.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedRetirementReceipt,
    SqlClientFailedRetirementRequest,
)

_ERROR = "mssql_native.sqlclient_failed_retirement_unknown"
DropOutcome = Literal["succeeded", "unknown", "no_effect"]


def _digest(*parts: object) -> str:
    return sha256(":".join(str(part) for part in parts).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SqlClientFailedRetirementProgress:
    """One request-bound durable prefix; fields may only be appended in order."""

    request_sha256: str
    intent_sha256: str | None = None
    effect_attempt: int = 0
    drop_sha256: str | None = None
    drop_outcome: DropOutcome | None = None
    absence_sha256: str | None = None
    terminal_sha256: str | None = None
    directory_sha256: str | None = None
    capacity_sha256: str | None = None


class SqlClientFailedRetirementProgressStore(Protocol):
    """Durable monotonic compare-and-set store owned by deployment composition."""

    def observe(self, request_sha256: str) -> SqlClientFailedRetirementProgress: ...

    def record(
        self,
        before: SqlClientFailedRetirementProgress,
        after: SqlClientFailedRetirementProgress,
    ) -> SqlClientFailedRetirementProgress: ...


class SqlClientFailedRetirementAttemptAuthority(Protocol):
    """Reservation owner that supplies its retained or recovered terminal writer."""

    def acquire_terminal(self, request: SqlClientFailedRetirementRequest) -> Any: ...

    def release_terminal(self, request: SqlClientFailedRetirementRequest) -> None: ...


class SqlClientFailedRetirementEffects(Protocol):
    """Exact SQL effects executed under the deployment's canonical DDL lock."""

    def observe_exact_incarnation(self, request: SqlClientFailedRetirementRequest) -> str: ...

    def drop_exact(self, request: SqlClientFailedRetirementRequest, intent_sha256: str) -> DropOutcome: ...

    def reconcile_drop(self, request: SqlClientFailedRetirementRequest, intent_sha256: str) -> DropOutcome: ...

    def observe_absence(self, request: SqlClientFailedRetirementRequest, drop_sha256: str) -> str: ...

    def observe_capacity_release(self, request: SqlClientFailedRetirementRequest, directory_sha256: str) -> str: ...


class SqlClientFailedRetirementTerminal:
    """Resume the missing suffix without repeating an uncertain destructive effect."""

    def __init__(
        self,
        reservation: SqlClientFailedRetirementAttemptAuthority,
        progress: SqlClientFailedRetirementProgressStore,
        effects: SqlClientFailedRetirementEffects,
        *,
        deadline: Callable[[], float],
    ) -> None:
        if not callable(deadline):
            raise ValueError(_ERROR)
        self._reservation = reservation
        self._progress = progress
        self._effects = effects
        self._deadline = deadline

    def observe_or_retire(self, request: SqlClientFailedRetirementRequest) -> SqlClientFailedRetirementReceipt:
        """Complete DROP, absence, RETIRED, directory and capacity evidence."""
        released = False
        try:
            receipt, released = self._observe_or_retire(request)
            return receipt
        finally:
            if not released:
                self._reservation.release_terminal(request)

    def _observe_or_retire(
        self, request: SqlClientFailedRetirementRequest
    ) -> tuple[SqlClientFailedRetirementReceipt, bool]:
        if type(request) is not SqlClientFailedRetirementRequest or request.subject.stage is None:
            raise ValueError(_ERROR)
        request.__post_init__()
        progress = self._checked(request, self._progress.observe(request.request_sha256))
        if progress.directory_sha256 is not None:
            if progress.capacity_sha256 is None:
                capacity = self._effects.observe_capacity_release(request, progress.directory_sha256)
                progress = self._record(progress, replace(progress, capacity_sha256=capacity), request)
            return self._receipt(request, progress), True
        attempt = self._reservation.acquire_terminal(request)
        if progress.intent_sha256 is None:
            incarnation = self._effects.observe_exact_incarnation(request)
            if incarnation != self._incarnation(request):
                raise RuntimeError(_ERROR)
            progress = self._record(
                progress,
                replace(progress, intent_sha256=_digest(request.request_sha256, "drop", 0)),
                request,
            )
            created_intent = True
        else:
            created_intent = False
        intent_sha256 = progress.intent_sha256
        if intent_sha256 is None:
            raise RuntimeError(_ERROR)
        if progress.drop_sha256 is None:
            outcome = (
                self._effects.drop_exact(request, intent_sha256)
                if created_intent
                else self._effects.reconcile_drop(request, intent_sha256)
            )
            progress = self._record_drop(progress, outcome, request)
        if progress.drop_outcome == "unknown":
            outcome = self._effects.reconcile_drop(request, intent_sha256)
            progress = self._record_drop(progress, outcome, request)
        if progress.drop_outcome == "no_effect":
            retry_intent = _digest(request.request_sha256, "drop", 1, progress.drop_sha256)
            progress = self._record(
                progress,
                replace(progress, intent_sha256=retry_intent, effect_attempt=1, drop_sha256=None, drop_outcome=None),
                request,
            )
            incarnation = self._effects.observe_exact_incarnation(request)
            if incarnation != self._incarnation(request):
                raise RuntimeError(_ERROR)
            progress = self._record_drop(progress, self._effects.drop_exact(request, retry_intent), request)
        if progress.drop_outcome != "succeeded" or progress.drop_sha256 is None:
            raise RuntimeError(_ERROR)
        if progress.absence_sha256 is None:
            absence = self._effects.observe_absence(request, progress.drop_sha256)
            progress = self._record(progress, replace(progress, absence_sha256=absence), request)
        absence_sha256 = progress.absence_sha256
        drop_sha256 = progress.drop_sha256
        if absence_sha256 is None or drop_sha256 is None:
            raise RuntimeError(_ERROR)
        slots = tuple(slot for slot in attempt.directory.state.slots if slot.command_sha256 == request.request_sha256)
        if len(slots) != 1:
            raise RuntimeError(_ERROR)
        operation_id = slots[0].operation_id
        lifecycle, directory = attempt.complete_retirement(
            operation_id,
            request.subject.observation_sha256,
            absence_sha256,
            request.authorization.authority_sha256,
            drop_sha256,
            absence_sha256,
            deadline=self._deadline(),
        )
        if progress.terminal_sha256 is None:
            progress = self._record(
                progress,
                replace(progress, terminal_sha256=_digest(request.request_sha256, lifecycle.revision, "RETIRED")),
                request,
            )
        if progress.directory_sha256 is None:
            progress = self._record(
                progress,
                replace(
                    progress, directory_sha256=_digest(request.subject.directory_key, directory.revision, "closed")
                ),
                request,
            )
        if progress.capacity_sha256 is None:
            directory_sha256 = progress.directory_sha256
            if directory_sha256 is None:
                raise RuntimeError(_ERROR)
            self._reservation.release_terminal(request)
            capacity = self._effects.observe_capacity_release(request, directory_sha256)
            progress = self._record(progress, replace(progress, capacity_sha256=capacity), request)
        else:
            self._reservation.release_terminal(request)
        return self._receipt(request, progress), True

    @classmethod
    def _receipt(
        cls, request: SqlClientFailedRetirementRequest, progress: SqlClientFailedRetirementProgress
    ) -> SqlClientFailedRetirementReceipt:
        return SqlClientFailedRetirementReceipt.bind(
            request_sha256=request.request_sha256,
            object_incarnation_sha256=cls._incarnation(request),
            drop_settlement_sha256=progress.drop_sha256,
            absence_sha256=progress.absence_sha256,
            terminal_sha256=progress.terminal_sha256,
            directory_sha256=progress.directory_sha256,
            capacity_sha256=progress.capacity_sha256,
        )

    def _record_drop(
        self,
        progress: SqlClientFailedRetirementProgress,
        outcome: DropOutcome,
        request: SqlClientFailedRetirementRequest,
    ) -> SqlClientFailedRetirementProgress:
        if outcome not in ("succeeded", "unknown", "no_effect") or progress.intent_sha256 is None:
            raise RuntimeError(_ERROR)
        proof = _digest(progress.intent_sha256, progress.effect_attempt, outcome)
        return self._record(progress, replace(progress, drop_sha256=proof, drop_outcome=outcome), request)

    def _record(
        self,
        before: SqlClientFailedRetirementProgress,
        after: SqlClientFailedRetirementProgress,
        request: SqlClientFailedRetirementRequest,
    ) -> SqlClientFailedRetirementProgress:
        return self._checked(request, self._progress.record(before, after))

    @staticmethod
    def _checked(
        request: SqlClientFailedRetirementRequest,
        progress: SqlClientFailedRetirementProgress,
    ) -> SqlClientFailedRetirementProgress:
        if type(progress) is not SqlClientFailedRetirementProgress or progress.request_sha256 != request.request_sha256:
            raise RuntimeError(_ERROR)
        prefix = (
            progress.intent_sha256,
            progress.drop_sha256,
            progress.absence_sha256,
            progress.terminal_sha256,
            progress.directory_sha256,
            progress.capacity_sha256,
        )
        seen_none = False
        for value in prefix:
            if value is None:
                seen_none = True
            elif seen_none:
                raise RuntimeError(_ERROR)
        if (progress.drop_sha256 is None) != (progress.drop_outcome is None):
            raise RuntimeError(_ERROR)
        return progress

    @staticmethod
    def _incarnation(request: SqlClientFailedRetirementRequest) -> str:
        subject = request.subject
        return _digest(subject.subject_sha256, subject.object_identity, subject.expected_object_nonce)


__all__ = (
    "SqlClientFailedRetirementAttemptAuthority",
    "SqlClientFailedRetirementEffects",
    "SqlClientFailedRetirementProgress",
    "SqlClientFailedRetirementProgressStore",
    "SqlClientFailedRetirementTerminal",
)
