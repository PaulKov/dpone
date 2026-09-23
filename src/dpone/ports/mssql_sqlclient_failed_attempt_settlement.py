"""Capabilities required to settle one exact contained SqlClient failure."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptDisposition,
    SqlClientFailedAttemptSettlementReceipt,
    SqlClientFailedRetirementReceipt,
    SqlClientFailedRetirementRequest,
    decode_failed_retirement_request,
    encode_failed_retirement_request,
)


class SqlClientFailedRetirementObserver(Protocol):
    """Issue one exact request only from current contained durable state."""

    def observe(
        self, plan: NativeChunkPlan, attempt_id: str, lease: WindowLease
    ) -> SqlClientFailedRetirementRequest: ...

    def operation_key(self, plan: NativeChunkPlan, attempt_id: str, lease: WindowLease) -> str: ...


class SqlClientFailedAttemptSettlementCapability(Protocol):
    """Nominal settle-before-retry capability consumed by the native importer."""

    def settle(
        self, plan: NativeChunkPlan, attempt_id: str, lease: WindowLease
    ) -> SqlClientFailedAttemptSettlementReceipt: ...


class SqlClientFailedRetirementReservation(Protocol):
    """Own idempotent CONTAINED-to-RETIREMENT_REQUIRED reservation."""

    def reserve(self, request: SqlClientFailedRetirementRequest) -> None: ...


class SqlClientFailedRetirementTerminal(Protocol):
    """Observe or complete exact DROP/absence/RETIRED durable facts."""

    def observe_or_retire(self, request: SqlClientFailedRetirementRequest) -> SqlClientFailedRetirementReceipt: ...


class SqlClientFailedSettlementJournal(Protocol):
    """Persist intent before retirement and replay one exact terminal receipt."""

    def observe_or_resume(
        self,
        operation_key: str,
        observe_request: Callable[[], SqlClientFailedRetirementRequest],
        resume: Callable[[SqlClientFailedRetirementRequest], SqlClientFailedAttemptSettlementReceipt],
    ) -> SqlClientFailedAttemptSettlementReceipt: ...


__all__ = (
    "SqlClientFailedAttemptDisposition",
    "SqlClientFailedAttemptSettlementCapability",
    "SqlClientFailedAttemptSettlementReceipt",
    "SqlClientFailedRetirementReceipt",
    "SqlClientFailedRetirementRequest",
    "SqlClientFailedRetirementReservation",
    "SqlClientFailedRetirementTerminal",
    "SqlClientFailedRetirementObserver",
    "SqlClientFailedSettlementJournal",
    "decode_failed_retirement_request",
    "encode_failed_retirement_request",
)
