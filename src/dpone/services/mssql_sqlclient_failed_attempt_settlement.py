"""Nominal settle-before-retry orchestration for acknowledged SqlClient errors."""

from __future__ import annotations

from hashlib import sha256

from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptDisposition,
    SqlClientFailedAttemptSettlementReceipt,
    SqlClientFailedRetirementReceipt,
    SqlClientFailedRetirementRequest,
)
from dpone.ports.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedRetirementObserver,
    SqlClientFailedRetirementReservation,
    SqlClientFailedRetirementTerminal,
    SqlClientFailedSettlementJournal,
)

_ERROR = "mssql_native.sqlclient_failed_settlement_unknown"


def failed_settlement_operation_key(request: SqlClientFailedRetirementRequest) -> str:
    """Derive one idempotency key from the exact observer-issued request."""
    if type(request) is not SqlClientFailedRetirementRequest:
        raise ValueError(_ERROR)
    return sha256(b"dpone.sqlclient.failed-settlement-operation.v2\0" + request.request_sha256.encode()).hexdigest()


class SqlClientFailedAttemptSettlementService:
    """Observe, retire and durably authorize a successor in closed order."""

    def __init__(
        self,
        observer: SqlClientFailedRetirementObserver,
        reservation: SqlClientFailedRetirementReservation,
        retirement: SqlClientFailedRetirementTerminal,
        journal: SqlClientFailedSettlementJournal,
    ) -> None:
        self._observer = observer
        self._reservation = reservation
        self._retirement = retirement
        self._journal = journal

    def settle(
        self, plan: NativeChunkPlan, attempt_id: str, lease: WindowLease
    ) -> SqlClientFailedAttemptSettlementReceipt:
        operation_key = self._observer.operation_key(plan, attempt_id, lease)

        def observe() -> SqlClientFailedRetirementRequest:
            request = self._observer.observe(plan, attempt_id, lease)
            if type(request) is not SqlClientFailedRetirementRequest:
                raise RuntimeError(_ERROR)
            return request

        def resume(request: SqlClientFailedRetirementRequest) -> SqlClientFailedAttemptSettlementReceipt:
            self._reservation.reserve(request)
            retirement = self._retirement.observe_or_retire(request)
            if type(retirement) is not SqlClientFailedRetirementReceipt:
                raise RuntimeError(_ERROR)
            retirement.__post_init__()
            if retirement.request_sha256 != request.request_sha256:
                raise RuntimeError(_ERROR)
            return SqlClientFailedAttemptSettlementReceipt.bind(
                request_sha256=request.request_sha256,
                retirement_receipt_sha256=retirement.receipt_sha256,
                operation_key=operation_key,
                disposition=SqlClientFailedAttemptDisposition.RETRY_READY,
            )

        receipt = self._journal.observe_or_resume(operation_key, observe, resume)
        if type(receipt) is not SqlClientFailedAttemptSettlementReceipt or receipt.operation_key != operation_key:
            raise RuntimeError(_ERROR)
        receipt.__post_init__()
        return receipt


__all__ = ("SqlClientFailedAttemptSettlementService", "failed_settlement_operation_key")
