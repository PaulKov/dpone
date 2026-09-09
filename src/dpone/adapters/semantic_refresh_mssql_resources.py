"""Idempotent aggregate resource reservations for semantic refresh workflows."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Protocol

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESOURCE_COLUMNS = {
    "prepared_models": ("max_prepared_models", "reserved_prepared_models", False),
    "sealed_extract_bytes": ("max_sealed_extract_bytes", "reserved_sealed_extract_bytes", True),
    "clickhouse_staging_bytes": (
        "max_clickhouse_staging_bytes",
        "reserved_clickhouse_staging_bytes",
        True,
    ),
    "shadow_bytes": ("max_shadow_bytes", "reserved_shadow_bytes", True),
    "retained_generation_bytes": (
        "max_peak_bytes",
        "reserved_retained_generation_bytes",
        True,
    ),
}


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshResourceReservationError(RuntimeError):
    """Raised before allocation when an aggregate workflow budget cannot be proven."""


class MssqlSemanticRefreshResourceLedger:
    """CAS-reserve and release exact allocations against one durable workflow budget."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema

    def reserve(
        self,
        *,
        allocation_id: str,
        reservation_id: str,
        resource_kind: str,
        amount: int,
    ) -> dict[str, object]:
        """Reserve one create-once allocation or reconcile its exact replay."""

        _validate_request(allocation_id, reservation_id, resource_kind, amount)
        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            self._begin_and_lock(cursor, allocation_id)
            self.reserve_in_transaction(
                cursor,
                allocation_id=allocation_id,
                reservation_id=reservation_id,
                resource_kind=resource_kind,
                amount=amount,
            )
            connection.commit()
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshResourceReservationError):
                raise
            raise SemanticRefreshResourceReservationError("resource reservation failed") from exc
        finally:
            cursor.close()
            connection.close()
        return {
            "allocation_id": allocation_id,
            "reservation_id": reservation_id,
            "resource_kind": resource_kind,
            "amount": amount,
            "status": "RESERVED",
        }

    def reserve_in_transaction(
        self,
        cursor: _Cursor,
        *,
        allocation_id: str,
        reservation_id: str,
        resource_kind: str,
        amount: int,
    ) -> None:
        """Reserve one exact allocation on an owner transaction."""

        _validate_request(allocation_id, reservation_id, resource_kind, amount)
        existing = self._allocation(cursor, allocation_id)
        expected = (reservation_id, resource_kind, amount, "RESERVED")
        if existing is not None:
            if existing != expected:
                raise SemanticRefreshResourceReservationError("resource allocation identity conflicts")
            return
        self._increase(cursor, reservation_id, resource_kind, amount)
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_resource_allocations")} (
    allocation_id, reservation_id, resource_kind, amount, status
)
OUTPUT inserted.allocation_id
VALUES (?, ?, ?, ?, N'RESERVED');
""".strip(),
            allocation_id,
            reservation_id,
            resource_kind,
            amount,
        )
        _require_output_identity(cursor, allocation_id, "resource allocation")

    def release(self, *, allocation_id: str) -> dict[str, object]:
        """Release one exact allocation only after evidence-bound safe cleanup."""

        if not isinstance(allocation_id, str) or not allocation_id.strip():
            raise ValueError("allocation_id must be non-empty")
        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            self._begin_and_lock(cursor, allocation_id)
            self.release_in_transaction(cursor, allocation_id=allocation_id)
            connection.commit()
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshResourceReservationError):
                raise
            raise SemanticRefreshResourceReservationError("resource release failed") from exc
        finally:
            cursor.close()
            connection.close()
        return {"allocation_id": allocation_id, "status": "RELEASED"}

    def release_in_transaction(self, cursor: _Cursor, *, allocation_id: str) -> None:
        """Release one exact allocation on an authority-owning transaction."""

        existing = self._allocation(cursor, allocation_id)
        if existing is None:
            raise SemanticRefreshResourceReservationError("resource allocation is absent")
        reservation_id, resource_kind, amount, status = existing
        if status == "RESERVED":
            self._decrease(cursor, str(reservation_id), str(resource_kind), int(amount))
            cursor.execute(
                f"""
UPDATE {self._table("semantic_refresh_resource_allocations")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'RELEASED'
OUTPUT inserted.allocation_id
WHERE allocation_id = ? AND status = N'RESERVED';
""".strip(),
                allocation_id,
            )
            _require_output_identity(cursor, allocation_id, "resource allocation release")
        elif status != "RELEASED":
            raise SemanticRefreshResourceReservationError("resource allocation status conflicts")

    def allocation_in_transaction(self, cursor: _Cursor, allocation_id: str) -> tuple[Any, ...] | None:
        """Return one locked allocation row for protected closure reconciliation."""

        return self._allocation(cursor, allocation_id)

    def allocations_for_reservation_in_transaction(
        self,
        cursor: _Cursor,
        reservation_id: str,
    ) -> tuple[tuple[Any, ...], ...]:
        """Return the complete locked allocation inventory for one reservation."""

        cursor.execute(
            f"""
SELECT allocation_id, reservation_id, resource_kind, amount, status
FROM {self._table("semantic_refresh_resource_allocations")} WITH (UPDLOCK, HOLDLOCK)
WHERE reservation_id = ?
ORDER BY allocation_id;
""".strip(),
            reservation_id,
        )
        return tuple(tuple(row) for row in cursor.fetchall())

    def _increase(self, cursor: _Cursor, reservation_id: str, resource_kind: str, amount: int) -> None:
        max_column, reserved_column, affects_peak = _RESOURCE_COLUMNS[resource_kind]
        peak_amount = amount if affects_peak else 0
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
SET {reserved_column} = {reserved_column} + ?,
    reserved_peak_bytes = reserved_peak_bytes + ?
OUTPUT inserted.reservation_id
WHERE reservation_id = ? AND status = N'PREPARING'
  AND {reserved_column} + ? <= {max_column}
  AND reserved_peak_bytes + ? <= max_peak_bytes;
""".strip(),
            amount,
            peak_amount,
            reservation_id,
            amount,
            peak_amount,
        )
        _require_output_identity(cursor, reservation_id, "aggregate resource budget")

    def _decrease(self, cursor: _Cursor, reservation_id: str, resource_kind: str, amount: int) -> None:
        _max_column, reserved_column, affects_peak = _RESOURCE_COLUMNS[resource_kind]
        peak_amount = amount if affects_peak else 0
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
SET {reserved_column} = {reserved_column} - ?,
    reserved_peak_bytes = reserved_peak_bytes - ?
OUTPUT inserted.reservation_id
WHERE reservation_id = ?
  AND {reserved_column} >= ? AND reserved_peak_bytes >= ?;
""".strip(),
            amount,
            peak_amount,
            reservation_id,
            amount,
            peak_amount,
        )
        _require_output_identity(cursor, reservation_id, "aggregate resource release")

    def _allocation(self, cursor: _Cursor, allocation_id: str) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT reservation_id, resource_kind, amount, status
FROM {self._table("semantic_refresh_resource_allocations")} WITH (UPDLOCK, HOLDLOCK)
WHERE allocation_id = ?;
""".strip(),
            allocation_id,
        )
        row = cursor.fetchone()
        return None if row is None else tuple(row)

    def _begin_and_lock(self, cursor: _Cursor, allocation_id: str) -> None:
        cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        cursor.execute(
            """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
            f"dpone:semantic-refresh:resource:{allocation_id}",
        )
        row = cursor.fetchone()
        if row is None or isinstance(row[0], bool) or not isinstance(row[0], int) or row[0] < 0:
            raise SemanticRefreshResourceReservationError("resource allocation lock was not acquired")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _validate_request(allocation_id: str, reservation_id: str, resource_kind: str, amount: int) -> None:
    for value, field_name in ((allocation_id, "allocation_id"), (reservation_id, "reservation_id")):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must be non-empty")
    if resource_kind not in _RESOURCE_COLUMNS:
        raise ValueError("resource_kind is unsupported")
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        raise ValueError("amount must be a positive integer")


def _require_output_identity(cursor: _Cursor, expected: str, label: str) -> None:
    row = cursor.fetchone()
    if row is None or tuple(row) != (expected,):
        raise SemanticRefreshResourceReservationError(f"{label} compare-and-set failed")


__all__ = [
    "MssqlSemanticRefreshResourceLedger",
    "SemanticRefreshResourceReservationError",
]
