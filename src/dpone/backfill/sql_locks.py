"""SQL external campaign locks for durable backfill state backends."""

from __future__ import annotations

import hashlib
from typing import Any


class PostgresAdvisoryCampaignLock:
    """Session-level PostgreSQL advisory lock keyed by backfill run key."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def acquire(self, run_key: str) -> bool:
        rows = self._records(f"SELECT pg_try_advisory_lock({_advisory_lock_id(run_key)}) AS locked")
        return _first_bool(rows, "locked")

    def release(self, run_key: str) -> None:
        self._records(f"SELECT pg_advisory_unlock({_advisory_lock_id(run_key)}) AS unlocked")

    def _records(self, sql: str) -> Any:
        get_records = getattr(self._connector, "get_records", None)
        if not callable(get_records):
            return []
        return get_records(sql, as_dict=True)


class MSSQLApplicationCampaignLock:
    """Session-level SQL Server application lock keyed by backfill run key."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def acquire(self, run_key: str) -> bool:
        rows = self._records(
            """
            DECLARE @lock_result int;
            EXEC @lock_result = sp_getapplock
                @Resource = ?,
                @LockMode = 'Exclusive',
                @LockOwner = 'Session',
                @LockTimeout = 0;
            SELECT @lock_result AS lock_result
            """,
            (_application_lock_name(run_key),),
        )
        return _first_int(rows, "lock_result", default=-999) >= 0

    def is_held(self, run_key: str) -> bool:
        """Prove that this exact SQL session still owns the exclusive lock."""

        rows = self._records(
            """
            SELECT APPLOCK_MODE(N'public', ?, 'Session') AS lock_mode
            """,
            (_application_lock_name(run_key),),
        )
        for row in rows or []:
            if isinstance(row, dict):
                return str(row.get("lock_mode") or "").casefold() == "exclusive"
            if isinstance(row, (list, tuple)) and row:
                return str(row[0] or "").casefold() == "exclusive"
            return str(getattr(row, "lock_mode", "") or "").casefold() == "exclusive"
        return False

    def release(self, run_key: str) -> None:
        self._records(
            """
            DECLARE @release_result int;
            EXEC @release_result = sp_releaseapplock
                @Resource = ?,
                @LockOwner = 'Session';
            SELECT @release_result AS release_result
            """,
            (_application_lock_name(run_key),),
        )

    def _records(self, sql: str, params: tuple[str, ...]) -> Any:
        get_records = getattr(self._connector, "get_records", None)
        if not callable(get_records):
            return []
        return get_records(sql, params=params, as_dict=True)


class MSSQLPublicationTransactionGate:
    """Serialize publication authority handoff inside the caller transaction."""

    def acquire(self, connector: Any, run_key: str, *, timeout_ms: int = 300_000) -> None:
        rows = connector.get_records(
            """
            DECLARE @lock_result int;
            EXEC @lock_result = sp_getapplock
                @Resource = ?,
                @LockMode = 'Exclusive',
                @LockOwner = 'Transaction',
                @LockTimeout = ?;
            SELECT @lock_result AS lock_result
            """,
            (_publication_gate_name(run_key), timeout_ms),
            as_dict=True,
        )
        if _first_int(rows, "lock_result", default=-999) < 0:
            raise RuntimeError("mssql_backfill_publication.campaign_transaction_gate_unavailable")


def _first_bool(rows: Any, key: str) -> bool:
    for row in rows or []:
        if isinstance(row, dict):
            return bool(row.get(key))
        if isinstance(row, (list, tuple)) and row:
            return bool(row[0])
        value = getattr(row, key, None)
        if value is not None:
            return bool(value)
    return False


def _first_int(rows: Any, key: str, *, default: int) -> int:
    for row in rows or []:
        if isinstance(row, dict):
            return int(row.get(key, default))
        if isinstance(row, (list, tuple)) and row:
            return int(row[0])
        value = getattr(row, key, None)
        if value is not None:
            return int(value)
    return default


def _advisory_lock_id(value: str) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    unsigned = int.from_bytes(digest, "big", signed=False)
    return unsigned - (1 << 64) if unsigned >= (1 << 63) else unsigned


def _application_lock_name(value: str) -> str:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()
    return f"dpone:backfill:{digest}"


def _publication_gate_name(value: str) -> str:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()
    return f"dpone:backfill-publication-gate:{digest}"


__all__ = [
    "MSSQLApplicationCampaignLock",
    "MSSQLPublicationTransactionGate",
    "PostgresAdvisoryCampaignLock",
]
