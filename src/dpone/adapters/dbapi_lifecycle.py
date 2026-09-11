"""Small shared DB-API row and cleanup operations without transaction policy.

Cleanup preserves the primary failure. A swallowed rollback/close failure is
never evidence that a target commit was undone or a writer became quiescent.
The owning adapter must independently reconcile those outcomes.
"""

from __future__ import annotations

from typing import Any

from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


def row(cursor: SqlControlCursor) -> tuple[Any, ...] | None:
    """Detach a driver row without changing SQL NULL or empty-result semantics."""
    value = cursor.fetchone()
    return None if value is None else tuple(value)


def rollback(connection: SqlControlConnection | None) -> None:
    """Best-effort cleanup, retaining the caller's primary outcome/failure."""
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


def close(value: object | None) -> None:
    """Best-effort resource release, without asserting server-side closure."""
    if value is not None:
        try:
            value.close()  # type: ignore[attr-defined]
        except Exception:
            pass
