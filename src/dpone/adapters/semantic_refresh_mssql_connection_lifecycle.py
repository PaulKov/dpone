"""Best-effort client-handle lifecycle for SQL Server adapter transactions."""

from __future__ import annotations

from typing import Protocol

from dpone.adapters.semantic_refresh_mssql_continuation_target import (
    SemanticRefreshMssqlContinuationTargetError,
    apply_connection_query_timeout,
)


class SemanticRefreshMssqlAtomicWorkerAdmissionError(RuntimeError):
    """Raised when worker authority cannot be admitted as one exact transaction."""


class MssqlWorkerConnection(Protocol):
    """Minimum connection capability used by atomic worker admission."""

    autocommit: bool
    timeout: int

    def cursor(self) -> object: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


def prepare_transaction_cursor(connection: MssqlWorkerConnection, maximum_seconds: int) -> object:
    """Apply transaction settings that must exist before cursor creation."""

    connection.autocommit = False
    try:
        apply_connection_query_timeout(connection, maximum_seconds)
    except SemanticRefreshMssqlContinuationTargetError as exc:
        raise SemanticRefreshMssqlAtomicWorkerAdmissionError(str(exc)) from exc
    return connection.cursor()


def rollback_if_open(connection: object | None) -> None:
    """Attempt rollback only before commit acknowledgement becomes uncertain."""

    if connection is not None:
        try:
            connection.rollback()  # type: ignore[attr-defined]
        except Exception:
            pass


def close_handle(resource: object | None) -> None:
    """Close one client handle without claiming physical pool quarantine."""

    if resource is not None:
        try:
            resource.close()  # type: ignore[attr-defined]
        except Exception:
            pass


__all__ = [
    "SemanticRefreshMssqlAtomicWorkerAdmissionError",
    "MssqlWorkerConnection",
    "close_handle",
    "prepare_transaction_cursor",
    "rollback_if_open",
]
