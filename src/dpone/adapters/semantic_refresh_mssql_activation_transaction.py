"""Serializable transaction boundary for MSSQL semantic-refresh activation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_activation_support import (
    SemanticRefreshMssqlActivationError,
)


class ActivationCursor(Protocol):
    """DB-API cursor surface required by activation."""

    def execute(self, sql: str, *parameters: object) -> ActivationCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def close(self) -> None: ...


class ActivationConnection(Protocol):
    """DB-API connection surface required by activation."""

    autocommit: bool

    def cursor(self) -> ActivationCursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class MssqlSemanticRefreshActivationTransactionMixin:
    """Own create-only row reconciliation and transaction lifecycle."""

    _connection_factory: Callable[[], ActivationConnection]

    @staticmethod
    def _insert_absent_exact(
        cursor: ActivationCursor,
        *,
        select_sql: str,
        select_parameters: tuple[object, ...],
        expected: tuple[object, ...],
        insert_sql: str,
        insert_parameters: tuple[object, ...],
        label: str,
    ) -> None:
        cursor.execute(select_sql, *select_parameters)
        existing = row(cursor)
        if existing is None:
            cursor.execute(insert_sql, *insert_parameters)
        elif existing != expected:
            raise SemanticRefreshMssqlActivationError(f"{label} authority differs")

    def _transaction(
        self,
        lock_name: str,
        action: Callable[[ActivationCursor], None],
    ) -> None:
        connection: ActivationConnection | None = None
        cursor: ActivationCursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(
                """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
                f"dpone:semantic-refresh:activation:{lock_name}",
            )
            result = cursor.fetchone()
            if result is None or isinstance(result[0], bool) or not isinstance(result[0], int) or result[0] < 0:
                raise SemanticRefreshMssqlActivationError("activation application lock was not acquired")
            action(cursor)
            connection.commit()
        except SemanticRefreshMssqlActivationError:
            rollback(connection)
            raise
        except Exception as exc:
            rollback(connection)
            raise SemanticRefreshMssqlActivationError("semantic-refresh activation failed") from exc
        finally:
            close(cursor)
            close(connection)


def row(cursor: ActivationCursor) -> tuple[Any, ...] | None:
    """Normalize a DB-API row."""

    value = cursor.fetchone()
    return None if value is None else tuple(value)


def rollback(connection: ActivationConnection | None) -> None:
    """Best-effort rollback without masking the primary error."""

    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


def close(resource: object | None) -> None:
    """Best-effort DB-API resource close."""

    if resource is not None:
        try:
            resource.close()  # type: ignore[attr-defined]
        except Exception:
            pass
