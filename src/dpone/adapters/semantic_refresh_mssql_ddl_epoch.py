"""Protected observation boundary for the machine-enforced MSSQL DDL epoch."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Protocol

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlDdlEpochObservationError(RuntimeError):
    """Raised when the protected epoch cannot be observed exactly."""


class MssqlSemanticRefreshDdlEpochObserver:
    """Read the singleton epoch under a repeatable protected snapshot."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._table = f"[{control_schema}].[semantic_refresh_ddl_epoch]"

    def observe(self) -> int:
        """Return the exact positive epoch or fail closed."""

        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(f"SELECT current_epoch FROM {self._table} WITH (UPDLOCK, HOLDLOCK) WHERE singleton_id = 1;")
            row = cursor.fetchone()
            if row is None or isinstance(row[0], bool) or not isinstance(row[0], int) or row[0] <= 0:
                raise SemanticRefreshMssqlDdlEpochObservationError("DDL epoch is absent or invalid")
            connection.commit()
            return int(row[0])
        except SemanticRefreshMssqlDdlEpochObservationError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlDdlEpochObservationError("DDL epoch observation failed") from exc
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


def _rollback(connection: _Connection | None) -> None:
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


__all__ = [
    "MssqlSemanticRefreshDdlEpochObserver",
    "SemanticRefreshMssqlDdlEpochObservationError",
]
