"""Transactional MSSQL resolver for protected activation prerequisites."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_prerequisite_queries import (
    MssqlPrerequisiteAuthorityQueries,
)
from dpone.ports.semantic_refresh_mssql_primitives import MssqlPrerequisiteAuthorityClaim


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlPrerequisiteAuthorityError(RuntimeError):
    """Raised when a protected route/runtime prerequisite is not current."""


class MssqlSemanticRefreshPrerequisiteAuthority:
    """Recheck one canonical model receipt closure under serializable locks."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not control_schema.replace("_", "a").isalnum() or not control_schema[0].isalpha():
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._queries = MssqlPrerequisiteAuthorityQueries(control_schema)

    def require_current(self, claim: MssqlPrerequisiteAuthorityClaim) -> None:
        """Fail closed unless the exact activation/route/runtime closure remains active."""

        if not isinstance(claim, MssqlPrerequisiteAuthorityClaim):
            raise TypeError("claim must be a protected prerequisite authority claim")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            self._queries.require_current(cursor, (claim,))
            connection.commit()
        except Exception as exc:
            if connection is not None:
                connection.rollback()
            raise SemanticRefreshMssqlPrerequisiteAuthorityError(
                "protected MSSQL prerequisite authority is not current and exact"
            ) from exc
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


__all__ = [
    "MssqlSemanticRefreshPrerequisiteAuthority",
    "SemanticRefreshMssqlPrerequisiteAuthorityError",
]
