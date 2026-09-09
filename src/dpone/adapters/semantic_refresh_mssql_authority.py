"""Thin DB-API loader for protected MSSQL canonical admission authority."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Protocol

from dpone.ports.semantic_refresh_mssql_authority import MssqlCanonicalAuthorityRecord

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


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


class SemanticRefreshMssqlAuthorityLoadError(RuntimeError):
    """Raised when protected canonical authority is absent or ambiguous."""


class MssqlSemanticRefreshCanonicalAuthorityLoader:
    """Load one exact authority record under a serializable control-state read."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._table = f"[{control_schema}].[semantic_refresh_canonical_authorities]"

    def load(self, workflow_execution_binding_sha256: str) -> MssqlCanonicalAuthorityRecord:
        """Return the durable authority selected only by immutable binding digest."""

        if (
            not isinstance(workflow_execution_binding_sha256, str)
            or _DIGEST.fullmatch(workflow_execution_binding_sha256) is None
        ):
            raise ValueError("workflow_execution_binding_sha256 must be a canonical digest")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(
                f"""
SELECT workflow_execution_id, authority_sha256, authority_json, status
FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
                workflow_execution_binding_sha256,
            )
            row = cursor.fetchone()
            if row is None:
                raise SemanticRefreshMssqlAuthorityLoadError("canonical admission authority is absent")
            workflow_execution_id, authority_sha256, authority_json, status = tuple(row)
            if not isinstance(workflow_execution_id, str) or not workflow_execution_id.strip():
                raise SemanticRefreshMssqlAuthorityLoadError(
                    "canonical admission workflow execution identity is invalid"
                )
            if not isinstance(authority_sha256, str) or _DIGEST.fullmatch(authority_sha256) is None:
                raise SemanticRefreshMssqlAuthorityLoadError("canonical admission authority digest is invalid")
            if not isinstance(authority_json, str) or not authority_json.strip():
                raise SemanticRefreshMssqlAuthorityLoadError("canonical admission authority document is absent")
            if status != "ACTIVE":
                raise SemanticRefreshMssqlAuthorityLoadError("canonical admission authority is not ACTIVE")
            connection.commit()
            return MssqlCanonicalAuthorityRecord(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                workflow_execution_id=workflow_execution_id,
                authority_sha256=authority_sha256,
                authority_json=authority_json,
                status=status,
            )
        except Exception as exc:
            if connection is not None:
                connection.rollback()
            if isinstance(exc, SemanticRefreshMssqlAuthorityLoadError):
                raise
            raise SemanticRefreshMssqlAuthorityLoadError("canonical admission authority read failed") from exc
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


__all__ = [
    "MssqlSemanticRefreshCanonicalAuthorityLoader",
    "SemanticRefreshMssqlAuthorityLoadError",
]
